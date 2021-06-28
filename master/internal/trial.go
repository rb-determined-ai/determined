package internal

import (
	"fmt"
	"sort"
	"time"

	"github.com/determined-ai/determined/master/pkg/workload"

	"github.com/determined-ai/determined/master/pkg/actor/actors"

	"github.com/hashicorp/go-multierror"

	"github.com/determined-ai/determined/proto/pkg/trialv1"

	"github.com/google/uuid"

	"github.com/pkg/errors"

	apiutils "github.com/determined-ai/determined/master/internal/api"
	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/internal/sproto"
	"github.com/determined-ai/determined/master/pkg/actor"
	aproto "github.com/determined-ai/determined/master/pkg/agent"
	"github.com/determined-ai/determined/master/pkg/archive"
	cproto "github.com/determined-ai/determined/master/pkg/container"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/schemas"
	"github.com/determined-ai/determined/master/pkg/schemas/expconf"
	"github.com/determined-ai/determined/master/pkg/ssh"
	"github.com/determined-ai/determined/master/pkg/tasks"
)

// trial is an actor which is responsible for handling:
//  - messages from the scheduler,
//  - messages from the experiment,
//  - messages from the trial container(s), and
//  - keeping the trial table of the database up-to-date.
type trial struct {
	id           int
	idSet        bool
	experimentID int

	// System dependencies.
	rm     *actor.Ref
	logger *actor.Ref
	db     *db.PgDB

	// Fields that are essentially configuration for the trial.
	config              expconf.ExperimentConfig
	taskSpec            *tasks.TaskSpec
	modelDefinition     archive.Archive
	warmStartCheckpoint *model.Checkpoint
	generatedKeys *ssh.PrivateAndPublicKeys

	// experimentState is essentially or our target state.
	experimentState model.State
	// searcher encapsulates the searcher state of the trial.
	searcher TrialSearcherState
	// restarts is a failure count, it increments when the trial fails and we retry it.
	restarts int
	// runID is a count of how many times the task container(s) have stopped and restarted, which
	// could be due to a failure or due to normal pausing and continuing. When RunID increments,
	// it effectively invalidates many outstanding messages associated with the previous run.
	runID int
	// stopped marks that ctx.Self().Stop() has been called and we are in the process
	// of stopping the trial. This is helpful to guarantee the condition to reschedule
	// a task is mutually exclusive with the trial closing.
	stopped bool
	// finalState records the termination state of a closing trial.
	finalState model.State

	// The following fields tracks the interaction with the resource providers.
	// The existence of task signifies the trial has requested to be allocated.
	task *sproto.AllocateRequest
	// The existence of allocations signifies the trial has been allocated.
	allocations []sproto.Allocation
	// The following fields tracks containers and their states.
	containers           map[cproto.ID]cproto.Container
	terminatedFirst      *cproto.ID
	terminatedContainers map[cproto.ID]sproto.TaskContainerStopped
	// preemption encapsulates the preemption state of the currently allocated task.
	// If there is no current task, or it is unallocated, it is nil.
	preemption *preemption
	// rendezvous encapsulates logic of rendezvousing containers of the currently
	// allocated task. If there is no current task, or it is unallocated, it is nil.
	rendezvous *rendezvous
}

// newTrial creates a trial which will try to schedule itself after it receives its first workload.
func newTrial(
	experimentID int,
	initialState model.State,
	searcher TrialSearcherState,
	rm, logger *actor.Ref,
	db *db.PgDB,
	config expconf.ExperimentConfig,
	warmStartCheckpoint *model.Checkpoint,
	taskSpec *tasks.TaskSpec,
	modelDefinition archive.Archive,
) *trial {
	return &trial{
		experimentID:    experimentID,
		experimentState: initialState,
		searcher:        searcher,

		rm:     rm,
		logger: logger,
		db:     db,

		config:              config,
		taskSpec:            taskSpec,
		modelDefinition:     modelDefinition,
		warmStartCheckpoint: warmStartCheckpoint,

		containers:           make(map[cproto.ID]cproto.Container),
		terminatedContainers: make(map[cproto.ID]sproto.TaskContainerStopped),
	}
}

func (t *trial) Receive(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	case actor.PreStart:
		ctx.AddLabel("experiment-id", t.experimentID)
		if t.idSet {
			ctx.AddLabel("trial-id", t.id)
			if err := t.recover(); err != nil {
				return err
			}
			ctx.AddLabel("task-run-id", t.runID)
		}
	case actor.PostStop:
		return t.close()

	case model.State:
		t.experimentState = msg
		switch {
		case t.experimentState == model.ActiveState:
			return t.allocate(ctx)
		case t.experimentState == model.PausedState:
			return t.terminate(ctx, true)
		case model.StoppingStates[t.experimentState]:
			return t.terminate(ctx, false)
		}
	case TrialSearcherState:
		t.searcher = msg
		if !t.searcher.Complete {
			return t.allocate(ctx)
		}

	case sproto.ResourcesAllocated, sproto.TaskContainerStateChanged,
		sproto.ReleaseResources, sproto.ContainerLog:
		return t.processTask(ctx)
	case watchRendezvousInfo, unwatchRendezvousInfo, rendezvousTimeout:
		return t.processRendezvous(ctx)
	case watchPreemption, unwatchPreemption, preemptionTimeout, ackPreemption:
		return t.processPreemption(ctx)

	default:
		return actor.ErrUnexpectedMessage(ctx)
	}

	return nil
}

func (t *trial) allocate(ctx *actor.Context) error {
	if !(t.task == nil &&
		!t.searcher.Complete &&
		t.experimentState == model.ActiveState &&
		!t.stopped) {
		return nil
	}

	var name string
	if t.idSet {
		name = fmt.Sprintf("Trial %d (Experiment %d)", t.id, t.experimentID)
	} else {
		name = fmt.Sprintf("Trial (Experiment %d)", t.experimentID)
	}

	t.task = &sproto.AllocateRequest{
		ID:             sproto.NewTaskID(),
		Name:           name,
		Group:          ctx.Self().Parent(),
		SlotsNeeded:    t.config.Resources().SlotsPerTrial(),
		NonPreemptible: false,
		Label:          t.config.Resources().AgentLabel(),
		ResourcePool:   t.config.Resources().ResourcePool(),
		FittingRequirements: sproto.FittingRequirements{
			SingleAgent: false,
		},
		TaskActor: ctx.Self(),
	}
	if err := ctx.Ask(t.rm, *t.task).Error(); err != nil {
		t.release(ctx)
		return errors.Wrap(err, "failed to request allocation")
	}
	return nil
}

func (t *trial) recover() error {
	runID, restarts, err := t.db.TrialRunIDAndRestartCount(t.id)
	if err != nil {
		return errors.Wrap(err, "restoring old trial state")
	}
	t.runID = runID + 1
	t.restarts = restarts
	return nil
}

func (t *trial) close() error {
	if !t.idSet {
		return nil
	}

	if !t.stopped {
		t.finalState = model.ErrorState
	}

	if err := t.db.EndTrialRuns(t.id); err != nil {
		return errors.Wrap(err, "failed to close trial runs on exit")
	}

	if err := t.db.UpdateTrial(t.id, t.finalState); err != nil {
		return errors.Wrap(err, "failed to update trial with end state")
	}

	return nil
}

func (t *trial) processTask(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	case sproto.ResourcesAllocated:
		return t.processAllocated(ctx, msg)
	case sproto.TaskContainerStateChanged:
		return t.processContainerMessage(ctx, msg)
	case sproto.ReleaseResources:
		return t.terminate(ctx, true)
	case sproto.ContainerLog:
		t.insertLog(ctx, &msg.Container.ID, msg.Message())
	default:
		return actor.ErrUnexpectedMessage(ctx)
	}
	return nil
}

func (t *trial) processContainerMessage(
	ctx *actor.Context, msg sproto.TaskContainerStateChanged,
) error {
	t.containers[msg.Container.ID] = msg.Container
	switch msg.Container.State {
	case cproto.Running:
		t.processContainerRunning(ctx, msg)
	case cproto.Terminated:
		return t.processContainerTerminated(ctx, msg)
	}
	return nil
}

func (t *trial) setID(id int) {
	t.id = id
	t.idSet = true
}

func (t *trial) processAllocated(ctx *actor.Context, msg sproto.ResourcesAllocated) error {
	// Ignore this message if it is from the last run of the trial.
	if t.task == nil || msg.ID != t.task.ID {
		ctx.Log().Infof("ignoring and releasing stale allocation %v (task = %v)", msg, t.task)
		t.release(ctx)
		return nil
	}

	t.allocations = msg.Allocations

	if t.generatedKeys == nil {
		generatedKeys, err := ssh.GenerateKey(nil)
		if err != nil {
			return errors.Wrap(err, "failed to generate keys for trial")
		}
		t.generatedKeys = &generatedKeys
	}

	if !t.idSet {
		modelTrial := model.NewTrial(
			t.searcher.Create.RequestID,
			t.experimentID,
			model.JSONObj(t.searcher.Create.Hparams),
			t.warmStartCheckpoint,
			int64(t.searcher.Create.TrialSeed))
		if err := t.db.AddTrial(modelTrial); err != nil {
			return errors.Wrap(err, "failed to save trial to database")
		}
		t.setID(modelTrial.ID)
		ctx.AddLabel("trial-id", t.id)
		ctx.Tell(t.rm, sproto.SetTaskName{
			Name:        fmt.Sprintf("Trial %d (Experiment %d)", t.id, t.experimentID),
			TaskHandler: ctx.Self(),
		})
		ctx.Tell(ctx.Self().Parent(), trialCreated{requestID: t.searcher.Create.RequestID, trialID: t.id})
	}

	t.runID++
	if err := t.db.AddTrialRun(t.id, t.runID); err != nil {
		return errors.Wrap(err, "failed to save trial run")
	}
	ctx.AddLabel("task-run-id", t.runID)

	ctx.Log().Infof("starting trial container")

	taskToken, err := t.db.StartTaskSession(string(t.task.ID))
	if err != nil {
		return errors.Wrap(err, "cannot start a new task session for a trial")
	}
	t.preemption = newPreemption(t.runID)
	t.rendezvous = newRendezvous(t.runID, ranksFromAllocations(msg.Allocations))
	actors.NotifyAfter(ctx, rendezvousTimeoutDuration, rendezvousTimeout{runID: t.runID})

	latestCheckpoint, err := t.db.LatestCheckpointForTrial(t.id)
	switch {
	case err != nil:
		return errors.Wrapf(err, "failed to query latest checkpoint for trial")
	case latestCheckpoint == nil:
		latestCheckpoint = t.warmStartCheckpoint
	}

	trialSpec := &tasks.TrialSpec{
		Base: *t.taskSpec,

		ExperimentID:     t.experimentID,
		TrialID:          t.id,
		TaskRunID:        t.runID,
		ExperimentConfig:    schemas.Copy(t.config).(expconf.ExperimentConfig),
		ModelDefinition:     t.modelDefinition,
		HParams:          t.searcher.Create.Hparams,
		TrialSeed:        t.searcher.Create.TrialSeed,
		LatestCheckpoint: latestCheckpoint,
		IsMultiAgent:        len(t.allocations) > 1,
	}

	for rank, a := range msg.Allocations {
		a.Start(ctx, trialSpec.ToTaskSpec(t.generatedKeys, taskToken), rank)
	}

	return nil
}

func (t *trial) processContainerRunning(ctx *actor.Context, msg sproto.TaskContainerStateChanged) {
	rank := t.rendezvous.rank(msg.Container.ID)
	ctx.Log().Infof("found container running: %s (rank %d)", msg.Container.ID, rank)

	t.rendezvous.containerStarted(msg.Container.ID, msg.ContainerStarted.Addresses)

	if t.rendezvous.ready() {
		ctx.Log().Info("all containers are connected successfully (task container state changed)")
	}
}

func (t *trial) processContainerTerminated(
	ctx *actor.Context, msg sproto.TaskContainerStateChanged,
) error {
	ctx.Log().Infof("found container terminated: %s", msg.Container.ID)
	t.insertLog(ctx, &msg.Container.ID, msg.ContainerStopped.String())

	if t.terminatedFirst == nil {
		t.terminatedFirst = &msg.Container.ID
	}
	t.terminatedContainers[msg.Container.ID] = *msg.ContainerStopped
	t.rendezvous.containerTerminated(msg.Container.ID)

	switch {
	case !t.rendezvous.ready(), msg.ContainerStopped.Failure != nil:
		return t.terminate(ctx, false)
	default:
		return t.terminate(ctx, true)
	}
}

func (t *trial) processRendezvous(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	case watchRendezvousInfo:
		ctx.RespondWithResult(t.rendezvous.watch(msg.id))
	case unwatchRendezvousInfo:
		t.rendezvous.unwatch(msg.id)
	case rendezvousTimeout:
		if err := t.rendezvous.checkTimeout(msg.runID); err != nil {
			ctx.Tell(t.logger, model.TrialLog{TrialID: t.id, Message: err.Error()})
		}
	default:
		return actor.ErrUnexpectedMessage(ctx)
	}
	return nil
}

func (t *trial) processPreemption(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	case watchPreemption:
		ctx.RespondWithResult(t.preemption.watch(msg.id))
	case unwatchPreemption:
		t.preemption.unwatch(msg.id)
	case preemptionTimeout:
		if err := t.preemption.checkTimeout(t.runID); err != nil {
			ctx.Log().WithError(err).Info("forcibly terminating trial")
			return t.terminate(ctx, false)
		}
	case ackPreemption:
		if err := t.preemption.acknowledge(msg.runID); err != nil {
			if ctx.ExpectingResponse() {
				ctx.Respond(err)
			}
		}
	default:
		return actor.ErrUnexpectedMessage(ctx)
	}
	return nil
}

// terminate terminates the trial. All paths to a terminated trial SHOULD go through this
// function. Exception paths (DB errors, network calls, etc) can terminate by just returning
// an error and letting the resource manager cleanup after the actor dies. It (along with
// the t.terminated(ctx) function invoked once we're actually done terminating) handle all
// the business logic (and edge cases) of how to clean up a trial from any state.
func (t *trial) terminate(ctx *actor.Context, graceful bool) error {
	switch {
	case t.task == nil:
		ctx.Log().Info("terminating trial before resources are requested")
		return t.terminated(ctx)
	case len(t.allocations) == 0:
		ctx.Log().Info("terminating trial before resources are allocated")
		return t.terminated(ctx)
	case len(t.allocations) == len(t.terminatedContainers):
		ctx.Log().Info("terminating trial because all containers have exited")
		return t.terminated(ctx)
	case len(t.terminatedContainers) > 0 && graceful:
		// Working on it.
	case t.rendezvous.ready() && graceful:
		ctx.Log().Info("gracefully terminating trial")
		t.preemption.preempt()
		ctx.Tell(ctx.Self(), preemptionTimeout{t.runID})
	default:
		ctx.Log().Info("forcibly terminating trial")
		for _, allocation := range t.allocations {
			allocation.Kill(ctx)
		}
	}
	return nil
}

// terminated deciding what action to take to cleanup or restart a trial and taking that action.
func (t *trial) terminated(ctx *actor.Context) error {
	var reschedule, failed bool
	var report workload.ExitedReason
	var stop model.State
	status := t.taskExitStatus()
	switch {
	// Check reasons that indicate this termination should be final.
	case t.searcher.Finished():
		stop = model.CompletedState
	case model.StoppingStates[t.experimentState]:
		stop = model.StoppingToTerminalStates[t.experimentState]
	case status.Failure != nil && t.restarts == t.config.MaxRestarts():
		failed = true
		report = workload.Errored
		stop = model.ErrorState

	// Cases where an exit is acceptable.
	case status.Failure.FailureType == aproto.TaskAborted:
		reschedule = true
	case status.Failure != nil:
		failed = true
		reschedule = true
	case t.experimentState == model.PausedState:
		reschedule = false
	case t.preemption.acknowledged() && t.searcher.Complete:
		reschedule = false
	case t.preemption.acknowledged():
		reschedule = true

	// If no error and nothing else to guide us, this must've been a "user requested stop".
	default:
		report = workload.UserCanceled
		stop = model.CanceledState
	}

	ctx.Log().
		WithError(status.Failure).
		WithField("reschedule", reschedule).
		WithField("reported_reason", report).
		WithField("stopping_state", stop).
		WithField("search_finished", t.searcher.Complete).
		WithField("preempted", t.preemption.acknowledged()).
		Info("trial terminated")

	t.release(ctx)
	if err := t.reset(); err != nil {
		return errors.Wrap(err, "failed to reset task")
	}

	if failed {
		ctx.Log().WithError(status.Failure).Errorf(
			"trial failed (restart %d/%d)", t.restarts, t.config.MaxRestarts(),
		)
		t.restarts++
		if err := t.db.SetTrialRestartCount(t.id, t.restarts); err != nil {
			return errors.Wrap(err, "failed to persist restart count")
		}
	}

	if reschedule {
		if err := t.allocate(ctx); err != nil {
			return errors.Wrap(err, "failed to reschedule trial")
		}
	}

	if report != "" {
		ctx.Tell(ctx.Self().Parent(), trialReportEarlyExit{trialID: t.id, reason: report})
	}

	if stop != "" {
		t.stop(ctx, stop)
	}

	return nil
}

func (t *trial) stop(ctx *actor.Context, state model.State) {
	if t.stopped {
		return
	}

	t.stopped = true
	t.finalState = state
	ctx.Self().Stop()
}

func (t *trial) release(ctx *actor.Context) {
	ctx.Tell(t.rm, sproto.ResourcesReleased{TaskActor: ctx.Self()})
}

func (t *trial) reset() error {
	var mErr *multierror.Error

	if t.task != nil {
		if err := t.db.DeleteTaskSessionByTaskID(string(t.task.ID)); err != nil {
			mErr = multierror.Append(mErr, errors.Wrap(err, "error delete task session for a trial"))
		}
	}

	if len(t.allocations) != 0 {
		if err := t.db.CompleteTrialRun(t.id, t.runID); err != nil {
			mErr = multierror.Append(mErr, errors.Wrap(err, "failed to mark trial run completed"))
		}
	}

	t.preemption.close()
	t.preemption = nil
	t.rendezvous.close()
	t.rendezvous = nil
	t.task = nil
	t.allocations = nil
	t.terminatedFirst = nil
	t.terminatedContainers = make(map[cproto.ID]sproto.TaskContainerStopped)

	return mErr.ErrorOrNil()
}

func (t *trial) taskExitStatus() aproto.ContainerStopped {
	anyStarted := func(cs map[cproto.ID]cproto.Container) bool {
		for _, c := range cs {
			if c.State != cproto.Assigned {
				return true
			}
		}
		return false
	}

	if !anyStarted(t.containers) {
		return aproto.ContainerError(aproto.TaskAborted, errors.New("task aborted"))
	}
	if t.terminatedFirst != nil {
		return t.terminatedContainers[*t.terminatedFirst].ContainerStopped
	}
	return aproto.ContainerError(aproto.AgentError, errors.New("no error status provided"))
}

func (t *trial) insertLog(ctx *actor.Context, cID *cproto.ID, msg string) {
	// Log messages should never come in before the trial ID is set, since no trial runners are
	// launched until after the trial ID is set. But for futureproofing, we will log an error while
	// we protect the database.
	if !t.idSet {
		ctx.Log().Warnf("not saving log message from container without a trial ID: %s", msg)
		return
	}

	if t.logger == nil {
		// A trial created for a unit test does not have a logger.
		return
	}

	var cIDStr string
	if cID != nil {
		cIDStr = string(*cID)
	}
	now := time.Now()
	msg += "\n"
	level := "INFO"
	source := "master"
	stdType := "stdout"
	ctx.Tell(t.logger, model.TrialLog{
		TrialID: t.id,
		Log:     &msg,

		ContainerID: &cIDStr,
		Timestamp:   &now,
		Level:       &level,
		Source:      &source,
		StdType:     &stdType,
	})
}

const (
	// MinLocalRendezvousPort is the smallest port to use (from the container's point of view;
	// it will be mapped to some arbitrary port on the host) for communication across containers.
	MinLocalRendezvousPort = 1734

	// MaxLocalRendezvousPort is the largest port to use for communication across containers.
	// Each distributed trial can take up to 2 host based ports and we assume a maximum.
	// of 16 slot per agent. MaxLocalRendezvousPort = MinLocalRendezvousPort + 2*16 - 1.
	MaxLocalRendezvousPort = MinLocalRendezvousPort + 2*16 - 1
)

var rendezvousTimeoutDuration = 10 * time.Minute

type (
	// watchRendezvousInfo begins watching for rendezvous info.
	// When all the containers are ready, the trial will send all the
	// peer addresses on the channel in the response.
	watchRendezvousInfo   struct{ id cproto.ID }
	rendezvousInfoOrError struct {
		info *trialv1.RendezvousInfo
		err  error
	}
	rendezvousWatcher struct {
		C <-chan rendezvousInfoOrError
	}
	unwatchRendezvousInfo struct{ id cproto.ID }

	// It is possible that it takes very long for all containers to be connected after the first
	// container is connected. This might happen when the k8s cluster waits for new instances
	// to spin up, which might not happen at all. At the same time, taking up part of all
	// the resources and waiting is wasteful. So we need to detect this situation.
	rendezvousTimeout struct {
		runID int
	}

	// rendezvous encapsulates the rendezvous state of a trial.
	rendezvous struct {
		runID             int
		watchers          map[cproto.ID]chan<- rendezvousInfoOrError
		ranks             map[cproto.ID]int
		addresses         map[cproto.ID][]cproto.Address
		lastWatchTime     time.Time
		allReadySucceeded bool
	}
)

func newRendezvous(runID int, ranks map[cproto.ID]int) *rendezvous {
	return &rendezvous{
		runID:     runID,
		ranks:     ranks,
		addresses: map[cproto.ID][]cproto.Address{},
		watchers:  map[cproto.ID]chan<- rendezvousInfoOrError{},
	}
}

func ranksFromAllocations(allocations []sproto.Allocation) map[cproto.ID]int {
	ranks := map[cproto.ID]int{}
	for rank, a := range allocations {
		ranks[a.Summary().ID] = rank
	}
	return ranks
}

func (r *rendezvous) watch(id cproto.ID) (rendezvousWatcher, error) {
	if r == nil {
		return rendezvousWatcher{}, apiutils.AsValidationError(
			"no rendezvous for unallocated task",
		)
	} else if _, ok := r.ranks[id]; !ok {
		return rendezvousWatcher{}, apiutils.AsValidationError(
			"rendezvous request from stale container: %s", id,
		)
	} else if _, ok := r.watchers[id]; ok {
		return rendezvousWatcher{}, apiutils.AsValidationError(
			"rendezvous request from already connected container: %s", id,
		)
	}

	// Channel is size 1 since rendezvous info will only ever be sent once.
	w := make(chan rendezvousInfoOrError, 1)
	r.watchers[id] = w
	r.lastWatchTime = time.Now()
	if r.ready() {
		r.push()
	}
	return rendezvousWatcher{C: w}, nil
}

func (r *rendezvous) unwatch(id cproto.ID) {
	if r == nil {
		return
	}
	delete(r.watchers, id)
}

func (r *rendezvous) containerStarted(id cproto.ID, addresses []cproto.Address) {
	r.addresses[id] = addresses
	if r.ready() {
		r.push()
	}
}

func (r *rendezvous) containerTerminated(id cproto.ID) {
	delete(r.addresses, id)
}

func (r rendezvous) rank(id cproto.ID) int {
	return r.ranks[id]
}

// ready returns true if and only if all the containers are reported to be started with the
// ContainerStarted message and their sockets to be connected with the containerConnected
// message. The two messages are not guaranteed to come in-order. During each run of the
// trial, once all the containers are ready this function will return true afterward because this
// function is used in deciding if the trial should be forcibly killed when terminating.
func (r *rendezvous) ready() bool {
	// If a trial has passed allReady it can never return to a state of not ready until the
	// current containers are all terminated.
	if r.allReadySucceeded {
		return true
	}

	allAddressesArrived := len(r.addresses) == len(r.ranks)
	allWaiting := len(r.watchers) == len(r.ranks)

	r.allReadySucceeded = allAddressesArrived && allWaiting
	return r.allReadySucceeded
}

// push gathers up the external addresses for the exposed ports and sends them to all the
// containers in the trial.
func (r rendezvous) push() bool {
	if !r.ready() {
		return false
	}
	caddrs, raddrs, err := r.info()
	for _, caddr := range caddrs {
		w := r.watchers[caddr.id]
		w <- rendezvousInfoOrError{
			info: &trialv1.RendezvousInfo{
				Addresses: raddrs,
				Rank:      int32(r.ranks[caddr.id]),
			},
			err: err,
		}
		close(w)
		delete(r.watchers, caddr.id)
	}
	return true
}

// checkTimeout checks if the task should timeout waiting for rendezvous.
func (r *rendezvous) checkTimeout(runID int) error {
	if r == nil {
		return nil
	}

	if r.runID == runID && time.Now().After(r.lastWatchTime.Add(rendezvousTimeoutDuration)) {
		return errors.New("some containers are taking a long time to " +
			"connect to master; when running on kubernetes this may happen " +
			"because only some of the pods have been scheduled; it is possible " +
			"that some pods will never be scheduled without adding compute " +
			"resources or pausing / killing other experiments in the cluster",
		)
	}
	return nil
}

func (r *rendezvous) close() {
	if r == nil {
		return
	}

	for cID, w := range r.watchers {
		w <- rendezvousInfoOrError{err: errors.New("task terminated")}
		close(w)
		delete(r.watchers, cID)
	}
}

type cAddress struct {
	id        cproto.ID
	addresses []cproto.Address
	ordinal   int
}

func (r *rendezvous) info() ([]cAddress, []string, error) {
	var caddrs []cAddress
	for id, rank := range r.ranks {
		caddr := cAddress{
			id:        id,
			addresses: r.addresses[id],
			ordinal:   rank,
		}
		caddrs = append(caddrs, caddr)

		sort.Slice(caddr.addresses, func(i, j int) bool {
			a := caddr.addresses[i]
			b := caddr.addresses[j]

			return a.ContainerPort < b.ContainerPort
		})
	}

	sort.Slice(caddrs, func(i, j int) bool {
		a := caddrs[i]
		b := caddrs[j]
		switch {
		case a.ordinal == 0 && b.ordinal != 0:
			return true
		case a.ordinal != 0 && b.ordinal == 0:
			return false
		default:
			return a.id < b.id
		}
	})

	var raddrs []string
	var err *multierror.Error
	for _, caddr := range caddrs {
		var addrs []cproto.Address
		for _, addr := range caddr.addresses {
			if MinLocalRendezvousPort <= addr.ContainerPort &&
				addr.ContainerPort <= MaxLocalRendezvousPort {
				addrs = append(addrs, addr)
			}
		}

		if len(addrs) == 1 {
			raddrs = append(raddrs, formatAddress(addrs[0]))
		} else {
			err = multierror.Append(err, fmt.Errorf(
				"found %d rendezvous addresses instead of 1 for container %s; dropping rendezvous addresses %v",
				len(addrs), caddr.id, addrs))
		}
	}
	return caddrs, raddrs, err.ErrorOrNil()
}

func formatAddress(p cproto.Address) string {
	return fmt.Sprintf("%s:%d", p.HostIP, p.HostPort)
}

var (
	preemptionTimeoutDuration = time.Hour
	errNoPreemptionStatus     = errors.New("no preemption status available for unallocated task")
)

type (
	// watchPreemption begins watching if the task has been preempted.
	// The task responds to this message with a channel of bools, where sends of true
	// indicate to preempt and sends of false are used to synchronize (e.g. you want to
	// block until you receive _something_ but not until the first preemption).
	watchPreemption   struct{ id uuid.UUID }
	preemptionWatcher struct{ C <-chan struct{} }
	unwatchPreemption struct{ id uuid.UUID }
	ackPreemption     struct{ runID int }

	// preemptionTimeout is the time after which we forcibly terminate a trial that has no
	// preempted.
	preemptionTimeout struct {
		runID int
	}

	// preemption represents the preemption status of a task. A task is assumed to be preempted
	// exactly one time. The object is "nil safe" - it'll gracefully handle calls on a nil
	// preemption. This is nice until we move to trial has many task actors / generic task actor,
	// where the lifetime of a "preemption" is equivalent to the lifetime of task and they can be
	// initialized together.
	preemption struct {
		runID        int
		preempted    bool
		acked bool
		preemptedAt  time.Time
		// Map of watcher ID to a bool indicating if the trial should preempt.
		watchers map[uuid.UUID]chan<- struct{}
	}
)

func newPreemption(runID int) *preemption {
	return &preemption{
		runID:        runID,
		preempted:    false,
		acked: false,
		watchers:     map[uuid.UUID]chan<- struct{}{},
	}
}

func (p *preemption) watch(id uuid.UUID) (preemptionWatcher, error) {
	if p == nil {
		return preemptionWatcher{}, errNoPreemptionStatus
	}

	// Size 1; at most a single message can be sent and we don't want to block.
	w := make(chan struct{}, 1)
	p.watchers[id] = w

	if p.preempted {
		w <- struct{}{}
		close(w)
		delete(p.watchers, id)
	}

	return preemptionWatcher{C: w}, nil
}

func (p *preemption) unwatch(id uuid.UUID) {
	if p == nil {
		return
	}
	delete(p.watchers, id)
}

func (p *preemption) preempt() {
	if p == nil {
		return
	}
	p.preempted = true
	p.preemptedAt = time.Now()
	for id, w := range p.watchers {
		w <- struct{}{}
		close(w)
		delete(p.watchers, id)
	}
}

func (p *preemption) acknowledge(runID int) error {
	if p == nil {
		return errNoPreemptionStatus
	}

	if p.runID != runID {
		return errStaleRun{expected: runID, actual: p.runID}
	}
	p.acked = true
	return nil
}

func (p *preemption) acknowledged() bool {
	if p == nil {
		return false
	}

	return p.acked
}

func (p *preemption) checkTimeout(runID int) error {
	if p == nil {
		return nil
	}

	if p.runID == runID && time.Now().After(p.preemptedAt.Add(preemptionTimeoutDuration)) {
		return errors.New("preemption timeout out")
	}
	return nil
}

func (p *preemption) close() {
	if p == nil {
		return
	}
	p.preempt()
}

type errStaleRun struct {
	expected, actual int
}

func (e errStaleRun) Error() string {
	return fmt.Sprintf("stale run %d != %d (expected != actual)", e.expected, e.actual)
}

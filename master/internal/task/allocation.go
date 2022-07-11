package task

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/determined-ai/determined/master/internal/cluster"
	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/internal/prom"
	"github.com/determined-ai/determined/master/internal/proxy"
	"github.com/determined-ai/determined/master/internal/sproto"
	"github.com/determined-ai/determined/master/internal/telemetry"
	"github.com/determined-ai/determined/master/pkg/actor"
	"github.com/determined-ai/determined/master/pkg/actor/actors"
	"github.com/determined-ai/determined/master/pkg/cproto"
	detLogger "github.com/determined-ai/determined/master/pkg/logger"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/ptrs"
	"github.com/determined-ai/determined/master/pkg/tasks"
)

type (
	// Allocation encapsulates all the state of a single allocation.
	Allocation struct {
		// System dependencies.
		db     db.DB
		rm     *actor.Ref
		logger *Logger

		// The request to create the allocation, essentially our configuration.
		req sproto.AllocateRequest
		// The persisted representation.
		model model.Allocation

		// State of all our resources.
		resources resourcesList
		// Separates the existence of resources from us having started them.
		resourcesStarted bool
		// Tracks the initial container exit, unless we caused the failure by killed the trial.
		exitErr error
		// Marks that we intentionally killed the allocation so we can know to
		// ignore any errors from containers dying. Not set when we kill an already
		// terminating trial.
		killedWhileRunning bool
		// Marks that the trial exited successfully, but we killed some daemon containers.
		killedDaemons bool
		// We send a kill when we terminate a task forcibly. we terminate forcibly when a container
		// exits non zero. we don't need to send all these kills, so this exists.
		killCooldown *time.Time
		// tracks if we have finished termination.
		exited bool

		// State for specific sub-behaviors of an allocation.
		// Encapsulates the preemption state of the currently allocated task.
		// If there is no current task, or it is unallocated, it is nil.
		preemption *Preemption
		// Encapsulates logic of rendezvousing containers of the currently
		// allocated task. If there is no current task, or it is unallocated, it is nil.
		rendezvous *rendezvous
		// Encapsulates the logic of watching for idle timeouts.
		idleTimeoutWatcher *IdleTimeoutWatcher
		// proxy state
		proxies []string
		// proxyAddress is provided by determined.exec.prep_container if the RM doesn't provide it.
		proxyAddress *string
		// active all gather state
		allGather *allGather

		logCtx   detLogger.Context
		restored bool
	}

	// MarkResourcesDaemon marks the given reservation as a daemon. In the event of a normal exit,
	// the allocation will not wait for it to exit on its own and instead will kill it and instead
	// await it's hopefully quick termination.
	MarkResourcesDaemon struct {
		AllocationID model.AllocationID
		ResourcesID  sproto.ResourcesID
	}
	// AllocationExited summarizes the exit status of an allocation.
	AllocationExited struct {
		// userRequestedStop is when a container unexpectedly exits with 0.
		UserRequestedStop bool
		Err               error
		FinalState        AllocationState
	}
	// BuildTaskSpec is a message to request the task spec from the parent task. This
	// is just a hack since building a task spec cant be semi-costly and we want to defer it
	// until it is needed (we save stuff to the DB and make SSH keys, doing this for 10k trials
	// at once is real bad.
	BuildTaskSpec struct{}
	// AllocationSignal is an interface for signals that can be sent to an allocation.
	AllocationSignal string
	// AllocationSignalWithReason is an message for signals that can be sent to an allocation
	// along with an informational reason about why the signal was sent.
	AllocationSignalWithReason struct {
		AllocationSignal    AllocationSignal
		InformationalReason string
	}
	// AllocationState requests allocation state. A copy is filled and returned.
	AllocationState struct {
		State     model.AllocationState
		Resources map[sproto.ResourcesID]sproto.ResourcesSummary
		Ready     bool

		Addresses  map[sproto.ResourcesID][]cproto.Address
		Containers map[sproto.ResourcesID][]cproto.Container
	}
	// AllocationReady marks an allocation as ready.
	AllocationReady struct {
		Message string
	}
	// SetAllocationProxyAddress manually sets the allocation proxy address.
	SetAllocationProxyAddress struct {
		ProxyAddress string
	}
)

const (
	// Kill is the signal to kill an allocation; analogous to in SIGKILL.
	Kill AllocationSignal = "kill"
	// Terminate is the signal to kill an allocation; analogous to in SIGTERM.
	Terminate AllocationSignal = "terminate"
)

var (
	// resourcesNotAllocated is a sentinel
	resourcesNotAllocated = &sproto.ResourcesAllocated{}
	// taskSpecNotBuilt is a sentinel
	resourcesNotAllocated = &tasks.TaskSpec{}
)

const (
	killCooldown       = 30 * time.Second
	okExitMessage      = "allocation exited successfully"
	missingExitMessage = ""
)

// NewAllocation returns a new allocation, which tracks allocation state in a fairly generic way.
func NewAllocation(
	logCtx detLogger.Context, req sproto.AllocateRequest, db db.DB, rm *actor.Ref, logger *Logger,
) actor.Actor {
	return &Allocation{
		db:     db,
		rm:     rm,
		logger: logger,

		req: req,
		model: model.Allocation{
			AllocationID: req.AllocationID,
			TaskID:       req.TaskID,
			Slots:        req.SlotsNeeded,
			AgentLabel:   req.Name,
			ResourcePool: req.ResourcePool,
		},

		resources: resourcesList{},

		logCtx: detLogger.MergeContexts(logCtx, detLogger.Context{
			"allocation-id": req.AllocationID,
		}),
	}
}

Allocation struct {
	// System dependencies.
	db     db.DB
	rm     *actor.Ref
	logger *Logger

	// The request to create the allocation, essentially our configuration.
	req sproto.AllocateRequest
	// The persisted representation.
	model model.Allocation

	// State of all our resources.
	resources resourcesList
	// Separates the existence of resources from us having started them.
	resourcesStarted bool
	// Tracks the initial container exit, unless we caused the failure by killed the trial.
	exitErr error
	// Marks that we intentionally killed the allocation so we can know to
	// ignore any errors from containers dying. Not set when we kill an already
	// terminating trial.
	killedWhileRunning bool
	// Marks that the trial exited successfully, but we killed some daemon containers.
	killedDaemons bool
	// We send a kill when we terminate a task forcibly. we terminate forcibly when a container
	// exits non zero. we don't need to send all these kills, so this exists.
	killCooldown *time.Time
	// tracks if we have finished termination.
	exited bool

	// State for specific sub-behaviors of an allocation.
	// Encapsulates the preemption state of the currently allocated task.
	// If there is no current task, or it is unallocated, it is nil.
	preemption *Preemption
	// Encapsulates logic of rendezvousing containers of the currently
	// allocated task. If there is no current task, or it is unallocated, it is nil.
	rendezvous *rendezvous
	// Encapsulates the logic of watching for idle timeouts.
	idleTimeoutWatcher *IdleTimeoutWatcher
	// proxy state
	proxies []string
	// proxyAddress is provided by determined.exec.prep_container if the RM doesn't provide it.
	proxyAddress *string
	// active all gather state
	allGather *allGather

	logCtx   detLogger.Context
	restored bool

	///////////////
	_init struct {
		added bool
		session bool
		proxy bool
		// list of containers we actually started, so we know what to teardown
		containersStarted []sproto.ResourcesID
	}

	_resources struct {
		requested bool
		assigned *sproto.ResourcesAllocated
		done bool
	}

	_taskSpec struct {
		requested bool
		spec *tasks.TaskSpec
		done bool
	}

	_closing bool
	_reason string

	// "teardown" means asynchronous cleanups that are required, like making sure containers are
	// cleaned up.  Synchronous cleanup happens in deinit, like writing to the database.
	_teardown struct {
		wantKill bool
		killSent bool
		done bool
	}
}

/*
Theory:

1. I define an "async object" as an object which is probably some implementation of a state machine,
   and it is able to send and receive signals from other objects.  These sorts of objects are common
   in event-based programming (gnome's C-based glib framework, for instance).  There may be a more
   official phrase than "async object" but I don't know it.

2. Systems of async objects may be executed on a single thread or on many threads.  The
   single-threaded case may use helper threads, but it would have some mechanic by which operations
   done on helper threads are brought back into the single main thread before processing.

3. When async object frameworks operate on a single thread, each object can have two types of
   methods: synchronous or asynchronous.

4. A synchronous operation would be like a simple getter.  Another object can call that getter and
   get the result immediately.  In a single-threaded paradigm, this is safe because if object A is
   active (that is, actively mutatating its state), then any other object B must be inactive (that
   is, in a well-defined state, waiting for new events), so A can call B's getters without risk of
   reading a partially-defined state.  Synchronous objects don't have to be getters; A could
   register as a listener of B (inserting itself into a linked list in B), or other things that
   don't modify B's state machine.

5. Allowing a synchronous operation on an object to mutate that object's state can be dangerous.  If
   the top-level object which is operating (mutating its state) makes a synchronous call to B that
   mutate's B's state, then you have broken the assumption that only one object is mutating its
   state at a time.  Ultimately this may lead to "callback hell", where the execution path becomes
   unclear and it's also unclear which methods on which objects are safe to call at a given time.

6. Asynchronous operations avoid callback hell.  An async operation usually takes the form of a
   queueable message to be processed later.  Async operations have the negative side-effect of
   creating additional states, where object A wants a response from B before proceeding.

7. In multi-threaded paradigms, synchronous operations either require locks or are not allowed.
   This is why our actor system is based on 100% message passing (asynchronous operations).

8. As a result, even simple getters result in a pair of messages, which results in an additional
   state for the getter.  Our actor system supports Asks, which allow the return message to skip
   the message queue, avoiding the additional state.  Asks have the negative side-effect that you
   can create deadlock, if two objects ask each other something, since the Ask sender is effectively
   unresponse itself while it waits for the response from the Ask recipient.

Conclusions:

A. I doubt we are ready to go fully single-threaded.  I think it would be totally worth it from a
   complexity perspective, but I think it might be a lot of work and pretty high-risk.

B. Therefore we are stuck with async-only and 100% message passing (regardless of whether or not we
   use our actor system to accomplish it).

C. Therefore we need to get better at writing state machines that fit this paradigm.  That means
   we need to be able to add new states relatively cheaply; easy to read, easy to understand, and
   obviously correct.
*/

/*
Allocation actor problems:
- .Cleanup()  (in fact, it seems like this code exists just because the actor is so complex that we
               can't tell if Cleanup is necessary or not!)
- .terminated()

- PostStop message

- ResourcesReleased in multiple places

*/

/*
actor system problems:
- Tells are for pub/sub mechanics (where you don't care if messages are ever received) but
  unsuitable for normal asynchronous behaviors.  In principle, two tells seem like they could
  replace on ask, but in practice, if the actor receiving the first tell is dead, the actor sending
  the first Tell has no way to know.  It will just be in datalock forever waiting for the second
  Tell.

- The extra states introduced by the actor system are dizzyingly complicated.  An Ask can fail in
  several different ways:
  - the actor fails to respond in Receive
  - the actor inbox is already closed
  - the actor shuts down while the message is in the queue

  In a world with normal function calls and normal locks, only one of these states would be allowed.

- the stop message is processed in a queue with other messages.  That should not be the case.
  Those mechanics make sense inside an actor's state machine, but the actor itself should not have
  those mechanics; stop should prevent additional messages.  Besides, the inbox isn't closed until
  the stop message is processed, so there are still messages that get discarded.

- the message queue guarantees everything is handled in-order... but most things only need to be
  in-order relative to similar things.  E.g. a GetThing could be handled completely independently of
  anything else.  Additionally, having a message queue causes the "message was not processed" case
  to always exist.

  But overall... enforcing everything to happen async isn't a terrible rule I think.  I still have
  mixed feelings on this.  Certainly, it makes it a lot easier for async objects to operate across
  multiple threads.

  Are we ready to go serial-by-default?

???

- It has a hierarchy of ownership, but it does not support structured concurrency.  E.g. parents are
  not required to shutdown their children before they shut down.  In an asynchronous framework, I
  would expect any object with children to have two shutdown-related states: a closing state and a
  closed state.  The closing state is when the normal state machine operation has finished, but
  there is still asynchronous cleanup to do (the children need to be closed), and the closed state
  is after all child objects are already closed.

????????????????

- It introduces additional "weird" states.  I would expect an asynchronous framework to have to
  worry about two lifetimes per object: when memory for an object is allocated, and when an object
  is active (that is, after initialization ends and before the object is closed).  If you try to
  invoke a method on a closed object that is still allocated, such as AllGather.Watch() after the
  AllGather is closed, I would expect

- it introduces an extra layer of lifetimes to worry about.  Normally you have two types of
  lifetimes for an object: when it is allocated, and when it is alive (not closed).  A thing can be
  dead but you can still send messages to it, and it should have well-defined mechanics as to what
  to do if you try to pass a message to it while its closed.  E.g. calling .Watch on an AllGather
  object that was already closed should return with an error immediately.
*/

/*

func advanceState() is a proposed pattern for writing state machines.

It has the following benefits:
- It is simple and flexible.
- Things which were once complex, like breaking an Ask into two Tells, become simple.
- Teardown logic for the state machine, which should always be allowed at any time, is written at
  the top of the advance() function, and therefore applies equally to every possible incoming event.
- In the end, the physical layout of the code approximates the execution path of the code.  Not as
  closely as a goroutine that blocks on select() calls... but that pattern doesn't work cleanly with
  the preemption logic.

Note:
- The state machine must always be able to continue.  Error handling described for the existing
  Allocation actor's Receive() is about right.
*/

// init is called once in PreStart
func (a *Allocation) init(ctx *actor.Context) error {
	RegisterAllocation(a.model.AllocationID, ctx.Self())
	ctx.AddLabels(a.logCtx)

	if a.req.Restore {
		// Load allocation.
		ctx.Log().Debug("RequestResources load allocation")
		err := db.Bun().NewSelect().Model(&a.model).
			Where("allocation_id = ?", a.model.AllocationID).
			Scan(context.TODO())
		if err != nil {
			return errors.Wrap(err, "loading trial allocation")
		}
	} else {
		// Insert new allocation.
		ctx.Log().Debug("RequestResources add allocation")

		a.setModelState(model.AllocationStatePending)
		if err := a.db.AddAllocation(&a.model); err != nil {
			return errors.Wrap(err, "saving trial allocation")
		}
		a._init.added = true

		token, err := a.db.StartAllocationSession(a.model.AllocationID)
		if err != nil {
			return errors.Wrap(err, "starting a new allocation session")
		}
		a._init.session = true
	}

	a.req.TaskActor = ctx.Self()
	return nil
}

// deinit is called once in PostStop
func (a *Allocation) deinit(ctx *actor.Context) error {
	if a._init.session {
		if err := a.db.DeleteAllocationSession(a.model.AllocationID); err != nil {
			ctx.Log().WithError(err).Error("error deleting allocation session")
		}
	}

	if a._init.added {
		_, err := db.Bun().NewDelete().Model((*ResourcesWithState)(nil)).
			Where("allocation_id = ?", a.model.AllocationID).
			Exec(context.TODO())
		if err != nil {
			ctx.Log().WithError(err).Error("error purging restorable")
		}

		a.model.EndTime = ptrs.Ptr(time.Now().UTC())
		if err := a.db.CompleteAllocation(&a.model); err != nil {
			ctx.Log().WithError(err).Error("failed to mark allocation completed")
		}
	}

	if a._init.proxy {
		a.unregisterProxies()
	}

	telemetry.ReportAllocationTerminal(
		ctx.Self().System(), a.db, a.model, a.resources.firstDevice())

	a.UnregisterAllocation(a.model.AllocationID)
}

func (a *Allocation) teardownState(ctx *actor.Context) error {
	// undo any resources we have running or allocated
	if a._resources.requested && !a._teardown.done {
		if a._resources.allocated == nil {
			// wait till resources are allocated so we can cancel them
			// XXX: prolly we should be able to cancel them any time.
			return nil
		}

		if a._teardown.wantKill {
			// XXX: need kill resend logic
			if !a._teardown.killSent {
				for _, id := range containersStarted {
					// XXX: is it safe to kill a container we tried to start but which hasn't started?
					// XXX: check if resources is already exited?
					a._resources.allocated[id].Kill()
				}
				a._teardown.killSent = true
			}
		} else if !a._teardown.preempted {
			a.preemption.Preempt()
			a._teardown.preempted = true
		}

		// wait for all containers to exit
		for id, resrc := range a._resources.allocated {
			// XXX also handle things that weren't started
			if !resrc.Exited {
				return nil
			}
		}

		// release resources and stop
		a.markResourcesReleased(ctx)

		if err := a.purgeRestorableResources(ctx); err != nil {
			ctx.Log().WithError(err).Error("failed to purge restorable resources")
		}

		// XXX fix exit message
		a.sendEvent(ctx, sproto.Event{ExitedEvent: ptrs.Ptr("FIXME")})
		ctx.Tell(a.rm, sproto.ResourcesReleased{TaskActor: ctx.Self()})

		a._teardown.done = true
	}

	// XXX: another guard for after-stop messages
	ctx.Self().Stop()
	return nil
}


func (a *Allocation) advanceState(ctx *actor.Context) error {
	if a._closing {
		return a.teardownState()
	}

	// acquire resources
	if !a._resources.done {
		// request resources
		if !a._resources.requested {
			err := a.RequestResources()
			if err != nil {
				return err
			}
			a._resources.requested = true
		}
		// wait for resources to be assigned
		if a._resources.assigned == nil {
			return nil
		}
		// check for error sentinel
		if a._resources.assigned == resourcesNotAssigned {
			return errors.New("resource manager failed while attempting to acquire resources")
		}
		// proccess the newly assigned resources
		err := a.ResourcesAllocated()
		if err != nil {
			return err
		}
		a._resources.done = true
	}

	// ask the task actor to build us a task spec
	if !a._taskSpec.done {
		// request the task spec
		if !a._taskSpec.requested {
			a.RequestTaskSpec()
			a._taskSpec.requested = true
		}
		// wait for the spec to be built
		if a._taskSpec.spec == nil {
			return nil
		}
		// check for error sentinel
		if a._taskSpec.spec == taskSpecNotBuilt {
			return errors.New("parent failed while building TaskSpec")
		}
		err := a.StartTask()
		if err != nil {
			return err
		}
		a._taskSpec.done = true
	}

	// setup is done, wait for all non-daemon containers to exit

	// XXX

	// done setting up, begin normal operation
}

func (a *Allocation) close(reason string) {
	if !a._closing {
		a._closing = true
		a._reason = reason
	}
}

/* Because we are in a multithreaded paradigm which is 100% message-passing [B], we are going to
   receive messages that mutate state and others that don't: simple getters, or registering
   listeners... things that would be synchronous operations if we were all single-threaded [8].

   The basic rule is: state is only mutated by advanceState(), which guarantees that the state
   machine is defined in one place.  Anything which does not mutate state should be handled right
   in Receive(), to keep advanceState() as simple as possible.
*/
func (a *Allocation) _receive(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	case actor.PreStart:
		if err := a.init(ctx); err != nil {
			// undo whatever we did, since the actor system doesn't send PostStop if PreStart fails.
			if err2 := a.deinit(ctx); err != nil {
				ctx.Log().WithError(err2).Error("performing all gather through master")
			}
			return err
		}
	case actor.PostStop:
		return a.deinit(ctx)

	// getters are always handled in Receive, not in AdvanceState.  Here in Receive, we know we are
	// not in the middle of a state change, so this is a safe time to respond to getters.
	case sproto.GetResourcesContainerState:
		if v, ok := a.resources[msg.ResourcesID]; ok {
			if v.container == nil {
				ctx.Respond(fmt.Errorf("no container associated with %s", msg.ResourcesID))
			} else {
				ctx.Respond(*v.container)
			}
		} else {
			ctx.Respond(fmt.Errorf("unknown resources %s", msg.ResourcesID))
		}
	case AllocationState:
		if ctx.ExpectingResponse() {
			ctx.Respond(a.State())
		}

	// XXX what kind of message is this?
	case sproto.ContainerLog:
		a.sendEvent(ctx, msg.ToEvent())

	// state changing messages
	case sproto.ResourcesAllocated:
		a._resources.allocated = &msg
	case tasks.TaskSpec:
		a._taskSpec.spec = &msg

	case sproto.ReleaseResources:
		a.close("allocation being preempted by the scheduler")
		a._teardown.wantKill = a._teardown.wantKill || msg.ForcePreemption

	case sproto.ChangeRP:
		a.close("allocation resource pool changed")

	case AllocationSignal:
		// a.HandleSignal(ctx, AllocationSignalWithReason{AllocationSignal: msg})
		// XXX this is an empty string, right?
		a.close("FIXME")
		a._teardown.wantKill = a._teardown.wantKill || (msg == Kill)

	case AllocationSignalWithReason:
		a.close(ctx, msg.InformationalReason)
		a._teardown.wantKill = a._teardown.wantKill || (msg.AllocationSignal == Kill)

	case MarkResourcesDaemon:
		if err := a.SetResourcesAsDaemon(ctx, msg.AllocationID, msg.ResourcesID); err != nil {
			a.Error(ctx, err)
		}

	// XXX: it's not clear to me what this message is about
	// These messages allow users (and sometimes an orchestrator, such as HP search)
	// to interact with the allocation. The usually trace back to API calls.
	case AllocationReady:
		// AllocationReady only comes from the running container, so to
		// avoid a race condition with the slower transition to running state
		// which comes via polling for dispatcher RM, move the state to running now.
		a.setMostProgressedModelState(model.AllocationStateRunning)
		a.model.IsReady = ptrs.Ptr(true)
		if err := a.db.UpdateAllocationState(a.model); err != nil {
			a.Error(ctx, err)
		}
		a.sendEvent(ctx, sproto.Event{ServiceReadyEvent: ptrs.Ptr(true)})

	// XXX: this message is also unclear to me
	case SetAllocationProxyAddress:
		if a.req.ProxyPort == nil {
			if ctx.ExpectingResponse() {
				ctx.Respond(ErrBehaviorUnsupported{Behavior: fmt.Sprintf("%T", msg)})
			}
		} else {
			a.proxyAddress = &msg.ProxyAddress
			a.registerProxies(ctx, a.containerProxyAddresses())
			a._init.proxy = true
		}

	///////////////////////////
	case sproto.ResourcesStateChanged:
		a.ResourcesStateChanged(ctx, msg)
	case sproto.ResourcesFailure:
		a.RestoreResourceFailure(ctx, msg)

	///////////////////////////

	return a.advanceState()
}

// Receive implements actor.Actor for the allocation.
// The normal flow of an Allocation is to:
//	(1) request resources,
// 	(2) receive resources,
//	(3) start the given task on the resources and
//	(4) monitor the task as it runs and handle releasing it's resources.
//
// Additionally, there are secondary flows that force exits, such as a
// reservation dying or the scheduler requesting us to stop, or being killed
// by the user; and there are user interactions driven by APIs, along the way,
// such as watching preemption, watching rendezvous, marking resources as
// 'daemon' resources, etc.
//
// An important note is error handling; the allocation cannot suddenly exit -
// it must clean up its resources. If an error occurs that should not force a
// stop, just return the error to the initiator (ctx.Respond for APIs) or log it
// and move on. If an error occurs that should force a stop, it is imperative
// the error is never returned by Receive, and that a.Error(ctx, err) is called,
// that way the allocation can cleanup properly.
func (a *Allocation) Receive(ctx *actor.Context) error {
	switch msg := ctx.Message().(type) {
	// These messages handle interaction with the resource manager. The generally
	// handle the primary allocation lifecycle/functionality.
//	case actor.PreStart:
//		RegisterAllocation(a.model.AllocationID, ctx.Self())
//		ctx.AddLabels(a.logCtx)
//		if err := a.RequestResources(ctx); err != nil {
//			a.Error(ctx, err)
//		}
//	case actor.PostStop:
//		a.Cleanup(ctx)
//		UnregisterAllocation(a.model.AllocationID)

//	case sproto.ResourcesAllocated:
//		if err := a.ResourcesAllocated(ctx, msg); err != nil {
//			a.Error(ctx, err)
//		}
	case sproto.ResourcesStateChanged:
		a.ResourcesStateChanged(ctx, msg)
	case sproto.ResourcesFailure:
		a.RestoreResourceFailure(ctx, msg)
//	case sproto.GetResourcesContainerState:
//		if v, ok := a.resources[msg.ResourcesID]; ok {
//			if v.container == nil {
//				ctx.Respond(fmt.Errorf("no container associated with %s", msg.ResourcesID))
//			} else {
//				ctx.Respond(*v.container)
//			}
//		} else {
//			ctx.Respond(fmt.Errorf("unknown resources %s", msg.ResourcesID))
//		}
// 	case sproto.ReleaseResources:
// 		a.Terminate(ctx, "allocation being preempted by the scheduler", msg.ForcePreemption)
// 	case sproto.ChangeRP:
// 		a.Terminate(ctx, "allocation resource pool changed", false)
// 	case sproto.ContainerLog:
// 		a.sendEvent(ctx, msg.ToEvent())

	// These messages allow users (and sometimes an orchestrator, such as HP search)
	// to interact with the allocation. The usually trace back to API calls.
	case AllocationReady:
		// AllocationReady only comes from the running container, so to
		// avoid a race condition with the slower transition to running state
		// which comes via polling for dispatcher RM, move the state to running now.
		a.setMostProgressedModelState(model.AllocationStateRunning)
		a.model.IsReady = ptrs.Ptr(true)
		if err := a.db.UpdateAllocationState(a.model); err != nil {
			a.Error(ctx, err)
		}
		a.sendEvent(ctx, sproto.Event{ServiceReadyEvent: ptrs.Ptr(true)})
	case MarkResourcesDaemon:
		if err := a.SetResourcesAsDaemon(ctx, msg.AllocationID, msg.ResourcesID); err != nil {
			a.Error(ctx, err)
		}
//	case AllocationSignal:
//		a.HandleSignal(ctx, AllocationSignalWithReason{AllocationSignal: msg})
//	case AllocationSignalWithReason:
//		a.HandleSignal(ctx, msg)
// 	case AllocationState:
// 		if ctx.ExpectingResponse() {
// 			ctx.Respond(a.State())
// 		}
	case SetAllocationProxyAddress:
		if a.req.ProxyPort == nil {
			if ctx.ExpectingResponse() {
				ctx.Respond(ErrBehaviorUnsupported{Behavior: fmt.Sprintf("%T", msg)})
			}
			return nil
		}
		a.proxyAddress = &msg.ProxyAddress
		a.registerProxies(ctx, a.containerProxyAddresses())
		a._init.proxy = true

	//////// rendezvous, traffic originates from container
	case WatchRendezvousInfo, UnwatchRendezvousInfo, rendezvousTimeout:
		if a.rendezvous == nil {
			if len(a.resources) == 0 {
				return ErrAllocationUnfulfilled{Action: fmt.Sprintf("%T", msg)}
			}

			switch a.resources.first().Summary().ResourcesType {
			case sproto.ResourcesTypeDockerContainer, sproto.ResourcesTypeK8sPod:
				break
			default:
				return ErrBehaviorUnsupported{Behavior: fmt.Sprintf("%T", msg)}
			}

			switch msg.(type) {
			case WatchRendezvousInfo:
				a.rendezvous = newRendezvous(ctx, a.model.AllocationID, a.resources)
			case UnwatchRendezvousInfo, rendezvousTimeout:
				// Ignore without active rendezvous.
				return nil
			}
		}

		switch msg := ctx.Message().(type) {
		case WatchRendezvousInfo:
			if w, err := a.rendezvous.watch(msg); err != nil {
				ctx.Respond(err)
			} else {
				ctx.Respond(w)
			}
		case UnwatchRendezvousInfo:
			a.rendezvous.unwatch(msg)
		case rendezvousTimeout:
			if err := a.rendezvous.checkTimeout(msg); err != nil {
				a.logger.Insert(ctx, a.enrichLog(model.TaskLog{Log: err.Error()}))
			}
		default:
			a.Error(ctx, actor.ErrUnexpectedMessage(ctx))
		}

	//////// generic allgather, traffic originates from container
	case WatchAllGather, UnwatchAllGather, allGatherTimeout:
		if a.allGather == nil {
			switch msg.(type) {
			case WatchAllGather:
				a.allGather = newAllGather(ctx)
			case UnwatchAllGather, allGatherTimeout:
				// Ignore without active all gather.
				return nil
			}
		}

		switch msg := ctx.Message().(type) {
		case WatchAllGather:
			watcher := a.allGather.watch(msg)
			ctx.Respond(watcher)
		case UnwatchAllGather:
			a.allGather.unwatch(msg)
		case allGatherTimeout:
			if err := a.allGather.checkTimeout(msg); err != nil {
				a.logger.Insert(ctx, a.enrichLog(model.TaskLog{Log: err.Error()}))
				ctx.Log().WithError(err).Error("performing all gather through master")
			}
		default:
			return actor.ErrUnexpectedMessage(ctx)
		}

		if a.allGather.done() {
			a.allGather = nil
		}

	//////// preemption watcher, traffic originates from container
	case WatchPreemption, UnwatchPreemption, PreemptionTimeout, AckPreemption:
		if !a.req.Preemptible {
			if ctx.ExpectingResponse() {
				ctx.Respond(ErrBehaviorDisabled{preemption})
			}
			return nil
		}
		if err := a.preemption.ReceiveMsg(ctx); err != nil {
			a.logger.Insert(ctx, a.enrichLog(model.TaskLog{Log: err.Error()}))
			a.Error(ctx, err)
		}
	case IdleTimeoutWatcherTick, IdleWatcherNoteActivity:
		if a.req.IdleTimeout == nil {
			if ctx.ExpectingResponse() {
				ctx.Respond(ErrBehaviorDisabled{idleWatcher})
			}
			return nil
		}
		if err := a.idleTimeoutWatcher.ReceiveMsg(ctx); err != nil {
			a.Error(ctx, err)
		}

	default:
		a.Error(ctx, actor.ErrUnexpectedMessage(ctx))
	}
	return nil
}

// RequestResources sets up the allocation.
func (a *Allocation) RequestResources(ctx *actor.Context) error {
	// ASK ASYNC
	// if err := ctx.Ask(a.rm, a.req).Error(); err != nil {
	// 	return errors.Wrap(err, "failed to request allocation")
	// }
	resp := ctx.Ask(a.rm, a.req)
	go func(){
		// Tell ourselves the answer, or Tell ourselves it's not coming.
		ans := resp.GetOrElse(resourcesNotAllocated)
		// XXX: this is abusing the ctx object a bit
		ctx.Tell(ctx.Self(), ans)
	}()
	return nil
}

// Cleanup ensures an allocation is properly closed. It tries to do everything before failing and
// ensures we don't leave any resources running.
func (a *Allocation) Cleanup(ctx *actor.Context) {
	// Just in-case code.
	if !a.exited {
		ctx.Log().Info("exit did not run properly")
		for _, r := range a.resources {
			if r.Exited == nil {
				ctx.Log().Infof("allocation exited with unterminated reservation: %v", r.Summary())
				r.Kill(ctx, a.logCtx)
			}
		}
		if a.resourcesStarted {
			a.markResourcesReleased(ctx)
		}

		if err := a.purgeRestorableResources(ctx); err != nil {
			ctx.Log().WithError(err).Error("failed to purge restorable resources")
		}

		a.sendEvent(ctx, sproto.Event{ExitedEvent: ptrs.Ptr("allocation did not exit correctly")})
		ctx.Tell(a.rm, sproto.ResourcesReleased{TaskActor: ctx.Self()})
	}
}

// ResourcesAllocated handles receiving resources from the resource manager. Note: it makes a single
// ask to the parent to build its task spec.. this is mostly a hack to defer lots of computationally
// heavy stuff unless it is necessarily (which also works to spread occurrences of the same work
// out). Eventually, Allocations should just be started with their TaskSpec.
func (a *Allocation) ResourcesAllocated(ctx *actor.Context) error {

	a.sendEvent(ctx, sproto.Event{ScheduledEvent: &a.model.AllocationID})
	if !a.req.Restore {
		// PREFER STRUCTURAL SOLUTION
		// if a.getModelState() != model.AllocationStatePending {
		// 	// If we have moved on from the pending state, these must be stale (and we must have
		// 	// already released them, just the scheduler hasn't gotten word yet).
		// 	return ErrStaleResourcesReceived{}
		// }

		a.setModelState(model.AllocationStateAssigned)
	} else {
		ctx.Log().Debugf("ResourcesAllocated restored state: %s", a.getModelState())
	}

	a.setMostProgressedModelState(model.AllocationStateAssigned)
	if err := a.resources.append(a._resources.Allocated.Resources); err != nil {
		return errors.Wrapf(err, "appending resources")
	}
}


func (a *Allocation) RequestTaskSpec() error {
	// ASYNC ASK
	// resp := ctx.Ask(ctx.Self().Parent(), BuildTaskSpec{})
	// switch ok, err := resp.ErrorOrTimeout(time.Hour); {
	// case err != nil:
	// 	return errors.Wrapf(err, "could not get task spec")
	// case !ok:
	// 	return errors.Wrapf(err, "timeout getting task spec, likely a deadlock")
	// }
	// a._taskSpec := resp.Get().(tasks.TaskSpec)
	resp := ctx.Ask()
	go func(){
		// Tell ourselves the answer, or Tell ourselves it's not coming.
		ans := resp.GetOrElse(taskSpecNotBuilt)
		// XXX: this is abusing the ctx object a bit
		ctx.Tell(ctx.Self(), ans)
	}()
}

func (a *Allocation) StartTask() error {
	if err := a.db.UpdateAllocationState(a.model); err != nil {
		return errors.Wrap(err, "updating allocation state")
	}

	now := time.Now().UTC()
	err := a.db.RecordTaskStats(&model.TaskStats{
		AllocationID: msg.ID,
		EventType:    "QUEUED",
		StartTime:    &msg.JobSubmissionTime,
		EndTime:      &now,
	})
	if err != nil {
		return errors.Wrap(err, "recording task queued stats")
	}

	if a.req.Preemptible {
		a.preemption = NewPreemption(a.model.AllocationID)
	}

	if cfg := a.req.IdleTimeout; cfg != nil {
		a.idleTimeoutWatcher = NewIdleTimeoutWatcher(a.req.Name, cfg)
		a.idleTimeoutWatcher.PreStart(ctx)
	}

	if !a.req.Restore {
		token, err := a.db.StartAllocationSession(a.model.AllocationID)
		if err != nil {
			return errors.Wrap(err, "starting a new allocation session")
		}

		// XXX: This looks like if it fails, only the just-in-case code cleans it up
		//      (advanceState should clean this up better I think)
		for cID, r := range a.resources {
			if err := r.Start(ctx, a.logCtx, a._taskSpec, sproto.ResourcesRuntimeInfo{
				Token:        token,
				AgentRank:    a.resources[cID].Rank,
				IsMultiAgent: len(a.resources) > 1,
			}); err != nil {
				return fmt.Errorf("starting resources (%v): %w", r, err)
			}
			a._init.containersStarted = append(a._init.containersStarted, cID)
		}
	} else if a.getModelState() == model.AllocationStateRunning {
		// Restore proxies.
		for _, r := range a.resources {
			if a.req.ProxyPort != nil && r.Started != nil && r.Started.Addresses != nil {
				a.registerProxies(ctx, r.Started.Addresses)
			}
		}
	}

	a.resourcesStarted = true
	a.sendEvent(ctx, sproto.Event{AssignedEvent: &msg})
	return nil
}

// SetResourcesAsDaemon sets the reservation as a daemon reservation. This means we won't wait for
// it to exit in errorless exits and instead will kill the forcibly.
func (a *Allocation) SetResourcesAsDaemon(
	ctx *actor.Context, aID model.AllocationID, rID sproto.ResourcesID,
) error {
	if aID != a.model.AllocationID {
		ctx.Respond(ErrStaleAllocation{aID, a.model.AllocationID})
		return nil
	} else if _, ok := a.resources[rID]; !ok {
		ctx.Respond(ErrStaleResources{ID: rID})
		return nil
	} else if len(a.resources) <= 1 {
		a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
			Log: `Ignoring request to daemonize resources within an allocation for an allocation
			with only one manageable set of resources, because this would just kill it. This is
			expected in when using Slurm.`,
			Level: ptrs.Ptr(model.LogLevelInfo),
		}))
		return nil
	}

	a.resources[rID].Daemon = true
	if err := a.resources[rID].Persist(); err != nil {
		return err
	}

	if len(a.resources.daemons()) == len(a.resources) {
		ctx.Log().Warnf("all resources were marked as daemon, exiting")
		a.close("all resources were marked as daemon"):w
		a._terminate.wantKill = true
	}

	return nil
}

// HandleSignal handles an external signal to kill or terminate the allocation.
func (a *Allocation) HandleSignal(ctx *actor.Context, msg AllocationSignalWithReason) {
	switch msg.AllocationSignal {
	case Kill:
		a.Kill(ctx, msg.InformationalReason)
	case Terminate:
		a.Terminate(ctx, msg.InformationalReason, false)
	}
}

// ResourcesStateChanged handles changes in container states. It can move us to ready,
// kill us or close us normally depending on the changes, among other things.
func (a *Allocation) ResourcesStateChanged(
	ctx *actor.Context, msg sproto.ResourcesStateChanged,
) {
	if _, ok := a.resources[msg.ResourcesID]; !ok {
		ctx.Log().
			WithField("container", msg.Container).
			WithError(ErrStaleResources{ID: msg.ResourcesID}).Warnf("old state change")
		return
	}

	a.resources[msg.ResourcesID].container = msg.Container
	ctx.Log().Debugf("resources %v are %s [rank=%d, container=%+v]",
		msg.ResourcesID, msg.ResourcesState, a.resources[msg.ResourcesID].Rank, msg.Container,
	)
	switch msg.ResourcesState {
	case sproto.Pulling:
		a.setMostProgressedModelState(model.AllocationStatePulling)
		a.model.StartTime = ptrs.Ptr(time.Now().UTC().Truncate(time.Millisecond))
		if err := a.db.UpdateAllocationStartTime(a.model); err != nil {
			ctx.Log().
				WithError(err).
				Errorf("allocation will not be properly accounted for")
		}
	case sproto.Starting:
		a.setMostProgressedModelState(model.AllocationStateStarting)
	case sproto.Running:
		if a.resources[msg.ResourcesID].Started != nil {
			// Only recognize the first start message for each resource, since the slurm resource
			// manager is polling based instead and sends us a message that the resources are
			// running each time it polls.
			return
		}

		a.setMostProgressedModelState(model.AllocationStateRunning)

		a.resources[msg.ResourcesID].Started = msg.ResourcesStarted
		if err := a.resources[msg.ResourcesID].Persist(); err != nil {
			a.Error(ctx, err)
			return
		}

		if a.rendezvous != nil && a.rendezvous.try() {
			ctx.Log().Info("all containers are connected successfully (task container state changed)")
		}
		if a.req.ProxyPort != nil && msg.ResourcesStarted.Addresses != nil {
			a.registerProxies(ctx, msg.ResourcesStarted.Addresses)
		}

		a.sendEvent(ctx, sproto.Event{
			ContainerID:           coalesceString(msg.ContainerIDStr(), ""),
			ResourcesStartedEvent: msg.ResourcesStarted,
		})

		prom.AssociateAllocationTask(a.req.AllocationID,
			a.req.TaskID,
			a.req.TaskActor.Address(),
			a.req.JobID)
		prom.AddAllocationResources(a.resources[msg.ResourcesID].Summary(), msg.ResourcesStarted)

	case sproto.Terminated:
		if a.resources[msg.ResourcesID].Exited != nil {
			// If we have already received the exit for this container, we only recognize the first.
			// If there are multiples, it's likely due to one being resent after a kill signal was
			// repeated. Agents always re-ack termination to ensure it is received in the event
			// of network failures and they always re-ack the same exit, anyway.
			return
		}

		a.setMostProgressedModelState(model.AllocationStateTerminating)

		a.resources[msg.ResourcesID].Exited = msg.ResourcesStopped

		ctx.Tell(a.rm, sproto.ResourcesReleased{
			TaskActor:   ctx.Self(),
			ResourcesID: &msg.ResourcesID,
		})

		if err := a.resources[msg.ResourcesID].Persist(); err != nil {
			a.Error(ctx, err)
			return
		}

		switch {
		case a.killedWhileRunning:
			a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
				ContainerID: msg.ContainerIDStr(),
				Log: fmt.Sprintf(
					"resources were killed: %s",
					msg.ResourcesStopped.String(),
				),
			}))
			a.Exit(ctx, "resources were killed")
		case msg.ResourcesStopped.Failure != nil:
			a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
				ContainerID: msg.ContainerIDStr(),
				Log:         msg.ResourcesStopped.String(),
				Level:       ptrs.Ptr(model.LogLevelError),
			}))
			a.Error(ctx, *msg.ResourcesStopped.Failure)
		default:
			a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
				ContainerID: msg.ContainerIDStr(),
				Log:         msg.ResourcesStopped.String(),
				Level:       ptrs.Ptr(model.LogLevelInfo),
			}))
			a.Exit(ctx, msg.ResourcesStopped.String())
		}

		for cID := range a.resources {
			prom.DisassociateAllocationTask(a.req.AllocationID,
				a.req.TaskID,
				a.req.TaskActor.Address(),
				a.req.JobID)
			prom.RemoveAllocationResources(a.resources[cID].Summary())
		}
	}

	if err := a.db.UpdateAllocationState(a.model); err != nil {
		ctx.Log().Error(err)
	}
}

// RestoreResourceFailure handles the restored resource failures.
func (a *Allocation) RestoreResourceFailure(
	ctx *actor.Context, msg sproto.ResourcesFailure) {
	ctx.Log().Debugf("allocation resource failure")
	a.setMostProgressedModelState(model.AllocationStateTerminating)

	if err := a.db.UpdateAllocationState(a.model); err != nil {
		ctx.Log().Error(err)
	}

	if a.req.Restore {
		a.model.EndTime = cluster.TheLastBootClusterHeartbeat()
	} else {
		a.model.EndTime = ptrs.Ptr(time.Now().UTC())
	}

	if err := a.db.CompleteAllocation(&a.model); err != nil {
		ctx.Log().WithError(err).Error("failed to mark allocation completed")
	}

	a.Error(ctx, msg)
}

// Exit attempts to exit an allocation while not killing or preempting it.
func (a *Allocation) Exit(ctx *actor.Context, reason string) (exited bool) {
	switch {
	case !a.resourcesStarted:
		a.terminated(ctx, reason)
		return true
	case len(a.resources.exited()) == len(a.resources):
		a.terminated(ctx, reason)
		return true
	case a.allNonDaemonsExited():
		a.killedDaemons = true
		a.kill(ctx, reason)
	case len(a.resources.failed()) > 0:
		a.kill(ctx, reason)
	}
	return false
}

// Terminate attempts to close an allocation by gracefully stopping it (though a kill are possible).
func (a *Allocation) Terminate(ctx *actor.Context, reason string, forcePreemption bool) {
	if exited := a.Exit(ctx, reason); exited {
		return
	}
	switch {
	case a.req.Preemptible && (a.rendezvous != nil && a.rendezvous.ready()) || forcePreemption:
		a.preempt(ctx, reason)
	default:
		a.kill(ctx, reason)
	}
}

// Kill attempts to close an allocation by killing it.
func (a *Allocation) Kill(ctx *actor.Context, reason string) {
	if exited := a.Exit(ctx, reason); exited {
		return
	}
	a.kill(ctx, reason)
}

// Error closes the allocation due to an error, beginning the kill flow.
func (a *Allocation) Error(ctx *actor.Context, err error) {
	ctx.Log().WithError(err).Errorf("allocation encountered fatal error")
	if a.exitErr == nil {
		a.exitErr = err
	}
	a.Kill(ctx, err.Error())
}

func (a *Allocation) allNonDaemonsExited() bool {
	for id := range a.resources {
		_, terminated := a.resources.exited()[id]
		_, daemon := a.resources.daemons()[id]
		if !(terminated || daemon) {
			return false
		}
	}
	return true
}

func (a *Allocation) preempt(ctx *actor.Context, reason string) {
	ctx.Log().WithField("reason", reason).Info("decided to gracefully terminate allocation")
	a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
		Level: ptrs.Ptr(model.LogLevelInfo),
		Log: fmt.Sprintf(
			"gracefully terminating allocation's remaining resources (reason: %s)",
			reason,
		),
	}))

	a.preemption.Preempt()
	actors.NotifyAfter(ctx, preemptionTimeoutDuration, PreemptionTimeout{a.model.AllocationID})
}

func (a *Allocation) kill(ctx *actor.Context, reason string) {
	if a.killCooldown != nil && time.Now().UTC().Before(*a.killCooldown) {
		ctx.Log().Debug("still inside of kill cooldown")
		return
	}

	ctx.Log().WithField("reason", reason).Info("decided to kill allocation")
	a.logger.Insert(ctx, a.enrichLog(model.TaskLog{
		Level: ptrs.Ptr(model.LogLevelInfo),
		Log: fmt.Sprintf(
			"forcibly killing allocation's remaining resources (reason: %s)",
			reason,
		),
	}))

	for _, r := range a.resources.active() {
		r.Kill(ctx, a.logCtx)
	}

	if len(a.resources.exited()) == 0 {
		a.killedWhileRunning = true
	}

	// Once a job has been killed, resend the kill every 30s, in the event it is lost (has
	// happened before due to network failures).
	a.killCooldown = ptrs.Ptr(time.Now().UTC().Add(killCooldown))
	actors.NotifyAfter(ctx, killCooldown, AllocationSignalWithReason{
		AllocationSignal:    Kill,
		InformationalReason: "killing again after 30s without all container exits",
	})
}

func (a *Allocation) registerProxies(ctx *actor.Context, addresses []cproto.Address) {
	cfg := a.req.ProxyPort
	if cfg == nil {
		return
	}
	if len(a.resources) > 1 {
		// We don't support proxying multi-reservation allocations.
		ctx.Log().Warnf("proxy for multi-reservation allocation aborted")
		return
	}

	for _, address := range addresses {
		// Only proxy the port we expect to proxy. If a dockerfile uses an EXPOSE command,
		// additional addresses will appear here, but currently we only proxy one uuid to one
		// port, so it doesn't make sense to send multiple proxy.Register messages for a
		// single ServiceID (only the last one would work).
		if address.ContainerPort != cfg.Port {
			continue
		}

		// We are keying on allocation id instead of container id. Revisit this when we need to
		// proxy multi-container tasks or when containers are created prior to being
		// assigned to an agent.
		ctx.Ask(ctx.Self().System().Get(actor.Addr("proxy")), proxy.Register{
			ServiceID: cfg.ServiceID,
			URL: &url.URL{
				Scheme: "http",
				Host:   fmt.Sprintf("%s:%d", address.HostIP, address.HostPort),
			},
			ProxyTCP:        cfg.ProxyTCP,
			Unauthenticated: cfg.Unauthenticated,
		})
		a.proxies = append(a.proxies, cfg.ServiceID)
	}

	if len(a.proxies) != 1 {
		ctx.Log().Errorf("did not proxy as expected %v (found addrs %v)", len(a.proxies), addresses)
	}
}

func (a *Allocation) unregisterProxies(ctx *actor.Context) {
	cfg := a.req.ProxyPort
	if cfg == nil {
		return
	}

	if len(a.resources) > 1 {
		// Can't proxy more than one reservation, so we never would've made them.
		return
	}

	for _, serviceID := range a.proxies {
		ctx.Tell(ctx.Self().System().Get(actor.Addr("proxy")), proxy.Unregister{
			ServiceID: serviceID,
		})
	}
}

// containerProxyAddresses forms the container address when proxyAddress is given.
func (a *Allocation) containerProxyAddresses() []cproto.Address {
	if a.proxyAddress == nil || a.req.ProxyPort == nil {
		return []cproto.Address{}
	}
	return []cproto.Address{
		{
			ContainerIP:   *a.proxyAddress,
			ContainerPort: a.req.ProxyPort.Port,
			HostIP:        *a.proxyAddress,
			HostPort:      a.req.ProxyPort.Port,
		},
	}
}

func (a *Allocation) terminated(ctx *actor.Context, reason string) {
	a.setMostProgressedModelState(model.AllocationStateTerminated)
	exit := &AllocationExited{FinalState: a.State()}
	if a.exited {
		// Never exit twice. If this were allowed, a trial could receive two task.AllocationExited
		// messages. On receipt of the first message, the trial awaits our exit. Once we exit, it
		// reschedules a new allocation, receives the second message and erroneously awaits the new
		// allocation's stop. Once the new allocation asks the trial to build its task spec, they
		// deadlock.
		// This occurred when an allocation completed and was preempted in quick succession.
		return
	}
	a.exited = true
	exitReason := fmt.Sprintf("allocation terminated after %s", reason)
	defer ctx.Tell(ctx.Self().Parent(), exit)
	defer ctx.Tell(a.rm, sproto.ResourcesReleased{TaskActor: ctx.Self()})
	defer a.unregisterProxies(ctx)
	defer ctx.Self().Stop()
	defer a.sendEvent(ctx, sproto.Event{ExitedEvent: &exitReason})
	if err := a.purgeRestorableResources(ctx); err != nil {
		ctx.Log().WithError(err).Error("failed to purge restorable resources")
	}

	if len(a.resources) == 0 {
		return
	}
	defer a.markResourcesReleased(ctx)

	if a.req.Preemptible {
		defer a.preemption.Close()
	}
	if a.rendezvous != nil {
		defer a.rendezvous.close()
	}
	switch {
	case a.killedWhileRunning:
		exitReason = fmt.Sprintf("allocation stopped after %s", reason)
		ctx.Log().Info(exitReason)
		return
	case a.req.Preemptible && a.preemption.Acknowledged():
		exitReason = fmt.Sprintf("allocation stopped after %s", reason)
		ctx.Log().Info(exitReason)
		return
	case a.exitErr == nil && len(a.resources.exited()) > 0:
		// This is true because searcher and preemption exits both ack preemption.
		exit.UserRequestedStop = true
		exitReason = fmt.Sprintf("allocation stopped early after %s", reason)
		ctx.Log().Info(exitReason)
		return
	case a.exitErr != nil:
		switch err := a.exitErr.(type) {
		case sproto.ResourcesFailure:
			switch err.FailureType {
			case sproto.ResourcesFailed, sproto.TaskError:
				exitReason = fmt.Sprintf("allocation failed: %s", err)
				ctx.Log().Info(exitReason)
				exit.Err = err
				return
			case sproto.AgentError, sproto.AgentFailed:
				exitReason = fmt.Sprintf("allocation failed due to agent failure: %s", err)
				ctx.Log().Warn(exitReason)
				exit.Err = err
				return
			case sproto.TaskAborted, sproto.ResourcesAborted:
				exitReason = fmt.Sprintf("allocation aborted: %s", err.FailureType)
				ctx.Log().Debug(exitReason)
				exit.Err = err
				return
			default:
				panic(fmt.Errorf("unexpected allocation failure: %w", err))
			}
		default:
			exitReason = fmt.Sprintf("allocation handler crashed due to error: %s", err)
			ctx.Log().Error(exitReason)
			exit.Err = err
			return
		}
	default:
		// If we ever exit without a reason and we have no exited resources, something has gone
		// wrong.
		panic("allocation exited early without a valid reason")
	}
}

// markResourcesReleased persists completion information.
func (a *Allocation) markResourcesReleased(ctx *actor.Context) {
	a.model.EndTime = ptrs.Ptr(time.Now().UTC())
	if err := a.db.DeleteAllocationSession(a.model.AllocationID); err != nil {
		ctx.Log().WithError(err).Error("error deleting allocation session")
	}
	if err := a.db.CompleteAllocation(&a.model); err != nil {
		ctx.Log().WithError(err).Error("failed to mark allocation completed")
	}

	telemetry.ReportAllocationTerminal(
		ctx.Self().System(), a.db, a.model, a.resources.firstDevice())
}

func (a *Allocation) purgeRestorableResources(ctx *actor.Context) error {
	_, err := db.Bun().NewDelete().Model((*ResourcesWithState)(nil)).
		Where("allocation_id = ?", a.model.AllocationID).
		Exec(context.TODO())

	return err
}

const killedLogSubstr = "exit code 137"

func (a *Allocation) enrichLog(log model.TaskLog) model.TaskLog {
	log.TaskID = string(a.req.TaskID)

	if log.Timestamp == nil || log.Timestamp.IsZero() {
		log.Timestamp = ptrs.Ptr(time.Now().UTC())
	}

	if a.killedDaemons && strings.Contains(log.Log, killedLogSubstr) {
		log.Level = ptrs.Ptr(model.LogLevelDebug)
	} else if log.Level == nil {
		log.Level = ptrs.Ptr(model.LogLevelInfo)
	}

	if log.Source == nil {
		log.Source = ptrs.Ptr("master")
	}

	if log.StdType == nil {
		log.StdType = ptrs.Ptr("stdout")
	}

	log.Log += "\n"
	return log
}

func (a *Allocation) sendEvent(ctx *actor.Context, ev sproto.Event) {
	ev = a.enrichEvent(ctx, ev)
	a.logger.Insert(ctx, a.enrichLog(ev.ToTaskLog()))
	if a.req.StreamEvents != nil {
		ctx.Tell(a.req.StreamEvents.To, ev)
	}
}

func (a *Allocation) enrichEvent(ctx *actor.Context, ev sproto.Event) sproto.Event {
	ev.ParentID = ctx.Self().Parent().Address().Local()
	ev.Description = a.req.Name
	ev.IsReady = coalesceBool(a.model.IsReady, false)
	if ev.State == "" {
		ev.State = string(a.getModelState())
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	return ev
}

// State returns a deepcopy of our state.
func (a *Allocation) State() AllocationState {
	addresses := map[sproto.ResourcesID][]cproto.Address{}
	containers := map[sproto.ResourcesID][]cproto.Container{}
	resources := map[sproto.ResourcesID]sproto.ResourcesSummary{}
	for id, r := range a.resources {
		resources[id] = r.Summary()

		switch {
		case r.Started != nil && r.Started.Addresses != nil:
			a := r.Started.Addresses
			na := make([]cproto.Address, len(a))
			copy(na, a)
			addresses[id] = na
		case a.proxyAddress != nil:
			addresses[id] = a.containerProxyAddresses()
		}

		if r.container != nil {
			containers[id] = append(containers[id], *r.container)
		}
	}

	return AllocationState{
		State:      a.getModelState(),
		Resources:  resources,
		Addresses:  addresses,
		Containers: containers,
		Ready: a.rendezvous != nil && a.rendezvous.ready() ||
			coalesceBool(a.model.IsReady, false),
	}
}

func (a *Allocation) setModelState(v model.AllocationState) {
	a.model.State = &v
}

func (a *Allocation) setMostProgressedModelState(v model.AllocationState) {
	a.setModelState(model.MostProgressedAllocationState(a.getModelState(), v))
}

func (a *Allocation) getModelState() model.AllocationState {
	if a.model.State == nil {
		return model.AllocationStatePending
	}
	return *a.model.State
}

func (a *AllocationExited) String() string {
	switch {
	case a == nil:
		return missingExitMessage
	case a.Err != nil:
		return a.Err.Error()
	default:
		return okExitMessage
	}
}

// FirstContainer returns the first container in the allocation state.
func (a AllocationState) FirstContainer() *cproto.Container {
	for _, cs := range a.Containers {
		for _, c := range cs {
			return &c
		}
	}
	return nil
}

// FirstContainerAddresses returns the first container's addresses in the allocation state.
func (a AllocationState) FirstContainerAddresses() []cproto.Address {
	for _, ca := range a.Addresses {
		return ca
	}
	return nil
}

func coalesceBool(x *bool, fallback bool) bool {
	if x == nil {
		return fallback
	}
	return *x
}

func coalesceString(x *string, fallback string) string {
	if x == nil {
		return fallback
	}
	return *x
}

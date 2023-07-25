package internal

import (
	"context"
	"database/sql"
	"time"
	"fmt"
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/pkg/model"
	"github.com/determined-ai/determined/master/pkg/stream"
)

type JsonB []byte

// TrialMsg is a stream.Event.
type TrialMsg struct {
	bun.BaseModel `bun:"table:trials"`

	// immutable attributes
	ID int                      `bun:"id,pk"`
	TaskID model.TaskID         `bun:"task_id"`
	ExperimentID int            `bun:"experiment_id"`
	RequestID model.RequestID   `bun:"request_id"`
	Seed int64                  `bun:"seed"`
	HParams JsonB               `bun:"hparams"`

	// warmstart checkpoint id?

	// mutable attributes
	State model.State           `bun:"state"`
	StartTime time.Time         `bun:"start_time"`
	EndTime *time.Time          `bun:"end_time"`
	RunnerState string          `bun:"runner_state"`
	Restarts int                `bun:"restarts"`
	Tags JsonB                  `bun:"tags"`

	// metadata
	Seq int64                   `bun:"seq"`

	// total batches?

	cache *websocket.PreparedMessage
}

func (tm *TrialMsg) SeqNum() int64 {
	return tm.Seq
}

func (tm *TrialMsg) PreparedMessage() *websocket.PreparedMessage {
	if tm.cache != nil {
		return tm.cache
	}
	jbytes, err := json.Marshal(tm)
	if err != nil {
		log.Errorf("error marshaling message for streaming: %v", err.Error())
		return nil
	}
	tm.cache, err = websocket.NewPreparedMessage(websocket.BinaryMessage, jbytes)
	if err != nil {
		log.Errorf("error preparing message for streaming: %v", err.Error())
		return nil
	}
	return tm.cache
}

// scan for updates to the trials table
func newTrialMsgs(since int64, ctx context.Context) (int64, []*TrialMsg, error) {
	var trialMsgs []*TrialMsg
	err := db.Bun().NewSelect().Model(&trialMsgs).Where("seq > ?", since).Scan(ctx)
	if err != nil && errors.Cause(err) != sql.ErrNoRows {
		fmt.Printf("error: %v\n", err)
		return since, nil, err
	}


	newSince := since
	for _, tm := range trialMsgs {
		if tm.Seq > newSince {
			newSince = tm.Seq
		}
	}

	return newSince, trialMsgs, nil
}

// TrialSubscriptionSpec are what a user submits to define their trial subscriptions.
type TrialSubscriptionSpec struct {
	TrialIds      []int  `json:"trial_ids"`
	ExperimentIds []int  `json:"experiment_ids"`
}

// When a user submits a new TrialSubscriptionSpec, we scrape the database for initial matches.
func (tss *TrialSubscriptionSpec) InitialScan(ctx context.Context) (
	[]*websocket.PreparedMessage, error,
) {
	if len(tss.TrialIds) == 0 && len(tss.ExperimentIds) == 0 {
		return nil, nil
	}
	var trialMsgs []*TrialMsg
	q := db.Bun().NewSelect().Model(&trialMsgs)
	where := q.Where
	if len(tss.TrialIds) > 0 {
		q = where("id in (?)", bun.In(tss.TrialIds))
		where = q.WhereOr
	}
	if len(tss.ExperimentIds) > 0 {
		q = where("experiment_id in (?)", bun.In(tss.ExperimentIds))
		where = q.WhereOr
	}
	err := q.Scan(ctx)
	if err != nil && errors.Cause(err) != sql.ErrNoRows {
		fmt.Printf("error: %v\n", err)
		return nil, err
	}

	var out []*websocket.PreparedMessage
	for _, msg := range trialMsgs {
		out = append(out, msg.PreparedMessage())
	}
	return out, nil
}

// TrialSubscription implements logic for
type TrialSubscription struct {
	TrialIds      map[int]bool
	ExperimentIds map[int]bool
}

func (ts *TrialSubscription) Init() {
	if ts.TrialIds != nil {
		return
	}
	ts.TrialIds = make(map[int]bool)
	ts.ExperimentIds = make(map[int]bool)
}

func (ts *TrialSubscription) AddSpec(spec TrialSubscriptionSpec) {
	ts.Init()
	for _, id := range spec.TrialIds {
		ts.TrialIds[id] = true
	}
	for _, id := range spec.ExperimentIds {
		ts.ExperimentIds[id] = true
	}
}

func (ts *TrialSubscription) DropSpec(spec TrialSubscriptionSpec) {
	ts.Init()
	for _, id := range spec.TrialIds {
		delete(ts.TrialIds, id)
	}
	for _, id := range spec.ExperimentIds {
		delete(ts.ExperimentIds, id)
	}
}

func (ts *TrialSubscription) MakePredicate() func(*TrialMsg) bool {
	ts.Init()
	// make a copy of the maps, because the predicate must run safely off-thread
	trialIds := make(map[int]bool)
	experimentIds := make(map[int]bool)
	for id, _ := range ts.TrialIds {
		trialIds[id] = true
	}
	for id, _ := range ts.ExperimentIds {
		experimentIds[id] = true
	}

	// return a closure around our copied maps
	return func(msg *TrialMsg) bool {
		if _, ok := trialIds[msg.ID]; ok {
			return true
		}
		if _, ok := experimentIds[msg.ExperimentID]; ok {
			return true
		}
		return false
	}
}

// SubscriptionSetSpec is what users submit to modify their subscriptions.
type SubscriptionSetSpec struct {
	Trials *TrialSubscriptionSpec `json:"trials"`
}
// {"add": {"trials": {"trial_ids": [...], "experiment_ids": [...]}}}

// SpecMods is the message that a user writes to a websocket to modify their subscriptions
type SpecMods struct {
	Add SubscriptionSetSpec `json:"add"`
	Drop SubscriptionSetSpec `json:"drop"`
}

// SubscriptionSet allows for subscriptions to each type in the PubSubSystem.
type SubscriptionSet struct {
	Trials TrialSubscription
}

// Apply is a helper function that applies a whole set of subscription changes and returns a whole
// list of new initial scans as a result.
func (ss *SubscriptionSet) Apply(mods SpecMods, ctx context.Context) (
	[]*websocket.PreparedMessage, error,
) {
	var out []*websocket.PreparedMessage

	// trials
	if mods.Add.Trials != nil || mods.Drop.Trials != nil {
		// Modify Subscription before initial scan, to avoid dropping messages.
		if mods.Add.Trials != nil {
			ss.Trials.AddSpec(*mods.Add.Trials)
		}
		if mods.Drop.Trials != nil {
			ss.Trials.DropSpec(*mods.Drop.Trials)
		}
		// Only call SetPredicate() once per Subscription type, since it is a sync point between the
		// websocket thread and the publisher thread.
		// XXX ss.Trials.SetPredicate(ss.Trials.MakePredicate())
		// Now do initial scan.
		if mods.Add.Trials != nil {
			msgs, err := mods.Add.Trials.InitialScan(ctx)
			if err != nil {
				return nil, err
			}
			out = append(out, msgs...)
		}
	}

	// ... other types here ...

	return out, nil
}

// PubSubSystem contains all publishers, and handles all websockets.  It will connect each websocket
// with the appropriate set of publishers, based on that websocket's subscriptions.
type PubSubSystem struct {
	TrialPublisher *stream.Publisher[*TrialMsg]
}

func NewPubSubSystem() PubSubSystem {
	return PubSubSystem {
		TrialPublisher: stream.NewPublisher[*TrialMsg](),
	}
}

func (pss PubSubSystem) Start(ctx context.Context) {
	// start each publisher
	go publishTrials(ctx, pss.TrialPublisher)
}

// Websocket is an Echo websocket endpoint.
func (pss PubSubSystem) Websocket(socket *websocket.Conn, c echo.Context) error {
	streamer := stream.NewStreamer()
	ss := SubscriptionSet{}

	// detect context cancelation, and bring it into the websocket thread
	go func() {
		<-c.Request().Context().Done()
		streamer.Close()
	}()

	// always be reading for new subscriptions
	go func() {
		// TODO: close streamer if reader goroutine dies?
		for {
			var mods SpecMods
			err := socket.ReadJSON(&mods)
			if err != nil {
				if websocket.IsUnexpectedCloseError(
					err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure,
				) {
					log.Errorf("unexpected close error: %v", err)
				}
				break
			}
			// wake up streamer goroutine with the newly-read SpecMods
			func() {
				streamer.Cond.L.Lock()
				defer streamer.Cond.L.Unlock()
				streamer.Cond.Signal()
				streamer.SubscriptionSpecs = append(streamer.SubscriptionSpecs, mods)
			}()
		}
	}()

	user := 1

	// stream TrialMsg events
	trialPredicate := func(ev *TrialMsg) bool {
		return true
	}
	sub := stream.NewSubscription[*TrialMsg](streamer, trialPredicate)
	pss.TrialPublisher.AddSubscription(sub, user)
	defer pss.TrialPublisher.RemoveSubscription(sub, user)

	// stream events until the cows come home

	for {
		subs, events, closed := streamer.WaitForSomething()
		// were we closed?
		if closed {
			return nil
		}
		if len(subs) > 0 {
			for _, sub := range subs {
				mods := sub.(SpecMods)
				msgs, err := ss.Apply(mods, c.Request().Context())
				if err != nil {
					return errors.Wrapf(err, "error modifying subscriptions")
				}
				events = append(events, msgs...)
			}
		}
		// write events to the websocket
		for _, ev := range events {
			err := socket.WritePreparedMessage(ev)
			// TODO: don't log broken pipe errors.
			if err != nil {
				return errors.Wrapf(err, "error writing to socket")
			}
		}
	}

	return nil
}

func publishLoop[T stream.Event](
	ctx context.Context,
	channelName string,
	rescanFn func(int64, context.Context) (int64, []T, error),
	publisher *stream.Publisher[T],
) error {
	minReconn := 1 * time.Second
	maxReconn := 10 * time.Second

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Printf("reportProblem: %v\n", err.Error())
		}
	}

	listener := pq.NewListener(
		"postgresql://postgres:postgres@localhost/determined?sslmode=disable",
		minReconn,
		maxReconn,
		reportProblem,
	)

	// start listening
	err := listener.Listen(channelName)
	if err != nil {
		return errors.Wrapf(err, "failed to listen: %v", channelName)
	}

	// scan for initial since
	// TODO: actually just ask for the maximum seq directly.
	since, _, err := rescanFn(0, ctx)
	if err != nil {
		return errors.Wrap(err, "failed initial scan")
	}

	for {
		select {
		// Are we canceled?
		case <-ctx.Done():
			fmt.Printf("publishTrials canceled\n")
			return nil

		// Is there work to do?
		case <-listener.Notify:
			break

		// The pq listener example includes a timeout case, so we do too.
		// (https://pkg.go.dev/github.com/lib/pq/example/listen)
		case <-time.After(30 * time.Second):
			go listener.Ping()
		}

		var evs []T
		since, evs, err = rescanFn(since, ctx)
		if err != nil {
			return errors.Wrap(err, "failed wakeup scan")
		}
		// noop?
		if len(evs) == 0 {
			continue
		}
		// generate updates
		var updates []stream.Update[T]
		for _, ev := range evs {
			update := stream.Update[T]{
				Event: ev,
				// TODO: get valid uids from database instead.
				Users: []int{1, 2},
			}
			updates = append(updates, update)
		}
		stream.Broadcast(publisher, updates)
	}

	return nil
}

// publishTrials streams updates to trial publishers
func publishTrials(ctx context.Context, publisher *stream.Publisher[*TrialMsg]) error {
	return publishLoop(
		ctx,
		"stream_trial_chan",
		newTrialMsgs,
		publisher,
	)
}

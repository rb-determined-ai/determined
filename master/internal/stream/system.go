package stream

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
	go publishLoop(ctx, "stream_trial_chan", newTrialMsgs, pss.TrialPublisher)
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
			// convert opqaue []interface{} to usable []SpecMod
			var mods []SpecMods
			for _, sub := range subs {
				mods = append(mods, sub.(SpecMods))
			}
			msgs, err := ss.Apply(mods, c.Request().Context())
			if err != nil {
				return errors.Wrapf(err, "error modifying subscriptions")
			}
			events = append(events, msgs...)
			// TODO: also append a sync message (or one sync per SpecMods)
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

// SubscriptionSetSpec is what users submit to modify their subscriptions.
type SubscriptionSetSpec struct {
	Trials TrialSubscriptionSpec `json:"trials"`
}

// SpecMods is the message that a user writes to a websocket to modify their subscriptions
type SpecMods struct {
	Add SubscriptionSetSpec `json:"add"`
	Drop SubscriptionSetSpec `json:"drop"`
}

// SubscriptionSet allows for subscriptions to each type in the PubSubSystem.
type SubscriptionSet struct {
	Trials TrialSubscription
}

// SubscriptionSpec is what a user specifies through the REST API.
type SubscriptionSpec[T stream.Event] interface {
	InitialScan(ctx context.Context) ([]*websocket.PreparedMessage, error)
}

// Subscription is the internal representation of what a user has subscribed to.
type Subscription[T stream.Event] interface {
	AddSpec(spec SubscriptionSpec[T])
	DropSpec(spec SubscriptionSpec[T])
	StreamSubscription() stream.Subscription[T]
	MakePredicate() func([]T) bool
}

// applyOne is a helper function to SubscriptionSet.Apply()
func applyOne[T stream.Event](
	sub *Subscription[T], add *SubscriptionSpec[T], drop *SubscriptionSpec[T],
) bool {
	dirty := false

	// Modify Subscription before initial scan, to avoid dropping messages.
	if add != nil {
		sub.AddSpec(*add)
		dirty = true
	}
	if mods.Drop.Trials != nil {
		sub.DropSpec(*drop)
		dirty = true
	}

	return dirty
}

// appendInitialScans is a helper function to SubscriptionSet.Apply()
func appendInitialScans[T stream.Event](
	msgs []*websocket.PreparedMessage, err error, add *SubscriptionSpec[T], ctx context.Context,
) ([]*websocket.PreparedMessage, error) {
	if err != nil || add == nil {
		return nil, err
	}
	var newMsgs []*websocket.PreparedMessage
	newMsgs, err = add.InitialScan(ctx)
	if err != nil {
		return nil, err
	}
	msgs = append(msgs, newMsgs)
	return msgs, err
}

// updateOneSub is a helper function to SubscriptionSet.Apply()
func updateOneSub[T stream.Event](
	sub *Subscription[T], bool dirty,
) {
	if dirty {
		sub.StreamSubscription().SetPredicate(sub.MakePredicate())
	}
}

// Apply takes a list of received SpecMods from the websocket, updates the stream.Subscriptions we
// have, and returns the result of initial scans for newly-added subscriptions.
func (ss *SubscriptionSet) Apply(subs []interface{}, ctx context.Context) (
	[]*websocket.PreparedMessage, error,
) {
	trialsDirty := false
	// expsDirty := false
	// make all changes to subscriptions first
	for _, sub := range subs {
		sm := sub.(SpecMods)
		trialsDirty |= applyOne(ss.Trials, sm.Add.Trials, sm.Drop.Trials)
		// expsDirty |= applyOne(ss.Experiments, sm.Add.Experiments, sm.Drop.Experiments)
	}

	// do initial scans
	var msgs []*websocket.PreparedMessage
	var error err
	for _, sm := range subs {
		sm := sub.(SpecMods)
		msgs, err = appendInitialScan(msgs, err, sm.Add.Trials, ctx)
		// msgs, err = appendInitialScan(msgs, err, sm.Add.Experiments, ctx)
	}
	if err != nil {
		return nil, err
	}

	// update subscriptions
	updateOneSub(ss.Trials, trialsDirty)
	// updateOneSub(ss.Experiment, expsDirty)

	return msgs, err
}

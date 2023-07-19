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
}

func (tm *TrialMsg) SeqNum() int64 {
	return tm.Seq
}

func (tm *TrialMsg) PreparedMessage() *websocket.PreparedMessage {
	jbytes, err := json.Marshal(tm)
	if err != nil {
		log.Errorf("error marshaling message for streaming: %v", err.Error())
		return nil
	}
	msg, err := websocket.NewPreparedMessage(websocket.BinaryMessage, jbytes)
	if err != nil {
		log.Errorf("error preparing message for streaming: %v", err.Error())
		return nil
	}
	return msg
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

	// detect context cancelation, and bring it into the websocket thread
	go func() {
		<-c.Request().Context().Done()
		streamer.Close()
	}()

	user := 1

	// stream TrialMsg events
	trialPredicate := func(ev *TrialMsg) bool {
		return true
	}
	sub := stream.NewSubscription[*TrialMsg](streamer, trialPredicate)
	pss.TrialPublisher.AddSubscription(sub, user)
	defer pss.TrialPublisher.RemoveSubscription(sub, user)

	// TODO: process initial events

	// stream events until the cows come home

	for {
		events, closed := streamer.WaitForSomething()
		// were we closed?
		if closed {
			return nil
		}
		// write events to the websocket
		for _, ev := range events {
			err := socket.WritePreparedMessage(ev)
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

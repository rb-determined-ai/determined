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

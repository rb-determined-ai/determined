package stream

import (
	"context"
	"database/sql"
	"time"
	"fmt"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"

	"github.com/determined-ai/determined/master/internal/db"
	"github.com/determined-ai/determined/master/pkg/model"
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
	return prepareMessageWithCache(tm, &tm.cache)
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

// TrialFilterMod is what a user submits to define a trial subscription.
type TrialFilterMod struct {
	TrialIds      []int  `json:"trial_ids"`
	ExperimentIds []int  `json:"experiment_ids"`
}

// When a user submits a new TrialFilterMod, we scrape the database for initial matches.
func (tss TrialFilterMod) InitialScan(ctx context.Context) (
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

type TrialFilterMaker struct {
	TrialIds      map[int]bool
	ExperimentIds map[int]bool
}

func NewTrialFilterMaker() FilterMaker[*TrialMsg] {
	return &TrialFilterMaker{make(map[int]bool), make(map[int]bool)}
}

func (ts *TrialFilterMaker) AddSpec(spec FilterMod) {
	tSpec := spec.(TrialFilterMod)
	for _, id := range tSpec.TrialIds {
		ts.TrialIds[id] = true
	}
	for _, id := range tSpec.ExperimentIds {
		ts.ExperimentIds[id] = true
	}
}

func (ts *TrialFilterMaker) DropSpec(spec FilterMod) {
	tSpec := spec.(TrialFilterMod)
	for _, id := range tSpec.TrialIds {
		delete(ts.TrialIds, id)
	}
	for _, id := range tSpec.ExperimentIds {
		delete(ts.ExperimentIds, id)
	}
}

func (ts *TrialFilterMaker) MakeFilter() func(*TrialMsg) bool {
	// Should this filter even run?
	if len(ts.TrialIds) == 0 && len(ts.ExperimentIds) == 0 {
		return nil
	}

	// Make a copy of the maps, because the filter must run safely off-thread.
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

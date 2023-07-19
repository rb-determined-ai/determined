package main

import (
	"context"
	"time"
	"math/rand"
	// "encoding/json"
	"github.com/goccy/go-json"

	"github.com/gorilla/websocket"

	"github.com/determined-ai/determined/master/pkg/stream"
)

// benchmark goal:
// - 10k streamers
// - each event applies to 10% of streamers
// - average streamer lifetime: 2min
// - 1k events per minute

const nStreamers = 10000; // 10k streamers
const nUsers = 1000; // 1k users
const avgLifetime = 120; // average streamer lifetime, 2m
const eventRelevanceDenom = 10; // 1 of 10 events apply to each streamer
const batchSize = 100;

type Msg struct {
	ID int64
}

func (m *Msg) SeqNum() int64 {
	return m.ID
}

func (m *Msg) PreparedMessage() *websocket.PreparedMessage {
	_, err := json.Marshal(m)
	if err != nil {
		println(err.Error())
		return nil
	}
	jbytes := []byte("{ID: 1}")
	msg, err := websocket.NewPreparedMessage(websocket.BinaryMessage, jbytes)
	if err != nil {
		println(err.Error())
		return nil
	}
	return msg
}

func EventGen(p *stream.Publisher[*Msg]) {
	nextReportTime := time.Now().Add(1 * time.Second)
	id := int64(0)
	lastReportId := id
	for {
		// generate updates
		var updates []stream.Update[*Msg]
		for n := 0; n < batchSize; n++ {
			event := &Msg{ID: id}
			id++
			// generate user map
			users := make([]int, 0, nUsers * 2 / eventRelevanceDenom)
			for u := 0; u < nUsers; u++ {
				if rand.Uint32() % eventRelevanceDenom == 0 {
					users = append(users, u)
				}
			}
			updates = append(updates, stream.Update[*Msg]{event, users})
		}
		// broadcast updates
		stream.Broadcast(p, updates)
		now := time.Now()
		if now.After(nextReportTime) {
			nreported := id - lastReportId
			lastReportId = id
			nextReportTime = nextReportTime.Add(1 * time.Second)
			println("events per second:", nreported)
		}
	}
}

func OneStreamer(p *stream.Publisher[*Msg], id int) {
	user := id % nUsers

	// pick a random lifetime
	lifetime := avgLifetime + time.Duration(rand.NormFloat64() * float64(avgLifetime) / 4.0)
	deadline := time.Now().Add(lifetime * time.Second)
	ctx, _ := context.WithDeadline(context.Background(), deadline)


	streamer := stream.NewStreamer()

	go func(){
		<-ctx.Done()
		streamer.Close()
	}()

	pred := func(ev *Msg) bool {
		return true
	}

	sub := stream.NewSubscription[*Msg](streamer, pred)

	// don't care about newest published yet
	_ = p.AddSubscription(sub, user)
	defer p.RemoveSubscription(sub, user)

	var events []*websocket.PreparedMessage

	// Listen for wakeups on our Streamer.Cond.
	waitForSomething := func() ([]*websocket.PreparedMessage, bool) {
		streamer.Cond.L.Lock()
		defer streamer.Cond.L.Unlock()
		for len(streamer.Events) == 0 && !streamer.Closed {
			streamer.Cond.Wait()
		}
		// steal events
		events := streamer.Events
		streamer.Events = nil
		return events, streamer.Closed
	}

	for {
		// hand a batch of events to the callback
		if len(events) > 0 {
			// presently a noop
		}
		// wait for more events
		var closed bool
		events, closed = waitForSomething()
		// were we closed?
		if closed {
			return
		}
	}
}

func Streamer(p *stream.Publisher[*Msg], id int) {
	// watch the thundering herd
	time.Sleep(2 * time.Second)

	for {
		OneStreamer(p, id)
	}
}

func main() {
	publisher := stream.NewPublisher[*Msg]()
	for i := 0; i < nStreamers; i++ {
		go Streamer(publisher, i)
	}

	EventGen(publisher)
}

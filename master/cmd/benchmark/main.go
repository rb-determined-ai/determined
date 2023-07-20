package main

import (
	"context"
	"time"
	"math/rand"

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

type MsgType = struct{}
// type RecvType = []*stream.Event[MsgType]
type RecvType = []*[]byte

func EventGen(p *stream.Publisher[MsgType]) {
	nextReportTime := time.Now().Add(1 * time.Second)
	id := uint64(0)
	lastReportId := id
	for {
		// generate updates
		var updates []stream.Update[MsgType]
		for n := 0; n < batchSize; n++ {
			event := &stream.Event[MsgType]{ID: id}
			id++
			// generate user map
			users := make([]int, 0, nUsers * 2 / eventRelevanceDenom)
			for u := 0; u < nUsers; u++ {
				if rand.Uint32() % eventRelevanceDenom == 0 {
					users = append(users, u)
				}
			}
			updates = append(updates, stream.Update[MsgType]{event, users})
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

func OneStreamer(p *stream.Publisher[MsgType], id int) {
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

	pred := func(ev *stream.Event[MsgType]) bool {
		return true
	}

	sub := stream.NewSubscription[MsgType](streamer, pred)

	// don't care about newest published yet
	_ = stream.AddSubscription(p, sub, user)
	defer stream.RemoveSubscription(p, sub, user)

	var events RecvType

	// Listen for wakeups on our Streamer.Cond.
	waitForSomething := func() (RecvType, bool) {
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

	onEvents := func(evs RecvType) {
		// presently a noop
		// time.Sleep(20 * time.Millisecond)
	}

	for {
		// hand a batch of events to the callback
		if len(events) > 0 {
			onEvents(events)
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

func Streamer(p *stream.Publisher[MsgType], id int) {
	// watch the thundering herd
	time.Sleep(2 * time.Second)

	for {
		OneStreamer(p, id)
	}
}

func main() {
	publisher := stream.NewPublisher[MsgType]()
	for i := 0; i < nStreamers; i++ {
		go Streamer(publisher, i)
	}

	EventGen(publisher)
}

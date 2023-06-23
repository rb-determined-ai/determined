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
const eventsPerSec = 10000; // 1k events per second
const eventRelevanceDenom = 10; // 1 of 10 events apply to each streamer
const batchSize = 100;

func EventGen(p *stream.Publisher[int]) {
	const reportPeriod = eventsPerSec
	id := uint64(0)
	lastReport := id
	i := 0
	last := time.Now()
	lastReportTime := last
	interval := time.Second / eventsPerSec * batchSize
	for {
		now := time.Now()
		next := last.Add(interval)
		if next.After(now) {
			// time.Sleep(next.Sub(now))
			last = next
		}else{
			last = now
		}
		// generate updates
		var updates []stream.Update[int]
		for n := 0; n < batchSize; n++ {
			event := &stream.Event[int]{id, i}
			i = (i + 1) % eventRelevanceDenom
			id++
			// generate user map
			users := make([]int, 0, nUsers * 2 / eventRelevanceDenom)
			for u := 0; u < nUsers; u++ {
				if rand.Uint32() % eventRelevanceDenom == 0 {
					users = append(users, u)
				}
			}
			updates = append(updates, stream.Update[int]{event, users})
		}
		// broadcast updates
		stream.Broadcast(p, updates)
		if id - lastReport > reportPeriod {
			func(){
				nreported := id - lastReport
				lastReport = id
				now := time.Now()
				reportWindow := float64(now.Sub(lastReportTime)) / float64(time.Second)
				lastReportTime = now
				println("events per second:", int(float64(nreported)/reportWindow))
			}()
		}
	}
}

func OneStreamer(p *stream.Publisher[int], id int) {
	// pick a random lifetime
	lifetime := avgLifetime + time.Duration(rand.NormFloat64() * float64(avgLifetime) / 4.0)
	deadline := time.Now().Add(lifetime * time.Second)
	ctx, _ := context.WithDeadline(context.Background(), deadline)

	filter := func(ev *stream.Event[int]) bool {
		return true
		return id % eventRelevanceDenom == ev.Msg
	}

	onEvents := func(evs []*stream.Event[int]) {
		// presently a noop
	}

	user := id % nUsers
	stream.Stream(p, 0, user, filter, onEvents, ctx)
}

func Streamer(p *stream.Publisher[int], id int) {
	// watch the thundering herd
	time.Sleep(2 * time.Second)

	for {
		OneStreamer(p, id)
	}
}

func main() {
	publisher := stream.NewPublisher[int]()
	for i := 0; i < nStreamers; i++ {
		go Streamer(publisher, i)
	}

	EventGen(publisher)
}

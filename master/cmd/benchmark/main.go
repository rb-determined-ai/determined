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
			time.Sleep(next.Sub(now))
			last = next
		}else{
			last = now
		}
		func(){
			p.Lock.Lock()
			defer p.Lock.Unlock()
			for n := 0; n < batchSize; n++ {
				stream.UpdateUnlocked(p, id, i)
				i = (i + 1) % eventRelevanceDenom
				id++
			}
		}()
		if id - lastReport > reportPeriod {
			func(){
				p.Lock.Lock()
				defer p.Lock.Unlock()
				n := 0
				for ev := &p.Head.SubscriberEvent; ev.Event.ID > 0; ev = ev.Next {
					n++
				}
				nreported := id - lastReport
				lastReport = id
				now := time.Now()
				reportWindow := float64(now.Sub(lastReportTime)) / float64(time.Second)
				lastReportTime = now
				println("events per second:", int(float64(nreported)/reportWindow), "events in queue:", n)
			}()
		}
		stream.Broadcast(p)
	}
}

func OneStreamer(p *stream.Publisher[int], id int) {
	// pick a random lifetime
	lifetime := avgLifetime + time.Duration(rand.NormFloat64() * float64(avgLifetime) / 4.0)
	deadline := time.Now().Add(lifetime * time.Second)
	ctx, _ := context.WithDeadline(context.Background(), deadline)

	filter := func(ev *stream.Event[int]) bool {
		return id % eventRelevanceDenom == ev.Msg
	}
	sub := stream.NewSubscriber[int](filter)

	onEvent := func(ev *stream.Event[int]) {
		// noop
	}

	stream.Stream(p, sub, 0, onEvent, ctx)
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

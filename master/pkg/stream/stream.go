package stream

import (
	"sync"
	"context"
)

// func LinkedAppend[T Linked](Linked head
//
// type Listener interface {
// 	Next() *Listener
// 	Prev() *Listener
// 	SetNext*
// }
//
// type Schedulable interface {
// 	Next *Schedulable
// 	Prev *Schedulable
// }
//
// type Streamer struct {
// 	Next()
// }
//
// type Wakeable[T any] interface {
// 	Wake(T)
// }
//
// // ExperimentBroadcaster gets database wakeups, scans for new updates, then calls wakeup on each
// // subscriber.
// type ExperimentBroadcaster {
// 	Listeners Wakeable[T any]
// }
//
//
// // interface is basis of generic list add and list remove functions
// type Linked interface {
// 	Next() *Linked
// 	Prev() *Linked
// 	SetNext(*Linked)
// 	SetPrev(*Linked)
// }
//
// // basic strategy //
//
// // These work on anything that implements Linked
// func ListAppend[T Linked](head T, item T) { ... }
// func ListPrepend[T Linked](head T, item T) { ... }
// func ListRemove[T Linked](item T) { ... }
//
// // Basic strategy, combine with gogen for Next/Prev/SetNext/SetPrev.
// // Has drawback that Thing can only be in one list.
// type Thing struct {
// 	ThingData string
// 	Next *Thing
// 	Prev *Thing
// }
//
// // offset-of strategy //
//
// type Link struct {
// 	Next *Link
// 	Prev *Link
// }
//
// type Thing struct {
// 	ThingData string
// 	// One link per list you expect it to be in
// 	SubscriberItem Link
// 	CancelableItem Link
// }
//
// // iterate through subscribers
// for(l := subscribers.next; l != subscribers; l = l->Next){
// 	thing := ThingContainerOfSubscriberItem(l) // uses offset of Thing.SubscriberItem
// 	...
// }
//
// // iterate through cancelables
// for(l := cancelables.next; l != cancelables; l = l->Next){
// 	thing := ThingContainerOfCancelableItem(l) // uses offset of Thing.CancelableItem
// 	...
// }
//
// // there is a wakeup/filter thread
// // all broadcasters operate within that thread, making for easy sync points to dedupe events
// // broadcaster wakes up, scans db for (msg, owner, workspace) tuples
// // for msg, owner, workspace in tuples:
// //    build a []byte of said msg, only once for all streams
// //    for each listener:
// //        pass the (msg, byts, owner, workspace)
// //        listener checks owner/workspace, passes to streamer
//
// type Subscriber struct {
// 	OwnerId StreamAuth
// }
//
// type SubscriberFn
//
// type Publisher[T any] struct {
// 	Subscribers []Subscriber[T]
// }
//
// type Subscriber[T any] interface {
// 	OnEvent(T, []byte, StreamAuth)
// }
//
// type ExperimentSubscriber struct {
// }
//
// func (*s ExperimentSubscriber) OnEvent(msg ExperimentMsg, byts []byte, streamAuth StreamAuth){
// }
//
// func (p *Subscriber) OnEvent[T](msg T, byts []byte, streamAuth StreamAuth) {
// }
//
// func StreamExperiments(user UserID) {
// 	authFilter := rbac.GetStreamAuth(UserId)
//
// 	lock.Lock()
//
// 	onEvent := func(msg ExperimentMsg, byts []byte, streamAuth StreamAuth) {
//
// 	}
// }
//
// // pub distributes messages and calls wakeup
//
// type Subscriber[T any] {
// 	Filter func(msg T, streamAuth StreamAuth)bool
// 	Lock sync.Mutex
// 	Cond sync.Condition
// 	Msgs []T
// 	bool Closed
// }
//
// type Publisher[T any] {
// 	Subscribers []*Subscriber[T]
// }
//
// type Update[T] struct {
// 	Msg T
// 	StreamAuth StreamAuth
// }

// publisher keeps a linked list of data, streamers wake up and check list
//
// memory wasted is equal to the total number of events * longest lag duration, rather than
// number of events * lag duration * number of laggards
//
// periodically the publisher walks the list of subscribers, cleaning and looking for the oldest
// value
//
// adding a subscriber is synchronous, but removing a subscriber is asynchronous
//
// Memory usage (A):
// + one Streamer goroutine and one websocket goroutine per websocket.
// + each Event only exists in memory once, for as long as all Streamers need it.
//
// SQL Pressure (A):
// + Only Publishers query the database after each update.
// + Streamers only query database on initial connection.
//
// Lock contention (B):
// - New subscribers lock until the publisher is idle.
// + Otherwise, unsubscribing requires no lock, and other locks would not be contentious.
//
// Complexity (C):
// - Violates "communicate to share" rule in order to avoid scaling memory by
//   number of events * number of streamers.
// - Uses locks instead of channels to keep backpressure off of publisher thread.
// + Overall just a couple hundred lines of code.
// + Interaction with external code is a simple EventCB, which is hard to mess up.
//
// Benchmarking results:
//
//     This system really falls over around 10k streamers and 1k events per second, which feels like
//     not very much.  It appears that the context switching cost is dominating performance.  The
//     design choice to always wakeup and run filters on streamer is really hurting us.

// Event callbacks have no access to the linked list.
type Event[T any] struct {
	ID uint64
	Msg T
	// StreamAuth StreamAuth
}

type Subscriber[T any] struct {
	// filter is applied before a wakeup
	Filter func(*Event[T]) bool
	Cond *sync.Cond
	Events []*Event[T]
	// Is this subscriber closed?  Set externally, Subscriber notices eventually.
	Closed bool
}

type Publisher[T any] struct {
	Lock sync.Mutex
	Subscribers []*Subscriber[T]
	// The most recent published event (won't be published again)
	NewestPublished uint64
}

// returns current NewestPublished
func AddSubscriber[T any](p *Publisher[T], sub *Subscriber[T]) uint64 {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.Subscribers = append(p.Subscribers, sub)
	return p.NewestPublished
}

func Broadcast[T any](p *Publisher[T], events []*Event[T]) {
	// XXX: not sure how to reuse buf
	buf := make([]*Event[T], len(events))

	p.Lock.Lock()
	defer p.Lock.Unlock()
	i := 0
	n := len(p.Subscribers)
	for i < n {
		sub := p.Subscribers[i]
		if sub.Closed {
			// replace this position with the final subscriber
			p.Subscribers[i] = p.Subscribers[n-1]
			n--;
			// leave i alone, we'll revisit this position
			continue
		}

		// Gather messages this subscriber cares about.
		l := 0
		for _, event := range events {
			if sub.Filter(event/*, event.StreamAuth*/) {
				buf[l] = event
				l++
			}
		}
		if l > 0 {
			// Pass messages and wake up subscriber.
			func(){
				sub.Cond.L.Lock()
				defer sub.Cond.L.Unlock()
				sub.Events = append(sub.Events, buf[:l]...)
				sub.Cond.Signal()
			}()
		}

		i++;
	}
	if n < len(p.Subscribers) {
		// shorten list
		p.Subscribers = p.Subscribers[:n]
	}

	// track our newest published message
	for _, event := range events {
		if event.ID > p.NewestPublished {
			p.NewestPublished = event.ID
		}
	}
}

func NewSubscriber[T any](filter func(*Event[T]) bool) *Subscriber[T] {
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	return &Subscriber[T]{Filter: filter, Cond: cond}
}

func CloseSubscriber[T any](sub *Subscriber[T]){
	sub.Cond.L.Lock()
	defer sub.Cond.L.Unlock()
	sub.Closed = true
	sub.Cond.Signal()
}

func Stream[T any](
	p *Publisher[T],
	since uint64,
	filter func(*Event[T]) bool,
	onEvents func([]*Event[T]),
	ctx context.Context,
) {
	sub := NewSubscriber(filter)

	// We need a way to detect ctx.Done() and also a sync.Cond-based shutdown for this streamer,
	// since the REST API endpoint uses a context (which is idiomatic and simple), but the publisher
	// uses sync.Cond instead of channels (because the publisher must never block).
	//
	// One solution is to introduce an extra goroutine to receive Cond wakeups and send events over
	// channels.  That introduces an extra context switch per event per websocket.
	//
	// A lighter weight solution is to use one goroutine per websocket to wait on the context and
	// call trigger the Cond-based shutdown.  That solution requires an equal number of goroutines
	// but only requires a context switch once over the whole lifetime of the websocket.
	//
	// An even lighter soultion would be to use one goroutine to watch all contexts (a dynamic
	// number of them) using a hierarchical waiting scheme, like this one:
	//
	//     https://cyolo.io/blog/how-we-enabled-dynamic-channel-selection-at-scale-in-go/
	//
	// But that's probably not useful at our scale yet, and probably we should avoid
	// one-goroutine-per websocket before we fight that battle anyway.
	go func(){
		<-ctx.Done()
		CloseSubscriber(sub)
	}()

	newestPublished := AddSubscriber(p, sub)

	var events []*Event[T]

	if since < newestPublished {
		// // Get initial elements from the database.
		// events := SQLGatherEvents(since, newestDeleted) // TODO: add database filtering
		// for _, event := range events {
		// 	msgs <- event
		// }
	}

	waitForSomething := func() ([]*Event[T], bool) {
		sub.Cond.L.Lock()
		defer sub.Cond.L.Unlock()
		for len(sub.Events) == 0 && !sub.Closed {
			sub.Cond.Wait()
		}
		// steal events
		events := sub.Events
		sub.Events = nil
		return events, sub.Closed
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

// type EventStreamer[T any] {
// 	sub *Subscriber[T]
// 	msgs chan[*Event[T]]
// }
//
// type Streamer[T any] interface {
// 	Msgs() chan *Event[T]
// 	Close()
// }
//
// func (s *Streamer) Close() {
// 	sub.Cond.L.Lock()
// 	defer sub.Cond.L.Unlock()
// 	sub.Closed = true
// }
//
// func WebSocket(ctx context.Context) {
// 	streamer := StreamFromPublisher(GlobalExperimentPublisher)
// 	defer streamer.Close()
//
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return
// 		case msg, ok := <-streamer.Msgs():
// 			if !ok {
// 				// stream was broken (auth changed)
// 				return
// 			}
// 		}
// 	}
// }

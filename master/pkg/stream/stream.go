package stream

import (
	"sync"
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
//
// func Publish[T](p *Publisher[T], updates []Update[T]) {
// 	// XXX: not sure how to reuse buf
// 	buf := make([]T, len(updates))
// 	i := 0
// 	n := len(p.Subscribers)
// 	for i < n {
// 		sub := p.Subscribers[i]
// 		if sub.Closed {
// 			// replace this position with the final subscriber
// 			p.Subscribers[i] = p.Subscribers[-1]
// 			// shorten list
// 			n--;
// 			// leave i alone, we'll revisit this position
// 			continue
// 		}
// 		// Gather messages this subscriber cares about.
// 		l := 0
// 		for _, update := range udpates {
// 			if sub.Filter(update.Msg, update.StreamAuth) {
// 				buf[l++] = update.Msg
// 			}
// 		}
// 		if l > 0 {
// 			// Pass messages and wake up subscriber.
// 			func(){
// 				sub.Cond.L.Lock()
// 				defer sub.Cond.L.Unlock()
// 				sub.Msgs = append(sub.Msgs, buf[:l]...)
// 				sub.Cond.Signal()
// 			}()
// 		}
// 		i++;
// 	}
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

// Subscribers have access to a singly-linked list.
type SubscriberEvent[T any] struct {
	Event Event[T]
	Next *SubscriberEvent[T]
}

// Publisher maintains a doubly-linked list.
type PublisherEvent[T any] struct {
	SubscriberEvent SubscriberEvent[T]
	Prev *PublisherEvent[T]
}

type Subscriber[T any] struct {
	// Is this subscriber closed?  Set externally, Subscriber notices eventually.
	Closed bool
	// Is this subscriber still reading published messages?  Only when Stream finishes is this set.
	DoneReading bool
	// What is the newest message this Subscriber has seen?
	// The publisher will not delete messages newer than or equal to Seen.
	// The subscriber will not peek at messages older than Seen.
	Seen uint64
	Cond *sync.Cond
	// Head is the head of a linked list of events.
	Head *SubscriberEvent[T]
}

type Publisher[T any] struct {
	Lock sync.Mutex
	Subscribers []*Subscriber[T]
	// Add to the head.
	Head *PublisherEvent[T]
	// Delete from the tail.  Note the Tail starts with a dummy event of Seen=0.
	Tail *PublisherEvent[T]
	// The most recent of deleted events.
	NewestDeleted uint64
}

func NewPublisher[T any]() *Publisher[T] {
	sentinel := &PublisherEvent[T]{}
	return &Publisher[T]{
		Head: sentinel,
		Tail: sentinel,
	}
}

// add a msg to the publisher
func UpdateUnlocked[T any](p *Publisher[T], ID uint64, msg T/*, streamAuth StreamAuth*/) {
	publisherEvent := &PublisherEvent[T]{
		SubscriberEvent: SubscriberEvent[T]{
			Event: Event[T]{
				ID: ID,
				Msg: msg,
				//StreamAuth: StreamAuth,
			},
			Next: &p.Head.SubscriberEvent,
		},
	}
	p.Head.Prev = publisherEvent
	p.Head = publisherEvent
}

// add a subscriber to the publisher, assign sub.Seen
func AddSubscriber[T any](p *Publisher[T], sub *Subscriber[T]) (*SubscriberEvent[T], uint64) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	p.Subscribers = append(p.Subscribers, sub)
	sub.Head = &p.Head.SubscriberEvent

	// Note that sub.Seen starts as zero, meaning new subs effectively preserve the current
	// published list until they have a chance to see all of it (naturally).

	// Return newestDeleted, so the sub knows what to query the database for.
	return sub.Head, p.NewestDeleted
}

// wake up subscribers, and give them a pointer to the new head of the list
func Broadcast[T any](p *Publisher[T]) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	i := 0
	n := len(p.Subscribers)
	lowestSeen := ^uint64(0)
	head := &p.Head.SubscriberEvent
	for i < n {
		sub := p.Subscribers[i]
		if sub.DoneReading {
			// replace this position with the final subscriber
			p.Subscribers[i] = p.Subscribers[n-1]
			n--;
			// leave i alone, we'll revisit this position
			continue
		}

		// keep track of lowestSeen
		if sub.Seen < lowestSeen {
			lowestSeen = sub.Seen
		}

		// wake up subscriber with new Head.
		func(){
			sub.Cond.L.Lock()
			defer sub.Cond.L.Unlock()
			sub.Head = head
			sub.Cond.Signal()
		}()
		i++;
	}
	if n < len(p.Subscribers) {
		// shorten list
		p.Subscribers = p.Subscribers[:n]
	}

	// walk backwards through oldest events, and unlink any that are older than lowestSeen.
	sentinel := p.Tail
	oldest := sentinel.Prev
	for oldest != nil && oldest.SubscriberEvent.Event.ID < lowestSeen {
		temp := oldest.Prev
		oldest.SubscriberEvent.Next = nil
		oldest.Prev = nil
		if oldest.SubscriberEvent.Event.ID > p.NewestDeleted {
			p.NewestDeleted = oldest.SubscriberEvent.Event.ID
		}
		oldest = temp
	}
	// did we empty the whole list?
	if oldest == nil {
		// Update p.Head to point to our sentinel at p.Tail.
		//
		// Note that Subscribers may still have a pointer to the old p.Head, so it won't be GC'd
		// immediately, but those subscribers have already promised not to iterate past it by
		// setting their sub.Seen values.
		p.Head = sentinel
	} else {
		// new list tail is the oldest with ID >= lowestSeen
		sentinel.Prev = oldest
		sentinel.Prev.SubscriberEvent.Next = &sentinel.SubscriberEvent
	}
}

func NewSubscriber[T any]() *Subscriber[T] {
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	return &Subscriber[T]{Cond: cond}
}

func CloseSubscriber[T any](sub *Subscriber[T]){
	sub.Cond.L.Lock()
	defer sub.Cond.L.Unlock()
	sub.Closed = true
	sub.Cond.Signal()
}

func Stream[T any](p *Publisher[T], sub *Subscriber[T], since uint64, msgs chan *Event[T]) {
	// if we were to do the SQL first, and base sub.Seen on that, we could connect to the publisher
	// after a new message was already broadcast and deleted, and we would miss it.
	// Subscribe to the publisher, and find out where it's stream starst.
	// head, newestDeleted := AddSubscriber(p, sub)
	head, _ := AddSubscriber(p, sub)

	defer close(msgs)

	defer func(){
		sub.DoneReading = true
	}()

	if since < sub.Seen {
		// // Get initial elements from the database.
		// events := SQLGatherEvents(since, newestDeleted) // TODO: add database filtering
		// for _, event := range events {
		// 	msgs <- event
		// }
	}

	// start processing events from publisher
	waitForSomething := func(head *SubscriberEvent[T]) (*SubscriberEvent[T], bool) {
		sub.Cond.L.Lock()
		defer sub.Cond.L.Unlock()
		for sub.Head == head && !sub.Closed {
			sub.Cond.Wait()
		}
		return sub.Head, sub.Closed
	}

	for {
		// process intial or additional events
		for event := head; event.Event.ID > sub.Seen ; event = event.Next {
			msgs <- &event.Event
			// check if we're closed
			if sub.Closed {
				return
			}
		}
		// done with events up to our head
		if sub.Seen < head.Event.ID {
			sub.Seen = head.Event.ID
		}
		// wait for more
		var closed bool
		head, closed = waitForSomething(head)
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

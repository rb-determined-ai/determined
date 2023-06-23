package stream

import (
	"sync"
	"context"
)

// Event callbacks have no access to the linked list.
type Event[T any] struct {
	ID uint64
	Msg T
}

// Update contains an Event and a slice of applicable user ids
type Update[T any] struct {
	Event *Event[T]
	Users []int
}

type Subscriber[T any] struct {
	User int
	// filter is applied before a wakeup
	Filter func(*Event[T]) bool
	Cond *sync.Cond
	Events []*Event[T]
	// Is this subscriber closed?  Set externally, Subscriber notices eventually.
	Closed bool
}

type SubscriberGroup[T any] struct {
	Subscribers []*Subscriber[T]
	WakeupID uint64
	Events []*Event[T]
}

type Publisher[T any] struct {
	Lock sync.Mutex
	// map userids to subscribers matching those userids
	// XXX: benchmark with slice instead of map
	UserSubs map[int]*SubscriberGroup[T]
	// The most recent published event (won't be published again)
	NewestPublished uint64
	WakeupID uint64
}

func NewPublisher[T any]() *Publisher[T]{
	return &Publisher[T]{
		UserSubs: make(map[int]*SubscriberGroup[T]),
	}
}

// returns current NewestPublished
func AddSubscriber[T any](p *Publisher[T], sub *Subscriber[T]) uint64 {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usersub, ok := p.UserSubs[sub.User]
	if !ok {
		usersub = &SubscriberGroup[T]{}
		p.UserSubs[sub.User] = usersub
	}
	usersub.Subscribers = append(usersub.Subscribers, sub)
	return p.NewestPublished
}

func RemoveSubscriber[T any](p *Publisher[T], sub *Subscriber[T]) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usersub := p.UserSubs[sub.User]
	for i, s := range usersub.Subscribers {
		if s == sub {
			last := len(usersub.Subscribers) - 1
			usersub.Subscribers[i] = usersub.Subscribers[last]
			usersub.Subscribers = usersub.Subscribers[:last]
			return
		}
	}
}

func Broadcast[T any](p *Publisher[T], updates []Update[T]) {
	p.Lock.Lock()
	defer p.Lock.Unlock()

	// start with a fresh wakeupid
	p.WakeupID++
	wakeupID := p.WakeupID

	for _, update := range updates {
		// keep track of the newest published event
		if update.Event.ID > p.NewestPublished {
			p.NewestPublished = update.Event.ID
		}
		for _, user := range update.Users {
			// find matching user group
			usersub, ok := p.UserSubs[user]
			if !ok || len(usersub.Subscribers) == 0 {
				continue
			}
			// re-initialize usersub?
			if wakeupID != usersub.WakeupID {
				usersub.Events = nil
				usersub.WakeupID = wakeupID
			}
			// add event to usersub
			usersub.Events = append(usersub.Events, update.Event)
		}
	}

	// do wakeups
	for _, usersub := range p.UserSubs {
		// any updates for this user?
		if usersub.WakeupID != wakeupID {
			continue
		}
		for _, sub := range usersub.Subscribers {
			func(){
				wakeup := false
				for _, event := range usersub.Events {
					// does this subscriber want this event?
					if !sub.Filter(event) {
						continue
					}
					// is it the first event for this subscriber?
					if !wakeup {
						wakeup = true
						sub.Cond.L.Lock()
						defer sub.Cond.L.Unlock()
						defer sub.Cond.Signal()
					}
					sub.Events = append(sub.Events, event)
				}
			}()
		}
	}
}

func NewSubscriber[T any](user int, filter func(*Event[T]) bool) *Subscriber[T] {
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	return &Subscriber[T]{User: user, Filter: filter, Cond: cond}
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
	user int,
	filter func(*Event[T]) bool,
	onEvents func([]*Event[T]),
	ctx context.Context,
) {
	sub := NewSubscriber(user, filter)

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
	defer RemoveSubscriber(p, sub)

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

package stream

import (
	"sync"
)

// Event callbacks have no access to the linked list.
type Event[T any] struct {
	ID uint64
	Msg T
	// only marshal once per message
	Bytes []byte
}

// Update contains an Event and a slice of applicable user ids
type Update[T any] struct {
	Event *Event[T]
	Users []int
}

// Streamer aggregates many events and wakeups into a single slice of pre-marshaled messages.
// One streamer may be associated with many Subscription[T]'s, but it should only have at most one
// Subscription per type T.  One Streamer is intended to belong to one websocket connection.
type Streamer struct {
	Cond *sync.Cond
	// Events is a slice of slice pointers, because a slice of slices is actually big enough to
	// cause a 25% performance penalty in terms of total wakeup throughput.
	Events []*[]byte
	// Closed is set externally, and noticed eventually.
	Closed bool
}

func NewStreamer() *Streamer {
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	return &Streamer{ Cond: cond }
}

func (s *Streamer) Close() {
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	s.Cond.Signal()
	s.Closed = true
}

type Subscription[T any] struct {
	// Which streamer is collecting events from this Subscription?
	Streamer *Streamer
	// Decide if the streamer wants this event.
	Predicate func(*Event[T]) bool
	// wakeupID prevent duplicate wakeups if multiple events in a single Broadcast are relevant
	wakeupID uint64
}

func NewSubscription[T any](streamer *Streamer, predicate func(*Event[T]) bool) *Subscription[T] {
	return &Subscription[T]{ Streamer: streamer, Predicate: predicate }
}

// UserGroup is a set of filters belonging to the same user.  It is part of stream rbac enforcement.
type UserGroup[T any] struct {
	Subscriptions []*Subscription[T]
	Events []*Event[T]

	// a self-pointer for efficient update tracking
	next     *UserGroup[T]
	wakeupID uint64
}

type Publisher[T any] struct {
	Lock sync.Mutex
	// map userids to subscriptions matching those userids
	UserGroups map[int]*UserGroup[T]
	// The most recent published event (won't be published again)
	NewestPublished uint64
	WakeupID uint64
}

func NewPublisher[T any]() *Publisher[T]{
	return &Publisher[T]{
		UserGroups: make(map[int]*UserGroup[T]),
	}
}

// returns current NewestPublished
func AddSubscription[T any](p *Publisher[T], sub *Subscription[T], user int) uint64 {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usergrp, ok := p.UserGroups[user]
	if !ok {
		usergrp = &UserGroup[T]{}
		p.UserGroups[user] = usergrp
	}
	usergrp.Subscriptions = append(usergrp.Subscriptions, sub)
	return p.NewestPublished
}

func RemoveSubscription[T any](p *Publisher[T], sub *Subscription[T], user int) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usergrp := p.UserGroups[user]
	for i, s := range usergrp.Subscriptions {
		if s == sub {
			last := len(usergrp.Subscriptions) - 1
			usergrp.Subscriptions[i] = usergrp.Subscriptions[last]
			usergrp.Subscriptions = usergrp.Subscriptions[:last]
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

	groupSentinel := UserGroup[T]{}
	activeGroups := &groupSentinel

	// pass each update to each UserGroup representing users who are allowed to see it
	for _, update := range updates {
		// keep track of the newest published event
		if update.Event.ID > p.NewestPublished {
			p.NewestPublished = update.Event.ID
		}
		for _, user := range update.Users {
			// find matching user group
			usergrp, ok := p.UserGroups[user]
			if !ok || len(usergrp.Subscriptions) == 0 {
				continue
			}
			// first event for this user sub?
			if wakeupID != usergrp.wakeupID {
				usergrp.wakeupID = wakeupID
				usergrp.next = activeGroups
				activeGroups = usergrp
				// re-initialize events
				usergrp.Events = nil
			}
			// add event to usergrp
			usergrp.Events = append(usergrp.Events, update.Event)
		}
	}

	// do wakeups, visiting the active usergrps we collected in our list
	usergrp := activeGroups
	next := usergrp.next
	activeGroups = nil
	for ; usergrp != &groupSentinel; usergrp = next {
		// break down the list as we go, so gc is effective
		next = usergrp.next
		usergrp.next = nil

		// Deliver fewer wakeups: any streamer may own many subscriptions in this UserGroup,
		// but since SubscriptionGroups are user-based, no streamer can own subsriptions in two
		// groups.
		func(){
			for _, sub := range usergrp.Subscriptions {
				for _, event := range usergrp.Events {
					// does this subscription want this event?
					if !sub.Predicate(event){
						continue
					}
					// is it the first event for this Subscription?
					if sub.wakeupID != wakeupID {
						sub.wakeupID = wakeupID
						sub.Streamer.Cond.L.Lock()
						defer sub.Streamer.Cond.L.Unlock()
						sub.Streamer.Cond.Signal()
					}
					// append bytes into the Streamer, which is type-independent
					// TODO: actually marshal with caching

					// sub.Streamer.Events = append(sub.Streamer.Events, event)
					sub.Streamer.Events = append(sub.Streamer.Events, &event.Bytes)
				}
			}
		}()
	}
}

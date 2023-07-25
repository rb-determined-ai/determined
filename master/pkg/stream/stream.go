package stream

import (
	"sync"

	"github.com/gorilla/websocket"
)

// Event is an object with a message and a sequence number and json marshal caching.
type Event interface {
	SeqNum() int64
	PreparedMessage() *websocket.PreparedMessage
}

// Update contains an Event and a slice of applicable user ids
type Update[T Event] struct {
	Event T
	Users []int
}
// Streamer aggregates many events and wakeups into a single slice of pre-marshaled messages.
// One streamer may be associated with many Subscription[T]'s, but it should only have at most one
// Subscription per type T.  One Streamer is intended to belong to one websocket connection.
type Streamer struct {
	Cond *sync.Cond
	Events []*websocket.PreparedMessage
	// SubscriptionSpecs are opaque to the streaming API; but they are passed through the Streamer
	// to make writing websocket goroutines easier.
	SubscriptionSpecs []interface{}
	// Closed is set externally, and noticed eventually.
	Closed bool
}

func NewStreamer() *Streamer {
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	return &Streamer{ Cond: cond }
}

// WaitForSomething returns a tuple of (subSpecs, events, closed)
func (s *Streamer) WaitForSomething() ([]interface{}, []*websocket.PreparedMessage, bool) {
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	s.Cond.Wait()
	// steal outputs
	subs := s.SubscriptionSpecs
	s.SubscriptionSpecs = nil
	events := s.Events
	s.Events = nil
	return subs, events, s.Closed
}

func (s *Streamer) Close() {
	s.Cond.L.Lock()
	defer s.Cond.L.Unlock()
	s.Cond.Signal()
	s.Closed = true
}

type Subscription[T Event] struct {
	// Which streamer is collecting events from this Subscription?
	Streamer *Streamer
	// Decide if the streamer wants this event.
	Predicate func(T) bool
	// wakeupID prevent duplicate wakeups if multiple events in a single Broadcast are relevant
	wakeupID int64
	// track our publisher for atomic updates to our predicate function
	publisher *Publisher[T]
}

func NewSubscription[T Event](streamer *Streamer, predicate func(T) bool) *Subscription[T] {
	return &Subscription[T]{ Streamer: streamer, Predicate: predicate }
}

func (s *Subscription[T]) SetPredicate(predicate func(T) bool) {
	if s.publisher != nil {
		// wait for a break between broadcasts
		s.publisher.Lock.Lock()
		defer s.publisher.Lock.Unlock()
	}
	s.Predicate = predicate
}

// UserGroup is a set of filters belonging to the same user.  It is part of stream rbac enforcement.
type UserGroup[T Event] struct {
	Subscriptions []*Subscription[T]
	Events []T

	// a self-pointer for efficient update tracking
	next     *UserGroup[T]
	wakeupID int64
}

type Publisher[T Event] struct {
	Lock sync.Mutex
	// map userids to subscriptions matching those userids
	UserGroups map[int]*UserGroup[T]
	// The most recent published event (won't be published again)
	NewestPublished int64
	WakeupID int64
}

func NewPublisher[T Event]() *Publisher[T]{
	return &Publisher[T]{
		UserGroups: make(map[int]*UserGroup[T]),
	}
}

// returns current NewestPublished
func (p *Publisher[T]) AddSubscription(sub *Subscription[T], user int) int64 {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usergrp, ok := p.UserGroups[user]
	if !ok {
		usergrp = &UserGroup[T]{}
		p.UserGroups[user] = usergrp
	}
	usergrp.Subscriptions = append(usergrp.Subscriptions, sub)
	sub.publisher = p
	return p.NewestPublished
}

func (p *Publisher[T]) RemoveSubscription(sub *Subscription[T], user int) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	usergrp := p.UserGroups[user]
	for i, s := range usergrp.Subscriptions {
		if s != sub {
			continue
		}
		last := len(usergrp.Subscriptions) - 1
		usergrp.Subscriptions[i] = usergrp.Subscriptions[last]
		usergrp.Subscriptions = usergrp.Subscriptions[:last]
		sub.publisher = nil
		return
	}
}

func Broadcast[T Event](p *Publisher[T], updates []Update[T]) {
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
		if update.Event.SeqNum() > p.NewestPublished {
			p.NewestPublished = update.Event.SeqNum()
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
					sub.Streamer.Events = append(sub.Streamer.Events, event.PreparedMessage())
				}
			}
		}()
	}
}

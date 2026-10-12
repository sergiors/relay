package cron

import (
	"context"
	"sync"

	"relay/internal/schedule"
)

// unsavedOccurrences is the scheduler-owned, bounded in-memory set of exact
// occurrence intents whose initial durable persistence failed. It exists so a
// due occurrence is never published without a durable copy AND never dropped
// merely because the local store rejected the write: the exact intent is held
// until the durable retry worker can flush it to the outbox.
//
// Ownership and bound: entries are keyed by schedule identity (app + schedule
// name), so the set holds at most one occurrence per schedule and is bounded by
// the number of registered schedules — it never grows with the length of a store
// outage. A slot exists to protect an unsaved intent from being overwritten: if a
// later occurrence for the same schedule reaches retain while the slot is still
// occupied, it WAITS for the slot rather than replacing the held one, and that
// occurrence is owned by its blocked caller.
//
// A suppressed tick is NOT retained here. gocron does not queue overlapping
// callbacks for one job (LimitModeReschedule suppresses a tick whose previous
// callback is still running), so an occurrence whose callback never runs is
// never created and never held; recovery and the bounded catch-up remain
// latest-only for any occurrence the worker never got to attempt.
//
// Handoff: release does not merely wake a waiter and hope it runs before the
// next snapshot. It hands the freed slot to the oldest waiter by closing that
// waiter's channel and recording a pending handoff; flushUnsaved waits
// (awaitSettled) until that handoff completes, so an occurrence admitted by a
// release is always observed by the draining cycle instead of racing it. A
// waiter observes the lifecycle context, so shutdown cannot hang on a full set.
//
// A held occurrence is never published while it is only in memory, and it is
// never silently dropped: it is either flushed to the outbox
// (SavePendingOccurrence) or, if the lifecycle is cancelled while it waits for
// its slot, surfaced by the caller as the documented unprotectable-loss window —
// a shutdown or crash while every durable store rejects the intent cannot be
// protected without a third durable failure domain.
type unsavedOccurrences struct {
	mu   sync.Mutex
	held map[unsavedKey]schedule.Occurrence
	// waiters is a per-schedule FIFO of channels, one per goroutine blocked in
	// retain for that schedule. release hands a freed slot to the oldest waiter
	// by closing its channel.
	waiters map[unsavedKey][]chan struct{}
	// handoffs counts, per schedule, released slots whose waiter has not yet
	// finished (recorded its own occurrence or abandoned the slot). awaitSettled
	// waits for it to reach zero.
	handoffs map[unsavedKey]int
	// settled broadcasts handoff-progress changes to awaitSettled waiters.
	settled *sync.Cond
	// onWait, when non-nil, is invoked after a retainer has registered as a waiter
	// (and before it blocks). It is a test-only observation seam: production never
	// sets it. Tests set it before starting the retaining goroutine, so the read
	// here is ordered by goroutine start.
	onWait func(unsavedKey)
}

// unsavedKey is the schedule identity that owns one in-memory slot. The handler
// is deliberately excluded: occurrence identity is (app, schedule name,
// instant), and a schedule's handler may change without changing which callback
// fires.
type unsavedKey struct {
	app      string
	schedule string
}

// unsavedKeyFor returns the slot key for o.
func unsavedKeyFor(o schedule.Occurrence) unsavedKey {
	return unsavedKey{app: o.App, schedule: o.Schedule}
}

// newUnsavedOccurrences returns an empty set.
func newUnsavedOccurrences() *unsavedOccurrences {
	u := &unsavedOccurrences{
		held:     map[unsavedKey]schedule.Occurrence{},
		waiters:  map[unsavedKey][]chan struct{}{},
		handoffs: map[unsavedKey]int{},
	}
	u.settled = sync.NewCond(&u.mu)
	return u
}

// retain records o in its schedule's slot, blocking (under ctx) while a previous
// occurrence for the SAME schedule is still unflushed. It returns false only
// when ctx is cancelled while waiting, meaning the occurrence could not be
// retained. It never overwrites an existing entry: a waiter inserts only after
// release hands it the freed slot, so if a second occurrence for one schedule
// does reach retain, both are kept — the first held, the second waiting. A
// cancelled lifecycle abandons a handed slot (chaining it to the next waiter)
// rather than consuming it, so cancellation is resolved deterministically.
func (u *unsavedOccurrences) retain(ctx context.Context, o schedule.Occurrence) bool {
	key := unsavedKeyFor(o)
	for {
		u.mu.Lock()
		if _, ok := u.held[key]; !ok {
			u.held[key] = o
			u.mu.Unlock()
			return true
		}
		ch := make(chan struct{})
		u.waiters[key] = append(u.waiters[key], ch)
		onWait := u.onWait
		u.mu.Unlock()
		if onWait != nil {
			onWait(key)
		}
		// A lifecycle that is already cancelled when we resume gives up WITHOUT
		// consuming a handed slot. Checking here (rather than only inside the
		// select) keeps the choice deterministic when both a cancelled context and
		// a handed-over slot are ready: select would otherwise pick between them at
		// random.
		if ctx.Err() != nil {
			u.abandonWaiter(key, ch)
			return false
		}

		select {
		case <-ch:
			// release handed the slot to us and recorded a pending handoff. Settle
			// that handoff; if a fresh retainer took the slot in the meantime,
			// re-check and wait again rather than overwriting it.
			u.mu.Lock()
			u.finishHandoffLocked(key)
			if _, ok := u.held[key]; ok {
				u.mu.Unlock()
				continue
			}
			u.held[key] = o
			u.mu.Unlock()
			return true
		case <-ctx.Done():
			u.abandonWaiter(key, ch)
			return false
		}
	}
}

// abandonWaiter completes a cancelled retainer. If it is still registered it is
// simply removed; if release already handed it the slot (its channel was closed
// and a handoff counted) the handoff is settled and the still-free slot is passed
// to the next waiter, if any, so it is not stranded.
func (u *unsavedOccurrences) abandonWaiter(key unsavedKey, ch chan struct{}) {
	u.mu.Lock()
	if u.removeWaiterLocked(key, ch) {
		u.mu.Unlock()
		return
	}
	u.finishHandoffLocked(key)
	u.handoffNextLocked(key)
	u.mu.Unlock()
}

// removeWaiterLocked removes ch from key's waiter FIFO, reporting whether it was
// still registered. The caller holds u.mu.
func (u *unsavedOccurrences) removeWaiterLocked(key unsavedKey, ch chan struct{}) bool {
	ws := u.waiters[key]
	for i, w := range ws {
		if w == ch {
			u.waiters[key] = append(ws[:i], ws[i+1:]...)
			return true
		}
	}
	return false
}

// finishHandoffLocked records that one handed-off slot has settled and wakes any
// awaitSettled waiter. The caller holds u.mu.
func (u *unsavedOccurrences) finishHandoffLocked(key unsavedKey) {
	if u.handoffs[key] > 0 {
		u.handoffs[key]--
	}
	u.settled.Broadcast()
}

// handoffNextLocked hands the still-free slot for key to the next registered
// waiter, if any, so an abandoned handoff does not strand a later waiter. The
// caller holds u.mu and has just finished a handoff.
func (u *unsavedOccurrences) handoffNextLocked(key unsavedKey) {
	if _, ok := u.held[key]; ok {
		return
	}
	ws := u.waiters[key]
	if len(ws) == 0 {
		return
	}
	ch := ws[0]
	u.waiters[key] = ws[1:]
	u.handoffs[key]++
	close(ch)
}

// release drops o's entry after it was flushed to the outbox. It removes the
// entry only when it is still the SAME occurrence (so a stale release can never
// discard a newer entry) and hands the freed slot to the oldest same-schedule
// waiter, recording the handoff for awaitSettled.
func (u *unsavedOccurrences) release(o schedule.Occurrence) {
	key := unsavedKeyFor(o)
	u.mu.Lock()
	cur, ok := u.held[key]
	if ok && cur.ID() == o.ID() {
		delete(u.held, key)
		if ws := u.waiters[key]; len(ws) > 0 {
			ch := ws[0]
			u.waiters[key] = ws[1:]
			u.handoffs[key]++
			close(ch)
		}
	}
	u.mu.Unlock()
}

// awaitSettled blocks until every slot released for o's schedule has been taken
// up (recorded) or abandoned by its waiter. A draining flush calls it after each
// release so the next snapshot observes an occurrence the release admitted
// instead of racing its asynchronous admission.
func (u *unsavedOccurrences) awaitSettled(o schedule.Occurrence) {
	key := unsavedKeyFor(o)
	u.mu.Lock()
	for u.handoffs[key] > 0 {
		u.settled.Wait()
	}
	u.mu.Unlock()
}

// snapshot returns a copy of the currently held occurrences for a flush pass.
func (u *unsavedOccurrences) snapshot() []schedule.Occurrence {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]schedule.Occurrence, 0, len(u.held))
	for _, o := range u.held {
		out = append(out, o)
	}
	return out
}

// len reports how many occurrences are currently held.
func (u *unsavedOccurrences) len() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.held)
}

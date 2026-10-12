package cron

import (
	"context"
	"testing"
	"time"

	"relay/internal/schedule"
)

// unsavedSlotOcc builds an occurrence for one fixed schedule slot at the given
// instant, so several occurrences share an unsavedOccurrences key while remaining
// distinct occurrences.
func unsavedSlotOcc(instant time.Time) schedule.Occurrence {
	return schedule.Occurrence{App: "app-a", Schedule: "sched-a", Handler: "jobs.a", ScheduledAt: instant}
}

// gateFeeder installs an onWait seam that pauses every blocked retainer on its own
// fresh gate and hands that gate to the test, so retainers can be released one at
// a time deterministically (no sleeps, no reliance on select tie-breaking).
func gateFeeder(u *unsavedOccurrences) <-chan chan struct{} {
	gates := make(chan chan struct{}, 8)
	u.onWait = func(unsavedKey) {
		g := make(chan struct{})
		gates <- g
		<-g
	}
	return gates
}

// assertNoUnsavedLeaks fails when waiters or handoff counters remain for key.
func assertNoUnsavedLeaks(t *testing.T, u *unsavedOccurrences, key unsavedKey) {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if n := len(u.waiters[key]); n != 0 {
		t.Fatalf("waiter leak for %v = %d, want 0", key, n)
	}
	if n := u.handoffs[key]; n != 0 {
		t.Fatalf("handoff leak for %v = %d, want 0", key, n)
	}
}

// TestUnsavedHandoffCancelledWaiterChainsToNext pins handoff-settle branch (a): a
// waiter that was ALREADY handed the freed slot, then is cancelled before it
// consumes it, must not take the slot; the handoff is settled and chained to the
// next waiter in the SAME critical section, so awaitSettled (which flush relies
// on to re-snapshot) cannot observe a transient empty handoff count and return
// early. The cancellation is deterministic because a done context abandons a
// handed slot without a select tie-break.
func TestUnsavedHandoffCancelledWaiterChainsToNext(t *testing.T) {
	base := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	held := unsavedSlotOcc(base)
	first := unsavedSlotOcc(base.Add(time.Minute))
	second := unsavedSlotOcc(base.Add(2 * time.Minute))
	key := unsavedKeyFor(held)

	u := newUnsavedOccurrences()
	gates := gateFeeder(u)
	if !u.retain(context.Background(), held) {
		t.Fatal("held retain failed")
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	firstDone := make(chan bool, 1)
	go func() { firstDone <- u.retain(ctx1, first) }()
	g1 := <-gates

	secondDone := make(chan bool, 1)
	go func() { secondDone <- u.retain(context.Background(), second) }()
	g2 := <-gates

	// release hands the slot to the first waiter and counts a handoff.
	u.release(held)
	// The first waiter is cancelled before it consumes the handed slot.
	cancel1()
	close(g1)

	// awaitSettled must NOT return while the chained handoff to the second waiter
	// is unsettled. The second waiter is paused (its gate is open-closed below),
	// so it cannot consume yet.
	settled := make(chan struct{})
	go func() { u.awaitSettled(held); close(settled) }()
	select {
	case <-settled:
		t.Fatal("awaitSettled returned before the chained handoff settled")
	default:
	}

	if got := <-firstDone; got {
		t.Fatal("a cancelled waiter must not consume the handed slot")
	}
	// The abandoned slot was chained to the second waiter; releasing it admits
	// that waiter.
	close(g2)
	if got := <-secondDone; !got {
		t.Fatal("the chained waiter must be admitted")
	}
	<-settled

	if got := u.snapshot(); len(got) != 1 || got[0].ID() != second.ID() {
		t.Fatalf("held = %v, want the chained waiter's occurrence", got)
	}
	assertNoUnsavedLeaks(t, u, key)
}

// TestUnsavedFreshRetainerForcesWaiterToRewait pins handoff-settle branch (b): a
// fresh retainer takes the freed slot between release and the waiter's
// consumption, so the waiter must re-check (not overwrite) and re-wait; releasing
// the fresh occurrence then admits it, and a snapshot — what flush drains — sees
// the admitted intent.
func TestUnsavedFreshRetainerForcesWaiterToRewait(t *testing.T) {
	base := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	held := unsavedSlotOcc(base)
	waiter := unsavedSlotOcc(base.Add(time.Minute))
	fresh := unsavedSlotOcc(base.Add(2 * time.Minute))
	key := unsavedKeyFor(held)

	u := newUnsavedOccurrences()
	gates := gateFeeder(u)
	if !u.retain(context.Background(), held) {
		t.Fatal("held retain failed")
	}

	waiterDone := make(chan bool, 1)
	go func() { waiterDone <- u.retain(context.Background(), waiter) }()
	g1 := <-gates

	// release hands the slot to the waiter ...
	u.release(held)
	// ... but a fresh retainer takes the (now free) slot before the waiter
	// consumes its handed slot.
	if !u.retain(context.Background(), fresh) {
		t.Fatal("fresh retain failed")
	}

	// Consuming the handed slot must NOT overwrite the fresh occurrence: the
	// waiter re-checks, finds the slot occupied, and re-waits (a new gate).
	close(g1)
	g2 := <-gates
	if got := u.snapshot(); len(got) != 1 || got[0].ID() != fresh.ID() {
		t.Fatalf("held = %v, want the fresh occurrence intact", got)
	}

	// Releasing the fresh occurrence admits the re-waiting waiter; awaitSettled
	// waits for that chained handoff, so a snapshot (what flush drains) sees the
	// admitted intent.
	u.release(fresh)
	settled := make(chan struct{})
	go func() { u.awaitSettled(fresh); close(settled) }()
	close(g2)
	if got := <-waiterDone; !got {
		t.Fatal("the re-waiting waiter must eventually be admitted")
	}
	<-settled

	if got := u.snapshot(); len(got) != 1 || got[0].ID() != waiter.ID() {
		t.Fatalf("held = %v, want the admitted waiter", got)
	}
	assertNoUnsavedLeaks(t, u, key)
}

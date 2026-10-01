package cron

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/schedule"
)

// blockingPublisher models a Relay publisher callback wedged in a Redis-facing
// call: it ignores the gocron job context entirely and blocks until release.
// That is exactly the non-cooperative callback gocron's own bounded Shutdown
// can abandon (ErrStopJobsTimedOut) while it is still touching Redis.
type blockingPublisher struct {
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	calls    int
	returned chan struct{}
}

func newBlockingPublisher() *blockingPublisher {
	return &blockingPublisher{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (b *blockingPublisher) PublishOccurrence(_ context.Context, _ schedule.Occurrence) (bool, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	b.once.Do(func() { close(b.entered) })
	<-b.release // deliberately ignores ctx: a wedged Redis call
	close(b.returned)
	return true, nil
}

func (b *blockingPublisher) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// TestStopStrictlyJoinsInFlightCallbackAfterBound pins the audited race at the
// scheduler level: a Relay publisher callback is blocked in a Redis-facing call
// that ignores cancellation. Stop's bound expires and it returns the context
// error while the callback is STILL in flight, but a subsequent Stop (the
// worker barrier's unbounded second call) must not return until that callback
// has actually left the publisher. Only then may the caller tear Redis down.
//
// The sequence is channel-driven; the bounded selects are deadlock guards, not
// sequencing waits, and there are no sleeps.
func TestStopStrictlyJoinsInFlightCallbackAfterBound(t *testing.T) {
	fp := newBlockingPublisher()
	s := New(fp, testLogger())
	s.ReplaceApp("fn", schedTemplate("jobs.a", "0 3 * * *", "", ""))
	s.Start()

	fireNow(t, s, "fn/jobs.a")
	select {
	case <-fp.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher callback never entered its Redis-facing call")
	}

	// The bound expires while the callback is still wedged: Stop reports the
	// context error, exactly as the worker step's timeout path observes.
	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := s.Stop(stopCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop under expired bound = %v, want context.DeadlineExceeded", err)
	}
	if n := fp.callCount(); n != 1 {
		t.Fatalf("publisher calls = %d, want 1 (callback still in flight)", n)
	}

	// A second Stop (the barrier's unbounded join) must block until the
	// callback really returns, not merely until the first bound fired.
	joined := make(chan error, 1)
	go func() { joined <- s.Stop(context.Background()) }()
	select {
	case err := <-joined:
		t.Fatalf("Strict join returned %v while the callback was still in flight", err)
	case <-time.After(150 * time.Millisecond):
	}
	select {
	case <-fp.returned:
		t.Fatal("publisher callback returned before release")
	default:
	}

	close(fp.release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("Strict join = %v, want nil after the callback returned", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Strict join did not complete after the callback was released")
	}
	select {
	case <-fp.returned:
	case <-time.After(time.Second):
		t.Fatal("publisher callback did not return after release")
	}
}

// TestStopRefusesNewCallbacksAfterShutdown pins the admission guard: once Stop
// has closed the tracker, a task body that somehow reaches the scheduler cannot
// start a new Redis-facing publish.
func TestStopRefusesNewCallbacksAfterShutdown(t *testing.T) {
	var tracker callbackTracker
	if !tracker.begin() {
		t.Fatal("begin before close = false, want true")
	}
	tracker.close()
	if tracker.begin() {
		t.Fatal("begin after close = true, want false (no new callback admitted)")
	}
	select {
	case <-tracker.doneCh():
		t.Fatal("join completed while an admitted callback was still outstanding")
	default:
	}
	tracker.end()
	select {
	case <-tracker.doneCh():
	case <-time.After(2 * time.Second):
		t.Fatal("join did not complete after the admitted callback returned")
	}
}

// TestCallbackTrackerCloseIdempotentAndDoneStable pins that close is
// idempotent and that doneCh reports the same completion for every caller.
func TestCallbackTrackerCloseIdempotentAndDoneStable(t *testing.T) {
	var tracker callbackTracker
	first := func() <-chan struct{} { tracker.close(); return tracker.doneCh() }
	d1 := first()
	tracker.close()
	if d2 := tracker.doneCh(); d1 != d2 {
		t.Fatal("doneCh returned a different channel after a second close")
	}
	select {
	case <-d1:
	case <-time.After(2 * time.Second):
		t.Fatal("join did not complete with no callbacks admitted")
	}
}

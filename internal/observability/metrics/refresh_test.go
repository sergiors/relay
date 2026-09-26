package metrics

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeGaugeSource is a GaugeSource that counts Refresh calls. When block is
// non-nil, each Refresh signals entry on entered and then waits for either
// block to close or ctx cancellation, so tests can pin ordering and cancellation
// deterministically without sleeps.
type fakeGaugeSource struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	block   chan struct{}
}

func (f *fakeGaugeSource) Refresh(ctx context.Context) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.block == nil {
		return
	}
	select {
	case f.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
	case <-f.block:
	}
}

func (f *fakeGaugeSource) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// waitForCalls polls until the source has been refreshed at least n times or the
// deadline passes, returning whether it reached n. Bounded polling, never a
// blind sleep.
func (f *fakeGaugeSource) waitForCalls(n int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.count() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return f.count() >= n
}

// recordingSource records its own name into a shared, mutex-guarded sequence.
type recordingSource struct {
	seq  *[]string
	mu   *sync.Mutex
	name string
}

func (s *recordingSource) Refresh(context.Context) {
	s.mu.Lock()
	*s.seq = append(*s.seq, s.name)
	s.mu.Unlock()
}

// TestRefresherTicksAtInterval verifies Start invokes each source's Refresh once
// per tick, repeatedly, for a short interval.
func TestRefresherTicksAtInterval(t *testing.T) {
	src := &fakeGaugeSource{}
	ref := NewRefresher(2*time.Millisecond, src)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ref.Start(ctx) }()

	if !src.waitForCalls(3) {
		t.Fatalf("source refreshed %d times, want >= 3 within the deadline", src.count())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancellation")
	}
}

// TestRefresherInvokesSourcesInOrder verifies refreshAll invokes every source
// sequentially in registration order.
func TestRefresherInvokesSourcesInOrder(t *testing.T) {
	var seq []string
	var mu sync.Mutex
	ref := NewRefresher(time.Hour,
		&recordingSource{seq: &seq, mu: &mu, name: "a"},
		&recordingSource{seq: &seq, mu: &mu, name: "b"},
		&recordingSource{seq: &seq, mu: &mu, name: "c"},
	)
	ref.refreshAll(context.Background())

	want := []string{"a", "b", "c"}
	if len(seq) != len(want) {
		t.Fatalf("refresh sequence = %v, want %v", seq, want)
	}
	for i, name := range want {
		if seq[i] != name {
			t.Fatalf("refresh order = %v, want %v", seq, want)
		}
	}
}

// TestRefresherSkipsNilSource verifies nil entries in the source slice are
// skipped while non-nil sources still refresh.
func TestRefresherSkipsNilSource(t *testing.T) {
	src := &fakeGaugeSource{}
	ref := NewRefresher(time.Hour, nil, src, nil)
	ref.refreshAll(context.Background())
	if src.count() != 1 {
		t.Fatalf("non-nil source refreshed %d times, want 1 (nil entries skipped)", src.count())
	}
}

// TestRefresherCancellationBeforeFirstTick verifies Start returns promptly on an
// already-cancelled ctx without refreshing any source.
func TestRefresherCancellationBeforeFirstTick(t *testing.T) {
	src := &fakeGaugeSource{}
	ref := NewRefresher(time.Hour, src)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() { defer close(done); ref.Start(ctx) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after a pre-cancelled ctx")
	}
	if src.count() != 0 {
		t.Fatalf("source refreshed %d times before cancellation, want 0", src.count())
	}
}

// TestRefresherCancellationMidRefresh verifies cancelling ctx while a source's
// Refresh is in flight makes the source observe ctx cancellation and Start
// return.
func TestRefresherCancellationMidRefresh(t *testing.T) {
	src := &fakeGaugeSource{entered: make(chan struct{}, 1), block: make(chan struct{})}
	ref := NewRefresher(time.Millisecond, src)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ref.Start(ctx) }()

	select {
	case <-src.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("source never entered Refresh")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancelling a mid-refresh ctx")
	}
}

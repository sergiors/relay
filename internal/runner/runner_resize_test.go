package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// TestRunnerPerFunctionSemaphoreResizedLive proves a function's per-function
// semaphore is resized in place when its resolved template concurrency changes,
// without a restart: the new (smaller) bound is enforced on the next Handle
// using the already-registered function. The global cap is raised so the
// per-function bound is the binding one.
func TestRunnerPerFunctionSemaphoreResizedLive(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 40 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 4, exec)}, silentLogger(), nil)
	r.SetMaxConcurrency(16)

	// Warm the per-function semaphore at capacity 4.
	runConcurrent(t, r, 8)
	if got := exec.peakConcurrency(); got > 4 {
		t.Fatalf("initial peak = %d, want <= 4", got)
	}

	// Swap the function to concurrency 1 (same runner, no restart) and reset the
	// peak observation.
	r.Registry().Replace("f", fnWithConcurrency(t, "f", 1, exec))
	exec.resetPeak()

	runConcurrent(t, r, 8)
	if got := exec.peakConcurrency(); got != 1 {
		t.Fatalf("peak after live shrink = %d, want exactly 1 (semaphore resized)", got)
	}

	// Raise it to 3 and prove the new bound admits up to 3 concurrently.
	r.Registry().Replace("f", fnWithConcurrency(t, "f", 3, exec))
	exec.resetPeak()
	runConcurrent(t, r, 8)
	if got := exec.peakConcurrency(); got > 3 {
		t.Fatalf("peak after live increase = %d, want <= 3", got)
	}
	if got := exec.peakConcurrency(); got < 2 {
		t.Fatalf("peak after live increase = %d, want > 1 to prove resized admission", got)
	}
}

// TestRunnerSemaphoreResizeDoesNotStrandInFlight proves an in-flight invocation
// releases to the semaphore pointer it captured, so replacing the map entry on a
// resize never strands a held slot (a shrink cannot block a release on a full
// new channel). A holding invocation keeps the sole slot of a concurrency-1
// function; the function is then resized to a larger bound while it is in
// flight; both the original holder and later invocations must complete.
func TestRunnerSemaphoreResizeDoesNotStrandInFlight(t *testing.T) {
	release := make(chan struct{})
	holding := newHoldingExecutor(release)
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 1, holding)}, silentLogger(), nil)
	r.SetMaxConcurrency(16)

	firstDone := make(chan struct{})
	go func() {
		_ = r.Handle(context.Background(), "resize-inflight-1", map[string]any{"status": "ok"})
		close(firstDone)
	}()
	holding.waitEntered()

	// Resize the function while the holding invocation owns the old semaphore's
	// only slot.
	r.Registry().Replace("f", fnWithConcurrency(t, "f", 3, holding))

	// A second invocation must now fit (the new semaphore has spare capacity) and
	// complete independently of the first.
	secondDone := make(chan struct{})
	go func() {
		_ = r.Handle(context.Background(), "resize-inflight-2", map[string]any{"status": "ok"})
		close(secondDone)
	}()

	// Release the first invocation: it must release to its captured (old)
	// semaphore without blocking.
	close(release)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight invocation did not release after a semaphore resize")
	}
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("invocation after resize did not complete")
	}
}

// TestRunnerRemoveFunctionSemaphore proves removal drops a function's semaphore
// so a later recreation of the same name starts from the fresh bound rather than
// inheriting a stale one.
func TestRunnerRemoveFunctionSemaphore(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 20 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 5, exec)}, silentLogger(), nil)
	r.SetMaxConcurrency(16)

	// Warm the semaphore at 5.
	runConcurrent(t, r, 5)
	r.RemoveFunctionSemaphore("f")

	r.fnSemsMu.Lock()
	_, ok := r.fnSems["f"]
	r.fnSemsMu.Unlock()
	if ok {
		t.Fatal("RemoveFunctionSemaphore must drop the per-function semaphore")
	}

	// Recreate at 1: a fresh semaphore must enforce 1, not the stale 5.
	r.Registry().Replace("f", fnWithConcurrency(t, "f", 1, exec))
	exec.resetPeak()
	runConcurrent(t, r, 8)
	if got := exec.peakConcurrency(); got != 1 {
		t.Fatalf("peak after recreation = %d, want exactly 1 (fresh semaphore)", got)
	}
}

// TestRunnerPerFunctionSemaphoreRaceSafety hammers concurrent swaps with
// differing concurrency against concurrent Handles. Its value is under -race:
// the resize replaces map entries under fnSemsMu and each acquisition releases
// to its captured pointer.
func TestRunnerPerFunctionSemaphoreRaceSafety(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 2, exec)}, silentLogger(), nil)
	r.SetMaxConcurrency(16)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			r.Registry().Replace("f", fnWithConcurrency(t, "f", 1+(i%4), exec))
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 60; j++ {
				_ = r.Handle(context.Background(), "race", map[string]any{"status": "ok"})
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestRunnerSemaphoreResizeIsRegistryAuthoritative proves the resize target is
// read from the CURRENT registry entry, not a caller's stale snapshot: calling
// concurrencySems with an outdated fallback must not shrink a semaphore a newer
// registry entry already raised.
func TestRunnerSemaphoreResizeIsRegistryAuthoritative(t *testing.T) {
	exec := &concurrencyTrackingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 4, exec)}, silentLogger(), nil)

	// Install a semaphore at the current bound.
	_, s := r.concurrencySems("f", 4)
	if s.capacity != 4 {
		t.Fatalf("semaphore capacity = %d, want 4", s.capacity)
	}

	// A stale caller passes the old fallback 2 while the registry already says 4:
	// the semaphore must stay at 4, never be shrunk by the stale value.
	_, s = r.concurrencySems("f", 2)
	if s.capacity != 4 {
		t.Fatalf("semaphore capacity after stale fallback = %d, want 4 (registry authoritative)", s.capacity)
	}
}

// TestRunnerPerFunctionSemaphoreClipsToGlobal proves the per-function semaphore
// is sized to min(template concurrency, MAX_CONCURRENCY): a function asking for
// 15 under a global cap of 8 gets a capacity-8 semaphore (not 15), matching the
// runtime's clipped warm-pool bound. A later live reconcile to 4 resizes it to
// 4. This is the runner half of "function concurrency 15 with MAX_CONCURRENCY=8
// => effective live capacity 8".
func TestRunnerPerFunctionSemaphoreClipsToGlobal(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 40 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 15, exec)}, silentLogger(), nil)
	r.SetMaxConcurrency(8)

	// The installed semaphore capacity is the effective bound, not the raw 15.
	_, s := r.concurrencySems("f", 15)
	if s.capacity != 8 {
		t.Fatalf("per-function semaphore capacity = %d, want 8 (clipped to MAX_CONCURRENCY)", s.capacity)
	}

	// Admission agrees: 16 concurrent Handles never exceed the global cap of 8.
	runConcurrent(t, r, 16)
	if got := exec.peakConcurrency(); got != 8 {
		t.Fatalf("peak concurrency = %d, want exactly 8 (effective clip)", got)
	}

	// A live reconcile to concurrency 4 (below the cap) resizes to 4.
	r.Registry().Replace("f", fnWithConcurrency(t, "f", 4, exec))
	_, s = r.concurrencySems("f", 4)
	if s.capacity != 4 {
		t.Fatalf("per-function semaphore capacity after reconcile = %d, want 4", s.capacity)
	}
	exec.resetPeak()
	runConcurrent(t, r, 16)
	if got := exec.peakConcurrency(); got != 4 {
		t.Fatalf("peak concurrency after reconcile = %d, want exactly 4", got)
	}
}

// TestRunnerEffectiveConcurrencyClipsAndDefaults pins the clip rule directly: a
// value above the global cap is clipped, one below is untouched, a
// zero/negative template value falls back to the function default, and a
// zero-valued Runner's global falls back to DefaultMaxConcurrency.
func TestRunnerEffectiveConcurrencyClipsAndDefaults(t *testing.T) {
	r := NewWithMetrics(nil, silentLogger(), nil)
	r.SetMaxConcurrency(8)

	if got := r.effectiveConcurrency("abs", 15); got != 8 {
		t.Fatalf("effectiveConcurrency(15) = %d, want 8", got)
	}
	if got := r.effectiveConcurrency("abs", 4); got != 4 {
		t.Fatalf("effectiveConcurrency(4) = %d, want 4", got)
	}
	if got := r.effectiveConcurrency("abs", 0); got != function.DefaultConcurrency {
		t.Fatalf("effectiveConcurrency(0) = %d, want %d", got, function.DefaultConcurrency)
	}

	var zero Runner
	if got := zero.effectiveConcurrency("abs", 15); got != DefaultMaxConcurrency {
		t.Fatalf("zero Runner effectiveConcurrency(15) = %d, want default %d", got, DefaultMaxConcurrency)
	}
}

// resetPeak clears the recorded peak between observation phases.
func (f *concurrencyTrackingExecutor) resetPeak() {
	f.mu.Lock()
	f.peak = 0
	f.mu.Unlock()
}

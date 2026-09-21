package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// concurrencyTrackingExecutor records the peak concurrent executions, so a test
// can assert the runner's global and per-function concurrency caps are honored.
// Each execution blocks briefly so concurrent Handles overlap.
type concurrencyTrackingExecutor struct {
	mu       sync.Mutex
	running  int
	peak     int
	blockDur time.Duration
	calls    int
}

func (f *concurrencyTrackingExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	f.mu.Lock()
	f.running++
	if f.running > f.peak {
		f.peak = f.running
	}
	f.calls++
	block := f.blockDur
	f.mu.Unlock()
	select {
	case <-time.After(block):
	case <-ctx.Done():
	}
	f.mu.Lock()
	f.running--
	f.mu.Unlock()
	return nil
}

func (f *concurrencyTrackingExecutor) peakConcurrency() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

func (f *concurrencyTrackingExecutor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// runConcurrent fires n concurrent Handles against the runner and waits.
func runConcurrent(t *testing.T, r *Runner, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Handle(context.Background(), "conc-event", map[string]any{"status": "ok"})
		}()
	}
	wg.Wait()
}

// runTwo fires n concurrent Handles against each of this runner's two functions
// ("a" and "b", both always-matching) and waits.
func runTwo(t *testing.T, r *Runner, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = r.Handle(context.Background(), "conc-event", map[string]any{"status": "ok"})
		}()
		go func() {
			defer wg.Done()
			_ = r.Handle(context.Background(), "conc-event", map[string]any{"status": "ok"})
		}()
	}
	wg.Wait()
}

// TestRunnerGlobalConcurrencyDefault proves the default global cap (8) is
// enforced: 12 concurrent Handles against a blocking executor never exceed 8
// concurrent executions, and all complete.
func TestRunnerGlobalConcurrencyDefault(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 50 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 0, exec)}, testutil.DiscardLogger(), nil)

	runConcurrent(t, r, 12)

	if got := exec.peakConcurrency(); got > 8 {
		t.Fatalf("peak global concurrency = %d, want <= 8", got)
	}
	// Must have had real parallelism (not serialized).
	if got := exec.peakConcurrency(); got < 2 {
		t.Fatalf("peak global concurrency = %d, want > 1 to prove parallelism", got)
	}
	if got := exec.callCount(); got != 12 {
		t.Fatalf("executor calls = %d, want 12", got)
	}
}

// TestRunnerGlobalConcurrencyExplicit proves SetMaxConcurrency(2) caps global
// concurrency.
func TestRunnerGlobalConcurrencyExplicit(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 50 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 0, exec)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrency(2)

	runConcurrent(t, r, 12)

	if got := exec.peakConcurrency(); got > 2 {
		t.Fatalf("peak global concurrency = %d, want <= 2", got)
	}
	if got := exec.callCount(); got != 12 {
		t.Fatalf("executor calls = %d, want 12", got)
	}
}

// TestRunnerPerFunctionDefault proves the per-function default (2) is applied
// to a template whose Concurrency is 0 (unparsed): many concurrent Handles of a
// single function never exceed 2 concurrent executions per function.
func TestRunnerPerFunctionDefault(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 50 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 0, exec)}, testutil.DiscardLogger(), nil)
	// Raise the global cap so the per-function limit is the binding one.
	r.SetMaxConcurrency(16)

	runConcurrent(t, r, 16)

	if got := exec.peakConcurrency(); got > 2 {
		t.Fatalf("peak per-function concurrency = %d, want <= 2 (function default)", got)
	}
	if got := exec.callCount(); got != 16 {
		t.Fatalf("executor calls = %d, want 16", got)
	}
}

// TestRunnerPerFunctionExplicit1 proves an explicit per-function concurrency of
// 1 strictly serializes the function's executions.
func TestRunnerPerFunctionExplicit1(t *testing.T) {
	exec := &concurrencyTrackingExecutor{blockDur: 30 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 1, exec)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrency(8)

	runConcurrent(t, r, 8)

	if got := exec.peakConcurrency(); got != 1 {
		t.Fatalf("peak per-function concurrency = %d, want exactly 1 (serialized)", got)
	}
	if got := exec.callCount(); got != 8 {
		t.Fatalf("executor calls = %d, want 8", got)
	}
}

// TestRunnerGlobalSharedAcrossFunctions proves the global cap is shared across
// distinct functions: two functions firing 12 concurrent Handles against the
// same global cap of 2 never exceed 2 in total. A single shared executor
// measures the global peak (the union of both functions' executions).
func TestRunnerGlobalSharedAcrossFunctions(t *testing.T) {
	shared := &concurrencyTrackingExecutor{blockDur: 30 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithConcurrency(t, "a", 8, shared),
		fnWithConcurrency(t, "b", 8, shared),
	}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrency(2)

	runTwo(t, r, 12)

	// The shared executor's peak is the global peak across both functions.
	if got := shared.peakConcurrency(); got > 2 {
		t.Fatalf("global peak concurrency across functions = %d, want <= 2", got)
	}
	// 12 concurrent Handles on each of two functions both match every event, so
	// every Handle executes both functions: 12*2 Handles * 2 functions = 48 calls.
	if got := shared.callCount(); got != 48 {
		t.Fatalf("executor calls = %d, want 48 (12 Handles * 2 functions * 2 funcs each)", got)
	}
}

// TestRunnerPerFunctionIndependent proves each function's concurrency is
// independent: two functions each with concurrency 2 and a high-enough global
// cap (8) let both saturate to 2 concurrently, so the total can reach 4 while
// each stays within its own cap. One shared executor measures the union.
func TestRunnerPerFunctionIndependent(t *testing.T) {
	shared := &concurrencyTrackingExecutor{blockDur: 30 * time.Millisecond}
	r := NewWithMetrics([]*PreparedFunction{
		fnWithConcurrency(t, "a", 2, shared),
		fnWithConcurrency(t, "b", 2, shared),
	}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrency(8)

	runTwo(t, r, 8)

	// The union of both per-function slots can reach up to 2+2 = 4.
	if got := shared.peakConcurrency(); got > 4 {
		t.Fatalf("total peak concurrency across functions = %d, want <= 4 (2+2)", got)
	}
	// Both functions saturating must produce real overlap (>= 2).
	if got := shared.peakConcurrency(); got < 2 {
		t.Fatalf("total peak concurrency across functions = %d, want >= 2", got)
	}
}

// TestRunnerSlotTimeoutLeavesPending proves that when no concurrency slot frees
// within the (test-overridden) slot wait, Handle returns an
// ErrInvocationNotEligible-wrapped error (so the message stays pending at the
// stream layer) and the executor is NOT called for the timed-out invocation.
func TestRunnerSlotTimeoutLeavesPending(t *testing.T) {
	release := make(chan struct{})
	holding := newBlockingExecutor(release)
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 1, holding)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrency(1)
	// Test override (package-internal field): keep the wait short so the timeout
	// path is exercised quickly without a 30s wait.
	r.slotWait = 50 * time.Millisecond

	// First invocation acquires the single slot and blocks until release.
	firstDone := make(chan struct{})
	go func() {
		_ = r.Handle(context.Background(), "f-1", map[string]any{"status": "ok"})
		close(firstDone)
	}()
	holding.waitEntered()

	// Second Handle must time out waiting for the slot and return a wrapped
	// ErrInvocationNotEligible without executing.
	start := time.Now()
	err := r.Handle(context.Background(), "f-2", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected the second Handle to error (slot timeout -> not eligible)")
	}
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("second Handle error = %v, want wrapped ErrInvocationNotEligible", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("slot timeout took %v, want ~50ms (overridden slotWait)", elapsed)
	}
	// The executor must not have been called for the timed-out invocation.
	if got := holding.callCount(); got != 1 {
		t.Fatalf("executor calls = %d, want 1 (only the first, holding invocation)", got)
	}

	// Release the first invocation; it completes and Handle returns.
	close(release)
	<-firstDone
}

// TestRunnerConcurrencyWaitsCounter proves that a blocked slot acquisition
// increments the concurrency_waits_total counter and that the
// in_flight_invocations gauge returns to zero after the executions drain.
func TestRunnerConcurrencyWaitsCounter(t *testing.T) {
	m := metrics.New()
	release := make(chan struct{})
	exec := newBlockingExecutor(release)
	r := NewWithMetrics([]*PreparedFunction{fnWithConcurrency(t, "f", 1, exec)}, testutil.DiscardLogger(), m)
	r.SetMaxConcurrency(1)
	r.slotWait = 50 * time.Millisecond

	// First invocation holds the sole slot.
	firstDone := make(chan struct{})
	go func() {
		_ = r.Handle(context.Background(), "f-1", map[string]any{"status": "ok"})
		close(firstDone)
	}()
	exec.waitEntered()

	// A second invocation blocks (the slot is held) and times out; this counts a
	// wait even though it does not execute.
	_ = r.Handle(context.Background(), "f-2", map[string]any{"status": "ok"})
	if got := m.Counter(metrics.MetricConcurrencyWaits); got == 0 {
		t.Fatalf("concurrency_waits_total = %d, want >= 1 after a blocked acquire", got)
	}

	// Release the first invocation; once it completes, the in-flight gauge must
	// return to zero (all slots released).
	close(release)
	<-firstDone
	if got := m.Gauge(metrics.MetricInFlightInvocations); got != 0 {
		t.Fatalf("in_flight_invocations = %g, want 0 after drain", got)
	}
}

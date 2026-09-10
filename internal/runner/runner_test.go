package runner

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// Set and Replace keep the function set sorted by name, so Names() and iteration
// order never depend on the order functions were discovered or swapped in.
func TestRegistryNamesDeterministic(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))
	reg := r.Registry()

	// Inserting "zeta" before "alpha": the slice must come out sorted anyway.
	reg.Set([]*PreparedFunction{newFn(t, "zeta"), newFn(t, "alpha")})
	reg.Replace("mid", newFn(t, "mid"))
	reg.Replace("beta", newFn(t, "beta"))

	if got := strings.Join(reg.Names(), ","); got != "alpha,beta,mid,zeta" {
		t.Fatalf("Names order not deterministic: got %q", got)
	}

	// Removal keeps the remainder sorted.
	reg.Replace("alpha", nil)
	if got := strings.Join(reg.Names(), ","); got != "beta,mid,zeta" {
		t.Fatalf("Names order after removal: got %q", got)
	}
}

// Records invocations without touching Docker, satisfying the runner's local
// executor interface.
type fakeExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	// Yield inside the invocation so registry swaps can interleave; the runner
	// must keep using its snapshot regardless.
	time.Sleep(time.Millisecond)
	return nil
}

func newFn(t *testing.T, name string) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{Name: name, Template: &function.Template{Runtime: "node24"}},
		&runtime.Prepared{Name: name, Image: "x"},
		&fakeExecutor{},
	)
}

// Runs Handle concurrently with registry swaps under -race and asserts the
// runner neither panics nor errors while the set is being replaced mid-iteration.
func TestHandleVsSwapSnapshotConsistency(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))

	names := []string{"a", "b", "c"}
	var fns []*PreparedFunction
	for _, n := range names {
		fns = append(fns, newFn(t, n))
	}
	r.Registry().Set(fns)

	var swaps atomic.Int64
	stop := make(chan struct{})

	var swappers sync.WaitGroup
	for w := 0; w < 4; w++ {
		swappers.Add(1)
		go func() {
			defer swappers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Replace a single entry; occasionally mark it unavailable to
				// exercise the skip path.
				i := int(swaps.Add(1)) % len(names)
				if i%2 == 0 {
					r.Registry().Replace(names[i], NewUnavailable(function.Function{Name: names[i]}))
				} else {
					r.Registry().Replace(names[i], newFn(t, names[i]))
				}
			}
		}()
	}

	var running atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			running.Add(1)
			err := r.Handle(context.Background(), "m", map[string]any{"a": 1})
			running.Add(-1)
			if err != nil {
				t.Errorf("handle returned error: %v", err)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	swappers.Wait()
	<-done

	if swaps.Load() == 0 {
		t.Fatal("expected at least one swap during the test")
	}
}

// countingExecutor records how many times it was invoked, so a test can assert
// that a completed invocation is skipped (not executed) on redelivery.
type countingExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *countingExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil
}

func (f *countingExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// metaCaptureExecutor records the RunMeta the runner injects into ctx, so tests
// can assert the executor receives the diagnostic hostname/message/event labels
// without Docker.
type metaCaptureExecutor struct {
	mu   sync.Mutex
	meta runtime.RunMeta
}

func (m *metaCaptureExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	m.mu.Lock()
	m.meta = runtime.RunMetaFrom(ctx)
	m.mu.Unlock()
	return nil
}

func (m *metaCaptureExecutor) got() runtime.RunMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta
}

// TestHandleInjectsRunMeta verifies the runner stamps each invocation's
// diagnostic metadata (with the configured hostname) into the context the
// executor sees, without changing the Executor interface.
func TestHandleInjectsRunMeta(t *testing.T) {
	exec := &metaCaptureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	r.SetHostname("worker-9")

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"event_id": "evt_42", "event_name": "INSERT", "status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	want := runtime.RunMeta{
		Function:  "user-events",
		Handler:   "index.run",
		MessageID: "1757-0",
		EventID:   "evt_42",
		EventName: "INSERT",
		Hostname:  "worker-9",
		Image:     "x",
	}
	if got := exec.got(); got != want {
		t.Fatalf("RunMeta = %+v, want %+v", got, want)
	}
}

// TestHandleRunMetaEmptyHostnameWhenUnset verifies that not calling SetHostname
// yields an empty hostname label (never a panic) — the diagnostic hostname is
// best-effort.
func TestHandleRunMetaEmptyHostnameWhenUnset(t *testing.T) {
	exec := &metaCaptureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	// SetHostname deliberately NOT called.

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	meta := exec.got()
	if meta.Hostname != "" {
		t.Fatalf("expected empty hostname when SetHostname unset, got %q", meta.Hostname)
	}
	if meta.Function != "user-events" {
		t.Fatalf("expected Function set, got %q", meta.Function)
	}
}

// fakeInvocationState is an in-memory InvocationState for runner tests,
// avoiding a Redis dependency. It records which invocations have completed,
// which are protected by an active running deadline or a retry backoff, and
// which are exhausted, mirroring the real stream.invocationState semantics
// (TryStart/RecordFailure/MarkComplete/MarkExhausted/IsTerminal).
type fakeInvocationState struct {
	mu        sync.Mutex
	done      map[string]bool
	marks     []string
	running   map[string]time.Time // invocation -> running deadline
	nextAt    map[string]time.Time // invocation -> next-attempt deadline
	exhausted map[string]int       // invocation -> attempts
	attempts  map[string]int       // invocation -> highest attempt started
	now       func() time.Time
	// failures records the backoff passed to RecordFailure, for tests to assert
	// the retry schedule.
	failures []time.Duration
}

func newFakeInvocationState() *fakeInvocationState {
	return &fakeInvocationState{
		done:      map[string]bool{},
		running:   map[string]time.Time{},
		nextAt:    map[string]time.Time{},
		exhausted: map[string]int{},
		attempts:  map[string]int{},
		now:       time.Now,
	}
}

// setClock overrides the fake's clock so tests can freeze or advance time.
func (p *fakeInvocationState) setClock(now func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.now = now
}

// advance moves the fake's clock forward by d, so a retry backoff or running
// deadline that has not yet elapsed can be made to expire. It is a test hook
// mirroring the real store's injectable clock. Successive advances accumulate.
func (p *fakeInvocationState) advance(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prev := p.now
	p.now = func() time.Time { return prev().Add(d) }
}

func (p *fakeInvocationState) IsComplete(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[invocation]
}

func (p *fakeInvocationState) MarkComplete(invocation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[invocation] = true
	delete(p.running, invocation)
	delete(p.nextAt, invocation)
	delete(p.exhausted, invocation)
	p.marks = append(p.marks, invocation)
}

// TryStart claims the invocation for a new execution unless it is complete,
// exhausted, or protected by an active running deadline or retry backoff. It
// mirrors the real store's read-then-write semantics, returning the 1-based
// attempt number and the wait until eligible when not started.
func (p *fakeInvocationState) TryStart(invocation string, timeout time.Duration) (started bool, attempt int, wait time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done[invocation] {
		return false, 0, 0
	}
	if n, ok := p.exhausted[invocation]; ok {
		return false, n, 0
	}
	if dl, ok := p.running[invocation]; ok && p.now().Before(dl) {
		return false, p.attempts[invocation], dl.Sub(p.now())
	}
	if dl, ok := p.nextAt[invocation]; ok && p.now().Before(dl) {
		return false, p.attempts[invocation], dl.Sub(p.now())
	}
	// Eligible: start the next attempt (1 + the highest attempt so far).
	attempt = p.attempts[invocation] + 1
	p.attempts[invocation] = attempt
	p.running[invocation] = p.now().Add(timeout)
	return true, attempt, 0
}

// RecordFailure records the retry backoff for a failed attempt, gating the
// invocation until now+backoff.
func (p *fakeInvocationState) RecordFailure(invocation string, backoff time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures = append(p.failures, backoff)
	delete(p.running, invocation)
	p.nextAt[invocation] = p.now().Add(backoff)
}

// MarkExhausted records that the invocation's attempts are exhausted.
func (p *fakeInvocationState) MarkExhausted(invocation string, attempts int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.running, invocation)
	delete(p.nextAt, invocation)
	p.exhausted[invocation] = attempts
}

// IsTerminal reports whether the invocation is complete or exhausted.
func (p *fakeInvocationState) IsTerminal(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[invocation] || p.exhausted[invocation] > 0
}

// runningDeadline returns the persisted running deadline for an invocation, for
// tests to inspect the fake's internals.
func (p *fakeInvocationState) runningDeadline(invocation string) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	dl, ok := p.running[invocation]
	return dl, ok
}

// TestHandleSkipsCompletedInvocation verifies that when invocation state is
// present in ctx and an invocation already completed, Handle skips it: the
// executor is not called and no success/failure metrics are recorded for it.
func TestHandleSkipsCompletedInvocation(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), m)

	// First delivery: no invocation state, so the handler runs and records success.
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}

	// Second delivery with invocation state marking the invocation already
	// complete: the handler must be skipped (not executed) and not counted as a
	// success.
	prog := newFakeInvocationState()
	prog.done["user-events/index.run"] = true
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (skipped)", exec.count())
	}

	got := m.Snapshot()
	// The skip must not add a second success invocation.
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=success} count=1") {
		t.Errorf("expected exactly one success invocation; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Errorf("unexpected failure metric; got:\n%s", got)
	}
}

// TestHandleMarksCompleteOnExecution verifies that a successful execution
// records the invocation via MarkComplete so a later redelivery can skip it.
func TestHandleMarksCompleteOnExecution(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(prog.marks) != 1 || prog.marks[0] != "user-events/index.run" {
		t.Fatalf("marks = %v, want [user-events/index.run]", prog.marks)
	}
	if !prog.IsComplete("user-events/index.run") {
		t.Fatalf("invocation should be marked complete")
	}
}

// TestHandleNoInvocationStateBehavesAsBefore verifies that Handle without
// invocation state in ctx runs every matching handler (nil-safe, no state
// bookkeeping).
func TestHandleNoInvocationStateBehavesAsBefore(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

// ctxAwareExecutor blocks until the invocation context is done and returns its
// error, so a test can observe the deadline the runner actually imposed.
type ctxAwareExecutor struct{}

func (ctxAwareExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	<-ctx.Done()
	return ctx.Err()
}

// fnWithTimeout builds a prepared function whose single rule matches any event
// and carries the given handler timeout and the default retry count.
func fnWithTimeout(t *testing.T, name string, timeout time.Duration, executor Executor) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: timeout, Retries: function.DefaultRetries}},
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// TestSetMaxHandlerTimeoutCapsRuleTimeout verifies that a configured max
// handler timeout caps a rule's (larger) timeout: the executor observes the
// capped deadline and Handle returns a deadline error.
func TestSetMaxHandlerTimeoutCapsRuleTimeout(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", time.Hour, ctxAwareExecutor{})}, silentLogger(), nil)
	r.SetMaxHandlerTimeout(50 * time.Millisecond)

	start := time.Now()
	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the capped handler")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	// The handler must have been cut off near the cap, not the 1h rule timeout.
	if elapsed > 2*time.Second {
		t.Fatalf("handler ran %s, expected it to be capped near 50ms", elapsed)
	}
}

// TestSetMaxHandlerTimeoutUncappedByDefault verifies that with no cap set, the
// rule's own (small) timeout is honored and not shortened to some default: the
// executor observes a deadline at least as long as the rule timeout.
func TestSetMaxHandlerTimeoutUncappedByDefault(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", 200*time.Millisecond, ctxAwareExecutor{})}, silentLogger(), nil)
	// SetMaxHandlerTimeout deliberately NOT called (cap stays 0 = uncapped).

	start := time.Now()
	err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the rule timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	// The observed deadline must be the rule's own 200ms, not shortened to 50ms.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("handler returned after %s, expected it to run the full 200ms rule timeout", elapsed)
	}
}

// TestHandleTryStartGuardsInFlightInvocation verifies that when an invocation is
// protected by an active running deadline (a future deadline), Handle skips the
// executor: another replica may be executing it, so it must not run concurrently.
// Because nothing executed and the invocation is protected, Handle returns
// ErrInvocationNotEligible so the stream layer leaves the message pending (the
// cross-replica ACK-hazard fix).
func TestHandleTryStartGuardsInFlightInvocation(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	// Freeze the clock and mark the invocation running with a future deadline.
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	prog.running["user-events/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle error = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (in-flight invocation skipped)", exec.count())
	}
}

// TestHandleExecutesAfterDeadlineExpires verifies that once an invocation's
// running deadline has passed, Handle executes it again (the marker is stale).
func TestHandleExecutesAfterDeadlineExpires(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark running with a deadline that has already passed.
	prog.running["user-events/index.run"] = now.Add(-time.Second)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (expired deadline is eligible)", exec.count())
	}
}

// TestHandleFailureSchedulesRetry verifies that a failing invocation records a
// retry backoff (RecordFailure) instead of clearing the marker: the invocation
// is gated until the backoff elapses, so an immediate second delivery is skipped
// (not eligible) rather than re-run.
func TestHandleFailureSchedulesRetry(t *testing.T) {
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// First delivery fails: a retry backoff is recorded (not an endRunning).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected failure")
	}
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after failure")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure", prog.failures)
	}
	if prog.failures[0] != time.Minute {
		t.Fatalf("first retry backoff = %s, want 1m", prog.failures[0])
	}

	// Second delivery (immediate, no clock advance): the invocation is gated by
	// the retry backoff, so it is not eligible and Handle returns
	// ErrInvocationNotEligible.
	exec.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("second delivery error = %v, want ErrInvocationNotEligible (backoff gating)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (backoff-gated retry skipped)", exec.count())
	}
}

// TestHandleTimeoutSchedulesRetry verifies that a handler that times out
// (executor returns ctx.Err) causes Handle to error and RecordFailure to be
// called, scheduling a retry backoff.
func TestHandleTimeoutSchedulesRetry(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithTimeout(t, "user-events", 50*time.Millisecond, ctxAwareExecutor{})}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected a deadline error from the timed-out handler")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after timeout")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure after timeout", prog.failures)
	}
}

// TestHandleMarshalErrorSchedulesRetry verifies that when json.Marshal fails
// after TryStart has already claimed the invocation, Handle records a retry
// backoff (RecordFailure) so the deterministic marshal failure is treated as a
// failed attempt. The event carries a value json.Marshal cannot encode (a chan),
// which errors with "unsupported type".
func TestHandleMarshalErrorSchedulesRetry(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// json.Marshal of a map containing a chan value errors ("unsupported type"),
	// so Handle fails after TryStart has claimed the invocation.
	event := map[string]any{"bad": make(chan int)}
	if err := r.Handle(ctx, "1757-0", event); err == nil {
		t.Fatal("expected a marshal error")
	}
	// The running marker must be cleared and a retry backoff recorded.
	if _, ok := prog.runningDeadline("user-events/index.run"); ok {
		t.Fatalf("running marker should be cleared after marshal error")
	}
	if len(prog.failures) != 1 {
		t.Fatalf("failures = %v, want one RecordFailure after marshal error", prog.failures)
	}
	// The executor must never have been reached (marshal failed before handoff).
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (marshal failed before execution)", exec.count())
	}
}

// bufferLogger returns a logger that captures output into a buffer, so tests can
// assert on the structured log lines the runner emits (e.g. the panic log).
func bufferLogger() (*log.Logger, *strings.Builder) {
	var b strings.Builder
	return log.New(&b, "", 0), &b
}

// TestHandleExecutorPanicTreatedAsFailedAttempt verifies that an executor panic
// is converted into a normal failed attempt: with retries:0 the invocation is
// exhausted after the single panicking attempt (IsTerminal), the failure metrics
// are counted, and Handle returns an error wrapping the panic instead of letting
// it escape. This mirrors TestHandleRetriesZeroExhaustsAfterOneAttempt but with
// a panicking executor.
func TestHandleExecutorPanicTreatedAsFailedAttempt(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 0, panicExecutor{})}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, want ErrInvocationExhausted", err)
	}
	if !strings.Contains(err.Error(), "executor panic") {
		t.Fatalf("error = %v, want it to mention the executor panic", err)
	}
	if !prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should be terminal (exhausted) after a panicking attempt")
	}
	if len(prog.failures) != 0 {
		t.Fatalf("failures = %v, want none (exhausted, not retried)", prog.failures)
	}
	// The panic must be counted as a failed invocation, not a success.
	got := m.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=failure} count=1") {
		t.Errorf("expected one failure invocation; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=success") {
		t.Errorf("unexpected success metric; got:\n%s", got)
	}
}

// TestHandleExecutorPanicSchedulesRetry verifies that a panicking execution is
// retryable: with the default retry count, the first panic records a retry
// backoff (RecordFailure with retryBackoff(1)=1m) and Handle returns a plain
// (non-exhausted) error; after advancing the clock, a second panicking attempt
// records the 2m backoff. This proves a panic flows through the same
// retry/exhaustion machinery as any other failure.
func TestHandleExecutorPanicSchedulesRetry(t *testing.T) {
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", function.DefaultRetries, panicExecutor{})}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1: panics → retryable, records a 1m backoff.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 1 error = %v, want a plain (retryable) error", err)
	}
	if len(prog.failures) != 1 || prog.failures[0] != time.Minute {
		t.Fatalf("failures after attempt 1 = %v, want [1m]", prog.failures)
	}
	if prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should NOT be terminal after a retryable panic")
	}

	// Advance past the 1m backoff; attempt 2 panics → 2m backoff.
	prog.advance(retryBackoff(1))
	err = r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 2 error = %v, want a plain (retryable) error", err)
	}
	if len(prog.failures) != 2 || prog.failures[1] != 2*time.Minute {
		t.Fatalf("failures after attempt 2 = %v, want [1m 2m]", prog.failures)
	}
}

// TestHandlePanicLogsStack verifies that a panicking execution emits a dedicated
// panic log line containing the panic value and a stack frame marker, so the
// bug is visible and attributable.
func TestHandlePanicLogsStack(t *testing.T) {
	logger, buf := bufferLogger()
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 0, panicExecutor{})}, logger, nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected Handle to return an error for the panicking execution")
	}
	out := buf.String()
	if !strings.Contains(out, "PANICKED") {
		t.Fatalf("log does not contain the PANICKED marker:\n%s", out)
	}
	if !strings.Contains(out, "executor boom") {
		t.Fatalf("log does not contain the panic value:\n%s", out)
	}
	// The stack must be present (a goroutine/panic frame marker).
	if !strings.Contains(out, "goroutine") && !strings.Contains(out, "runtime/panic") {
		t.Fatalf("log does not contain a stack frame marker:\n%s", out)
	}
}

package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// doneObservingContext signals the FIRST time its Done method is observed. The
// runner's concurrency semaphore fast path never touches ctx, and Handle
// consults no other ctx.Done() before the semaphore acquire, so the first
// signal deterministically proves the waiter entered the semaphore's BLOCKING
// select — the exact "waiting on a slot" moment — without any sleep.
type doneObservingContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *doneObservingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

// startBarrierState wraps the in-memory InvocationState with a channel barrier
// around TryStart, so a test can hold the claim at its exact boundary and
// deterministically land a cancellation at/after the successful claim. It is the
// "cancellation racing TryStart" seam: the delegate ignores ctx (like a Redis
// script that committed before it observed cancellation), so the claim wins and
// the runner must PRESERVE it rather than roll it back.
type startBarrierState struct {
	*fakeInvocationState
	entered chan struct{}
	release chan struct{}
}

func (s *startBarrierState) TryStart(invocation string, timeout time.Duration) (bool, stream.InvocationClaim, time.Duration, error) {
	close(s.entered)
	<-s.release
	return s.fakeInvocationState.TryStart(invocation, timeout)
}

// assertNoRunnerLeak asserts that a canceled delivery stranded no concurrency
// slot (global or per-app) and left the in-flight gauge at zero. It is the
// "no resource leaks" companion to the cancellation tests for those three
// runner-owned resources.
//
// It does NOT observe the warm-container budget: the runner keeps no permit
// accounting of its own, so there is nothing here to read. The warm permit
// acquired before TryStart is released by the deferred
// runtime.WarmPermitFrom(...).Release() on the app and schedule paths (and
// consumed/released by the container Execute creates); that accounting is
// guaranteed by those defers and covered by the runtime warm-budget tests, not
// this helper.
func assertNoRunnerLeak(t *testing.T, r *Runner) {
	t.Helper()
	if got := r.inFlight.Load(); got != 0 {
		t.Fatalf("in_flight_invocations = %d, want 0 (slot leaked)", got)
	}
	if g := r.globalSem.Load(); g != nil {
		if got := len(g.slots); got != 0 {
			t.Fatalf("global semaphore occupancy = %d, want 0 (slot leaked)", got)
		}
	}
	r.fnSemsMu.Lock()
	defer r.fnSemsMu.Unlock()
	for name, s := range r.fnSems {
		if got := len(s.slots); got != 0 {
			t.Fatalf("app %q semaphore occupancy = %d, want 0 (slot leaked)", name, got)
		}
	}
}

// assertNoCharge asserts the invocation claimed no attempt and recorded no
// failure or exhaustion marker. It is the PRE-claim shutdown contract: the
// cancellation lands before execution, so nothing reached the handler and no
// failure telemetry is possible either. A POST-claim cancellation is different:
// the runner's failure telemetry is still incremented for the executor error
// before recordFailure's guard suppresses the persisted transition, so those
// cases assert persisted state directly (see the ...NoRetryOrExhaustionTransition
// tests below), not with this helper.
func assertNoCharge(t *testing.T, prog *fakeInvocationState, invocation string) {
	t.Helper()
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if got := prog.attempts[invocation]; got != 0 {
		t.Fatalf("handler attempts = %d, want 0 (no claim)", got)
	}
	if len(prog.failures) != 0 {
		t.Fatalf("recorded failures = %v, want none", prog.failures)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none", prog.exhausted)
	}
	if len(prog.marks) != 0 {
		t.Fatalf("completions = %v, want none", prog.marks)
	}
}

// TestRunnerPreClaimCancellationSkipsClaim pins the event path's pre-claim
// cancellation check: a delivery whose lifecycle context is already canceled
// still acquires (the semaphore's immediate fast path and the warm admitter do
// not consult ctx), but the runner must NOT call TryStart. The message is
// reported not-eligible (so the stream leaves it pending), no attempt is
// claimed, no handler runs, and every acquired resource is released.
func TestRunnerPreClaimCancellationSkipsClaim(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{fnWithConcurrency(t, "fn", 1, exec)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)

	prog := newFakeInvocationState()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled BEFORE admission
	ctx = stream.WithInvocationState(ctx, prog)

	err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle under a canceled lifecycle = %v, want ErrInvocationNotEligible", err)
	}
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	assertNoCharge(t, prog, "fn/index.run")
	assertNoRunnerLeak(t, r)
}

// TestRunnerPreClaimCancellationWhileSlotBlocked pins the "while waiting"
// cancellation case: a delivery blocked on the runner's concurrency semaphore
// returns promptly on cancellation without claiming an attempt and without
// stranding the slot the first invocation still holds. The semaphore is full, so
// the fast path cannot admit even before the blocking wait; and the waiter is
// synchronized to be INSIDE the blocked select (via doneObservingContext) before
// canceling, so the prompt return is deterministically cancellation — not a
// timeout and not "cancel happened to land before the wait began".
func TestRunnerPreClaimCancellationWhileSlotBlocked(t *testing.T) {
	release := make(chan struct{})
	holding := newBlockingExecutor(release)
	r := NewWithMetrics([]*PreparedApp{fnWithConcurrency(t, "fn", 1, holding)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)
	// Long slotWait so the prompt return cannot be a timeout.
	r.slotWait = 10 * time.Second

	firstDone := make(chan struct{})
	go func() {
		_ = r.Handle(context.Background(), "first", map[string]any{"status": "ok"})
		close(firstDone)
	}()
	holding.waitEntered() // the sole global+per-app slot is now held

	base, cancel := context.WithCancel(context.Background())
	watch := &doneObservingContext{Context: base, entered: make(chan struct{})}
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(watch, prog)

	errCh := make(chan error, 1)
	go func() { errCh <- r.Handle(ctx, "second", map[string]any{"status": "ok"}) }()

	// Synchronize on the waiter entering the blocked semaphore select BEFORE
	// canceling, so the cancellation demonstrably lands while it is waiting.
	select {
	case <-watch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("second Handle never entered the blocked semaphore select")
	}
	// Cancel; the blocked acquire must observe ctx.Done rather than burn slotWait.
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("blocked Handle after cancel = %v, want ErrInvocationNotEligible", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Handle did not return promptly on cancellation")
	}
	if got := holding.callCount(); got != 1 {
		t.Fatalf("executor calls = %d, want 1 (only the holding invocation)", got)
	}
	assertNoCharge(t, prog, "fn/index.run")

	// The holding invocation still owns its slot; release it and assert the
	// runner drains to zero with no leaked slot from the canceled delivery.
	close(release)
	<-firstDone
	assertNoRunnerLeak(t, r)
}

// TestRunnerWarmAdmissionCancellationLeavesPending pins the warm-budget half of
// the same "while waiting" case: a delivery blocked inside warm-budget admission
// returns promptly on lifecycle cancellation (rather than waiting out
// warmAdmitWait), leaves the message pending WITHOUT claiming an handler
// attempt, and releases the concurrency slots it already held. warmWait is long,
// so the prompt return itself proves ctx (not a timeout) ended the wait.
func TestRunnerWarmAdmissionCancellationLeavesPending(t *testing.T) {
	exec := &warmAdmitterExecutor{
		admitEntered: make(chan struct{}),
		admitBlock:   make(chan struct{}), // never closed: only cancellation unblocks
	}
	r := NewWithMetrics([]*PreparedApp{fnWithConcurrency(t, "fn", 1, exec)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)
	r.warmWait = 10 * time.Second // prompt return cannot be a timeout

	prog := newFakeInvocationState()
	ctx, cancel := context.WithCancel(context.Background())
	ctx = stream.WithInvocationState(ctx, prog)

	errCh := make(chan error, 1)
	go func() { errCh <- r.Handle(ctx, "m-1", map[string]any{"status": "ok"}) }()
	<-exec.admitEntered // the runner is blocked inside AcquireWarmPermit
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("Handle under warm-admission cancellation = %v, want ErrInvocationNotEligible", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handle did not return promptly when warm admission was canceled")
	}
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	assertNoCharge(t, prog, "fn/index.run")
	assertNoRunnerLeak(t, r)
}

// TestRunnerCancellationAtTryStartPreservesClaim pins the race the fix must NOT
// roll back: a cancellation that lands while the claim is being confirmed and a
// successful claim commits must leave that claim in place. The claimed attempt is
// spent (the accepted claim-before-execute window), but no retry/backoff
// transition and no exhausted marker is written, and the running marker stays
// recoverable, so a later delivery claims the next attempt.
func TestRunnerCancellationAtTryStartPreservesClaim(t *testing.T) {
	// ctxAwareExecutor returns immediately with ctx.Err() once the (already
	// canceled) invocation context is observed.
	r := NewWithMetrics([]*PreparedApp{fnWithConcurrency(t, "fn", 1, ctxAwareExecutor{})}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)

	base := newFakeInvocationState()
	prog := &startBarrierState{
		fakeInvocationState: base,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = stream.WithInvocationState(ctx, prog)

	errCh := make(chan error, 1)
	go func() { errCh <- r.Handle(ctx, "m-1", map[string]any{"status": "ok"}) }()

	// The runner reached TryStart (pre-claim check passed). Cancel now, then let
	// the claim commit: this is the "cancellation races an already-started Redis
	// transition" window.
	<-prog.entered
	cancel()
	close(prog.release)

	select {
	case err := <-errCh:
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("Handle = %v, want ErrInvocationNotEligible", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handle did not return after the claim committed")
	}

	base.mu.Lock()
	attempt := base.attempts["fn/index.run"]
	failures := len(base.failures)
	exhausted := len(base.exhausted)
	base.mu.Unlock()
	if attempt != 1 {
		t.Fatalf("handler attempt = %d, want 1 (a committed claim is preserved, never rolled back)", attempt)
	}
	if failures != 0 || exhausted != 0 {
		t.Fatalf("failures=%d exhausted=%d, want 0/0 (no retry/exhaustion transition)", failures, exhausted)
	}
	if _, ok := base.runningDeadline("fn/index.run"); !ok {
		t.Fatal("running deadline absent after cancellation; want the claim left recoverable")
	}
	assertNoRunnerLeak(t, r)
}

// TestRunnerCancellationDuringExecuteNoRetryOrExhaustionTransition pins the F-003
// core against PERSISTED invocation state: a handler that is already running when
// the lifecycle is canceled returns a cancellation error, and the runner must
// leave the invocation pending WITHOUT issuing a retry/backoff transition, an
// exhausted marker, or a DLQ entry. The claimed attempt (1) is spent and its
// running deadline remains, so the message is recoverable rather than
// prematurely terminal.
//
// Failure TELEMETRY is deliberately outside this guarantee and IS asserted here
// to keep the distinction explicit: the executor error reaches the failure branch
// and increments handler_failure_total before recordFailure's cancellation guard
// is consulted, so the counter moves even though the persisted retry/exhaustion
// state does not. The test locks both halves.
func TestRunnerCancellationDuringExecuteNoRetryOrExhaustionTransition(t *testing.T) {
	release := make(chan struct{})
	exec := newBlockingExecutor(release)
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{fnWithConcurrency(t, "fn", 1, exec)}, testutil.DiscardLogger(), m)
	r.SetMaxConcurrentInvocations(1)

	prog := newFakeInvocationState()
	ctx, cancel := context.WithCancel(context.Background())
	ctx = stream.WithInvocationState(ctx, prog)

	errCh := make(chan error, 1)
	go func() { errCh <- r.Handle(ctx, "m-1", map[string]any{"status": "ok"}) }()
	exec.waitEntered() // claim confirmed, handler running
	cancel()           // lifecycle shutdown mid-execution

	select {
	case err := <-errCh:
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("Handle = %v, want ErrInvocationNotEligible", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Handle did not return after cancellation")
	}

	prog.mu.Lock()
	attempt := prog.attempts["fn/index.run"]
	failures := len(prog.failures)
	exhausted := len(prog.exhausted)
	prog.mu.Unlock()
	if attempt != 1 {
		t.Fatalf("handler attempt = %d, want 1 (claim spent, not rolled back)", attempt)
	}
	if failures != 0 || exhausted != 0 {
		t.Fatalf("failures=%d exhausted=%d, want 0/0 (no persisted retry/exhaustion transition)", failures, exhausted)
	}
	if _, ok := prog.runningDeadline("fn/index.run"); !ok {
		t.Fatal("running deadline absent after cancellation; want the claim left recoverable")
	}
	// The persisted state is untouched, but the cancellation error still reached
	// the failure branch's telemetry before recordFailure's guard: this is the
	// exact persisted-state-vs-telemetry distinction the fix documents.
	if got := m.Counter(metrics.MetricHandlerFailure); got != 1 {
		t.Fatalf("handler_failure_total = %d, want 1 (post-claim execution telemetry is still counted)", got)
	}
	assertNoRunnerLeak(t, r)
}

// TestRunnerSchedulePreClaimCancellationSkipsClaim is the schedule path's
// pre-claim cancellation: the occurrence must not be admitted (no descriptor
// pinned, no attempt claimed) and stays pending.
func TestRunnerSchedulePreClaimCancellationSkipsClaim(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{schedFn(t, "fn", exec, time.Second)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)

	prog := newFakeInvocationState()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = stream.WithInvocationState(ctx, prog)

	err := r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`))
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("InvokeHandler under a canceled lifecycle = %v, want ErrInvocationNotEligible", err)
	}
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	assertNoCharge(t, prog, "fn/index.run")
	assertNoRunnerLeak(t, r)
}

// TestRunnerScheduleCancellationDuringExecuteNoRetryOrExhaustionTransition is the
// schedule path's core F-003 case against PERSISTED invocation state: an admitted
// occurrence canceled mid-execution stays pending with its attempt spent and no
// retry/backoff transition, exhausted marker, or DLQ entry, and its pinned
// descriptor remains so the occurrence completes its lifecycle on a later
// delivery.
//
// As on the event path, failure TELEMETRY is outside the guarantee and is
// asserted: invokeOnce counts handler_failure_total for the post-claim executor
// error before InvokeHandler's recordFailure guard suppresses the persisted
// transition.
func TestRunnerScheduleCancellationDuringExecuteNoRetryOrExhaustionTransition(t *testing.T) {
	release := make(chan struct{})
	exec := newBlockingExecutor(release)
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{schedFn(t, "fn", exec, time.Second)}, testutil.DiscardLogger(), m)
	r.SetMaxConcurrentInvocations(1)

	prog := newFakeInvocationState()
	ctx, cancel := context.WithCancel(context.Background())
	ctx = stream.WithInvocationState(ctx, prog)

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`))
	}()
	exec.waitEntered()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("InvokeHandler = %v, want ErrInvocationNotEligible", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InvokeHandler did not return after cancellation")
	}

	prog.mu.Lock()
	attempt := prog.attempts["fn/index.run"]
	failures := len(prog.failures)
	exhausted := len(prog.exhausted)
	pinned := prog.scheduleDesc != nil
	prog.mu.Unlock()
	if attempt != 1 {
		t.Fatalf("handler attempt = %d, want 1 (claim spent, not rolled back)", attempt)
	}
	if failures != 0 || exhausted != 0 {
		t.Fatalf("failures=%d exhausted=%d, want 0/0 (no persisted retry/exhaustion transition)", failures, exhausted)
	}
	if !pinned {
		t.Fatal("schedule descriptor was not pinned by the successful admission")
	}
	if _, ok := prog.runningDeadline("fn/index.run"); !ok {
		t.Fatal("running deadline absent after cancellation; want the claim left recoverable")
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 1 {
		t.Fatalf("handler_failure_total = %d, want 1 (post-claim execution telemetry is still counted)", got)
	}
	assertNoRunnerLeak(t, r)
}

// TestRunnerRealTimeoutStillCharges pins the counterfactual: the cancellation
// gate keys on the PARENT lifecycle context, so a genuine handler timeout on a
// live delivery (the invocation's own context deadline elapsing) is still a
// failed attempt that records a retry. Without this, the fix could silently
// swallow real timeouts.
func TestRunnerRealTimeoutStillCharges(t *testing.T) {
	// The executor blocks until its invocation context times out (the rule's
	// 20ms timeout), returning context.DeadlineExceeded. The parent ctx stays
	// alive, so this is a real failed attempt.
	exec := &countingExecutor{hold: time.Hour}
	prepared := buildFn(fnSpec{name: "fn", rules: []app.EventRule{alwaysMatchRule(20 * time.Millisecond)}}, exec)
	r := NewWithMetrics([]*PreparedApp{prepared}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "m-1", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("Handle on a timed-out handler = nil, want a retryable error")
	}
	if errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle = %v, want a recorded retry (not a protected skip)", err)
	}
	prog.mu.Lock()
	defer prog.mu.Unlock()
	if len(prog.failures) != 1 {
		t.Fatalf("recorded failures = %v, want exactly one retry backoff", prog.failures)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none after the first failed attempt", prog.exhausted)
	}
	assertNoRunnerLeak(t, r)
}

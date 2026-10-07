package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// These tests pin the F-002 claim-before-execute boundary: the persisted handler
// attempt (the count that drives retry/exhaustion) is claimed by TryStart, which
// runs AFTER the concurrency slots and the warm-budget admission but BEFORE the
// event is serialized and the executor is dispatched. So:
//
//   - a capacity rejection BEFORE the claim (a concurrency-slot timeout, or a
//     saturated warm-budget admission) records no attempt and charges no
//     retry/DLQ — the delivery is pure backpressure;
//   - a crash AFTER the confirmed claim but before the handler runs spends the
//     claimed attempt: the persisted running marker's deadline elapses and a
//     later delivery claims the NEXT attempt.
//
// Crucially, TryStart claims an attempt but does NOT itself exhaust: it carries
// no retry budget and never writes an exhausted marker. A crash ALONE therefore
// cannot dead-letter a message — it only advances the persisted attempt count.
// Exhaustion (and so the DLQ) still requires a later real execution that fails
// once the persisted count has reached 1+retries. Repeated lost claims simply
// make that eventual real failure exhaust with a larger persisted attempt count
// (see TestHandleRepeatedUnexecutedClaimsAdvanceAttemptUntilRealFailureExhausts).
//
// No production hook is placed between TryStart and runInvocation so a test can
// "crash" exactly there. In production those two points are adjacent except for
// serialization/secret resolution, whose failures are REAL failed attempts
// charged through recordFailure, and an actual process crash is not injectable;
// a test-only hook in the per-invocation hot path is not justified. The window
// is instead demonstrated from both sides with existing seams: the post-crash
// state below is produced by a real TryStart (the same InvocationState interface
// Handle calls), the claim is observed at executor entry
// (TestHandleClaimsAttemptBeforeExecutorRuns), and the create-time warm-budget
// saturation path (TestRunnerWarmBudgetCreateSaturationLeavesPendingWithoutCharge)
// confirms a claim through the real Handle flow and then abandons execution
// without running the handler. The store transition that TryStart performs is
// pinned separately in internal/stream
// (TestInvocationTryStartNeverExhaustsAcrossRepeatedElapsedClaims,
// TestIntegrationAtomicTryStartAttemptOnce,
// TestIntegrationAtomicCrashReclaimAfterDeadline).

// claimAfterCrash performs a real, successful TryStart through the InvocationState
// interface — the same call Handle makes — to produce the exact state a process
// crash immediately after a confirmed claim (and before the handler runs) leaves
// behind. It deliberately does not seed the fake's internal maps: the attempt,
// running deadline, and claim token all come from TryStart itself.
func claimAfterCrash(t *testing.T, prog *fakeInvocationState, invocation string, timeout time.Duration) {
	t.Helper()
	started, claim, _, err := prog.TryStart(invocation, timeout)
	if err != nil {
		t.Fatalf("TryStart crash seed: %v", err)
	}
	if !started {
		t.Fatalf("TryStart crash seed did not claim %s (started=false)", invocation)
	}
	if claim.Attempt != 1 || claim.Token == "" {
		t.Fatalf("TryStart crash seed claim = %+v, want attempt 1 with a token", claim)
	}
}

// admitAfterCrash is claimAfterCrash for the schedule path: it pins the admitted
// descriptor and claims attempt 1 atomically through TryStartScheduled (the same
// call InvokeHandler makes), producing the post-admission crash state.
func admitAfterCrash(t *testing.T, prog *fakeInvocationState, desc stream.ScheduleDescriptor) {
	t.Helper()
	admission, err := prog.TryStartScheduled(desc, true, func(d stream.ScheduleDescriptor) string {
		return "fn/" + d.Handler
	})
	if err != nil {
		t.Fatalf("TryStartScheduled crash seed: %v", err)
	}
	if !admission.Started || admission.Claim.Attempt != 1 {
		t.Fatalf("TryStartScheduled crash seed = %+v, want a confirmed attempt-1 admission", admission)
	}
}

// TestHandleClaimsAttemptBeforeExecutorRuns pins the boundary itself from the
// executor's perspective: at the moment the executor is entered, the handler
// attempt is already persisted (claimed by TryStart), proving the claim is the
// attempt boundary rather than the executor dispatch.
func TestHandleClaimsAttemptBeforeExecutorRuns(t *testing.T) {
	prog := newFakeInvocationState()
	exec := &stateProbeExecutor{probe: func() (bool, bool) {
		_, ok := prog.runningDeadline("fn/index.run")
		return ok, ok
	}}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "fn", exec)}, testutil.DiscardLogger(), nil)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	calls, runningAt := exec.got()
	if calls != 1 {
		t.Fatalf("executor calls = %d, want 1", calls)
	}
	if !runningAt {
		t.Fatalf("the running marker was not persisted before the executor ran")
	}
	if got := prog.attempts["fn/index.run"]; got != 1 {
		t.Fatalf("attempt at execution = %d, want 1 (TryStart is the attempt boundary)", got)
	}
	if !prog.IsComplete("fn/index.run") {
		t.Fatalf("invocation should be complete after a successful execution")
	}
}

// TestHandleCrashAfterClaimSkipsUntilDeadlineThenExecutes models a process crash
// between the confirmed claim and the executor start. The post-crash state is
// produced by a real TryStart (not hand-seeded): the running marker (attempt 1)
// is persisted and no transition ever follows. A redelivery while the running
// deadline is still active must skip the invocation (protected) WITHOUT calling
// the handler; once the deadline elapses the claim is eligible again and the NEXT
// attempt runs and completes.
func TestHandleCrashAfterClaimSkipsUntilDeadlineThenExecutes(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", app.DefaultRetries, exec)}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// A real confirmed claim, then no transition: the crash-after-claim state.
	claimAfterCrash(t, prog, "fn/index.run", time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Redelivery within the crash window: protected, handler not called, attempt
	// unchanged, no failure/retry/exhaustion accounting.
	err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("redelivery within the crash window = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls within the crash window = %d, want 0", exec.count())
	}
	if got := prog.attempts["fn/index.run"]; got != 1 {
		t.Fatalf("attempt within the crash window = %d, want 1 (unchanged)", got)
	}
	if len(prog.failures) != 0 || len(prog.exhausted) != 0 {
		t.Fatalf("failures=%v exhausted=%v, want none (a protected skip is not a failure)", prog.failures, prog.exhausted)
	}

	// Let the claimed running deadline elapse: the invocation is eligible again and
	// the next claim crosses to attempt 2, which executes and completes.
	prog.advance(2 * time.Hour)
	if err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("redelivery after the deadline = %v, want nil", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls after the deadline = %d, want 1", exec.count())
	}
	if got := prog.attempts["fn/index.run"]; got != 2 {
		t.Fatalf("attempt after the crash window = %d, want 2 (the unexecuted claim was spent)", got)
	}
	if !prog.IsComplete("fn/index.run") {
		t.Fatalf("invocation should be complete after the post-crash attempt")
	}
}

// TestHandlePostCrashFailureExhaustsWithInflatedAttemptCount is the F-002
// failure mode, showing precisely that the crash is NOT what dead-letters: with
// retries:0 (one allowed attempt), a real TryStart spends attempt 1 unexecuted;
// the next, REAL delivery claims attempt 2 and, because it actually FAILS,
// exhausts the invocation. The crash only inflated the persisted count to 2 — the
// post-crash failing execution is what writes the terminal marker and drives the
// DLQ. The handler ran exactly once, yet the exhausted attempt count is 2.
func TestHandlePostCrashFailureExhaustsWithInflatedAttemptCount(t *testing.T) {
	exec := &countingExecutor{fail: true}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 0, exec)}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// A real confirmed claim of attempt 1, then no transition: the crash state.
	claimAfterCrash(t, prog, "fn/index.run", time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	// The protected redelivery does not execute.
	if err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"}); !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("redelivery within the crash window = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls within the crash window = %d, want 0", exec.count())
	}

	// Deadline elapses; attempt 2 runs and fails. retries:0 => maxAttempts 1, so
	// attempt 2 exhausts even though the handler ran only once.
	prog.advance(2 * time.Hour)
	err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"})
	var exhausted *stream.HandlerExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("post-crash failure = %v (%T), want *stream.HandlerExhaustedError", err, err)
	}
	if len(exhausted.Invocations) != 1 || exhausted.Invocations[0].Attempts != 2 {
		t.Fatalf("exhausted invocations = %+v, want one with attempts 2", exhausted.Invocations)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (the crash consumed an attempt without executing)", exec.count())
	}
	if !prog.IsTerminal("fn/index.run") {
		t.Fatalf("invocation should be terminal (exhausted)")
	}
}

// TestHandlePreClaimCapacityRejectionDoesNotSpendAttempt pins the other side of
// the boundary with metrics: when the warm-budget admission is saturated BEFORE
// the claim, Handle leaves the delivery pending and charges neither a retry nor a
// DLQ, and no handler counter moves — the attempt set stays empty.
func TestHandlePreClaimCapacityRejectionDoesNotSpendAttempt(t *testing.T) {
	m := metrics.New()
	exec := &warmAdmitterExecutor{admitErr: errors.New("warm budget saturated")}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", app.DefaultRetries, exec)}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("Handle under pre-claim saturation = %v, want ErrInvocationNotEligible", err)
	}
	if exec.calls != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.calls)
	}
	prog.mu.Lock()
	attempts := len(prog.attempts)
	prog.mu.Unlock()
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (pre-claim rejection records nothing)", attempts)
	}
	if got := m.Counter(metrics.MetricHandlerSuccess) + m.Counter(metrics.MetricHandlerFailure); got != 0 {
		t.Fatalf("handler execution counters = %d, want 0", got)
	}
	fs := m.AppStatsSnapshot()
	for _, s := range fs {
		if s.RetriesTotal != 0 || s.DLQTotal != 0 {
			t.Fatalf("app stats = %+v, want zero retries and DLQ for a pre-claim rejection", s)
		}
	}
}

// TestInvokeHandlerClaimsAttemptBeforeExecutorRuns pins the same boundary on the
// schedule path: the first admission atomically pins the descriptor and claims the
// attempt, and the executor observes the persisted running marker at entry.
func TestInvokeHandlerClaimsAttemptBeforeExecutorRuns(t *testing.T) {
	prog := newFakeInvocationState()
	exec := &stateProbeExecutor{probe: func() (bool, bool) {
		_, ok := prog.runningDeadline("fn/index.run")
		return ok, ok
	}}
	r := NewWithMetrics([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, app.DefaultRetries)}, testutil.DiscardLogger(), nil)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "1-0", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	calls, runningAt := exec.got()
	if calls != 1 {
		t.Fatalf("executor calls = %d, want 1", calls)
	}
	if !runningAt {
		t.Fatalf("the running marker was not persisted before the executor ran")
	}
	if got := prog.attempts["fn/index.run"]; got != 1 {
		t.Fatalf("attempt at execution = %d, want 1", got)
	}
}

// TestInvokeHandlerCrashAfterClaimWindowSpendsAttempt pins the schedule-path
// F-002 window: a crash after the admitted, confirmed claim leaves a running
// marker with attempt 1; a protected redelivery does not execute, and once the
// deadline elapses the next attempt runs. The unexecuted claim was spent.
func TestInvokeHandlerCrashAfterClaimWindowSpendsAttempt(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, app.DefaultRetries)}, testutil.DiscardLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// A real atomic schedule admission (descriptor pinned + attempt 1 claimed),
	// then no transition: the crash-after-admission state.
	admitAfterCrash(t, prog, stream.ScheduleDescriptor{
		Schedule: "sched", Handler: "index.run", Timeout: time.Second, Retries: app.DefaultRetries,
	})
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "1-0", "fn", "sched", "index.run", []byte(`{}`)); !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("redelivery within the crash window = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls within the crash window = %d, want 0", exec.count())
	}

	prog.advance(2 * time.Hour)
	if err := r.InvokeHandler(ctx, "1-0", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("redelivery after the deadline = %v, want nil", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls after the deadline = %d, want 1", exec.count())
	}
	if got := prog.attempts["fn/index.run"]; got != 2 {
		t.Fatalf("attempt after the crash window = %d, want 2 (the unexecuted claim was spent)", got)
	}
}

// interruptThenFailExecutor models repeated interruptions inside a running
// Handle: the first `saturate` Execute calls return the create-time warm-budget
// sentinel (runtime.ErrWarmBudgetSaturated) — the deterministic, no-sleep way the
// real Handle flow confirms a TryStart claim and then abandons the attempt
// WITHOUT running any handler code (Handle maps the sentinel to a pending skip,
// charging no failure/retry/DLQ). Once the interruptions are spent it delegates
// to countingExecutor, so the next confirmed attempt actually runs the handler
// and (with fail=true) fails.
type interruptThenFailExecutor struct {
	countingExecutor
	saturateMu sync.Mutex
	saturate   int
}

func (e *interruptThenFailExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, eventJSON []byte, env []string) error {
	e.saturateMu.Lock()
	if e.saturate > 0 {
		e.saturate--
		e.saturateMu.Unlock()
		return runtime.ErrWarmBudgetSaturated
	}
	e.saturateMu.Unlock()
	return e.countingExecutor.Execute(ctx, prepared, handler, eventJSON, env)
}

// TestHandleRepeatedUnexecutedClaimsAdvanceAttemptUntilRealFailureExhausts pins
// the repeated-interruption behavior of the claim-before-execute window with a
// real Handle flow and a frozen clock (no sleeps). Each interrupted delivery
// performs a fresh, confirmed TryStart (through the runner's normal admission
// path) and then abandons the attempt before the handler runs, so the persisted
// attempt advances 1, 2, 3 while the message stays unresolved (Handle returns
// ErrInvocationNotEligible), the handler never runs, and nothing is charged.
//
// Only the final delivery actually executes: with retries:0 (maxAttempts 1) the
// already-inflated persisted count makes that one real failure exhaust, so the
// typed *stream.HandlerExhaustedError (the DLQ driver) reports attempts=4. This
// is the point of the semantics: the crashes did not dead-letter — they raised
// the attempt count until a real failing execution did.
func TestHandleRepeatedUnexecutedClaimsAdvanceAttemptUntilRealFailureExhausts(t *testing.T) {
	const interruptions = 3
	m := metrics.New()
	exec := &interruptThenFailExecutor{saturate: interruptions}
	exec.countingExecutor.fail = true // the eventual real attempt fails
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 0, exec)}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	ctx := stream.WithInvocationState(context.Background(), prog)

	for i := 1; i <= interruptions; i++ {
		err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"})
		if !errors.Is(err, stream.ErrInvocationNotEligible) {
			t.Fatalf("interrupted delivery %d = %v, want ErrInvocationNotEligible (message pending)", i, err)
		}
		if got := prog.attempts["fn/index.run"]; got != i {
			t.Fatalf("persisted attempt after interruption %d = %d, want %d", i, got, i)
		}
		if exec.count() != 0 {
			t.Fatalf("handler executions after interruption %d = %d, want 0", i, exec.count())
		}
		if prog.IsTerminal("fn/index.run") {
			t.Fatalf("invocation terminal after interruption %d; an unexecuted claim must not exhaust", i)
		}
		if len(prog.failures) != 0 || len(prog.exhausted) != 0 {
			t.Fatalf("failures=%v exhausted=%v after interruption %d, want none", prog.failures, prog.exhausted, i)
		}
		// Let the claimed running deadline elapse so the next delivery is eligible
		// and claims the NEXT attempt (the same real-time elapse a crash waits out).
		prog.advance(time.Hour)
	}

	// The first delivery to actually run the handler fails; retries:0 => the
	// persisted attempt (4) is past the budget, so this real failure exhausts.
	err := r.Handle(ctx, "1-0", map[string]any{"status": "ok"})
	var exhausted *stream.HandlerExhaustedError
	if !errors.As(err, &exhausted) {
		t.Fatalf("first real attempt after interruptions = %v (%T), want *stream.HandlerExhaustedError", err, err)
	}
	if exec.count() != 1 {
		t.Fatalf("handler executions = %d, want 1 (only the final attempt ran user code)", exec.count())
	}
	if len(exhausted.Invocations) != 1 || exhausted.Invocations[0].Attempts != interruptions+1 {
		t.Fatalf("exhausted invocations = %+v, want one with attempts %d", exhausted.Invocations, interruptions+1)
	}
	if !prog.IsTerminal("fn/index.run") {
		t.Fatalf("invocation should be terminal (exhausted) after the real failure")
	}
	// The interruptions charged no retries; the DLQ point is the real exhaustion.
	appLabel := []metrics.Label{{Name: "app", Value: "fn"}}
	if got := m.CounterLabels(metrics.MetricFunctionRetries, appLabel); got != 0 {
		t.Fatalf("retries charged during the interruptions = %d, want 0", got)
	}
	if got := m.CounterLabels(metrics.MetricFunctionDLQ, appLabel); got != 1 {
		t.Fatalf("DLQ charge = %d, want 1 (only the real failure exhausted)", got)
	}
}

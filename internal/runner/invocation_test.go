package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// scriptedExecutor records how many times it was invoked and can be told to
// fail on demand. It is the per-function executor used by the redelivery tests:
// each function gets its own instance so call counts and failure behavior are
// independent.
type scriptedExecutor struct {
	mu    sync.Mutex
	calls int
	fail  bool
}

func (s *scriptedExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	s.mu.Lock()
	s.calls++
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return fmt.Errorf("boom")
	}
	return nil
}

func (s *scriptedExecutor) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedExecutor) setFail(f bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = f
}

// TestHandleSkipsCompletedInvocationsOnRedelivery is the core redelivery
// scenario: a message matching two functions is delivered, one invocation
// succeeds and one fails; on redelivery the succeeded one is skipped (its
// executor is not called again) while the failed one is retried; once it
// succeeds, a third delivery skips both and Handle returns nil (so the stream
// layer would ACK).
func TestHandleSkipsCompletedInvocationsOnRedelivery(t *testing.T) {
	alpha := &scriptedExecutor{}
	beta := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", alpha),
		alwaysMatchFn(t, "beta", beta),
	}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: alpha succeeds, beta fails. Handle returns an error.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected delivery 1 to fail (beta)")
	}
	if !prog.IsComplete("alpha/index.run") {
		t.Errorf("alpha should be marked after success")
	}
	if prog.IsComplete("beta/index.run") {
		t.Errorf("beta should NOT be marked after failure")
	}
	if alpha.count() != 1 || beta.count() != 1 {
		t.Fatalf("delivery1 calls: alpha=%d beta=%d, want 1/1", alpha.count(), beta.count())
	}

	// Delivery 2 (redelivery): alpha skipped, beta retried and now succeeds. The
	// retry backoff from delivery 1 must have elapsed for beta to be eligible.
	prog.advance(2 * time.Minute)
	beta.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("delivery 2: %v", err)
	}
	if alpha.count() != 1 {
		t.Errorf("alpha should be skipped on redelivery (calls=%d, want 1)", alpha.count())
	}
	if beta.count() != 2 {
		t.Errorf("beta should be retried (calls=%d, want 2)", beta.count())
	}
	if !prog.IsComplete("beta/index.run") {
		t.Errorf("beta should be marked after delivery-2 success")
	}

	// Delivery 3: both skipped, Handle returns nil (stream would ACK).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("delivery 3: %v", err)
	}
	if alpha.count() != 1 || beta.count() != 2 {
		t.Fatalf("delivery3 calls: alpha=%d beta=%d, want 1/2", alpha.count(), beta.count())
	}
}

// TestHandleMultiHandlerFirstFailsSecondSucceeds is the primary aggregate
// regression test: a message matching two handlers where the FIRST (by sorted
// name) fails and the SECOND succeeds in the SAME delivery. The failure of A
// must not prevent B from running; Handle returns a plain (retryable) error so
// the message stays pending; B is marked complete and A is not. On a later
// delivery A is retried (B skipped, not re-run) and, once A succeeds, a further
// delivery skips both and returns nil (the stream would ACK).
func TestHandleMultiHandlerFirstFailsSecondSucceeds(t *testing.T) {
	a := &scriptedExecutor{fail: true} // sorts first, fails delivery 1
	b := &scriptedExecutor{}           // sorts second, succeeds always
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", a), // sorts before "beta"
		alwaysMatchFn(t, "beta", b),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: handler A fails yet handler B still executes and succeeds.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil {
		t.Fatal("expected delivery 1 to return an error (A failed)")
	}
	if errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("delivery 1 error = %v, want a plain (not not-eligible) error", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("delivery 1 error = %v, want a plain (not exhausted) error", err)
	}
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("delivery1 calls: alpha=%d beta=%d, want 1/1 (B must still run)", a.count(), b.count())
	}
	if prog.IsComplete("alpha/index.run") {
		t.Errorf("alpha should NOT be marked after failure")
	}
	if !prog.IsComplete("beta/index.run") {
		t.Errorf("beta should be marked after success")
	}

	// Delivery 2 (advance past A's 1m backoff): A is retried and now succeeds;
	// B is skipped (executor not called again). Handle returns nil (stream ACKs).
	prog.advance(2 * time.Minute)
	a.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("delivery 2: %v", err)
	}
	if a.count() != 2 {
		t.Errorf("alpha calls = %d, want 2 (retried)", a.count())
	}
	if b.count() != 1 {
		t.Errorf("beta calls = %d, want 1 (skipped on redelivery)", b.count())
	}
	if !prog.IsComplete("alpha/index.run") {
		t.Errorf("alpha should be marked after delivery-2 success")
	}

	// Delivery 3: both skipped, nil, no extra executor calls.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("delivery 3: %v", err)
	}
	if a.count() != 2 || b.count() != 1 {
		t.Fatalf("delivery3 calls: alpha=%d beta=%d, want 2/1 (both skipped)", a.count(), b.count())
	}
}

// TestHandleSkippedInvocationsDoNotCountMetrics verifies that a skipped
// invocation (already completed on a previous delivery) is not counted as an
// execution: handler_success_total and function_handler_success_total only
// count real executions, while function_events_total still counts the function
// as engaged on every match (attribution, not execution).
func TestHandleSkippedInvocationsDoNotCountMetrics(t *testing.T) {
	m := metrics.New()
	alpha := &scriptedExecutor{}
	beta := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", alpha),
		alwaysMatchFn(t, "beta", beta),
	}, silentLogger(), m)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: alpha succeeds, beta fails.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected delivery 1 to fail")
	}
	// Delivery 2: alpha skipped, beta succeeds. The retry backoff from delivery 1
	// must have elapsed for beta to be eligible.
	prog.advance(2 * time.Minute)
	beta.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("delivery 2: %v", err)
	}

	if got := m.Counter("handler_success_total"); got != 2 {
		t.Errorf("handler_success_total = %d, want 2 (one alpha + one beta, not the alpha skip)", got)
	}
	if got := m.Counter("handler_failure_total"); got != 1 {
		t.Errorf("handler_failure_total = %d, want 1", got)
	}
	if got := m.Counter("events_received_total"); got != 2 {
		t.Errorf("events_received_total = %d, want 2", got)
	}

	fs := m.FunctionStatsSnapshot()
	byName := map[string]metrics.FunctionStat{}
	for _, f := range fs {
		byName[f.Function] = f
	}
	if byName["alpha"].HandlerSuccessTotal != 1 {
		t.Errorf("alpha success = %d, want 1 (not 2; the skip is not an execution)", byName["alpha"].HandlerSuccessTotal)
	}
	if byName["alpha"].Events != 2 {
		t.Errorf("alpha events = %d, want 2 (counted on each match)", byName["alpha"].Events)
	}
	if byName["beta"].Events != 2 {
		t.Errorf("beta events = %d, want 2 (counted on each match)", byName["beta"].Events)
	}
}

// TestHandleMarksStateOnlyAfterSuccess verifies that a failing invocation is
// never marked, and a later success marks it.
func TestHandleMarksStateOnlyAfterSuccess(t *testing.T) {
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Failure: not marked.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected failure")
	}
	if prog.IsComplete("user-events/index.run") {
		t.Errorf("invocation must not be marked after failure")
	}
	if len(prog.marks) != 0 {
		t.Errorf("marks = %v, want none after failure", prog.marks)
	}

	// Later success: marked. The retry backoff from the failure must have elapsed.
	prog.advance(2 * time.Minute)
	exec.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("success: %v", err)
	}
	if !prog.IsComplete("user-events/index.run") {
		t.Errorf("invocation should be marked after success")
	}
}

// TestHandleWithoutStateUnchanged verifies that with no invocation state in
// ctx, every matching handler runs on every Handle call, nothing is marked,
// and there is no panic.
func TestHandleWithoutStateUnchanged(t *testing.T) {
	a := &scriptedExecutor{}
	b := &scriptedExecutor{}
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", a),
		alwaysMatchFn(t, "beta", b),
	}, silentLogger(), nil)

	for i := 0; i < 2; i++ {
		if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
			t.Fatalf("handle %d: %v", i, err)
		}
	}
	if a.count() != 2 || b.count() != 2 {
		t.Fatalf("calls: alpha=%d beta=%d, want 2/2", a.count(), b.count())
	}
}

// TestHandleMultipleFunctionsIndependentState verifies that each function's
// invocation state is tracked independently across redeliveries. Now that
// Handle aggregates outcomes instead of failing fast, ALL matching functions
// are attempted on every eligible delivery regardless of the others' outcomes:
// a failure in one handler no longer prevents the later, sorted ones from
// running.
func TestHandleMultipleFunctionsIndependentState(t *testing.T) {
	a := &scriptedExecutor{}           // A: always succeeds
	c := &scriptedExecutor{fail: true} // C: fails delivery 1, succeeds delivery 2
	z := &scriptedExecutor{fail: true} // Z: always fails
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "A", a),
		alwaysMatchFn(t, "C", c),
		alwaysMatchFn(t, "Z", z),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: each independent invocation gets its own attempt — A succeeds
	// (marked), C and Z each fail (separately). Handle returns the first plain
	// error (Z sorts last but its failure is aggregated, not fail-fast).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected delivery 1 to fail")
	}
	if !prog.IsComplete("A/index.run") {
		t.Errorf("A should be marked")
	}
	if prog.IsComplete("C/index.run") {
		t.Errorf("C should not be marked after failure")
	}
	if prog.IsComplete("Z/index.run") {
		t.Errorf("Z should not be marked")
	}
	if a.count() != 1 || c.count() != 1 || z.count() != 1 {
		t.Fatalf("delivery1 calls: A=%d C=%d Z=%d, want 1/1/1 (all independently attempted)", a.count(), c.count(), z.count())
	}

	// Delivery 2: A skipped, C retried (succeeds, marked), Z retried (fails). The
	// retry backoffs from delivery 1 must have elapsed for C and Z to be eligible.
	prog.advance(2 * time.Minute)
	c.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected delivery 2 to fail (Z)")
	}
	if a.count() != 1 {
		t.Errorf("A calls = %d, want 1 (skipped)", a.count())
	}
	if c.count() != 2 {
		t.Errorf("C calls = %d, want 2 (retried)", c.count())
	}
	if z.count() != 2 {
		t.Errorf("Z calls = %d, want 2 (retried)", z.count())
	}
	if !prog.IsComplete("C/index.run") {
		t.Errorf("C should be marked after success")
	}
	if prog.IsComplete("Z/index.run") {
		t.Errorf("Z should still be unmarked")
	}

	// Delivery 3: A+C skipped, Z retried (fails). Z's backoff from delivery 2
	// must have elapsed.
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected delivery 3 to fail (Z)")
	}
	if a.count() != 1 {
		t.Errorf("A calls = %d, want 1 (skipped)", a.count())
	}
	if c.count() != 2 {
		t.Errorf("C calls = %d, want 2 (skipped)", c.count())
	}
	if z.count() != 3 {
		t.Errorf("Z calls = %d, want 3 (retried)", z.count())
	}
}

// TestRetryBackoffSchedule pins the fixed retry backoff schedule: attempt 1 →
// 1m, 2 → 2m, 3 → 5m, 4+ → 10m (capped).
func TestRetryBackoffSchedule(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Minute},
		{2, 2 * time.Minute},
		{3, 5 * time.Minute},
		{4, 10 * time.Minute},
		{5, 10 * time.Minute},
		{10, 10 * time.Minute},
	}
	for _, tc := range cases {
		if got := retryBackoff(tc.attempt); got != tc.want {
			t.Errorf("retryBackoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

// fnWithRetries builds a prepared function whose single rule matches any event
// and carries the given retry count (additional attempts after the first).
func fnWithRetries(t *testing.T, name string, retries int, executor Executor) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: time.Second, Retries: retries}},
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// TestHandleRetriesZeroExhaustsAfterOneAttempt verifies that a rule with
// retries:0 (only the initial attempt) exhausts after a single failure: the
// invocation is marked exhausted and Handle returns ErrInvocationExhausted (the
// sole invocation is terminal).
func TestHandleRetriesZeroExhaustsAfterOneAttempt(t *testing.T) {
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 0, exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, want ErrInvocationExhausted", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
	if !prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should be terminal (exhausted) after retries:0 failure")
	}
	if len(prog.failures) != 0 {
		t.Fatalf("failures = %v, want none (exhausted, not retried)", prog.failures)
	}
}

// TestHandleDefaultRetriesFiveAttempts verifies that a rule with the default
// retry count (4) allows 5 total attempts before exhaustion, with the backoff
// schedule 1m/2m/5m/10m applied after attempts 1..4.
func TestHandleDefaultRetriesFiveAttempts(t *testing.T) {
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", function.DefaultRetries, exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempts 1..4 are retryable (each schedules a backoff); attempt 5 exhausts.
	for i := 1; i <= 4; i++ {
		err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
		if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
			t.Fatalf("attempt %d: expected a retryable failure, got %v", i, err)
		}
		// Advance past the just-scheduled backoff so the next attempt is eligible.
		prog.advance(retryBackoff(i))
	}
	if exec.count() != 4 {
		t.Fatalf("executor calls after 4 retryable attempts = %d, want 4", exec.count())
	}
	// Attempt 5 exhausts.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 5 error = %v, want ErrInvocationExhausted", err)
	}
	if exec.count() != 5 {
		t.Fatalf("executor calls = %d, want 5", exec.count())
	}
	if !prog.IsTerminal("user-events/index.run") {
		t.Fatalf("invocation should be terminal after 5 attempts")
	}
	// Backoff schedule: 1m, 2m, 5m, 10m.
	want := []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute}
	if len(prog.failures) != len(want) {
		t.Fatalf("failures = %v, want %v", prog.failures, want)
	}
	for i, w := range want {
		if prog.failures[i] != w {
			t.Errorf("failure[%d] backoff = %s, want %s", i, prog.failures[i], w)
		}
	}
}

// TestHandleExhaustedAndOtherCompletesRoutesToDLQ verifies the aggregate
// exhaustion decision: when a failing invocation exhausts (retries:0) and the
// other matched invocation executes and completes in the SAME delivery, every
// matched invocation is terminal (one exhausted, one complete), so Handle
// returns ErrInvocationExhausted and the stream layer routes the message to the
// DLQ. Note both run in one delivery (no fail-fast).
func TestHandleExhaustedAndOtherCompletesRoutesToDLQ(t *testing.T) {
	alpha := &scriptedExecutor{fail: true} // exhausts
	beta := &scriptedExecutor{}            // succeeds
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, alpha),
		fnWithRetries(t, "beta", 0, beta),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	// alpha exhausts and beta completes this delivery → all matched terminal →
	// the message routes to the DLQ.
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, want ErrInvocationExhausted", err)
	}
	if !prog.IsTerminal("alpha/index.run") {
		t.Fatalf("alpha should be terminal (exhausted)")
	}
	if !prog.IsComplete("beta/index.run") {
		t.Fatalf("beta should be complete (it succeeded this delivery)")
	}
}

// TestHandleExhaustedOtherProtectedKeepsPending verifies the cross-replica
// hazard: when a failing invocation exhausts (retries:0) but another matched
// invocation is protected (waiting out a retry backoff or running on another
// replica), NOT all matched invocations are terminal — the protected one is
// unresolved — so Handle returns ErrInvocationNotEligible (nothing else
// executed, protected skip) and the message stays pending rather than being
// routed to the DLQ or ACKed.
func TestHandleExhaustedOtherProtectedKeepsPending(t *testing.T) {
	alpha := &scriptedExecutor{fail: true} // exhausts
	beta := &scriptedExecutor{}            // protected (never executed)
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, alpha),
		fnWithRetries(t, "beta", 0, beta),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark beta protected by a future retry backoff so it is skipped (not
	// eligible) this delivery.
	prog.nextAt["beta/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	// alpha exhausts but beta remains unresolved (protected) → the message must
	// NOT be DLQ'd and must NOT be ACKed: ErrInvocationNotEligible (protected
	// skip, nothing else executed) keeps it pending.
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle error = %v, want ErrInvocationNotEligible (beta unresolved)", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, must NOT be ErrInvocationExhausted (beta unresolved)", err)
	}
	if !prog.IsTerminal("alpha/index.run") {
		t.Fatalf("alpha should be terminal (exhausted)")
	}
	// beta is protected, not complete/exhausted → not terminal.
	if prog.IsTerminal("beta/index.run") {
		t.Fatalf("beta should NOT be terminal (it is only protected)")
	}
}

// TestHandleSuccessWithProtectedSkipNotEligible pins the cross-replica ACK
// hazard: a succeeded invocation does not let the message ACK while a sibling
// invocation is unresolved — the stream keeps it pending and reclaim replays it.
func TestHandleSuccessWithProtectedSkipNotEligible(t *testing.T) {
	alpha := &scriptedExecutor{} // executes and succeeds this delivery
	beta := &scriptedExecutor{}  // protected, never executes
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", alpha),
		alwaysMatchFn(t, "beta", beta),
	}, silentLogger(), nil)

	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark beta protected by a future retry backoff so it is skipped (not
	// eligible) this delivery.
	prog.nextAt["beta/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	// alpha succeeded but beta remains unresolved (protected) → the message must
	// NOT be ACKed: ErrInvocationNotEligible (protected skip) keeps it pending.
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle error = %v, want ErrInvocationNotEligible (beta unresolved)", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, must NOT be ErrInvocationExhausted (beta unresolved)", err)
	}
	if alpha.count() != 1 {
		t.Fatalf("alpha calls = %d, want 1 (executed and succeeded)", alpha.count())
	}
	if !prog.IsComplete("alpha/index.run") {
		t.Errorf("alpha should be marked complete despite the message not being ACKed")
	}
	if beta.count() != 0 {
		t.Fatalf("beta calls = %d, want 0 (protected invocation skipped)", beta.count())
	}
}

// TestHandleNotEligibleWhenProtected verifies that when the sole matched
// invocation is protected (running or waiting out a retry backoff) and nothing
// executes, Handle returns ErrInvocationNotEligible so the stream layer leaves
// the message pending.
func TestHandleNotEligibleWhenProtected(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Mark the invocation waiting out a retry backoff (future next-attempt).
	prog.nextAt["user-events/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle error = %v, want ErrInvocationNotEligible", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (protected invocation skipped)", exec.count())
	}
}

// TestHandleRetriesTotalOnlyOnRetryable verifies that function_retries_total
// increments only on retryable failures, not on the exhausting failure.
func TestHandleRetriesTotalOnlyOnRetryable(t *testing.T) {
	m := metrics.New()
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "user-events", 1, exec)}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1: retryable → function_retries_total increments.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected attempt 1 to fail")
	}
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 || fs[0].RetriesTotal != 1 {
		t.Fatalf("retries after attempt 1 = %+v, want 1", fs)
	}
	// Attempt 2: exhausts → function_retries_total does NOT increment, but
	// function_dlq_total does.
	prog.advance(retryBackoff(1))
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 2 error = %v, want ErrInvocationExhausted", err)
	}
	fs = m.FunctionStatsSnapshot()
	if len(fs) != 1 || fs[0].RetriesTotal != 1 {
		t.Fatalf("retries after exhaustion = %+v, want still 1", fs)
	}
	if fs[0].DLQTotal != 1 {
		t.Fatalf("dlq after exhaustion = %d, want 1", fs[0].DLQTotal)
	}
}

// --- Lifecycle regressions #4-#8: removed functions/rules on the event path ---
//
// These pin the invariant that Handle re-matches against the CURRENT registry
// snapshot every delivery, so a removed function or removed rule drops out of
// `matched` on the next delivery: it stops gating the ACK, its stale
// invocation-state field is never consulted, and it is never DLQ'd.

// regression #4: a function whose rule matches H is registered; its invocation
// is pre-marked as waiting out a retry backoff (which WOULD block a redelivery
// if it still gated). The function is then REMOVED from the registry. A
// redelivery matches nothing, so Handle returns nil (the stream would ACK) and
// the removed invocation's backoff no longer blocks.
func TestHandleRemovedFunctionDoesNotGateAck(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// Pre-mark alpha waiting out a future retry backoff.
	prog.nextAt["alpha/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Remove the function from the registry before redelivery.
	r.Registry().Replace("alpha", nil)

	// Nothing matches → nil (ACK). The removed invocation must not block.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle = %v, want nil (removed function must not block the ack)", err)
	}
	// The removed invocation is never exhausted (not marked in the fake).
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none (removed invocation must not be marked exhausted)", prog.exhausted)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (function removed, nothing to run)", exec.count())
	}
}

// regression #5: the function stays registered but its handler is removed from
// the template rules, so MatchingRules no longer matches H. Redelivery matches
// nothing → nil (ACK); the stale invocation-state field is not consulted.
func TestHandleRemovedRuleDoesNotGateAck(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", exec)}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	prog.nextAt["alpha/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Swap to a template with NO matching rules (empty rule set).
	swapped := NewPrepared(
		function.Function{
			Name: "alpha",
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{},
			},
		},
		&runtime.Prepared{Name: "alpha", Image: "x"},
		exec,
	)
	r.Registry().Replace("alpha", swapped)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle = %v, want nil (removed rule must not block the ack)", err)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none (removed rule invocation must not be marked exhausted)", prog.exhausted)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 (no matching rules)", exec.count())
	}
}

// regression #6: two functions A (removed) and B (valid, its invocation
// pre-marked complete). A redelivery matches only B, which is complete → Handle
// returns nil (ACK). A's removal does not re-gate anything.
func TestHandleRemovedAndCompleteMatchesAck(t *testing.T) {
	a := &countingExecutor{}
	b := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", a),
		alwaysMatchFn(t, "beta", b),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	// B already completed on a previous delivery.
	prog.done["beta/index.run"] = true
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Remove function alpha before redelivery.
	r.Registry().Replace("alpha", nil)

	// Only B matches, and B is complete → nil (ACK).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle = %v, want nil (only the complete B matches)", err)
	}
	if a.count() != 0 || b.count() != 0 {
		t.Fatalf("executor calls: alpha=%d beta=%d, want 0/0 (A removed, B complete)", a.count(), b.count())
	}
}

// regression #7: A removed; B valid but its invocation is protected by an
// active retry backoff (TryStart returns started=false, wait>0), so it is NOT
// eligible. Handle must return stream.ErrInvocationNotEligible (message stays
// pending) — NOT an ACK. This pins that a removed (obsolete) A does not
// force-ACK a message whose B is still unresolved.
func TestHandleRemovedDoesNotForceAckWhenOtherProtected(t *testing.T) {
	a := &countingExecutor{}
	b := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{
		alwaysMatchFn(t, "alpha", a),
		alwaysMatchFn(t, "beta", b),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	// B waits out a future retry backoff → not eligible this delivery.
	prog.nextAt["beta/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Remove function alpha before redelivery.
	r.Registry().Replace("alpha", nil)

	// Only B matches and B is protected → not eligible, message stays pending.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationNotEligible) {
		t.Fatalf("handle = %v, want ErrInvocationNotEligible (B unresolved; removed A must not force an ack)", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle = %v, must NOT be ErrInvocationExhausted (B resolved-late would be pending, not DLQ)", err)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none (removed A must not be marked exhausted)", prog.exhausted)
	}
}

// regression #8: no DLQ accounting for a removed invocation. On a removed
// function/rule redelivery, MarkExhausted is never called for the removed
// invocation and function_dlq_total is not bumped. Combined with #4/#5/#6's
// assertions on prog.exhausted, this pins there is no DLQ path for removals.
func TestHandleRemovedFunctionNoDLQAccounting(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", exec)}, silentLogger(), m)
	prog := newFakeInvocationState()
	now := time.Now()
	prog.setClock(func() time.Time { return now })
	prog.nextAt["alpha/index.run"] = now.Add(time.Hour)
	ctx := stream.WithInvocationState(context.Background(), prog)

	r.Registry().Replace("alpha", nil)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle = %v, want nil", err)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none (removed invocation must not be MarkExhausted → no DLQ)", prog.exhausted)
	}
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 0 {
		t.Fatalf("function stats = %+v, want none (no DLQ/metrics for a removed function)", fs)
	}
}

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
// invocation state is tracked independently across redeliveries. Names are
// chosen so the always-failing function (Z) sorts last: the runner iterates in
// sorted order and returns on the first failure, so Z must come after C for C
// to be retried on delivery 2.
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

	// Delivery 1: A succeeds (marked), C fails (unmarked), Z never runs (unmarked).
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
	if a.count() != 1 || c.count() != 1 || z.count() != 0 {
		t.Fatalf("delivery1 calls: A=%d C=%d Z=%d, want 1/1/0", a.count(), c.count(), z.count())
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
	if z.count() != 1 {
		t.Errorf("Z calls = %d, want 1 (retried)", z.count())
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
	if z.count() != 2 {
		t.Errorf("Z calls = %d, want 2 (retried)", z.count())
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

// TestHandleExhaustedWithOtherRunnableKeepsPending verifies that when a failing
// invocation exhausts but another matched invocation is still runnable (not
// terminal), Handle returns the plain error (not ErrInvocationExhausted) so the
// message stays pending and the other invocation continues.
func TestHandleExhaustedWithOtherRunnableKeepsPending(t *testing.T) {
	alpha := &scriptedExecutor{fail: true} // exhausts
	beta := &scriptedExecutor{}            // succeeds
	r := NewWithMetrics([]*PreparedFunction{
		fnWithRetries(t, "alpha", 0, alpha),
		fnWithRetries(t, "beta", 0, beta),
	}, silentLogger(), nil)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// alpha exhausts (retries:0) but beta is still runnable, so the message is
	// NOT terminal: Handle returns a plain error, not ErrInvocationExhausted.
	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err == nil || errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, want a plain (non-exhausted) error", err)
	}
	if !prog.IsTerminal("alpha/index.run") {
		t.Fatalf("alpha should be terminal (exhausted)")
	}
	if prog.IsTerminal("beta/index.run") {
		t.Fatalf("beta should NOT be terminal (it succeeded and is complete)")
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

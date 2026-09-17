package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/stream"
)

// execStats finds the FunctionStat for name in the registry's snapshot, or a
// zero value when absent.
func execStats(m *metrics.Registry, name string) metrics.FunctionStat {
	fs := m.FunctionStatsSnapshot()
	for _, f := range fs {
		if f.Function == name {
			return f
		}
	}
	return metrics.FunctionStat{}
}

// TestFirstExecutionSetsTimestamps pins the simplest attribution: the very
// first successful execution sets last_execution AND last_success (last success
// never precedes last execution), and leaves failure/DLQ untouched.
func TestFirstExecutionSetsTimestamps(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", &fixedExecutor{})}, silentLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	s := execStats(m, "alpha")
	now := time.Now().Unix()
	if s.LastExecution == 0 || s.LastExecution > now {
		t.Fatalf("LastExecution = %d, want within (0, now]", s.LastExecution)
	}
	if s.LastSuccess < s.LastExecution {
		t.Fatalf("LastSuccess = %d, want >= LastExecution = %d (set later in the same attempt)", s.LastSuccess, s.LastExecution)
	}
	if s.LastFailure != 0 || s.LastDLQ != 0 {
		t.Fatalf("success must not set failure/DLQ: %+v", s)
	}
}

// TestFailureSetsExecutionAndFailure verifies a failed attempt sets
// last_execution AND last_failure, keeps last_success untouched, and does NOT
// set last_dlq (a retryable failure is not a DLQ attribution).
func TestFailureSetsExecutionAndFailure(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", &fixedExecutor{err: true})}, silentLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}

	s := execStats(m, "alpha")
	now := time.Now().Unix()
	if s.LastExecution == 0 || s.LastExecution > now {
		t.Fatalf("LastExecution = %d, want within (0, now]", s.LastExecution)
	}
	if s.LastFailure < s.LastExecution {
		t.Fatalf("LastFailure = %d, want >= LastExecution = %d", s.LastFailure, s.LastExecution)
	}
	if s.LastSuccess != 0 {
		t.Fatalf("failure must not set success: %+v", s)
	}
	if s.LastDLQ != 0 {
		t.Fatalf("a retryable failure must never set LastDLQ: %+v", s)
	}
}

// TestRetryAttemptsUpdateTimestampsThenSuccessPreservesFailure drives a
// retryable failure, then another retryable attempt, then a success: each
// claimed attempt re-stamps last_execution/last_failure, and the later success
// does NOT erase the earlier failure timestamp.
func TestRetryAttemptsUpdateTimestampsThenSuccessPreservesFailure(t *testing.T) {
	m := metrics.New()
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", exec)}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1: retryable failure (execution + failure stamped).
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected attempt 1 to fail")
	}
	s1 := execStats(m, "alpha")
	if s1.LastExecution == 0 || s1.LastFailure < s1.LastExecution || s1.LastSuccess != 0 || s1.LastDLQ != 0 {
		t.Fatalf("attempt 1 = %+v", s1)
	}

	// Attempt 2 (retry): still failing; failure time moves to (>=) attempt 1's.
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected attempt 2 to fail")
	}
	s2 := execStats(m, "alpha")
	if s2.LastExecution < s1.LastExecution {
		t.Fatalf("retry must re-stamp LastExecution: %+v -> %+v", s1, s2)
	}
	if s2.LastFailure < s2.LastExecution || s2.LastDLQ != 0 {
		t.Fatalf("attempt 2 = %+v", s2)
	}

	// Attempt 3: succeeds. Success is stamped; the earlier failure time is NOT
	// erased (it must remain >= s2's, since time only moves forward, proving it
	// was never cleared back to zero).
	prog.advance(2 * time.Minute)
	exec.setFail(false)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("attempt 3: %v", err)
	}
	s3 := execStats(m, "alpha")
	if s3.LastSuccess < s3.LastExecution {
		t.Fatalf("LastSuccess = %d, want >= LastExecution = %d", s3.LastSuccess, s3.LastExecution)
	}
	if s3.LastFailure < s2.LastFailure {
		t.Fatalf("success must preserve the earlier failure time: %+v", s3)
	}
	if s3.LastDLQ != 0 {
		t.Fatalf("no DLQ attribution ever happened: %+v", s3)
	}
}

// TestExhaustedAttemptSetsDLQOnlyOnExhaustion pins the DLQ attribution point:
// retryable failures never set last_dlq; the attempt that exhausts (and routes
// the message to the DLQ) does — once.
func TestExhaustedAttemptSetsDLQOnlyOnExhaustion(t *testing.T) {
	m := metrics.New()
	exec := &scriptedExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "alpha", 1, exec)}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1: retryable → execution+failure, NO DLQ.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected attempt 1 to fail")
	}
	s := execStats(m, "alpha")
	if s.LastDLQ != 0 {
		t.Fatalf("retryable attempt must not set LastDLQ: %+v", s)
	}

	// Attempt 2: exhausts → the message routes to the DLQ → LastDLQ is stamped.
	prog.advance(retryBackoff(1))
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("attempt 2 error = %v, want ErrInvocationExhausted", err)
	}
	s = execStats(m, "alpha")
	if s.LastDLQ == 0 || s.LastDLQ < s.LastExecution {
		t.Fatalf("exhausted attempt must set LastDLQ (>= LastExecution %d): %+v", s.LastExecution, s)
	}

	// A later redelivery of the now-terminal invocation takes the skip path:
	// none of the four timestamps may move again (the DLQ was already
	// attributed at the original exhausting attempt). Only function_events
	// keeps counting the match.
	prog.advance(retryBackoff(2))
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("terminal skip: got %v, want nil (all matched terminal → stream would ACK)", err)
	}
	after := execStats(m, "alpha")
	if after.LastExecution != s.LastExecution || after.LastSuccess != s.LastSuccess ||
		after.LastFailure != s.LastFailure || after.LastDLQ != s.LastDLQ {
		t.Fatalf("terminal skip must not move timestamps: before=%+v after=%+v", s, after)
	}
}

// TestExecuteRuleTerminalSkipDoesNotUpdateTimestamps is the matcher/registry
// guard: an event whose invocation is already terminal (complete) is skipped
// before any execution — last_execution/last_failure/last_dlq must not move.
func TestExecuteRuleTerminalSkipDoesNotUpdateTimestamps(t *testing.T) {
	m := metrics.New()
	exec := &scriptedExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "alpha", exec)}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// First delivery succeeds and stamps execution/success.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	before := execStats(m, "alpha")

	// Redelivery: the invocation is already complete → terminal skip, not an
	// execution.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (skipped on redelivery)", exec.count())
	}
	after := execStats(m, "alpha")
	if before.LastExecution != after.LastExecution || before.LastSuccess != after.LastSuccess ||
		before.LastFailure != after.LastFailure || before.LastDLQ != after.LastDLQ {
		t.Fatalf("terminal skip must not move timestamps: before=%+v after=%+v", before, after)
	}
}

// TestInvokeHandlerTimestamps verifies the schedule path updates the same
// timestamps: success stamps execution+success (failure/DLQ untouched); failure
// stamps execution+failure and no DLQ for a retryable attempt.
func TestInvokeHandlerTimestamps(t *testing.T) {
	// Success case.
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "alpha", &fixedExecutor{}, function.DefaultTimeout)}, silentLogger(), m)
	if err := r.InvokeHandler(context.Background(), "1-0", "alpha", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	s := execStats(m, "alpha")
	now := time.Now().Unix()
	if s.LastExecution == 0 || s.LastExecution > now {
		t.Fatalf("LastExecution = %d, want within (0, now]", s.LastExecution)
	}
	if s.LastSuccess < s.LastExecution {
		t.Fatalf("LastSuccess = %d, want >= LastExecution", s.LastSuccess)
	}
	if s.LastFailure != 0 || s.LastDLQ != 0 {
		t.Fatalf("schedule success must not touch failure/DLQ: %+v", s)
	}

	// Failure case (retryable: the schedule carries the default retry count).
	m2 := metrics.New()
	r2 := NewWithMetrics([]*PreparedFunction{schedFn(t, "beta", &fixedExecutor{err: true}, function.DefaultTimeout)}, silentLogger(), m2)
	if err := r2.InvokeHandler(context.Background(), "1-1", "beta", "index.run", []byte(`{}`)); err == nil {
		t.Fatal("expected InvokeHandler to fail")
	}
	fs := execStats(m2, "beta")
	if fs.LastExecution == 0 || fs.LastFailure < fs.LastExecution {
		t.Fatalf("schedule failure = %+v", fs)
	}
	if fs.LastSuccess != 0 || fs.LastDLQ != 0 {
		t.Fatalf("retryable schedule failure must not set success/DLQ: %+v", fs)
	}
}

// TestInvokeHandlerExhaustedSetsDLQ pins the schedule path's exhaustion: the
// failure that exhausts the schedule's retry budget stamps last_dlq (the
// invocation is routed to the DLQ because a schedule has exactly one
// invocation).
// TestInvokeHandlerExhaustedSetsDLQ pins the schedule path's exhaustion:
// schedFn carries a zero-retry schedule (not parsed from YAML, so no default),
// meaning maxAttempts = 1 and the FIRST failure exhausts — a schedule has
// exactly ONE invocation, so the message is terminal and is routed to the DLQ;
// last_dlq must be stamped at that point.
func TestInvokeHandlerExhaustedSetsDLQ(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "alpha", &fixedExecutor{err: true}, function.DefaultTimeout)}, silentLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.InvokeHandler(ctx, "1-0", "alpha", "index.run", []byte(`{}`))
	if err == nil {
		t.Fatal("expected InvokeHandler to fail")
	}
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("a retries:0 schedule failure must wrap ErrInvocationExhausted (single invocation, terminal): %v", err)
	}
	s := execStats(m, "alpha")
	if s.LastExecution == 0 || s.LastFailure < s.LastExecution {
		t.Fatalf("exhausted schedule attempt = %+v", s)
	}
	if s.LastDLQ == 0 || s.LastDLQ < s.LastExecution {
		t.Fatalf("exhausted schedule attempt must set LastDLQ (>= LastExecution): %+v", s)
	}
	if s.LastSuccess != 0 {
		t.Fatalf("failed schedule attempt must not set success: %+v", s)
	}
}

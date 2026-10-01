package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestHandleClassifiesUnmatchedEvent pins the unmatched branch: an event that
// matches no rule is counted once as received+unmatched (never matched), the
// app-engaged counter is not touched, and Handle returns nil so the stream
// layer ACKs it (an unmatched event is terminal and never retried).
func TestHandleClassifiesUnmatchedEvent(t *testing.T) {
	m := metrics.New()
	// An app that declares no event rules matches nothing.
	pf := buildFn(fnSpec{name: "selective"}, &countingExecutor{})
	r := NewWithMetrics([]*PreparedApp{pf}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle unmatched: %v", err)
	}

	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Errorf("events_received_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 1 {
		t.Errorf("events_unmatched_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 0 {
		t.Errorf("events_matched_total = %d, want 0", got)
	}
	if len(m.AppStatsSnapshot()) != 0 {
		t.Errorf("unmatched event must not engage a function: %+v", m.AppStatsSnapshot())
	}
}

// TestHandleClassifiesMatchedEventWithNoRulesOnOtherApp pins that the
// partition is computed from the full rule set: an app with no matching
// rules does not contribute, while an app that matches marks the event
// matched. received == matched + unmatched.
func TestHandleClassifiesMatchedEventWithNoRulesOnOtherApp(t *testing.T) {
	m := metrics.New()
	noRule := buildFn(fnSpec{name: "norule"}, &countingExecutor{})
	r := NewWithMetrics([]*PreparedApp{
		noRule,
		alwaysMatchFn(t, "matched", &countingExecutor{}),
	}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}
	// Only "matched" is engaged.
	if fs := m.AppStatsSnapshot(); len(fs) != 1 || fs[0].App != "matched" {
		t.Errorf("only the matching function may be engaged: %+v", fs)
	}
}

// TestHandleCountsAppOnceDespiteMultipleMatchingRules pins that an app
// with several matching rules is engaged once per logical event, not once per
// rule, while every rule still runs.
func TestHandleCountsAppOnceDespiteMultipleMatchingRules(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	pf := buildFn(fnSpec{name: "multi", rules: []app.EventRule{
		{Handler: "a.run", Pattern: app.Pattern{}, Timeout: time.Second, Retries: app.DefaultRetries},
		{Handler: "b.run", Pattern: app.Pattern{}, Timeout: time.Second, Retries: app.DefaultRetries},
	}}, exec)
	r := NewWithMetrics([]*PreparedApp{pf}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 2 {
		t.Fatalf("executor calls = %d, want 2 (both matching rules run)", exec.count())
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 1 {
		t.Fatalf("function must be engaged once for the event: %+v", fs)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1", got)
	}
}

// TestHandleFailureStaysMatched pins that the classification is decided from
// matching before execution: a failing handler does not move the event out of
// the matched class, and it is not counted unmatched.
func TestHandleFailureStaysMatched(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "a", &countingExecutor{fail: true})}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1 (a failure stays matched)", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}
}

// TestHandleClassificationClaimedOnceAcrossRedeliveries pins the invariant end
// to end: with invocation state, repeated deliveries of the same logical event
// (a retrying failure) count received/matched exactly once, while the
// per-app engaged counter also stays at one.
func TestHandleClassificationClaimedOnceAcrossRedeliveries(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{fail: true}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "a", app.DefaultRetries, exec)}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Five deliveries (the fifth exhausts) all carry the same logical event.
	for i := 1; i <= 5; i++ {
		_ = r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
		prog.advance(11 * time.Minute)
	}

	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Errorf("events_received_total = %d, want 1 (one logical event, five deliveries)", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 1 {
		t.Errorf("function engaged = %+v, want matched 1", fs)
	}
}

// TestHandleClassificationCountsNothingOnClaimError pins the fail-closed
// classification: when the claim cannot be written, no classification counter is
// incremented (a missed count is preferable to a double count). The handler still
// runs, so this is not a delivery failure.
func TestHandleClassificationCountsNothingOnClaimError(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "a", exec)}, testutil.DiscardLogger(), m)

	prog := newFakeInvocationState()
	prog.classifyErr = context.DeadlineExceeded
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (classification failure must not block execution)", exec.count())
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 0 {
		t.Errorf("events_received_total = %d, want 0 (claim error counts nothing)", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 0 {
		t.Errorf("events_matched_total = %d, want 0", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}
	// The handler still ran (and recorded its own success), but the
	// app-engaged classification counter must stay at zero.
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 0 {
		t.Errorf("claim error must not engage a function: %+v", fs)
	}
}

// TestHandleWithoutInvocationStateCountsEveryCall pins the direct-caller path:
// with no invocation state there is no dedup channel, so each Handle call is
// treated as a distinct logical event.
func TestHandleWithoutInvocationStateCountsEveryCall(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "a", &countingExecutor{})}, testutil.DiscardLogger(), m)

	for i := 0; i < 3; i++ {
		if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 3 {
		t.Errorf("events_received_total = %d, want 3", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 3 {
		t.Errorf("events_matched_total = %d, want 3", got)
	}
}

// TestHandleMatchedButUnavailableIsMatchedNotUnmatched pins the core fix: an event
// that matches ONLY a configured-but-unavailable app is classified MATCHED,
// not unmatched, and the app is counted as engaged — even though the
// invocation cannot run. No handler attempt is claimed (no TryStart) and no
// handler execution counter is touched, because no handler ran.
func TestHandleMatchedButUnavailableIsMatchedNotUnmatched(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{unavailableMatchFn(t, "broken")}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable (matched but unavailable is retryable)", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, must NOT be ErrInvocationExhausted (unavailability must not DLQ)", err)
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Errorf("events_received_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0 (matched, not unmatched)", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].App != "broken" || fs[0].EventsMatchedTotal != 1 {
		t.Fatalf("unavailable function must be engaged as matched: %+v", fs)
	}
	// No handler ran: no attempt claimed, no handler execution counters.
	if len(prog.attempts) != 0 || len(prog.marks) != 0 || len(prog.failures) != 0 {
		t.Fatalf("no handler ran, so invocation state must be untouched: attempts=%v marks=%v failures=%v",
			prog.attempts, prog.marks, prog.failures)
	}
	if got := m.Counter(metrics.MetricHandlerSuccess); got != 0 {
		t.Errorf("handler_success_total = %d, want 0 (no handler ran)", got)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 0 {
		t.Errorf("handler_failure_total = %d, want 0 (no handler ran)", got)
	}
}

// TestHandleMixedFanOutAvailableCompletesUnavailablePending pins the mixed
// fan-out contract: a message matching an AVAILABLE app (which completes)
// and an UNAVAILABLE app (which cannot run) must run the available
// invocation to completion while returning a retryable error for the
// unavailable one, so the message stays pending and is not ACKed. The available
// invocation's completion is persisted so a redelivery skips it.
func TestHandleMixedFanOutAvailableCompletesUnavailablePending(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{
		alwaysMatchFn(t, "available", exec),
		unavailableMatchFn(t, "broken"),
	}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable (unavailable sibling unresolved)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("available executor calls = %d, want 1 (available invocation must still run)", exec.count())
	}
	if !prog.IsComplete("available/index.run") {
		t.Fatalf("available invocation must be marked complete")
	}
	if prog.IsTerminal("broken/index.run") {
		t.Fatalf("unavailable invocation must remain unresolved (not terminal)")
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}

	// Redelivery: the completed available invocation must NOT re-run; the
	// unavailable one is still unresolved, so the message stays pending.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("redelivery error = %v, want ErrAppUnavailable", err)
	}
	if exec.count() != 1 {
		t.Fatalf("available executor calls after redelivery = %d, want 1 (no re-run)", exec.count())
	}
}

// TestHandleMixedFanOutUnresolvedWorkCompletesAfterAvailable pins that a message
// left pending for an unavailable sibling completes normally once that sibling
// becomes available: the already-completed available invocation is skipped and
// the now-available invocation runs, after which Handle returns nil (the stream
// ACKs).
func TestHandleMixedFanOutUnresolvedWorkCompletesAfterAvailable(t *testing.T) {
	m := metrics.New()
	availableExec := &countingExecutor{}
	recoveredExec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{
		alwaysMatchFn(t, "available", availableExec),
		unavailableMatchFn(t, "broken"),
	}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: available completes, broken is unresolved → pending.
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("delivery 1 error = %v, want ErrAppUnavailable", err)
	}
	if availableExec.count() != 1 {
		t.Fatalf("available executions = %d, want 1", availableExec.count())
	}

	// The app is rebuilt and becomes available; the unavailable entry is
	// swapped for a runnable one with the SAME name (preserving invocation-state
	// identity/dedup).
	r.Registry().Replace("broken", alwaysMatchFn(t, "broken", recoveredExec))

	err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("delivery 2 error = %v, want nil once all matched work is complete", err)
	}
	if availableExec.count() != 1 {
		t.Fatalf("completed available invocation must not re-run: calls = %d, want 1", availableExec.count())
	}
	if recoveredExec.count() != 1 {
		t.Fatalf("recovered invocation must run: calls = %d, want 1", recoveredExec.count())
	}
	if !prog.IsComplete("broken/index.run") {
		t.Fatalf("recovered invocation must be marked complete")
	}
}

// TestHandleUnavailableOnlyDoesNotIncrementHandlerFailure pins that an
// unavailable match never counts as a handler failure/attempt: the per-app
// failure, retry, and DLQ counters stay untouched (no handler ran), while the
// app is still engaged as matched. This is the "no handler attempt when no
// handler ran" invariant.
func TestHandleUnavailableOnlyDoesNotIncrementHandlerFailure(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{unavailableMatchFn(t, "broken")}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable", err)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 0 {
		t.Errorf("handler_failure_total = %d, want 0 (no handler ran)", got)
	}
	if got := m.Counter(metrics.MetricRetries); got != 0 {
		t.Errorf("retries_total = %d, want 0 (no handler attempt)", got)
	}
	if got := m.Counter(metrics.MetricDLQEntries); got != 0 {
		t.Errorf("dlq_entries_total = %d, want 0 (unavailability must not DLQ)", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 1 ||
		fs[0].HandlerFailureTotal != 0 || fs[0].RetriesTotal != 0 || fs[0].DLQTotal != 0 {
		t.Fatalf("per-function stats must record engagement only: %+v", fs)
	}
}

// TestHandleUnavailableExhaustedStaysPendingNotDLQ pins the interaction with an
// already-exhausted unavailable invocation. An unavailable match that is not
// complete is unresolved and holds the message pending — this worker cannot
// attribute the exhausted attempt count from an unavailable entry, and ACKing or
// DLQing would race another replica's write or drop metadata, so unavailability
// alone never routes to the DLQ. Once the app becomes available again, the
// normal path reads the persisted exhausted marker and routes the message to the
// DLQ with the correct attempt metadata.
func TestHandleUnavailableExhaustedStaysPendingNotDLQ(t *testing.T) {
	m := metrics.New()
	prog := newFakeInvocationState()
	// broken already exhausted its attempts (retries:0 → attempt 1) on a previous
	// delivery, before it became unavailable.
	prog.exhausted["broken/index.run"] = 1
	ctx := stream.WithInvocationState(context.Background(), prog)

	unavailable := NewWithMetrics([]*PreparedApp{unavailableMatchFn(t, "broken")}, testutil.DiscardLogger(), m)
	err := unavailable.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable (unavailable stays pending)", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("handle error = %v, must NOT be ErrInvocationExhausted (unavailability must not DLQ)", err)
	}

	// Once available again, the persisted exhausted marker drives the normal DLQ
	// routing with the correct attempt count.
	recovered := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "broken", &countingExecutor{})}, testutil.DiscardLogger(), m)
	err = recovered.Handle(ctx, "1757-0", map[string]any{"status": "ok"})
	if !errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("recovered handle error = %v, want ErrInvocationExhausted", err)
	}
	var typed *stream.HandlerExhaustedError
	if !errors.As(err, &typed) || len(typed.Invocations) != 1 || typed.Invocations[0].Attempts != 1 {
		t.Fatalf("recovered exhaustion metadata = %+v, want one invocation with attempts 1", err)
	}
}

// TestHandleUnavailableMatchMetricsClaimedOnceAcrossRedeliveries pins that the
// matched classification for an unavailable match is claimed exactly once across
// redeliveries: repeated pending deliveries do not double-count received/matched,
// and the app-engaged counter also stays at one.
func TestHandleUnavailableMatchMetricsClaimedOnceAcrossRedeliveries(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{unavailableMatchFn(t, "broken")}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	for i := 0; i < 3; i++ {
		if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("delivery %d error = %v, want ErrAppUnavailable", i+1, err)
		}
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Errorf("events_received_total = %d, want 1 across redeliveries", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched_total = %d, want 1 across redeliveries", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched_total = %d, want 0", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 1 {
		t.Fatalf("function engagement must be claimed once: %+v", fs)
	}
}

package runner

import (
	"context"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestHandleClassifiesUnmatchedEvent pins the unmatched branch: an event that
// matches no rule is counted once as received+unmatched (never matched), the
// function-engaged counter is not touched, and Handle returns nil so the stream
// layer ACKs it (an unmatched event is terminal and never retried).
func TestHandleClassifiesUnmatchedEvent(t *testing.T) {
	m := metrics.New()
	// A function that declares no event rules matches nothing.
	pf := buildFn(fnSpec{name: "selective"}, &countingExecutor{})
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), m)

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
	if len(m.FunctionStatsSnapshot()) != 0 {
		t.Errorf("unmatched event must not engage a function: %+v", m.FunctionStatsSnapshot())
	}
}

// TestHandleClassifiesMatchedEventWithNoRulesOnOtherFunction pins that the
// partition is computed from the full rule set: a function with no matching
// rules does not contribute, while a function that matches marks the event
// matched. received == matched + unmatched.
func TestHandleClassifiesMatchedEventWithNoRulesOnOtherFunction(t *testing.T) {
	m := metrics.New()
	noRule := buildFn(fnSpec{name: "norule"}, &countingExecutor{})
	r := NewWithMetrics([]*PreparedFunction{
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
	if fs := m.FunctionStatsSnapshot(); len(fs) != 1 || fs[0].Function != "matched" {
		t.Errorf("only the matching function may be engaged: %+v", fs)
	}
}

// TestHandleCountsFunctionOnceDespiteMultipleMatchingRules pins that a function
// with several matching rules is engaged once per logical event, not once per
// rule, while every rule still runs.
func TestHandleCountsFunctionOnceDespiteMultipleMatchingRules(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	pf := buildFn(fnSpec{name: "multi", rules: []function.EventRule{
		{Handler: "a.run", Pattern: function.Pattern{}, Timeout: time.Second, Retries: function.DefaultRetries},
		{Handler: "b.run", Pattern: function.Pattern{}, Timeout: time.Second, Retries: function.DefaultRetries},
	}}, exec)
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 2 {
		t.Fatalf("executor calls = %d, want 2 (both matching rules run)", exec.count())
	}
	fs := m.FunctionStatsSnapshot()
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
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "a", &countingExecutor{fail: true})}, testutil.DiscardLogger(), m)
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
// per-function engaged counter also stays at one.
func TestHandleClassificationClaimedOnceAcrossRedeliveries(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{fail: true}
	r := NewWithMetrics([]*PreparedFunction{fnWithRetries(t, "a", function.DefaultRetries, exec)}, testutil.DiscardLogger(), m)
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
	fs := m.FunctionStatsSnapshot()
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
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "a", exec)}, testutil.DiscardLogger(), m)

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
	// function-engaged classification counter must stay at zero.
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 || fs[0].EventsMatchedTotal != 0 {
		t.Errorf("claim error must not engage a function: %+v", fs)
	}
}

// TestHandleWithoutInvocationStateCountsEveryCall pins the direct-caller path:
// with no invocation state there is no dedup channel, so each Handle call is
// treated as a distinct logical event.
func TestHandleWithoutInvocationStateCountsEveryCall(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "a", &countingExecutor{})}, testutil.DiscardLogger(), m)

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

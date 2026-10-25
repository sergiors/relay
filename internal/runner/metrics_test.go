package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// alwaysMatchFn lives in helpers_test.go.

func TestHandleRecordsSuccessMetrics(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", &countingExecutor{})}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	got := m.Snapshot()
	for _, want := range []string{
		"events_received_total count=1",
		"events_matched_total count=1",
		"handler_invocations_total{app=user-events,handler=index.run,outcome=success} count=1",
		"handler_success_total count=1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("snapshot missing %q; got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "events_unmatched_total count=1") {
		t.Errorf("a matched event must not count unmatched; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Errorf("unexpected failure metric; got:\n%s", got)
	}
	if strings.Contains(got, metrics.MetricHandlerFailure) {
		t.Errorf("unexpected failure total; got:\n%s", got)
	}
	// The duration observation must be recorded (count 1, non-zero sum/max).
	if !containsDuration(got, "handler_duration_seconds{app=user-events,handler=index.run} count=1 sum=") {
		t.Errorf("expected handler_duration_seconds observation; got:\n%s", got)
	}
}

func TestHandleRecordsFailureMetrics(t *testing.T) {
	m := metrics.New()
	// The short hold keeps the observed duration deterministically nonzero.
	exec := &countingExecutor{fail: true, hold: 2 * time.Millisecond}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}

	got := m.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{app=user-events,handler=index.run,outcome=failure} count=1") {
		t.Errorf("expected failure counter; got:\n%s", got)
	}
	if !strings.Contains(got, "handler_failure_total count=1") {
		t.Errorf("expected failure total; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=success") {
		t.Errorf("unexpected success metric; got:\n%s", got)
	}
	if strings.Contains(got, metrics.MetricHandlerSuccess) {
		t.Errorf("unexpected success total; got:\n%s", got)
	}
	// The direct (no-state) path counts the failed attempt exactly once too:
	// one per-app attribution and exactly one nonzero-duration observation.
	if got := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: "user-events"}}); got != 1 {
		t.Errorf("function_handler_failure_total{app=user-events} = %d, want 1", got)
	}
	if count, sum := m.HistogramLabels(metrics.MetricHandlerDuration, []metrics.Label{
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); count != 1 || sum <= 0 {
		t.Errorf("handler_duration_seconds = (%d observations, sum %v), want exactly one nonzero-duration observation", count, sum)
	}
}

// TestHandleClaimedExecutionFailureCountsOnce pins the claimed execution
// failure path against duplicate charging: one failing handler execution with
// a confirmed claim counts the failure telemetry exactly once across the
// global, labeled, and per-app counters, with exactly one nonzero-duration
// observation, and leaves the retry machinery unchanged (one 1m RecordFailure
// backoff and one retry counter, not terminal).
func TestHandleClaimedExecutionFailureCountsOnce(t *testing.T) {
	m := metrics.New()
	// The short hold keeps the observed duration deterministically nonzero.
	exec := &countingExecutor{fail: true, hold: 2 * time.Millisecond}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), m)
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
	if got := prog.attempts["user-events/index.run"]; got != 1 {
		t.Fatalf("claimed attempt = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 1 {
		t.Fatalf("handler_failure_total = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
		{Name: "outcome", Value: "failure"},
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); got != 1 {
		t.Fatalf("handler_invocations_total{outcome=failure} = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
		{Name: "outcome", Value: "success"},
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	}); got != 0 {
		t.Fatalf("handler_invocations_total{outcome=success} = %d, want 0", got)
	}
	if got := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: "user-events"}}); got != 1 {
		t.Fatalf("function_handler_failure_total{app=user-events} = %d, want 1", got)
	}
	count, sum := m.HistogramLabels(metrics.MetricHandlerDuration, []metrics.Label{
		{Name: "app", Value: "user-events"},
		{Name: "handler", Value: "index.run"},
	})
	if count != 1 || sum <= 0 {
		t.Fatalf("handler_duration_seconds = (%d observations, sum %v), want exactly one nonzero-duration observation", count, sum)
	}
	// The retry outcome is unchanged: one retry backoff was recorded and the
	// retry counter moved once, with the invocation not yet terminal.
	if len(prog.failures) != 1 || prog.failures[0] != time.Minute {
		t.Fatalf("failures = %v, want one 1m RecordFailure", prog.failures)
	}
	if got := m.CounterLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "app", Value: "user-events"}}); got != 1 {
		t.Fatalf("function_retries_total{app=user-events} = %d, want 1", got)
	}
	if prog.IsTerminal("user-events/index.run") {
		t.Fatal("a first failure under the default retry budget must not be terminal")
	}
}

// TestRemoveAppDeletesRunnerSeries exercises the production retirement
// path (the worker wraps RemoveAppImages with metrics.RemoveApp at the
// reconciler's RemoveApp hook): after Handle runs success and failure for
// one app, RemoveApp must drop ALL of that app's series from the
// registry while leaving the global totals intact.
func TestRemoveAppDeletesRunnerSeries(t *testing.T) {
	m := metrics.New()
	// A success runner (user-events + other) and a failure runner (user-events
	// again) share the same registry, so one app accumulates both outcomes
	// and another is isolated.
	success := NewWithMetrics(
		[]*PreparedApp{alwaysMatchFn(t, "user-events", &countingExecutor{}), alwaysMatchFn(t, "other", &countingExecutor{})},
		testutil.DiscardLogger(), m)
	failure := NewWithMetrics(
		[]*PreparedApp{alwaysMatchFn(t, "user-events", &countingExecutor{fail: true})},
		testutil.DiscardLogger(), m)
	if err := success.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle success: %v", err)
	}
	if err := failure.Handle(context.Background(), "1757-1", map[string]any{"status": "err"}); err == nil {
		t.Fatal("expected handle to fail")
	}

	// Both user-events and other have app series before removal.
	before := m.Snapshot()
	foundUE := false
	for _, line := range strings.Split(before, "\n") {
		if fnLabelIs(line, "user-events") {
			foundUE = true
			break
		}
	}
	if !foundUE {
		t.Fatalf("expected user-events series before removal; got:\n%s", before)
	}
	foundOther := false
	for _, line := range strings.Split(before, "\n") {
		if fnLabelIs(line, "other") {
			foundOther = true
			break
		}
	}
	if !foundOther {
		t.Fatalf("expected other series before removal; got:\n%s", before)
	}
	// Capture the global totals: removal must never touch them.
	wantSuccess := m.Counter(metrics.MetricHandlerSuccess)
	wantFailure := m.Counter(metrics.MetricHandlerFailure)

	m.RemoveApp("user-events")

	got := m.Snapshot()
	// All of user-events' app-scoped series are gone — no series at all
	// carrying the app=user-events label on any of the appMetrics vecs.
	for _, mname := range []string{
		metrics.MetricAppEventsMatched,
		metrics.MetricFunctionHandlerSuccess,
		metrics.MetricFunctionHandlerFailure,
		metrics.MetricHandlerInvocations,
		metrics.MetricHandlerDuration,
	} {
		for _, line := range strings.Split(got, "\n") {
			if strings.HasPrefix(line, mname+"{") && fnLabelIs(line, "user-events") {
				t.Fatalf("user-events %s series must be removed; got:\n%s", mname, got)
			}
		}
	}
	// The other app's series survive.
	if !strings.Contains(got, "app=other,") && !strings.Contains(got, "app=other}") {
		t.Fatalf("other function's series must survive removal; got:\n%s", got)
	}
	// Global totals unchanged.
	if got := m.Counter(metrics.MetricHandlerSuccess); got != wantSuccess {
		t.Fatalf("handler_success_total = %d, want %d", got, wantSuccess)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != wantFailure {
		t.Fatalf("handler_failure_total = %d, want %d", got, wantFailure)
	}
}

// fnLabelIs reports whether the rendered line carries the exact app label
// value name (a full label value, not a prefix of another).
func fnLabelIs(line, name string) bool {
	token := "app=" + name
	idx := strings.Index(line, token)
	if idx < 0 {
		return false
	}
	rest := line[idx+len(token):]
	return strings.HasPrefix(rest, ",") || strings.HasPrefix(rest, "}") || rest == ""
}

// containsDuration asserts a duration metric line with a prefix and the given
// count appears (without pinning the precise sum, which varies by timing).
func containsDuration(snapshot, prefix string) bool {
	for _, line := range strings.Split(snapshot, "\n") {
		if strings.HasPrefix(line, prefix) && strings.Contains(line, " sum=") {
			return true
		}
	}
	return false
}

// assertNoHandlerFailureTelemetry pins the telemetry-free side of the failed
// attempt boundary for one app/handler series: no global failure total, no
// labeled invocation outcome counter (either outcome), no per-app failure
// attribution, and no duration observation moved. It is the counterpart of
// recordHandlerFailure, so a path that never confirmed a claim (or rejected the
// work pre-claim) can prove it charged nothing.
func assertNoHandlerFailureTelemetry(t *testing.T, m *metrics.Registry, app, handler string) {
	t.Helper()
	if got := m.Counter(metrics.MetricHandlerFailure); got != 0 {
		t.Fatalf("handler_failure_total = %d, want 0", got)
	}
	for _, outcome := range []string{"failure", "success"} {
		if got := m.CounterLabels(metrics.MetricHandlerInvocations, []metrics.Label{
			{Name: "outcome", Value: outcome},
			{Name: "app", Value: app},
			{Name: "handler", Value: handler},
		}); got != 0 {
			t.Fatalf("handler_invocations_total{outcome=%s} = %d, want 0", outcome, got)
		}
	}
	if got := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: app}}); got != 0 {
		t.Fatalf("function_handler_failure_total{app=%s} = %d, want 0", app, got)
	}
	if count, sum := m.HistogramLabels(metrics.MetricHandlerDuration, []metrics.Label{
		{Name: "app", Value: app},
		{Name: "handler", Value: handler},
	}); count != 0 || sum != 0 {
		t.Fatalf("handler_duration_seconds = (%d observations, sum %v), want no observations", count, sum)
	}
}

func TestHandleAppLevelCounters(t *testing.T) {
	m := metrics.New()
	// Two apps, each with a single always-matching rule.
	r := NewWithMetrics([]*PreparedApp{
		alwaysMatchFn(t, "a", &countingExecutor{}),
		alwaysMatchFn(t, "b", &countingExecutor{}),
	}, testutil.DiscardLogger(), m)

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// One event matching two apps: global message-level counter is 1, but
	// each app is engaged once.
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Fatalf("events_received_total = %d, want 1", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 2 {
		t.Fatalf("function stats len = %d, want 2: %+v", len(fs), fs)
	}
	for _, f := range fs {
		if f.EventsMatchedTotal != 1 {
			t.Fatalf("function %s events matched = %d, want 1", f.App, f.EventsMatchedTotal)
		}
		if f.HandlerSuccessTotal != 1 {
			t.Fatalf("function %s success = %d, want 1", f.App, f.HandlerSuccessTotal)
		}
		if f.HandlerFailureTotal != 0 {
			t.Fatalf("function %s failure = %d, want 0", f.App, f.HandlerFailureTotal)
		}
	}
}

func TestHandleAppFailureRetryAndDLQ(t *testing.T) {
	// Without invocation state, a failure counts a retry but never a DLQ: the
	// DLQ decision needs a Redis-backed attempt count to know exhaustion.
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "a", &countingExecutor{fail: true})}, testutil.DiscardLogger(), m)
	ctx := stream.WithDeliveryAttempt(context.Background(), 2)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("function stats len = %d, want 1: %+v", len(fs), fs)
	}
	if fs[0].RetriesTotal != 1 {
		t.Fatalf("retries = %d, want 1", fs[0].RetriesTotal)
	}
	if fs[0].DLQTotal != 0 {
		t.Fatalf("dlq = %d, want 0 (no invocation state, no exhaustion decision)", fs[0].DLQTotal)
	}
	if fs[0].HandlerFailureTotal != 1 {
		t.Fatalf("failure = %d, want 1", fs[0].HandlerFailureTotal)
	}
}

func TestHandleWithNilMetricsDoesNotPanic(t *testing.T) {
	// NewWithMetrics(nil registry) must not panic when handling succeeds/fails.
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", &countingExecutor{})}, testutil.DiscardLogger(), nil)
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	rf := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", &countingExecutor{fail: true})}, testutil.DiscardLogger(), nil)
	if err := rf.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err == nil {
		t.Fatal("expected handle to fail")
	}
}

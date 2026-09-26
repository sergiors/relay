package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// invokeFn builds a prepared function from a parsed YAML template, so the
// manual-invocation tests exercise the real matcher (parsed patterns), not a
// hand-built any-event rule. It fails the test on a parse error.
func invokeFn(t *testing.T, name, tmplYAML string, exec Executor) *PreparedFunction {
	t.Helper()
	tmpl, err := function.ParseTemplate([]byte(tmplYAML))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return NewPrepared(
		function.Function{Name: name, Template: tmpl},
		&runtime.Prepared{Name: name, Image: "x"},
		exec,
	)
}

// TestInvokeFunctionExecutesMatchingHandler verifies the successful single-rule
// case: the matching handler runs on the executor with the marshalled event and
// a RunMeta classified as an event invocation, and the handler-execution metrics
// are recorded.
func TestInvokeFunctionExecutesMatchingHandler(t *testing.T) {
	exec := &captureExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`, exec)}, testutil.DiscardLogger(), m)

	event := map[string]any{"event_name": "INSERT", "id": 7}
	invoked, err := r.InvokeFunction(context.Background(), "fn", event)
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 1 {
		t.Fatalf("invoked = %d, want 1", invoked)
	}

	handler, payload := exec.got()
	if handler != "events.created.handler" {
		t.Fatalf("handler = %q, want events.created.handler", handler)
	}
	if !strings.Contains(string(payload), `"event_name":"INSERT"`) {
		t.Fatalf("payload = %q, want the marshalled event", payload)
	}
	meta := exec.gotMeta()
	if meta.Type != runtime.ContainerTypeEvent {
		t.Fatalf("RunMeta.Type = %q, want %q", meta.Type, runtime.ContainerTypeEvent)
	}
	if meta.Function != "fn" || meta.Handler != "events.created.handler" {
		t.Fatalf("RunMeta function/handler = %q/%q", meta.Function, meta.Handler)
	}
	if meta.MessageID != "" {
		t.Fatalf("RunMeta.MessageID = %q, want empty (no stream message)", meta.MessageID)
	}

	got := m.Snapshot()
	if !strings.Contains(got, "handler_success_total count=1") {
		t.Fatalf("expected handler_success_total; got:\n%s", got)
	}
	if !strings.Contains(got, "function_handler_success_total{function=fn} count=1") {
		t.Fatalf("expected per-function success; got:\n%s", got)
	}
}

// TestInvokeFunctionMultipleMatchingRules verifies every matching rule runs, in
// declaration order, and the returned count reflects all of them.
func TestInvokeFunctionMultipleMatchingRules(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: first.run
    pattern:
      event_name: [INSERT]
  - handler: second.run
    pattern:
      event_name: [INSERT]
  - handler: skipped.run
    pattern:
      event_name: [MODIFY]
`, exec)}, testutil.DiscardLogger())

	invoked, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"})
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 2 {
		t.Fatalf("invoked = %d, want 2 (only matching rules)", invoked)
	}
	// captureExecutor records the last invocation only; the count plus a
	// successful run is the assertion here (the order is anchored by the
	// single-rule and failure tests, which observe the handler identity).
	if _, payload := exec.got(); len(payload) == 0 {
		t.Fatal("executor was not invoked")
	}
}

// TestInvokeFunctionNoMatchingRules verifies a non-matching event is a
// successful no-op: zero handlers, no error, no execution, and no metrics.
func TestInvokeFunctionNoMatchingRules(t *testing.T) {
	exec := &countingExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`, exec)}, testutil.DiscardLogger(), m)

	invoked, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "DELETE"})
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 0 {
		t.Fatalf("invoked = %d, want 0", invoked)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
	if fs := m.FunctionStatsSnapshot(); len(fs) != 0 {
		t.Fatalf("function stats = %+v, want none for a no-match invocation", fs)
	}
}

// TestInvokeFunctionFailureRunsLaterHandlers verifies a failure in one matching
// handler does not prevent later matching handlers from running, and the first
// failure is returned after all handlers were attempted.
func TestInvokeFunctionFailureRunsLaterHandlers(t *testing.T) {
	failing := &countingExecutor{fail: true}
	// Two rules sharing one executor: the executor fails for the first rule, but
	// the second rule must still be attempted.
	r := New([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: first.run
    pattern:
      event_name: [INSERT]
  - handler: second.run
    pattern:
      event_name: [INSERT]
`, failing)}, testutil.DiscardLogger())

	invoked, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"})
	if err == nil {
		t.Fatal("expected a failure error")
	}
	if invoked != 2 {
		t.Fatalf("invoked = %d, want 2 (both handlers attempted)", invoked)
	}
	if failing.count() != 2 {
		t.Fatalf("executor calls = %d, want 2 (failure must not stop later handlers)", failing.count())
	}
	if !strings.Contains(err.Error(), `handler "first.run"`) {
		t.Fatalf("error = %v, want it to name the first failing handler", err)
	}
}

// TestInvokeFunctionUnknownFunction verifies an absent function is reported with
// the stable not-found sentinel and nothing executes.
func TestInvokeFunctionUnknownFunction(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{invokeFn(t, "present", `runtime: node24
events:
  - handler: index.run
    pattern: {}
`, exec)}, testutil.DiscardLogger())

	invoked, err := r.InvokeFunction(context.Background(), "ghost", map[string]any{"x": 1})
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("err = %v, want ErrFunctionNotFound", err)
	}
	if invoked != 0 {
		t.Fatalf("invoked = %d, want 0", invoked)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0", exec.count())
	}
}

// TestInvokeFunctionUnavailableFunction verifies a registered but unrunnable
// function is reported with the stable unavailable sentinel.
func TestInvokeFunctionUnavailableFunction(t *testing.T) {
	r := New([]*PreparedFunction{
		NewUnavailable(function.Function{Name: "broken", Template: &function.Template{Runtime: "node24"}}),
	}, testutil.DiscardLogger())

	invoked, err := r.InvokeFunction(context.Background(), "broken", map[string]any{"x": 1})
	if !errors.Is(err, ErrFunctionUnavailable) {
		t.Fatalf("err = %v, want ErrFunctionUnavailable", err)
	}
	if invoked != 0 {
		t.Fatalf("invoked = %d, want 0", invoked)
	}
}

// TestInvokeFunctionWritesNoBrokerState verifies manual invocation never touches
// stream invocation state, even when one is present in the context: no
// classification claim, no per-invocation marks, no retry backoffs, no
// exhaustion.
func TestInvokeFunctionWritesNoBrokerState(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: index.run
    pattern: {}
`, exec)}, testutil.DiscardLogger())

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	invoked, err := r.InvokeFunction(ctx, "fn", map[string]any{"event_name": "INSERT"})
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 1 {
		t.Fatalf("invoked = %d, want 1", invoked)
	}
	if len(prog.marks) != 0 {
		t.Fatalf("marks = %v, want none (manual invocation writes no invocation state)", prog.marks)
	}
	if prog.classified {
		t.Fatal("manual invocation must not claim the stream event classification")
	}
	if len(prog.failures) != 0 {
		t.Fatalf("failures = %v, want none", prog.failures)
	}
	if len(prog.exhausted) != 0 {
		t.Fatalf("exhausted = %v, want none", prog.exhausted)
	}
}

// TestInvokeFunctionNoEventClassificationCounters verifies manual invocation
// never increments the events_received/matched/unmatched partition or the
// per-function events-matched counter, even though handler execution metrics
// (which reflect a real execution) are recorded.
func TestInvokeFunctionNoEventClassificationCounters(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: index.run
    pattern: {}
`, &countingExecutor{})}, testutil.DiscardLogger(), m)

	if _, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"}); err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}

	got := m.Snapshot()
	for _, forbidden := range []string{
		"events_received_total",
		"events_matched_total",
		"events_unmatched_total",
		"function_events_matched_total",
		"retries_total",
		"dlq_entries_total",
	} {
		if strings.Contains(got, forbidden) {
			t.Errorf("manual invocation must not increment %s:\n%s", forbidden, got)
		}
	}
	// A real execution still records the handler execution counters.
	if !strings.Contains(got, "handler_success_total count=1") {
		t.Errorf("expected handler_success_total for the real execution:\n%s", got)
	}
}

// TestInvokeFunctionNoRetryOrDLQOnFailure verifies a manual-invocation failure
// records the handler-failure execution metrics (it is a real failed execution)
// but never a retry or DLQ counter: there is no broker retry lifecycle.
func TestInvokeFunctionNoRetryOrDLQOnFailure(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: index.run
    pattern: {}
`, &countingExecutor{fail: true})}, testutil.DiscardLogger(), m)

	if _, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"}); err == nil {
		t.Fatal("expected failure")
	}
	got := m.Snapshot()
	if !strings.Contains(got, "handler_failure_total count=1") {
		t.Errorf("expected handler_failure_total:\n%s", got)
	}
	if strings.Contains(got, "retries_total") || strings.Contains(got, "dlq_entries_total") {
		t.Errorf("manual failure must not count retries/DLQ:\n%s", got)
	}
}

// TestInvokeFunctionResolvesEnvAndSecrets verifies manual invocation reuses the
// per-invocation env/secret resolution, exactly like the event and schedule
// paths.
func TestInvokeFunctionResolvesEnvAndSecrets(t *testing.T) {
	exec := &captureExecutor{}
	pf := fnWithEnv(t, "fn", exec,
		map[string]string{"API_URL": "https://api.example.com"},
		map[string]function.SecretRef{"DATABASE_URL": "db-url"})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())
	r.SetSecretProvider(&fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}})

	if _, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"x": 1}); err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	joined := strings.Join(exec.gotEnv(), " ")
	for _, want := range []string{"API_URL=https://api.example.com", "DATABASE_URL=postgres://secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("extra env missing %q: %v", want, exec.gotEnv())
		}
	}
}

// TestInvokeFunctionTimeoutCap verifies the per-rule timeout is capped at the
// configured maximum exactly like Handle: the executor observes the cap, not the
// larger rule timeout. The function is built directly (not parsed) so the rule
// can carry a timeout above the template-validation cap, isolating the runner's
// runtime cap as the thing under test.
func TestInvokeFunctionTimeoutCap(t *testing.T) {
	r := New([]*PreparedFunction{fnWithTimeout(t, "fn", time.Hour, ctxAwareExecutor{})}, testutil.DiscardLogger())
	r.SetMaxHandlerTimeout(50 * time.Millisecond)

	start := time.Now()
	_, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"x": 1})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the capped handler")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("handler ran %s, expected it to be capped near 50ms", elapsed)
	}
}

// TestInvokeFunctionMixedOutcomesAttributeMetrics verifies a mixed multi-rule
// run records one per-function success and one per-handler failure for the same
// function, with the last_success/last_failure timestamps advanced and no event
// classification.
func TestInvokeFunctionMixedOutcomesAttributeMetrics(t *testing.T) {
	// The first rule fails, the second succeeds. Both share one executor that
	// fails only its first call.
	exec := &flipExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{invokeFn(t, "fn", `runtime: node24
events:
  - handler: first.run
    pattern:
      event_name: [INSERT]
  - handler: second.run
    pattern:
      event_name: [INSERT]
`, exec)}, testutil.DiscardLogger(), m)

	invoked, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"})
	if err == nil {
		t.Fatal("expected the first handler to fail")
	}
	if invoked != 2 {
		t.Fatalf("invoked = %d, want 2", invoked)
	}

	success := m.CounterLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "fn"}})
	failure := m.CounterLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: "fn"}})
	if success != 1 || failure != 1 {
		t.Fatalf("per-function success/failure = %d/%d, want 1/1", success, failure)
	}
	fs := m.FunctionStatsSnapshot()
	if len(fs) != 1 {
		t.Fatalf("function stats = %+v, want one entry", fs)
	}
	if fs[0].LastSuccess == 0 || fs[0].LastFailure == 0 || fs[0].LastExecution == 0 {
		t.Fatalf("expected execution/success/failure timestamps, got %+v", fs[0])
	}
	if fs[0].EventsMatchedTotal != 0 {
		t.Fatalf("events matched = %d, want 0 (manual invocation classifies no event)", fs[0].EventsMatchedTotal)
	}
}

// flipExecutor fails its first invocation and succeeds thereafter, so a
// multi-rule manual invocation exercises both outcome branches with one
// executor.
type flipExecutor struct {
	calls int
}

func (f *flipExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	f.calls++
	if f.calls == 1 {
		return errors.New("first fails")
	}
	return nil
}

package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// InvokeHandler executes the handler with the schedule payload on the executor
// and no invocation state; a successful run records success metrics.
func TestInvokeHandlerSuccess(t *testing.T) {
	exec := &captureExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "fn", exec, function.DefaultTimeout)}, testutil.DiscardLogger(), m)

	err := r.InvokeHandler(context.Background(), "1-0", "fn", "index.run", []byte(`{"source":"relay.schedule"}`))
	if err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}

	handler, payload := exec.got()
	if handler != "index.run" {
		t.Fatalf("handler = %q, want index.run", handler)
	}
	if !strings.Contains(string(payload), "relay.schedule") {
		t.Fatalf("payload = %q, want schedule payload", payload)
	}

	got := m.Snapshot()
	if !strings.Contains(got, "handler_success_total count=1") {
		t.Fatalf("expected handler_success_total; got:\n%s", got)
	}
	if !strings.Contains(got, "handler_invocations_total{function=fn,handler=index.run,outcome=success} count=1") {
		t.Fatalf("expected success invocation counter; got:\n%s", got)
	}
	if !strings.Contains(got, "function_handler_success_total{function=fn} count=1") {
		t.Fatalf("expected per-function success; got:\n%s", got)
	}
	// InvokeHandler (called directly here, without the stream layer) must not
	// touch the event classification counters: received/matched/unmatched are
	// counted by Handle's event path, not by the schedule execution path.
	if strings.Contains(got, metrics.MetricEventsReceived) ||
		strings.Contains(got, metrics.MetricEventsMatched) ||
		strings.Contains(got, metrics.MetricEventsUnmatched) {
		t.Fatalf("schedule must not inflate event counters:\n%s", got)
	}
	if strings.Contains(got, metrics.MetricFunctionEventsMatched) {
		t.Fatalf("schedule must not inflate function_events_matched_total:\n%s", got)
	}
}

// A failing invocation records failure metrics and returns the error.
func TestInvokeHandlerFailure(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "fn", &countingExecutor{fail: true}, function.DefaultTimeout)}, testutil.DiscardLogger(), m)

	err := r.InvokeHandler(context.Background(), "1-0", "fn", "index.run", []byte(`{}`))
	if err == nil {
		t.Fatal("expected InvokeHandler to fail")
	}

	got := m.Snapshot()
	if !strings.Contains(got, "handler_failure_total count=1") {
		t.Fatalf("expected handler_failure_total; got:\n%s", got)
	}
	if !strings.Contains(got, "handler_invocations_total{function=fn,handler=index.run,outcome=failure} count=1") {
		t.Fatalf("expected failure invocation counter; got:\n%s", got)
	}
	if !strings.Contains(got, "function_handler_failure_total{function=fn} count=1") {
		t.Fatalf("expected per-function failure; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=success") {
		t.Fatalf("unexpected success metric; got:\n%s", got)
	}
}

// Missing or unavailable functions return an error without executing.
func TestInvokeHandlerMissingFunction(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedFunction{schedFn(t, "present", exec, function.DefaultTimeout)}, testutil.DiscardLogger())

	err := r.InvokeHandler(context.Background(), "1-0", "ghost", "index.run", []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), `function "ghost" is not available`) {
		t.Fatalf("err = %v, want not-available error", err)
	}
	if _, payload := exec.got(); len(payload) != 0 {
		t.Fatalf("executor should not run for a missing function")
	}

	// Unavailable (nil prepared) function.
	r = New([]*PreparedFunction{NewUnavailable(function.Function{Name: "broken", Template: &function.Template{Runtime: "node24"}})}, testutil.DiscardLogger())
	err = r.InvokeHandler(context.Background(), "1-0", "broken", "index.run", []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), `function "broken" is not available`) {
		t.Fatalf("err = %v, want not-available error for unavailable function", err)
	}
}

// SetMaxHandlerTimeout caps the schedule timeout exactly like a rule timeout:
// the executor observes a deadline at the cap, not the larger schedule timeout.
func TestInvokeHandlerTimeoutCap(t *testing.T) {
	r := New([]*PreparedFunction{schedFn(t, "fn", ctxAwareExecutor{}, time.Hour)}, testutil.DiscardLogger())
	r.SetMaxHandlerTimeout(50 * time.Millisecond)

	start := time.Now()
	err := r.InvokeHandler(context.Background(), "1-0", "fn", "index.run", []byte(`{}`))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a deadline error from the capped handler")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("handler ran %s, expected it to be capped near 50ms", elapsed)
	}
}

// InvokeHandler resolves template env values and secret references into the
// per-invocation extra env, like Handle.
func TestInvokeHandlerSecretResolution(t *testing.T) {
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}}
	pf := fnWithEnv(t, "fn", exec,
		map[string]string{"API_URL": "https://api.example.com"},
		map[string]function.SecretRef{"DATABASE_URL": "db-url"})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())
	r.SetSecretProvider(prov)

	err := r.InvokeHandler(context.Background(), "1-0", "fn", "index.run", []byte(`{}`))
	if err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	extra := exec.gotEnv()
	joined := strings.Join(extra, " ")
	for _, want := range []string{"API_URL=https://api.example.com", "DATABASE_URL=postgres://secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("extra env missing %q: %v", want, extra)
		}
	}
}

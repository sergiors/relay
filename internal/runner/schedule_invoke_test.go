package runner

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
)

// payloadCaptureExecutor records the handler and event JSON it was invoked
// with, so a test can assert the schedule payload reached the executor.
type payloadCaptureExecutor struct {
	mu      sync.Mutex
	handler string
	payload []byte
}

func (e *payloadCaptureExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, eventJSON []byte, _ []string) error {
	e.mu.Lock()
	e.handler = handler
	e.payload = append([]byte(nil), eventJSON...)
	e.mu.Unlock()
	return nil
}

func (e *payloadCaptureExecutor) got() (string, []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.handler, append([]byte(nil), e.payload...)
}

// schedFn returns a prepared function with an empty rule set but that still
// carries the runtime/template the InvokeHandler path reads (env/secrets). Its
// schedule entry carries the given timeout, which is what InvokeHandler now
// resolves (the template's schedule entry being the single source of truth).
func schedFn(t *testing.T, name string, executor Executor, scheduleTimeout time.Duration) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{
			Name: name,
			Template: &function.Template{
				Runtime: "node24",
				Rules:   []function.Rule{{Handler: "index.run", Pattern: function.Pattern{}, Timeout: scheduleTimeout, Retries: function.DefaultRetries}},
				Schedules: []function.Schedule{{
					Handler:  "index.run",
					Cron:     "0 3 * * *",
					Location: time.UTC,
					Timeout:  scheduleTimeout,
				}},
			},
		},
		&runtime.Prepared{Name: name, Image: "x"},
		executor,
	)
}

// InvokeHandler executes the handler with the schedule payload on the executor
// and no invocation state; a successful run records success metrics.
func TestInvokeHandlerSuccess(t *testing.T) {
	exec := &payloadCaptureExecutor{}
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "fn", exec, function.DefaultTimeout)}, silentLogger(), m)

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
	// inflate the event counters: events_processed_total is counted by the stream
	// layer (stream.processScheduleMessage) and events_received_total by Handle's
	// message path, not by the schedule execution path.
	if strings.Contains(got, metrics.MetricEventsReceived) || strings.Contains(got, metrics.MetricEventsProcessed) {
		t.Fatalf("schedule must not inflate event counters:\n%s", got)
	}
	if strings.Contains(got, metrics.MetricFunctionEvents) {
		t.Fatalf("schedule must not inflate function_events_total:\n%s", got)
	}
}

// A failing invocation records failure metrics and returns the error.
func TestInvokeHandlerFailure(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics([]*PreparedFunction{schedFn(t, "fn", &fixedExecutor{err: true}, function.DefaultTimeout)}, silentLogger(), m)

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
	exec := &payloadCaptureExecutor{}
	r := New([]*PreparedFunction{schedFn(t, "present", exec, function.DefaultTimeout)}, silentLogger())

	err := r.InvokeHandler(context.Background(), "1-0", "ghost", "index.run", []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), `function "ghost" is not available`) {
		t.Fatalf("err = %v, want not-available error", err)
	}
	if _, payload := exec.got(); len(payload) != 0 {
		t.Fatalf("executor should not run for a missing function")
	}

	// Unavailable (nil prepared) function.
	r = New([]*PreparedFunction{NewUnavailable(function.Function{Name: "broken", Template: &function.Template{Runtime: "node24"}})}, silentLogger())
	err = r.InvokeHandler(context.Background(), "1-0", "broken", "index.run", []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), `function "broken" is not available`) {
		t.Fatalf("err = %v, want not-available error for unavailable function", err)
	}
}

// SetMaxHandlerTimeout caps the schedule timeout exactly like a rule timeout:
// the executor observes a deadline at the cap, not the larger schedule timeout.
func TestInvokeHandlerTimeoutCap(t *testing.T) {
	r := New([]*PreparedFunction{schedFn(t, "fn", ctxAwareExecutor{}, time.Hour)}, silentLogger())
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
	exec := &envCaptureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}}
	pf := fnWithEnv(t, "fn", exec,
		map[string]string{"API_URL": "https://api.example.com"},
		map[string]function.SecretRef{"DATABASE_URL": "db-url"})
	r := New([]*PreparedFunction{pf}, silentLogger())
	r.SetSecretProvider(prov)

	err := r.InvokeHandler(context.Background(), "1-0", "fn", "index.run", []byte(`{}`))
	if err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	extra := exec.got()
	joined := strings.Join(extra, " ")
	for _, want := range []string{"API_URL=https://api.example.com", "DATABASE_URL=postgres://secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("extra env missing %q: %v", want, extra)
		}
	}
}

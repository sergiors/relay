package runner

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"relay/internal/observability/tracing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// spanRecorder bundles the in-memory exporter with the provider that owns it, so
// a test can force-flush before reading (production Setup uses a batch
// processor; the tests flush synchronously rather than waiting for its timer).
type spanRecorder struct {
	exp      *tracetest.InMemoryExporter
	provider *tracing.Provider
}

// withSpanRecorder installs an SDK tracer provider backed by an in-memory
// recorder through the production Setup seam, restoring the previous global
// provider on cleanup. No test hook is added to production code: Setup is the
// same path the worker uses, only the exporter is swapped. The runner package's
// tests do not run in parallel, so the global provider swap is race-free within
// the package.
func withSpanRecorder(t *testing.T) *spanRecorder {
	t.Helper()
	// The production Setup honors OTEL_SDK_DISABLED; clear it so an ambient
	// value in the test environment cannot disable the injected exporter.
	t.Setenv("OTEL_SDK_DISABLED", "")
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	return &spanRecorder{exp: exp, provider: provider}
}

// flush forces the batch processor to export, then returns the recorded spans.
func (r *spanRecorder) flush(t *testing.T) tracetest.SpanStubs {
	t.Helper()
	if err := r.provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	return r.exp.GetSpans()
}

// spanByName returns the single recorded span with the given name, or fails the
// test.
func spanByName(t *testing.T, rec *spanRecorder, name string) tracetest.SpanStub {
	t.Helper()
	spans := rec.flush(t)
	var found []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d spans named %q, want 1; all: %v", len(found), name, spanNames(spans))
	}
	return found[0]
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

// attrString returns the string value of a recorded span attribute, or "".
func attrString(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestHandleEmitsFunctionInvokeSpan proves the shared invocation path emits one
// function.invoke span per executed handler, carrying the low-cardinality
// function/handler/runtime attributes and a success result.
func TestHandleEmitsFunctionInvokeSpan(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	if err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	span := spanByName(t, exp, "function.invoke")
	if got := attrString(span, "function.name"); got != "demo" {
		t.Errorf("function.name = %q, want demo", got)
	}
	if got := attrString(span, "function.handler"); got != "index.run" {
		t.Errorf("function.handler = %q, want index.run", got)
	}
	if got := attrString(span, "function.runtime"); got != "node24" {
		t.Errorf("function.runtime = %q, want node24", got)
	}
	if got := attrString(span, "function.result"); got != "success" {
		t.Errorf("function.result = %q, want success", got)
	}
	if span.Status.Code != codes.Ok {
		t.Errorf("span status = %v, want codes.Ok", span.Status.Code)
	}
}

// TestHandleFailureSpanRecordsError proves a failed invocation records the error
// and sets codes.Error on its function.invoke span, so a trace surfaces the
// failure at the boundary while the existing retry behavior is unchanged.
func TestHandleFailureSpanRecordsError(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &countingExecutor{fail: true}
	r := New([]*PreparedFunction{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"})
	if err == nil {
		t.Fatal("Handle returned nil, want the executor failure")
	}

	span := spanByName(t, exp, "function.invoke")
	if got := attrString(span, "function.result"); got != "failure" {
		t.Errorf("function.result = %q, want failure", got)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want codes.Error", span.Status.Code)
	}
	if len(span.Events) == 0 {
		t.Error("failure span has no recorded error event")
	}
}

// TestRunInvocationSpanPropagatesToExecutor proves the function.invoke span's
// context reaches the executor, so a runtime layer can nest its own spans under
// the invocation span.
func TestRunInvocationSpanPropagatesToExecutor(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &spanAwareExecutor{}
	r := New([]*PreparedFunction{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	if err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !exec.sawSpan {
		t.Fatal("executor context carries no recording span; function.invoke was not propagated")
	}
	// The child span the executor started must be parented to function.invoke.
	child := spanByName(t, exp, "test.executor.child")
	parent := spanByName(t, exp, "function.invoke")
	if child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Errorf("child parent = %s, want function.invoke %s", child.Parent.SpanID(), parent.SpanContext.SpanID())
	}
}

// spanAwareExecutor starts a child span from its invocation context so the test
// can prove the trace context is handed to the executor.
type spanAwareExecutor struct {
	sawSpan bool
}

func (e *spanAwareExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	_, span := tracing.Start(ctx, "test.executor.child")
	e.sawSpan = span.IsRecording()
	span.End()
	return nil
}

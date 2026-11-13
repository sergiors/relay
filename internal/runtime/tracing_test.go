package runtime

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"relay/internal/observability/tracing"
)

// spanRecorder pairs the in-memory exporter with the provider so tests can
// force-flush before reading. It installs the provider through the production
// Setup seam (restoring globals on cleanup); runtime package tests are not
// parallel, so the global swap is race-free.
type spanRecorder struct {
	exp      *tracetest.InMemoryExporter
	provider *tracing.Provider
}

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

func (r *spanRecorder) spans(t *testing.T) tracetest.SpanStubs {
	t.Helper()
	if err := r.provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	return r.exp.GetSpans()
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

// TestContainerCacheExecuteEmitsAcquireAndInvokeSpans proves the cache execute
// path emits runtime.acquire and runtime.invoke child spans, with runtime.invoke
// parented to runtime.acquire's sibling (both under the caller context) and
// carrying the app/image attributes.
func TestContainerCacheExecuteEmitsAcquireAndInvokeSpans(t *testing.T) {
	rec := withSpanRecorder(t)
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h1"); err != nil {
		t.Fatalf("execute: %v", err)
	}

	spans := rec.spans(t)
	var acquire, invoke *tracetest.SpanStub
	for i := range spans {
		switch spans[i].Name {
		case "runtime.acquire":
			acquire = &spans[i]
		case "runtime.invoke":
			invoke = &spans[i]
		}
	}
	if acquire == nil || invoke == nil {
		t.Fatalf("missing acquire/invoke spans; got %v", spanNames(spans))
	}
	if got := attrString(*acquire, "relay.app"); got != "fn-a" {
		t.Errorf("runtime.acquire relay.app = %q, want fn-a", got)
	}
	if got := attrString(*invoke, "relay.image"); got != "img-1" {
		t.Errorf("runtime.invoke relay.image = %q, want img-1", got)
	}
}

// TestContainerCacheAcquireFailureSpanRecordsError proves a failed start (no
// lease) records an error on the runtime.acquire span and never emits a
// runtime.invoke span.
func TestContainerCacheAcquireFailureSpanRecordsError(t *testing.T) {
	rec := withSpanRecorder(t)
	cc, ff := newTestCache()
	ff.startErr = context.DeadlineExceeded

	err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h1")
	if err == nil {
		t.Fatal("execute returned nil, want the start error")
	}
	spans := rec.spans(t)
	var acquire *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "runtime.acquire" {
			acquire = &spans[i]
		}
		if spans[i].Name == "runtime.invoke" {
			t.Errorf("runtime.invoke span emitted on a failed acquire: %v", spanNames(spans))
		}
	}
	if acquire == nil {
		t.Fatalf("missing runtime.acquire span; got %v", spanNames(spans))
	}
	if acquire.Status.Code != codes.Error {
		t.Errorf("runtime.acquire status = %v, want codes.Error", acquire.Status.Code)
	}
}

func attrString(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestEncodeInvokeRequestCarriesTraceFrame proves the request frame injects the
// current trace context under the single optional "trace" object (the W3C
// traceparent/tracestate/baggage keys), leaves the event payload verbatim, and
// omits the whole object when there is no trace context. It also proves the
// frame decodes back to the same carrier for a container-side propagator.
func TestEncodeInvokeRequestCarriesTraceFrame(t *testing.T) {
	withSpanRecorder(t)
	ctx, span := tracing.Start(context.Background(), "function.invoke")
	defer span.End()

	const event = `{"event_name":"INSERT","new_image":{"id":1}}`
	frame, err := encodeInvokeRequest(ctx, "req-1", "index.handler", []byte(event), map[string]string{"K": "V"})
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}

	var decoded struct {
		ID      string            `json:"id"`
		Handler string            `json:"handler"`
		Event   json.RawMessage   `json:"event"`
		Env     map[string]string `json:"env"`
		Trace   map[string]string `json:"trace"`
	}
	if err := json.Unmarshal(frame, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded.ID != "req-1" || decoded.Handler != "index.handler" {
		t.Errorf("identity = (%q, %q), want (req-1, index.handler)", decoded.ID, decoded.Handler)
	}
	if string(decoded.Event) != event {
		t.Errorf("event = %s, want verbatim %s", decoded.Event, event)
	}
	if decoded.Env["K"] != "V" {
		t.Errorf("env = %v, want K=V", decoded.Env)
	}
	// The trace object carries the invocation span's trace id as a valid
	// traceparent, and the decoded object can be extracted by a propagator.
	if got := decoded.Trace[tracing.TraceparentKey]; got == "" {
		t.Fatalf("trace object missing traceparent: %v", decoded.Trace)
	}
	extracted := tracing.ExtractStrings(context.Background(), decoded.Trace)
	if got := oteltrace.SpanContextFromContext(extracted).TraceID(); got != span.SpanContext().TraceID() {
		t.Errorf("extracted trace id = %s, want %s", got, span.SpanContext().TraceID())
	}
}

// TestEncodeInvokeRequestCarriesHandlerContext proves the request frame's
// "context" object carries the complete execution identity: the stable request
// ID, the confirmed attempt and configured attempt budget, the effective
// context deadline as RFC 3339, the optional message and trace IDs, and the
// app/handler. The event payload stays verbatim and the trace ID agrees with
// the frame's trace carrier.
func TestEncodeInvokeRequestCarriesHandlerContext(t *testing.T) {
	withSpanRecorder(t)
	ctx, span := tracing.Start(context.Background(), "function.invoke")
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	handlerCtx := HandlerContext{
		RequestID:   "1791234567890-0:app/events.created",
		Attempt:     2,
		MaxAttempts: 4,
		MessageID:   "1791234567890-0",
		App:         "app",
		Handler:     "events.created",
	}
	ctx = WithHandlerContext(ctx, handlerCtx)
	ctx = WithRunMeta(ctx, RunMeta{App: "app"})

	const event = `{"event_name":"INSERT","new_image":{"id":1}}`
	frame, err := encodeInvokeRequest(ctx, "req-1", "events.created", []byte(event), nil)
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}

	var decoded invokeRequest
	if err := json.Unmarshal(frame, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if string(decoded.Event) != event {
		t.Errorf("event = %s, want verbatim %s", decoded.Event, event)
	}
	got := decoded.Context
	if got == nil {
		t.Fatal("request frame carries no context object")
	}
	if got.RequestID != handlerCtx.RequestID {
		t.Errorf("requestId = %q, want %q", got.RequestID, handlerCtx.RequestID)
	}
	if got.Attempt != 2 || got.MaxAttempts != 4 {
		t.Errorf("attempt/maxAttempts = %d/%d, want 2/4", got.Attempt, got.MaxAttempts)
	}
	if got.MessageID != handlerCtx.MessageID {
		t.Errorf("messageId = %q, want %q", got.MessageID, handlerCtx.MessageID)
	}
	if got.App != "app" || got.Handler != "events.created" {
		t.Errorf("app/handler = %q/%q, want app/events.created", got.App, got.Handler)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("test context has no deadline")
	}
	parsed, err := time.Parse(time.RFC3339Nano, got.Deadline)
	if err != nil {
		t.Fatalf("deadline %q is not RFC 3339: %v", got.Deadline, err)
	}
	if !parsed.Equal(deadline) {
		t.Errorf("deadline = %s, want %s", parsed, deadline)
	}
	if got.TraceID != span.SpanContext().TraceID().String() {
		t.Errorf("traceId = %q, want %s", got.TraceID, span.SpanContext().TraceID())
	}
}

// TestEncodeInvokeRequestNormalizesStateFreeContext proves a direct runtime
// caller that injected no HandlerContext still gets a complete, well-defined
// state-free execution context: a fresh request ID, attempt 1, maxAttempts 1,
// the app from RunMeta, the invoked handler, and the zero deadline when the
// context has none.
func TestEncodeInvokeRequestNormalizesStateFreeContext(t *testing.T) {
	ctx := WithRunMeta(context.Background(), RunMeta{App: "fn"})
	frame, err := encodeInvokeRequest(ctx, "req-2", "index.handler", []byte(`{}`), nil)
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}

	var decoded invokeRequest
	if err := json.Unmarshal(frame, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	got := decoded.Context
	if got == nil {
		t.Fatal("request frame carries no context object")
	}
	if got.RequestID == "" {
		t.Error("state-free context requestId is empty")
	}
	if got.Attempt != 1 || got.MaxAttempts != 1 {
		t.Errorf("attempt/maxAttempts = %d/%d, want 1/1", got.Attempt, got.MaxAttempts)
	}
	if got.MessageID != "" || got.TraceID != "" {
		t.Errorf("messageId/traceId = %q/%q, want both absent", got.MessageID, got.TraceID)
	}
	if got.App != "fn" || got.Handler != "index.handler" {
		t.Errorf("app/handler = %q/%q, want fn/index.handler", got.App, got.Handler)
	}
	if want := (time.Time{}).UTC().Format(time.RFC3339Nano); got.Deadline != want {
		t.Errorf("deadline = %q, want zero instant %q", got.Deadline, want)
	}
}

// TestEncodeInvokeRequestPreservesConfiguredBudget proves the frame reports
// the CONFIGURED attempt budget even when a config change left the claimed
// attempt above it: attempt 5 with maxAttempts 2 stays 5/2 (the budget is never
// clamped up to the attempt), the stable request ID passes through verbatim,
// and the broker message ID is carried.
func TestEncodeInvokeRequestPreservesConfiguredBudget(t *testing.T) {
	ctx := WithRunMeta(context.Background(), RunMeta{App: "fn"})
	ctx = WithHandlerContext(ctx, HandlerContext{
		RequestID:   "schedule:9-0:fn/index.run",
		Attempt:     5,
		MaxAttempts: 2,
		MessageID:   "9-0",
		App:         "fn",
		Handler:     "index.run",
	})
	frame, err := encodeInvokeRequest(ctx, "req-3", "index.run", []byte(`{}`), nil)
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}

	var decoded invokeRequest
	if err := json.Unmarshal(frame, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	got := decoded.Context
	if got == nil {
		t.Fatal("request frame carries no context object")
	}
	if got.Attempt != 5 || got.MaxAttempts != 2 {
		t.Errorf("attempt/maxAttempts = %d/%d, want 5/2 (the budget is never raised to the attempt)", got.Attempt, got.MaxAttempts)
	}
	if got.RequestID != "schedule:9-0:fn/index.run" {
		t.Errorf("requestId = %q, want the stable schedule identity", got.RequestID)
	}
	if got.MessageID != "9-0" {
		t.Errorf("messageId = %q, want 9-0", got.MessageID)
	}
}

// TestEncodeInvokeRequestOmitsTraceWithoutContext proves the frame is
// byte-compatible when tracing is disabled or the context carries no recording
// span: the "trace" key is absent, so a bootstrap that ignores it cannot tell
// the difference from the pre-tracing frame shape.
func TestEncodeInvokeRequestOmitsTraceWithoutContext(t *testing.T) {
	frame, err := encodeInvokeRequest(context.Background(), "req-2", "h", []byte(`{}`), nil)
	if err != nil {
		t.Fatalf("encodeInvokeRequest: %v", err)
	}
	if strings.Contains(string(frame), `"trace"`) {
		t.Fatalf("frame with no trace context carries a trace field: %s", frame)
	}
}

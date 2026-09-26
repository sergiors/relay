package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"relay/internal/observability/tracing"
	"relay/internal/schedule"
)

// withSpanRecorder installs an SDK provider backed by an in-memory exporter
// through the production Setup seam, restoring the globals on cleanup. The
// stream package's tests do not run in parallel, so the global swap is
// race-free within the package.
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

func spanByName(t *testing.T, rec *spanRecorder, name string) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range rec.spans(t) {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d spans named %q, want 1", len(found), name)
	}
	return found[0]
}

func attrString(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// processMessageConsumer builds a Consumer whose invocation-state seam is the
// in-memory fake, so processMessage can be driven directly without Redis.
func processMessageConsumer(store invocationStateStore) *Consumer {
	return newConsumer(ConsumerConfig{
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
}

// TestProcessMessageEmitsStreamSpanWithContext proves the per-message span is
// emitted with low-cardinality messaging attributes and that its context (and
// the dispatch child span) reaches the handler, so the runner's function.invoke
// nests under it. The handler returns a retryable error so no Redis round trip
// is needed (the message is left pending).
func TestProcessMessageEmitsStreamSpanWithContext(t *testing.T) {
	rec := withSpanRecorder(t)
	c := processMessageConsumer(newFakeInvocationStore(nil))

	var sawChildOfDispatch bool
	handler := func(ctx context.Context, _ string, _ map[string]any) error {
		// The handler sees the stream.message -> stream.dispatch context.
		_, span := tracing.Start(ctx, "test.handler")
		sawChildOfDispatch = span.IsRecording()
		span.End()
		return errors.New("retryable failure")
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
	c.processMessage(context.Background(), msg, 1, handler)

	streamSpan := spanByName(t, rec, "stream.message")
	if got := attrString(streamSpan, "messaging.destination.name"); got != "s" {
		t.Errorf("messaging.destination.name = %q, want s", got)
	}
	if got := attrString(streamSpan, "messaging.consumer.group.name"); got != "g" {
		t.Errorf("messaging.consumer.group.name = %q, want g", got)
	}
	if got := attrString(streamSpan, "relay.outcome"); got != "pending" {
		t.Errorf("relay.outcome = %q, want pending (retryable failure)", got)
	}
	if len(streamSpan.Events) == 0 {
		// The retryable handler error is recorded by the dispatch span, not the
		// message span (which records the delivery outcome, not the handler error).
		t.Log("stream.message has no error event (expected; the dispatch child records it)")
	}

	dispatch := spanByName(t, rec, "stream.dispatch")
	if dispatch.Status.Code != codes.Error {
		t.Errorf("stream.dispatch status = %v, want codes.Error", dispatch.Status.Code)
	}
	if dispatch.Parent.SpanID() != streamSpan.SpanContext.SpanID() {
		t.Errorf("dispatch parent = %s, want stream.message %s", dispatch.Parent.SpanID(), streamSpan.SpanContext.SpanID())
	}
	if !sawChildOfDispatch {
		t.Error("handler context carries no recording span; trace context did not reach the handler")
	}
}

// TestProcessMessageExtractsTraceMetadataAsRemoteParent proves that flat W3C
// trace metadata carried beside the event payload (traceparent/tracestate/
// baggage, as the schedule publisher writes) is extracted BEFORE the
// stream.message span: the span becomes a child of the remote parent (same trace
// id, remote parent span id) and the event payload is still delivered untouched.
func TestProcessMessageExtractsTraceMetadataAsRemoteParent(t *testing.T) {
	rec := withSpanRecorder(t)
	c := processMessageConsumer(newFakeInvocationStore(nil))

	// A known remote parent: trace id 4bf92f... / span id 00f067....
	const remoteTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	remoteTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	remoteSpanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")

	const payload = `{"event_name":"X","new_image":{"id":7}}`
	var gotEvent map[string]any
	handler := func(_ context.Context, _ string, event map[string]any) error {
		gotEvent = event
		return errors.New("retryable failure")
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{
		"event":       payload,
		"traceparent": remoteTraceparent,
		"tracestate":  "vendor=opaque",
		"baggage":     "k=v",
	}}
	c.processMessage(context.Background(), msg, 1, handler)

	streamSpan := spanByName(t, rec, "stream.message")
	if streamSpan.SpanContext.TraceID() != remoteTraceID {
		t.Errorf("stream.message trace id = %s, want remote %s", streamSpan.SpanContext.TraceID(), remoteTraceID)
	}
	if streamSpan.Parent.SpanID() != remoteSpanID {
		t.Errorf("stream.message parent span id = %s, want remote %s", streamSpan.Parent.SpanID(), remoteSpanID)
	}
	if !streamSpan.Parent.IsRemote() {
		t.Error("stream.message parent is not marked remote; trace metadata was not extracted")
	}

	// The event payload must be delivered verbatim; trace metadata never leaks
	// into the handler's event map.
	if gotEvent == nil {
		t.Fatal("handler received no event")
	}
	if gotEvent["event_name"] != "X" {
		t.Errorf("event_name = %v, want X", gotEvent["event_name"])
	}
	if _, leaked := gotEvent["traceparent"]; leaked {
		t.Error("traceparent leaked into the event payload")
	}
}

// TestProcessMessageWithoutTraceMetadataIsRoot proves an ordinary external event
// (no trace fields) yields a root stream.message span: extraction is a no-op and
// no remote parent is attached. This is the default path for non-schedule
// producers (e.g. a raw `redis-cli XADD ... event '...'`).
func TestProcessMessageWithoutTraceMetadataIsRoot(t *testing.T) {
	rec := withSpanRecorder(t)
	c := processMessageConsumer(newFakeInvocationStore(nil))

	handler := func(context.Context, string, map[string]any) error {
		return errors.New("retryable failure")
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
	c.processMessage(context.Background(), msg, 1, handler)

	streamSpan := spanByName(t, rec, "stream.message")
	if streamSpan.Parent.IsValid() {
		t.Errorf("stream.message has remote parent %s, want a root span", streamSpan.Parent.SpanID())
	}
}

// TestTraceCarrierFromMessageIgnoresNonStringAndEmpty proves the field mapper
// only takes non-empty string values under the exact W3C keys, so a malformed or
// unrelated field can never fabricate a carrier (and the "event" payload is
// never consulted).
func TestTraceCarrierFromMessageIgnoresNonStringAndEmpty(t *testing.T) {
	got := traceCarrierFromMessage(map[string]any{
		"event":       `{"event_name":"X"}`,
		"traceparent": 123, // non-string
		"tracestate":  "",  // empty
		"unrelated":   "x",
	})
	if got != nil {
		t.Fatalf("traceCarrierFromMessage = %v, want nil", got)
	}

	got = traceCarrierFromMessage(map[string]any{
		"event":       `{"event_name":"X"}`,
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	})
	if got == nil || got[tracing.TraceparentKey] == "" {
		t.Fatalf("traceCarrierFromMessage = %v, want the traceparent field", got)
	}
	if _, ok := got["event"]; ok {
		t.Error("traceCarrierFromMessage copied the event payload into the carrier")
	}
}

// TestProcessMessageDisabledTracingStillProcesses proves a message processes
// normally with tracing disabled (the default): no exporter, no network, and no
// change to the at-least-once contract (the retryable error is returned to the
// caller via the span's outcome still pending).
func TestProcessMessageDisabledTracingStillProcesses(t *testing.T) {
	// Explicitly install a disabled provider (the default state).
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	otel.SetTracerProvider(noop.NewTracerProvider())

	c := processMessageConsumer(newFakeInvocationStore(nil))
	called := false
	handler := func(context.Context, string, map[string]any) error {
		called = true
		return errors.New("retryable failure")
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"X"}`}}
	c.processMessage(context.Background(), msg, 1, handler)
	if !called {
		t.Fatal("handler was not called with tracing disabled")
	}
}

// TestProcessMessageRedeliveryDistinctSpansSameUpstreamTrace proves the
// redelivery semantics: two deliveries of the SAME stream entry (reusing its
// flat traceparent metadata) produce DISTINCT stream.message spans (different
// span ids) that belong to the SAME upstream trace, and each delivery's handler
// runs under its own delivery span. This is the core "retries reprocess the
// original entry" behavior.
func TestProcessMessageRedeliveryDistinctSpansSameUpstreamTrace(t *testing.T) {
	rec := withSpanRecorder(t)
	c := processMessageConsumer(newFakeInvocationStore(nil))

	const remoteTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	remoteTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{
		"event":       `{"event_name":"X"}`,
		"traceparent": remoteTraceparent,
	}}
	handler := func(context.Context, string, map[string]any) error {
		return errors.New("retryable failure")
	}
	// Delivery 1 and a redelivery (delivery 2) of the same message.
	c.processMessage(context.Background(), msg, 1, handler)
	c.processMessage(context.Background(), msg, 2, handler)

	spans := rec.spans(t)
	var messages []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "stream.message" {
			messages = append(messages, s)
		}
	}
	if len(messages) != 2 {
		t.Fatalf("stream.message spans = %d, want 2 (one per delivery)", len(messages))
	}
	if messages[0].SpanContext.SpanID() == messages[1].SpanContext.SpanID() {
		t.Fatal("redelivery reused the same stream.message span id; deliveries must be distinct spans")
	}
	for i, m := range messages {
		if m.SpanContext.TraceID() != remoteTraceID {
			t.Errorf("delivery %d trace id = %s, want the shared upstream %s", i+1, m.SpanContext.TraceID(), remoteTraceID)
		}
		if m.Parent.SpanID().String() != "00f067aa0ba902b7" {
			t.Errorf("delivery %d parent = %s, want the reused upstream parent", i+1, m.Parent.SpanID())
		}
	}
}

// TestScheduleRedeliveryDistinctSpansSameUpstreamTrace proves schedule
// occurrences ride the same redelivery semantics: two deliveries of one schedule
// message produce distinct stream.message spans under the one upstream trace,
// with the schedule runner invoked on each eligible delivery. The schedule
// runner returns a retryable error so the message stays pending (no Redis round
// trip) while still exercising the schedule path.
func TestScheduleRedeliveryDistinctSpansSameUpstreamTrace(t *testing.T) {
	rec := withSpanRecorder(t)
	occ := schedule.Occurrence{
		Function:    "courses",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	envelope, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	calls := 0
	c := processMessageConsumer(newFakeInvocationStore(nil))
	c.scheduleRunner = func(context.Context, string, string, string, []byte) error {
		calls++
		return errors.New("retryable schedule failure")
	}

	const remoteTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	remoteTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	msg := redis.XMessage{ID: "9-0", Values: map[string]any{
		"event":       string(envelope),
		"traceparent": remoteTraceparent,
	}}
	handler := func(context.Context, string, map[string]any) error { return nil }

	c.processMessage(context.Background(), msg, 1, handler)
	c.processMessage(context.Background(), msg, 2, handler)
	if calls != 2 {
		t.Fatalf("schedule runner calls = %d, want 2 (both deliveries eligible)", calls)
	}

	var messages []tracetest.SpanStub
	for _, s := range rec.spans(t) {
		if s.Name == "stream.message" {
			messages = append(messages, s)
		}
	}
	if len(messages) != 2 {
		t.Fatalf("stream.message spans = %d, want 2", len(messages))
	}
	if messages[0].SpanContext.SpanID() == messages[1].SpanContext.SpanID() {
		t.Fatal("schedule redelivery reused the same span id; deliveries must be distinct")
	}
	for i, m := range messages {
		if m.SpanContext.TraceID() != remoteTraceID {
			t.Errorf("schedule delivery %d trace id = %s, want shared upstream %s", i+1, m.SpanContext.TraceID(), remoteTraceID)
		}
	}
}

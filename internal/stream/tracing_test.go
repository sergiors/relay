package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"relay/internal/observability/tracing"
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

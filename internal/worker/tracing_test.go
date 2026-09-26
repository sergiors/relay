package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"relay/internal/observability/tracing"
)

// TestRunDisabledTracingNoNetworkNoCollector proves that a default worker
// configuration (no OTEL endpoint) never constructs an exporter or opens a
// connection: tracing.Setup with the worker's environment returns a disabled
// provider. Run itself is not exercised here (it needs Redis/Docker), but this
// is the exact Setup call Run makes first, so the "no network when disabled"
// contract is pinned at the worker's boundary.
func TestRunDisabledTracingNoNetworkNoCollector(t *testing.T) {
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	provider, err := tracing.Setup(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true with no OTEL endpoint; want disabled")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("disabled Shutdown = %v, want nil", err)
	}
}

// TestStartupRootSpanParenting proves the worker's root+child startup span
// shape through the same Setup + tracing.Start seam Run uses: a relay.startup
// root with functions.load as its child, and the root's trace id shared with the
// child. Run's full path is not exercised (it needs Redis/Docker), but the span
// construction and naming are exactly Run's.
func TestStartupRootSpanParenting(t *testing.T) {
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})

	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	startupCtx, root := tracing.Start(context.Background(), "relay.startup")
	_, child := tracing.Start(startupCtx, "functions.load")
	child.End()
	root.End()
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}
	var rootStub, childStub tracetest.SpanStub
	for _, s := range spans {
		switch s.Name {
		case "relay.startup":
			rootStub = s
		case "functions.load":
			childStub = s
		}
	}
	if childStub.Name == "" || rootStub.Name == "" {
		t.Fatalf("missing expected spans: %+v", spans)
	}
	if childStub.Parent.SpanID() != rootStub.SpanContext.SpanID() {
		t.Errorf("functions.load parent = %s, want relay.startup %s",
			childStub.Parent.SpanID(), rootStub.SpanContext.SpanID())
	}
	if childStub.SpanContext.TraceID() != rootStub.SpanContext.TraceID() {
		t.Errorf("child trace id = %s, want root %s",
			childStub.SpanContext.TraceID(), rootStub.SpanContext.TraceID())
	}
}

package tracing

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// resetGlobals restores the process-global tracer provider and propagator after
// a test that calls Setup. Setup mutates the OTel globals (that is its purpose);
// tests that exercise it must not leak the SDK provider into other packages'
// tests.
func resetGlobals(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
}

// TestSetupDefaultDisabledIsNoop proves that with no OTEL environment, Setup
// installs a disabled provider and Start yields a non-recording span: no
// exporter, no goroutine, and no network. It also proves the disabled path
// ignores an unrelated environment.
func TestSetupDefaultDisabledIsNoop(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	provider, err := Setup(context.Background(), discard())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true, want false with no OTEL endpoint")
	}

	_, span := Start(context.Background(), "relay.startup")
	if span.IsRecording() {
		t.Error("span is recording under a disabled provider; want a no-op span")
	}
	span.End()

	// Shutdown on a disabled provider is a safe no-op for every caller.
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Errorf("disabled Shutdown = %v, want nil", err)
	}
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("disabled ForceFlush = %v, want nil", err)
	}
}

// TestSetupSDKDisabledOverridesEndpoint proves OTEL_SDK_DISABLED=true is the
// standard opt-out even when a collector endpoint is configured: Setup returns a
// disabled provider and never constructs an OTLP exporter.
func TestSetupSDKDisabledOverridesEndpoint(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "TRUE") // case-insensitive per the spec
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")

	provider, err := Setup(context.Background(), discard())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true, want false when OTEL_SDK_DISABLED=true")
	}
}

// TestSetupInjectsW3CPropagator proves the standard W3C TraceContext + baggage
// propagator is installed globally even when tracing is disabled, so a disabled
// worker can still parse and forward incoming trace context.
func TestSetupInjectsW3CPropagator(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	if _, err := Setup(context.Background(), discard()); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// A context carrying a known trace id must round-trip through the global
	// propagator as a traceparent header.
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	carrier := propagation.MapCarrier{}
	InjectMap(ctx, carrier)
	if got := carrier.Get(TraceparentKey); got != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("traceparent = %q, want the W3C header", got)
	}

	// Extraction reconstructs the same span context (parent propagation).
	extracted := ExtractMap(context.Background(), carrier)
	if got := trace.SpanContextFromContext(extracted); got.TraceID() != traceID {
		t.Fatalf("extracted trace id = %s, want %s", got.TraceID(), traceID)
	}
}

// TestSetupWithExporterRecordsSpansAndParent proves the enabled path end to end:
// an injected in-memory exporter captures spans, a parent context produces a
// child span with the parent's trace id, and error recording sets codes.Error.
// It uses no network (the in-memory exporter replaces the OTLP exporter).
func TestSetupWithExporterRecordsSpansAndParent(t *testing.T) {
	resetGlobals(t)
	exp := tracetest.NewInMemoryExporter()
	provider, err := Setup(context.Background(), discard(), WithExporter(exp))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !provider.Enabled() {
		t.Fatal("provider.Enabled() = false, want true with an injected exporter")
	}
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	// Root span, then a child span started from the root's context.
	rootCtx, root := Start(context.Background(), "relay.startup")
	childCtx, child := Start(rootCtx, "functions.load")
	if got, want := trace.SpanContextFromContext(childCtx).TraceID(), root.SpanContext().TraceID(); got != want {
		t.Fatalf("child trace id = %s, want parent %s", got, want)
	}

	// Record a boundary error: it must be captured and set codes.Error.
	child.RecordError(context.Canceled)
	child.SetStatus(codes.Error, context.Canceled.Error())
	child.End()
	root.End()

	// Force a flush so the batch processor exports without waiting for its
	// timer; the in-memory exporter is synchronous once flushed.
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported %d spans, want 2: %+v", len(spans), spans)
	}
	byName := map[string]tracetest.SpanStub{}
	for _, s := range spans {
		byName[s.Name] = s
	}
	rootStub, ok := byName["relay.startup"]
	if !ok {
		t.Fatalf("missing relay.startup span in %+v", spans)
	}
	childStub, ok := byName["functions.load"]
	if !ok {
		t.Fatalf("missing functions.load span in %+v", spans)
	}
	if childStub.Parent.SpanID() != rootStub.SpanContext.SpanID() {
		t.Errorf("child parent span id = %s, want %s", childStub.Parent.SpanID(), rootStub.SpanContext.SpanID())
	}
	if childStub.Status.Code != codes.Error {
		t.Errorf("child status = %v, want codes.Error", childStub.Status.Code)
	}
	if len(childStub.Events) == 0 {
		t.Error("child span has no recorded error event")
	}
}

// TestStartAfterSetupDisabledIsNoop pins that the package-level Start resolves
// the global provider per call: a span started after a disabled Setup is a
// no-op even if an earlier provider was installed in the same process.
func TestStartAfterSetupDisabledIsNoop(t *testing.T) {
	resetGlobals(t)
	exp := tracetest.NewInMemoryExporter()
	enabled, err := Setup(context.Background(), discard(), WithExporter(exp))
	if err != nil {
		t.Fatalf("Setup(enabled): %v", err)
	}
	if !enabled.Enabled() {
		t.Fatal("expected an enabled provider")
	}

	// Reinstall a disabled provider; Start must follow it.
	t.Setenv("OTEL_SDK_DISABLED", "true")
	disabled, err := Setup(context.Background(), discard())
	if err != nil {
		t.Fatalf("Setup(disabled): %v", err)
	}
	if disabled.Enabled() {
		t.Fatal("expected a disabled provider")
	}
	_, span := Start(context.Background(), "relay.should-not-record")
	if span.IsRecording() {
		t.Error("span records after a disabled Setup; want a no-op span")
	}
	span.End()
}

// TestSetupServiceNameResource proves the resource carries service.name, with
// the explicit default used when no environment overrides it.
func TestSetupServiceNameResource(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

	exp := tracetest.NewInMemoryExporter()
	provider, err := Setup(context.Background(), discard(), WithExporter(exp))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	_, span := Start(context.Background(), "relay.resource-check")
	span.End()
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got, ok := spans[0].Resource.Set().Value("service.name")
	if !ok {
		t.Fatalf("resource missing service.name: %+v", spans[0].Resource.Attributes())
	}
	if got.AsString() != DefaultServiceName {
		t.Fatalf("service.name = %q, want %q", got.AsString(), DefaultServiceName)
	}
}

// TestSetupDefaultServiceNameConstant guards the exported default against an
// accidental empty value (OTel rejects an empty service.name).
func TestSetupDefaultServiceNameConstant(t *testing.T) {
	if DefaultServiceName == "" {
		t.Fatal("DefaultServiceName must not be empty")
	}
}

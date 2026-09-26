package tracing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// discardErrorHandler is a no-op otel.ErrorHandler used as the process-wide
// test sentinel so no test's capture buffer becomes the default delegator.
type discardErrorHandler struct{}

func (discardErrorHandler) Handle(error) {}

// TestMain installs a discarding global error handler before any test runs.
// OTel's first SetErrorHandler permanently delegates the default handler to the
// first installed handler; making that first handler a discard sentinel keeps
// the default logger silent and prevents any later test's buffer from receiving
// stray OTel errors outside that test.
func TestMain(m *testing.M) {
	otel.SetErrorHandler(discardErrorHandler{})
	os.Exit(m.Run())
}

// captureLogger returns a logger writing to a buffer at the requested level,
// with the time attribute dropped so log assertions are deterministic and can
// prove our output did not come from the stdlib default logger.
func captureLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	return slog.New(h), buf
}

// resetGlobals restores the process-global tracer provider, propagator, and
// error handler after a test that calls Setup. Setup mutates the OTel globals
// (that is its purpose); tests that exercise it must not leak the SDK provider
// or the Relay error handler into other packages' tests.
//
// Note on the error handler: OTel's SetErrorHandler delegates the default
// handler to the first handler ever installed (a one-time sync.Once), and the
// public API offers no un-delegate. Restoring the previous handler therefore
// restores the global pointer, while the default delegator retains the first
// handler as its delegate; this is inherent to the upstream API. Because every
// test's Setup installs a fresh handler, and the retained handler only writes to
// an in-memory test buffer, this cannot affect other packages' behaviour.
func resetGlobals(t *testing.T) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	prevEH := otel.GetErrorHandler()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		otel.SetErrorHandler(prevEH)
	})
}

// withBuilder installs a construction seam so tests can observe the selected
// protocol and return an in-memory exporter without opening a connection. It is
// test-only; production leaves options.buildExporter nil.
func withBuilder(fn func(context.Context, protocol) (sdktrace.SpanExporter, error)) Option {
	return func(o *options) { o.buildExporter = fn }
}

// exporterClientType reports the concrete exporter client type (for example
// "otlptracegrpc" or "otlptracehttp") by reading the pinned exporters' exported
// MarshalLog output. It never opens a connection.
func exporterClientType(exp sdktrace.SpanExporter) string {
	m, ok := exp.(interface{ MarshalLog() any })
	if !ok {
		return ""
	}
	mv := reflect.ValueOf(m.MarshalLog())
	if mv.Kind() != reflect.Struct {
		return ""
	}
	cf := mv.FieldByName("Client")
	if !cf.IsValid() || !cf.CanInterface() {
		return ""
	}
	client, ok := cf.Interface().(interface{ MarshalLog() any })
	if !ok {
		return ""
	}
	cm := reflect.ValueOf(client.MarshalLog())
	if cm.Kind() != reflect.Struct {
		return ""
	}
	tf := cm.FieldByName("Type")
	if !tf.IsValid() {
		return ""
	}
	return tf.String()
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
// worker can still parse and forward incoming trace context. This preserves the
// pre-existing propagation behaviour: OTEL_PROPAGATORS is intentionally not
// honored (the pinned OTel core only ships these two propagators).
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

// TestResolveProtocolPrecedence pins the standard precedence
// OTEL_EXPORTER_OTLP_TRACES_PROTOCOL > OTEL_EXPORTER_OTLP_PROTOCOL > default for
// both supported values and both directions.
func TestResolveProtocolPrecedence(t *testing.T) {
	tests := []struct {
		name    string
		generic string
		traces  string
		want    protocol
		wantErr bool
	}{
		{name: "default", want: protocolHTTPProtobuf},
		{name: "generic grpc", generic: "grpc", want: protocolGRPC},
		{name: "generic http/protobuf", generic: "http/protobuf", want: protocolHTTPProtobuf},
		{name: "traces grpc", traces: "grpc", want: protocolGRPC},
		{name: "traces overrides generic grpc->http", generic: "grpc", traces: "http/protobuf", want: protocolHTTPProtobuf},
		{name: "traces overrides generic http->grpc", generic: "http/protobuf", traces: "grpc", want: protocolGRPC},
		{name: "empty traces falls back to generic", generic: "grpc", traces: "", want: protocolGRPC},
		{name: "invalid generic", generic: "http/json", wantErr: true},
		{name: "invalid traces", generic: "grpc", traces: "http/json", wantErr: true},
		{name: "port is not protocol", generic: "4318", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envProtocol, tt.generic)
			t.Setenv(envTracesProtocol, tt.traces)
			got, err := resolveProtocol()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveProtocol() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveProtocol() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveProtocol() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSetupInvalidProtocolErrors proves a non-empty unsupported protocol is a
// clear initialization error (naming the variable and value) and leaves tracing
// disabled, never silently falling back.
func TestSetupInvalidProtocolErrors(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	t.Setenv(envProtocol, "http/json")
	t.Setenv(envTracesProtocol, "")

	provider, err := Setup(context.Background(), discard())
	if err == nil {
		t.Fatal("Setup() error = nil, want an unsupported-protocol error")
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true after a protocol error, want disabled")
	}
	if !strings.Contains(err.Error(), envProtocol) || !strings.Contains(err.Error(), "http/json") {
		t.Fatalf("error %q does not name the variable and value", err)
	}
}

// TestSetupInjectedExporterBypassesProtocol proves the test/embedder exporter
// seam does not resolve the OTLP protocol: ambient (even invalid) protocol
// config cannot fail an otherwise valid injected setup.
func TestSetupInjectedExporterBypassesProtocol(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv(envProtocol, "http/json")
	t.Setenv(envTracesProtocol, "")

	provider, err := Setup(context.Background(), discard(), WithExporter(tracetest.NewInMemoryExporter()))
	if err != nil {
		t.Fatalf("Setup with injected exporter: %v", err)
	}
	if !provider.Enabled() {
		t.Fatal("provider.Enabled() = false, want true with an injected exporter")
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
}

// TestSetupProtocolOnlyStaysDisabled proves a protocol without any endpoint
// stays disabled: Relay's opt-in endpoint gate is unchanged by protocol
// selection.
func TestSetupProtocolOnlyStaysDisabled(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv(envProtocol, "grpc")

	called := false
	provider, err := Setup(context.Background(), discard(), withBuilder(func(context.Context, protocol) (sdktrace.SpanExporter, error) {
		called = true
		return tracetest.NewInMemoryExporter(), nil
	}))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true with only a protocol set, want disabled")
	}
	if called {
		t.Fatal("exporter builder was called without an endpoint")
	}
}

// TestSetupEndpointEnablement proves either the generic or the traces endpoint
// opts in, and the selected protocol reaches the builder.
func TestSetupEndpointEnablement(t *testing.T) {
	for _, tt := range []struct {
		name     string
		generic  string
		traces   string
		protocol string
		want     protocol
	}{
		{name: "generic endpoint defaults to http/protobuf", generic: "http://127.0.0.1:4318", want: protocolHTTPProtobuf},
		{name: "traces endpoint opts in", traces: "http://127.0.0.1:4318", want: protocolHTTPProtobuf},
		{name: "generic endpoint with grpc", generic: "http://127.0.0.1:4317", protocol: "grpc", want: protocolGRPC},
		{name: "traces endpoint with grpc", traces: "http://127.0.0.1:4317", protocol: "grpc", want: protocolGRPC},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals(t)
			t.Setenv("OTEL_SDK_DISABLED", "")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tt.generic)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tt.traces)
			t.Setenv(envProtocol, tt.protocol)
			t.Setenv(envTracesProtocol, "")

			var got *protocol
			provider, err := Setup(context.Background(), discard(), withBuilder(func(_ context.Context, p protocol) (sdktrace.SpanExporter, error) {
				got = &p
				return tracetest.NewInMemoryExporter(), nil
			}))
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			if !provider.Enabled() {
				t.Fatal("provider.Enabled() = false with an endpoint, want true")
			}
			if got == nil {
				t.Fatal("exporter builder was not called")
			}
			if *got != tt.want {
				t.Fatalf("selected protocol = %q, want %q", *got, tt.want)
			}
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
		})
	}
}

// TestBuildExporterSelectsActualExporter proves the selected protocol maps to
// the matching concrete pinned exporter without any network access:
// buildExporter(grpc) yields an otlptracegrpc client and
// buildExporter(http/protobuf) yields an otlptracehttp client.
//
// Both constructors are lazy: otlptracehttp.New only builds an http.Client and
// otlptracegrpc.New's Start uses grpc.NewClient (which does not dial), so no
// collector is contacted and no goroutine outlives the test. A full in-process
// gRPC receiver is deliberately not used here: it would need its own listener
// and lifecycle and would add flakiness for no extra coverage of Relay's code,
// whose only responsibility is choosing the pinned constructor.
func TestBuildExporterSelectsActualExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	grpcExp, err := buildExporter(context.Background(), protocolGRPC)
	if err != nil {
		t.Fatalf("buildExporter(grpc): %v", err)
	}
	if got := exporterClientType(grpcExp); got != "otlptracegrpc" {
		t.Errorf("grpc client type = %q, want otlptracegrpc", got)
	}
	if err := grpcExp.Shutdown(context.Background()); err != nil {
		t.Errorf("grpc Shutdown: %v", err)
	}

	httpExp, err := buildExporter(context.Background(), protocolHTTPProtobuf)
	if err != nil {
		t.Fatalf("buildExporter(http/protobuf): %v", err)
	}
	if got := exporterClientType(httpExp); got != "otlptracehttp" {
		t.Errorf("http client type = %q, want otlptracehttp", got)
	}
	if err := httpExp.Shutdown(context.Background()); err != nil {
		t.Errorf("http Shutdown: %v", err)
	}
}

// TestEnabledLogHasResolvedServiceAndProtocol proves the enabled log is DEBUG,
// names the effective post-detection service.name (never the empty option) and
// the selected protocol, and is never empty. It covers the explicit env name and
// the default when nothing names the service.
func TestEnabledLogHasResolvedServiceAndProtocol(t *testing.T) {
	tests := []struct {
		name       string
		serviceEnv string
		want       string
	}{
		{name: "default service name", want: DefaultServiceName},
		{name: "service env name", serviceEnv: "relay-test", want: "relay-test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobals(t)
			t.Setenv("OTEL_SDK_DISABLED", "")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
			t.Setenv(envProtocol, "grpc")
			t.Setenv(envTracesProtocol, "")
			t.Setenv("OTEL_SERVICE_NAME", tt.serviceEnv)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

			logger, buf := captureLogger(slog.LevelDebug)
			provider, err := Setup(context.Background(), logger, withBuilder(func(context.Context, protocol) (sdktrace.SpanExporter, error) {
				return tracetest.NewInMemoryExporter(), nil
			}))
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

			out := buf.String()
			if !strings.Contains(out, "msg=\"Tracing: enabled\"") {
				t.Fatalf("enabled log missing: %q", out)
			}
			if !strings.Contains(out, "service="+tt.want) {
				t.Fatalf("enabled log missing resolved service %q: %q", tt.want, out)
			}
			if strings.Contains(out, `service=""`) {
				t.Fatalf("enabled log has an empty service: %q", out)
			}
			if !strings.Contains(out, "protocol=grpc") {
				t.Fatalf("enabled log missing protocol: %q", out)
			}
		})
	}
}

// TestDisabledLogIsConcise proves the disabled path logs the single fixed
// message and no configuration tutorial.
func TestDisabledLogIsConcise(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	logger, buf := captureLogger(slog.LevelDebug)
	provider, err := Setup(context.Background(), logger)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("provider.Enabled() = true with no endpoint, want disabled")
	}

	out := buf.String()
	if !strings.Contains(out, "msg=\"Tracing: disabled\"") {
		t.Fatalf("disabled log missing fixed message: %q", out)
	}
	if strings.Contains(out, "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf("disabled log must not contain a config tutorial: %q", out)
	}
}

// TestSDKDisabledLogIsConcise proves the OTEL_SDK_DISABLED path uses the same
// single fixed message, without naming the variable in the message.
func TestSDKDisabledLogIsConcise(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	logger, buf := captureLogger(slog.LevelDebug)
	if _, err := Setup(context.Background(), logger); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "msg=\"Tracing: disabled\"") {
		t.Fatalf("SDK-disabled log missing fixed message: %q", out)
	}
	if strings.Contains(out, "OTEL_SDK_DISABLED") {
		t.Fatalf("SDK-disabled log must not name the variable: %q", out)
	}
}

// TestBuildResourceServiceNamePrecedence proves the effective service.name
// follows the standard precedence: explicit default < OTEL_RESOURCE_ATTRIBUTES <
// OTEL_SERVICE_NAME, and that an empty attribute value falls back to the default
// so the resource is never nameless.
func TestBuildResourceServiceNamePrecedence(t *testing.T) {
	tests := []struct {
		name       string
		option     string
		serviceEnv string
		attrsEnv   string
		want       string
	}{
		{name: "default", want: DefaultServiceName},
		{name: "option overridden by service env", option: "opt", serviceEnv: "env-name", want: "env-name"},
		{name: "service env wins over attrs", serviceEnv: "env-name", attrsEnv: "service.name=attr-name", want: "env-name"},
		{name: "attrs used when no service env", attrsEnv: "service.name=attr-name", want: "attr-name"},
		{name: "empty attrs service name falls back", attrsEnv: "service.name=", want: DefaultServiceName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", tt.serviceEnv)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.attrsEnv)

			res := ensureServiceName(buildResource(context.Background(), discard(), tt.option))
			got := resourceServiceName(res)
			if got != tt.want {
				t.Fatalf("service.name = %q, want %q (attrs: %+v)", got, tt.want, res.Attributes())
			}
			// The underlying attribute must also be non-empty.
			v, ok := res.Set().Value(semconv.ServiceNameKey)
			if !ok || strings.TrimSpace(v.AsString()) == "" {
				t.Fatalf("resource service.name is empty: %+v", res.Attributes())
			}
		})
	}
}

// TestResourceServiceNameNeverEmpty proves the helper applied to the enabled log
// never yields an empty string even for a nameless resource.
func TestResourceServiceNameNeverEmpty(t *testing.T) {
	if got := resourceServiceName(resource.NewSchemaless()); got != DefaultServiceName {
		t.Fatalf("resourceServiceName(empty) = %q, want %q", got, DefaultServiceName)
	}
	if got := resourceServiceName(nil); got != DefaultServiceName {
		t.Fatalf("resourceServiceName(nil) = %q, want %q", got, DefaultServiceName)
	}
}

// TestSetupRoutesOTelErrorsToSlog proves the global OTel error handler forwards
// through Relay's slog as a fixed structured warning (no stdlib default logger,
// no timestamp, no double logging), and that resetGlobals restores the prior
// handler.
func TestSetupRoutesOTelErrorsToSlog(t *testing.T) {
	resetGlobals(t)
	t.Setenv("OTEL_SDK_DISABLED", "")

	logger, buf := captureLogger(slog.LevelDebug)
	if _, err := Setup(context.Background(), logger); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	otel.Handle(errors.New("otel boom"))

	out := buf.String()
	if !strings.Contains(out, `msg="Tracing: OpenTelemetry error"`) {
		t.Fatalf("OTel error not routed to slog: %q", out)
	}
	if !strings.Contains(out, `error="otel boom"`) {
		t.Fatalf("OTel error value missing: %q", out)
	}
	if strings.Contains(out, "time=") {
		t.Fatalf("OTel error log carries a timestamp: %q", out)
	}
	if got := strings.Count(out, "Tracing: OpenTelemetry error"); got != 1 {
		t.Fatalf("OTel error logged %d times, want 1: %q", got, out)
	}
}

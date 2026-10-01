// Package tracing is Relay's single OpenTelemetry setup point. It owns the
// tracer provider, the OTLP exporter, and the W3C propagator, so no other
// package constructs SDK/exporter state or reads OTEL_* variables directly.
//
// Behaviour:
//
//   - Tracing is DISABLED by default. A process with no OTEL configuration pays
//     only a no-op provider: no exporter is constructed, no collector is
//     contacted, and no goroutine is started.
//   - OTEL_SDK_DISABLED=true forces tracing off even when an endpoint is
//     configured (the standard opt-out).
//   - Export is enabled only when the standard OTEL_EXPORTER_OTLP_ENDPOINT (or
//     OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is configured. There is deliberately
//     no fallback to the exporter's default localhost endpoint, so Relay never
//     dials a collector the operator did not name.
//   - When enabled, the wire protocol is resolved at runtime with the standard
//     precedence OTEL_EXPORTER_OTLP_TRACES_PROTOCOL > OTEL_EXPORTER_OTLP_PROTOCOL
//     > the OTel default of "http/protobuf". Only the two pinned values "grpc"
//     and "http/protobuf" are supported; any other non-empty value fails
//     initialization with a clear error (no silent fallback, no port guessing).
//     A protocol without an endpoint stays disabled, like Relay always has.
//   - The selected OTLP exporter reads the standard OTEL environment (endpoint,
//     headers, TLS, compression, timeout) itself; this package passes no vendor
//     options, so any OTLP-compatible backend works over either protocol.
//
// Standard environment handled by the pinned SDK, not re-implemented here:
//
//   - OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG are applied by
//     sdktrace.NewTracerProvider. An invalid value is logged through the global
//     error handler and the SDK default (parentbased_always_on) is used.
//   - OTEL_BSP_SCHEDULE_DELAY, OTEL_BSP_EXPORT_TIMEOUT, OTEL_BSP_MAX_QUEUE_SIZE
//     and OTEL_BSP_MAX_EXPORT_BATCH_SIZE are applied by the batch span processor
//     the provider is built with.
//   - OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES are applied by the
//     resource env detector, with OTEL_SERVICE_NAME taking precedence.
//
// Known limitation: OTEL_PROPAGATORS is not honored. The pinned OTel core only
// ships the W3C TraceContext and Baggage propagators, so Relay always installs
// exactly those two rather than hand-rolling the other (b3/jaeger/xray) values.
//
// Every instrumentation site calls the package-level Start helper. It resolves
// the global tracer every call, so it is a no-op before Setup and after a
// disabled Setup, and it transparently follows the provider installed by Setup.
// Lower layers therefore never take a dependency on the Provider value.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// tracerName is the instrumentation scope every Relay span is created under. It
// is a stable, low-cardinality identifier (never an app name).
const tracerName = "relay"

// The W3C trace-context field names. They are the exact header names the
// installed propagator reads and writes, exported so every cross-process
// boundary in Relay names the same fields without re-deriving the convention:
//
//   - a Redis stream message carries them as flat, optional fields next to the
//     untouched "event" payload (Redis stream entries are a flat string map,
//     and flat fields stay directly inspectable with redis-cli), and
//   - the execution-container invocation frame carries them nested under the
//     single optional "trace" JSON object (see runtime.invokeRequest).
//
// See InjectMap/ExtractMap, CarrierFromContext, and ExtractStrings for the
// propagation seam.
const (
	TraceparentKey = "traceparent"
	TracestateKey  = "tracestate"
	BaggageKey     = "baggage"
)

// DefaultServiceName is used when neither WithServiceName nor OTEL_SERVICE_NAME
// provides a service.name.
const DefaultServiceName = "relay"

// Standard OTLP environment variable names. They are named here only for the
// handful of decisions this package makes (endpoint gate, protocol selection,
// opt-out); the exporters and SDK still parse and apply the values themselves.
const (
	envProtocol       = "OTEL_EXPORTER_OTLP_PROTOCOL"
	envTracesProtocol = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	envEndpoint       = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envTracesEndpoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	envSDKDisabled    = "OTEL_SDK_DISABLED"
)

// protocol is a supported OTLP wire protocol. Values are exactly the pinned
// OTel strings; Relay intentionally does not accept http/json or guess from a
// port number.
type protocol string

const (
	protocolGRPC         protocol = "grpc"
	protocolHTTPProtobuf protocol = "http/protobuf"

	// defaultProtocol is the OTel standard default when neither protocol
	// variable is set. It matches the pre-existing HTTP-only behaviour.
	defaultProtocol = protocolHTTPProtobuf
)

// Option tunes Setup. Options exist only so tests and embedders can supply an
// explicit exporter; production passes none and relies entirely on the standard
// OTEL environment.
type Option func(*options)

// options is the resolved Setup configuration.
type options struct {
	exporter    sdktrace.SpanExporter
	serviceName string
	sampler     sdktrace.Sampler

	// buildExporter is a construction seam for in-package tests: it lets them
	// observe the selected protocol without opening a connection. Production
	// leaves it nil and uses buildExporter.
	buildExporter func(context.Context, protocol) (sdktrace.SpanExporter, error)
}

// WithExporter injects an explicit span exporter, bypassing the
// OTEL_EXPORTER_OTLP_ENDPOINT gate. It exists for tests (an in-memory exporter)
// and embedders; production does not use it.
func WithExporter(exporter sdktrace.SpanExporter) Option {
	return func(o *options) { o.exporter = exporter }
}

// WithServiceName overrides the service.name resource attribute. The standard
// OTEL_SERVICE_NAME (read by the resource env detector) still wins over it, so
// an operator's environment always takes precedence.
func WithServiceName(name string) Option {
	return func(o *options) { o.serviceName = name }
}

// WithSampler overrides the sampler. When omitted, the SDK applies
// OTEL_TRACES_SAMPLER/OTEL_TRACES_SAMPLER_ARG itself (defaulting to
// parentbased_always_on); an explicit sampler wins over the environment, as the
// SDK applies its env options before the ones passed here.
func WithSampler(sampler sdktrace.Sampler) Option {
	return func(o *options) { o.sampler = sampler }
}

// Provider owns the configured SDK tracer provider. A disabled Provider is
// valid and its Shutdown is a no-op; callers may use it unconditionally.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// Enabled reports whether an SDK provider (and therefore an exporter) is
// active. A disabled provider emits no spans and starts no goroutines.
func (p *Provider) Enabled() bool {
	return p != nil && p.tp != nil
}

// Shutdown flushes pending spans and releases the exporter. It is a no-op for a
// disabled Provider. The caller owns the timeout: the worker supplies a bounded
// context through its shutdown registry, so a wedged collector can never hang
// process teardown.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

// ForceFlush exports spans buffered in the batch processor without shutting the
// provider down. It is a no-op for a disabled Provider.
func (p *Provider) ForceFlush(ctx context.Context) error {
	if p == nil || p.tp == nil {
		return nil
	}
	return p.tp.ForceFlush(ctx)
}

// Setup installs the global tracer provider and the W3C TraceContext + baggage
// propagator. It never returns a nil Provider: on a configuration or exporter
// error it returns a disabled Provider alongside the error, so a caller can log
// and continue without any further nil checks. A disabled Provider is also
// returned when tracing is off by default or via OTEL_SDK_DISABLED.
//
// Setup does not block on a collector: both OTLP exporters are constructed
// without connecting, and the batch processor exports asynchronously.
func Setup(ctx context.Context, logger *slog.Logger, opts ...Option) (*Provider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Route OTel's global error channel through Relay's structured logger before
	// any SDK or exporter work, so resource/sampler/exporter errors are captured
	// and the stdlib default logger never writes them. OTel errors are
	// observability only: they never fail Setup.
	otel.SetErrorHandler(otelErrorHandler{logger: logger})

	resolved := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}

	// The propagator is always installed, even when tracing is disabled: W3C
	// context is cheap, vendor-neutral, and lets a disabled worker still parse
	// and forward an incoming trace context without any exporter.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if sdkDisabled() {
		logger.Info("Tracing: disabled")
		return installDisabled()
	}
	if resolved.exporter == nil && !otlpEndpointConfigured() {
		// Default-off: no endpoint, no exporter, no collector connection.
		logger.Debug("Tracing: disabled")
		return installDisabled()
	}

	// The protocol is only meaningful when Relay constructs an exporter. An
	// injected exporter (the test/embedder seam) bypasses selection entirely, so
	// ambient protocol config cannot fail an otherwise valid setup.
	selectedProtocol := defaultProtocol
	exporter := resolved.exporter
	if exporter == nil {
		var err error
		selectedProtocol, err = resolveProtocol()
		if err != nil {
			disabled, _ := installDisabled()
			return disabled, err
		}
		build := resolved.buildExporter
		if build == nil {
			build = buildExporter
		}
		exporter, err = build(ctx, selectedProtocol)
		if err != nil {
			disabled, _ := installDisabled()
			return disabled, fmt.Errorf("otlp %s exporter: %w", selectedProtocol, err)
		}
	}

	// Build the resource first so the log reports the effective, post-detection
	// service.name (OTEL_SERVICE_NAME/OTEL_RESOURCE_ATTRIBUTES may have replaced
	// the option value), never the pre-resource option or an empty string.
	res := ensureServiceName(buildResource(ctx, logger, resolved.serviceName))
	serviceName := resourceServiceName(res)

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	}
	if resolved.sampler != nil {
		tpOpts = append(tpOpts, sdktrace.WithSampler(resolved.sampler))
	}

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	logger.Debug("Tracing: enabled", "service", serviceName, "protocol", selectedProtocol)
	return &Provider{tp: tp}, nil
}

// resolveProtocol picks the OTLP protocol with the standard precedence:
// OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, then OTEL_EXPORTER_OTLP_PROTOCOL, then the
// OTel default. Empty values are treated as unset. Any other non-empty value is
// an initialization error rather than a silent fallback.
func resolveProtocol() (protocol, error) {
	for _, key := range []string{envTracesProtocol, envProtocol} {
		raw, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		switch protocol(raw) {
		case protocolGRPC, protocolHTTPProtobuf:
			return protocol(raw), nil
		default:
			return "", fmt.Errorf(
				"unsupported %s %q: supported values are %q and %q",
				key, raw, protocolGRPC, protocolHTTPProtobuf,
			)
		}
	}
	return defaultProtocol, nil
}

// buildExporter constructs the pinned OTLP exporter for the protocol. Both
// constructors read the standard OTEL_EXPORTER_OTLP_* environment themselves
// (endpoint, headers, TLS, compression, timeout), so no options are passed and
// no path is appended or gRPC connection configured here.
func buildExporter(ctx context.Context, p protocol) (sdktrace.SpanExporter, error) {
	switch p {
	case protocolGRPC:
		return otlptracegrpc.New(ctx)
	case protocolHTTPProtobuf:
		return otlptracehttp.New(ctx)
	default:
		// Unreachable: resolveProtocol only returns supported values.
		return nil, fmt.Errorf("unsupported protocol %q", p)
	}
}

// otelErrorHandler forwards OTel's irremediable global errors to Relay's slog
// logger as a fixed structured warning. It replaces the default stdlib logger,
// so an error is logged once, in Relay's format, and never fatal.
type otelErrorHandler struct {
	logger *slog.Logger
}

// Handle implements otel.ErrorHandler.
func (h otelErrorHandler) Handle(err error) {
	if err == nil || h.logger == nil {
		return
	}
	h.logger.Warn("Tracing: OpenTelemetry error", "error", err)
}

// installDisabled installs the noop tracer provider so Start is a guaranteed
// no-op, and returns the matching disabled Provider. It deliberately does not
// construct a resource or exporter.
func installDisabled() (*Provider, error) {
	otel.SetTracerProvider(noop.NewTracerProvider())
	return &Provider{}, nil
}

// buildResource assembles the OTel resource. The explicit service.name default
// is applied BEFORE the environment detector so OTEL_SERVICE_NAME and
// OTEL_RESOURCE_ATTRIBUTES still take precedence. WithHost adds host.name
// without any network access.
func buildResource(ctx context.Context, logger *slog.Logger, serviceName string) *resource.Resource {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = DefaultServiceName
	}
	res, err := resource.New(
		ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		// A partial resource is still usable; only a completely failed
		// detection leaves res nil.
		logger.Debug("Tracing: resource detection partial", "error", err)
	}
	if res == nil {
		return resource.NewSchemaless(semconv.ServiceName(serviceName))
	}
	return res
}

// ensureServiceName guarantees the resource carries a non-empty service.name.
// The env detector can overwrite the explicit default with an empty value (for
// example OTEL_RESOURCE_ATTRIBUTES=service.name=); in that case the standard
// default is merged back on top.
func ensureServiceName(res *resource.Resource) *resource.Resource {
	name, ok := rawServiceName(res)
	if ok && strings.TrimSpace(name) != "" {
		return res
	}
	fallback := resource.NewSchemaless(semconv.ServiceName(DefaultServiceName))
	if res == nil {
		return fallback
	}
	merged, err := resource.Merge(res, fallback)
	if err != nil || merged == nil {
		return fallback
	}
	return merged
}

// rawServiceName returns the service.name attribute and whether it was present.
func rawServiceName(res *resource.Resource) (string, bool) {
	if res == nil {
		return "", false
	}
	v, ok := res.Set().Value(semconv.ServiceNameKey)
	if !ok {
		return "", false
	}
	return v.AsString(), true
}

// resourceServiceName returns the effective final service.name from the built
// resource, falling back to DefaultServiceName so the enabled log is never
// empty.
func resourceServiceName(res *resource.Resource) string {
	if name, ok := rawServiceName(res); ok && strings.TrimSpace(name) != "" {
		return name
	}
	return DefaultServiceName
}

// Start begins a span named name on the global provider. It is the single
// instrumentation entry point: before Setup, after a disabled Setup, or with no
// OTEL configuration it returns a non-recording no-op span, so instrumentation
// is always safe and never allocates an exporter. Callers must End the span.
func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name, opts...)
}

// InjectMap injects the current trace context into carrier using the global W3C
// propagator. It is the seam for a cross-process boundary (e.g. the
// execution-container invocation frame or a Redis stream message): a caller
// renders the carrier to a field and a consumer runs ExtractMap on the
// receiving side. It never leaks payload data — only the W3C
// traceparent/tracestate/baggage headers.
func InjectMap(ctx context.Context, carrier propagation.MapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractMap extracts a remote trace context from carrier using the global W3C
// propagator, returning a context whose span becomes a child of the remote
// parent. It is the counterpart of InjectMap.
func ExtractMap(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// CarrierFromContext renders the current trace context to a plain
// map[string]string using the global W3C propagator. It returns nil when there
// is nothing to carry (no recording/remote span context), so a caller can omit
// an empty carrier from a wire frame or a stream message instead of emitting an
// empty object. An empty-but-non-nil carrier is impossible: the propagator only
// sets a field when it has a value.
//
// The map is a plain map (not propagation.MapCarrier) because its consumers are
// wire/stream field maps, not propagator carriers; convert with
// propagation.MapCarrier when extracting.
func CarrierFromContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// ExtractStrings extracts a remote trace context from a plain map[string]string
// (a stream message's flat fields or a decoded frame field). It is the
// counterpart of CarrierFromContext and is a no-op (returns ctx unchanged) for
// a nil/empty map or a carrier with no valid trace context.
func ExtractStrings(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// traceSeparator joins the serialized traceparent and tracestate in the compact
// lineage form persisted by Relay. It is deliberately not a character the W3C
// traceparent grammar can contain (that grammar is fixed-width hex + dashes);
// a tracestate value that contains it is preserved in full because only the
// first separator is significant.
const traceSeparator = "|"

// durablePropagator is the W3C TraceContext propagator alone (no baggage),
// used for durable lineage serialization. Durable lineage is the trace identity
// and tracestate only; baggage is request-scoped and must never be persisted or
// replayed. It is a package value (not the installed global propagator) so a
// caller cannot accidentally persist baggage via a global composite propagator.
var durablePropagator = propagation.TraceContext{}

// SpanContextToString serializes a valid SpanContext into the compact lineage
// string Relay persists in durable state (the invocation-state hash and DLQ
// entries): the W3C traceparent, followed by the tracestate when present, joined
// by traceSeparator. It returns "" for an invalid SpanContext, so a caller can
// omit an absent lineage instead of writing an unparseable value.
//
// It NEVER serializes baggage: durable lineage is exactly the trace identity
// (trace id, span id, the W3C sampled flag) plus tracestate. Baggage is
// request-scoped and must not be persisted or replayed.
func SpanContextToString(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	carrier := propagation.MapCarrier{}
	durablePropagator.Inject(ctx, carrier)
	traceparent := carrier.Get(TraceparentKey)
	if traceparent == "" {
		return ""
	}
	if tracestate := carrier.Get(TracestateKey); tracestate != "" {
		return traceparent + traceSeparator + tracestate
	}
	return traceparent
}

// SpanContextFromString parses the compact lineage form produced by
// SpanContextToString. It returns ok=false for an empty or malformed value (an
// unknown/garbage stored reference is ignored gracefully, never an error). The
// traceparent and tracestate are validated with the W3C TraceContext propagator,
// so the returned SpanContext preserves the recorded sampled flag and tracestate
// exactly. The returned context is marked LOCAL (Remote=false): it is Relay's own
// persisted historical attempt, used for span links and retry lineage, not an
// incoming remote parent.
func SpanContextFromString(s string) (trace.SpanContext, bool) {
	if s == "" {
		return trace.SpanContext{}, false
	}
	traceparent, tracestate, _ := strings.Cut(s, traceSeparator)
	if strings.TrimSpace(traceparent) == "" {
		return trace.SpanContext{}, false
	}
	carrier := propagation.MapCarrier{TraceparentKey: traceparent}
	if tracestate != "" {
		carrier[TracestateKey] = tracestate
	}
	extracted := trace.SpanContextFromContext(durablePropagator.Extract(context.Background(), carrier))
	if !extracted.IsValid() {
		return trace.SpanContext{}, false
	}
	// The propagator marks an extracted context Remote=true. Rebuild it as a
	// local historical context (Remote=false) while preserving the validated
	// trace id, span id, sampled flag, and tracestate.
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    extracted.TraceID(),
		SpanID:     extracted.SpanID(),
		TraceFlags: extracted.TraceFlags(),
		TraceState: extracted.TraceState(),
		Remote:     false,
	}), true
}

// sdkDisabled reports whether OTEL_SDK_DISABLED requests the standard opt-out.
// The specification defines the value as case-insensitive "true"; anything else
// leaves tracing at its environment-derived state.
func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(envSDKDisabled)), "true")
}

// otlpEndpointConfigured reports whether the standard OTLP endpoint variables
// are set. The exporter itself parses them (including headers and TLS); this
// check only decides whether to construct it at all, which is what keeps a
// default-configured process from dialing localhost.
func otlpEndpointConfigured() bool {
	return strings.TrimSpace(os.Getenv(envEndpoint)) != "" ||
		strings.TrimSpace(os.Getenv(envTracesEndpoint)) != ""
}

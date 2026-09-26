// Package tracing is Relay's single OpenTelemetry setup point. It owns the
// tracer provider, the OTLP/HTTP exporter, and the W3C propagator, so no other
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
//   - When enabled, the OTLP/HTTP exporter reads the standard OTEL environment
//     (endpoint, headers, TLS, compression) itself; this package passes no
//     vendor options, so any OTLP-compatible backend works.
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
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// tracerName is the instrumentation scope every Relay span is created under. It
// is a stable, low-cardinality identifier (never a function name).
const tracerName = "relay"

// TraceparentKey is the W3C trace-context header name. It is exported so a
// future cross-process boundary can name the exact field to carry without
// re-deriving the convention. See InjectMap/ExtractMap for the propagation
// seam: Relay deliberately does not add the field to the execution-container
// invocation protocol today (the bootstraps are language-specific and would
// ignore it), but a later revision can inject the header into a frame field and
// extract it on the container side without touching this package.
const TraceparentKey = "traceparent"

// DefaultServiceName is used when neither WithServiceName nor OTEL_SERVICE_NAME
// provides a service.name.
const DefaultServiceName = "relay"

// Option tunes Setup. Options exist only so tests and embedders can supply an
// explicit exporter; production passes none and relies entirely on the standard
// OTEL environment.
type Option func(*options)

// options is the resolved Setup configuration.
type options struct {
	exporter    sdktrace.SpanExporter
	serviceName string
	sampler     sdktrace.Sampler
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

// WithSampler overrides the sampler. When omitted, the SDK's standard
// environment handling (OTEL_TRACES_SAMPLER) applies, defaulting to
// parent-based always-on.
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
// Setup does not block on a collector: the OTLP/HTTP exporter is constructed
// without connecting, and the batch processor exports asynchronously.
func Setup(ctx context.Context, logger *slog.Logger, opts ...Option) (*Provider, error) {
	if logger == nil {
		logger = slog.Default()
	}
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

	if disabled := sdkDisabled(); disabled {
		logger.Info("Tracing: disabled by OTEL_SDK_DISABLED")
		return installDisabled()
	}
	if resolved.exporter == nil && !otlpEndpointConfigured() {
		// Default-off: no endpoint, no exporter, no collector connection.
		logger.Debug("Tracing: disabled (set OTEL_EXPORTER_OTLP_ENDPOINT to enable)")
		return installDisabled()
	}

	exporter := resolved.exporter
	if exporter == nil {
		var err error
		exporter, err = otlptracehttp.New(ctx)
		if err != nil {
			disabled, _ := installDisabled()
			return disabled, fmt.Errorf("otlp http exporter: %w", err)
		}
	}

	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithBatcher(exporter)}
	if resolved.sampler != nil {
		tpOpts = append(tpOpts, sdktrace.WithSampler(resolved.sampler))
	}
	tpOpts = append(tpOpts, sdktrace.WithResource(buildResource(ctx, logger, resolved.serviceName)))

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	logger.Info("Tracing: OTLP export enabled", "service", resolved.serviceName)
	return &Provider{tp: tp}, nil
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

// Start begins a span named name on the global provider. It is the single
// instrumentation entry point: before Setup, after a disabled Setup, or with no
// OTEL configuration it returns a non-recording no-op span, so instrumentation
// is always safe and never allocates an exporter. Callers must End the span.
func Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, name, opts...)
}

// InjectMap injects the current trace context into carrier using the global W3C
// propagator. It is the seam for a future cross-process boundary (e.g. the
// execution-container invocation frame): a caller renders the carrier to a
// field and a consumer runs ExtractMap on the receiving side. It never leaks
// payload data — only the W3C traceparent/tracestate/baggage headers.
func InjectMap(ctx context.Context, carrier propagation.MapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractMap extracts a remote trace context from carrier using the global W3C
// propagator, returning a context whose span becomes a child of the remote
// parent. It is the counterpart of InjectMap.
func ExtractMap(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// sdkDisabled reports whether OTEL_SDK_DISABLED requests the standard opt-out.
// The specification defines the value as case-insensitive "true"; anything else
// leaves tracing at its environment-derived state.
func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

// otlpEndpointConfigured reports whether the standard OTLP endpoint variables
// are set. The exporter itself parses them (including headers and TLS); this
// check only decides whether to construct it at all, which is what keeps a
// default-configured process from dialing localhost.
func otlpEndpointConfigured() bool {
	return strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" ||
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != ""
}

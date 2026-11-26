//go:build integration

package python

// End-to-end coverage for relay_runtime.tracing's Tracer through the REAL
// embedded bootstrap, Relay's managed OpenTelemetry provider, the REAL
// official SDK/exporters, and the controlled local OTLP receiver: decorated
// sync/async methods and manual spans must export as children of the
// propagated invocation trace, the generic exception status must stay free of
// any sensitive event unless explicitly opted in, a warm container must never
// inherit a previous invocation's trace, Logger records must correlate to the
// actually exported native spans, and an application-owned provider must
// never be replaced. The handler prints the global provider's type after the
// Tracer and Logger are used: neither installs a provider or exporter of its
// own, so the managed (or application-owned) provider type must be unchanged.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// requireTracerImport proves python3 can import the tracing facade from the
// source tree against the materialized OTel install before any bootstrap
// spawns, so a syntax or packaging problem fails with a direct diagnosis.
func requireTracerImport(t *testing.T, otelDir, src string) {
	t.Helper()
	pythonPath := src
	if otelDir != "" {
		pythonPath = otelDir + string(os.PathListSeparator) + src
	}
	if err := pythonProbe("from relay_runtime.tracing import Tracer", pythonPath); err != nil {
		t.Fatalf("python3 cannot import relay_runtime.tracing: %v", err)
	}
}

// requirePrintedIDs parses one "<label> trace=<32 hex> span=<16 hex>" line
// from the handler's stdout and returns the pair.
func requirePrintedIDs(t *testing.T, out, label string) (traceID, spanID string) {
	t.Helper()
	prefix := label + " trace="
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(trimmed, prefix)
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || !strings.HasPrefix(fields[1], "span=") {
			t.Fatalf("malformed %q id line: %q", label, trimmed)
		}
		return strings.TrimSpace(fields[0]), strings.TrimPrefix(fields[1], "span=")
	}
	t.Fatalf("no %q id line in stdout:\n%s", label, out)
	return "", ""
}

// tracerDecoratedHandlerSource exercises every manual entry point of the
// Tracer inside one invocation: a decorated sync method, a decorated async
// method with a configured name, and a manual span with constructor
// attributes and a SERVER kind, all under an ambient carrier trace.
const tracerDecoratedHandlerSource = `
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from relay_runtime.tracing import Tracer

TRACER = Tracer("handler.tracer")


@TRACER.capture_method
def decorated(event, context):
    trace.get_current_span().set_attribute("decorated.step", event["step"])
    return "sync-%s" % event["step"]


@TRACER.capture_method(name="orders.find")
async def decorated_async(event, context):
    ctx = trace.get_current_span().get_span_context()
    assert ctx.is_valid, "the async span must run inside the invocation trace"
    return "async-%s" % event["step"]


async def work(event, context):
    sync_result = decorated(event, context)
    async_result = await decorated_async(event, context)
    with TRACER.span(
        "manual.export",
        attributes={"manual.step": event["step"]},
        kind=SpanKind.SERVER,
    ) as span:
        span.set_attribute("manual.late", "yes")
        assert trace.get_current_span() is span
    print("provider=%s" % type(trace.get_tracer_provider()).__name__)
    print("done %s %s" % (sync_result, async_result))
`

// TestPythonRuntimeTracerManagedProviderExportsDecoratedAndManualSpans is
// the core Tracer scenario: spans created through the facade join the Relay
// carrier's trace as children with the right names and attributes, carry no
// status, and the managed provider type is unchanged after the Tracer runs.
func TestPythonRuntimeTracerManagedProviderExportsDecoratedAndManualSpans(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	requireTracerImport(t, otelDir, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", tracerDecoratedHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=tracer-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"1"}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "done sync-1 async-1") {
		t.Fatalf("handler did not run: %q", r.Out)
	}
	// The Tracer neither installs nor replaces a provider: after its spans
	// ran, the process still observes the single managed provider.
	if !strings.Contains(r.Out, "provider=TracerProvider") {
		t.Errorf("the managed provider must be unchanged after Tracer use, out = %q", r.Out)
	}

	spans := rec.waitForSpans(t, 3)
	byName := make(map[string]*otlpSpan, 3)
	for i := range spans {
		switch spans[i].Name {
		case "decorated", "orders.find", "manual.export":
			span := spans[i]
			byName[spans[i].Name] = &span
		default:
			t.Errorf("unexpected exported span %q: the process must export only application spans", spans[i].Name)
		}
	}
	if len(byName) != 3 {
		t.Fatalf("missing Tracer spans, got names from: %+v", spans)
	}

	for name, span := range byName {
		if span.TraceID != traceOne {
			t.Errorf("%s trace = %s, want the carrier trace %s", name, span.TraceID, traceOne)
		}
		if span.ParentSpanID != parentOne {
			t.Errorf("%s parent = %s, want the carrier's parent span %s", name, span.ParentSpanID, parentOne)
		}
		if span.SpanID == "" || span.SpanID == parentOne {
			t.Errorf("%s span id = %q, want its own id distinct from the parent", name, span.SpanID)
		}
		if span.ServiceName != "tracer-svc" {
			t.Errorf("%s resource service.name = %q, want tracer-svc", name, span.ServiceName)
		}
		if span.StatusCode != tracev1.Status_STATUS_CODE_UNSET {
			t.Errorf("%s status = %v, want UNSET for a successful call", name, span.StatusCode)
		}
	}
	if got := byName["decorated"].Attributes["decorated.step"]; got != "1" {
		t.Errorf("decorated attributes = %v, want decorated.step=1", byName["decorated"].Attributes)
	}
	manual := byName["manual.export"]
	if manual.Attributes["manual.step"] != "1" || manual.Attributes["manual.late"] != "yes" {
		t.Errorf("manual attributes = %v, want both the constructor and the late attribute", manual.Attributes)
	}

	closeAndWait(t, p, 15*time.Second)
	final := rec.snapshot()
	if len(final) != 3 {
		t.Errorf("exported spans after shutdown flush = %d, want exactly the 3 application spans: %+v", len(final), final)
	}
}

// tracerExceptionHandlerSource raises a sensitive ordinary Exception through
// the default form and through the explicit record_exception=True opt-in.
const tracerExceptionHandlerSource = `
from relay_runtime.tracing import Tracer

TRACER = Tracer("handler.tracer")


@TRACER.capture_method
def failing(event, context):
    raise ValueError("SECRET-CANARY-default")


@TRACER.capture_method(name="orders.fail.optin", record_exception=True)
def failing_optin(event, context):
    raise ValueError("SECRET-CANARY-optin")


async def work(event, context):
    for target in (failing, failing_optin):
        try:
            target(event, context)
        except ValueError:
            pass
    print("handled")
`

// TestPythonRuntimeTracerExceptionStatusStaysPrivate pins the privacy
// contract on the wire: by default an ordinary Exception exports only the
// generic ERROR status with no description and no exception event, so the
// message appears nowhere in the exported span, while the explicit opt-in
// adds exactly one native exception event that does carry it.
func TestPythonRuntimeTracerExceptionStatusStaysPrivate(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	requireTracerImport(t, otelDir, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", tracerExceptionHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=tracer-privacy-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("handled exceptions must not fail the invocation: %s", r.Err)
	}
	if !strings.Contains(r.Out, "handled") {
		t.Fatalf("handler did not run: %q", r.Out)
	}

	spans := rec.waitForSpans(t, 2)
	var defaultSpan, optInSpan *otlpSpan
	for i := range spans {
		switch spans[i].Name {
		case "failing":
			defaultSpan = &spans[i]
		case "orders.fail.optin":
			optInSpan = &spans[i]
		}
	}
	if defaultSpan == nil || optInSpan == nil {
		t.Fatalf("missing Tracer spans: %+v", spans)
	}

	// Default: generic status only — no description, no event, no message.
	if defaultSpan.StatusCode != tracev1.Status_STATUS_CODE_ERROR {
		t.Errorf("default status = %v, want ERROR", defaultSpan.StatusCode)
	}
	if defaultSpan.StatusMessage != "" {
		t.Errorf("default status message = %q, want empty (generic status only)", defaultSpan.StatusMessage)
	}
	if len(defaultSpan.Events) != 0 {
		t.Errorf("default events = %+v, want none (exception recording is off by default)", defaultSpan.Events)
	}
	blob := defaultSpan.StatusMessage + fmt.Sprint(defaultSpan.Attributes) + fmt.Sprint(defaultSpan.Events)
	if strings.Contains(blob, "SECRET-CANARY-default") {
		t.Errorf("the sensitive default message leaked into the exported span: %s", blob)
	}
	if defaultSpan.TraceID != traceOne || defaultSpan.ParentSpanID != parentOne {
		t.Errorf("failing span = trace %s parent %s, want the carrier trace", defaultSpan.TraceID, defaultSpan.ParentSpanID)
	}

	// Opt-in: exactly one native exception event carrying the message.
	if optInSpan.StatusCode != tracev1.Status_STATUS_CODE_ERROR {
		t.Errorf("opt-in status = %v, want ERROR", optInSpan.StatusCode)
	}
	if len(optInSpan.Events) != 1 {
		t.Fatalf("opt-in events = %+v, want exactly one exception event", optInSpan.Events)
	}
	event := optInSpan.Events[0]
	if event.Name != "exception" {
		t.Errorf("opt-in event name = %q, want the native exception event", event.Name)
	}
	if event.Attributes["exception.type"] != "ValueError" {
		t.Errorf("exception.type = %q, want ValueError", event.Attributes["exception.type"])
	}
	if !strings.Contains(event.Attributes["exception.message"], "SECRET-CANARY-optin") {
		t.Errorf("exception.message = %q, want the opted-in message", event.Attributes["exception.message"])
	}

	closeAndWait(t, p, 15*time.Second)
}

// tracerWarmTraceHandlerSource creates one decorated span per invocation and
// prints the trace id that was actually active inside it.
const tracerWarmTraceHandlerSource = `
from opentelemetry import trace

from relay_runtime.tracing import Tracer

TRACER = Tracer("handler.tracer")


@TRACER.capture_method
def probe(event, context):
    ctx = trace.get_current_span().get_span_context()
    print("probe trace=%032x span=%016x" % (ctx.trace_id, ctx.span_id))
    return None


async def work(event, context):
    probe(event, context)
`

// TestPythonRuntimeTracerWarmInvocationsDoNotInheritTraces pins the
// per-invocation context boundary through the Tracer in a warm container: a
// traced span joins the propagated carrier trace, the NEXT invocation with no
// carrier starts a fresh root trace of its own, and a later carrier is joined
// instead of the stale one — no context leaks across invocations.
func TestPythonRuntimeTracerWarmInvocationsDoNotInheritTraces(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	requireTracerImport(t, otelDir, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", tracerWarmTraceHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=tracer-warm-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	// Given a traced first invocation on the carrier trace
	r1 := p.invokeTrace(t, "handler.work", `{}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	spans := rec.waitForSpans(t, 1)
	if spans[0].TraceID != traceOne || spans[0].ParentSpanID != parentOne {
		t.Fatalf("first span = %+v, want the carrier trace", spans[0])
	}

	// when a warm invocation runs WITHOUT a carrier, then its span starts a
	// fresh root trace that is neither the stale one nor any zero id
	r2 := p.invoke(t, "handler.work", `{}`, nil)
	if !r2.OK {
		t.Fatalf("carrier-less warm invoke failed: %s", r2.Err)
	}
	spans = rec.waitForSpans(t, 2)
	untraced := spans[1]
	if untraced.TraceID == traceOne || untraced.TraceID == "" || untraced.TraceID == "00000000000000000000000000000000" {
		t.Errorf("carrier-less span trace = %q, want a fresh trace distinct from the stale %s", untraced.TraceID, traceOne)
	}
	if untraced.ParentSpanID != "" {
		t.Errorf("carrier-less span parent = %q, want a new root with no parent", untraced.ParentSpanID)
	}
	printed, _ := requirePrintedIDs(t, r2.Out, "probe")
	if printed != untraced.TraceID {
		t.Errorf("printed trace = %s, exported trace = %s", printed, untraced.TraceID)
	}

	// and a later invocation carries a DIFFERENT trace, it must join that one
	r3 := p.invokeTrace(t, "handler.work", `{}`, nil,
		traceCarrier(traceTwo, parentTwo))
	if !r3.OK {
		t.Fatalf("third invoke failed: %s", r3.Err)
	}
	spans = rec.waitForSpans(t, 3)
	third := spans[2]
	if third.TraceID != traceTwo || third.ParentSpanID != parentTwo {
		t.Errorf("third span = %+v, want the second carrier trace", third)
	}
	if third.TraceID == untraced.TraceID {
		t.Errorf("the second and third invocations shared trace %s, want independent traces", third.TraceID)
	}

	closeAndWait(t, p, 15*time.Second)
}

// tracerLoggerCorrelationHandlerSource logs inside a parent manual span,
// inside a captured method's span, and inside a nested manual span, printing
// the live trace/span ids beside each record.
const tracerLoggerCorrelationHandlerSource = `
from opentelemetry import trace

from relay_runtime.logging import Logger
from relay_runtime.tracing import Tracer

TRACER = Tracer("handler.tracer")
LOG = Logger("tracer-correlation", "INFO")


@TRACER.capture_method
def traced(event, context):
    ctx = trace.get_current_span().get_span_context()
    print("captured trace=%032x span=%016x" % (ctx.trace_id, ctx.span_id))
    LOG.info("captured-record")
    with TRACER.span("manual.inner") as inner:
        ictx = inner.get_span_context()
        print("inner trace=%032x span=%016x" % (ictx.trace_id, ictx.span_id))
        LOG.info("manual-record")
    return "logged"


async def work(event, context):
    with TRACER.span("outer.parent") as parent:
        pctx = parent.get_span_context()
        print("parent trace=%032x span=%016x" % (pctx.trace_id, pctx.span_id))
        LOG.info("parent-record")
        traced(event, context)
    print("provider=%s" % type(trace.get_tracer_provider()).__name__)
    print("done")
`

// TestPythonRuntimeTracerLoggerIDsMatchExportedSpan pins the Logger/Tracer
// correlation through the real pipeline: for records emitted inside a parent
// manual span, inside a captured method's span, and inside a nested manual
// span, the record's trace_id/span_id equal the live native span at the call
// site AND reference the span that actually reached the receiver, with the
// expected parent chain — and neither component installed a provider.
func TestPythonRuntimeTracerLoggerIDsMatchExportedSpan(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	requireTracerImport(t, otelDir, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", tracerLoggerCorrelationHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=tracer-log-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "done") {
		t.Fatalf("handler did not run: %q", r.Out)
	}
	if !strings.Contains(r.Out, "provider=TracerProvider") {
		t.Errorf("Logger and Tracer must not install or replace a provider, out = %q", r.Out)
	}

	// The live ids each record was created beside
	parentTrace, parentSpan := requirePrintedIDs(t, r.Out, "parent")
	capturedTrace, capturedSpan := requirePrintedIDs(t, r.Out, "captured")
	innerTrace, innerSpan := requirePrintedIDs(t, r.Out, "inner")

	// Every record's correlation equals the live span at its call site...
	parentRec := awaitJSONRecord(t, p, "parent-record")
	capturedRec := awaitJSONRecord(t, p, "captured-record")
	innerRec := awaitJSONRecord(t, p, "manual-record")
	for message, rec := range map[string]map[string]any{
		"parent-record":   parentRec,
		"captured-record": capturedRec,
		"manual-record":   innerRec,
	} {
		requireCoreRecord(t, rec, "tracer-correlation", "INFO")
		if rec["trace_id"] != traceOne {
			t.Errorf("record %s trace_id = %v, want the carrier trace %s", message, rec["trace_id"], traceOne)
		}
	}
	if parentRec["span_id"] != parentSpan {
		t.Errorf("parent record span_id = %v, want the printed %s", parentRec["span_id"], parentSpan)
	}
	if capturedRec["span_id"] != capturedSpan {
		t.Errorf("captured record span_id = %v, want the printed %s", capturedRec["span_id"], capturedSpan)
	}
	if innerRec["span_id"] != innerSpan {
		t.Errorf("manual record span_id = %v, want the printed %s", innerRec["span_id"], innerSpan)
	}
	if parentRec["trace_id"] != parentTrace || capturedRec["trace_id"] != capturedTrace || innerRec["trace_id"] != innerTrace {
		t.Errorf("record trace ids (%v, %v, %v) must equal the printed live traces (%s, %s, %s)",
			parentRec["trace_id"], capturedRec["trace_id"], innerRec["trace_id"],
			parentTrace, capturedTrace, innerTrace)
	}

	// ...and every printed id references a span that actually reached the
	// receiver, forming the expected parent chain on the carrier trace.
	spans := rec.waitForSpans(t, 3)
	byID := make(map[string]otlpSpan, len(spans))
	for _, span := range spans {
		byID[span.SpanID] = span
	}
	parentExported, ok := byID[parentSpan]
	if !ok {
		t.Fatalf("no exported span for the parent record's id %s: %+v", parentSpan, spans)
	}
	if parentExported.Name != "outer.parent" || parentExported.TraceID != traceOne || parentExported.ParentSpanID != parentOne {
		t.Errorf("parent span = %+v, want outer.parent under the carrier parent", parentExported)
	}
	capturedExported, ok := byID[capturedSpan]
	if !ok {
		t.Fatalf("no exported span for the captured record's id %s: %+v", capturedSpan, spans)
	}
	if capturedExported.Name != "traced" || capturedExported.ParentSpanID != parentSpan {
		t.Errorf("captured span = %+v, want traced as a child of outer.parent", capturedExported)
	}
	innerExported, ok := byID[innerSpan]
	if !ok {
		t.Fatalf("no exported span for the manual record's id %s: %+v", innerSpan, spans)
	}
	if innerExported.Name != "manual.inner" || innerExported.ParentSpanID != capturedSpan {
		t.Errorf("manual span = %+v, want manual.inner as a child of the captured span", innerExported)
	}

	requireRecordCount(t, p, "parent-record", 1)
	requireRecordCount(t, p, "captured-record", 1)
	requireRecordCount(t, p, "manual-record", 1)

	closeAndWait(t, p, 15*time.Second)
}

// tracerAppOwnedHandlerSource uses the Tracer against whatever provider the
// application installed, then prints the global provider's type.
const tracerAppOwnedHandlerSource = `
from opentelemetry import trace

from relay_runtime.tracing import Tracer

TRACER = Tracer("handler.tracer")


async def work(event, context):
    with TRACER.span("app.owned.span") as span:
        span.set_attribute("owned", "yes")
        assert trace.get_current_span() is span
    print("provider=%s" % type(trace.get_tracer_provider()).__name__)
    print("done")
`

// TestPythonTracerAppOwnedProviderIsNotReplaced proves the Tracer attaches to
// an application-owned provider instead of replacing it: with
// RELAY_OTEL_PROVIDER=application and the app's provider registered at
// interpreter startup, the Tracer's spans reach only the application's
// receiver (joining the propagated carrier trace), Relay's configured
// endpoint receives nothing, the provider type the handler observes after
// Tracer use is still the application's own class, and the opt-out stays
// silent.
func TestPythonTracerAppOwnedProviderIsNotReplaced(t *testing.T) {
	otelDir := requireOtelPython(t)
	appOwned := newHTTPOTLPReceiver(t)
	relayOwned := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	requireTracerImport(t, otelDir, src)

	dir := t.TempDir()
	// The application's provider, registered at interpreter startup exactly
	// like a user-installed provider, with a distinguishing subclass so the
	// handler can prove the global provider is still the app's own after the
	// Tracer used it.
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(`
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


class AppTracerProvider(TracerProvider):
    pass


_provider = AppTracerProvider(resource=Resource.create({"service.name": "app-owned"}))
_provider.add_span_processor(BatchSpanProcessor(
    # The explicit endpoint constructor argument is the FULL signal URL.
    OTLPSpanExporter(endpoint=os.environ["APP_OTEL_ENDPOINT"] + "/v1/traces"),
))
trace.set_tracer_provider(_provider)
`), 0o644); err != nil {
		t.Fatalf("write sitecustomize: %v", err)
	}
	writeHandler(t, dir, "handler", tracerAppOwnedHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"RELAY_OTEL_PROVIDER=application",
		"APP_OTEL_ENDPOINT=" + appOwned.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": relayOwned.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "done") {
		t.Fatalf("handler did not run: %q", r.Out)
	}
	// The Tracer used the app's provider without replacing it.
	if !strings.Contains(r.Out, "provider=AppTracerProvider") {
		t.Errorf("the application's provider must remain global after Tracer use, out = %q", r.Out)
	}

	spans := appOwned.waitForSpans(t, 1)
	span := spans[0]
	if span.Name != "app.owned.span" {
		t.Errorf("app receiver span = %+v, want the Tracer's manual span", span)
	}
	if span.ServiceName != "app-owned" {
		t.Errorf("resource service.name = %q, want the application's own app-owned", span.ServiceName)
	}
	if span.TraceID != traceOne || span.ParentSpanID != parentOne {
		t.Errorf("span = trace %s parent %s, want the carrier trace", span.TraceID, span.ParentSpanID)
	}
	if span.Attributes["owned"] != "yes" {
		t.Errorf("span attributes = %v, want owned=yes", span.Attributes)
	}
	if span.StatusCode != tracev1.Status_STATUS_CODE_UNSET {
		t.Errorf("span status = %v, want UNSET", span.StatusCode)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(relayOwned.snapshot()); n != 0 {
		t.Errorf("Relay-configured receiver spans = %d, want 0 (Relay must create no provider or exporter in application mode)", n)
	}
	stderr := p.stderrString()
	if strings.Contains(stderr, "another tracer provider") || strings.Contains(stderr, "provider setup failed") {
		t.Errorf("the application-owned provider must be left alone silently, stderr:\n%s", stderr)
	}
}

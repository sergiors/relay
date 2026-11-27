//go:build integration

package python

// End-to-end coverage for the managed process-lifetime OpenTelemetry provider
// (see internal/runtime/python/_relay_telemetry.py) through the REAL embedded
// bootstrap, the REAL official SDK/exporters, and a controlled local OTLP
// receiver speaking the actual wire protocols. The bootstrap is driven over the
// authenticated execution channel exactly as Relay drives a warm container.
//
// The real OpenTelemetry packages are imported by the host interpreter when it
// already has them; otherwise they are materialized once per test run into a
// shared temporary directory (uv first, pip fallback) and placed on the child's
// PYTHONPATH. When neither works the tests FAIL with an explicit environmental
// message rather than skipping.

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// otelPythonProbe is the import that proves the host can run the provider
// scenarios: the SDK plus BOTH OTLP exporters, matching the managed install.
const otelPythonProbe = "import opentelemetry.sdk.trace, " +
	"opentelemetry.exporter.otlp.proto.grpc.trace_exporter, " +
	"opentelemetry.exporter.otlp.proto.http.trace_exporter"

// otelPythonSpecs (the managed-install set used below) lives in
// otel_floors_test.go, outside this file's integration build tag, so the
// floor-sync unit check runs in plain unit tests too.

var (
	otelPythonOnce sync.Once
	otelPythonDir  string // "" when the system interpreter already suffices
	otelPythonErr  error
)

// TestMain removes the shared materialized OTel install after the package's
// tests have run.
func TestMain(m *testing.M) {
	code := m.Run()
	if otelPythonDir != "" {
		_ = os.RemoveAll(otelPythonDir)
	}
	os.Exit(code)
}

// pythonProbe runs the import spec in a fresh interpreter and returns its
// combined output when it fails.
func pythonProbe(spec, pythonPath string) error {
	cmd := exec.Command("python3", "-c", spec)
	env := os.Environ()
	if pythonPath != "" {
		env = append(env, "PYTHONPATH="+pythonPath)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

// installOtelPython materializes the managed OTel set into a fresh temp
// directory with uv (preferred, matching production's installer) or pip.
// Every attempt's failure is preserved and reported together, so a broken
// fallback can never mask the primary installer's real error.
func installOtelPython() (string, error) {
	python, err := exec.LookPath("python3")
	if err != nil {
		return "", fmt.Errorf("locate python3: %w", err)
	}
	type installer struct {
		name string
		bin  string
		args []string
	}
	var attempts []installer
	if uv, err := exec.LookPath("uv"); err == nil {
		attempts = append(attempts, installer{name: "uv", bin: uv, args: []string{
			"pip", "install", "--python", python,
		}})
	} else {
		attempts = append(attempts, installer{name: "uv lookup"})
	}
	attempts = append(attempts, installer{name: "pip", bin: python, args: []string{
		"-m", "pip", "install", "--quiet",
	}})

	var failures []string
	for _, inst := range attempts {
		if inst.bin == "" {
			failures = append(failures, inst.name+": not found on PATH")
			continue
		}
		dir, err := os.MkdirTemp("", "relay-otel-python-")
		if err != nil {
			return "", err
		}
		args := append(append([]string{}, inst.args...), "--target", dir)
		args = append(args, otelPythonSpecs...)
		out, err := exec.Command(inst.bin, args...).CombinedOutput()
		if err != nil {
			failures = append(failures,
				fmt.Sprintf("%s install: %s: %s", inst.name, err, tailOutput(out)))
			_ = os.RemoveAll(dir)
			continue
		}
		if probeErr := pythonProbe(otelPythonProbe, dir); probeErr != nil {
			failures = append(failures,
				fmt.Sprintf("%s install: installed set is not importable: %s", inst.name, probeErr))
			_ = os.RemoveAll(dir)
			continue
		}
		return dir, nil
	}
	return "", fmt.Errorf("all managed-install attempts failed:\n  %s",
		strings.Join(failures, "\n  "))
}

func tailOutput(out []byte) string {
	const limit = 600
	s := strings.TrimSpace(string(out))
	if len(s) > limit {
		s = s[len(s)-limit:]
	}
	return s
}

// requireOtelPython resolves the shared real-OTel PYTHONPATH directory,
// installing it once per test run when the host interpreter lacks the
// packages. It FAILS (never skips) when neither the system nor a managed
// install can provide them.
func requireOtelPython(t *testing.T) string {
	t.Helper()
	requirePython(t)
	otelPythonOnce.Do(func() {
		if err := pythonProbe(otelPythonProbe, ""); err == nil {
			return
		}
		otelPythonDir, otelPythonErr = installOtelPython()
	})
	if otelPythonErr != nil {
		t.Fatalf("environmental blocker: python3 cannot import the managed OpenTelemetry set and no install succeeded: %v",
			otelPythonErr)
	}
	return otelPythonDir
}

// otelPythonEnv builds the child's PYTHONPATH override: the materialized OTel
// install (when any) followed by the given directories (the test dir with the
// embedded runtime modules and handlers, optionally the DI source tree).
func otelPythonEnv(otelDir string, dirs ...string) string {
	parts := make([]string, 0, len(dirs)+1)
	if otelDir != "" {
		parts = append(parts, otelDir)
	}
	parts = append(parts, dirs...)
	return "PYTHONPATH=" + strings.Join(parts, string(os.PathListSeparator))
}

// otlpEvent is one exported span event plus its attributes (the native
// exception event's name and payload, for example).
type otlpEvent struct {
	Name       string
	Attributes map[string]string
}

// otlpSpan is one exported span plus its resource attributes.
type otlpSpan struct {
	Name          string
	TraceID       string
	SpanID        string
	ParentSpanID  string
	ServiceName   string
	Attributes    map[string]string
	Resource      map[string]string
	StatusCode    tracev1.Status_StatusCode
	StatusMessage string
	Events        []otlpEvent
}

// otlpReceiver is a controlled local OTLP collector in the test process. It
// speaks the real wire formats: an HTTP/protobuf endpoint and a gRPC endpoint,
// both decoding ExportTraceServiceRequest with the official OTLP protos.
type otlpReceiver struct {
	mu       sync.Mutex
	spans    []otlpSpan
	auths    []string // Authorization header observed per HTTP export request
	endpoint string

	httpServer  *httptest.Server
	grpcServer  *grpc.Server
	grpcAddress string
}

func (r *otlpReceiver) record(req *collectortrace.ExportTraceServiceRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		resource := make(map[string]string, len(rs.GetResource().GetAttributes()))
		for _, attr := range rs.GetResource().GetAttributes() {
			resource[attr.GetKey()] = attr.GetValue().GetStringValue()
		}
		service := resource["service.name"]
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				attrs := make(map[string]string, len(sp.GetAttributes()))
				for _, a := range sp.GetAttributes() {
					attrs[a.GetKey()] = a.GetValue().GetStringValue()
				}
				events := make([]otlpEvent, 0, len(sp.GetEvents()))
				for _, ev := range sp.GetEvents() {
					eventAttrs := make(map[string]string, len(ev.GetAttributes()))
					for _, a := range ev.GetAttributes() {
						eventAttrs[a.GetKey()] = a.GetValue().GetStringValue()
					}
					events = append(events, otlpEvent{
						Name:       ev.GetName(),
						Attributes: eventAttrs,
					})
				}
				status := sp.GetStatus()
				r.spans = append(r.spans, otlpSpan{
					Name:          sp.GetName(),
					TraceID:       hex.EncodeToString(sp.GetTraceId()),
					SpanID:        hex.EncodeToString(sp.GetSpanId()),
					ParentSpanID:  hex.EncodeToString(sp.GetParentSpanId()),
					ServiceName:   service,
					Attributes:    attrs,
					Resource:      resource,
					StatusCode:    status.GetCode(),
					StatusMessage: status.GetMessage(),
					Events:        events,
				})
			}
		}
	}
}

// recordAuth remembers the Authorization header of one HTTP export request.
func (r *otlpReceiver) recordAuth(value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.auths = append(r.auths, value)
}

// authHeaders returns the Authorization headers observed so far.
func (r *otlpReceiver) authHeaders() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auths...)
}

// snapshot returns a copy of the spans exported so far.
func (r *otlpReceiver) snapshot() []otlpSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]otlpSpan(nil), r.spans...)
}

// waitForSpans polls until at least min spans have been exported, then returns
// them; it fails the test with the observed spans when the bound expires.
func (r *otlpReceiver) waitForSpans(t *testing.T, min int) []otlpSpan {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		spans := r.snapshot()
		if len(spans) >= min {
			return spans
		}
		if time.Now().After(deadline) {
			t.Fatalf("receiver got %d span(s), want >= %d; spans: %+v", len(spans), min, spans)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// newHTTPOTLPReceiver serves OTLP over HTTP/protobuf on a loopback httptest
// server. Its endpoint is the standard base URL (the exporter appends the
// signal path).
func newHTTPOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	r := &otlpReceiver{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var export collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.recordAuth(req.Header.Get("Authorization"))
		r.record(&export)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	r.httpServer = httptest.NewServer(mux)
	t.Cleanup(r.httpServer.Close)
	r.endpoint = r.httpServer.URL
	return r
}

type grpcTraceService struct {
	collectortrace.UnimplementedTraceServiceServer
	rec *otlpReceiver
}

func (s *grpcTraceService) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.rec.record(req)
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// newGRPCOTLPReceiver serves OTLP over gRPC on a loopback listener using the
// official TraceService.
func newGRPCOTLPReceiver(t *testing.T) *otlpReceiver {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the gRPC OTLP receiver: %v", err)
	}
	r := &otlpReceiver{}
	r.grpcServer = grpc.NewServer()
	collectortrace.RegisterTraceServiceServer(r.grpcServer, &grpcTraceService{rec: r})
	go func() { _ = r.grpcServer.Serve(lis) }()
	t.Cleanup(func() {
		r.grpcServer.Stop()
		_ = lis.Close()
	})
	r.grpcAddress = lis.Addr().String()
	r.endpoint = "http://" + r.grpcAddress
	return r
}

// traceCarrier returns a W3C traceparent for the given 32-hex trace id and
// 16-hex parent span id, sampled.
func traceCarrier(traceID, parentID string) map[string]string {
	return map[string]string{
		"traceparent": "00-" + traceID + "-" + parentID + "-01",
	}
}

const (
	traceOne    = "11111111111111111111111111111111"
	parentOne   = "aaaaaaaaaaaaaaaa"
	traceTwo    = "22222222222222222222222222222222"
	parentTwo   = "bbbbbbbbbbbbbbbb"
	traceThree  = "33333333333333333333333333333333"
	parentThree = "cccccccccccccccc"
)

// providerHandlerSource creates a real SDK span and records the global
// provider's type AT MODULE IMPORT time — the observable proof of the
// documented ordering: the provider is initialized after the first
// invocation's environment is applied and BEFORE the handler module is
// imported.
const providerHandlerSource = `
from opentelemetry import trace

IMPORT_PROVIDER = type(trace.get_tracer_provider()).__name__
_TRACER = trace.get_tracer("handler.mod")


def work(event, context):
    print("import-provider=%s" % IMPORT_PROVIDER)
    with _TRACER.start_as_current_span("handler.work") as span:
        span.set_attribute("work.step", event["step"])
    print("span-created step=%s" % event["step"])
`

// closeAndWait closes the authenticated channel (Relay is done) and waits for
// a clean exit within bound, returning the elapsed time.
func closeAndWait(t *testing.T, p *pyProc, bound time.Duration) time.Duration {
	t.Helper()
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	start := time.Now()
	if err := p.waitExit(t, bound); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s", err, p.stderrString())
	}
	return time.Since(start)
}

// assertSpanNamesProveNoInvocationSpan fails when any exported span is not
// the application's own: Relay adds no invocation span inside the managed
// process.
func assertSpanNamesProveNoInvocationSpan(t *testing.T, spans []otlpSpan) {
	t.Helper()
	for _, span := range spans {
		if span.Name != "handler.work" {
			t.Errorf("unexpected exported span %q: the managed process must export only application spans", span.Name)
		}
	}
}

// TestPythonManagedProviderExportsHTTPSpans is the core scenario over
// OTLP/HTTP protobuf: the provider initializes ONCE — after the first
// invocation's environment is applied (the endpoint arrives only in that
// frame) and before the handler module is imported — SDK spans join the Relay
// carrier's trace as children, the Resource carries the configured
// service.name, a warm reuse still exports, and a later endpoint change in the
// frame cannot reconfigure the already-initialized provider.
func TestPythonManagedProviderExportsHTTPSpans(t *testing.T) {
	otelDir := requireOtelPython(t)
	recA := newHTTPOTLPReceiver(t)
	recB := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_SERVICE_NAME=orders-svc",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	// Given the endpoint, exporter authentication, and resource attributes only
	// in the FIRST invocation's application environment — exactly where the
	// environment boundary keeps application values — when the handler module
	// is imported the provider is already registered with that configuration
	// applied, and its span joins the invocation's carrier trace.
	const frameAuthCanary = "CANARY-FRAME-AUTH-e2e"
	r1 := p.invokeTrace(t, "handler.work", `{"step":"1"}`, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": recA.endpoint,
		"OTEL_EXPORTER_OTLP_HEADERS":  "authorization=" + frameAuthCanary,
		"OTEL_RESOURCE_ATTRIBUTES":    "deployment.environment=staging",
	}, traceCarrier(traceOne, parentOne))
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "import-provider=TracerProvider") {
		t.Fatalf("module import must observe the initialized SDK provider, out = %q", r1.Out)
	}
	if !strings.Contains(r1.Out, "span-created step=1") {
		t.Fatalf("handler did not run: %q", r1.Out)
	}

	spans := recA.waitForSpans(t, 1)
	var child *otlpSpan
	for i := range spans {
		if spans[i].Name == "handler.work" && spans[i].TraceID == traceOne {
			child = &spans[i]
		}
	}
	if child == nil {
		t.Fatalf("no handler.work span on the carrier trace; spans: %+v", spans)
	}
	if child.ParentSpanID != parentOne {
		t.Errorf("span parent = %s, want the carrier's parent span %s", child.ParentSpanID, parentOne)
	}
	if child.SpanID == parentOne || child.SpanID == "" {
		t.Errorf("span id = %q, want its own id distinct from the parent", child.SpanID)
	}
	if child.ServiceName != "orders-svc" {
		t.Errorf("resource service.name = %q, want orders-svc", child.ServiceName)
	}
	if child.Attributes["work.step"] != "1" {
		t.Errorf("span attributes = %v, want work.step=1", child.Attributes)
	}
	if got := child.Resource["deployment.environment"]; got != "staging" {
		t.Errorf("resource deployment.environment = %q, want the frame's staging (app resource config applied before import)", got)
	}
	if auths := recA.authHeaders(); len(auths) == 0 || auths[len(auths)-1] != frameAuthCanary {
		t.Errorf("export Authorization headers = %v, want the frame's auth value %q", auths, frameAuthCanary)
	}
	assertSpanNamesProveNoInvocationSpan(t, spans)

	// Warm reuse: a second invocation with NO endpoint in its frame still
	// exports — the provider was initialized exactly once.
	r2 := p.invokeTrace(t, "handler.work", `{"step":"2"}`, nil,
		traceCarrier(traceTwo, parentTwo))
	if !r2.OK {
		t.Fatalf("warm reuse invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "import-provider=TracerProvider") {
		t.Errorf("warm invocation must observe the same initialized provider, out = %q", r2.Out)
	}
	spans = recA.waitForSpans(t, 2)
	found := false
	for _, sp := range spans {
		if sp.TraceID == traceTwo && sp.ParentSpanID == parentTwo {
			found = true
		}
	}
	if !found {
		t.Errorf("warm span did not join the second carrier trace; spans: %+v", spans)
	}

	// Given a CHANGED endpoint in a later frame, when the invocation runs, the
	// provider must not reconfigure: exports keep flowing to the first
	// receiver and the second stays empty.
	r3 := p.invokeTrace(t, "handler.work", `{"step":"3"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": recB.endpoint},
		traceCarrier(traceThree, parentThree))
	if !r3.OK {
		t.Fatalf("rotated-endpoint invoke failed: %s", r3.Err)
	}
	spans = recA.waitForSpans(t, 3)
	found = false
	for _, sp := range spans {
		if sp.TraceID == traceThree {
			found = true
		}
	}
	if !found {
		t.Errorf("span after an endpoint frame change must still reach the original receiver; spans: %+v", spans)
	}
	if n := len(recB.snapshot()); n != 0 {
		t.Errorf("second receiver got %d span(s), want 0 (the provider is initialized once per process)", n)
	}
	assertSpanNamesProveNoInvocationSpan(t, spans)

	// The graceful channel close flushes the provider on shutdown.
	closeAndWait(t, p, 15*time.Second)
	if n := len(recA.snapshot()); n < 3 {
		t.Errorf("receiver spans after shutdown flush = %d, want >= 3", n)
	}
	if n := len(recB.snapshot()); n != 0 {
		t.Errorf("second receiver spans after shutdown = %d, want 0", n)
	}
}

// TestPythonManagedProviderExportsGRPCSpans exercises the other supported
// transport end to end: with the gRPC protocol configured, the managed
// exporter delivers spans to a real gRPC OTLP endpoint and the Resource keeps
// the configured service name.
func TestPythonManagedProviderExportsGRPCSpans(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newGRPCOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_SERVICE_NAME=grpc-svc",
		"OTEL_EXPORTER_OTLP_PROTOCOL=grpc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"g"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=TracerProvider") {
		t.Fatalf("module import must observe the initialized SDK provider, out = %q", r.Out)
	}

	spans := rec.waitForSpans(t, 1)
	sp := spans[0]
	if sp.Name != "handler.work" || sp.TraceID != traceOne || sp.ParentSpanID != parentOne {
		t.Errorf("gRPC span = %+v, want handler.work on the carrier trace", sp)
	}
	if sp.ServiceName != "grpc-svc" {
		t.Errorf("resource service.name = %q, want grpc-svc", sp.ServiceName)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(rec.snapshot()); n < 1 {
		t.Errorf("gRPC receiver spans after shutdown flush = %d, want >= 1", n)
	}
}

// TestPythonManagedProviderDisabledByEnv proves the standard opt-out: with
// OTEL_SDK_DISABLED=true and a configured endpoint, Relay creates NO provider
// and NO exporter — the process keeps the proxy sentinel and nothing is ever
// delivered to the endpoint.
func TestPythonManagedProviderDisabledByEnv(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_SDK_DISABLED=true",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"d"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke with a disabled SDK must still run: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
		t.Errorf("disabled process must keep the proxy provider, out = %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(rec.snapshot()); n != 0 {
		t.Errorf("receiver spans = %d, want 0 (OTEL_SDK_DISABLED constructs no exporter)", n)
	}
}

// TestPythonWorkerDisabledOverriddenByAppFrame pins the source-tier
// precedence for the SDK opt-out: the worker injects OTEL_SDK_DISABLED=true
// into the container environment, an application frame value of false is
// applied before provider initialization and starts the provider anyway.
func TestPythonWorkerDisabledOverriddenByAppFrame(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_SDK_DISABLED=true",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"f"}`, map[string]string{
		"OTEL_SDK_DISABLED":           "false",
		"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
	}, traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=TracerProvider") {
		t.Errorf("the application frame's OTEL_SDK_DISABLED=false must win, out = %q", r.Out)
	}
	rec.waitForSpans(t, 1)

	closeAndWait(t, p, 15*time.Second)
}

// TestPythonManagedProviderWithoutEndpointStaysDisabled proves there is no
// implicit localhost exporter: with no endpoint anywhere the process never
// constructs a provider (the proxy sentinel stays), so no collector can be
// dialed — the operator names the target or nothing is exported.
func TestPythonManagedProviderWithoutEndpointStaysDisabled(t *testing.T) {
	otelDir := requireOtelPython(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"n"}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke must run without any endpoint: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
		t.Errorf("a process with no endpoint must keep the proxy provider (no exporter, no dial), out = %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)
}

// TestPythonApplicationOwnershipOptOutKeepsProviderUnmanaged proves the
// app-only ownership opt-out: with RELAY_OTEL_PROVIDER=application the
// bootstrap creates nothing even though an endpoint is configured, leaving the
// application free to install and own its provider.
func TestPythonApplicationOwnershipOptOutKeepsProviderUnmanaged(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"RELAY_OTEL_PROVIDER=application",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"o"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
		t.Errorf("the ownership opt-out must keep the proxy provider, out = %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(rec.snapshot()); n != 0 {
		t.Errorf("receiver spans = %d, want 0 (Relay must not own a provider in application mode)", n)
	}
	if strings.Contains(p.stderrString(), "another tracer provider") ||
		strings.Contains(p.stderrString(), "provider setup failed") {
		t.Errorf("opt-out must be silent, stderr:\n%s", p.stderrString())
	}
}

// TestPythonPreExistingProviderPreserved proves application-owned mode when
// the application registered its provider BEFORE the bootstrap ran
// (sitecustomize at interpreter startup): its provider serves the handler's
// spans — Relay neither replaces it nor shuts it down before its own
// at-exit flush — and Relay's own configured endpoint receives nothing.
func TestPythonPreExistingProviderPreserved(t *testing.T) {
	otelDir := requireOtelPython(t)
	appOwned := newHTTPOTLPReceiver(t)
	relayOwned := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(`
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_provider = TracerProvider(resource=Resource.create({"service.name": "app-owned"}))
_provider.add_span_processor(BatchSpanProcessor(
    # The explicit endpoint constructor argument is the FULL signal URL.
    OTLPSpanExporter(endpoint=os.environ["APP_OTEL_ENDPOINT"] + "/v1/traces")
))
trace.set_tracer_provider(_provider)
`), 0o644); err != nil {
		t.Fatalf("write sitecustomize: %v", err)
	}
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"APP_OTEL_ENDPOINT=" + appOwned.endpoint,
		"OTEL_SERVICE_NAME=relay-managed-svc",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"p"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": relayOwned.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=TracerProvider") {
		t.Fatalf("the application's pre-registered provider must be the global one, out = %q", r.Out)
	}

	// The application's provider serves spans while the process runs — proof
	// Relay did not replace or shut it down.
	spans := appOwned.waitForSpans(t, 1)
	if spans[0].ServiceName != "app-owned" {
		t.Errorf("resource service.name = %q, want the application's own app-owned", spans[0].ServiceName)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(relayOwned.snapshot()); n != 0 {
		t.Errorf("Relay-configured receiver spans = %d, want 0 (a pre-existing provider is application-owned)", n)
	}
	stderr := p.stderrString()
	if strings.Contains(stderr, "another tracer provider") || strings.Contains(stderr, "provider setup failed") {
		t.Errorf("a pre-existing provider must be left alone silently, stderr:\n%s", stderr)
	}
}

// TestPythonW3CExtractionIndependentOfGlobalPropagator proves Relay parses its
// carrier with its own W3C extractors: an application that installed a
// completely different GLOBAL propagator still receives the incoming trace
// identity, and its propagator is left untouched.
func TestPythonW3CExtractionIndependentOfGlobalPropagator(t *testing.T) {
	otelDir := requireOtelPython(t)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(`
from opentelemetry import propagate
from opentelemetry.propagators import textmap


class CustomPropagator(textmap.TextMapPropagator):
    @property
    def fields(self):
        return {"x-custom"}

    def extract(self, carrier, context=None, getter=textmap.default_getter):
        return context

    def inject(self, carrier, context=None, setter=textmap.default_setter):
        carrier["x-custom"] = "custom"


propagate.set_global_textmap(CustomPropagator())
`), 0o644); err != nil {
		t.Fatalf("write sitecustomize: %v", err)
	}
	writeHandler(t, dir, "handler", `
from opentelemetry import propagate as otel_propagate
from opentelemetry import trace


def inspect(event, context):
    propagator = otel_propagate.get_global_textmap()
    ctx = trace.get_current_span().get_span_context()
    print("propagator=%s" % type(propagator).__name__)
    print("trace=%032x valid=%s" % (ctx.trace_id, ctx.is_valid))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{otelPythonEnv(otelDir, dir)})

	r := p.invokeTrace(t, "handler.inspect", `{}`, nil, traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "propagator=CustomPropagator") {
		t.Errorf("the application's global propagator must stay installed, out = %q", r.Out)
	}
	if !strings.Contains(r.Out, "trace="+traceOne+" valid=True") {
		t.Errorf("Relay's own W3C extraction must attach the carrier trace regardless of the global propagator, out = %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)
}

// TestPythonMalformedBaggageKeepsTraceIdentity proves extraction is
// best-effort per component: valid baggage arrives with the trace, while a
// malformed baggage member cannot discard an otherwise valid trace identity or
// fail the invocation.
func TestPythonMalformedBaggageKeepsTraceIdentity(t *testing.T) {
	otelDir := requireOtelPython(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from opentelemetry import baggage, trace


def work(event, context):
    ctx = trace.get_current_span().get_span_context()
    print("trace=%032x" % ctx.trace_id)
    print("baggage=%s" % sorted(baggage.get_all().items()))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{otelPythonEnv(otelDir, dir)})

	good := traceCarrier(traceOne, parentOne)
	good["baggage"] = "user=alice"
	r1 := p.invokeTrace(t, "handler.work", `{}`, nil, good)
	if !r1.OK {
		t.Fatalf("valid-carrier invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "trace="+traceOne) || !strings.Contains(r1.Out, "baggage=[('user', 'alice')]") {
		t.Errorf("valid carrier out = %q, want the trace id and baggage", r1.Out)
	}

	bad := traceCarrier(traceTwo, parentTwo)
	bad["baggage"] = "!!!not-baggage"
	r2 := p.invokeTrace(t, "handler.work", `{}`, nil, bad)
	if !r2.OK {
		t.Fatalf("malformed baggage must not fail the invocation: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "trace="+traceTwo) {
		t.Errorf("malformed baggage must keep the trace identity, out = %q", r2.Out)
	}
	if !strings.Contains(r2.Out, "baggage=[]") {
		t.Errorf("malformed baggage must degrade to empty baggage, out = %q", r2.Out)
	}

	closeAndWait(t, p, 15*time.Second)
}

// TestPythonContextDetachedAfterHandlerError proves the per-invocation
// boundary with the real API: a traced invocation that RAISES still detaches
// its context, so the warm process's next untraced invocation observes no
// leftover span.
func TestPythonContextDetachedAfterHandlerError(t *testing.T) {
	otelDir := requireOtelPython(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from opentelemetry import trace


def traced(event, context):
    ctx = trace.get_current_span().get_span_context()
    print("trace=%032x" % ctx.trace_id)
    raise ValueError("traced-boom")


def inspect(event, context):
    ctx = trace.get_current_span().get_span_context()
    print("current=%032x valid=%s" % (ctx.trace_id, ctx.is_valid))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{otelPythonEnv(otelDir, dir)})

	r1 := p.invokeTrace(t, "handler.traced", `{}`, nil, traceCarrier(traceOne, parentOne))
	if r1.OK || !strings.Contains(r1.Err, "traced-boom") {
		t.Fatalf("raising handler = %+v, want the handler failure after seeing the trace", r1)
	}
	if !strings.Contains(r1.Out, "trace="+traceOne) {
		t.Errorf("the traced invocation must observe the carrier trace, out = %q", r1.Out)
	}

	r2 := p.invoke(t, "handler.inspect", `{}`, nil)
	if !r2.OK {
		t.Fatalf("post-failure untraced invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "current=00000000000000000000000000000000 valid=False") {
		t.Errorf("the untraced invocation must see a clean context (no leaked span), out = %q", r2.Out)
	}

	closeAndWait(t, p, 15*time.Second)
}

// TestPythonDependencyCleanupRunsBeforeProviderShutdown proves the shutdown
// ordering inside the bootstrap: container-scoped dependency cleanup runs
// while the provider is STILL ALIVE (a span created during cleanup is
// exported by the provider's own flush that follows), so a provider shutdown
// before cleanup would silently drop it.
func TestPythonDependencyCleanupRunsBeforeProviderShutdown(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)
	src := relayRuntimeSrc(t)
	_ = requireRelayRuntimeImport(t, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from opentelemetry import trace

from relay_runtime import Depends, bind


def provide_resource():
    yield "res"
    tracer = trace.get_tracer("di")
    with tracer.start_as_current_span("di.cleanup"):
        print("cleanup-span-started")


@bind
def serve(event, context, res=Depends(provide_resource, scope="container")):
    print("served %s" % res)
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=di-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invoke(t, "handler.serve", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "served res") {
		t.Fatalf("handler did not run: %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)

	// The cleanup span was created AFTER the channel closed (during DI
	// cleanup) and still reached the receiver: the provider outlived the
	// cleanup and flushed afterwards.
	spans := rec.snapshot()
	found := false
	for _, sp := range spans {
		if sp.Name == "di.cleanup" {
			found = true
			if sp.ServiceName != "di-svc" {
				t.Errorf("cleanup span service.name = %q, want di-svc", sp.ServiceName)
			}
		}
	}
	if !found {
		t.Errorf("the container-scoped cleanup span was not exported — provider shutdown must follow DI cleanup; spans: %+v", spans)
	}
	out := p.stdoutString()
	if strings.LastIndex(out, "cleanup-span-started") < strings.LastIndex(out, "served res") {
		t.Errorf("cleanup must run during graceful shutdown after the last invocation:\n%s", out)
	}
}

// TestPythonProviderShutdownBoundedOnExporterOutage proves the bounded flush:
// when the collector accepts connections but never answers, an invocation
// still succeeds (telemetry is best-effort), and the graceful channel close
// abandons the stuck export at the fixed shutdown bound instead of hanging the
// container's exit on the exporter's own timeout.
func TestPythonProviderShutdownBoundedOnExporterOutage(t *testing.T) {
	otelDir := requireOtelPython(t)

	// A black-hole collector: it accepts connections and never responds, so
	// every export attempt would block until the exporter's (much larger)
	// timeout.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the black-hole collector: %v", err)
	}
	var heldMu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			heldMu.Lock()
			held = append(held, conn)
			heldMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		heldMu.Lock()
		for _, conn := range held {
			_ = conn.Close()
		}
		heldMu.Unlock()
	})

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://" + lis.Addr().String(),
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_EXPORTER_OTLP_TIMEOUT=60000",
		"OTEL_BSP_SCHEDULE_DELAY=20",
		"OTEL_SERVICE_NAME=outage-svc",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"x"}`, nil,
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("an exporter outage must never fail the invocation: %s", r.Err)
	}

	// The flush is bounded: the process exits well before the exporter's
	// 60-second transport timeout could elapse.
	closeAndWait(t, p, 30*time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), "provider shutdown exceeded 8s") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(p.stderrString(), "provider shutdown exceeded 8s") {
		t.Errorf("the stuck flush must be abandoned at the coordinated 8s shutdown bound; stderr:\n%s",
			p.stderrString())
	}
}

// TestPythonProviderPartialInitIsDiscardedNotLeaked pins the partial-init
// boundary: when provider registration fails AFTER the provider and its batch
// processor were constructed (the processor's export worker already started,
// never attached), the failure is reported once, only Relay's own objects are
// released, the handler import observes NO registered provider, and neither
// the invocation nor the channel-close exit is affected — best-effort
// telemetry can never hang or fail the container.
func TestPythonProviderPartialInitIsDiscardedNotLeaked(t *testing.T) {
	otelDir := requireOtelPython(t)

	dir := t.TempDir()
	// fault injection at the process boundary: a sitecustomize on the test
	// PYTHONPATH runs before the bootstrap and makes the registration step
	// fail deterministically, without touching production code
	if err := os.WriteFile(filepath.Join(dir, "sitecustomize.py"), []byte(`
from opentelemetry.sdk.trace import TracerProvider


def _fail_add_span_processor(self, span_processor):
    raise RuntimeError("injected add_span_processor failure")


TracerProvider.add_span_processor = _fail_add_span_processor
`), 0o644); err != nil {
		t.Fatalf("write sitecustomize fault injection: %v", err)
	}
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"OTEL_SERVICE_NAME=partial-svc",
	})

	// when the first invocation applies its endpoint so Relay attempts (and
	// fails) provider registration
	r := p.invoke(t, "handler.work", `{"step":"1"}`, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:9",
	})

	// then the telemetry failure never fails the invocation
	if !r.OK {
		t.Fatalf("invoke failed despite best-effort telemetry: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
		t.Errorf("out = %q, want the handler import to observe NO registered provider", r.Out)
	}
	if !strings.Contains(r.Out, "span-created step=1") {
		t.Errorf("handler did not run: %q", r.Out)
	}

	// and the partial initialization is reported once
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), "provider registration failed (RuntimeError)") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(p.stderrString(), "provider registration failed (RuntimeError)") {
		t.Errorf("stderr = %q, want the registration-failure diagnostic", p.stderrString())
	}

	// and the discarded provider and unattached processor shut down without
	// hanging: the channel-close exit stays inside the bounded budget
	closeAndWait(t, p, 5*time.Second)
}

// TestPythonRelayOtelProviderInvalidValueFailsSafe proves the ownership
// setting's safe failure mode: RELAY_OTEL_PROVIDER only accepts "managed" or
// "application"; any other non-empty value is invalid, so Relay skips provider
// creation entirely (a typo can never silently override a provider the
// application installs) and emits one fixed diagnostic that names the setting
// but never echoes the supplied value.
func TestPythonRelayOtelProviderInvalidValueFailsSafe(t *testing.T) {
	otelDir := requireOtelPython(t)
	rec := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"RELAY_OTEL_PROVIDER=some-other-tool",
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=invalid-optout-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	// The invocation still runs; no provider is constructed despite the
	// configured endpoint.
	r := p.invokeTrace(t, "handler.work", `{"step":"i"}`,
		map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint},
		traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("an invalid ownership value must never fail the invocation: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
		t.Errorf("an invalid ownership value must skip provider creation, out = %q", r.Out)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(rec.snapshot()); n != 0 {
		t.Errorf("receiver spans = %d, want 0 (no provider is created for an invalid value)", n)
	}
	stderr := p.stderrString()
	if !strings.Contains(stderr, "unsupported value for RELAY_OTEL_PROVIDER") {
		t.Errorf("the invalid value must be reported with a fixed diagnostic, stderr:\n%s", stderr)
	}
	if strings.Contains(stderr, "some-other-tool") {
		t.Errorf("the diagnostic must never echo the supplied value, stderr:\n%s", stderr)
	}
}

// TestPythonManagedEndpointCredentialGuard proves the pre-construction
// endpoint guard: an effective endpoint embedding userinfo, a query, or a
// fragment is rejected BEFORE any exporter exists — with a fixed diagnostic
// that names the variable but never echoes the secret — while a credential-free
// endpoint keeps working with authentication headers, and the trace-specific
// endpoint keeps standard precedence over the generic one in BOTH the guard
// and the export target.
func TestPythonManagedEndpointCredentialGuard(t *testing.T) {
	otelDir := requireOtelPython(t)

	const (
		urlUserinfoCanary = "relaycanary:secretpw"
		queryCanary       = "CANARYQUERYSECRET"
		fragmentCanary    = "CANARYFRAGMENTSECRET"
		authorityCanary   = "otp-canary@collector"
		headerCanary      = "GUARD-HEADER-CANARY-42"
		genericDiagName   = "OTEL_EXPORTER_OTLP_ENDPOINT"
		traceSpecificDiag = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
		credentialsMarker = "includes userinfo, a query, or a fragment"
	)

	spawn := func(t *testing.T, extraEnv ...string) *pyProc {
		t.Helper()
		dir := t.TempDir()
		writeHandler(t, dir, "handler", providerHandlerSource)
		env := append([]string{
			otelPythonEnv(otelDir, dir),
			"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
			"OTEL_BSP_SCHEDULE_DELAY=50",
		}, extraEnv...)
		return startEmbeddedBootstrap(t, dir, Bootstrap, env)
	}

	// reject asserts the safe-failure shape: the invocation runs, no provider
	// is constructed, the fixed diagnostic names the variable, and no secret
	// value from the endpoint appears anywhere on stderr.
	reject := func(t *testing.T, p *pyProc, r invokeResult, variable string, secrets ...string) {
		t.Helper()
		if !r.OK {
			t.Fatalf("a rejected endpoint must never fail the invocation: %s", r.Err)
		}
		if !strings.Contains(r.Out, "import-provider=ProxyTracerProvider") {
			t.Errorf("a credential-bearing endpoint must skip provider creation, out = %q", r.Out)
		}
		closeAndWait(t, p, 15*time.Second)
		stderr := p.stderrString()
		if !strings.Contains(stderr, variable+" "+credentialsMarker) {
			t.Errorf("stderr must carry the fixed diagnostic for %s, got:\n%s", variable, stderr)
		}
		for _, secret := range secrets {
			if strings.Contains(stderr, secret) {
				t.Errorf("stderr echoes the secret value %q:\n%s", secret, stderr)
			}
			if strings.Contains(p.stdoutString(), secret) {
				t.Errorf("stdout echoes the secret value %q", secret)
			}
		}
	}

	t.Run("generic endpoint with userinfo is rejected", func(t *testing.T) {
		p := spawn(t)
		r := p.invokeTrace(t, "handler.work", `{"step":"u"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://" + urlUserinfoCanary + "@127.0.0.1:4318",
		}, traceCarrier(traceOne, parentOne))
		reject(t, p, r, genericDiagName, urlUserinfoCanary, "secretpw")
	})

	t.Run("gRPC authority endpoint with userinfo is rejected", func(t *testing.T) {
		p := spawn(t, "OTEL_EXPORTER_OTLP_PROTOCOL=grpc")
		r := p.invokeTrace(t, "handler.work", `{"step":"a"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": authorityCanary + ":4317",
		}, traceCarrier(traceOne, parentOne))
		reject(t, p, r, genericDiagName, authorityCanary)
	})

	t.Run("trace-specific endpoint with query is rejected beside a safe generic", func(t *testing.T) {
		p := spawn(t)
		// The trace-specific value wins standard precedence, so the guard must
		// judge IT and name it — a safe generic endpoint cannot mask it.
		r := p.invokeTrace(t, "handler.work", `{"step":"q"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://127.0.0.1:4318",
			"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4318/v1/traces?apikey=" + queryCanary,
		}, traceCarrier(traceOne, parentOne))
		reject(t, p, r, traceSpecificDiag, queryCanary)
	})

	t.Run("endpoint with fragment is rejected", func(t *testing.T) {
		p := spawn(t)
		r := p.invokeTrace(t, "handler.work", `{"step":"f"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318#" + fragmentCanary,
		}, traceCarrier(traceOne, parentOne))
		reject(t, p, r, genericDiagName, fragmentCanary)
	})

	t.Run("safe endpoint with authentication headers is accepted", func(t *testing.T) {
		rec := newHTTPOTLPReceiver(t)
		p := spawn(t)
		r := p.invokeTrace(t, "handler.work", `{"step":"s"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_EXPORTER_OTLP_HEADERS":  "authorization=" + headerCanary,
		}, traceCarrier(traceOne, parentOne))
		if !r.OK {
			t.Fatalf("a credential-free endpoint with headers must run: %s", r.Err)
		}
		if !strings.Contains(r.Out, "import-provider=TracerProvider") {
			t.Fatalf("a safe endpoint with headers must create the provider, out = %q", r.Out)
		}
		spans := rec.waitForSpans(t, 1)
		if spans[0].Name != "handler.work" {
			t.Errorf("span = %+v, want handler.work", spans[0])
		}
		if auths := rec.authHeaders(); len(auths) == 0 || auths[len(auths)-1] != headerCanary {
			t.Errorf("export Authorization headers = %v, want %q", auths, headerCanary)
		}
		closeAndWait(t, p, 15*time.Second)
	})

	t.Run("trace-specific safe endpoint wins over generic", func(t *testing.T) {
		generic := newHTTPOTLPReceiver(t)
		traces := newHTTPOTLPReceiver(t)
		p := spawn(t)
		r := p.invokeTrace(t, "handler.work", `{"step":"p"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT":        generic.endpoint,
			"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": traces.endpoint + "/v1/traces",
		}, traceCarrier(traceOne, parentOne))
		if !r.OK {
			t.Fatalf("invoke failed: %s", r.Err)
		}
		spans := traces.waitForSpans(t, 1)
		if spans[0].TraceID != traceOne {
			t.Errorf("trace-specific span = %+v, want the carrier trace", spans[0])
		}
		if n := len(generic.snapshot()); n != 0 {
			t.Errorf("generic receiver spans = %d, want 0 (trace-specific endpoint has precedence)", n)
		}
		closeAndWait(t, p, 15*time.Second)
	})

	t.Run("safe trace-specific endpoint bypasses a credential-bearing generic", func(t *testing.T) {
		traces := newHTTPOTLPReceiver(t)
		p := spawn(t)
		r := p.invokeTrace(t, "handler.work", `{"step":"b"}`, map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://" + urlUserinfoCanary + "@127.0.0.1:4318",
			"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": traces.endpoint + "/v1/traces",
		}, traceCarrier(traceOne, parentOne))
		if !r.OK {
			t.Fatalf("invoke failed: %s", r.Err)
		}
		if !strings.Contains(r.Out, "import-provider=TracerProvider") {
			t.Fatalf("the effective (trace-specific) endpoint is safe, so the provider must start, out = %q", r.Out)
		}
		traces.waitForSpans(t, 1)
		closeAndWait(t, p, 15*time.Second)
		stderr := p.stderrString()
		if strings.Contains(stderr, credentialsMarker) {
			t.Errorf("the unused generic endpoint must not trigger the credential diagnostic, stderr:\n%s", stderr)
		}
		if strings.Contains(stderr, urlUserinfoCanary) || strings.Contains(stderr, "secretpw") {
			t.Errorf("stderr echoes the unused generic endpoint's userinfo, stderr:\n%s", stderr)
		}
	})
}

// TestPythonAppOwnedTracerProviderEntrySelection proves the standard app-owned
// provider selection through the real API: a frame value for
// OTEL_PYTHON_TRACER_PROVIDER names an application entry point, the API loads
// THAT provider during Relay's ownership check, and Relay therefore creates
// nothing — the application's provider serves the spans while Relay's own
// configured endpoint receives nothing. On the Go side the selection value is
// part of the private provider-generation identity, so changing it rotates
// the warm process (see TestManagedAppFingerprintCoversAppOTelChanges and
// TestManagedAppInvocationFingerprint).
func TestPythonAppOwnedTracerProviderEntrySelection(t *testing.T) {
	otelDir := requireOtelPython(t)
	appOwned := newHTTPOTLPReceiver(t)
	relayOwned := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	// A minimal installable distribution exposing the official
	// opentelemetry_tracer_provider entry point, discoverable on the test
	// PYTHONPATH exactly like a user-installed provider package.
	writeFile := func(path, contents string) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	writeFile(filepath.Join(dir, "testtracer.py"), `
import os

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


class AppTracerProvider(TracerProvider):
    pass


def make_provider():
    provider = AppTracerProvider(resource=Resource.create({"service.name": "entry-owned"}))
    provider.add_span_processor(BatchSpanProcessor(
        # The explicit endpoint constructor argument is the FULL signal URL.
        OTLPSpanExporter(endpoint=os.environ["APP_OTEL_ENDPOINT"] + "/v1/traces")
    ))
    return provider
`)
	writeFile(filepath.Join(dir, "testtracer-1.0.dist-info", "METADATA"),
		"Metadata-Version: 2.1\nName: testtracer\nVersion: 1.0\n")
	writeFile(filepath.Join(dir, "testtracer-1.0.dist-info", "entry_points.txt"),
		"[opentelemetry_tracer_provider]\nappowned = testtracer:make_provider\n")

	writeHandler(t, dir, "handler", providerHandlerSource)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir),
		"APP_OTEL_ENDPOINT=" + appOwned.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.work", `{"step":"e"}`, map[string]string{
		"OTEL_PYTHON_TRACER_PROVIDER": "appowned",
		"OTEL_EXPORTER_OTLP_ENDPOINT": relayOwned.endpoint,
	}, traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "import-provider=AppTracerProvider") {
		t.Fatalf("the frame's OTEL_PYTHON_TRACER_PROVIDER must select the application's provider, out = %q", r.Out)
	}

	// The application's provider serves spans while the process runs; Relay's
	// configured endpoint stays untouched because Relay created nothing.
	spans := appOwned.waitForSpans(t, 1)
	if spans[0].ServiceName != "entry-owned" {
		t.Errorf("resource service.name = %q, want the entry-point provider's entry-owned", spans[0].ServiceName)
	}

	closeAndWait(t, p, 15*time.Second)
	if n := len(relayOwned.snapshot()); n != 0 {
		t.Errorf("Relay-configured receiver spans = %d, want 0 (the app-owned provider must be preserved)", n)
	}
	stderr := p.stderrString()
	if strings.Contains(stderr, "another tracer provider") || strings.Contains(stderr, "provider setup failed") {
		t.Errorf("entry-point provider selection must be silent, stderr:\n%s", stderr)
	}
}

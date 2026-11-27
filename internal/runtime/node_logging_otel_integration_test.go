//go:build integration

package runtime

// Real-manager managed-OTel LOGGING correlation acceptance for the Node
// engine: ONE nodejs26.x invocation image driven by the production Manager →
// Prepare → Execute path — production frame encoding of a REAL Go-managed
// invocation trace, production execEnv, production /relay/otel install — with
// the PACKAGED public SDK (@sergiors/relay-runtime) vendored into the app
// source exactly as an application dependency would ship it. The handler
// opens a NESTED active span through the SDK Tracer (never the raw OTel API)
// under the injected trace context and logs once inside it through the SDK
// Logger; the test asserts the stderr JSON record's trace_id/span_id
// against BOTH exports: the Go in-process export of the worker-created
// invocation span, and the container's OTLP export of the nested span as
// received by a controlled local collector.
//
// The collector binds all interfaces and is advertised over the execution
// network's gateway: the container reaches the host through its subnet
// gateway, never loopback (127.0.0.1 is not routable from a bridge container).
//
// Host constraint: like every newManager-based execution test, this FAILS
// CLOSED on Docker Desktop / remote Docker hosts (the control-network route
// proof cannot hold there — see newManager and the relayincontainer suite
// notes); it runs on Linux CI where the bridge lives in this host's network
// namespace. It stays integration-tagged and is validated by vet plus CI.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"relay/internal/observability/tracing"
	"relay/internal/testutil"
)

// nodeLoggerCorrelationHandler is the application under test: it imports the
// packaged structured logger and Tracer from its own vendored copy, opens one
// nested active span under the frame's extracted trace context, logs once
// inside it, and reports the span identity it observed on stdout.
const nodeLoggerCorrelationHandler = `
import { Logger } from "./vendor/relay-runtime/logging.js";
import { Tracer } from "./vendor/relay-runtime/tracing.js";

const log = new Logger({ service: "otel-correlation", level: "INFO" });
const tracer = new Tracer("otel-correlation");

export async function check(event, context) {
  await tracer.span("app.work", async (span) => {
    log.info("correlated-log", { step: event.step });
    const active = span.spanContext();
    console.log("nested-active=" + active.traceId + "/" + active.spanId);
  });
}
`

// nodeRuntimeSDKFiles are the built SDK modules the vendored copy ships; they
// are self-contained relative ESM imports of one another.
var nodeRuntimeSDKFiles = []string{
	"logging.js",
	"logging-context.js",
	"logging-format.js",
	"tracing.js",
}

// missingNodeRuntimeSDKFiles lists the packaged SDK modules absent from a dist
// directory, so an incomplete build is never vendored.
func missingNodeRuntimeSDKFiles(distDir string) []string {
	var missing []string
	for _, name := range nodeRuntimeSDKFiles {
		if _, err := os.Stat(filepath.Join(distDir, name)); err != nil {
			missing = append(missing, name)
		}
	}
	return missing
}

// requireNodeRuntimeDist returns the packaged SDK's dist directory:
// packages/node/dist when it already carries every vendored module, otherwise
// an isolated temporary npm+tsc build (never touching the working tree). The
// second result reports whether the caller owns the temporary build's removal.
func requireNodeRuntimeDist(t *testing.T) (distDir string, temporary bool) {
	t.Helper()
	// the package's tests run in internal/runtime — two levels below the root
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve the repository root: %v", err)
	}
	sourcePkg := filepath.Join(root, "packages", "node")
	builtDist := filepath.Join(sourcePkg, "dist")
	if len(missingNodeRuntimeSDKFiles(builtDist)) == 0 {
		return builtDist, false
	}

	npmPath, err := exec.LookPath("npm")
	if err != nil {
		t.Fatalf("environmental blocker: packages/node/dist is missing and npm cannot build it: %v", err)
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatalf("environmental blocker: node is required to build the packaged SDK: %v", err)
	}
	dir, err := os.MkdirTemp("", "relay-node-sdk-build-")
	if err != nil {
		t.Fatalf("temporary SDK build dir: %v", err)
	}
	for _, entry := range []string{"package.json", "src", "tsconfig.json"} {
		if err := copyFileOrDir(filepath.Join(sourcePkg, entry), filepath.Join(dir, entry)); err != nil {
			_ = os.RemoveAll(dir)
			t.Fatalf("stage %s for the isolated SDK build: %v", entry, err)
		}
	}
	install := exec.Command(npmPath, "install", "--no-audit", "--no-fund", "--loglevel=error")
	install.Dir = dir
	if out, err := install.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("environmental blocker: isolated npm install for the packaged SDK failed: %v\n%s", err, out)
	}
	build := exec.Command(nodePath,
		filepath.Join(dir, "node_modules", "typescript", "bin", "tsc"),
		"--project", filepath.Join(dir, "tsconfig.json"))
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("environmental blocker: isolated packaged-SDK compile failed: %v\n%s", err, out)
	}
	if missing := missingNodeRuntimeSDKFiles(filepath.Join(dir, "dist")); len(missing) > 0 {
		_ = os.RemoveAll(dir)
		t.Fatalf("environmental blocker: the isolated packaged-SDK build is missing %v", missing)
	}
	return filepath.Join(dir, "dist"), true
}

// copyFileOrDir recursively copies the file or directory src to dst.
func copyFileOrDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// writeVendoredNodeRuntime stages the packaged SDK into the app source's
// vendor directory — a plain relative import in the container, independent of
// any package-registry install.
func writeVendoredNodeRuntime(t *testing.T, dir, distDir string) {
	t.Helper()
	vendorDir := filepath.Join(dir, "vendor", "relay-runtime")
	if err := os.MkdirAll(vendorDir, 0o755); err != nil {
		t.Fatalf("mkdir the vendored SDK directory: %v", err)
	}
	for _, name := range nodeRuntimeSDKFiles {
		data, err := os.ReadFile(filepath.Join(distDir, name))
		if err != nil {
			t.Fatalf("read packaged SDK module %s: %v", name, err)
		}
		writeFile(t, dir, "vendor/relay-runtime/"+name, string(data))
	}
}

// managerOtlpSpan is one exported span over the wire.
type managerOtlpSpan struct {
	Name         string
	TraceID      string
	SpanID       string
	ParentSpanID string
}

// managerOtlpReceiver is a controlled local OTLP/HTTP collector bound on ALL
// interfaces, so a bridge-network container can reach it over its subnet
// gateway (the endpoint is completed with that gateway once the manager's
// execution network exists).
type managerOtlpReceiver struct {
	mu       sync.Mutex
	spans    []managerOtlpSpan
	listener net.Listener
	server   *http.Server
}

func newManagerOtlpReceiver(t *testing.T) *managerOtlpReceiver {
	t.Helper()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen for the OTLP collector: %v", err)
	}
	receiver := &managerOtlpReceiver{listener: listener}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := readLimitedBody(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var export collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		receiver.record(&export)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	receiver.server = &http.Server{Handler: mux}
	go func() { _ = receiver.server.Serve(listener) }()
	t.Cleanup(func() { _ = receiver.server.Close() })
	return receiver
}

func readLimitedBody(req *http.Request) ([]byte, error) {
	defer req.Body.Close()
	const maxBody = 8 << 20
	body, err := io.ReadAll(io.LimitReader(req.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBody {
		return nil, fmt.Errorf("export body exceeds %d bytes", maxBody)
	}
	return body, nil
}

func (r *managerOtlpReceiver) record(req *collectortrace.ExportTraceServiceRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ss := range req.GetResourceSpans() {
		for _, scope := range ss.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				r.spans = append(r.spans, managerOtlpSpan{
					Name:         span.GetName(),
					TraceID:      hex.EncodeToString(span.GetTraceId()),
					SpanID:       hex.EncodeToString(span.GetSpanId()),
					ParentSpanID: hex.EncodeToString(span.GetParentSpanId()),
				})
			}
		}
	}
}

// endpoint completes the collector's base URL with the host address the
// container can route to — the execution network's gateway.
func (r *managerOtlpReceiver) endpoint(gateway string) string {
	return "http://" + net.JoinHostPort(gateway, strconv.Itoa(r.listener.Addr().(*net.TCPAddr).Port))
}

func (r *managerOtlpReceiver) waitForSpans(t *testing.T, min int) []managerOtlpSpan {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		r.mu.Lock()
		spans := append([]managerOtlpSpan(nil), r.spans...)
		r.mu.Unlock()
		if len(spans) >= min {
			return spans
		}
		if time.Now().After(deadline) {
			t.Fatalf("collector got %d span(s), want >= %d; spans: %+v", len(spans), min, spans)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// executionNetworkGateway returns the gateway IP of a Docker network — the
// host-owned address on the container's own subnet.
func executionNetworkGateway(t *testing.T, ctx context.Context, cli *client.Client, networkName string) string {
	t.Helper()
	insp, err := cli.NetworkInspect(ctx, networkName, client.NetworkInspectOptions{})
	if err != nil {
		t.Fatalf("inspect execution network %q: %v", networkName, err)
	}
	for _, config := range insp.Network.IPAM.Config {
		if config.Gateway.IsValid() {
			return config.Gateway.String()
		}
	}
	t.Fatalf("execution network %q has no IPAM gateway; the collector endpoint cannot be formed", networkName)
	return ""
}

// managerSpanByName returns the first Go-exported span with the name and
// trace id.
func managerSpanByName(spans tracetest.SpanStubs, name, traceID string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name && spans[i].SpanContext.TraceID().String() == traceID {
			return &spans[i]
		}
	}
	return nil
}

// collectorSpanByID returns the first collected span matching name, trace,
// and span id.
func collectorSpanByID(spans []managerOtlpSpan, name, traceID, spanID string) *managerOtlpSpan {
	for i := range spans {
		if spans[i].Name == name && spans[i].TraceID == traceID && spans[i].SpanID == spanID {
			return &spans[i]
		}
	}
	return nil
}

// jsonLogRecordsWithMessage parses stderr JSON records carrying the message.
func jsonLogRecordsWithMessage(out, message string) []map[string]any {
	var matched []map[string]any
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "{") {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
			continue
		}
		if record["message"] == message {
			matched = append(matched, record)
		}
	}
	return matched
}

// TestIntegrationNode26LoggerTraceCorrelatesWithExportedSpanOverManagerExecute
// proves the full correlation chain through the REAL manager execution path:
// a Go-managed invocation span (exported in-process) → production frame
// encoding of its W3C context → the container's nested active span created
// through the packaged SDK Tracer (exported over real OTLP to a controlled
// collector) → the packaged Logger's JSON record on the container's stderr,
// whose trace_id/span_id must match the exported spans on both sides while
// carrying the invocation identity.
func TestIntegrationNode26LoggerTraceCorrelatesWithExportedSpanOverManagerExecute(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	appName := "node26-logger-otel-" + runtime.GOARCH
	t.Cleanup(cleanupImagePrefixes(cli, "relay-app-"+appName+":"))

	dir := t.TempDir()
	fn := newAppFromTemplate(t, appName, dir, `
runtime: nodejs26.x
events:
  - handler: handler.check
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.js", nodeLoggerCorrelationHandler)
	distDir, temporary := requireNodeRuntimeDist(t)
	if temporary {
		t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(distDir)) })
	}
	writeVendoredNodeRuntime(t, dir, distDir)

	// The real Go invocation trace, exported in-process; its context is what
	// the production frame encoder injects into the container.
	goSpans := withSpanRecorder(t)

	// The collector is reachable only once the execution network's gateway is
	// known — that happens with the manager.
	collector := newManagerOtlpReceiver(t)

	m, _ := newManager(t)
	if len(m.networks) == 0 {
		t.Fatal("the test manager carries no execution network for the collector gateway")
	}
	gateway := executionNetworkGateway(t, ctx, cli, m.networks[0])
	endpoint := collector.endpoint(gateway)

	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare nodejs26.x managed-otel app: %v", err)
	}
	if prepared.otelConfig == "" {
		t.Fatal("prepared Node invocation image has no managed provider identity")
	}
	if !hasEntry(prepared.execEnv, "OTEL_SERVICE_NAME="+appName) {
		t.Errorf("execEnv = %v, want the injected service default %q",
			prepared.execEnv, "OTEL_SERVICE_NAME="+appName)
	}

	runCtx, span := tracing.Start(context.Background(), "function.invoke")
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()
	if !span.SpanContext().IsValid() {
		t.Fatalf("the worker-created invocation span context is invalid: %+v", span.SpanContext())
	}

	// The endpoint and the batch delay arrive as per-invocation frame env —
	// the same production channel template env values and secrets take.
	execErr := m.Execute(runCtx, prepared, "handler.check",
		[]byte(`{"status":"COMPLETED","step":"a"}`),
		[]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT=" + endpoint,
			"OTEL_BSP_SCHEDULE_DELAY=50",
		})
	span.End()
	if execErr != nil {
		t.Fatalf("execute under the worker-created invocation trace: %v", execErr)
	}

	// Then the Go side exported the invocation span under the injected ids.
	goList := goSpans.spans(t)
	invocation := managerSpanByName(goList, "function.invoke", traceID)
	if invocation == nil || invocation.SpanContext.SpanID().String() != spanID {
		names := make([]string, 0, len(goList))
		for _, stub := range goList {
			names = append(names, stub.Name)
		}
		t.Fatalf("the worker-created function.invoke span %s/%s was not exported; span names: %v",
			traceID, spanID, names)
	}

	// And the container emitted exactly one structured record for the log.
	_, stderr := awaitAppStreamsFor(t, ctx, cli, fn.Name, "correlated-log")
	records := jsonLogRecordsWithMessage(stderr, "correlated-log")
	if len(records) != 1 {
		t.Fatalf("structured records with message %q = %d, want 1; stderr:\n%s",
			"correlated-log", len(records), stderr)
	}
	record := records[0]
	if record["level"] != "INFO" || record["service"] != "otel-correlation" ||
		record["logger"] != "relay_runtime.otel-correlation" {
		t.Errorf("record identity = level %v service %v logger %v, want INFO/otel-correlation/relay_runtime.otel-correlation; record: %+v",
			record["level"], record["service"], record["logger"], record)
	}
	if record["app"] != appName || record["handler"] != "handler.check" ||
		record["attempt"] != float64(1) || record["cold_start"] != true {
		t.Errorf("record invocation fields = app %v handler %v attempt %v cold_start %v, want the frame's identity on the cold start; record: %+v",
			record["app"], record["handler"], record["attempt"], record["cold_start"], record)
	}
	requestID, _ := record["request_id"].(string)
	if requestID == "" {
		t.Errorf("record request_id = %v, want the generated per-invocation id", record["request_id"])
	}
	recordTrace, _ := record["trace_id"].(string)
	recordSpan, _ := record["span_id"].(string)
	if recordTrace != traceID {
		t.Errorf("record trace_id = %q, want the worker-created invocation trace %q", recordTrace, traceID)
	}
	if recordSpan == "" || recordSpan == spanID {
		t.Errorf("record span_id = %q, want the NESTED handler span (not a worker parent %q)", recordSpan, spanID)
	}
	attributes, ok := record["attributes"].(map[string]any)
	if !ok || attributes["step"] != "a" {
		t.Errorf("record attributes = %v, want {step: a}", record["attributes"])
	}

	// The handler's own stdout observation agrees with the record.
	wantMarker := "nested-active=" + traceID + "/" + recordSpan
	stdout, _ := awaitAppStreamsFor(t, ctx, cli, fn.Name, wantMarker)
	if !strings.Contains(stdout, wantMarker) {
		t.Errorf("stdout must carry %q; stdout:\n%s", wantMarker, stdout)
	}

	// When the container's OTLP export arrives, the record's ids are exactly
	// the exported nested span's ids, and the nested span parents to the span
	// that was ACTIVE when the production invocation frame was encoded:
	// Manager.Execute (runtime.execute) → container cache (runtime.acquire,
	// already ended) → runtime.invoke → lease.invoke → encodeInvokeRequest
	// renders the trace carrier from that ctx, so the injected traceparent's
	// parent-id — and therefore the Node nested span's ParentSpanID — is
	// runtime.invoke's span id.
	collected := collector.waitForSpans(t, 1)
	nested := collectorSpanByID(collected, "app.work", traceID, recordSpan)
	if nested == nil {
		names := make([]string, 0, len(collected))
		for _, got := range collected {
			names = append(names, got.Name+"/"+got.TraceID+"/"+got.SpanID)
		}
		t.Fatalf("no exported app.work span %s/%s; exported: %v",
			traceID, recordSpan, names)
	}
	invoke := managerSpanByName(goList, "runtime.invoke", traceID)
	if invoke == nil {
		names := make([]string, 0, len(goList))
		for _, stub := range goList {
			names = append(names, stub.Name)
		}
		t.Fatalf("the frame-encoding runtime.invoke span was not exported for trace %s; span names: %v",
			traceID, names)
	}
	invokeSpanID := invoke.SpanContext.SpanID().String()
	if nested.ParentSpanID != invokeSpanID {
		t.Errorf("exported nested span parent = %q, want the frame-encoding runtime.invoke span %q (function.invoke %q)",
			nested.ParentSpanID, invokeSpanID, spanID)
	}
}

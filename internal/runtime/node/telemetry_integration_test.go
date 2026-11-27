//go:build integration

package node

// End-to-end coverage for Relay's managed Node OpenTelemetry telemetry (see
// internal/runtime/node/telemetry.mjs) through the REAL embedded bootstrap,
// the REAL official pinned packages, and a controlled local OTLP receiver
// speaking the actual wire protocols. The bootstrap is driven over the
// authenticated execution channel exactly as Relay drives a warm container.
//
// The official packages are materialized once per test run from the SAME
// private manifest and pins the invocation image stages (the embedded
// otel/package.json), into a shared temporary directory — pnpm first,
// matching the image's installer, npm as fallback — and linked into each
// test's fake image layout: the private /relay/otel project plus the /app
// API singleton symlink. When neither installer works the tests FAIL with an
// explicit environmental message rather than skipping.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	otelNodeOnce    sync.Once
	otelNodeDir     string
	otelNodeErr     error
	otelNodeV19Once sync.Once
	otelNodeV19Dir  string
	otelNodeV19Err  error
	otelAppDepsOnce sync.Once
	otelAppDepsDir  string
	otelAppDepsErr  error
)

// TestMain removes the shared materialized OpenTelemetry installs after the
// package's tests have run.
func TestMain(m *testing.M) {
	code := m.Run()
	for _, dir := range []string{otelNodeDir, otelNodeV19Dir, otelAppDepsDir} {
		if dir != "" {
			_ = os.RemoveAll(dir)
		}
	}
	os.Exit(code)
}

// otelNodeInstallSpecs returns the staged private manifest's dependencies as
// name@version install specs: the harness installs exactly the set the
// invocation image installs, so the official pinned packages can never drift
// from the plan.
func otelNodeInstallSpecs(t *testing.T) []string {
	t.Helper()
	var doc struct {
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(otelPackageJSON, &doc); err != nil {
		t.Fatalf("staged private manifest is not a JSON object: %v", err)
	}
	if len(doc.Dependencies) == 0 {
		t.Fatal("staged private manifest declares no dependencies")
	}
	specs := make([]string, 0, len(doc.Dependencies))
	for name, version := range doc.Dependencies {
		specs = append(specs, name+"@"+version)
	}
	slices.Sort(specs)
	return specs
}

// lockedOtelVersion resolves a package's committed version from the staged
// pnpm lock, so a second installed package (the application-owned provider's
// SDK) can never drift from the coupled release set.
func lockedOtelVersion(t *testing.T, name string) string {
	t.Helper()
	re := regexp.MustCompile(`'` + regexp.QuoteMeta(name) + `@(\d+\.\d+\.\d+)`)
	if m := re.FindStringSubmatch(string(otelLockfile)); m != nil {
		return m[1]
	}
	t.Fatalf("the committed OTel lock does not resolve %s; regenerate otel/pnpm-lock.yaml", name)
	return ""
}

// installOtelPackages materializes packages into dir with pnpm (the image's
// installer) or npm and then proves every probe import resolves from there.
// Every attempt's failure is preserved and reported together, so a broken
// fallback can never mask the primary installer's real error.
func installOtelPackages(dir string, specs []string, probeImports []string) error {
	if err := os.WriteFile(filepath.Join(dir, "package.json"),
		[]byte(`{"name":"relay-otel-test","version":"0.0.0","private":true}`), 0o644); err != nil {
		return err
	}
	type installer struct {
		name string
		bin  string
		args []string
	}
	var attempts []installer
	if pnpm, err := exec.LookPath("pnpm"); err == nil {
		attempts = append(attempts, installer{name: "pnpm", bin: pnpm, args: append([]string{
			"add", "-C", dir, "--save-exact", "--allow-build=protobufjs",
		}, specs...)})
	} else {
		attempts = append(attempts, installer{name: "pnpm lookup"})
	}
	if npm, err := exec.LookPath("npm"); err == nil {
		attempts = append(attempts, installer{name: "npm", bin: npm, args: append([]string{
			"install", "--prefix", dir, "--save-exact", "--no-audit", "--no-fund",
		}, specs...)})
	} else {
		attempts = append(attempts, installer{name: "npm lookup"})
	}

	var failures []string
	for _, inst := range attempts {
		if inst.bin == "" {
			failures = append(failures, inst.name+": not found on PATH")
			continue
		}
		out, err := exec.Command(inst.bin, inst.args...).CombinedOutput()
		if err != nil {
			failures = append(failures,
				inst.name+" install: "+err.Error()+": "+tailProcessOutput(out))
			continue
		}
		if probeErr := probeOtelInstall(dir, probeImports); probeErr != nil {
			failures = append(failures,
				inst.name+" install: installed set is not importable: "+probeErr.Error())
			continue
		}
		return nil
	}
	return errors.New("all install attempts failed:\n  " + strings.Join(failures, "\n  "))
}

// probeOtelInstall requires every probe import from dir in a fresh node
// process, proving the materialized set resolves exactly like the image's
// private project must.
func probeOtelInstall(dir string, imports []string) error {
	payload, err := json.Marshal(imports)
	if err != nil {
		return err
	}
	script := `for (const name of JSON.parse(process.env.RELAY_TEST_PROBE)) require(name);`
	cmd := exec.Command("node", "-e", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "RELAY_TEST_PROBE="+string(payload))
	if out, err := cmd.CombinedOutput(); err != nil {
		return errors.New(err.Error() + ": " + tailProcessOutput(out))
	}
	return nil
}

func tailProcessOutput(out []byte) string {
	const limit = 600
	s := strings.TrimSpace(string(out))
	if len(s) > limit {
		s = s[len(s)-limit:]
	}
	return s
}

// requireOtelNode resolves the shared real-OTel directory, installing the
// staged private manifest's exact package set once per test run. It FAILS
// (never skips) when no installer can materialize it.
func requireOtelNode(t *testing.T) string {
	t.Helper()
	requireNode(t)
	specs := otelNodeInstallSpecs(t)
	imports := runtimeOtelImports(t)
	otelNodeOnce.Do(func() {
		dir, err := os.MkdirTemp("", "relay-otel-node-")
		if err != nil {
			otelNodeErr = err
			return
		}
		if err := installOtelPackages(dir, specs, imports); err != nil {
			_ = os.RemoveAll(dir)
			otelNodeErr = err
			return
		}
		otelNodeDir = dir
	})
	if otelNodeErr != nil {
		t.Fatalf("environmental blocker: node cannot materialize the staged OpenTelemetry set and no installer succeeded: %v",
			otelNodeErr)
	}
	return otelNodeDir
}

// requireOtelAPI19 resolves a PHYSICALLY SEPARATE api@1.9.0 install — the
// version an application can still pin — for the cross-version global
// registry boundary tests. FAILS when it cannot be materialized.
func requireOtelAPI19(t *testing.T) string {
	t.Helper()
	requireNode(t)
	otelNodeV19Once.Do(func() {
		dir, err := os.MkdirTemp("", "relay-otel-api19-")
		if err != nil {
			otelNodeV19Err = err
			return
		}
		if err := installOtelPackages(dir,
			[]string{"@opentelemetry/api@1.9.0"}, []string{"@opentelemetry/api"}); err != nil {
			_ = os.RemoveAll(dir)
			otelNodeV19Err = err
			return
		}
		otelNodeV19Dir = dir
	})
	if otelNodeV19Err != nil {
		t.Fatalf("environmental blocker: node cannot materialize the separate api@1.9.0 package: %v",
			otelNodeV19Err)
	}
	return otelNodeV19Dir
}

// appOwnedSDKNodeVersion is the application's OWN choice of SDK release for
// the app-owned provider scenario — the coupled 0.301.2 line of the same
// release family the private project's sdk-trace 3.1.1 belongs to. Relay's
// staged manifest no longer carries sdk-node (Relay is trace-only), so the
// application's tree installs it explicitly; its api peer still resolves to
// the exact pinned api@1.10.0 declared beside it.
const appOwnedSDKNodeVersion = "0.301.2"

// requireOtelAppDeps resolves the application's own physical OTel tree (the
// lock-committed trace SDK plus the application's NodeSDK) — a directory
// distinct from the private project, so an application-owned provider
// registers from a physically separate API copy exactly like a vendored
// application dependency would.
func requireOtelAppDeps(t *testing.T) string {
	t.Helper()
	requireNode(t)
	specs := []string{
		"@opentelemetry/api@" + otelAPIVersion,
		"@opentelemetry/sdk-node@" + appOwnedSDKNodeVersion,
		"@opentelemetry/sdk-trace@" + lockedOtelVersion(t, "@opentelemetry/sdk-trace"),
		"@opentelemetry/context-async-hooks@" + lockedOtelVersion(t, "@opentelemetry/context-async-hooks"),
	}
	otelAppDepsOnce.Do(func() {
		dir, err := os.MkdirTemp("", "relay-otel-appdeps-")
		if err != nil {
			otelAppDepsErr = err
			return
		}
		if err := installOtelPackages(dir, specs, []string{
			"@opentelemetry/api",
			"@opentelemetry/sdk-node",
			"@opentelemetry/sdk-trace",
			"@opentelemetry/context-async-hooks",
		}); err != nil {
			_ = os.RemoveAll(dir)
			otelAppDepsErr = err
			return
		}
		otelAppDepsDir = dir
	})
	if otelAppDepsErr != nil {
		t.Fatalf("environmental blocker: node cannot materialize the application's OTel tree: %v",
			otelAppDepsErr)
	}
	return otelAppDepsDir
}

// materializeRealOtelModules writes the embedded runtime modules with the
// private OTel project re-anchored under the test root and links the shared
// official install there, mirroring the image's /relay/otel directory. mounted
// selects the HOT_RELOAD module layout exactly like the baked path does. It
// fails loudly when the OTEL_ROOT layout rewrite did not land, so a renamed
// production anchor can never silently fall back to a host /relay/otel.
// Returns the shared install directory.
func materializeRealOtelModules(t *testing.T, root string, mounted bool) string {
	t.Helper()
	shared := requireOtelNode(t)
	writeRuntimeModules(t, root, mounted)
	raw, err := os.ReadFile(filepath.Join(root, "relay", "telemetry.mjs"))
	if err != nil {
		t.Fatalf("read materialized telemetry module: %v", err)
	}
	want := `const OTEL_ROOT = ` + quote(filepath.Join(root, "relay", "otel"))
	if !strings.Contains(string(raw), want) {
		t.Fatalf("the layout rewrite did not re-anchor the private OTel project: telemetry.mjs carries no %q — update rewriteModuleLayout to the production anchor", want)
	}
	private := filepath.Join(root, "relay", "otel")
	if err := os.MkdirAll(private, 0o755); err != nil {
		t.Fatalf("mkdir private OTel project: %v", err)
	}
	if err := os.Symlink(filepath.Join(shared, "node_modules"),
		filepath.Join(private, "node_modules")); err != nil {
		t.Fatalf("link the shared OTel install into the private project: %v", err)
	}
	return shared
}

// linkManagedAPI creates the image's API singleton link at
// /app/node_modules/@opentelemetry/api pointing into the given install tree —
// the production shape where the bootstrap's createRequire and user modules
// resolve one API package.
func linkManagedAPI(t *testing.T, root, installDir string) {
	t.Helper()
	scope := filepath.Join(root, "app", "node_modules", "@opentelemetry")
	if err := os.MkdirAll(scope, 0o755); err != nil {
		t.Fatalf("mkdir app OTel scope: %v", err)
	}
	target := filepath.Join(installDir, "node_modules", "@opentelemetry", "api")
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("the install tree has no API package at %s: %v", target, err)
	}
	if err := os.Symlink(target, filepath.Join(scope, "api")); err != nil {
		t.Fatalf("link the managed API singleton: %v", err)
	}
}

// startNodeRealOtel starts the real embedded bootstrap against the REAL
// official package set in the production layout (private project + API
// singleton symlink).
func startNodeRealOtel(t *testing.T, root string, extraEnv []string) *nodeProc {
	t.Helper()
	shared := materializeRealOtelModules(t, root, false)
	linkManagedAPI(t, root, shared)
	return startNodeScriptEnv(t, root, string(Bootstrap), extraEnv)
}

// pinOtelEnv clears every ambient OTEL/RELAY ownership variable so the host's
// own telemetry configuration can never change a child decision; each
// scenario supplies its configuration through the worker environment or the
// first invocation's frame, exactly as production does.
func pinOtelEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_HEADERS",
		"OTEL_SDK_DISABLED",
		"OTEL_SERVICE_NAME",
		"OTEL_RESOURCE_ATTRIBUTES",
		"OTEL_BSP_SCHEDULE_DELAY",
		"OTEL_TRACES_SAMPLER",
		"OTEL_LOGS_EXPORTER",
		"RELAY_OTEL_PROVIDER",
	} {
		t.Setenv(name, "")
	}
}

// otlpSpan is one exported span plus its resource.
type otlpSpan struct {
	Name         string
	TraceID      string
	SpanID       string
	ParentSpanID string
	ServiceName  string
	Attributes   map[string]string
	Resource     map[string]string
}

// otlpReceiver is a controlled local OTLP collector in the test process. It
// speaks the real wire formats: an HTTP/protobuf endpoint and a gRPC
// endpoint, both decoding ExportTraceServiceRequest with the official OTLP
// protos. Requests on /v1/logs are counted: any logs traffic proves a logs
// export pipeline exists.
type otlpReceiver struct {
	mu        sync.Mutex
	spans     []otlpSpan
	logsPosts int
	auths     []string

	httpServer *httptest.Server
	endpoint   string
	grpcServer *grpc.Server
}

func (r *otlpReceiver) record(req *collectortrace.ExportTraceServiceRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		resource := make(map[string]string, len(rs.GetResource().GetAttributes()))
		for _, attr := range rs.GetResource().GetAttributes() {
			resource[attr.GetKey()] = attr.GetValue().GetStringValue()
		}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				attrs := make(map[string]string, len(sp.GetAttributes()))
				for _, a := range sp.GetAttributes() {
					attrs[a.GetKey()] = a.GetValue().GetStringValue()
				}
				r.spans = append(r.spans, otlpSpan{
					Name:         sp.GetName(),
					TraceID:      hex.EncodeToString(sp.GetTraceId()),
					SpanID:       hex.EncodeToString(sp.GetSpanId()),
					ParentSpanID: hex.EncodeToString(sp.GetParentSpanId()),
					ServiceName:  resource["service.name"],
					Attributes:   attrs,
					Resource:     resource,
				})
			}
		}
	}
}

// snapshot returns a copy of the spans exported so far.
func (r *otlpReceiver) snapshot() []otlpSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]otlpSpan(nil), r.spans...)
}

// authHeaders returns the Authorization headers observed on HTTP export
// requests so far.
func (r *otlpReceiver) authHeaders() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auths...)
}

func (r *otlpReceiver) spanCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.spans)
}

func (r *otlpReceiver) logPostCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.logsPosts
}

// waitForSpans polls until at least min spans have been exported, then
// returns them; it fails the test with the observed spans when the bound
// expires.
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

// requireNoSpans fails when any span arrives within the quiet window.
func (r *otlpReceiver) requireNoSpans(t *testing.T, window time.Duration) {
	t.Helper()
	time.Sleep(window)
	if n := r.spanCount(); n != 0 {
		t.Fatalf("receiver got %d span(s), want none; spans: %+v", n, r.snapshot())
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
		r.mu.Lock()
		r.auths = append(r.auths, req.Header.Get("Authorization"))
		r.mu.Unlock()
		r.record(&export)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		r.mu.Lock()
		r.logsPosts++
		r.mu.Unlock()
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
	r.endpoint = "http://" + lis.Addr().String()
	return r
}

// traceCarrier returns a W3C traceparent (and optional baggage) for the
// given 32-hex trace id and 16-hex parent span id, sampled.
func traceCarrier(traceID, parentID string) map[string]string {
	return map[string]string{
		"traceparent": "00-" + traceID + "-" + parentID + "-01",
	}
}

const (
	traceRealOne    = "11111111111111111111111111111111"
	parentRealOne   = "aaaaaaaaaaaaaaaa"
	traceRealTwo    = "22222222222222222222222222222222"
	parentRealTwo   = "bbbbbbbbbbbbbbbb"
	traceRealThree  = "33333333333333333333333333333333"
	parentRealThree = "cccccccccccccccc"
)

// realOtelHandlerSource is the shared handler module for the real-package
// scenarios. Every export reads only API-level state the bootstrap's telemetry
// module established before the module was imported (ensureStarted runs
// before the first loadHandler).
const realOtelHandlerSource = `
import { createRequire } from "node:module";
import { trace, propagation, logs, context as otelContext } from "@opentelemetry/api";

const cjsApi = createRequire(import.meta.url)("@opentelemetry/api");
const managedTracer = trace.getTracer("relay.test");

function spanIdOf(span) {
  if (!span) return "none";
  const sc = span.spanContext();
  return sc.traceId + "/" + sc.spanId;
}

export function inspect(event, context) {
  const span = trace.getSpan(otelContext.active());
  console.log("traceId=" + (span ? span.spanContext().traceId : "none"));
  console.log("spanId=" + (span ? span.spanContext().spanId : "none"));
  const baggage = propagation.getBaggage(otelContext.active());
  const entry = baggage ? baggage.getEntry("tenant") : null;
  console.log("baggage=" + (entry ? entry.value : "none"));
}

export async function inspectAsync(event, context) {
  await new Promise((resolve) => setTimeout(resolve, 20));
  const span = trace.getSpan(otelContext.active());
  console.log("traceId=" + (span ? span.spanContext().traceId : "none"));
  console.log("spanId=" + (span ? span.spanContext().spanId : "none"));
  const baggage = propagation.getBaggage(otelContext.active());
  const entry = baggage ? baggage.getEntry("tenant") : null;
  console.log("baggage=" + (entry ? entry.value : "none"));
  const probe = trace.getTracer("relay.probe").startSpan("probe");
  console.log("recording=" + probe.isRecording());
  probe.end();
}

export function copies(event, context) {
  console.log("esm=" + spanIdOf(trace.getSpan(otelContext.active())));
  console.log("cjs=" + spanIdOf(cjsApi.trace.getSpan(cjsApi.context.active())));
}

export function loaded(event, context) {
  const keys = Object.keys(createRequire(import.meta.url).cache);
  const has = (frag) => keys.some((key) => key.includes(frag));
  console.log("sdkTrace=" + has("sdk-trace"));
  console.log("otlpProto=" + has("exporter-trace-otlp-proto"));
  console.log("otlpGrpc=" + has("exporter-trace-otlp-grpc"));
  const span = trace.getTracer("relay.probe").startSpan("probe");
  console.log("recording=" + span.isRecording());
  span.end();
}

export function work(event, context) {
  const span = managedTracer.startSpan("handler.work", {
    attributes: { "work.step": event.step },
  });
  span.end();
  console.log("work-step=" + event.step);
}

export function logsemit(event, context) {
  const present = Object.prototype.hasOwnProperty.call(process.env, "OTEL_LOGS_EXPORTER");
  console.log("logsEnv=" + (present && process.env.OTEL_LOGS_EXPORTER !== ""
    ? process.env.OTEL_LOGS_EXPORTER
    : "<unset>"));
  logs.getLogger("relay.probe").emit({ body: "probe-record" });
  console.log("log-emitted");
  // The application registers its OWN logs provider under managed tracing:
  // the official registration succeeds only when the global logs registry is
  // FREE, which is exactly the proof that Relay's trace-only setup never
  // registered a LoggerProvider. The registered provider stays functional
  // (getLogger/emit below), so the application can actually use it. This is
  // the LAST logs action in the handler.
  const appProvider = {
    getLogger() {
      return { emit() {} };
    },
  };
  console.log("app-logs-registered=" + logs.setGlobalLoggerProvider(appProvider));
  const owned = logs.getLogger("app.owned");
  owned.emit({ body: "app-owned-record" });
  console.log("app-logs-active=true");
}

export async function slow(event, context) {
  await new Promise((resolve) => setTimeout(resolve, 1200));
  const span = managedTracer.startSpan("slow.work");
  span.end();
  console.log("slow-done traceId=" + span.spanContext().traceId);
}
`

// appOwnedProviderSource drives the application's OWN official NodeSDK from a
// PHYSICALLY SEPARATE app dependency tree (api@1.10.0 plus the locked
// sdk-node release), exactly the path an application that declares its own
// OpenTelemetry SDK takes. NodeSDK.start() runs during this first lazy module
// evaluation and attempts its OWN context-manager and provider registration —
// both must collide with Relay's already-active state instead of replacing
// it: Relay's async context manager must keep carrying the extracted W3C
// context across the module import and later awaits, the application provider
// must register and record a CHILD of that context, and Relay must never
// shut the application-owned provider down (the warm second invocation still
// records into the application's exporter).
const appOwnedProviderSource = `
import { trace, context as otelContext } from "@opentelemetry/api";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter } from "@opentelemetry/sdk-trace";

function activeTag() {
  const span = trace.getSpan(otelContext.active());
  if (!span) return "root";
  const sc = span.spanContext();
  return sc.traceId + "/" + sc.spanId;
}

const exporter = new InMemorySpanExporter();
const sdk = new NodeSDK({ traceExporter: exporter });
sdk.start();

const IMPORT_TAG = activeTag();

async function relayManagerAlive() {
  const key = Symbol.for("relay.appowned.ctxprobe");
  const marked = otelContext.active().setValue(key, "alive");
  return new Promise((resolve) => {
    otelContext.with(marked, async () => {
      await new Promise((r) => setTimeout(r, 10));
      resolve(otelContext.active().getValue(key) === "alive");
    });
  });
}

async function settleSpans(want) {
  // NodeSDK's default BatchSpanProcessor exports on the OTEL_BSP_SCHEDULE_DELAY
  // tick (50ms in this scenario); poll until the application's own exporter
  // has at least the expected spans, bounded at ~2s.
  for (let i = 0; i < 100; i++) {
    if (exporter.getFinishedSpans().length >= want) return;
    await new Promise((r) => setTimeout(r, 20));
  }
}

export async function ownwork(event, context) {
  const managerOk = await relayManagerAlive();
  const awaitTag = activeTag();
  const span = trace.getTracer("app-owned").startSpan("app.work");
  const live = span.isRecording();
  span.end();
  await settleSpans(event.want_spans);
  const spans = exporter.getFinishedSpans();
  const last = spans[spans.length - 1];
  const parent = last && last.parentSpanContext
    ? last.parentSpanContext.spanId
    : "none";
  console.log("app-manager-alive=" + managerOk);
  console.log("app-active-import=" + IMPORT_TAG);
  console.log("app-active-await=" + awaitTag);
  console.log("app-live=" + live);
  console.log("app-spans=" + spans.length);
  console.log("app-trace=" + span.spanContext().traceId);
  console.log("app-parent=" + parent);
}
`

// api19ManagedHandlerSource serves the separate api@1.9.0 application-copy
// scenario: that build is CommonJS-only, so the handler takes the default
// import form (ESM named-export detection cannot see every name on it) while
// the traced span still observes whether a provider delegate is active.
const api19ManagedHandlerSource = `
import api from "@opentelemetry/api";

const tracer = api.trace.getTracer("relay.test");

export function work(event, context) {
  const span = tracer.startSpan("handler.work");
  console.log("recording=" + span.isRecording());
  span.end();
  console.log("work-step=" + event.step);
}
`

// writeRealOtelHandler writes the shared handler module under test.
func writeRealOtelHandler(t *testing.T, root string) {
	t.Helper()
	writeModule(t, root, "index.mjs", realOtelHandlerSource)
}

// findSpan returns the first exported span matching name and trace id.
func findSpan(spans []otlpSpan, name, traceID string) *otlpSpan {
	for i := range spans {
		if spans[i].Name == name && spans[i].TraceID == traceID {
			return &spans[i]
		}
	}
	return nil
}

// requireNoTelemetryFailure asserts the child emitted none of the telemetry
// failure diagnostics. A successful managed start is silent — the export and
// recording observations are the positive proof — while these strings are the
// stable failure signals of registration, provider setup, and context
// installation.
func requireNoTelemetryFailure(t *testing.T, p *nodeProc) {
	t.Helper()
	stderr := p.stderrString()
	for _, failure := range []string{
		"registration failed",
		"provider setup failed",
		"context setup failed",
	} {
		if strings.Contains(stderr, failure) {
			t.Errorf("stderr = %q, want no telemetry failure %q", stderr, failure)
		}
	}
}

// TestNodeRuntimeRealOtelContextPropagation is the core propagation scenario
// over the REAL official W3C propagators: the invocation frame's traceparent
// and baggage must reach the handler as the ACTIVE context, one invocation's
// carrier must not leak into the next, and a carrier-less invocation must see
// the root context — with no provider started (there is no endpoint), so no
// connection is ever attempted.
func TestNodeRuntimeRealOtelContextPropagation(t *testing.T) {
	pinOtelEnv(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	// Given a W3C traceparent and baggage on the invocation frame, when the
	// handler reads the active context through the official API, then the
	// extracted ids and baggage are observable.
	carrier := traceCarrier(traceRealOne, parentRealOne)
	carrier["baggage"] = "tenant=acme"
	r1 := p.invokeTrace(t, "index.inspect", `{}`, nil, carrier)
	if !r1.OK {
		t.Fatalf("traced invoke failed: %s", r1.Err)
	}
	for _, want := range []string{
		"traceId=" + traceRealOne,
		"spanId=" + parentRealOne,
		"baggage=acme",
	} {
		if !strings.Contains(r1.Out, want) {
			t.Errorf("out = %q, want it to contain %q (the official W3C propagators must extract the frame carrier)", r1.Out, want)
		}
	}
	// Extraction itself must be silent: a propagator failure warning means the
	// carrier was dropped and the handler fell back to the root context.
	if strings.Contains(p.stderrString(), "context extraction failed") {
		t.Errorf("context extraction failed against the official propagators; stderr:\n%s", p.stderrString())
	}

	// Given a different carrier on the next invocation, the handler sees only
	// that carrier's identity.
	r2 := p.invokeTrace(t, "index.inspect", `{}`, nil, traceCarrier(traceRealTwo, parentRealTwo))
	if !r2.OK {
		t.Fatalf("second traced invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "traceId="+traceRealTwo) || strings.Contains(r2.Out, traceRealOne) {
		t.Errorf("second out = %q, want only the second carrier's trace id", r2.Out)
	}

	// Given no carrier, the handler observes the root context.
	r3 := p.invoke(t, "index.inspect", `{}`, nil)
	if !r3.OK {
		t.Fatalf("carrier-less invoke failed: %s", r3.Err)
	}
	if !strings.Contains(r3.Out, "traceId=none") || !strings.Contains(r3.Out, "baggage=none") {
		t.Errorf("carrier-less out = %q, want traceId=none and baggage=none", r3.Out)
	}

	// No endpoint was ever configured: the managed provider must never start.
	if strings.Contains(p.stderrString(), "telemetry: warning:") {
		t.Errorf("a clean carrier-only propagation run must emit no telemetry warnings; stderr:\n%s", p.stderrString())
	}
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s", err, p.stderrString())
	}
}

// TestNodeRuntimeRealOtelApiCopiesShareContext pins the dual-format API
// contract: @opentelemetry/api ships a physically single install with an ESM
// build (import) and a CJS build (require), so the handler's two import forms
// are DISTINCT module instances. What must be shared is the GLOBAL registry
// (Symbol.for-based) both copies read: the context the bootstrap installed
// through its CJS copy is equally observable through the handler's ESM copy
// and its CJS require. Functional equality of the observed context is the
// contract; object identity across module formats is not.
func TestNodeRuntimeRealOtelApiCopiesShareContext(t *testing.T) {
	pinOtelEnv(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	carrier := traceCarrier(traceRealOne, parentRealOne)
	r := p.invokeTrace(t, "index.copies", `{}`, nil, carrier)
	if !r.OK {
		t.Fatalf("copies invoke failed: %s", r.Err)
	}
	want := "esm=" + traceRealOne + "/" + parentRealOne
	if !strings.Contains(r.Out, want) {
		t.Errorf("ESM copy out = %q, want it to contain %q", r.Out, want)
	}
	want = "cjs=" + traceRealOne + "/" + parentRealOne
	if !strings.Contains(r.Out, want) {
		t.Errorf("CJS copy out = %q, want it to contain %q", r.Out, want)
	}
}

// TestNodeRuntimeRealOtelLazyProviderImports pins the lazy-import boundary
// with the real packages: without an endpoint the bootstrap loads only the
// API, the W3C propagators, and the context manager — the trace provider
// (sdk-trace) and every OTLP exporter stay out of the require cache and no
// provider registers — while a configured endpoint pulls in the trace provider
// and exactly the selected exporter on the first invocation.
func TestNodeRuntimeRealOtelLazyProviderImports(t *testing.T) {
	t.Run("no endpoint never loads the trace provider or an exporter", func(t *testing.T) {
		pinOtelEnv(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		p := startNodeRealOtel(t, root, nil)

		r := p.invoke(t, "index.loaded", `{}`, nil)
		if !r.OK {
			t.Fatalf("invoke failed: %s", r.Err)
		}
		for _, want := range []string{"sdkTrace=false", "otlpProto=false", "otlpGrpc=false", "recording=false"} {
			if !strings.Contains(r.Out, want) {
				t.Errorf("out = %q, want it to contain %q", r.Out, want)
			}
		}
		if strings.Contains(p.stderrString(), "telemetry: warning:") {
			t.Errorf("a clean no-endpoint run must emit no telemetry warnings; stderr:\n%s", p.stderrString())
		}
	})

	t.Run("endpoint loads the trace provider on the first invocation", func(t *testing.T) {
		pinOtelEnv(t)
		rec := newHTTPOTLPReceiver(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		p := startNodeRealOtel(t, root, nil)

		r := p.invokeTrace(t, "index.loaded", `{}`,
			map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
				"OTEL_BSP_SCHEDULE_DELAY":     "50",
			}, traceCarrier(traceRealOne, parentRealOne))
		if !r.OK {
			t.Fatalf("invoke failed: %s", r.Err)
		}
		for _, want := range []string{"sdkTrace=true", "otlpProto=true", "recording=true"} {
			if !strings.Contains(r.Out, want) {
				t.Errorf("out = %q, want it to contain %q", r.Out, want)
			}
		}
		// The provider constructs exactly ONE exporter: under the default
		// http/protobuf protocol the gRPC exporter package must stay unloaded
		// (the protocol branch is visible at package level).
		if strings.Contains(r.Out, "otlpGrpc=true") {
			t.Errorf("out = %q, want the gRPC exporter unloaded under http/protobuf", r.Out)
		}
		rec.waitForSpans(t, 1)
		if err := p.closeChannel(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
		if err := p.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
		}
	})
}

// TestNodeRuntimeRealOtelProviderOwnership is the provider-ownership matrix
// against the real SDK: only a managed process with a clean, credential-free
// endpoint registers a provider and exports; every opt-out and every refused
// configuration leaves the API unregistered (spans are non-recording), exports
// nothing, and warns — and NONE of these setup decisions may terminate the
// handler process.
func TestNodeRuntimeRealOtelProviderOwnership(t *testing.T) {
	t.Run("managed provider registers and exports on the carrier trace", func(t *testing.T) {
		pinOtelEnv(t)
		rec := newHTTPOTLPReceiver(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		p := startNodeRealOtel(t, root, nil)

		r1 := p.invokeTrace(t, "index.work", `{"step":"1"}`,
			map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
				"OTEL_BSP_SCHEDULE_DELAY":     "50",
				"OTEL_SERVICE_NAME":           "node-svc",
				"OTEL_RESOURCE_ATTRIBUTES":    "deployment.environment=staging",
			}, traceCarrier(traceRealOne, parentRealOne))
		if !r1.OK {
			t.Fatalf("first invoke failed: %s", r1.Err)
		}
		// A successful managed start is silent; the exported carrier spans
		// below are the positive proof that the provider registered.
		requireNoTelemetryFailure(t, p)

		spans := rec.waitForSpans(t, 1)
		child := findSpan(spans, "handler.work", traceRealOne)
		if child == nil {
			t.Fatalf("no handler.work span on the carrier trace; spans: %+v", spans)
		}
		if child.ParentSpanID != parentRealOne {
			t.Errorf("span parent = %s, want the carrier's parent %s", child.ParentSpanID, parentRealOne)
		}
		if child.SpanID == "" || child.SpanID == parentRealOne {
			t.Errorf("span id = %q, want its own id distinct from the parent", child.SpanID)
		}
		if child.ServiceName != "node-svc" {
			t.Errorf("resource service.name = %q, want node-svc", child.ServiceName)
		}
		if got := child.Attributes["work.step"]; got != "1" {
			t.Errorf("span attributes = %v, want work.step=1", child.Attributes)
		}
		if got := child.Resource["deployment.environment"]; got != "staging" {
			t.Errorf("resource deployment.environment = %q, want the frame's staging", got)
		}

		// Warm reuse: a second invocation with NO endpoint in its frame still
		// exports — the provider was initialized exactly once.
		r2 := p.invokeTrace(t, "index.work", `{"step":"2"}`, nil,
			traceCarrier(traceRealTwo, parentRealTwo))
		if !r2.OK {
			t.Fatalf("warm reuse invoke failed: %s", r2.Err)
		}
		spans = rec.waitForSpans(t, 2)
		if findSpan(spans, "handler.work", traceRealTwo) == nil {
			t.Errorf("warm span did not join the second carrier trace; spans: %+v", spans)
		}
		if err := p.closeChannel(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
		if err := p.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
		}
	})

	t.Run("application ownership leaves the api unregistered", func(t *testing.T) {
		pinOtelEnv(t)
		rec := newHTTPOTLPReceiver(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		p := startNodeRealOtel(t, root, nil)

		r := p.invokeTrace(t, "index.loaded", `{}`,
			map[string]string{
				"RELAY_OTEL_PROVIDER":         "application",
				"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			}, nil)
		if !r.OK {
			t.Fatalf("invoke failed: %s", r.Err)
		}
		if !strings.Contains(r.Out, "recording=false") {
			t.Errorf("out = %q, want recording=false (Relay must not register under application ownership)", r.Out)
		}
		requireNoTelemetryFailure(t, p)
		rec.requireNoSpans(t, 750*time.Millisecond)
		if err := p.closeChannel(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
		if err := p.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
		}
	})

	t.Run("application-owned provider registers from a physically separate api copy", func(t *testing.T) {
		pinOtelEnv(t)
		rec := newHTTPOTLPReceiver(t)
		appDeps := requireOtelAppDeps(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		writeModule(t, root, "src/own.mjs", appOwnedProviderSource)
		// The application's OWN tree under a nested node_modules — physically
		// distinct from the managed singleton the bootstrap resolves.
		if err := os.Symlink(filepath.Join(appDeps, "node_modules"),
			filepath.Join(root, "app", "src", "node_modules")); err != nil {
			t.Fatalf("link the application's OTel tree: %v", err)
		}
		shared := materializeRealOtelModules(t, root, false)
		linkManagedAPI(t, root, shared)

		// Fixture preconditions: the bootstrap's API is the managed singleton,
		// the application's copy is a physically separate install, both at the
		// engine's pinned version.
		bootstrapAPI, err := filepath.EvalSymlinks(filepath.Join(root, "app", "node_modules", "@opentelemetry", "api"))
		if err != nil {
			t.Fatalf("resolve the bootstrap's API package: %v", err)
		}
		sharedAPI, err := filepath.EvalSymlinks(filepath.Join(shared, "node_modules", "@opentelemetry", "api"))
		if err != nil {
			t.Fatalf("resolve the shared API package: %v", err)
		}
		if bootstrapAPI != sharedAPI {
			t.Fatalf("fixture precondition: the bootstrap's API must be the managed singleton (%s != %s)", bootstrapAPI, sharedAPI)
		}
		appAPI, err := filepath.EvalSymlinks(filepath.Join(root, "app", "src", "node_modules", "@opentelemetry", "api"))
		if err != nil {
			t.Fatalf("resolve the application's API package: %v", err)
		}
		if appAPI == bootstrapAPI {
			t.Fatalf("fixture precondition: the application's API copy must be physically separate, both resolve to %s", appAPI)
		}
		if got := packageVersion(t, appAPI); got != otelAPIVersion {
			t.Fatalf("the application's API copy is %s, want the pinned %s", got, otelAPIVersion)
		}
		p := startNodeScriptEnv(t, root, string(Bootstrap), nil)

		// Given application ownership with an endpoint Relay must not claim,
		// when the handler's first lazy module evaluation runs the
		// application's own official NodeSDK (which attempts its OWN
		// context-manager and provider registration) under a real carrier,
		// then Relay's already-active context manager keeps the extracted W3C
		// context across the module import and later awaits, the
		// application's provider registers and records a CHILD of that
		// context, and Relay never touches it: a warm second invocation on a
		// different carrier still records — while Relay itself exports
		// nothing.
		env := map[string]string{
			"RELAY_OTEL_PROVIDER":         "application",
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			// The application's NodeSDK builds its default BatchSpanProcessor
			// from this at construction (first lazy import); the handler polls
			// its own exporter against the resulting fast tick.
			"OTEL_BSP_SCHEDULE_DELAY": "50",
		}
		r1 := p.invokeTrace(t, "src.own.ownwork", `{"want_spans":1}`, env,
			traceCarrier(traceRealOne, parentRealOne))
		if !r1.OK {
			t.Fatalf("app-owned invoke failed: %s", r1.Err)
		}
		for _, want := range []string{
			"app-manager-alive=true",
			"app-active-import=" + traceRealOne + "/" + parentRealOne,
			"app-active-await=" + traceRealOne + "/" + parentRealOne,
			"app-live=true",
			"app-spans=1",
			"app-trace=" + traceRealOne,
			"app-parent=" + parentRealOne,
		} {
			if !strings.Contains(r1.Out, want) {
				t.Errorf("out = %q, want it to contain %q", r1.Out, want)
			}
		}

		r2 := p.invokeTrace(t, "src.own.ownwork", `{"want_spans":2}`, env,
			traceCarrier(traceRealTwo, parentRealTwo))
		if !r2.OK {
			t.Fatalf("warm app-owned invoke failed: %s", r2.Err)
		}
		for _, want := range []string{
			"app-spans=2",
			"app-trace=" + traceRealTwo,
			"app-parent=" + parentRealTwo,
		} {
			if !strings.Contains(r2.Out, want) {
				t.Errorf("warm out = %q, want it to contain %q (the application-owned provider must keep recording across Relay's warm reuse)", r2.Out, want)
			}
		}
		requireNoTelemetryFailure(t, p)
		rec.requireNoSpans(t, 750*time.Millisecond)
		if err := p.closeChannel(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
		if err := p.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
		}
	})

	// OTEL_SDK_DISABLED is covered by its own focused subtest below: disabled
	// export must not disable propagation, so it asserts more than the
	// opt-out table's provider probe could.
	optOutCases := []struct {
		name  string
		env   map[string]string
		warns string
	}{
		{
			name: "credential-bearing endpoint is refused",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://user:pass@127.0.0.1:9",
			},
			warns: "must not contain userinfo",
		},
		{
			name: "unsupported ownership value is refused",
			env: map[string]string{
				"RELAY_OTEL_PROVIDER":         "maybe",
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:9",
			},
			warns: "unsupported RELAY_OTEL_PROVIDER",
		},
		{
			name: "unsupported protocol is refused",
			env: map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:9",
				"OTEL_EXPORTER_OTLP_PROTOCOL": "http/json",
			},
			warns: "unsupported OTLP protocol",
		},
	}
	for _, tc := range optOutCases {
		t.Run(tc.name, func(t *testing.T) {
			pinOtelEnv(t)
			root := appRootWithoutOtel(t)
			writeRealOtelHandler(t, root)
			p := startNodeRealOtel(t, root, nil)

			r := p.invoke(t, "index.loaded", `{}`, tc.env)
			if !r.OK {
				t.Fatalf("invoke failed: %s (a refused telemetry setup must not fail the invocation)", r.Err)
			}
			if !strings.Contains(r.Out, "recording=false") {
				t.Errorf("out = %q, want recording=false (no provider registered)", r.Out)
			}
			if tc.warns != "" && !strings.Contains(p.stderrString(), tc.warns) {
				t.Errorf("stderr = %q, want the warning %q", p.stderrString(), tc.warns)
			}
			requireNoTelemetryFailure(t, p)
			if err := p.closeChannel(); err != nil {
				t.Fatalf("close channel: %v", err)
			}
			if err := p.waitExit(t, 10*time.Second); err != nil {
				t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
			}
		})
	}

	t.Run("sdk disabled keeps w3c propagation without a provider or export", func(t *testing.T) {
		pinOtelEnv(t)
		rec := newHTTPOTLPReceiver(t)
		root := appRootWithoutOtel(t)
		writeRealOtelHandler(t, root)
		p := startNodeRealOtel(t, root, nil)

		// Given OTEL_SDK_DISABLED=true WITH a configured endpoint and fast
		// batch delay (disabled must beat export configuration), when an
		// async handler reads its context after an await, then the extracted
		// traceparent AND baggage are still visible — disabled export must
		// not disable propagation — while no provider registers and nothing
		// exports.
		carrier := traceCarrier(traceRealOne, parentRealOne)
		carrier["baggage"] = "tenant=acme"
		r := p.invokeTrace(t, "index.inspectAsync", `{}`,
			map[string]string{
				"OTEL_SDK_DISABLED":           "true",
				"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
				"OTEL_BSP_SCHEDULE_DELAY":     "50",
			}, carrier)
		if !r.OK {
			t.Fatalf("disabled-SDK invoke failed: %s", r.Err)
		}
		for _, want := range []string{
			"traceId=" + traceRealOne,
			"spanId=" + parentRealOne,
			"baggage=acme",
			"recording=false",
		} {
			if !strings.Contains(r.Out, want) {
				t.Errorf("out = %q, want it to contain %q (SDK_DISABLED must keep async W3C propagation without a provider)", r.Out, want)
			}
		}
		requireNoTelemetryFailure(t, p)
		rec.requireNoSpans(t, 750*time.Millisecond)
		if err := p.closeChannel(); err != nil {
			t.Fatalf("close channel: %v", err)
		}
		if err := p.waitExit(t, 10*time.Second); err != nil {
			t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
		}
	})
}

// TestNodeRuntimeRealOtelGRPCProtocolExportsSpans pins the protocol branch
// with the real gRPC exporter package: OTEL_EXPORTER_OTLP_PROTOCOL=grpc
// selects the gRPC exporter and spans reach a receiver speaking the actual
// OTLP/gRPC wire protocol.
func TestNodeRuntimeRealOtelGRPCProtocolExportsSpans(t *testing.T) {
	pinOtelEnv(t)
	rec := newGRPCOTLPReceiver(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	r := p.invokeTrace(t, "index.work", `{"step":"1"}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "50",
			"OTEL_SERVICE_NAME":           "node-grpc",
		}, traceCarrier(traceRealThree, parentRealThree))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	spans := rec.waitForSpans(t, 1)
	child := findSpan(spans, "handler.work", traceRealThree)
	if child == nil {
		t.Fatalf("no gRPC-exported span on the carrier trace; spans: %+v", spans)
	}
	if child.ParentSpanID != parentRealThree {
		t.Errorf("span parent = %s, want the carrier's parent %s", child.ParentSpanID, parentRealThree)
	}
	if child.ServiceName != "node-grpc" {
		t.Errorf("resource service.name = %q, want node-grpc", child.ServiceName)
	}
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
	}
}

// TestNodeRuntimeRealOtelLogsPipelineAbsent is the logs OWNERSHIP test for
// Relay's trace-only setup with the real packages: Relay must never register
// an OTel LoggerProvider — pinned from the application handler by the
// official logs.setGlobalLoggerProvider() SUCCEEDING under managed tracing
// (registration only succeeds on a free registry, so its true result proves
// both that Relay left the registry untouched AND that the application can
// still register and use its own logs provider). The secondary guards stay:
// no OTEL_LOGS_EXPORTER value Relay used internally leaks into the
// application-visible environment, and no OTLP logs request reaches the
// receiver while traces flow.
func TestNodeRuntimeRealOtelLogsPipelineAbsent(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	r1 := p.invokeTrace(t, "index.logsemit", `{}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "50",
		}, nil)
	if !r1.OK {
		t.Fatalf("logsemit invoke failed: %s", r1.Err)
	}
	for _, want := range []string{
		"logsEnv=<unset>",
		"log-emitted",
		"app-logs-registered=true",
		"app-logs-active=true",
	} {
		if !strings.Contains(r1.Out, want) {
			t.Errorf("out = %q, want it to contain %q", r1.Out, want)
		}
	}

	// Traces flow in the same process, so a logs pipeline would be visible.
	r2 := p.invokeTrace(t, "index.work", `{"step":"1"}`, nil, nil)
	if !r2.OK {
		t.Fatalf("work invoke failed: %s", r2.Err)
	}
	rec.waitForSpans(t, 1)
	if n := rec.logPostCount(); n != 0 {
		t.Errorf("receiver got %d logs request(s), want none: Relay's NodeSDK setup must install no logs export pipeline", n)
	}
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
	}
}

// TestNodeRuntimeTelemetrySetupFailureKeepsProcessAlive pins the failure
// boundary: an exporter/provider setup that cannot be constructed (a gRPC
// exporter against a malformed endpoint URL) is caught as a best-effort
// telemetry warning — the invocation still succeeds, the process keeps
// serving for longer than the telemetry shutdown bound (an init-cleanup
// timeout must never process.exit a live handler process), and the ONLY exit
// is the final bounded shutdown after the channel closes, code 0.
func TestNodeRuntimeTelemetrySetupFailureKeepsProcessAlive(t *testing.T) {
	pinOtelEnv(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	// The endpoint passes Relay's credential check (no userinfo/query/fragment)
	// but is not a valid URL, so the real gRPC exporter constructor throws.
	r1 := p.invoke(t, "index.loaded", `{}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://[::1",
		})
	if !r1.OK {
		t.Fatalf("invoke with a broken telemetry setup must still succeed: %s", r1.Err)
	}
	if !strings.Contains(p.stderrString(), "provider setup failed") {
		t.Errorf("stderr = %q, want the best-effort 'provider setup failed' warning", p.stderrString())
	}
	if !strings.Contains(r1.Out, "recording=false") {
		t.Errorf("out = %q, want recording=false after the setup failure", r1.Out)
	}

	// Outlive the telemetry shutdown deadline: an init-cleanup whose shutdown
	// crossed its bound must never exit the process.
	time.Sleep(8 * time.Second)
	r2 := p.invoke(t, "index.loaded", `{}`, nil)
	if !r2.OK {
		t.Fatalf("the handler process must keep serving past the telemetry shutdown window: %s; stderr:\n%s",
			r2.Err, p.stderrString())
	}

	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("the only permitted exit is the final bounded shutdown with 0: %v; stderr:\n%s",
			err, p.stderrString())
	}
}

// TestNodeRuntimeManagedOtelWithSeparateAPI19AppCopyKeepsServing pins how
// managed tracing behaves when the application's singleton API path carries a
// PHYSICALLY SEPARATE api@1.9.0 build: the trace-only bootstrap performs its
// context-manager and provider registrations through the APPLICATION-VISIBLE
// API copy, so the global registry stays same-version and managed tracing
// still works end to end — the provider registers, a live span records and
// exports, the handler process keeps serving past the telemetry setup window
// (no setup/cleanup timer may terminate it), and the only exit is the final
// bounded shutdown with 0. The raw cross-version registration/read rules for
// two physically separate copies remain pinned by
// TestOtelApiVersionRegistrationBoundary; a registration-refused branch is
// not reachable through public fixtures under the trace-only design
// (registration always goes through the app-visible copy).
func TestNodeRuntimeManagedOtelWithSeparateAPI19AppCopyKeepsServing(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)
	v19 := requireOtelAPI19(t)
	root := appRootWithoutOtel(t)
	materializeRealOtelModules(t, root, false)
	// The application's api at the singleton path is the physically separate
	// 1.9.0 package.
	linkManagedAPI(t, root, v19)
	writeModule(t, root, "index.mjs", api19ManagedHandlerSource)
	p := startNodeScriptEnv(t, root, string(Bootstrap), nil)

	r1 := p.invoke(t, "index.work", `{"step":"1"}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "50",
			"OTEL_SERVICE_NAME":           "node-api19",
		})
	if !r1.OK {
		t.Fatalf("invoke with a separate api@1.9.0 application copy must still serve: %s", r1.Err)
	}
	// isRecording is read BEFORE end(): a live recording span proves the
	// provider delegate registered through the application's own copy.
	if !strings.Contains(r1.Out, "recording=true") {
		t.Errorf("out = %q, want recording=true while live (registration through the app-visible copy must succeed)", r1.Out)
	}
	// A successful managed start is silent; the exported span below is the
	// positive proof that registration succeeded through the app copy.
	requireNoTelemetryFailure(t, p)
	if strings.Contains(p.stderrString(), "registration failed") {
		t.Errorf("stderr = %q, want no registration failure (the registry is same-version through the app copy)", p.stderrString())
	}
	spans := rec.waitForSpans(t, 1)
	if spans[0].Name != "handler.work" || spans[0].ServiceName != "node-api19" {
		t.Errorf("exported span = %+v, want handler.work under node-api19", spans[0])
	}

	// Outlive the telemetry setup window: nothing may terminate the live
	// handler process while a provider is active.
	time.Sleep(8 * time.Second)
	r2 := p.invoke(t, "index.work", `{"step":"2"}`, nil)
	if !r2.OK {
		t.Fatalf("the handler process must keep serving past the setup window: %s; stderr:\n%s",
			r2.Err, p.stderrString())
	}
	rec.waitForSpans(t, 2)

	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("the only permitted exit is the final bounded shutdown with 0: %v; stderr:\n%s",
			err, p.stderrString())
	}
}

// TestNodeRuntimeRealOtelShutdownFlushesPendingSpans pins the final bounded
// shutdown: a span still queued in the batch processor (the schedule delay is
// far beyond the test's lifetime) must be flushed by the channel-close
// shutdown before the process exits 0.
func TestNodeRuntimeRealOtelShutdownFlushesPendingSpans(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	r := p.invokeTrace(t, "index.work", `{"step":"1"}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "600000",
			"OTEL_SERVICE_NAME":           "node-flush",
		}, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	// The batch delay is far away: nothing may export before shutdown.
	rec.requireNoSpans(t, 500*time.Millisecond)

	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s", err, p.stderrString())
	}
	spans := rec.waitForSpans(t, 1)
	if spans[0].Name != "handler.work" {
		t.Errorf("flushed span = %q, want handler.work", spans[0].Name)
	}
}

// TestNodeRuntimeRealOtelShutdownJoinsActiveInvocation pins the SIGTERM
// drain: the in-flight invocation must complete (its span is created AFTER
// the signal), the telemetry shutdown must then flush that span, and only
// then does the process exit with the SIGTERM code — join, flush, exit, in
// that order.
func TestNodeRuntimeRealOtelShutdownJoinsActiveInvocation(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	// Start the slow invocation with the endpoint configured; the huge batch
	// delay means the span can only reach the receiver via shutdown flush.
	frame, err := json.Marshal(reqFrame{
		Type: "invoke", ID: "slow-1", Handler: "index.slow",
		Event: json.RawMessage(`{}`),
		Env: map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "600000",
			"OTEL_SERVICE_NAME":           "node-drain",
		},
		Context: defaultHandlerContext("index.slow"),
	})
	if err != nil {
		t.Fatalf("marshal invoke frame: %v", err)
	}
	if err := channelWriteFrame(p.conn, frame); err != nil {
		t.Fatalf("write invoke frame: %v", err)
	}
	// Signal mid-invocation: the channel is torn down immediately, so no
	// result frame is expected (whatever arrives is tolerated).
	time.Sleep(300 * time.Millisecond)
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal SIGTERM: %v", err)
	}
	_ = p.conn.SetReadDeadline(time.Now().Add(4 * time.Second))
	_, _ = channelReadFrame(p.conn, channelResultCap)
	_ = p.conn.SetReadDeadline(time.Time{})

	exitErr := p.waitExit(t, 12*time.Second)
	var exit *exec.ExitError
	if !errors.As(exitErr, &exit) || exit.ExitCode() != 143 {
		t.Fatalf("exit = %v, want 143 (SIGTERM drain); stderr:\n%s", exitErr, p.stderrString())
	}

	// The invocation JOINED the shutdown: its completion marker was printed
	// after the signal.
	p.awaitStdoutQuiet()
	if !strings.Contains(p.stdoutString(), "slow-done") {
		t.Errorf("stdout = %q, want the in-flight invocation to complete after SIGTERM", p.stdoutString())
	}
	// And the span it created after the signal was flushed by the telemetry
	// shutdown before the exit.
	spans := rec.waitForSpans(t, 1)
	if spans[0].Name != "slow.work" {
		t.Errorf("flushed span = %q, want slow.work", spans[0].Name)
	}
}

// TestOtelApiVersionRegistrationBoundary pins the cross-version global
// registry contract with two PHYSICALLY SEPARATE official API packages
// (1.10.0 — Relay's pin — and 1.9.0 — a version an application can still
// declare). registerGlobal requires an EXACT version match for registration,
// while reads accept an equal-or-newer global. The boundary is deliberate:
// an application on 1.9 can OBSERVE what Relay's 1.10 side registers, but
// can NEVER register a provider or context manager after it — and from
// Relay's 1.10 view an application-registered 1.9 provider is invisible, not
// silently compatible.
func TestOtelApiVersionRegistrationBoundary(t *testing.T) {
	requireNode(t)
	shared := requireOtelNode(t)
	v19 := requireOtelAPI19(t)

	// Fixture identity: both copies are the official pinned packages, and the
	// 1.9 copy is physically separate.
	api110 := filepath.Join(shared, "node_modules", "@opentelemetry", "api")
	api19 := filepath.Join(v19, "node_modules", "@opentelemetry", "api")
	if got := packageVersion(t, api110); got != otelAPIVersion {
		t.Fatalf("the shared API package is %s, want the engine pin %s", got, otelAPIVersion)
	}
	if got := packageVersion(t, api19); got != "1.9.0" {
		t.Fatalf("the separate API package is %s, want 1.9.0", got)
	}
	real110, err := filepath.EvalSymlinks(api110)
	if err != nil {
		t.Fatalf("resolve the shared API package: %v", err)
	}
	real19, err := filepath.EvalSymlinks(api19)
	if err != nil {
		t.Fatalf("resolve the separate API package: %v", err)
	}
	if real110 == real19 {
		t.Fatalf("fixture precondition: the 1.9 copy must be physically separate, both resolve to %s", real110)
	}
	manager := filepath.Join(shared, "node_modules", "@opentelemetry", "context-async-hooks")

	t.Run("a 1.9 application cannot register after Relay's 1.10 globals", func(t *testing.T) {
		got := runOtelBoundaryScript(t, manager, `
const api110 = require(process.env.API110);
const api19 = require(process.env.API19);
const { AsyncLocalStorageContextManager } = require(process.env.MANAGER);
const manager = new AsyncLocalStorageContextManager();
manager.enable();
console.log("relay-manager-registered=" + api110.context.setGlobalContextManager(manager));
class AppProvider { getTracer() { return {}; } }
console.log("v19-register-provider-after-relay=" + api19.trace.setGlobalTracerProvider(new AppProvider()));
console.log("v19-register-manager-after-relay=" + api19.context.setGlobalContextManager({}));
console.log("v19-active-type=" + typeof api19.context.active());
console.log("v110-register-provider=" + api110.trace.setGlobalTracerProvider(new AppProvider()));
// The 1.9 copy READS the 1.10-registered provider: it observes the FOREIGN
// registered proxy instance, never its own local fallback.
console.log("v19-owns-local-fallback=" + (api19.trace.getTracerProvider() instanceof api19.ProxyTracerProvider));
`, api110, api19)
		want := map[string]string{
			"relay-manager-registered":          "true",
			"v19-register-provider-after-relay": "false",
			"v19-register-manager-after-relay":  "false",
			"v19-active-type":                   "object",
			"v110-register-provider":            "true",
			"v19-owns-local-fallback":           "false",
		}
		requireBoundaryKV(t, got, want)
	})

	t.Run("relay's 1.10 side cannot register into a 1.9-first registry", func(t *testing.T) {
		got := runOtelBoundaryScript(t, manager, `
const api19 = require(process.env.API19);
const api110 = require(process.env.API110);
const { AsyncLocalStorageContextManager } = require(process.env.MANAGER);
const manager = new AsyncLocalStorageContextManager();
manager.enable();
class AppProvider { getTracer() { return {}; } }
console.log("v19-manager-first=" + api19.context.setGlobalContextManager(manager));
console.log("v19-provider-first=" + api19.trace.setGlobalTracerProvider(new AppProvider()));
console.log("v110-register-provider-after-v19=" + api110.trace.setGlobalTracerProvider(new AppProvider()));
const second = new AsyncLocalStorageContextManager();
second.enable();
console.log("v110-register-manager-after-v19=" + api110.context.setGlobalContextManager(second));
// From Relay's 1.10 view the 1.9-created registry is NOT readable as a
// compatible global: the provider reads as unregistered — the refusal is
// loud, never a silent compatibility lie.
console.log("v110-provider-unregistered=" + (api110.trace.getTracerProvider() instanceof api110.ProxyTracerProvider));
`, api110, api19)
		want := map[string]string{
			"v19-manager-first":                "true",
			"v19-provider-first":               "true",
			"v110-register-provider-after-v19": "false",
			"v110-register-manager-after-v19":  "false",
			"v110-provider-unregistered":       "true",
		}
		requireBoundaryKV(t, got, want)
	})
}

// packageVersion reads an installed package's declared version.
func packageVersion(t *testing.T, pkgDir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(pkgDir, "package.json"))
	if err != nil {
		t.Fatalf("read package manifest %s: %v", pkgDir, err)
	}
	var doc struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse package manifest %s: %v", pkgDir, err)
	}
	return doc.Version
}

// runOtelBoundaryScript executes one node -e script with the two physically
// separate API packages and the shared context manager exposed through the
// environment, returning its key=value lines.
func runOtelBoundaryScript(t *testing.T, manager string, script string, api110, api19 string) map[string]string {
	t.Helper()
	cmd := exec.Command("node", "-e", script)
	cmd.Env = append(os.Environ(),
		"API110="+api110,
		"API19="+api19,
		"MANAGER="+manager,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("boundary script failed: %v\n%s", err, out)
	}
	got := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			got[key] = value
		}
	}
	return got
}

// requireBoundaryKV asserts every expected key=value line.
func requireBoundaryKV(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q (all observed: %v)", key, got[key], value, got)
		}
	}
}

// TestNodeRuntimeRealOtelMountedW3CContextThroughResolveHook is the
// HOT_RELOAD half of the real-package coverage: a source-mounted JavaScript
// handler under the mount root imports the official pinned
// @opentelemetry/api through the ACTUAL shared resolve hook — a throwing
// decoy copy sits at the mount root's node_modules, so only hook
// re-anchoring to the dependency image's singleton lets the handler load at
// all — and the incoming W3C traceparent and baggage are the active context
// across an await in that mounted module, with the warm process accepting a
// second, different carrier.
func TestNodeRuntimeRealOtelMountedW3CContextThroughResolveHook(t *testing.T) {
	pinOtelEnv(t)
	root := appRootWithoutOtel(t)
	shared := materializeRealOtelModules(t, root, true)
	linkManagedAPI(t, root, shared)
	writeMountedModule(t, root, "index.mjs", realOtelHandlerSource)
	// The conflicting host copy: a bare import that did NOT go through the
	// resolve hook would load this decoy and fail the invocation loudly.
	writeModule(t, root, "src/node_modules/@opentelemetry/api/package.json",
		`{"main":"decoy.cjs"}`)
	writeModule(t, root, "src/node_modules/@opentelemetry/api/decoy.cjs",
		`throw new Error("mount-decoy-api-loaded");`)
	p := startNodeMounted(t, root)

	carrier := traceCarrier(traceRealOne, parentRealOne)
	carrier["baggage"] = "tenant=acme"
	r1 := p.invokeTrace(t, "index.inspectAsync", `{}`, nil, carrier)
	if !r1.OK {
		t.Fatalf("mounted traced invoke failed (a decoy resolution fails here): %s", r1.Err)
	}
	for _, want := range []string{
		"traceId=" + traceRealOne,
		"spanId=" + parentRealOne,
		"baggage=acme",
	} {
		if !strings.Contains(r1.Out, want) {
			t.Errorf("mounted out = %q, want it to contain %q (the mounted handler must observe the context through the hook's API singleton)", r1.Out, want)
		}
	}

	r2 := p.invokeTrace(t, "index.inspectAsync", `{}`, nil, traceCarrier(traceRealTwo, parentRealTwo))
	if !r2.OK {
		t.Fatalf("second mounted traced invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "traceId="+traceRealTwo) || strings.Contains(r2.Out, traceRealOne) {
		t.Errorf("second mounted out = %q, want only the second carrier's trace id", r2.Out)
	}
	if strings.Contains(p.stderrString(), "context extraction failed") {
		t.Errorf("context extraction failed for the mounted handler; stderr:\n%s", p.stderrString())
	}
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s", err, p.stderrString())
	}
}

// TestNodeRuntimeRealOtelTraceSpecificEndpointAndProtocolWinExport verifies
// the ACTUAL official exporter precedence against the resolved process
// environment (the Go-side frame overlay is covered by the unit overlay
// tests; this pins what the real OTLP exporter reads): with conflicting
// generic (gRPC endpoint + grpc protocol) and trace-specific (HTTP/protobuf
// endpoint + http/protobuf protocol) pairs, plus generic exporter headers and
// resource attributes, the TRACE-SPECIFIC receiver must receive the exported
// span — carrying the Authorization header and the resource attribute — while
// the generic receiver receives nothing.
func TestNodeRuntimeRealOtelTraceSpecificEndpointAndProtocolWinExport(t *testing.T) {
	pinOtelEnv(t)
	generic := newGRPCOTLPReceiver(t)
	traceSpecific := newHTTPOTLPReceiver(t)
	root := appRootWithoutOtel(t)
	writeRealOtelHandler(t, root)
	p := startNodeRealOtel(t, root, nil)

	const authCanary = "Bearer relay-precedence-canary"
	r := p.invokeTrace(t, "index.work", `{"step":"1"}`,
		map[string]string{
			// The trace-specific endpoint is the COMPLETE signal URL: the
			// official exporter uses OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
			// verbatim (only the GENERIC endpoint gets /v1/traces appended),
			// while the generic gRPC endpoint stays a bare base URL.
			"OTEL_EXPORTER_OTLP_ENDPOINT":        generic.endpoint,
			"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": traceSpecific.endpoint + "/v1/traces",
			"OTEL_EXPORTER_OTLP_PROTOCOL":        "grpc",
			"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/protobuf",
			"OTEL_EXPORTER_OTLP_HEADERS":         "authorization=" + authCanary,
			"OTEL_RESOURCE_ATTRIBUTES":           "deployment.environment=precedence",
			"OTEL_BSP_SCHEDULE_DELAY":            "50",
			"OTEL_SERVICE_NAME":                  "node-precedence",
		}, traceCarrier(traceRealOne, parentRealOne))
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	spans := traceSpecific.waitForSpans(t, 1)
	child := findSpan(spans, "handler.work", traceRealOne)
	if child == nil {
		t.Fatalf("no span at the TRACE-SPECIFIC receiver; spans: %+v", spans)
	}
	if child.ParentSpanID != parentRealOne {
		t.Errorf("span parent = %s, want the carrier's parent %s", child.ParentSpanID, parentRealOne)
	}
	if child.ServiceName != "node-precedence" {
		t.Errorf("resource service.name = %q, want node-precedence", child.ServiceName)
	}
	if got := child.Resource["deployment.environment"]; got != "precedence" {
		t.Errorf("resource deployment.environment = %q, want the frame's precedence value", got)
	}
	auths := traceSpecific.authHeaders()
	if len(auths) == 0 || auths[len(auths)-1] != authCanary {
		t.Errorf("export Authorization headers = %v, want the generic frame value %q", auths, authCanary)
	}
	// The generic (gRPC) receiver must stay empty: both the endpoint and the
	// protocol generic values lost to their trace-specific counterparts.
	generic.requireNoSpans(t, 750*time.Millisecond)

	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
	}
}

// appPreloadSource runs via NODE_OPTIONS=--import BEFORE the bootstrap: it
// registers an application TracerProvider and its OWN context manager from
// the physically separate api@1.10.0 tree — public APIs only — and attaches a
// recorder whose shutdown() writes a marker file, so any attempt to shut the
// application-owned provider down is observable after process exit.
const appPreloadSource = `
import { writeFileSync } from "node:fs";
import { trace, context } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { TracerProvider } from "@opentelemetry/sdk-trace";

const marker = process.env.RELAY_TEST_APP_PROVIDER_MARKER;

class AppRecorder {
  onStart() {}
  onEnd() {}
  forceFlush() { return Promise.resolve(); }
  shutdown() {
    if (marker) writeFileSync(marker, "app-provider-shutdown");
    return Promise.resolve();
  }
}

const manager = new AsyncLocalStorageContextManager();
manager.enable();
console.log("preload-context-registered=" + context.setGlobalContextManager(manager));

const provider = new TracerProvider({ spanProcessors: [new AppRecorder()] });
console.log("preload-provider-registered=" + trace.setGlobalTracerProvider(provider));
console.log("app-preload-done=true");
`

// preRegisteredHandlerSource reads through the public API: the active
// extracted context (served by the PRELOAD's context manager) and live
// recording spans (served by the preload's provider) prove Relay left both
// registrations intact.
const preRegisteredHandlerSource = `
import { trace, context as otelContext } from "@opentelemetry/api";

export function check(event, context) {
  const span = trace.getSpan(otelContext.active());
  console.log("active=" + (span ? span.spanContext().traceId : "none"));
  const probe = trace.getTracer("app-first").startSpan("handler.probe");
  console.log("recording=" + probe.isRecording());
  probe.end();
  console.log("check-ok " + event.msg);
}
`

// TestNodeRuntimeRealOtelPreRegisteredAppProviderIsNeverReplacedOrShutdown
// pins the already-registered-provider contract under DEFAULT managed
// ownership: a NODE_OPTIONS --import preload under /app registers an
// application TracerProvider AND its own context manager from a physically
// separate api@1.10.0 tree before the bootstrap starts. When the first
// invocation supplies the managed endpoint trigger, Relay must attempt
// registration and BACK OFF (the registry is already occupied at the exact
// same version) — so the application provider stays the registered one,
// no Relay span ever reaches the receiver, Relay's setup/cleanup never shuts
// the application provider down (its recorder's shutdown marker stays absent
// after channel close), and handlers keep working across warm reuse with
// extraction flowing through the application's context manager.
func TestNodeRuntimeRealOtelPreRegisteredAppProviderIsNeverReplacedOrShutdown(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)
	appDeps := requireOtelAppDeps(t)
	root := appRootWithoutOtel(t)
	writeModule(t, root, "index.mjs", preRegisteredHandlerSource)
	writeModule(t, root, "src/otel-preload.mjs", appPreloadSource)
	if err := os.Symlink(filepath.Join(appDeps, "node_modules"),
		filepath.Join(root, "app", "src", "node_modules")); err != nil {
		t.Fatalf("link the application's OTel tree: %v", err)
	}
	shared := materializeRealOtelModules(t, root, false)
	linkManagedAPI(t, root, shared)

	// Fixture preconditions: the preload resolves a physically separate
	// api@1.10.0 copy, the bootstrap the managed singleton — both at the
	// engine's exact pin (same-version registry, distinct installs).
	bootstrapAPI, err := filepath.EvalSymlinks(filepath.Join(root, "app", "node_modules", "@opentelemetry", "api"))
	if err != nil {
		t.Fatalf("resolve the bootstrap's API package: %v", err)
	}
	appAPI, err := filepath.EvalSymlinks(filepath.Join(root, "app", "src", "node_modules", "@opentelemetry", "api"))
	if err != nil {
		t.Fatalf("resolve the preload's API package: %v", err)
	}
	if appAPI == bootstrapAPI {
		t.Fatalf("fixture precondition: the preload's API copy must be physically separate, both resolve to %s", appAPI)
	}
	if got := packageVersion(t, appAPI); got != otelAPIVersion {
		t.Fatalf("the preload's API copy is %s, want the exact pin %s", got, otelAPIVersion)
	}

	marker := filepath.Join(root, "APP-PROVIDER-SHUTDOWN")
	preload := filepath.Join(root, "app", "src", "otel-preload.mjs")
	p := startNodeScriptEnv(t, root, string(Bootstrap), []string{
		"NODE_OPTIONS=--import=file://" + filepath.ToSlash(preload),
		"RELAY_TEST_APP_PROVIDER_MARKER=" + marker,
	})

	// The preload ran to completion BEFORE the bootstrap and won both
	// registrations on the shared, same-version registry.
	p.awaitStdoutQuiet()
	for _, want := range []string{
		"preload-context-registered=true",
		"preload-provider-registered=true",
		"app-preload-done=true",
	} {
		if !strings.Contains(p.stdoutString(), want) {
			t.Errorf("stdout = %q, want it to contain %q", p.stdoutString(), want)
		}
	}

	// The first invocation carries the managed endpoint trigger: Relay must
	// attempt registration, be refused, and back off — the handler keeps
	// seeing the APPLICATION's provider and its extracted context.
	env := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
		"OTEL_BSP_SCHEDULE_DELAY":     "50",
	}
	r1 := p.invokeTrace(t, "index.check", `{"msg":"one"}`, env,
		traceCarrier(traceRealOne, parentRealOne))
	if !r1.OK {
		t.Fatalf("invoke with a pre-registered provider must succeed: %s", r1.Err)
	}
	for _, want := range []string{
		"active=" + traceRealOne,
		"recording=true",
		"check-ok one",
	} {
		if !strings.Contains(r1.Out, want) {
			t.Errorf("out = %q, want it to contain %q", r1.Out, want)
		}
	}
	if !strings.Contains(p.stderrString(), "registration failed") {
		t.Errorf("stderr = %q, want Relay to report the refused registration and back off", p.stderrString())
	}
	if strings.Contains(p.stderrString(), "provider setup failed") {
		t.Errorf("stderr = %q, Relay must back off cleanly beside a registered provider", p.stderrString())
	}

	// Warm reuse on a second carrier: still the application's provider,
	// still recording, extraction still flowing through the application's
	// context manager.
	r2 := p.invokeTrace(t, "index.check", `{"msg":"two"}`, nil,
		traceCarrier(traceRealTwo, parentRealTwo))
	if !r2.OK {
		t.Fatalf("warm reuse with a pre-registered provider must succeed: %s", r2.Err)
	}
	for _, want := range []string{
		"active=" + traceRealTwo,
		"recording=true",
		"check-ok two",
	} {
		if !strings.Contains(r2.Out, want) {
			t.Errorf("warm out = %q, want it to contain %q", r2.Out, want)
		}
	}

	// No Relay span may ever reach the receiver while the application-owned
	// provider holds the registry.
	rec.requireNoSpans(t, 750*time.Millisecond)

	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0: %v; stderr:\n%s", err, p.stderrString())
	}

	// The application-owned provider was never shut down: its recorder's
	// shutdown marker must not exist after the process exit.
	if _, err := os.Stat(marker); err == nil {
		t.Errorf("the application-owned provider was shut down (marker %s exists); "+
			"Relay must never shut an already registered provider down", marker)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat provider shutdown marker: %v", err)
	}
}

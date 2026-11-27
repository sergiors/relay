//go:build integration

package node

// Cross-boundary logging/telemetry acceptance for the Node engine: an actual
// Go-managed invocation trace is injected as the frame's W3C carrier, the
// Node handler opens a NESTED active span under it and emits one structured
// JSON record through the PACKAGED SDK Logger (@sergiors/relay-runtime), and
// the record's trace_id/span_id must agree with both exported spans — the Go
// side's exported invocation span (in-process exporter) and the Node side's
// OTLP-exported nested span (controlled local receiver).
//
// The packaged SDK is used exactly as an application would depend on it:
// packages/node/dist is vendored into the fake image's app node_modules. When
// dist is not built, an isolated temporary build compiles it without ever
// touching the working tree; when that fails the test FAILS with an explicit
// environmental message rather than skipping.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"relay/internal/observability/tracing"
)

// nodeLoggingCorrelationHandler is the application under test: it imports the
// packaged structured logger, opens one nested active span under the frame's
// extracted trace context, logs once inside it, and reports the span identity
// it observed on stdout.
const nodeLoggingCorrelationHandler = `
import { trace } from "@opentelemetry/api";
import { Logger } from "@sergiors/relay-runtime/logging";

const log = new Logger({ service: "otel-correlation", level: "INFO" });

export async function nested(event, context) {
  const tracer = trace.getTracer("relay.test");
  await tracer.startActiveSpan("app.work", async (span) => {
    log.info("correlated-log", { step: event.step });
    const active = span.spanContext();
    console.log("nested-active=" + active.traceId + "/" + active.spanId);
    span.end();
  });
}
`

var (
	nodeSDKOnce   sync.Once
	nodeSDKDir    string
	nodeSDKErr    error
	nodeSDKIsTemp bool
)

// requireNodeRuntimeDist returns a directory holding the packaged SDK layout
// (package.json + dist): packages/node itself when already built, otherwise
// an isolated temporary build. FAILS with an environmental message when the
// SDK can be neither found nor built.
func requireNodeRuntimeDist(t *testing.T) string {
	t.Helper()
	requireNode(t)
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve the repository root: %v", err)
	}
	sourcePkg := filepath.Join(root, "packages", "node")
	nodeSDKOnce.Do(func() {
		if _, statErr := os.Stat(filepath.Join(sourcePkg, "dist", "logging.js")); statErr == nil {
			nodeSDKDir = sourcePkg
			return
		}
		built, buildErr := buildNodeRuntimeDist(sourcePkg)
		if buildErr != nil {
			nodeSDKErr = buildErr
			return
		}
		nodeSDKDir = built
		nodeSDKIsTemp = true
	})
	if nodeSDKErr != nil {
		t.Fatalf("environmental blocker: packages/node/dist is missing and the isolated build failed: %v",
			nodeSDKErr)
	}
	return nodeSDKDir
}

// buildNodeRuntimeDist compiles packages/node in an isolated temporary copy
// (npm install + tsc), so an unbuilt checkout can still serve the packaged
// SDK to tests without modifying the working tree.
func buildNodeRuntimeDist(sourcePkg string) (string, error) {
	npmPath, err := exec.LookPath("npm")
	if err != nil {
		return "", fmt.Errorf("npm is required to build the Node SDK: %w", err)
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		return "", fmt.Errorf("node is required to build the Node SDK: %w", err)
	}
	dir, err := os.MkdirTemp("", "relay-node-sdk-build-")
	if err != nil {
		return "", err
	}
	for _, entry := range []string{"package.json", "src", "tsconfig.json"} {
		if err := copyTree(filepath.Join(sourcePkg, entry), filepath.Join(dir, entry)); err != nil {
			_ = os.RemoveAll(dir)
			return "", fmt.Errorf("stage %s: %w", entry, err)
		}
	}
	install := exec.Command(npmPath, "install", "--no-audit", "--no-fund", "--loglevel=error")
	install.Dir = dir
	if out, err := install.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("npm install the SDK dependencies: %v\n%s", err, tailProcessOutput(out))
	}
	build := exec.Command(nodePath,
		filepath.Join(dir, "node_modules", "typescript", "bin", "tsc"),
		"--project", filepath.Join(dir, "tsconfig.json"))
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("compile the SDK: %v\n%s", err, tailProcessOutput(out))
	}
	if _, err := os.Stat(filepath.Join(dir, "dist", "logging.js")); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("the built SDK has no dist/logging.js: %w", err)
	}
	return dir, nil
}

// copyTree recursively copies the file or directory src to dst.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// writeVendoredRelayRuntime installs the packaged SDK into the fake image's
// app node_modules exactly as an application dependency would appear.
func writeVendoredRelayRuntime(t *testing.T, root, pkgDir string) {
	t.Helper()
	dest := filepath.Join(root, "app", "node_modules", "@sergiors", "relay-runtime")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("mkdir the vendored runtime package: %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(pkgDir, "package.json"))
	if err != nil {
		t.Fatalf("read the runtime package manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "package.json"), manifest, 0o644); err != nil {
		t.Fatalf("write the vendored runtime manifest: %v", err)
	}
	if err := copyTree(filepath.Join(pkgDir, "dist"), filepath.Join(dest, "dist")); err != nil {
		t.Fatalf("vendor the built runtime dist: %v", err)
	}
}

// jsonLogRecords parses the JSON log records from a stderr/stdout window;
// non-JSON lifecycle lines are ignored.
func jsonLogRecords(out string) []map[string]any {
	var records []map[string]any
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "{") {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
			continue
		}
		records = append(records, record)
	}
	return records
}

// logRecordsWithMessage returns the records carrying exactly the message.
func logRecordsWithMessage(records []map[string]any, message string) []map[string]any {
	var matched []map[string]any
	for _, record := range records {
		if record["message"] == message {
			matched = append(matched, record)
		}
	}
	return matched
}

// TestIntegrationNodeLoggerCorrelatesWithExportedSpan proves the full
// correlation chain: Go invocation span (exported in-process) → injected
// traceparent → Node handler's NESTED active span (exported over real OTLP)
// → the packaged Logger's JSON record, whose trace_id/span_id must match the
// exported spans on both sides while carrying the invocation identity.
func TestIntegrationNodeLoggerCorrelatesWithExportedSpan(t *testing.T) {
	pinOtelEnv(t)
	rec := newHTTPOTLPReceiver(t)

	// Given the actual Go invocation trace: a real Go-managed span exported
	// through Relay's production tracing setup.
	goExport := tracetest.NewInMemoryExporter()
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	provider, err := tracing.Setup(context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		tracing.WithExporter(goExport))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	invokeCtx, goSpan := tracing.Start(context.Background(), "function.invoke")
	goTraceID := goSpan.SpanContext().TraceID().String()
	goSpanID := goSpan.SpanContext().SpanID().String()
	if !goSpan.SpanContext().IsValid() {
		t.Fatalf("the Go invocation span context is invalid: %+v", goSpan.SpanContext())
	}

	root := appRootWithoutOtel(t)
	writeModule(t, root, "index.mjs", nodeLoggingCorrelationHandler)
	writeVendoredRelayRuntime(t, root, requireNodeRuntimeDist(t))
	p := startNodeRealOtel(t, root, nil)

	// when the handler runs through the production channel under that trace
	// with a real managed provider exporting to the controlled receiver
	result := p.invokeTrace(t, "index.nested", `{"step":"a"}`,
		map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": rec.endpoint,
			"OTEL_BSP_SCHEDULE_DELAY":     "50",
		},
		traceCarrier(goTraceID, goSpanID))
	if !result.OK {
		t.Fatalf("invoke failed: %s", result.Err)
	}
	goSpan.End()
	if err := provider.ForceFlush(invokeCtx); err != nil {
		t.Fatalf("flush the Go invocation span: %v", err)
	}

	// then the Go side exported its span under the injected identity
	goExported := 0
	for _, stub := range goExport.GetSpans() {
		if stub.Name == "function.invoke" &&
			stub.SpanContext.TraceID().String() == goTraceID &&
			stub.SpanContext.SpanID().String() == goSpanID {
			goExported++
		}
	}
	if goExported != 1 {
		names := make([]string, 0, len(goExport.GetSpans()))
		for _, stub := range goExport.GetSpans() {
			names = append(names, stub.Name)
		}
		t.Fatalf("the actual Go invocation span exported %d time(s) as function.invoke/%s/%s; span names: %v",
			goExported, goTraceID, goSpanID, names)
	}

	// and the Node side emitted exactly one structured record for the log
	awaitStderrContains(t, p, "correlated-log")
	matched := logRecordsWithMessage(jsonLogRecords(p.stderrString()), "correlated-log")
	if len(matched) != 1 {
		t.Fatalf("structured records with message %q = %d, want 1; stderr:\n%s",
			"correlated-log", len(matched), p.stderrString())
	}
	record := matched[0]
	if record["level"] != "INFO" || record["service"] != "otel-correlation" ||
		record["logger"] != "relay_runtime.otel-correlation" {
		t.Errorf("record identity = level %v service %v logger %v, want INFO/otel-correlation/relay_runtime.otel-correlation; record: %+v",
			record["level"], record["service"], record["logger"], record)
	}
	if record["app"] != "testapp" || record["handler"] != "index.nested" ||
		record["request_id"] != "test-request" || record["attempt"] != float64(1) ||
		record["cold_start"] != true {
		t.Errorf("record invocation fields = app %v handler %v request_id %v attempt %v cold_start %v, want the frame's identity on the cold start; record: %+v",
			record["app"], record["handler"], record["request_id"], record["attempt"],
			record["cold_start"], record)
	}
	recordTrace, _ := record["trace_id"].(string)
	recordSpan, _ := record["span_id"].(string)
	if recordTrace != goTraceID {
		t.Errorf("record trace_id = %q, want the actual Go invocation trace %q", recordTrace, goTraceID)
	}
	if recordSpan == "" || recordSpan == goSpanID {
		t.Errorf("record span_id = %q, want the NESTED handler span (not the Go parent %q)", recordSpan, goSpanID)
	}
	attributes, ok := record["attributes"].(map[string]any)
	if !ok || attributes["step"] != "a" {
		t.Errorf("record attributes = %v, want {step: a}", record["attributes"])
	}

	// when the Node provider's OTLP export arrives at the receiver
	spans := rec.waitForSpans(t, 1)
	nested := findSpan(spans, "app.work", goTraceID)
	if nested == nil {
		names := make([]string, 0, len(spans))
		for _, span := range spans {
			names = append(names, span.Name+"/"+span.TraceID)
		}
		t.Fatalf("no exported app.work span for Go trace %s; exported: %v", goTraceID, names)
	}

	// then the record's ids are exactly the exported nested span's ids,
	// parented to the actual Go invocation span
	if nested.SpanID != recordSpan {
		t.Errorf("exported nested span id = %q, record span_id = %q; they must match",
			nested.SpanID, recordSpan)
	}
	if nested.ParentSpanID != goSpanID {
		t.Errorf("exported nested span parent = %q, want the Go invocation span %q",
			nested.ParentSpanID, goSpanID)
	}

	// and the handler itself observed that same active span
	wantMarker := "nested-active=" + goTraceID + "/" + nested.SpanID
	if !strings.Contains(result.Out, wantMarker) {
		t.Errorf("stdout must carry %q; stdout:\n%s", wantMarker, result.Out)
	}
}

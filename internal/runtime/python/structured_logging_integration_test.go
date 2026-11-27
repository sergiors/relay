//go:build integration

package python

// Structured application logging through relay_runtime.logging, driven end to
// end over the authenticated channel. A handler module that imports the SDK
// lazily (its module import runs INSIDE the first invocation) still emits one
// compact JSON record per line on stderr carrying the invocation metadata the
// bootstrap binds before the import. Each warm invocation starts from fresh
// metadata: cold_start flips after the first accepted invocation, temporary
// context keys reset even when the previous handler failed, and appended keys
// survive for the logger's lifetime. The SDK keeps working without an
// OpenTelemetry install, and an active span supplies trace correlation when
// one exists.
//
// The child imports relay_runtime from the live packages/python source tree,
// exactly as an app that declared relay-runtime as a dependency. startPython
// pins PYTHONPATH to the handler dir alone, so these tests spawn through
// startEmbeddedBootstrap with the SDK source appended (the dependency
// injection tests' pattern) plus the OTel API stub.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// requireSDKImport proves python3 can import the structured Logger from the
// source tree before any bootstrap spawns, so a syntax or version problem
// fails with a direct diagnosis instead of a confusing invocation error.
func requireSDKImport(t *testing.T, src string) {
	t.Helper()
	cmd := exec.Command("python3", "-c", "from relay_runtime.logging import Logger")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("python3 cannot import relay_runtime.logging from %s: %v\n%s", src, err, out)
	}
}

// structuredLoggingEnv puts dir ahead of the SDK source on the child's
// PYTHONPATH (exec keeps the last duplicate key) after the import precheck.
// OTEL_SERVICE_NAME is set but empty so service resolution falls through to
// the invocation app deterministically, regardless of the host environment.
func structuredLoggingEnv(t *testing.T, dir string) []string {
	t.Helper()
	src := relayRuntimeSrc(t)
	requireSDKImport(t, src)
	return []string{
		"PYTHONPATH=" + dir + string(os.PathListSeparator) + src,
		"OTEL_SERVICE_NAME=",
	}
}

// startPythonWithSDK is the startPython harness plus the SDK source entry the
// default startPython cannot carry: the OTel API stub first, then the embedded
// bootstrap with the extended PYTHONPATH.
func startPythonWithSDK(t *testing.T, dir string) *pyProc {
	t.Helper()
	writeOtelStub(t, dir)
	return startEmbeddedBootstrap(t, dir, Bootstrap, structuredLoggingEnv(t, dir))
}

// jsonRecords parses every single-line JSON object in out; non-JSON stderr
// (lifecycle output, handler tracebacks) is ignored.
func jsonRecords(t *testing.T, out string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "{") {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(trimmed), &record); err != nil {
			t.Errorf("stderr line looks like JSON but does not parse: %v\nline: %s", err, trimmed)
			continue
		}
		records = append(records, record)
	}
	return records
}

// awaitJSONRecords waits until the child's stderr carries at least count JSON
// records with the given message and returns them in emission order. The
// stderr collector goroutine is asynchronous to the result frame's arrival on
// the channel, so expected records are awaited instead of read immediately;
// pipe writes are ordered, so earlier records stay ahead of later ones.
func awaitJSONRecords(t *testing.T, p *pyProc, message string, count int) []map[string]any {
	t.Helper()
	bound := 3 * time.Second
	deadline := time.Now().Add(bound)
	var found []map[string]any
	for time.Now().Before(deadline) {
		found = found[:0]
		for _, record := range jsonRecords(t, p.stderrString()) {
			if record["message"] == message {
				found = append(found, record)
			}
		}
		if len(found) >= count {
			return found
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("found %d JSON record(s) with message %q within %v, want at least %d; stderr:\n%s",
		len(found), message, bound, count, p.stderrString())
	return nil
}

// awaitJSONRecord returns the single earliest JSON record with the message.
func awaitJSONRecord(t *testing.T, p *pyProc, message string) map[string]any {
	t.Helper()
	return awaitJSONRecords(t, p, message, 1)[0]
}

// requireCoreRecord asserts the record carries the required keys: an RFC 3339
// timestamp, the given level, a non-empty service (equal to wantService when
// non-empty), and a message string.
func requireCoreRecord(t *testing.T, record map[string]any, wantService, level string) {
	t.Helper()
	timestamp, ok := record["timestamp"].(string)
	if !ok || timestamp == "" {
		t.Errorf("record %v carries no timestamp string", record)
	} else if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Errorf("timestamp %q is not RFC 3339: %v", timestamp, err)
	}
	if got := record["level"]; got != level {
		t.Errorf("record %q level = %v, want %s", fmt.Sprint(record["message"]), got, level)
	}
	service, ok := record["service"].(string)
	if !ok || service == "" {
		t.Errorf("record %v carries no non-empty service string", record)
	} else if wantService != "" && service != wantService {
		t.Errorf("record %q service = %q, want %q", fmt.Sprint(record["message"]), service, wantService)
	}
	if _, ok := record["message"].(string); !ok {
		t.Errorf("record carries no message string: %v", record)
	}
}

// requireInvocationMetadata asserts the promised minimal invocation metadata
// (app, handler, request_id, attempt, cold_start) and that fields outside the
// minimal rule — max_attempts and message_id — are never emitted even though
// the frame may carry them.
func requireInvocationMetadata(
	t *testing.T, record map[string]any,
	app, handler, requestID string, attempt int, coldStart bool,
) {
	t.Helper()
	want := map[string]any{
		"app":        app,
		"handler":    handler,
		"request_id": requestID,
		"attempt":    float64(attempt),
		"cold_start": coldStart,
	}
	for key, wantValue := range want {
		if got := record[key]; got != wantValue {
			t.Errorf("record %q field %s = %#v, want %#v",
				fmt.Sprint(record["message"]), key, got, wantValue)
		}
	}
	for _, absent := range []string{"max_attempts", "message_id"} {
		if value, present := record[absent]; present {
			t.Errorf("record %q must omit %s under the minimal metadata rule, got %#v",
				fmt.Sprint(record["message"]), absent, value)
		}
	}
}

// requireAttributes returns the record's nested attributes object, failing the
// test when it is missing.
func requireAttributes(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	attributes, ok := record["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("record %v carries no nested attributes object", record)
	}
	return attributes
}

// requireLocation asserts the record carries a location referencing the
// emitting module (module-qualified or as a file name).
func requireLocation(t *testing.T, record map[string]any, moduleRef string) {
	t.Helper()
	location, ok := record["location"]
	if !ok {
		t.Fatalf("record %v carries no location", record)
	}
	encoded, err := json.Marshal(location)
	if err != nil {
		t.Fatalf("marshal location: %v", err)
	}
	if !strings.Contains(string(encoded), moduleRef) {
		t.Errorf("location %s does not reference %s", encoded, moduleRef)
	}
}

// requireRecordCount asserts stderr carries exactly want JSON records with the
// message — a second emission would mean a duplicated handler.
func requireRecordCount(t *testing.T, p *pyProc, message string, want int) {
	t.Helper()
	found := 0
	for _, record := range jsonRecords(t, p.stderrString()) {
		if record["message"] == message {
			found++
		}
	}
	if found != want {
		t.Errorf("JSON records with message %q = %d, want exactly %d; stderr:\n%s",
			message, found, want, p.stderrString())
	}
}

// TestPythonRuntimeStructuredLoggingMetadataOnLazyImport pins the metadata
// contract on the lazy-import path: the handler module is first imported
// DURING the invocation, yet its import-time record and its handler-time
// record both carry this invocation's identity, the frame's trace correlation,
// a source location, and the required JSON keys — exactly one import-time
// record each, on stderr. It also pins construction-time service resolution:
// an explicit service wins over the invocation app, while a default-constructed
// logger built at the lazy import resolves to the FIRST invocation's app
// identity and keeps it across later invocations.
func TestPythonRuntimeStructuredLoggingMetadataOnLazyImport(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("checkout-svc", level="INFO")
default_log = Logger()
log.info("sdk-import-record")
default_log.info("sdk-default-service-record")


def emit(event, context):
    log.info("sdk-handler-record", extra={"order": event["order"]})
    default_log.info("sdk-default-service-record", extra={"order": event["order"]})
    print("emit-done")
`)
	p := startPythonWithSDK(t, dir)

	r := p.invokeFrame(t, "handler.emit", `{"order":"o-1"}`, nil, nil, map[string]any{
		"requestId":   "req-lazy-1",
		"attempt":     2,
		"maxAttempts": 4,
		"deadline":    "2031-01-01T00:00:00Z",
		"traceId":     "4bf92f3577b34da6a3ce929d0e0e4736",
		"app":         "shop",
		"handler":     "handler.emit",
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "emit-done") {
		t.Fatalf("out = %q, want the handler to run", r.Out)
	}

	imported := awaitJSONRecord(t, p, "sdk-import-record")
	requireCoreRecord(t, imported, "checkout-svc", "INFO")
	requireInvocationMetadata(t, imported, "shop", "handler.emit", "req-lazy-1", 2, true)
	requireLocation(t, imported, "handler.")
	if got := imported["trace_id"]; got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("import-time record trace_id = %v, want the invocation frame's trace ID", got)
	}
	if _, hasSpan := imported["span_id"]; hasSpan {
		t.Errorf("record must not fabricate a span_id without an active span: %v", imported)
	}

	handled := awaitJSONRecord(t, p, "sdk-handler-record")
	requireCoreRecord(t, handled, "checkout-svc", "INFO")
	requireInvocationMetadata(t, handled, "shop", "handler.emit", "req-lazy-1", 2, true)
	requireLocation(t, handled, "handler.")
	if got := requireAttributes(t, handled)["order"]; got != "o-1" {
		t.Errorf("attributes.order = %v, want o-1", got)
	}

	// Construction-time service resolution at the lazy import: with the
	// environment service empty, the default-constructed logger resolves to
	// the first invocation's app identity while the explicit service above
	// wins for its sibling.
	importedDefault := awaitJSONRecord(t, p, "sdk-default-service-record")
	requireCoreRecord(t, importedDefault, "shop", "INFO")
	requireInvocationMetadata(t, importedDefault, "shop", "handler.emit", "req-lazy-1", 2, true)

	// A later invocation under a different app must not change the service
	// resolved once at construction.
	r2 := p.invokeFrame(t, "handler.emit", `{"order":"o-2"}`, nil, nil, map[string]any{
		"requestId":   "req-lazy-2",
		"attempt":     1,
		"maxAttempts": 4,
		"deadline":    "2031-01-01T00:00:00Z",
		"app":         "another-app",
		"handler":     "handler.emit",
	})
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	defaults := awaitJSONRecords(t, p, "sdk-default-service-record", 3)
	secondDefault := defaults[2]
	requireCoreRecord(t, secondDefault, "shop", "INFO")
	requireInvocationMetadata(t, secondDefault, "another-app", "handler.emit", "req-lazy-2", 1, false)
	if got := requireAttributes(t, secondDefault)["order"]; got != "o-2" {
		t.Errorf("second default-service record attributes.order = %v, want o-2", got)
	}
	handledSecond := awaitJSONRecords(t, p, "sdk-handler-record", 2)[1]
	requireCoreRecord(t, handledSecond, "checkout-svc", "INFO")
	requireInvocationMetadata(t, handledSecond, "another-app", "handler.emit", "req-lazy-2", 1, false)

	requireRecordCount(t, p, "sdk-import-record", 1)
	requireRecordCount(t, p, "sdk-handler-record", 2)
	requireRecordCount(t, p, "sdk-default-service-record", 3)
}

// TestPythonRuntimeStructuredLoggingWarmIsolationAndColdStart pins the warm
// lifecycle across two invocations of one process: the first accepted
// invocation reports cold_start true and the second false, each record carries
// its own invocation's request identity, and neither appended nor temporary
// context keys bound in the first invocation reach the second.
func TestPythonRuntimeStructuredLoggingWarmIsolationAndColdStart(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger()


def emit(event, context):
    log.append_keys(**{event["akey"]: event["avalue"]})
    with log.append_context_keys(**{event["tkey"]: event["tvalue"]}):
        log.info("warm-record", extra={"marker": event["avalue"]})
`)
	p := startPythonWithSDK(t, dir)

	frame := func(handler, requestID string) map[string]any {
		return map[string]any{
			"requestId":   requestID,
			"attempt":     1,
			"maxAttempts": 3,
			"deadline":    "2031-01-01T00:00:00Z",
			"app":         "shop",
			"handler":     handler,
		}
	}

	r1 := p.invokeFrame(t, "handler.emit",
		`{"akey":"appended_one","avalue":"a1","tkey":"phase_one","tvalue":"t1"}`,
		nil, nil, frame("handler.emit", "req-one"))
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	first := awaitJSONRecords(t, p, "warm-record", 1)[0]
	requireCoreRecord(t, first, "", "INFO")
	requireInvocationMetadata(t, first, "shop", "handler.emit", "req-one", 1, true)
	firstAttributes := requireAttributes(t, first)
	if firstAttributes["appended_one"] != "a1" {
		t.Errorf("first record appended key = %v, want a1", firstAttributes["appended_one"])
	}
	if firstAttributes["phase_one"] != "t1" {
		t.Errorf("first record temporary key = %v, want t1", firstAttributes["phase_one"])
	}
	if firstAttributes["marker"] != "a1" {
		t.Errorf("first record extra marker = %v, want a1", firstAttributes["marker"])
	}

	r2 := p.invokeTrace(t, "handler.emit",
		`{"akey":"appended_two","avalue":"a2","tkey":"phase_two","tvalue":"t2"}`,
		nil, map[string]string{
			"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
		})
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	second := awaitJSONRecords(t, p, "warm-record", 2)[1]
	requireCoreRecord(t, second, "", "INFO")
	requireInvocationMetadata(t, second, "testapp", "handler.emit", "test-request", 1, false)
	secondAttributes := requireAttributes(t, second)
	if secondAttributes["appended_two"] != "a2" {
		t.Errorf("second record appended key = %v, want a2", secondAttributes["appended_two"])
	}
	if secondAttributes["phase_two"] != "t2" {
		t.Errorf("second record temporary key = %v, want t2", secondAttributes["phase_two"])
	}
	if secondAttributes["marker"] != "a2" {
		t.Errorf("second record extra marker = %v, want a2", secondAttributes["marker"])
	}
	if leaked, present := secondAttributes["appended_one"]; present {
		t.Errorf("appended key from the first invocation leaked: %v", leaked)
	}
	if leaked, present := secondAttributes["phase_one"]; present {
		t.Errorf("temporary context key from the first invocation leaked: %v", leaked)
	}

	requireRecordCount(t, p, "warm-record", 2)
}

// TestPythonRuntimeStructuredLoggingContextResetsAfterFailure pins the
// failure path: a handler that logs an exception and re-raises still fails
// only its invocation, its records carry cold_start true with the exception
// object, and the next invocation starts from fresh metadata — its own
// request identity, cold_start false, no leaked temporary context key — while
// the container stays healthy.
func TestPythonRuntimeStructuredLoggingContextResetsAfterFailure(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("failure-svc", level="INFO")


def boom(event, context):
    log.append_keys(component="billing")
    with log.append_context_keys(stage="failing"):
        try:
            raise ValueError("structured-kaboom")
        except ValueError:
            log.exception("failure-record")
            raise


def ok(event, context):
    log.info("recovery-record", extra={"step": "recovered"})
`)
	p := startPythonWithSDK(t, dir)

	frame := func(handler, requestID string) map[string]any {
		return map[string]any{
			"requestId":   requestID,
			"attempt":     1,
			"maxAttempts": 3,
			"deadline":    "2031-01-01T00:00:00Z",
			"app":         "shop",
			"handler":     handler,
		}
	}

	failing := p.invokeFrame(t, "handler.boom", `{}`, nil, nil, frame("handler.boom", "req-fail"))
	if failing.OK {
		t.Fatal("expected the raising handler to fail the invocation")
	}
	if !strings.Contains(failing.Err, "structured-kaboom") {
		t.Errorf("failure error = %q, want the handler's message", failing.Err)
	}

	failure := awaitJSONRecord(t, p, "failure-record")
	requireCoreRecord(t, failure, "failure-svc", "ERROR")
	requireInvocationMetadata(t, failure, "shop", "handler.boom", "req-fail", 1, true)
	requireLocation(t, failure, "handler.")
	exception, ok := failure["exception"].(map[string]any)
	if !ok {
		t.Fatalf("failure record carries no exception object: %v", failure)
	}
	if exception["type"] != "ValueError" {
		t.Errorf("exception type = %v, want ValueError", exception["type"])
	}
	if message := fmt.Sprint(exception["message"]); !strings.Contains(message, "structured-kaboom") {
		t.Errorf("exception message = %q, want it to contain structured-kaboom", message)
	}
	failureAttributes := requireAttributes(t, failure)
	if failureAttributes["stage"] != "failing" {
		t.Errorf("failure record temporary key stage = %v, want failing", failureAttributes["stage"])
	}
	if failureAttributes["component"] != "billing" {
		t.Errorf("failure record appended component = %v, want billing", failureAttributes["component"])
	}

	recovering := p.invokeFrame(t, "handler.ok", `{}`, nil, nil, frame("handler.ok", "req-ok"))
	if !recovering.OK {
		t.Fatalf("post-failure invoke failed: %s", recovering.Err)
	}
	recovery := awaitJSONRecord(t, p, "recovery-record")
	requireCoreRecord(t, recovery, "failure-svc", "INFO")
	requireInvocationMetadata(t, recovery, "shop", "handler.ok", "req-ok", 1, false)
	if recoveryAttributes, hasAttributes := recovery["attributes"].(map[string]any); hasAttributes {
		if leaked, present := recoveryAttributes["stage"]; present {
			t.Errorf("temporary context key from the failed invocation leaked: %v", leaked)
		}
		if leaked, present := recoveryAttributes["component"]; present {
			t.Errorf("appended key from the failed invocation leaked: %v", leaked)
		}
		if recoveryAttributes["step"] != "recovered" {
			t.Errorf("recovery record extra step = %v, want recovered", recoveryAttributes["step"])
		}
	} else {
		t.Errorf("recovery record %v carries no attributes despite an extra field", recovery)
	}

	requireRecordCount(t, p, "failure-record", 1)
	requireRecordCount(t, p, "recovery-record", 1)
}

// TestPythonRuntimeStructuredLoggingWithoutOtel pins the guarded import: when
// the OpenTelemetry package raises on import, the SDK still emits complete
// JSON records with invocation metadata instead of failing the log call.
func TestPythonRuntimeStructuredLoggingWithoutOtel(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	pkg := filepath.Join(dir, "opentelemetry")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir opentelemetry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "__init__.py"),
		[]byte(`raise ImportError("otel unavailable")`), 0o644); err != nil {
		t.Fatalf("write broken opentelemetry: %v", err)
	}
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("no-otel-svc", level="INFO")


def emit(event, context):
    log.info("no-otel-record", extra={"ok": True})
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, structuredLoggingEnv(t, dir))

	r := p.invokeFrame(t, "handler.emit", `{}`, nil, nil, map[string]any{
		"requestId":   "req-no-otel",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "2031-01-01T00:00:00Z",
		"app":         "shop",
		"handler":     "handler.emit",
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	record := awaitJSONRecord(t, p, "no-otel-record")
	requireCoreRecord(t, record, "no-otel-svc", "INFO")
	requireInvocationMetadata(t, record, "shop", "handler.emit", "req-no-otel", 1, true)
	requireLocation(t, record, "handler.")
	if got := requireAttributes(t, record)["ok"]; got != true {
		t.Errorf("attributes.ok = %v, want true", got)
	}
	requireRecordCount(t, p, "no-otel-record", 1)
}

// TestPythonRuntimeStructuredLoggingActiveSpanCorrelation pins OTel
// correlation: inside a traced invocation the active span's trace and span IDs
// win over the invocation frame's trace ID, while an untraced invocation falls
// back to the frame (asserted by the lazy-import sibling test).
func TestPythonRuntimeStructuredLoggingActiveSpanCorrelation(t *testing.T) {
	otelDir := requireOtelPython(t)
	requirePython(t)
	src := relayRuntimeSrc(t)
	requireSDKImport(t, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("trace-svc", level="INFO")


def emit(event, context):
    log.info("correlation-record")
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap,
		[]string{otelPythonEnv(otelDir, dir, src)})

	r := p.invokeFrame(t, "handler.emit", `{}`, nil, traceCarrier(traceOne, parentOne),
		map[string]any{
			"requestId":   "req-trace",
			"attempt":     1,
			"maxAttempts": 1,
			"deadline":    "2031-01-01T00:00:00Z",
			"traceId":     "4bf92f3577b34da6a3ce929d0e0e4736",
			"app":         "shop",
			"handler":     "handler.emit",
		})
	if !r.OK {
		t.Fatalf("traced invoke failed: %s", r.Err)
	}

	record := awaitJSONRecord(t, p, "correlation-record")
	requireCoreRecord(t, record, "trace-svc", "INFO")
	requireInvocationMetadata(t, record, "shop", "handler.emit", "req-trace", 1, true)
	if got := record["trace_id"]; got != traceOne {
		t.Errorf("record trace_id = %v, want the active span's %s", got, traceOne)
	}
	if got := record["span_id"]; got != parentOne {
		t.Errorf("record span_id = %v, want the active span's %s", got, parentOne)
	}
	requireRecordCount(t, p, "correlation-record", 1)
}

// TestPythonRuntimeStructuredLoggingWithoutActiveSpanOmitsCorrelation pins
// the API-present root behavior: with the real OTel API importable and no
// active span (no carrier extracted for this invocation), a record omits
// trace_id and span_id instead of pairing the invocation frame's trace ID
// with a span that does not exist.
func TestPythonRuntimeStructuredLoggingWithoutActiveSpanOmitsCorrelation(t *testing.T) {
	otelDir := requireOtelPython(t)
	requirePython(t)
	src := relayRuntimeSrc(t)
	requireSDKImport(t, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("no-span-svc", level="INFO")


def emit(event, context):
    log.info("no-span-record")
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap,
		[]string{otelPythonEnv(otelDir, dir, src)})

	// an invocation whose frame carries a VALID W3C trace id but whose
	// delivery carries no traceparent: the API is present, root is active
	r := p.invokeFrame(t, "handler.emit", `{}`, nil, nil, map[string]any{
		"requestId":   "req-no-span",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "2031-01-01T00:00:00Z",
		"traceId":     "4bf92f3577b34da6a3ce929d0e0e4736",
		"app":         "shop",
		"handler":     "handler.emit",
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	record := awaitJSONRecord(t, p, "no-span-record")
	requireCoreRecord(t, record, "no-span-svc", "INFO")
	requireInvocationMetadata(t, record, "shop", "handler.emit", "req-no-span", 1, true)
	if traceID, present := record["trace_id"]; present {
		t.Errorf("record trace_id = %#v, want omitted: the API is present with no active span",
			traceID)
	}
	if spanID, present := record["span_id"]; present {
		t.Errorf("record span_id = %#v, want omitted: the API is present with no active span",
			spanID)
	}
	requireRecordCount(t, p, "no-span-record", 1)
}

// TestPythonRuntimeStaleScopeExitFromEarlierInvocationIsNoop pins the stale
// scope-handle guard at the runtime layer: a temporary scope opened by an
// EARLIER (now expired) invocation and exited inside a LATER invocation must
// be a no-op — the current invocation's own temporary fields survive it.
func TestPythonRuntimeStaleScopeExitFromEarlierInvocationIsNoop(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("stale-scope-svc", level="INFO")
SAVED = []


def emit(event, context):
    if event.get("open"):
        manager = log.append_context_keys(tenant="stale")
        manager.__enter__()
        SAVED.append(manager)
        log.info("first-record")
        return
    with log.append_context_keys(tenant="current"):
        log.info("before-stale-record")
        SAVED.pop().__exit__(None, None, None)
        log.info("after-stale-record")
`)
	p := startPythonWithSDK(t, dir)

	frame := func(requestID string) map[string]any {
		return map[string]any{
			"requestId":   requestID,
			"attempt":     1,
			"maxAttempts": 3,
			"deadline":    "2031-01-01T00:00:00Z",
			"app":         "shop",
			"handler":     "handler.emit",
		}
	}

	// given a scope opened by the first invocation and left un-exited
	r1 := p.invokeFrame(t, "handler.emit", `{"open":true}`, nil, nil, frame("req-one"))
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	first := awaitJSONRecord(t, p, "first-record")
	requireInvocationMetadata(t, first, "shop", "handler.emit", "req-one", 1, true)
	if got := requireAttributes(t, first)["tenant"]; got != "stale" {
		t.Errorf("first record tenant = %v, want stale", got)
	}

	// when the second invocation exits that stale handle inside its own scope
	r2 := p.invokeFrame(t, "handler.emit", `{}`, nil, nil, frame("req-two"))
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	before := awaitJSONRecord(t, p, "before-stale-record")
	after := awaitJSONRecord(t, p, "after-stale-record")

	// then the stale exit is a no-op: the current scope's field survives it,
	// under the second invocation's own identity
	requireInvocationMetadata(t, before, "shop", "handler.emit", "req-two", 1, false)
	if got := requireAttributes(t, before)["tenant"]; got != "current" {
		t.Errorf("before-stale record tenant = %v, want current", got)
	}
	requireInvocationMetadata(t, after, "shop", "handler.emit", "req-two", 1, false)
	if got := requireAttributes(t, after)["tenant"]; got != "current" {
		t.Errorf("after-stale record tenant = %v, want current (stale exit must be a no-op)", got)
	}
	requireRecordCount(t, p, "after-stale-record", 1)
}

// TestPythonRuntimeStructuredLoggingTraceFallbackValidatesW3C pins the frame
// trace fallback: a frame traceId that is not a valid W3C trace id (32
// lowercase hex digits, non-zero) is never fabricated into a record, while a
// valid one is emitted (covered by the lazy-import sibling test).
func TestPythonRuntimeStructuredLoggingTraceFallbackValidatesW3C(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from relay_runtime.logging import Logger

log = Logger("trace-fallback-svc", level="INFO")


def emit(event, context):
    log.info("trace-fallback-record")
`)
	p := startPythonWithSDK(t, dir)

	r := p.invokeFrame(t, "handler.emit", `{}`, nil, nil, map[string]any{
		"requestId":   "req-bad-trace",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "2031-01-01T00:00:00Z",
		"traceId":     "not-a-w3c-trace-id",
		"app":         "shop",
		"handler":     "handler.emit",
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}

	record := awaitJSONRecord(t, p, "trace-fallback-record")
	requireCoreRecord(t, record, "trace-fallback-svc", "INFO")
	requireInvocationMetadata(t, record, "shop", "handler.emit", "req-bad-trace", 1, true)
	if traceID, present := record["trace_id"]; present {
		t.Errorf("record trace_id = %#v, want omitted for a non-W3C frame trace ID", traceID)
	}
	requireRecordCount(t, p, "trace-fallback-record", 1)
}

// TestPythonRuntimeStructuredLoggingChildTaskNeverEmitsStaleContext pins the
// expired-context contract: a child task created during an invocation inherits
// that invocation's logging context — including a propagated OTel parent
// attached for the invocation — then emits only after the invocation has
// completed and the NEXT warm invocation has begun. Its record must still
// emit normally (complete JSON with the required keys and its own extra
// field), but must carry no stale invocation metadata (request_id, app,
// handler, attempt, cold_start), no stale trace correlation (trace_id,
// span_id), and no stale appended or temporary context keys — and none of the
// second invocation's identity either. Synchronization is pure Event-based:
// the child task waits on an asyncio.Event set inside the second handler, so
// no sleep orders the test.
func TestPythonRuntimeStructuredLoggingChildTaskNeverEmitsStaleContext(t *testing.T) {
	otelDir := requireOtelPython(t)
	requirePython(t)
	src := relayRuntimeSrc(t)
	requireSDKImport(t, src)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

from relay_runtime.logging import Logger

log = Logger("stale-svc")

_release = asyncio.Event()
_tasks = []


async def _linger():
    await _release.wait()
    log.info("child-record", extra={"who": "child"})


async def spawn(event, context):
    log.append_keys(invocation_key="stale-appended")
    with log.append_context_keys(temp_key="stale-temporary"):
        log.info("parent-record", extra={"who": "parent"})
        _tasks.append(asyncio.create_task(_linger()))


async def release(event, context):
    with log.append_context_keys(inv2_temp="present"):
        log.info("inv2-record", extra={"who": "inv2"})
    _release.set()
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{otelPythonEnv(otelDir, dir, src)})

	frame := func(handler, requestID string) map[string]any {
		return map[string]any{
			"requestId":   requestID,
			"attempt":     1,
			"maxAttempts": 3,
			"deadline":    "2031-01-01T00:00:00Z",
			"app":         "shop",
			"handler":     handler,
		}
	}

	r1 := p.invokeFrame(t, "handler.spawn", `{}`, nil, traceCarrier(traceOne, parentOne),
		frame("handler.spawn", "req-one"))
	if !r1.OK {
		t.Fatalf("spawn invoke failed: %s", r1.Err)
	}
	parent := awaitJSONRecord(t, p, "parent-record")
	requireInvocationMetadata(t, parent, "shop", "handler.spawn", "req-one", 1, true)
	if got := parent["trace_id"]; got != traceOne {
		t.Errorf("parent record trace_id = %v, want the propagated parent's %s", got, traceOne)
	}
	if got := parent["span_id"]; got != parentOne {
		t.Errorf("parent record span_id = %v, want the propagated parent's %s", got, parentOne)
	}
	parentAttributes := requireAttributes(t, parent)
	if parentAttributes["invocation_key"] != "stale-appended" {
		t.Errorf("parent record appended key = %v, want stale-appended", parentAttributes["invocation_key"])
	}
	if parentAttributes["temp_key"] != "stale-temporary" {
		t.Errorf("parent record temporary key = %v, want stale-temporary", parentAttributes["temp_key"])
	}

	// The second invocation begins and releases the child task, which then
	// emits from its inherited — now expired — context.
	r2 := p.invokeFrame(t, "handler.release", `{}`, nil, nil, frame("handler.release", "req-two"))
	if !r2.OK {
		t.Fatalf("release invoke failed: %s", r2.Err)
	}
	inv2 := awaitJSONRecord(t, p, "inv2-record")
	requireInvocationMetadata(t, inv2, "shop", "handler.release", "req-two", 1, false)
	for _, key := range []string{"trace_id", "span_id"} {
		if value, present := inv2[key]; present {
			t.Errorf("untraced second invocation record must not carry %s, got %#v", key, value)
		}
	}
	if present := requireAttributes(t, inv2)["inv2_temp"]; present != "present" {
		t.Errorf("second invocation temporary key = %v, want present", present)
	}

	child := awaitJSONRecord(t, p, "child-record")
	requireCoreRecord(t, child, "stale-svc", "INFO")
	for _, key := range []string{
		"request_id", "app", "handler", "attempt", "cold_start", "trace_id", "span_id",
	} {
		if value, present := child[key]; present {
			t.Errorf("stale child record must not carry %s, got %#v (record: %v)", key, value, child)
		}
	}
	childAttributes, hasAttributes := child["attributes"].(map[string]any)
	if !hasAttributes {
		t.Errorf("child record carries no attributes object: %v", child)
	} else {
		if childAttributes["who"] != "child" {
			t.Errorf("child record extra who = %v, want child (record must emit normally)", childAttributes["who"])
		}
		for _, key := range []string{"invocation_key", "temp_key", "inv2_temp"} {
			if value, present := childAttributes[key]; present {
				t.Errorf("stale child record carries %s=%#v", key, value)
			}
		}
	}

	requireRecordCount(t, p, "parent-record", 1)
	requireRecordCount(t, p, "inv2-record", 1)
	requireRecordCount(t, p, "child-record", 1)
}

// TestPythonRuntimeStructuredLoggingChildSpanMatchesExportedSpan pins native
// child-span correlation: inside a real recording child span, the record's
// trace_id and span_id equal the ACTIVE child span's ids, and those ids match
// the span the managed provider actually exports — application logs and
// exported traces agree.
func TestPythonRuntimeStructuredLoggingChildSpanMatchesExportedSpan(t *testing.T) {
	otelDir := requireOtelPython(t)
	requirePython(t)
	src := relayRuntimeSrc(t)
	requireSDKImport(t, src)
	rec := newHTTPOTLPReceiver(t)

	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
from opentelemetry import trace

from relay_runtime.logging import Logger

log = Logger("child-span-svc")


def emit(event, context):
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("child-span") as span:
        span_context = span.get_span_context()
        log.info("child-span-record")
        print("child=%032x/%016x" % (span_context.trace_id, span_context.span_id))
`)
	p := startEmbeddedBootstrap(t, dir, Bootstrap, []string{
		otelPythonEnv(otelDir, dir, src),
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + rec.endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"OTEL_SERVICE_NAME=child-span-svc",
		"OTEL_BSP_SCHEDULE_DELAY=50",
	})

	r := p.invokeTrace(t, "handler.emit", `{}`, nil, traceCarrier(traceOne, parentOne))
	if !r.OK {
		t.Fatalf("traced invoke failed: %s", r.Err)
	}

	var childTrace, childSpan string
	for _, line := range strings.Split(r.Out, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "child=")
		if !ok {
			continue
		}
		childTrace, childSpan, ok = strings.Cut(rest, "/")
		if !ok || childTrace == "" || childSpan == "" {
			t.Fatalf("child id line %q is not <trace>/<span>, out = %q", rest, r.Out)
		}
	}
	if childTrace == "" {
		t.Fatalf("handler never printed its child span ids, out = %q", r.Out)
	}
	if childTrace != traceOne {
		t.Errorf("child span trace = %s, want the carrier's inherited %s", childTrace, traceOne)
	}
	if childSpan == parentOne {
		t.Errorf("child span id = %s, want a fresh child id instead of the parent's", childSpan)
	}

	record := awaitJSONRecord(t, p, "child-span-record")
	requireCoreRecord(t, record, "child-span-svc", "INFO")
	requireInvocationMetadata(t, record, "testapp", "handler.emit", "test-request", 1, true)
	if got := record["trace_id"]; got != childTrace {
		t.Errorf("record trace_id = %v, want the active child span's %s", got, childTrace)
	}
	if got := record["span_id"]; got != childSpan {
		t.Errorf("record span_id = %v, want the active child span's %s", got, childSpan)
	}
	requireRecordCount(t, p, "child-span-record", 1)

	closeAndWait(t, p, 15*time.Second)

	spans := rec.snapshot()
	matched := false
	for _, span := range spans {
		if span.Name != "child-span" {
			continue
		}
		matched = true
		if span.TraceID != childTrace || span.SpanID != childSpan {
			t.Errorf("exported span %s/%s does not match the logged child span %s/%s",
				span.TraceID, span.SpanID, childTrace, childSpan)
		}
	}
	if !matched {
		t.Errorf("no child-span was exported; spans: %+v", spans)
	}
}

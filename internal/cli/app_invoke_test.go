package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// fakeInvoker records manual-invocation calls for CLI tests, so `relay app
// invoke` can be exercised end to end through the real worker socket without
// Docker or a live runtime pool.
type fakeInvoker struct {
	mu      sync.Mutex
	calls   int
	name    string
	event   map[string]any
	invoked int
	err     error
}

func (f *fakeInvoker) InvokeApp(_ context.Context, name string, event map[string]any) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.name = name
	f.event = event
	return f.invoked, f.err
}

func (f *fakeInvoker) snapshot() (calls int, name string, event map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.name, f.event
}

// startTestSocketWithInvoker starts a real worker query socket at path with the
// given manual invoker, so the CLI's socket round trip is exercised end to end
// using the test's injected Dependencies.
func startTestSocketWithInvoker(t *testing.T, path string, invoker worker.AppInvoker) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetInvoker(invoker)
	t.Cleanup(func() { _ = s.Close() })
}

// startTestSocketWithReplayer starts a real worker query socket at path with the
// given DLQ replayer wired, so `relay dlq replay` can be exercised end to end
// against the live runner seam.
func startTestSocketWithReplayer(t *testing.T, path string, replayer worker.HandlerReplayer) {
	t.Helper()
	s, err := worker.NewSocketServer(
		path,
		fakePoolSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetReplayer(replayer)
	t.Cleanup(func() { _ = s.Close() })
}

// TestAppInvokeEventFlag verifies the inline --event form: the event object
// reaches the worker's invoker and the concise success line is written.
func TestAppInvokeEventFlag(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{"event_name":"INSERT"}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	calls, name, event := inv.snapshot()
	if calls != 1 || name != "fn" {
		t.Fatalf("invoker calls/name = %d/%q, want 1/fn", calls, name)
	}
	if event["event_name"] != "INSERT" {
		t.Fatalf("invoker event = %#v", event)
	}
}

// TestAppInvokePluralOutput verifies the exact plural success line for more
// than one handler.
func TestAppInvokePluralOutput(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 3})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 3 handlers" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 3 handlers")
	}
}

// TestAppInvokeNoMatch verifies the exact no-match line when the worker
// reports zero invoked handlers.
func TestAppInvokeNoMatch(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 0})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want %q", out, "No matching handlers")
	}
}

// TestAppInvokeStdin verifies the piped-stdin form when no flag is given.
func TestAppInvokeStdin(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, `{"event_name":"INSERT"}`, "app", "invoke", "fn")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	if _, _, event := inv.snapshot(); event["event_name"] != "INSERT" {
		t.Fatalf("invoker event = %#v", event)
	}
}

// TestAppInvokeStdinMultiline verifies a pretty-printed, multiline JSON object
// on stdin is read whole and decoded by the worker's invoker.
func TestAppInvokeStdinMultiline(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	stdin := "{\n  \"event_name\": \"INSERT\",\n  \"nested\": {\"id\": 9}\n}\n"
	out, _, err := runCLIWithDeps(t, deps, stdin, "app", "invoke", "fn")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	_, _, event := inv.snapshot()
	if event["event_name"] != "INSERT" {
		t.Fatalf("invoker event = %#v", event)
	}
	nested, ok := event["nested"].(map[string]any)
	if !ok || nested["id"] != float64(9) {
		t.Fatalf("invoker nested = %#v, want map with id 9", event["nested"])
	}
}

// TestAppInvokeInvalidStdinJSON verifies malformed JSON from stdin is reported
// locally as an invalid-event error (the same validation as --event) and never
// dials the worker.
func TestAppInvokeInvalidStdinJSON(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, `{"event_name":`, "app", "invoke", "fn")
	if err == nil || !strings.Contains(err.Error(), "app invoke: invalid event JSON") {
		t.Fatalf("err = %v, want invalid event JSON error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeEventFlagIgnoresNonemptyStdin verifies an explicit --event is
// authoritative: it succeeds and is used even when the injected stdin carries a
// different nonempty payload.
func TestAppInvokeEventFlagIgnoresNonemptyStdin(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, `{"from":"stdin"}`, "app", "invoke", "fn", "--event", `{"from":"flag"}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	calls, name, event := inv.snapshot()
	if calls != 1 || name != "fn" {
		t.Fatalf("invoker calls/name = %d/%q, want 1/fn", calls, name)
	}
	if event["from"] != "flag" {
		t.Fatalf("invoker event = %#v, want the explicit --event payload", event)
	}
}

// TestResolveInvokeEventEventFlagDoesNotReadBlockingStdin verifies the
// authoritative --event path never touches a non-terminal stdin that would block
// forever, so an explicit event cannot hang on a bridged terminal stream.
func TestResolveInvokeEventEventFlagDoesNotReadBlockingStdin(t *testing.T) {
	raw, err := resolveInvokeEvent(`{"from":"flag"}`, true, blockingReader{}, false)
	if err != nil {
		t.Fatalf("resolveInvokeEvent: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil || got["from"] != "flag" {
		t.Fatalf("raw = %q, decode err = %v (got %#v)", raw, err, got)
	}
}

// TestAppInvokeEventFlagWithBlankStdin verifies whitespace-only piped stdin is
// not treated as a competing source: the explicit --event is used.
func TestAppInvokeEventFlagWithBlankStdin(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, "  \n", "app", "invoke", "fn", "--event", `{"from":"flag"}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	if _, _, event := inv.snapshot(); event["from"] != "flag" {
		t.Fatalf("invoker event = %#v, want the explicit --event payload", event)
	}
}

// TestAppInvokeEmptyEventFlagNoFallback verifies an explicitly empty --event is
// a usage error even when stdin carries a valid payload: the CLI must not fall
// back silently to stdin.
func TestAppInvokeEmptyEventFlagNoFallback(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, `{"a":1}`, "app", "invoke", "fn", "--event", "")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeFileFlagUnknown verifies the removed --file flag is rejected as
// an unknown flag and no legacy file path is honored.
func TestAppInvokeFileFlagUnknown(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--file", "event.json")
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") || !strings.Contains(err.Error(), "file") {
		t.Fatalf("err = %v, want an unknown --file flag error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeInvalidJSON verifies malformed inline JSON is rejected
// locally with a clear error and never dials the worker.
func TestAppInvokeInvalidJSON(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{not json`)
	if err == nil || !strings.Contains(err.Error(), "app invoke: invalid event JSON") {
		t.Fatalf("err = %v, want invalid event JSON error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeNonObjectJSONRejected verifies arrays, scalars, and null are
// rejected identically from --event and stdin: the matcher is defined over a
// JSON object. Null has a specific message so it is distinguishable from a
// mere syntax error.
func TestAppInvokeNonObjectJSONRejected(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	payloads := []struct {
		name    string
		payload string
		want    string
	}{
		{"array", `[1,2]`, "must be a JSON object"},
		{"string", `"str"`, "must be a JSON object"},
		{"number", `42`, "must be a JSON object"},
		{"bool", `true`, "must be a JSON object"},
		{"null", `null`, "must be a JSON object, not null"},
	}
	for _, p := range payloads {
		for _, source := range []string{"event", "stdin"} {
			t.Run(p.name+"/"+source, func(t *testing.T) {
				args := []string{"app", "invoke", "fn"}
				stdin := ""
				if source == "event" {
					args = append(args, "--event", p.payload)
				} else {
					stdin = p.payload
				}
				_, _, err := runCLIWithDeps(t, deps, stdin, args...)
				if err == nil || !strings.Contains(err.Error(), p.want) {
					t.Fatalf("payload %q via %s: err = %v, want %q", p.payload, source, err, p.want)
				}
			})
		}
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeNoPayload verifies a whitespace-only stdin with no flag is a
// usage error with guidance, not a silent success.
func TestAppInvokeNoPayload(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	// A non-empty stdin forces the test's reader (rather than os.Stdin), so the
	// trim-to-empty path is exercised deterministically.
	_, _, err := runCLIWithDeps(t, deps, "   \n", "app", "invoke", "fn")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestAppInvokeNoEventSource verifies a command with no --event and no stdin
// payload fails with the usage guidance (naming both accepted sources) and
// never dials the worker, without blocking for terminal input.
func TestAppInvokeNoEventSource(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	for _, want := range []string{"--event", "stdin"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want guidance mentioning %q", err, want)
		}
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestResolveInvokeEventInteractiveTerminalFailsFast verifies an interactive
// stdin with no payload fails fast with guidance instead of reading (and
// blocking on) a typed line.
func TestResolveInvokeEventInteractiveTerminalFailsFast(t *testing.T) {
	// A blocking reader would hang forever if the terminal guard were absent.
	_, err := resolveInvokeEvent("", false, blockingReader{}, true)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventTerminalNotReadWithEvent verifies an interactive
// terminal is never read when an explicit --event is present.
func TestResolveInvokeEventTerminalNotReadWithEvent(t *testing.T) {
	raw, err := resolveInvokeEvent(`{"event_name":"INSERT"}`, true, blockingReader{}, true)
	if err != nil {
		t.Fatalf("resolveInvokeEvent: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil || got["event_name"] != "INSERT" {
		t.Fatalf("raw = %q, decode err = %v (got %#v)", raw, err, got)
	}
}

// TestResolveInvokeEventNoReader verifies a nil (unavailable) stdin without
// --event is the no-payload usage error rather than a panic or a read.
func TestResolveInvokeEventNoReader(t *testing.T) {
	_, err := resolveInvokeEvent("", false, nil, false)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// blockingReader blocks forever on Read, so a test proves the interactive guard
// short-circuits before touching stdin.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) {
	select {} // block forever; the test would time out if Read were reached
}

// TestAppInvokeUnknownApp verifies the worker's unknown-app
// answer is surfaced as an error naming the app, so the operator sees the
// real cause.
func TestAppInvokeUnknownApp(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{err: fmt.Errorf("%w: %q", runner.ErrAppNotFound, "ghost")}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "ghost", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "app invoke") {
		t.Fatalf("err = %v, want an app invoke error", err)
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err = %v, want it to name the unknown app", err)
	}
}

// TestAppInvokeWorkerUnavailable verifies a missing worker socket is a hard
// error (there is no offline fallback for a manual invocation) and the error is
// clear.
func TestAppInvokeWorkerUnavailable(t *testing.T) {
	_, deps := openTempState(t)
	// No socket server started at deps.SocketPath.

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "app invoke") {
		t.Fatalf("err = %v, want an app invoke error", err)
	}
}

// TestAppInvokeTooManyArgs verifies extra positional arguments are a usage
// error.
func TestAppInvokeTooManyArgs(t *testing.T) {
	_, deps := openTempState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "extra", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "app invoke: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestAppInvokeMissingName verifies the required NAME argument is enforced.
func TestAppInvokeMissingName(t *testing.T) {
	_, deps := openTempState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "--event", `{}`)
	if err == nil {
		t.Fatal("err = nil, want a missing-argument error")
	}
}

// TestAppInvokeHelp verifies `relay app invoke --help` renders the
// usage line and the flag names, and does not dial the worker.
func TestAppInvokeHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "app", "invoke", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"relay app invoke NAME", "--event", "stdin"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "--file") {
		t.Fatalf("help still advertises the removed --file flag:\n%s", out)
	}
}

// TestAppHelpListsInvoke verifies the invoke subcommand is listed in the
// app help (and therefore the command is registered in the family).
func TestAppHelpListsInvoke(t *testing.T) {
	out, _, err := runCLI(t, "", "app", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "invoke") {
		t.Fatalf("function help must list invoke:\n%s", out)
	}
}

// TestAppInvokeDoesNotTouchStateDB verifies invoke never opens the state
// database: with a state path pointing at a directory (an unopenable DB) the
// command still succeeds through the socket, proving the state DB is not a
// dependency of the invoke path.
func TestAppInvokeDoesNotTouchStateDB(t *testing.T) {
	deps := testDeps(t)
	// Make opening the state database fail loudly: invoke must never call it.
	deps.OpenState = func(context.Context) (*state.State, func(), error) {
		return nil, nil, errors.New("state DB must not be opened by invoke")
	}
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil (invoke must not open the state DB)", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
}

// TestResolveInvokeEventExplicitEmptyFlag verifies an explicitly empty --event
// value (as opposed to an omitted flag) is a no-payload error rather than a
// silent fall-through to stdin.
func TestResolveInvokeEventExplicitEmptyFlag(t *testing.T) {
	_, err := resolveInvokeEvent("", true, strings.NewReader(`{"a":1}`), false)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventStdinPassthrough verifies the stdin path forwards the
// raw object.
func TestResolveInvokeEventStdinPassthrough(t *testing.T) {
	raw, err := resolveInvokeEvent("", false, strings.NewReader(`{"a":1}`), false)
	if err != nil {
		t.Fatalf("resolveInvokeEvent: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil || got["a"] != float64(1) {
		t.Fatalf("raw = %q, decode err = %v (got %#v)", raw, err, got)
	}
}

// TestStdinIsTerminalFalseForNonFile verifies the terminal check is false for a
// non-*os.File reader, so tests and embedding hosts never take the interactive
// branch.
func TestStdinIsTerminalFalseForNonFile(t *testing.T) {
	if stdinIsTerminal(strings.NewReader("")) {
		t.Fatal("a strings.Reader must never be treated as an interactive terminal")
	}
	if stdinIsTerminal(nil) {
		t.Fatal("nil reader must never be treated as an interactive terminal")
	}
}

// TestAppInvokeNoLogOnWriter verifies the command writer carries only the
// concise result line — no internal details or log output.
func TestAppInvokeNoLogOnWriter(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, stderr, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if out != "Invoked 1 handler\n" {
		t.Fatalf("stdout = %q, want exactly the success line", out)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// TestInvokerSatisfiesWorkerInterface is a compile-time-style assertion that the
// CLI test fake matches the worker's seam, so the socket wiring stays honest.
var _ worker.AppInvoker = (*fakeInvoker)(nil)

// cliTestExecutor is a minimal runner.Executor for the full-stack CLI test.
type cliTestExecutor struct {
	mu       sync.Mutex
	handlers []string
}

func (e *cliTestExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, _ []byte, _ []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	return nil
}

func (e *cliTestExecutor) got() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.handlers...)
}

// TestAppInvokeFullStack drives the real runner through the real worker
// socket via the CLI command, proving the whole stack end to end without Docker:
// a matching event invokes the rule's handler and prints the count, a
// non-matching event prints "No matching handlers", and an unknown app
// surfaces the worker's error.
func TestAppInvokeFullStack(t *testing.T) {
	_, deps := openTempState(t)
	exec := &cliTestExecutor{}
	tmpl, err := app.ParseTemplate([]byte(`runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	prepared := runner.NewPrepared(
		app.App{Name: "user-events", Template: tmpl},
		&runtime.Prepared{Name: "user-events", Image: "x"},
		exec,
	)
	run := runner.New([]*runner.PreparedApp{prepared}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	startTestSocketWithInvoker(t, deps.SocketPath, run)

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "user-events", "--event", `{"event_name":"INSERT"}`)
	if err != nil {
		t.Fatalf("matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	if got := exec.got(); len(got) != 1 || got[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v", got)
	}

	// The stdin form must reach the same runner through the same socket.
	out, _, err = runCLIWithDeps(t, deps, "{\n  \"event_name\": \"INSERT\"\n}\n", "app", "invoke", "user-events")
	if err != nil {
		t.Fatalf("stdin matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	if got := exec.got(); len(got) != 2 || got[1] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want the handler to run again via stdin", got)
	}

	out, _, err = runCLIWithDeps(t, deps, `{"event_name":"MODIFY"}`, "app", "invoke", "user-events")
	if err != nil {
		t.Fatalf("non-matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want no-match line", out)
	}

	_, _, err = runCLIWithDeps(t, deps, "", "app", "invoke", "ghost", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unknown function err = %v, want it to name ghost", err)
	}
}

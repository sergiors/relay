package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/worker"
)

// fakeInvoker records manual-invocation calls for CLI tests, so `relay function
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

func (f *fakeInvoker) InvokeFunction(_ context.Context, name string, event map[string]any) (int, error) {
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
func startTestSocketWithInvoker(t *testing.T, path string, invoker worker.FunctionInvoker) {
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

// TestFunctionInvokeEventFlag verifies the inline --event form: the event object
// reaches the worker's invoker and the concise success line is written.
func TestFunctionInvokeEventFlag(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{"event_name":"INSERT"}`)
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

// TestFunctionInvokePluralOutput verifies the exact plural success line for more
// than one handler.
func TestFunctionInvokePluralOutput(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 3})

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 3 handlers" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 3 handlers")
	}
}

// TestFunctionInvokeNoMatch verifies the exact no-match line when the worker
// reports zero invoked handlers.
func TestFunctionInvokeNoMatch(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 0})

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want %q", out, "No matching handlers")
	}
}

// TestFunctionInvokeFileFlag verifies the --file form reads and validates a JSON
// object from disk.
func TestFunctionInvokeFileFlag(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	path := filepath.Join(t.TempDir(), "event.json")
	if err := os.WriteFile(path, []byte(`{"event_name":"MODIFY","id":9}`), 0o600); err != nil {
		t.Fatalf("write event file: %v", err)
	}

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--file", path)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	_, _, event := inv.snapshot()
	if event["event_name"] != "MODIFY" {
		t.Fatalf("invoker event = %#v", event)
	}
}

// TestFunctionInvokeStdin verifies the piped-stdin form when neither flag is
// given.
func TestFunctionInvokeStdin(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, `{"event_name":"INSERT"}`, "function", "invoke", "fn")
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

// TestFunctionInvokeBothFlagsRejected verifies --event and --file together are a
// usage error and nothing is sent to the worker.
func TestFunctionInvokeBothFlagsRejected(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`, "--file", "x.json")
	if err == nil || !strings.Contains(err.Error(), "--event and --file are mutually exclusive") {
		t.Fatalf("err = %v, want mutual-exclusion error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestFunctionInvokeInvalidJSON verifies malformed inline JSON is rejected
// locally with a clear error and never dials the worker.
func TestFunctionInvokeInvalidJSON(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{not json`)
	if err == nil || !strings.Contains(err.Error(), "must be a JSON object") {
		t.Fatalf("err = %v, want JSON-object error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestFunctionInvokeNonObjectJSONRejected verifies arrays, scalars, and null are
// rejected: the matcher is defined over a JSON object.
func TestFunctionInvokeNonObjectJSONRejected(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	for _, payload := range []string{`[1,2]`, `"str"`, `42`, `true`, `null`} {
		_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", payload)
		if err == nil || !strings.Contains(err.Error(), "must be a JSON object") {
			t.Fatalf("payload %q: err = %v, want JSON-object error", payload, err)
		}
	}
}

// TestFunctionInvokeNoPayload verifies a whitespace-only stdin with neither flag
// is a usage error with guidance, not a silent success.
func TestFunctionInvokeNoPayload(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	// A non-empty stdin forces the test's reader (rather than os.Stdin), so the
	// trim-to-empty path is exercised deterministically.
	_, _, err := runCLIWithDeps(t, deps, "   \n", "function", "invoke", "fn")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventInteractiveTerminalFailsFast verifies an interactive
// stdin with no payload fails fast with guidance instead of reading (and
// blocking on) a typed line.
func TestResolveInvokeEventInteractiveTerminalFailsFast(t *testing.T) {
	// A blocking reader would hang forever if the terminal guard were absent.
	_, err := resolveInvokeEvent("", false, "", false, blockingReader{}, true)
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

// TestFunctionInvokeUnknownFunction verifies the worker's unknown-function
// answer is surfaced as an error naming the function, so the operator sees the
// real cause.
func TestFunctionInvokeUnknownFunction(t *testing.T) {
	_, deps := openTempState(t)
	inv := &fakeInvoker{err: fmt.Errorf("%w: %q", runner.ErrFunctionNotFound, "ghost")}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "ghost", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "function invoke") {
		t.Fatalf("err = %v, want a function invoke error", err)
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("err = %v, want it to name the unknown function", err)
	}
}

// TestFunctionInvokeWorkerUnavailable verifies a missing worker socket is a hard
// error (there is no offline fallback for a manual invocation) and the error is
// clear.
func TestFunctionInvokeWorkerUnavailable(t *testing.T) {
	_, deps := openTempState(t)
	// No socket server started at deps.SocketPath.

	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "function invoke") {
		t.Fatalf("err = %v, want a function invoke error", err)
	}
}

// TestFunctionInvokeTooManyArgs verifies extra positional arguments are a usage
// error.
func TestFunctionInvokeTooManyArgs(t *testing.T) {
	_, deps := openTempState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "extra", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "function invoke: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
}

// TestFunctionInvokeMissingName verifies the required NAME argument is enforced.
func TestFunctionInvokeMissingName(t *testing.T) {
	_, deps := openTempState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "--event", `{}`)
	if err == nil {
		t.Fatal("err = nil, want a missing-argument error")
	}
}

// TestFunctionInvokeHelp verifies `relay function invoke --help` renders the
// usage line and the flag names, and does not dial the worker.
func TestFunctionInvokeHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "function", "invoke", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"relay function invoke NAME", "--event", "--file"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help missing %q:\n%s", want, out)
		}
	}
}

// TestFunctionHelpListsInvoke verifies the invoke subcommand is listed in the
// function help (and therefore the command is registered in the family).
func TestFunctionHelpListsInvoke(t *testing.T) {
	out, _, err := runCLI(t, "", "function", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "invoke") {
		t.Fatalf("function help must list invoke:\n%s", out)
	}
}

// TestFunctionInvokeDoesNotTouchStateDB verifies invoke never opens the state
// database: with a state path pointing at a directory (an unopenable DB) the
// command still succeeds through the socket, proving the state DB is not a
// dependency of the invoke path.
func TestFunctionInvokeDoesNotTouchStateDB(t *testing.T) {
	deps := testDeps(t)
	// Point StatePath at a directory: opening it as a SQLite DB would fail.
	dir := t.TempDir()
	deps.StatePath = dir
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil (invoke must not open the state DB)", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
}

// TestResolveInvokeEventRejectsEmptyFile verifies an empty file is not a valid
// event object.
func TestResolveInvokeEventRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := resolveInvokeEvent("", false, path, true, strings.NewReader(""), false)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventUnreadableFile verifies a missing file is reported
// clearly.
func TestResolveInvokeEventUnreadableFile(t *testing.T) {
	_, err := resolveInvokeEvent("", false, filepath.Join(t.TempDir(), "missing.json"), true, strings.NewReader(""), false)
	if err == nil || !strings.Contains(err.Error(), "read --file") {
		t.Fatalf("err = %v, want read-file error", err)
	}
}

// TestResolveInvokeEventExplicitEmptyFlag verifies an explicitly empty --event
// value (as opposed to an omitted flag) is a no-payload error rather than a
// silent fall-through to stdin.
func TestResolveInvokeEventExplicitEmptyFlag(t *testing.T) {
	_, err := resolveInvokeEvent("", true, "", false, strings.NewReader(`{"a":1}`), false)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventStdinPassthrough verifies the stdin path forwards the
// raw object.
func TestResolveInvokeEventStdinPassthrough(t *testing.T) {
	raw, err := resolveInvokeEvent("", false, "", false, strings.NewReader(`{"a":1}`), false)
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

// TestFunctionInvokeNoLogOnWriter verifies the command writer carries only the
// concise result line — no internal details or log output.
func TestFunctionInvokeNoLogOnWriter(t *testing.T) {
	_, deps := openTempState(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, stderr, err := runCLIWithDeps(t, deps, "", "function", "invoke", "fn", "--event", `{}`)
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
var _ worker.FunctionInvoker = (*fakeInvoker)(nil)

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

// TestFunctionInvokeFullStack drives the real runner through the real worker
// socket via the CLI command, proving the whole stack end to end without Docker:
// a matching event invokes the rule's handler and prints the count, a
// non-matching event prints "No matching handlers", and an unknown function
// surfaces the worker's error.
func TestFunctionInvokeFullStack(t *testing.T) {
	_, deps := openTempState(t)
	exec := &cliTestExecutor{}
	tmpl, err := function.ParseTemplate([]byte(`runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	prepared := runner.NewPrepared(
		function.Function{Name: "user-events", Template: tmpl},
		&runtime.Prepared{Name: "user-events", Image: "x"},
		exec,
	)
	run := runner.New([]*runner.PreparedFunction{prepared}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	startTestSocketWithInvoker(t, deps.SocketPath, run)

	out, _, err := runCLIWithDeps(t, deps, "", "function", "invoke", "user-events", "--event", `{"event_name":"INSERT"}`)
	if err != nil {
		t.Fatalf("matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	if got := exec.got(); len(got) != 1 || got[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v", got)
	}

	out, _, err = runCLIWithDeps(t, deps, "", "function", "invoke", "user-events", "--event", `{"event_name":"MODIFY"}`)
	if err != nil {
		t.Fatalf("non-matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want no-match line", out)
	}

	_, _, err = runCLIWithDeps(t, deps, "", "function", "invoke", "ghost", "--event", `{}`)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unknown function err = %v, want it to name ghost", err)
	}
}

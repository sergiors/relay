package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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

// invokeDeps returns the minimal dependencies for `app invoke`, whose only
// seam is the worker socket. The state opener fails loudly with an
// invoke-specific message so an accidental database dependency is caught, and
// the socket lives under /tmp to stay inside the Unix sun_path limit. Like the
// default testDeps, it never needs PostgreSQL, so the focused invoke tests
// always run.
func invokeDeps(t *testing.T) Dependencies {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "relay-cli-sock-")
	if err != nil {
		t.Fatalf("short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	return Dependencies{
		SocketPath: filepath.Join(sockDir, "relay.sock"),
		OpenState: func(context.Context) (*state.State, func(), error) {
			return nil, nil, errors.New("app invoke must not open the state DB")
		},
	}
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

// trackingReader records reads so a test can prove stdin was never touched.
// A read must never be needed to prove absence of an implicit source.
type trackingReader struct {
	reads int
}

func (r *trackingReader) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

// TestAppInvokeInlineJSON verifies the positional JSON form: the event object
// reaches the worker's invoker and the concise success line is written.
func TestAppInvokeInlineJSON(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{"event_name":"INSERT"}`)
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
	deps := invokeDeps(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 3})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{}`)
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
	deps := invokeDeps(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 0})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want %q", out, "No matching handlers")
	}
}

// TestAppInvokeStdinMarker verifies `NAME -` explicitly selects stdin and the
// payload is forwarded to the worker.
func TestAppInvokeStdinMarker(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, `{"event_name":"INSERT"}`, "app", "invoke", "fn", "-")
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
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	stdin := "{\n  \"event_name\": \"INSERT\",\n  \"nested\": {\"id\": 9}\n}\n"
	out, _, err := runCLIWithDeps(t, deps, stdin, "app", "invoke", "fn", "-")
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
// locally as an invalid-event error and never dials the worker.
func TestAppInvokeInvalidStdinJSON(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, `{"event_name":`, "app", "invoke", "fn", "-")
	if err == nil || !strings.Contains(err.Error(), "app invoke: invalid event JSON") {
		t.Fatalf("err = %v, want invalid event JSON error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeInlineIgnoresNonemptyStdin verifies inline JSON is authoritative:
// it succeeds and is used even when stdin carries a different payload, because
// only `-` selects stdin.
func TestAppInvokeInlineIgnoresNonemptyStdin(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, `{"from":"stdin"}`,
		"app", "invoke", "fn", `{"from":"argument"}`)
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
	if event["from"] != "argument" {
		t.Fatalf("invoker event = %#v, want the positional payload", event)
	}
}

// TestResolveInvokeEventInlineDoesNotReadStdin verifies an inline positional
// event never touches the injected reader.
func TestResolveInvokeEventInlineDoesNotReadStdin(t *testing.T) {
	stdin := &trackingReader{}
	raw, err := resolveInvokeEvent([]string{`{"from":"argument"}`}, stdin)
	if err != nil {
		t.Fatalf("resolveInvokeEvent: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil || got["from"] != "argument" {
		t.Fatalf("raw = %q, decode err = %v (got %#v)", raw, err, got)
	}
	if stdin.reads != 0 {
		t.Fatalf("stdin reads = %d, want 0 for an inline event", stdin.reads)
	}
}

// TestAppInvokeInlineWithBlankStdin verifies whitespace-only piped stdin is not
// a competing source: the positional event is used.
func TestAppInvokeInlineWithBlankStdin(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	out, _, err := runCLIWithDeps(t, deps, "  \n", "app", "invoke", "fn", `{"from":"argument"}`)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
	if _, _, event := inv.snapshot(); event["from"] != "argument" {
		t.Fatalf("invoker event = %#v, want the positional payload", event)
	}
}

// TestAppInvokeEmptyInlineNoFallback verifies an explicitly empty positional
// event is a usage error even when stdin carries a valid payload: stdin is
// never an implicit fallback.
func TestAppInvokeEmptyInlineNoFallback(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, `{"a":1}`, "app", "invoke", "fn", "")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeEventFlagRemoved verifies the removed --event flag is rejected
// as an unknown flag when it appears before NAME, and no legacy event path is
// honored.
func TestAppInvokeEventFlagRemoved(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "--event", `{"a":1}`, "fn")
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") ||
		!strings.Contains(err.Error(), "event") {
		t.Fatalf("err = %v, want an unknown --event flag error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeEventFlagAfterNameRejected verifies a trailing --event is not
// silently swallowed after NAME: parsing stops at the first positional, so the
// flag text and its value are extra arguments.
func TestAppInvokeEventFlagAfterNameRejected(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", "--event", `{"a":1}`)
	if err == nil || !strings.Contains(err.Error(), "app invoke: too many arguments") {
		t.Fatalf("err = %v, want too-many-arguments", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeFileFlagUnknown verifies the removed --file flag is rejected as
// an unknown flag and no legacy file path is honored.
func TestAppInvokeFileFlagUnknown(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "--file", "event.json", "fn")
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") ||
		!strings.Contains(err.Error(), "file") {
		t.Fatalf("err = %v, want an unknown --file flag error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeInvalidJSON verifies malformed inline JSON is rejected locally
// with a clear error and never dials the worker.
func TestAppInvokeInvalidJSON(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{not json`)
	if err == nil || !strings.Contains(err.Error(), "app invoke: invalid event JSON") {
		t.Fatalf("err = %v, want invalid event JSON error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeNonObjectJSONRejected verifies arrays, scalars, and null are
// rejected identically from the inline argument and stdin: the matcher is
// defined over a JSON object. Null has a specific message so it is
// distinguishable from a mere syntax error.
func TestAppInvokeNonObjectJSONRejected(t *testing.T) {
	deps := invokeDeps(t)
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
		for _, source := range []string{"inline", "stdin"} {
			t.Run(p.name+"/"+source, func(t *testing.T) {
				args := []string{"app", "invoke", "fn"}
				stdin := ""
				if source == "inline" {
					args = append(args, p.payload)
				} else {
					args = append(args, "-")
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

// TestAppInvokeBareNameIgnoresPipedStdin verifies a bare NAME never reads
// stdin: even a valid piped payload is ignored and the command is a usage
// error naming both accepted forms.
func TestAppInvokeBareNameIgnoresPipedStdin(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, `{"a":1}`, "app", "invoke", "fn")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	for _, want := range []string{"JSON object", "-"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want guidance mentioning %q", err, want)
		}
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestResolveInvokeEventBareNameDoesNotReadStdin verifies the bare-name path
// never dereferences the reader: a tracked reader must record zero reads.
func TestResolveInvokeEventBareNameDoesNotReadStdin(t *testing.T) {
	for _, args := range [][]string{nil, {}} {
		stdin := &trackingReader{}
		_, err := resolveInvokeEvent(args, stdin)
		if err == nil || !strings.Contains(err.Error(), "no event provided") {
			t.Fatalf("args %v: err = %v, want no-event error", args, err)
		}
		if stdin.reads != 0 {
			t.Fatalf("args %v: stdin reads = %d, want 0", args, stdin.reads)
		}
	}
}

// TestResolveInvokeEventStdinMarker verifies `-` reads the raw object from the
// supplied reader.
func TestResolveInvokeEventStdinMarker(t *testing.T) {
	raw, err := resolveInvokeEvent([]string{"-"}, strings.NewReader(`{"a":1}`))
	if err != nil {
		t.Fatalf("resolveInvokeEvent: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil || got["a"] != float64(1) {
		t.Fatalf("raw = %q, decode err = %v (got %#v)", raw, err, got)
	}
}

// TestResolveInvokeEventStdinMarkerNoReader verifies `-` with no available
// reader is the no-payload usage error rather than a panic.
func TestResolveInvokeEventStdinMarkerNoReader(t *testing.T) {
	_, err := resolveInvokeEvent([]string{"-"}, nil)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
}

// TestResolveInvokeEventStdinMarkerEmpty verifies an empty or whitespace-only
// stdin payload is the no-payload usage error.
func TestResolveInvokeEventStdinMarkerEmpty(t *testing.T) {
	for _, stdin := range []string{"", "   \n\t"} {
		_, err := resolveInvokeEvent([]string{"-"}, strings.NewReader(stdin))
		if err == nil || !strings.Contains(err.Error(), "no event provided") {
			t.Fatalf("stdin %q: err = %v, want no-event error", stdin, err)
		}
	}
}

// TestResolveInvokeEventStdinMarkerReadError verifies a stdin read failure is
// reported as a wrapped read error and never parsed.
func TestResolveInvokeEventStdinMarkerReadError(t *testing.T) {
	_, err := resolveInvokeEvent([]string{"-"}, errReader{data: `{"a":1}`})
	if err == nil || !strings.Contains(err.Error(), "app invoke: read stdin") {
		t.Fatalf("err = %v, want a stdin read error", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the underlying read error", err)
	}
}

// TestAppInvokeStdinMarkerEmptyPayload verifies `NAME -` with no payload on
// stdin is a usage error, not a silent success.
func TestAppInvokeStdinMarkerEmptyPayload(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "   \n", "app", "invoke", "fn", "-")
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeWhitespaceInline verifies a whitespace-only inline argument is
// the no-payload usage error.
func TestAppInvokeWhitespaceInline(t *testing.T) {
	stdin := &trackingReader{}
	_, err := resolveInvokeEvent([]string{"  \n"}, stdin)
	if err == nil || !strings.Contains(err.Error(), "no event provided") {
		t.Fatalf("err = %v, want no-event error", err)
	}
	if stdin.reads != 0 {
		t.Fatalf("stdin reads = %d, want 0", stdin.reads)
	}
}

// TestAppInvokeStdinMarkerReadError verifies a failing stdin read through the
// full command path is reported as a read error and never dials the worker.
func TestAppInvokeStdinMarkerReadError(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{invoked: 1}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	var out, errOut bytes.Buffer
	cmd := New(slog.New(slog.NewTextHandler(io.Discard, nil)), &out, deps)
	cmd.Reader = errReader{data: `{"a":1}`}
	cmd.ErrWriter = &errOut
	err := cmd.Run(t.Context(), []string{"relay", "app", "invoke", "fn", "-"})
	if err == nil || !strings.Contains(err.Error(), "app invoke: read stdin") {
		t.Fatalf("err = %v, want a stdin read error", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the underlying read error", err)
	}
	if out.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", out.String())
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0", calls)
	}
}

// TestAppInvokeUnknownApp verifies the worker's unknown-app answer is surfaced
// as an error naming the app, so the operator sees the real cause.
func TestAppInvokeUnknownApp(t *testing.T) {
	deps := invokeDeps(t)
	inv := &fakeInvoker{err: fmt.Errorf("%w: %q", runner.ErrAppNotFound, "ghost")}
	startTestSocketWithInvoker(t, deps.SocketPath, inv)

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "ghost", `{}`)
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
	deps := invokeDeps(t)
	// No socket server started at deps.SocketPath.

	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{}`)
	if err == nil || !strings.Contains(err.Error(), "app invoke") {
		t.Fatalf("err = %v, want an app invoke error", err)
	}
}

// TestAppInvokeTooManyArgs verifies extra positional arguments are a usage
// error. Parsing stops at NAME, so trailing tokens are never mistaken for
// flags.
func TestAppInvokeTooManyArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"inline plus extra", []string{"fn", `{}`, "extra"}},
		{"stdin marker plus extra", []string{"fn", "-", "extra"}},
		{"trailing legacy flag", []string{"fn", "--event", `{}`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := invokeDeps(t)
			inv := &fakeInvoker{invoked: 1}
			startTestSocketWithInvoker(t, deps.SocketPath, inv)

			args := append([]string{"app", "invoke"}, tc.args...)
			_, _, err := runCLIWithDeps(t, deps, `{"a":1}`, args...)
			if err == nil || !strings.Contains(err.Error(), "app invoke: too many arguments") {
				t.Fatalf("err = %v, want too-many-arguments", err)
			}
			if calls, _, _ := inv.snapshot(); calls != 0 {
				t.Fatalf("invoker calls = %d, want 0", calls)
			}
		})
	}
}

// TestAppInvokeMissingName verifies the required NAME argument is enforced.
func TestAppInvokeMissingName(t *testing.T) {
	deps := invokeDeps(t)
	_, _, err := runCLIWithDeps(t, deps, "", "app", "invoke")
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("err = %v, want a missing-NAME error", err)
	}
}

// TestAppInvokeHelp verifies `relay app invoke --help` renders the positional
// usage and stdin guidance, and advertises neither the removed --event nor
// --file flags.
func TestAppInvokeHelp(t *testing.T) {
	out, _, err := runCLIWithDeps(t, invokeDeps(t), "", "app", "invoke", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"relay app invoke NAME [JSON|-]", "stdin", "JSON object"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help missing %q:\n%s", want, out)
		}
	}
	for _, removed := range []string{"--event", "--file"} {
		if strings.Contains(out, removed) {
			t.Fatalf("help still advertises the removed %s flag:\n%s", removed, out)
		}
	}
}

// TestAppHelpListsInvoke verifies the invoke subcommand is listed in the app
// help (and therefore the command is registered in the family).
func TestAppHelpListsInvoke(t *testing.T) {
	out, _, err := runCLIWithDeps(t, invokeDeps(t), "", "app", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.Contains(out, "invoke") {
		t.Fatalf("function help must list invoke:\n%s", out)
	}
}

// TestAppInvokeDoesNotTouchStateDB verifies invoke never opens the state
// database: the injected opener fails loudly, yet the command succeeds through
// the socket.
func TestAppInvokeDoesNotTouchStateDB(t *testing.T) {
	deps := invokeDeps(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, _, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{}`)
	if err != nil {
		t.Fatalf("err = %v, want nil (invoke must not open the state DB)", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q", out)
	}
}

// TestAppInvokeNoLogOnWriter verifies the command writer carries only the
// concise result line: no internal details or log output.
func TestAppInvokeNoLogOnWriter(t *testing.T) {
	deps := invokeDeps(t)
	startTestSocketWithInvoker(t, deps.SocketPath, &fakeInvoker{invoked: 1})

	out, stderr, err := runCLIWithDeps(t, deps, "", "app", "invoke", "fn", `{}`)
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

// TestAppInvokeFullStack drives the real runner through the real worker socket
// via the CLI command, proving the whole stack end to end without Docker: a
// matching inline event invokes the rule's handler and prints the count, a
// matching event supplied on stdin via `-` invokes it again, a non-matching
// event prints "No matching handlers", and an unknown app surfaces the
// worker's error.
func TestAppInvokeFullStack(t *testing.T) {
	deps := invokeDeps(t)
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

	out, _, err := runCLIWithDeps(t, deps, "",
		"app", "invoke", "user-events", `{"event_name":"INSERT"}`)
	if err != nil {
		t.Fatalf("matching inline invoke: %v", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	if got := exec.got(); len(got) != 1 || got[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v", got)
	}

	// The explicit stdin form must reach the same runner through the socket.
	out, _, err = runCLIWithDeps(t, deps, "{\n  \"event_name\": \"INSERT\"\n}\n",
		"app", "invoke", "user-events", "-")
	if err != nil {
		t.Fatalf("stdin matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "Invoked 1 handler" {
		t.Fatalf("stdout = %q, want %q", out, "Invoked 1 handler")
	}
	if got := exec.got(); len(got) != 2 || got[1] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want the handler to run again via stdin", got)
	}

	out, _, err = runCLIWithDeps(t, deps, "",
		"app", "invoke", "user-events", `{"event_name":"MODIFY"}`)
	if err != nil {
		t.Fatalf("non-matching invoke: %v", err)
	}
	if strings.TrimSpace(out) != "No matching handlers" {
		t.Fatalf("stdout = %q, want no-match line", out)
	}

	_, _, err = runCLIWithDeps(t, deps, "", "app", "invoke", "ghost", `{}`)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unknown function err = %v, want it to name ghost", err)
	}
}

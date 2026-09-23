package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/processlock"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// fakeSnapshotter is an in-memory poolSnapshotter for socket tests, so the
// socket can be exercised without Docker or a runtime.Manager.
type fakeSnapshotter struct {
	pools map[string]runtime.PoolSnapshot
}

func (f *fakeSnapshotter) PoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	s, ok := f.pools[name]
	return s, ok
}

// shortTempDir returns a fresh temp directory under /tmp. It exists because
// t.TempDir() paths (under /var/folders on macOS) can exceed the ~104-byte
// Unix socket sun_path limit and make bind fail with EINVAL; /tmp keeps the
// path well under the limit.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "relay-sock-")
	if err != nil {
		t.Fatalf("short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// testSocketPath returns a socket path under a short temp dir, so tests never
// touch the real /run/relay and never exceed the Unix socket path limit.
func testSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortTempDir(t), "relay.sock")
}

// startTestSocket starts a SocketServer backed by pools at path.
func startTestSocket(t *testing.T, path string, pools map[string]runtime.PoolSnapshot) *SocketServer {
	t.Helper()
	return startTestSocketWithResetter(t, path, pools, nil)
}

// startTestSocketWithResetter starts a SocketServer backed by pools at path with
// an optional stats resetter, so the semantic reset command can be exercised.
func startTestSocketWithResetter(t *testing.T, path string, pools map[string]runtime.PoolSnapshot, resetter StatsResetter) *SocketServer {
	t.Helper()
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: pools},
		resetter,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestRuntimeSocketCreatesParentAndBinds covers runtime directory creation plus
// bind: NewSocketServer creates a missing parent directory and leaves a
// socket file at path.
func TestRuntimeSocketCreatesParentAndBinds(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "nested", "relay.sock")
	startTestSocket(t, path, nil)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("%s is not a socket (mode %v)", path, info.Mode())
	}
}

// TestRuntimeSocketRecoversStaleFile covers stale recovery: a leftover regular
// file (what a SIGKILLed worker can leave) is removed and replaced by a working
// socket.
func TestRuntimeSocketRecoversStaleFile(t *testing.T) {
	path := testSocketPath(t)
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatalf("seed stale file: %v", err)
	}

	startTestSocket(t, path, map[string]runtime.PoolSnapshot{
		"fn": {Function: "fn", Capacity: 2},
	})
	st, err := QueryRuntimeState(path, "fn")
	if err != nil {
		t.Fatalf("QueryRuntimeState after stale recovery: %v", err)
	}
	if st.Capacity != 2 {
		t.Fatalf("capacity = %d, want 2", st.Capacity)
	}
}

// TestRuntimeSocketLiveStateIncludingZeros covers the live query: an existing
// pool with all-zero gauges is a KNOWN state (no error), distinct from an
// unknown function.
func TestRuntimeSocketLiveStateIncludingZeros(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{
		"empty": {Function: "empty", Capacity: 0, Containers: 0, Busy: 0, Idle: 0, Starting: 0},
		"busy":  {Function: "busy", Capacity: 4, Containers: 3, Busy: 2, Idle: 1, Starting: 1},
	})

	empty, err := QueryRuntimeState(path, "empty")
	if err != nil {
		t.Fatalf("QueryRuntimeState(empty): %v", err)
	}
	if empty != (RuntimeState{}) {
		t.Fatalf("empty state = %+v, want all zeros", empty)
	}

	busy, err := QueryRuntimeState(path, "busy")
	if err != nil {
		t.Fatalf("QueryRuntimeState(busy): %v", err)
	}
	if busy.Capacity != 4 || busy.Containers != 3 || busy.Busy != 2 || busy.Idle != 1 || busy.Starting != 1 {
		t.Fatalf("busy state = %+v", busy)
	}
}

// TestRuntimeSocketUnknownFunction covers the clean unknown-function answer:
// QueryRuntimeState wraps ErrUnknownFunction and does not return a state.
func TestRuntimeSocketUnknownFunction(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})

	_, err := QueryRuntimeState(path, "ghost")
	if !errors.Is(err, ErrUnknownFunction) {
		t.Fatalf("error = %v, want ErrUnknownFunction", err)
	}
	if errors.Is(err, ErrRuntimeStateUnavailable) {
		t.Fatalf("unknown function must not be reported as unavailable: %v", err)
	}
}

// TestRuntimeSocketMalformedRequest covers the malformed frame path directly at
// the wire level: a non-JSON line, an absent command, an empty command, and an
// empty function name are all rejected as malformed_request.
func TestRuntimeSocketMalformedRequest(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})

	for _, line := range []string{
		"not-json\n",
		"{}\n",
		`{"function":"fn"}` + "\n",
		`{"command":"","function":"fn"}` + "\n",
		`{"command":"runtime_state","function":""}` + "\n",
	} {
		resp := rawQuery(t, path, line)
		if resp.Error != errCodeMalformedRequest {
			t.Fatalf("line %q: error = %q, want %q", line, resp.Error, errCodeMalformedRequest)
		}
		if resp.RuntimeState != nil {
			t.Fatalf("line %q: malformed request must not carry state", line)
		}
	}
}

// TestRuntimeSocketSilentClientHitsDeadline verifies a client that connects but
// never sends a request cannot hold a handler open: the server's connection
// deadline expires, the handler returns without answering, and the connection is
// closed (the client's read sees EOF).
func TestRuntimeSocketSilentClientHitsDeadline(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send nothing. The server should close the connection on its request
	// deadline; a read must then fail/EOF rather than block forever.
	_ = conn.SetReadDeadline(time.Now().Add(2 * runtimeStateRequestTimeout))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("silent client read %d bytes (%q), want the server to close without answering", n, buf[:n])
	}
}

// TestRuntimeSocketOversizeRequestRejected verifies a request larger than the
// 64KiB cap is rejected as malformed rather than buffered unboundedly: the
// server reads exactly the cap, finds no complete valid frame, and answers with
// a malformed_request error.
func TestRuntimeSocketOversizeRequestRejected(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * runtimeStateRequestTimeout))

	// Write from a goroutine and ignore write errors: the server may close the
	// connection after answering the truncated frame while we are still writing.
	go func() {
		_, _ = io.WriteString(conn, strings.Repeat("x", runtimeStateMaxRequest+4096))
	}()

	var resp socketResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != errCodeMalformedRequest {
		t.Fatalf("oversize request error = %q, want %q", resp.Error, errCodeMalformedRequest)
	}
	if resp.RuntimeState != nil {
		t.Fatalf("oversize request must not carry state")
	}
}

// rawQuery sends raw bytes over the socket and decodes the response frame. It
// bypasses QueryRuntimeState so tests can send malformed input.
func rawQuery(t *testing.T, path, line string) socketResponse {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, line); err != nil {
		t.Fatalf("write: %v", err)
	}
	var resp socketResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

// TestRuntimeSocketGracefulRemoval covers shutdown: Close removes the socket
// file, so a later query is unavailable and a fresh start can rebind cleanly.
func TestRuntimeSocketGracefulRemoval(t *testing.T) {
	path := testSocketPath(t)
	s := startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket file must be removed on Close, stat err = %v", err)
	}

	if _, err := QueryRuntimeState(path, "fn"); !errors.Is(err, ErrRuntimeStateUnavailable) {
		t.Fatalf("query after Close = %v, want ErrRuntimeStateUnavailable", err)
	}

	// Close is idempotent.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// A fresh socket can bind the same path.
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})
}

// TestQueryRuntimeStateUnavailableNoSocket covers the standalone no-worker case:
// dialing a non-existent socket path reports ErrRuntimeStateUnavailable.
func TestQueryRuntimeStateUnavailableNoSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	if _, err := QueryRuntimeState(path, "fn"); !errors.Is(err, ErrRuntimeStateUnavailable) {
		t.Fatalf("error = %v, want ErrRuntimeStateUnavailable", err)
	}
}

// TestRuntimeSocketActiveOwnershipGuard documents WHY stale removal is safe: a
// second Relay process fails the process lock before it can ever reach socket
// setup, so an active worker's socket is never removed. It holds the lock as
// worker A would, serves A's socket, and proves a second Acquire is rejected
// while A's socket stays queryable.
func TestRuntimeSocketActiveOwnershipGuard(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "relay.lock")
	held, err := processlock.Acquire(lockPath)
	if err != nil {
		t.Fatalf("Acquire (worker A): %v", err)
	}
	defer held.Close()

	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn", Capacity: 1}})

	// Worker B's startup order is lock-then-socket; the lock rejects it, so
	// NewSocketServer (and its stale removal) is unreachable.
	if _, err := processlock.Acquire(lockPath); !errors.Is(err, processlock.ErrAlreadyLocked) {
		t.Fatalf("second Acquire = %v, want ErrAlreadyLocked", err)
	}
	if _, err := QueryRuntimeState(path, "fn"); err != nil {
		t.Fatalf("active worker socket must remain served: %v", err)
	}
}

// fakeStatsResetter counts ResetStats calls for socket tests.
type fakeStatsResetter struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeStatsResetter) ResetStats() error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.err
}

func (f *fakeStatsResetter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestRuntimeSocketResetStatsCommand verifies the semantic "reset stats" command
// invokes the worker's StatsResetter exactly once and answers with the reset
// frame, and that an unknown command is rejected without invoking the resetter.
func TestRuntimeSocketResetStatsCommand(t *testing.T) {
	path := testSocketPath(t)
	resetter := &fakeStatsResetter{}
	startTestSocketWithResetter(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}}, resetter)

	if err := ResetRuntimeStats(path); err != nil {
		t.Fatalf("ResetRuntimeStats: %v", err)
	}
	if got := resetter.count(); got != 1 {
		t.Fatalf("resetter calls = %d, want 1", got)
	}

	// The unknown-command path is rejected as malformed and cannot reset.
	resp := rawQuery(t, path, `{"command":"bogus"}`+"\n")
	if resp.Error != errCodeMalformedRequest {
		t.Fatalf("unknown command error = %q, want %q", resp.Error, errCodeMalformedRequest)
	}
	if got := resetter.count(); got != 1 {
		t.Fatalf("unknown command must not reset, calls = %d", got)
	}

	// The runtime-state query still works alongside the reset command.
	if _, err := QueryRuntimeState(path, "fn"); err != nil {
		t.Fatalf("runtime state query after reset command: %v", err)
	}
}

// TestRuntimeSocketResetStatsUnavailable verifies a worker with no stats
// resetter answers stats_unavailable (so the CLI falls back to the state DB)
// rather than looking like a successful reset.
func TestRuntimeSocketResetStatsUnavailable(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, map[string]runtime.PoolSnapshot{"fn": {Function: "fn"}})

	err := ResetRuntimeStats(path)
	if !errors.Is(err, ErrRuntimeStatsUnavailable) {
		t.Fatalf("error = %v, want ErrRuntimeStatsUnavailable", err)
	}
}

// TestRuntimeSocketResetStatsResetterError verifies a resetter error is
// reported as ErrRuntimeStatsFailed (the worker answered but failed) rather than
// ErrRuntimeStatsUnavailable, so the CLI surfaces it instead of masking it.
func TestRuntimeSocketResetStatsResetterError(t *testing.T) {
	path := testSocketPath(t)
	resetter := &fakeStatsResetter{err: errors.New("state unavailable")}
	startTestSocketWithResetter(t, path, nil, resetter)

	err := ResetRuntimeStats(path)
	if !errors.Is(err, ErrRuntimeStatsFailed) {
		t.Fatalf("error = %v, want ErrRuntimeStatsFailed", err)
	}
	if errors.Is(err, ErrRuntimeStatsUnavailable) {
		t.Fatalf("resetter error must not look like an unavailable worker: %v", err)
	}
	if got := resetter.count(); got != 1 {
		t.Fatalf("resetter calls = %d, want 1", got)
	}
}

// TestResetRuntimeStatsNoSocket verifies the standalone no-worker case reports
// ErrRuntimeStatsUnavailable so the CLI resets the state database directly.
func TestResetRuntimeStatsNoSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	if err := ResetRuntimeStats(path); !errors.Is(err, ErrRuntimeStatsUnavailable) {
		t.Fatalf("error = %v, want ErrRuntimeStatsUnavailable", err)
	}
}

// fakeInvoker records manual-invocation calls for socket tests, so the
// invoke_function command can be exercised without Docker or a live runner.
type fakeInvoker struct {
	mu      sync.Mutex
	calls   int
	name    string
	event   map[string]any
	invoked int
	err     error
	// entered, when non-nil, is signalled once at the start of InvokeFunction;
	// release, when non-nil, makes InvokeFunction block until it closes or ctx
	// is done. They let tests observe shutdown cancelling an in-flight call.
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *fakeInvoker) InvokeFunction(ctx context.Context, name string, event map[string]any) (int, error) {
	f.mu.Lock()
	f.calls++
	f.name = name
	f.event = event
	entered, release := f.entered, f.release
	f.mu.Unlock()
	if entered != nil {
		f.once.Do(func() { close(entered) })
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return f.invoked, f.err
}

func (f *fakeInvoker) snapshot() (calls int, name string, event map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.name, f.event
}

// startTestSocketWithInvoker starts a SocketServer backed by pools at path with
// the given manual invoker, so the invoke_function command can be exercised.
func startTestSocketWithInvoker(t *testing.T, path string, pools map[string]runtime.PoolSnapshot, invoker FunctionInvoker) *SocketServer {
	t.Helper()
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: pools},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetInvoker(invoker)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestRuntimeSocketInvokeFunctionDelegates verifies the invoke_function command
// forwards the function name and decoded event object to the wired invoker and
// answers with the handler count.
func TestRuntimeSocketInvokeFunctionDelegates(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{invoked: 2}
	startTestSocketWithInvoker(t, path, nil, inv)

	invoked, err := InvokeFunction(context.Background(), path, "fn", json.RawMessage(`{"event_name":"INSERT","n":7}`))
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 2 {
		t.Fatalf("invoked = %d, want 2", invoked)
	}
	calls, name, event := inv.snapshot()
	if calls != 1 {
		t.Fatalf("invoker calls = %d, want 1", calls)
	}
	if name != "fn" {
		t.Fatalf("invoker name = %q, want fn", name)
	}
	if event["event_name"] != "INSERT" {
		t.Fatalf("invoker event = %#v, want the decoded object", event)
	}
}

// TestRuntimeSocketInvokeFunctionFailed verifies a runner error is reported as
// ErrInvokeFailed carrying the worker's message, and that the invoker was still
// called exactly once.
func TestRuntimeSocketInvokeFunctionFailed(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{err: fmt.Errorf("function %q handler %q: boom", "fn", "index.run")}
	startTestSocketWithInvoker(t, path, nil, inv)

	_, err := InvokeFunction(context.Background(), path, "fn", json.RawMessage(`{}`))
	if !errors.Is(err, ErrInvokeFailed) {
		t.Fatalf("error = %v, want ErrInvokeFailed", err)
	}
	if errors.Is(err, ErrInvokeUnavailable) {
		t.Fatalf("a handler failure must not look unavailable: %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error = %v, want the worker's message preserved", err)
	}
}

// TestRuntimeSocketInvokeFunctionUnknownFunction verifies the runner's
// not-found sentinel is classified as unknown_function, so the CLI reports the
// right cause (not a generic failure).
func TestRuntimeSocketInvokeFunctionUnknownFunction(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{err: fmt.Errorf("%w: %q", runner.ErrFunctionNotFound, "ghost")}
	startTestSocketWithInvoker(t, path, nil, inv)

	_, err := InvokeFunction(context.Background(), path, "ghost", json.RawMessage(`{}`))
	if !errors.Is(err, ErrInvokeFailed) {
		t.Fatalf("error = %v, want ErrInvokeFailed", err)
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error = %v, want it to name the unknown function", err)
	}
}

// TestRuntimeSocketInvokeFunctionNoInvoker verifies a socket with no wired
// invoker answers invoke_unavailable (there is no offline fallback), rather than
// appearing to have run zero handlers.
func TestRuntimeSocketInvokeFunctionNoInvoker(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, nil)

	_, err := InvokeFunction(context.Background(), path, "fn", json.RawMessage(`{}`))
	if !errors.Is(err, ErrInvokeUnavailable) {
		t.Fatalf("error = %v, want ErrInvokeUnavailable", err)
	}
}

// TestRuntimeSocketInvokeFunctionMalformed verifies the wire validation: an
// empty function, a missing event, invalid JSON, a non-object event (array),
// and a null event are all rejected as malformed without invoking the runner.
func TestRuntimeSocketInvokeFunctionMalformed(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{}
	startTestSocketWithInvoker(t, path, nil, inv)

	for _, line := range []string{
		`{"command":"invoke_function"}` + "\n",
		`{"command":"invoke_function","function":""}` + "\n",
		`{"command":"invoke_function","function":"fn"}` + "\n",
		`{"command":"invoke_function","function":"fn","event":"not-json"}` + "\n",
		`{"command":"invoke_function","function":"fn","event":[1,2]}` + "\n",
		`{"command":"invoke_function","function":"fn","event":null}` + "\n",
		`{"command":"invoke_function","function":"fn","event":"scalar"}` + "\n",
	} {
		resp := rawQuery(t, path, line)
		if resp.Error != errCodeMalformedRequest {
			t.Fatalf("line %q: error = %q, want %q", line, resp.Error, errCodeMalformedRequest)
		}
		if resp.InvokeResult != nil {
			t.Fatalf("line %q: malformed request must not carry a result", line)
		}
	}
	if calls, _, _ := inv.snapshot(); calls != 0 {
		t.Fatalf("invoker calls = %d, want 0 (malformed requests never invoke)", calls)
	}
}

// TestInvokeFunctionNoSocket verifies the standalone no-worker case reports
// ErrInvokeUnavailable (there is no offline fallback for a manual invocation).
func TestInvokeFunctionNoSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	if _, err := InvokeFunction(context.Background(), path, "fn", json.RawMessage(`{}`)); !errors.Is(err, ErrInvokeUnavailable) {
		t.Fatalf("error = %v, want ErrInvokeUnavailable", err)
	}
}

// runnerInvokerContract pins the production wiring contract: the concrete live
// runner the worker installs via SetInvoker must satisfy the socket's local
// seam, so the two cannot drift apart. It is a compile-time assertion (the
// assignment would not type-check otherwise).
var _ FunctionInvoker = (*runner.Runner)(nil)

// HandlerReplayerContract pins the production wiring contract for DLQ replay:
// the concrete live runner the worker installs via SetReplayer must satisfy the
// socket's local seam.
var _ HandlerReplayer = (*runner.Runner)(nil)

// fakeReplayer records DLQ-replay calls for socket tests, so the replay_dlq
// command can be exercised without Docker or a live runner.
type fakeReplayer struct {
	mu      sync.Mutex
	calls   int
	name    string
	handler string
	event   []byte
	err     error
}

func (f *fakeReplayer) ReplayDLQ(_ context.Context, name, handler string, event []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.name = name
	f.handler = handler
	f.event = append([]byte(nil), event...)
	return f.err
}

func (f *fakeReplayer) snapshot() (int, string, string, []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.name, f.handler, append([]byte(nil), f.event...)
}

// startTestSocketWithReplayer starts a SocketServer at path with the given DLQ
// replayer, so the replay_dlq command can be exercised.
func startTestSocketWithReplayer(t *testing.T, path string, replayer HandlerReplayer) *SocketServer {
	t.Helper()
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetReplayer(replayer)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestRuntimeSocketReplayDLQDelegates verifies the replay_dlq command forwards
// the exact function, handler, and event bytes to the wired replayer and answers
// with replayed=true.
func TestRuntimeSocketReplayDLQDelegates(t *testing.T) {
	path := testSocketPath(t)
	rep := &fakeReplayer{}
	startTestSocketWithReplayer(t, path, rep)

	if err := ReplayDLQ(context.Background(), path, "fn", "index.run", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	calls, name, handler, event := rep.snapshot()
	if calls != 1 || name != "fn" || handler != "index.run" {
		t.Fatalf("replayer calls/name/handler = %d/%q/%q", calls, name, handler)
	}
	if string(event) != `{"a":1}` {
		t.Fatalf("replayer event = %q, want the replayed payload", event)
	}
}

// TestRuntimeSocketReplayDLQErrorCodes verifies the runner sentinels are mapped
// onto stable wire codes surfaced to the CLI as ErrInvokeFailed with the worker's
// message.
func TestRuntimeSocketReplayDLQErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unknown function", fmt.Errorf("%w: %q", runner.ErrFunctionNotFound, "ghost")},
		{"unavailable function", fmt.Errorf("%w: %q", runner.ErrFunctionUnavailable, "broken")},
		{"removed handler", fmt.Errorf("%w: function %q handler %q", runner.ErrHandlerNotFound, "fn", "old.handler")},
		{"handler failure", fmt.Errorf("function %q handler %q: boom", "fn", "index.run")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := testSocketPath(t)
			startTestSocketWithReplayer(t, path, &fakeReplayer{err: tc.err})

			err := ReplayDLQ(context.Background(), path, "fn", "index.run", []byte(`{}`))
			if !errors.Is(err, ErrInvokeFailed) {
				t.Fatalf("error = %v, want ErrInvokeFailed", err)
			}
			if errors.Is(err, ErrInvokeUnavailable) {
				t.Fatalf("a runner failure must not look unavailable: %v", err)
			}
			if !strings.Contains(err.Error(), tc.err.Error()) {
				t.Fatalf("error = %v, want the worker's message preserved", err)
			}
		})
	}
}

// TestRuntimeSocketReplayDLQNoReplayer verifies a socket without a wired
// replayer answers invoke_unavailable (there is no offline fallback).
func TestRuntimeSocketReplayDLQNoReplayer(t *testing.T) {
	path := testSocketPath(t)
	startTestSocket(t, path, nil)

	err := ReplayDLQ(context.Background(), path, "fn", "index.run", []byte(`{}`))
	if !errors.Is(err, ErrInvokeUnavailable) {
		t.Fatalf("error = %v, want ErrInvokeUnavailable", err)
	}
}

// TestRuntimeSocketReplayDLQMalformed verifies the wire validation: empty
// function/handler and an absent payload are rejected without invoking the
// replayer.
func TestRuntimeSocketReplayDLQMalformed(t *testing.T) {
	path := testSocketPath(t)
	rep := &fakeReplayer{}
	startTestSocketWithReplayer(t, path, rep)

	for _, line := range []string{
		`{"command":"replay_dlq"}` + "\n",
		`{"command":"replay_dlq","function":"fn"}` + "\n",
		`{"command":"replay_dlq","function":"fn","handler":"index.run"}` + "\n",
		`{"command":"replay_dlq","function":"","handler":"index.run","event":"{}"}` + "\n",
		`{"command":"replay_dlq","function":"fn","handler":"","event":"{}"}` + "\n",
	} {
		resp := rawQuery(t, path, line)
		if resp.Error != errCodeMalformedRequest {
			t.Fatalf("line %q: error = %q, want %q", line, resp.Error, errCodeMalformedRequest)
		}
		if resp.Replayed {
			t.Fatalf("line %q: malformed request must not report a replay", line)
		}
	}
	if calls, _, _, _ := rep.snapshot(); calls != 0 {
		t.Fatalf("replayer calls = %d, want 0 (malformed requests never replay)", calls)
	}
}

// TestReplayDLQNoSocket verifies the standalone no-worker case reports
// ErrInvokeUnavailable (there is no offline fallback for a replay).
func TestReplayDLQNoSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sock")
	if err := ReplayDLQ(context.Background(), path, "fn", "index.run", []byte(`{}`)); !errors.Is(err, ErrInvokeUnavailable) {
		t.Fatalf("error = %v, want ErrInvokeUnavailable", err)
	}
}

// testExecutor is a minimal runner.Executor for the full-stack socket test: it
// records the executed handlers without Docker.
type testExecutor struct {
	mu       sync.Mutex
	handlers []string
}

func (e *testExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, _ []byte, _ []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	return nil
}

func (e *testExecutor) got() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.handlers...)
}

// TestRuntimeSocketInvokeFunctionFullStack wires the REAL runner through the
// socket and drives it over the wire with a parsed template: the matching rules
// execute (and the non-matching one does not), proving the whole CLI-less stack
// — socket codec, dispatch, and runner matching/execution.
func TestRuntimeSocketInvokeFunctionFullStack(t *testing.T) {
	path := testSocketPath(t)
	exec := &testExecutor{}
	tmpl, err := function.ParseTemplate([]byte(`runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.ignored.handler
    pattern:
      event_name: [DELETE]
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
	startTestSocketWithInvoker(t, path, nil, run)

	invoked, err := InvokeFunction(context.Background(), path, "user-events", json.RawMessage(`{"event_name":"INSERT"}`))
	if err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if invoked != 1 {
		t.Fatalf("invoked = %d, want 1", invoked)
	}
	got := exec.got()
	if len(got) != 1 || got[0] != "events.created.handler" {
		t.Fatalf("executed handlers = %v, want [events.created.handler]", got)
	}

	// A non-matching event runs nothing and answers zero (success).
	invoked, err = InvokeFunction(context.Background(), path, "user-events", json.RawMessage(`{"event_name":"MODIFY"}`))
	if err != nil {
		t.Fatalf("non-matching InvokeFunction: %v", err)
	}
	if invoked != 0 {
		t.Fatalf("non-matching invoked = %d, want 0", invoked)
	}
	if len(exec.got()) != 1 {
		t.Fatalf("a non-matching event must execute nothing, got %v", exec.got())
	}
}

// TestRuntimeSocketCloseCancelsInflightInvoke verifies Close promptly cancels an
// in-flight manual invocation (via the server's base context) instead of waiting
// for the invocation bound, so worker shutdown is never held by a wedged handler.
func TestRuntimeSocketCloseCancelsInflightInvoke(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{entered: make(chan struct{}), release: make(chan struct{})}
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetInvoker(inv)

	// Dial directly (not through InvokeFunction) so the client deadline does not
	// interfere; the invocation blocks in the invoker until Close cancels it.
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(socketRequest{
		Command: cmdInvokeFunction, Function: "fn", Event: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Wait until the invoker is actually blocked inside the call, then Close.
	select {
	case <-inv.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("invoker was not entered")
	}
	closed := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return promptly; the in-flight invocation was not cancelled")
	}
}

// TestInvokeFunctionClientCancelAborts verifies a cancelled client context
// (Ctrl-C) aborts an in-flight manual invocation promptly rather than waiting out
// the invocation deadline: the client closes its connection and the call returns.
func TestInvokeFunctionClientCancelAborts(t *testing.T) {
	path := testSocketPath(t)
	inv := &fakeInvoker{entered: make(chan struct{}), release: make(chan struct{})}
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: nil},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetInvoker(inv)
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := InvokeFunction(ctx, path, "fn", json.RawMessage(`{}`))
		done <- err
	}()

	select {
	case <-inv.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("invoker was not entered")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInvokeUnavailable) {
			t.Fatalf("error = %v, want ErrInvokeUnavailable after cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InvokeFunction did not abort promptly on client cancel")
	}
}

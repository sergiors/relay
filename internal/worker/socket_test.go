package worker

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/processlock"
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

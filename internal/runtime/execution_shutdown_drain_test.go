package runtime

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/testutil"
)

// These tests pin the shutdown teardown contract of one execution container:
// a SHUTDOWN discard of an IDLE container first closes the authenticated
// execution channel (so a managed bootstrap can flush its process-lifetime
// provider) and waits a bounded grace for the process to confirm exit before
// force-killing; every other discard reason — and a busy container, and a
// shutdown discard outside the Manager's final cache-teardown phase — keeps
// the historical immediate kill-then-remove order. A shutdown teardown also
// joins the container's reader, monitor, and exit-watcher goroutines, so the
// final cache teardown leaves none behind.

// closeSignalConn wraps the client end of the control-channel pipe so a test
// can observe the exact moment a teardown closes the execution channel.
type closeSignalConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *closeSignalConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *closeSignalConn) closedObserved() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// shutdownDrainFixture is one directly constructed execution container wired to
// a net.Pipe control channel and a scripted Docker client, with the full
// production goroutine set (protocol reader, death monitor, exit watcher)
// started exactly as connectExecutionContainer starts them.
type shutdownDrainFixture struct {
	c     *executionContainer
	peer  net.Conn
	ops   *managerOps
	conn  *closeSignalConn
	waits atomic.Int32
	kills atomic.Int32
	// channelClosedAtKill records whether the execution channel was already
	// closed when the first force-kill was issued.
	channelClosedAtKill atomic.Bool
}

// waitRoute builds the scripted /wait route. While confirmExit is set, a wait
// request succeeds as soon as the execution channel has been closed (the
// bootstrap observed EOF and exited); otherwise it blocks until its own context
// expires (a bootstrap that never exits). Either way the request is bounded by
// its context, exactly like the real daemon call.
func (f *shutdownDrainFixture) waitRoute(confirmExit bool) dockerRoute {
	return dockerRoute{
		method: http.MethodPost,
		path:   "/wait",
		body:   `{"StatusCode":0}`,
		fail: func(req *http.Request) error {
			f.waits.Add(1)
			if !confirmExit {
				<-req.Context().Done()
				return req.Context().Err()
			}
			select {
			case <-f.conn.closed:
				return nil
			case <-req.Context().Done():
				return req.Context().Err()
			}
		},
	}
}

// newShutdownDrainFixture builds the container with the full production
// goroutine set. confirmExit controls whether the scripted daemon reports the
// process gone once the channel closes (see waitRoute).
func newShutdownDrainFixture(t *testing.T, confirmExit bool) *shutdownDrainFixture {
	t.Helper()
	f := &shutdownDrainFixture{ops: newManagerOps(nil)}

	clientEnd, peerEnd := net.Pipe()
	t.Cleanup(func() { _ = peerEnd.Close() })
	conn := &closeSignalConn{Conn: clientEnd, closed: make(chan struct{})}
	f.conn = conn
	f.peer = peerEnd

	cli := newScriptedDockerClient(t,
		f.waitRoute(confirmExit),
		dockerRoute{
			method: http.MethodPost, path: "/kill",
			onMatch: func() {
				if f.kills.Add(1) == 1 {
					f.channelClosedAtKill.Store(conn.closedObserved())
				}
			},
		},
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}"},
	)

	f.c = &executionContainer{
		cli:        cli,
		log:        testutil.DiscardLogger(),
		fn:         "test-fn",
		image:      "test-image",
		id:         "test-container",
		ops:        containerOps{lifecycle: f.ops.context(), gate: f.ops},
		conn:       conn,
		fail:       make(chan failEvent, 4),
		exitInfo:   make(chan exitInfo, 1),
		connErr:    make(chan error, 1),
		protoErr:   make(chan error, 1),
		closed:     make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go f.c.readResponses()

	f.c.monitorDone = make(chan struct{})
	go f.c.monitor()

	f.c.watchDone = make(chan struct{})
	go f.c.watchExit()
	return f
}

// waitWatcherAdmitted blocks until the exit watcher has issued its long-lived
// wait request, so a test sequences shutdown steps against the real watcher.
func (f *shutdownDrainFixture) waitWatcherAdmitted(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for f.waits.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the exit watcher never issued its wait request")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertGoroutinesJoined proves the reader, monitor, and exit watcher have all
// concluded (their completion channels are closed).
func (f *shutdownDrainFixture) assertGoroutinesJoined(t *testing.T) {
	t.Helper()
	for name, ch := range map[string]chan struct{}{
		"protocol reader": f.c.readerDone,
		"death monitor":   f.c.monitorDone,
		"exit watcher":    f.c.watchDone,
	} {
		select {
		case <-ch:
		case <-time.After(execChannelWatchdog):
			t.Errorf("%s goroutine did not exit after the teardown", name)
		}
	}
}

// TestShutdownTeardownDrainsIdleBootstrapBeforeKill proves the graceful half:
// given an idle container whose Manager is in the final cache-teardown phase,
// a shutdown discard closes the execution channel FIRST, then waits for the
// process to confirm exit within the grace, then removes WITHOUT killing — and
// the teardown joins every container goroutine and releases its operation.
func TestShutdownTeardownDrainsIdleBootstrapBeforeKill(t *testing.T) {
	f := newShutdownDrainFixture(t, true)
	f.waitWatcherAdmitted(t)

	// Manager shutdown sequence: normal work closes (cancelling the watcher's
	// long-lived wait), then the cache-teardown phase opens.
	f.ops.closeNormal()
	select {
	case <-f.c.watchDone:
	case <-time.After(execChannelWatchdog):
		t.Fatal("the exit watcher did not conclude when normal admission closed")
	}
	f.ops.openTeardown()

	if !f.c.discardContext(context.Background(), reasonShutdown) {
		t.Fatal("the first shutdown discard must perform the teardown")
	}

	// The channel closed so the bootstrap could clean up, the process confirmed
	// exit, and no force-kill was ever issued.
	if !f.conn.closedObserved() {
		t.Error("shutdown teardown must close the execution channel before waiting")
	}
	if f.waits.Load() < 2 {
		t.Errorf("wait requests = %d, want >= 2 (watcher plus the drain)", f.waits.Load())
	}
	if n := f.kills.Load(); n != 0 {
		t.Errorf("kills = %d, want 0 (the bootstrap confirmed exit within the grace)", n)
	}
	if n := f.ops.count(); n != 0 {
		t.Errorf("operations after teardown = %d, want 0", n)
	}
	f.assertGoroutinesJoined(t)

	// The container is gone and refuses reuse.
	select {
	case <-f.c.removalDone():
	default:
		t.Error("removalDone must be closed after the teardown")
	}
	if !f.c.dead() {
		t.Error("a torn-down container must report dead")
	}
}

// TestShutdownTeardownForceKillsAfterDrainGraceExpires proves the bound: the
// channel still closes first (the bootstrap gets its cleanup opportunity), but
// a process that never exits is force-killed when the grace expires — the
// whole teardown stays within the shutdown context's budget.
func TestShutdownTeardownForceKillsAfterDrainGraceExpires(t *testing.T) {
	f := newShutdownDrainFixture(t, false) // the "bootstrap" never confirms exit
	f.waitWatcherAdmitted(t)
	f.ops.closeNormal()
	select {
	case <-f.c.watchDone:
	case <-time.After(execChannelWatchdog):
		t.Fatal("the exit watcher did not conclude when normal admission closed")
	}
	f.ops.openTeardown()

	// A short deadline caps the drain grace at half the remaining budget, so
	// the test observes the timeout-kill path without waiting the fixed grace.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	if !f.c.discardContext(ctx, reasonShutdown) {
		t.Fatal("the first shutdown discard must perform the teardown")
	}
	elapsed := time.Since(start)

	if !f.conn.closedObserved() {
		t.Error("the execution channel must close before the grace wait")
	}
	if f.waits.Load() < 2 {
		t.Errorf("wait requests = %d, want >= 2 (the drain must actually wait)", f.waits.Load())
	}
	if n := f.kills.Load(); n != 1 {
		t.Errorf("kills = %d, want 1 (an idle container that never exits is force-killed)", n)
	}
	if !f.channelClosedAtKill.Load() {
		t.Error("the force-kill must happen after the channel close")
	}
	if n := f.ops.count(); n != 0 {
		t.Errorf("operations after teardown = %d, want 0", n)
	}
	if elapsed > 3*time.Second {
		t.Errorf("teardown took %v, want it bounded by the shutdown budget", elapsed)
	}
	f.assertGoroutinesJoined(t)
}

// TestShutdownTeardownBusyContainerSkipsDrain proves shutdown never waits out
// an in-flight handler: while an invocation holds the container's exchange
// slot, a shutdown discard skips the drain entirely (no wait request, no early
// channel close) and keeps the historical immediate interrupt teardown.
func TestShutdownTeardownBusyContainerSkipsDrain(t *testing.T) {
	f := newShutdownDrainFixture(t, true)
	f.waitWatcherAdmitted(t)
	f.ops.closeNormal()
	select {
	case <-f.c.watchDone:
	case <-time.After(execChannelWatchdog):
		t.Fatal("the exit watcher did not conclude when normal admission closed")
	}
	f.ops.openTeardown()

	// Park a real invocation in its response read: it holds ioMu for the whole
	// exchange, so the drain's try-lock must fail.
	invokeDone := make(chan error, 1)
	go func() {
		invokeDone <- f.c.Invoke(context.Background(), "index.run", []byte(`{}`), nil)
	}()
	// The request frame arriving on the peer proves the exchange is in flight
	// (and holds the container's io slot).
	waitRead(t, readRequestAsync(f.peer))

	waitsBefore := f.waits.Load()
	if !f.c.discardContext(context.Background(), reasonShutdown) {
		t.Fatal("the first shutdown discard must perform the teardown")
	}
	if n := f.waits.Load(); n != waitsBefore {
		t.Errorf("drain wait requests = %d, want %d (a busy container skips the drain)",
			n, waitsBefore)
	}
	if n := f.kills.Load(); n != 1 {
		t.Errorf("kills = %d, want 1 (immediate interrupt teardown)", n)
	}
	if f.channelClosedAtKill.Load() {
		t.Error("the busy container's channel must not be closed before the kill")
	}
	// The parked invocation is released by the teardown's channel close.
	select {
	case <-invokeDone:
	case <-time.After(execChannelWatchdog):
		t.Error("the parked invocation never returned after the teardown")
	}
	if n := f.ops.count(); n != 0 {
		t.Errorf("operations after teardown = %d, want 0", n)
	}
	f.assertGoroutinesJoined(t)
}

// TestShutdownFlushGraceOutlivesPythonProviderFlush pins the cross-language
// shutdown budget: an idle shutdown teardown must give the managed Python
// bootstrap strictly longer to flush its process-lifetime provider than the
// bootstrap's own bounded flush (SHUTDOWN_TIMEOUT_SECONDS in
// internal/runtime/python/_relay_telemetry.py), or the force-kill would always
// truncate the flush. The Python bound is read from its module source so a
// coordinated change on either side fails here instead of silently narrowing
// the margin.
func TestShutdownFlushGraceOutlivesPythonProviderFlush(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("python", "_relay_telemetry.py"))
	if err != nil {
		t.Fatalf("read the embedded Python telemetry module: %v", err)
	}
	match := regexp.MustCompile(`SHUTDOWN_TIMEOUT_SECONDS\s*=\s*([0-9.]+)`).
		FindStringSubmatch(string(raw))
	if match == nil {
		t.Fatal("SHUTDOWN_TIMEOUT_SECONDS not found in _relay_telemetry.py")
	}
	seconds, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		t.Fatalf("parse SHUTDOWN_TIMEOUT_SECONDS %q: %v", match[1], err)
	}
	pythonFlush := time.Duration(seconds * float64(time.Second))

	if pythonFlush != 8*time.Second {
		t.Errorf("Python provider flush bound = %v, want the coordinated 8s", pythonFlush)
	}
	if executionShutdownFlushGrace != 10*time.Second {
		t.Errorf("Go shutdown drain grace = %v, want the coordinated 10s", executionShutdownFlushGrace)
	}
	if pythonFlush >= executionShutdownFlushGrace {
		t.Errorf("Python flush bound %v must fit strictly inside the Go drain grace %v, "+
			"or an idle container's provider flush is always killed mid-export",
			pythonFlush, executionShutdownFlushGrace)
	}

	// The grace is the fixed cap with no deadline, capped to half the
	// remaining budget under one, and zero once the budget is spent.
	if got := shutdownFlushGrace(context.Background()); got != executionShutdownFlushGrace {
		t.Errorf("grace without a deadline = %v, want the fixed %v", got, executionShutdownFlushGrace)
	}
	budgeted, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	// Half the remaining budget (allowing a little elapsed time between
	// creating the context and reading the grace).
	if got := shutdownFlushGrace(budgeted); got <= 150*time.Millisecond || got > 200*time.Millisecond {
		t.Errorf("grace with ~400ms remaining = %v, want about half the budget (<= 200ms)", got)
	}
	expired, cancelExpired := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelExpired()
	<-expired.Done()
	if got := shutdownFlushGrace(expired); got != 0 {
		t.Errorf("grace with an expired budget = %v, want 0 (force-kill immediately)", got)
	}
}

// TestNonShutdownTeardownsStayImmediate proves every non-shutdown discard
// reason keeps the historical kill-then-remove order even when the container
// is idle and the Manager is in the cache-teardown phase: no drain wait is
// issued and the channel is not closed ahead of the kill.
func TestNonShutdownTeardownsStayImmediate(t *testing.T) {
	reasons := []string{
		reasonTimeout,
		reasonProtocolError,
		reasonProcessExit,
		reasonImageChanged,
		reasonResourcesChanged,
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			f := newShutdownDrainFixture(t, true)
			f.waitWatcherAdmitted(t)
			f.ops.closeNormal()
			select {
			case <-f.c.watchDone:
			case <-time.After(execChannelWatchdog):
				t.Fatal("the exit watcher did not conclude when normal admission closed")
			}
			f.ops.openTeardown()

			watchesBefore := f.waits.Load()
			if !f.c.discardContext(context.Background(), reason) {
				t.Fatalf("the first %s discard must perform the teardown", reason)
			}
			if n := f.waits.Load(); n != watchesBefore {
				t.Errorf("wait requests = %d, want %d (non-shutdown teardown never drains)",
					n, watchesBefore)
			}
			if n := f.kills.Load(); n != 1 {
				t.Errorf("kills = %d, want 1 (immediate teardown)", n)
			}
			if f.channelClosedAtKill.Load() {
				t.Error("the channel must not be closed before the kill")
			}
			if n := f.ops.count(); n != 0 {
				t.Errorf("operations after teardown = %d, want 0", n)
			}
			f.assertGoroutinesJoined(t)
		})
	}
}

// TestShutdownTeardownWithoutTeardownPhaseStaysImmediate pins the create-race
// boundary: a shutdown discard that runs BEFORE the Manager's final
// cache-teardown phase opens (normal admission already closed) keeps the
// historical immediate teardown — no drain, no wait — so an ordinary cleanup
// never blocks on a bootstrap flush it does not own.
func TestShutdownTeardownWithoutTeardownPhaseStaysImmediate(t *testing.T) {
	f := newShutdownDrainFixture(t, true)
	f.waitWatcherAdmitted(t)
	f.ops.closeNormal()
	select {
	case <-f.c.watchDone:
	case <-time.After(execChannelWatchdog):
		t.Fatal("the exit watcher did not conclude when normal admission closed")
	}
	// The teardown phase deliberately stays closed.

	if !f.c.discardContext(context.Background(), reasonShutdown) {
		t.Fatal("the shutdown discard outside the teardown phase must still tear down")
	}
	if n := f.waits.Load(); n != 1 {
		t.Errorf("wait requests = %d, want 1 (only the watcher's; no drain outside the phase)", n)
	}
	if n := f.kills.Load(); n != 1 {
		t.Errorf("kills = %d, want 1 (immediate teardown outside the phase)", n)
	}
	if f.channelClosedAtKill.Load() {
		t.Error("the channel must not be closed before the kill")
	}
	if n := f.ops.count(); n != 0 {
		t.Errorf("operations after teardown = %d, want 0", n)
	}
	f.assertGoroutinesJoined(t)
}

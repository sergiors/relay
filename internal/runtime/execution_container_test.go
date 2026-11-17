package runtime

// Unit tests for one reused executionContainer speaking the persistent framed
// control channel (protocol.go) over a net.Pipe stand-in for the container's
// authenticated TCP connection. The harness plays the bootstrap side: it reads
// framed invoke requests and answers with framed result frames. Every scenario
// from the old stdin/stdout attach harness is preserved (normal exchange,
// reuse, blocked write/read cancellation, pre-cancel, deadline, the
// cancellation-vs-completion race, pool non-reuse, manager shutdown join,
// removal accounting) and extended with the control channel's correlation,
// protocol-violation, and transport-loss semantics.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// execChannelWatchdog bounds every wait in these tests. It is a watchdog only
// (the tests synchronize on channels, never on this duration), sized generously
// for a loaded CI machine.
const execChannelWatchdog = 5 * time.Second

// observingConn wraps the client end of the control-channel pipe and reports
// the exact instant the synchronous request Write ENTERS, before it can block on
// the pipe. A test can cancel only once Invoke has demonstrably reached the
// connection Write, so cancellation provably interrupts a blocked write rather
// than racing the call. All other net.Conn behavior is the embedded conn's.
type observingConn struct {
	net.Conn
	writeEntered chan struct{}
	once         sync.Once
}

func (o *observingConn) Write(b []byte) (int, error) {
	o.once.Do(func() { close(o.writeEntered) })
	return o.Conn.Write(b)
}

// gatedCloseConn wraps the client end of the pipe so a test can gate the exact
// connection Close issued by a discard path. Close signals its entry (the
// connection barrier), performs the REAL close — which wakes the protocol
// reader, whose EOF is the monitor's connection-loss wakeup — and only then
// parks until the test releases it. That freezes the discard state at the
// moment the close begins and lets the test force a competing monitor discard
// to fully complete before the closing discarder can proceed.
type gatedCloseConn struct {
	net.Conn
	entered   chan struct{}
	released  chan struct{}
	watchdog  time.Duration
	closeOnce sync.Once
}

func (g *gatedCloseConn) Close() error {
	g.closeOnce.Do(func() { close(g.entered) })
	err := g.Conn.Close()
	// Bounded watchdog only: the test's release signal is the ordering
	// mechanism, this merely prevents a leaked parked goroutine on failure.
	select {
	case <-g.released:
	case <-time.After(g.watchdog):
	}
	return err
}

// heldWriteConn gates the exact request Write an Invoke issues: it signals the
// observing conn's writeEntered BEFORE delegating here, then parks the Write
// until the test closes release — without ever touching the pipe, so a discard
// that closes the real connection cannot fail it. Releasing returns success,
// so the invocation proceeds to its resolve select only after the test has
// arranged the container state. The watchdog bounds only (like gatedCloseConn's):
// it prevents a leaked parked goroutine when a failing test never releases; the
// test's release signal is the ordering mechanism.
type heldWriteConn struct {
	net.Conn
	release  chan struct{}
	watchdog time.Duration
}

func (w *heldWriteConn) Write(b []byte) (int, error) {
	select {
	case <-w.release:
		return len(b), nil
	case <-time.After(w.watchdog):
		return 0, errors.New("held write was never released")
	}
}

// executionContainerHarness wires a real *executionContainer to a net.Pipe
// stand-in for its long-lived control-channel connection and a scripted Docker
// client for the kill/remove teardown. The pipe lets a test block the
// synchronous request Write (the peer never reads) or the response Read (the
// peer reads the request but never replies), then release it deterministically
// by cancelling the invocation context — the exact production cancellation
// path. The container's single protocol reader (readResponses) is running; the
// exit watcher and monitor are NOT started unless a test asks for the monitor,
// because the scripted daemon serves no wait endpoint.
type executionContainerHarness struct {
	c            *executionContainer
	peer         net.Conn
	reader       <-chan struct{} // c.readerDone
	writeEntered <-chan struct{}
	cleanup      func()

	// Gated-Close barriers (set only by the gated harness): closeEntered
	// signals when a discard path enters the connection Close; closeRelease is
	// closed by the test to let a parked Close proceed.
	closeEntered <-chan struct{}
	closeRelease chan struct{}

	// Held-Write barrier (set only by the held-write harness): releaseWrite is
	// closed by the test to let the parked request Write complete, so Invoke
	// reaches its resolve select only after the test has arranged the state.
	releaseWrite chan struct{}

	// killSignal closes on the first scripted Docker kill, so a test can wait
	// for a teardown's kill without polling the counter.
	killSignal chan struct{}
	killOnce   sync.Once

	mu     sync.Mutex
	kills  int
	remove int
}

func newExecutionContainerHarness(t *testing.T) *executionContainerHarness {
	t.Helper()
	return newExecutionContainerHarnessWithRemove(t, 0)
}

// newExecutionContainerHarnessWithRemove is newExecutionContainerHarness with
// a controllable Docker removal outcome: removeStatus 0 (or 200) scripts a
// successful remove; a 5xx status scripts a genuine removal failure (the daemon
// refused), letting a test pin how a teardown that could not clean up is
// accounted.
func newExecutionContainerHarnessWithRemove(t *testing.T, removeStatus int) *executionContainerHarness {
	t.Helper()
	return buildExecutionContainerHarness(t, removeStatus, false, false)
}

// newGatedExecutionContainerHarness is newExecutionContainerHarness with the
// connection Close gated behind the closeEntered/closeRelease barriers, for the
// cancellation-vs-monitor discard-race regression.
func newGatedExecutionContainerHarness(t *testing.T) *executionContainerHarness {
	t.Helper()
	return buildExecutionContainerHarness(t, 0, true, false)
}

// newHeldWriteExecutionContainerHarness is newExecutionContainerHarness with
// the request Write gated behind the releaseWrite barrier: the invocation
// registers its pending state and enters the Write, then parks there until the
// test releases it, so a test can deterministically arrange monitor/discard
// state BEFORE Invoke reaches its resolve select.
func newHeldWriteExecutionContainerHarness(t *testing.T) *executionContainerHarness {
	t.Helper()
	return buildExecutionContainerHarness(t, 0, false, true)
}

func buildExecutionContainerHarness(t *testing.T, removeStatus int, gatedClose, holdWrites bool) *executionContainerHarness {
	t.Helper()
	clientEnd, peerEnd := net.Pipe()
	conn := &observingConn{Conn: clientEnd, writeEntered: make(chan struct{})}
	h := &executionContainerHarness{
		peer:         peerEnd,
		writeEntered: conn.writeEntered,
		killSignal:   make(chan struct{}),
	}
	if holdWrites {
		held := &heldWriteConn{
			Conn:     clientEnd,
			release:  make(chan struct{}),
			watchdog: execChannelWatchdog,
		}
		// The observing conn stays outside so its writeEntered barrier fires
		// before the held conn parks the Write. The chain is built before the
		// reader starts, so the reader never observes a racy conn swap.
		conn.Conn = held
		h.releaseWrite = held.release
	}

	rmBody := "{}"
	if removeStatus >= 400 {
		rmBody = `{"message":"remove boom"}`
	}
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() {
			h.mu.Lock()
			h.kills++
			h.mu.Unlock()
			h.killOnce.Do(func() { close(h.killSignal) })
		}},
		dockerRoute{method: http.MethodDelete, path: "/containers/", status: removeStatus, body: rmBody, onMatch: func() {
			h.mu.Lock()
			h.remove++
			h.mu.Unlock()
		}},
	)

	var channelConn net.Conn = conn
	if gatedClose {
		h.closeRelease = make(chan struct{})
		gated := &gatedCloseConn{
			Conn:     conn,
			entered:  make(chan struct{}),
			released: h.closeRelease,
			watchdog: execChannelWatchdog,
		}
		h.closeEntered = gated.entered
		channelConn = gated
	}

	c := &executionContainer{
		cli:        cli,
		log:        testutil.DiscardLogger(),
		fn:         "test-fn",
		image:      "test-image",
		id:         "test-container",
		conn:       channelConn,
		fail:       make(chan failEvent, 4),
		exitInfo:   make(chan exitInfo, 1),
		connErr:    make(chan error, 1),
		protoErr:   make(chan error, 1),
		closed:     make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	h.c = c
	h.reader = c.readerDone
	go c.readResponses()
	h.cleanup = func() {
		_ = peerEnd.Close()
		_ = conn.Close()
		select {
		case <-h.reader:
		case <-time.After(execChannelWatchdog):
		}
	}
	t.Cleanup(h.cleanup)
	return h
}

// startMonitor starts the container's death monitor, for tests that exercise
// how death events route to a pending Invoke (correlation violations,
// transport loss).
func (h *executionContainerHarness) startMonitor() {
	go h.c.monitor()
}

func (h *executionContainerHarness) killsDone() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.kills
}

func (h *executionContainerHarness) removesDone() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.remove
}

func (h *executionContainerHarness) assertTornDown(t *testing.T) {
	t.Helper()
	if h.killsDone() < 1 {
		t.Error("expected a container kill on discard")
	}
	if h.removesDone() < 1 {
		t.Error("expected a container remove on discard")
	}
}

// requestRead is the outcome of reading one framed invoke request from the peer
// side of the control-channel pipe.
type requestRead struct {
	req invokeRequest
	err error
}

// readRequestAsync reads exactly one framed invoke request from the peer on its
// own goroutine (net.Pipe reads are synchronous and must not run on the test
// goroutine while an Invoke may be blocked writing), with a watchdog.
func readRequestAsync(conn net.Conn) <-chan requestRead {
	ch := make(chan requestRead, 1)
	go func() {
		raw, err := readFrame(conn, maxRequestFrame)
		if err != nil {
			ch <- requestRead{err: err}
			return
		}
		var req invokeRequest
		err = json.Unmarshal(raw, &req)
		if err == nil && req.Type != frameTypeInvoke {
			err = errors.New("frame is not an invoke request")
		}
		ch <- requestRead{req: req, err: err}
	}()
	return ch
}

func waitRead(t *testing.T, ch <-chan requestRead) invokeRequest {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read invoke request: %v", r.err)
		}
		return r.req
	case <-time.After(execChannelWatchdog):
		t.Fatal("timed out reading invoke request")
		return invokeRequest{}
	}
}

func waitInvoke(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(execChannelWatchdog):
		t.Fatal("timed out waiting for Invoke to return")
		return nil
	}
}

func waitReaderExit(t *testing.T, h *executionContainerHarness) {
	t.Helper()
	select {
	case <-h.reader:
	case <-time.After(execChannelWatchdog):
		t.Fatal("protocol reader goroutine did not exit after the connection closed")
	}
}

// waitDiscardComplete waits until the teardown has closed the connection
// (removalDone closes after the conn close and the reader join), so a
// follow-up attempt observes a deterministically closed channel instead of
// racing an in-flight teardown.
func (h *executionContainerHarness) waitDiscardComplete(t *testing.T) {
	t.Helper()
	select {
	case <-h.c.removalDone():
	case <-time.After(execChannelWatchdog):
		t.Fatalf("discard did not complete: the connection was never closed (kills=%d removes=%d)",
			h.killsDone(), h.removesDone())
	}
}

// assertRefusesUse proves a discarded container never runs another invocation:
// the attempt fails closed on the closed channel without any peer interaction
// (the pool-level non-reuse is pinned by the executeVersion pool test).
func (h *executionContainerHarness) assertRefusesUse(t *testing.T) {
	t.Helper()
	err := h.c.Invoke(context.Background(), "index.run", []byte(`{}`), nil)
	if err == nil {
		t.Fatal("a discarded container must never run another invocation")
	}
	if !strings.Contains(err.Error(), "docker run: ") {
		t.Errorf("refusal = %v, want the wrapped execution failure", err)
	}
}

// writeResult writes one framed result payload on the peer side.
func writeResult(t *testing.T, peer net.Conn, resp invokeResponse) {
	t.Helper()
	payload, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if err := writeFrame(peer, payload, maxResponseFrame); err != nil {
		t.Fatalf("write result frame: %v", err)
	}
}

// serveOne reads one invoke request from the peer and answers it with an OK
// result carrying the request's id. It is used when the invocation is driven by
// the pool (not by the test goroutine).
func (h *executionContainerHarness) serveOne(t *testing.T) {
	t.Helper()
	req := waitRead(t, readRequestAsync(h.peer))
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
}

// roundTrip drives one complete Invoke exchange on the test goroutine's behalf:
// start Invoke, read and answer its request, and return Invoke's result.
func (h *executionContainerHarness) roundTrip(t *testing.T, handler string) (invokeRequest, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, handler, []byte(`{"event_name":"INSERT"}`), nil)
	}()
	req := waitRead(t, readRequestAsync(h.peer))
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
	return req, waitInvoke(t, done)
}

// TestExecutionContainerInvokeNormalRequestResponse pins the happy path: one
// framed invoke request carrying the handler, the OK result completes the
// invocation, the container stays healthy, and a second invocation reuses the
// SAME connection.
func TestExecutionContainerInvokeNormalRequestResponse(t *testing.T) {
	h := newExecutionContainerHarness(t)

	req, err := h.roundTrip(t, "index.run")
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if req.Handler != "index.run" {
		t.Errorf("request handler = %q, want index.run", req.Handler)
	}
	if req.Type != frameTypeInvoke {
		t.Errorf("frame type = %q, want %q", req.Type, frameTypeInvoke)
	}
	if req.Context == nil {
		t.Error("request frame must carry its execution context")
	}
	if h.c.dead() {
		t.Fatal("a successful invocation must not discard the container")
	}

	if _, err := h.roundTrip(t, "index.run"); err != nil {
		t.Fatalf("reused invoke: %v", err)
	}
	if h.c.dead() {
		t.Fatal("the reused container must stay healthy")
	}
}

// TestExecutionContainerHandlerFailureKeepsContainer pins the contract split:
// a handler exception arrives as a bounded ok:false result and the container is
// NOT discarded — only transport/protocol failures are.
func TestExecutionContainerHandlerFailureKeepsContainer(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()
	req := waitRead(t, readRequestAsync(h.peer))
	writeResult(t, h.peer, invokeResponse{
		Type: frameTypeResult, ID: req.ID, OK: false, Error: "boom",
	})
	err := waitInvoke(t, done)
	if err == nil || !strings.Contains(err.Error(), `handler "index.run" failed: boom`) {
		t.Fatalf("error = %v, want the handler-failure message", err)
	}
	if h.c.dead() {
		t.Fatal("a handler failure must keep the container healthy")
	}
	if _, err := h.roundTrip(t, "index.run"); err != nil {
		t.Fatalf("invocation after a handler failure: %v", err)
	}
}

// TestExecutionContainerInvokeBlockedWriteCancelled verifies that a request
// Write blocked on the connection (the peer never reads) is released by context
// cancellation: Invoke returns the wrapped context error, the container is
// discarded with the timeout reason, kill/remove are issued, the reader exits,
// and the peer observes the connection close.
func TestExecutionContainerInvokeBlockedWriteCancelled(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	// Barrier: the observing conn closes writeEntered the instant Invoke reaches
	// the connection Write, BEFORE it can block. With the peer never reading,
	// the write cannot complete, so cancelling now provably interrupts an
	// in-flight write rather than racing the call itself.
	select {
	case <-h.writeEntered:
	case <-time.After(execChannelWatchdog):
		t.Fatal("timed out waiting for Invoke to enter the connection write")
	}
	cancel()

	err := waitInvoke(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v, want context.Canceled", err)
	}
	if !strings.HasPrefix(err.Error(), "docker run: ") {
		t.Fatalf("invoke error = %q, want the one-shot wrapped form", err)
	}
	if !h.c.dead() {
		t.Fatal("a cancelled invocation must discard the container")
	}
	if got := h.c.discardReason(); got != "timeout" {
		t.Fatalf("discard reason = %q, want %q", got, "timeout")
	}
	h.assertTornDown(t)
	waitReaderExit(t, h)

	// The blocked write is demonstrably released: the peer's read returns
	// promptly now that the connection was closed.
	peerRead := make(chan error, 1)
	go func() {
		_, e := h.peer.Read(make([]byte, 1))
		peerRead <- e
	}()
	select {
	case e := <-peerRead:
		if e == nil {
			t.Fatal("peer read unexpectedly succeeded after the connection closed")
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("the blocked request write was not released by cancellation")
	}
}

// TestExecutionContainerInvokeBlockedReadCancelled verifies that the response
// Read blocked on the connection (the request was written and consumed, but the
// container never answers) is released by context cancellation with the same
// no-leak proof: Invoke returns, the reader exits, and the container is
// discarded as a timeout.
func TestExecutionContainerInvokeBlockedReadCancelled(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	// Barrier: the request frame was written and consumed, so the reader is now
	// blocked awaiting a result that will never come.
	waitRead(t, readRequestAsync(h.peer))
	cancel()

	err := waitInvoke(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v, want context.Canceled", err)
	}
	if !h.c.dead() {
		t.Fatal("a cancelled invocation must discard the container")
	}
	if got := h.c.discardReason(); got != "timeout" {
		t.Fatalf("discard reason = %q, want %q", got, "timeout")
	}
	h.assertTornDown(t)
	waitReaderExit(t, h)
}

// TestExecutionContainerCancelClaimsTimeoutBeforeMonitorDiscard pins the
// cancellation/monitor discard race (regression): the teardown claim MUST be
// recorded before the connection close that wakes the monitor, so the
// monitor's transport-loss discard loses the once-only dying CAS and the
// recorded reason stays "timeout" instead of being mislabelled
// "protocol_error".
//
// Given an in-flight invocation (request written and consumed, result never
// coming) with the monitor running, when the invocation context is cancelled,
// then the invocation reports cancellation, the container is discarded exactly
// once with reason "timeout" (one kill, one remove), and the reader is joined.
//
// The ordering is enforced by channels, never by timing: closeEntered is the
// connection barrier (the callback has reached the Close), and the branch on
// the recorded reason forces a COMPLETE monitor discard — its kill is the
// observable — before a claim taken after the close may proceed. The watchdog
// bounds only, it never orders.
func TestExecutionContainerCancelClaimsTimeoutBeforeMonitorDiscard(t *testing.T) {
	h := newGatedExecutionContainerHarness(t)
	h.startMonitor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	// Barrier: the request frame was written and consumed, so the invocation
	// is registered and pending a result that will never come — the exact
	// state a real cancellation interrupts.
	waitRead(t, readRequestAsync(h.peer))

	cancel()

	// Barrier: the cancellation callback has reached the connection Close.
	select {
	case <-h.closeEntered:
	case <-time.After(execChannelWatchdog):
		t.Fatal("cancellation never reached the connection close")
	}
	if got := h.c.discardReason(); got != reasonTimeout {
		// The teardown claim was not recorded before the connection close
		// (the regression shape). Park the callback inside its Close and force
		// a full monitor discard — the kill proves the monitor's dying CAS
		// completed — so any claim the callback takes afterwards provably
		// loses, and the reason assertion below fails deterministically
		// instead of intermittently.
		select {
		case <-h.killSignal:
		case <-time.After(execChannelWatchdog):
			t.Fatal("monitor did not discard a container it was woken from")
		}
	}
	close(h.closeRelease)

	err := waitInvoke(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v, want context.Canceled", err)
	}
	if !strings.HasPrefix(err.Error(), "docker run: ") {
		t.Fatalf("invoke error = %q, want the one-shot wrapped form", err)
	}
	if !h.c.dead() {
		t.Fatal("a cancelled invocation must discard the container")
	}
	if got := h.c.discardReason(); got != reasonTimeout {
		t.Fatalf("discard reason = %q, want %q (the monitor must not relabel a cancellation)",
			got, reasonTimeout)
	}
	if kills, removes := h.killsDone(), h.removesDone(); kills != 1 || removes != 1 {
		t.Fatalf("teardown calls: kills=%d removes=%d, want exactly one of each",
			kills, removes)
	}
	waitReaderExit(t, h)
}

// TestExecutionContainerInvokePreCancelledDoesNotDiscard covers the "before
// write" case: an invocation whose context is already cancelled never wrote a
// request, so it must report cancellation WITHOUT discarding the healthy
// container or issuing kill/remove.
func TestExecutionContainerInvokePreCancelledDoesNotDiscard(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v, want context.Canceled", err)
	}
	if h.c.dead() {
		t.Fatal("a pre-cancelled invocation must not discard a container it never wrote to")
	}
	if h.killsDone() != 0 || h.removesDone() != 0 {
		t.Fatalf("pre-cancelled invocation issued teardown: kills=%d removes=%d",
			h.killsDone(), h.removesDone())
	}
	// The container must still be reusable.
	if _, err := h.roundTrip(t, "index.run"); err != nil {
		t.Fatalf("reuse after a pre-cancelled invocation: %v", err)
	}
}

// TestExecutionContainerInvokeDeadlineExceeded verifies an invocation deadline
// (not an explicit cancel) preserves context.DeadlineExceeded and the timeout
// discard, with the reader released.
func TestExecutionContainerInvokeDeadlineExceeded(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()

	waitRead(t, readRequestAsync(h.peer)) // request written; result never comes
	err := waitInvoke(t, done)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("invoke error = %v, want context.DeadlineExceeded", err)
	}
	if !strings.HasPrefix(err.Error(), "docker run: ") {
		t.Fatalf("invoke error = %q, want the one-shot wrapped form", err)
	}
	if got := h.c.discardReason(); got != "timeout" {
		t.Fatalf("discard reason = %q, want %q", got, "timeout")
	}
	waitReaderExit(t, h)
}

// TestExecutionContainerInvokeCancellationRacesCompletion exercises the
// completion/cancellation race: the result is delivered and the context
// cancelled concurrently. Whichever side wins, the outcome must be consistent —
// a cancelled invocation discards the container, and a completed invocation must
// NOT have been closed underneath (proved by reusing the same connection).
func TestExecutionContainerInvokeCancellationRacesCompletion(t *testing.T) {
	const iterations = 40
	for i := 0; i < iterations; i++ {
		h := newExecutionContainerHarness(t)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
		}()
		req := waitRead(t, readRequestAsync(h.peer))

		// Deliver the result and cancel back to back: the reader may deliver
		// just before or just after the cancellation callback starts.
		writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
		cancel()

		err := waitInvoke(t, done)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("iteration %d: invoke error = %v, want context.Canceled", i, err)
			}
			if !h.c.dead() {
				t.Fatalf("iteration %d: a cancelled invocation left the container reusable", i)
			}
			continue
		}
		if h.c.dead() {
			t.Fatalf("iteration %d: a completed invocation must not close its container", i)
		}
		// A nil result implies the cancellation callback never closed the
		// connection: a follow-up exchange on the SAME connection must succeed.
		if _, err := h.roundTrip(t, "index.run"); err != nil {
			t.Fatalf("iteration %d: no-premature-close violated, reuse failed: %v", i, err)
		}
	}
}

// TestExecutionContainerCancelledInvocationNotReusedByPool proves the cancelled
// container is dropped by the pool: after a cancelled invocation poisons it, the
// next execute starts a FRESH container (the dead one is never leased again).
func TestExecutionContainerCancelledInvocationNotReusedByPool(t *testing.T) {
	cc := newContainerCache()
	// Pre-create both harnesses on the test goroutine: the start factory runs on
	// the executing goroutine and must not touch *testing.T.
	first := newExecutionContainerHarness(t)
	second := newExecutionContainerHarness(t)
	var mu sync.Mutex
	remaining := []*executionContainerHarness{first, second}
	start := func() (reusableContainer, error) {
		mu.Lock()
		defer mu.Unlock()
		h := remaining[0]
		remaining = remaining[1:]
		return h.c, nil
	}

	// First execute: cancel while the container is blocked awaiting a result.
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cc.executeVersion(ctx, "fn", "img", cc.appConfig("fn"), 1,
			start, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	waitRead(t, readRequestAsync(first.peer))
	cancel()

	err := waitInvoke(t, firstDone)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first execute error = %v, want context.Canceled", err)
	}
	if !first.c.dead() {
		t.Fatal("the cancelled container must be dead")
	}

	// Second execute: the pool must NOT lease the dead container; a fresh start
	// is required.
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- cc.executeVersion(context.Background(), "fn", "img", cc.appConfig("fn"), 1,
			start, "index.run", []byte(`{}`), nil)
	}()
	second.serveOne(t)
	if err := waitInvoke(t, secondDone); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	mu.Lock()
	left := len(remaining)
	mu.Unlock()
	if left != 0 {
		t.Fatalf("%d harnesses were never started; the pool reused a dead container", left)
	}
}

// TestExecutionContainerResultIDViolationDiscardsPromptly pins strict
// correlation: a result whose id does not match the in-flight invocation is a
// protocol violation delivered to that invocation, which fails (never
// ambiguously succeeds) and discards the container as protocol_error — with no
// process-exit grace period.
func TestExecutionContainerResultIDViolationDiscardsPromptly(t *testing.T) {
	h := newExecutionContainerHarness(t)
	h.startMonitor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()
	waitRead(t, readRequestAsync(h.peer))
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: "wrong-id", OK: true})

	start := time.Now()
	err := waitInvoke(t, done)
	if err == nil {
		t.Fatal("a mismatched result id must never report success")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Errorf("error = %v, want the correlation violation surfaced", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("violation took %v to resolve, want prompt invalidation (no exit grace)", elapsed)
	}
	if !h.c.dead() {
		t.Fatal("a correlation violation must discard the container")
	}
	if got := h.c.discardReason(); got != "protocol_error" {
		t.Errorf("discard reason = %q, want protocol_error", got)
	}
	waitReaderExit(t, h)
}

// TestExecutionContainerClosedOutcomeSurfacesBufferedFailure pins the resolve
// race behind a correlation violation: onDeath queues the failure BEFORE the
// discard closes c.closed, so when Invoke reaches its resolve select both the
// failure branch and the closed branch can be ready at once, and Go's select
// may commit to either. Whichever it picks, the invocation must surface the
// buffered correlation cause — never the generic "container discarded" text —
// while the discard reason stays protocol_error.
//
// Given an in-flight invocation parked in the request write (pending
// registered, select unreachable), when a mismatched result id is delivered
// and its full discard completes, then releasing the write resolves the
// invocation with the correlation violation, promptly, with the container
// dead, reason protocol_error, one kill/remove each, and the reader joined.
//
// The ordering is by channel barriers, never by timing: writeEntered proves
// Invoke entered the held Write; waitDiscardComplete proves the conn close and
// the reader join (so the earlier-queued failure is buffered and c.closed is
// closed) before releaseWrite lets Invoke proceed. The watchdogs bound only.
func TestExecutionContainerClosedOutcomeSurfacesBufferedFailure(t *testing.T) {
	h := newHeldWriteExecutionContainerHarness(t)
	h.startMonitor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()

	// Barrier: the observing conn reports the instant Invoke reaches the
	// request Write. setPending has already run, the held conn keeps the Write
	// from completing, and Invoke cannot reach its resolve select yet.
	select {
	case <-h.writeEntered:
	case <-time.After(execChannelWatchdog):
		t.Fatal("timed out waiting for Invoke to enter the request write")
	}

	// The real protocol violation: the reader rejects the wrong-id result and
	// signals the monitor, which queues the failure against the pending
	// invocation and runs the full discard. The reader direction of the pipe
	// is untouched by the held Write.
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: "wrong-id", OK: true})

	// Barrier: removalDone closes after the conn close and the reader join, so
	// the failure queued BEFORE the discard is buffered on c.fail and
	// c.closed is closed — both select branches ready, arranged deterministically
	// before Invoke may select.
	h.waitDiscardComplete(t)

	close(h.releaseWrite)

	start := time.Now()
	err := waitInvoke(t, done)
	if err == nil {
		t.Fatal("a mismatched result id must never report success")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Errorf("error = %v, want the buffered correlation violation to survive the closed outcome", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("resolution took %v, want prompt resolution (no exit grace)", elapsed)
	}
	if !h.c.dead() {
		t.Fatal("a correlation violation must discard the container")
	}
	if got := h.c.discardReason(); got != "protocol_error" {
		t.Errorf("discard reason = %q, want protocol_error", got)
	}
	h.assertTornDown(t)
	waitReaderExit(t, h)
}

// TestExecutionContainerTransportLossFailsPendingInvocation pins loss
// semantics: when the connection dies with an invocation unresolved, the
// invocation fails (never succeeds), the container is discarded, and the reader
// is joined — bounded by the monitor's short exit-attribution grace.
func TestExecutionContainerTransportLossFailsPendingInvocation(t *testing.T) {
	h := newExecutionContainerHarness(t)
	h.startMonitor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()
	waitRead(t, readRequestAsync(h.peer))

	// The bootstrap side of the connection dies mid-invocation.
	if err := h.peer.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}

	start := time.Now()
	err := waitInvoke(t, done)
	if err == nil {
		t.Fatal("transport loss with an unresolved invocation must never report success")
	}
	if !strings.Contains(err.Error(), "docker run: ") {
		t.Errorf("error = %v, want the wrapped execution failure", err)
	}
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Errorf("transport loss resolved after %v, want within the bounded exit grace", elapsed)
	}
	if !h.c.dead() {
		t.Fatal("a lost connection must discard the container")
	}
	waitReaderExit(t, h)
}

// expectReaderSignal writes one raw payload (already framed) from the peer and
// returns the reader's protocol/transport signal, failing the test on timeout.
func expectReaderSignal(t *testing.T, h *executionContainerHarness, payload []byte) error {
	t.Helper()
	writeDone := make(chan error, 1)
	go func() { writeDone <- writeFrame(h.peer, payload, maxResponseFrame) }()
	if err := <-writeDone; err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
	select {
	case err := <-h.c.protoErr:
		return err
	case err := <-h.c.connErr:
		return err
	case <-time.After(execChannelWatchdog):
		t.Fatal("reader signaled no protocol/transport violation")
		return nil
	}
}

// TestExecutionContainerReaderRejectsProtocolViolations covers the reader's
// contract directly: every malformed, unexpected, uncorrelated, duplicate, or
// oversized frame on an AUTHENTICATED connection is a fatal signal (never
// silently dropped), and the reader exits after signaling.
func TestExecutionContainerReaderRejectsProtocolViolations(t *testing.T) {
	t.Run("malformed JSON result", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		err := expectReaderSignal(t, h, []byte("not a response"))
		if !strings.Contains(err.Error(), "malformed response frame") {
			t.Errorf("signal = %v, want the malformed-frame violation", err)
		}
		waitReaderExit(t, h)
	})
	t.Run("unexpected frame type", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		err := expectReaderSignal(t, h, []byte(`{"type":"invoke","id":"x"}`))
		if !strings.Contains(err.Error(), "invalid type") {
			t.Errorf("signal = %v, want the unexpected-type violation", err)
		}
		waitReaderExit(t, h)
	})
	t.Run("result with no pending invocation", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		err := expectReaderSignal(t, h, []byte(`{"type":"result","id":"idle","ok":true}`))
		if !strings.Contains(err.Error(), "no pending invocation") {
			t.Errorf("signal = %v, want the uncorrelated-result violation", err)
		}
		waitReaderExit(t, h)
	})
	t.Run("result id mismatch", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		h.c.setPending("registered", make(chan invokeResponse, 1))
		err := expectReaderSignal(t, h, []byte(`{"type":"result","id":"other","ok":true}`))
		if !strings.Contains(err.Error(), "does not match") {
			t.Errorf("signal = %v, want the id-mismatch violation", err)
		}
		waitReaderExit(t, h)
	})
	t.Run("duplicate result for one pending invocation", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		ch := make(chan invokeResponse, 1)
		h.c.setPending("dup", ch)
		line := []byte(`{"type":"result","id":"dup","ok":true}`)
		writeDone := make(chan error, 1)
		go func() {
			// Both frames back to back while the pending channel stays
			// UNCONSUMED: the first fills it, the second must be flagged rather
			// than blocking the reader on a full channel.
			if err := writeFrame(h.peer, line, maxResponseFrame); err != nil {
				writeDone <- err
				return
			}
			writeDone <- writeFrame(h.peer, line, maxResponseFrame)
		}()
		select {
		case err := <-h.c.protoErr:
			if !strings.Contains(err.Error(), "duplicate") {
				t.Errorf("signal = %v, want the duplicate-result violation", err)
			}
		case <-time.After(execChannelWatchdog):
			t.Fatal("a duplicate result must signal a protocol violation")
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("write duplicate frame: %v", err)
		}
		select {
		case resp := <-ch:
			if !resp.OK || resp.ID != "dup" {
				t.Fatalf("first result = %+v, want ok for id dup", resp)
			}
		case <-time.After(execChannelWatchdog):
			t.Fatal("the first result must be delivered")
		}
		waitReaderExit(t, h)
	})
	t.Run("oversized response frame", func(t *testing.T) {
		h := newExecutionContainerHarness(t)
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(maxResponseFrame+1))
		writeDone := make(chan error, 1)
		go func() {
			_, err := h.peer.Write(header[:])
			writeDone <- err
		}()
		if err := <-writeDone; err != nil {
			t.Fatalf("write oversize header: %v", err)
		}
		select {
		case err := <-h.c.connErr:
			if !strings.Contains(err.Error(), "exceeds") {
				t.Errorf("signal = %v, want the size-limit rejection", err)
			}
		case <-time.After(execChannelWatchdog):
			t.Fatal("an oversized frame must signal a transport-level failure")
		}
		waitReaderExit(t, h)
	})
}

// TestExecutionContainerRequestFrameTooLargeIsCallerError pins the caller
// contract half of the size limits: an event whose request frame exceeds
// maxRequestFrame fails the invocation WITHOUT touching the healthy container.
func TestExecutionContainerRequestFrameTooLargeIsCallerError(t *testing.T) {
	h := newExecutionContainerHarness(t)

	huge := []byte(`{"pad":"` + strings.Repeat("x", maxRequestFrame) + `"}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := h.c.Invoke(ctx, "index.run", huge, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want the request-size rejection", err)
	}
	if h.c.dead() {
		t.Fatal("an oversized request is a caller-contract failure; the container stays healthy")
	}
	if h.killsDone() != 0 || h.removesDone() != 0 {
		t.Fatalf("oversized request issued teardown: kills=%d removes=%d",
			h.killsDone(), h.removesDone())
	}
}

// TestManagerShutdownDoesNotHangOnStuckChannel verifies pool/manager shutdown
// cannot hang on a container whose protocol reader is blocked: CloseContext
// discards the stuck container through the bounded teardown (which closes the
// connection and releases the reader) and returns promptly.
func TestManagerShutdownDoesNotHangOnStuckChannel(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	h := newExecutionContainerHarness(t)

	// Seed one idle container whose reader is blocked (the peer never writes),
	// then simulate shutdown.
	lease, err := m.containers.acquire(context.Background(), "fn", "img", 1,
		func() (reusableContainer, error) { return h.c, nil })
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	lease.release()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- m.CloseContext(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("CloseContext hung on a stuck channel")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v, want prompt return", elapsed)
	}
	if !h.c.dead() {
		t.Fatal("pool shutdown did not discard the stuck container")
	}
	waitReaderExit(t, h)
}

// TestManagerShutdownReleasesInflightBlockedChannel covers the shutdown edge
// the idle case cannot: a container LEASED to an in-flight invocation whose
// request Write is blocked on the connection. Manager.CloseContext must tear
// the busy container down (closing the connection releases the blocked Write),
// so BOTH the invocation and shutdown return, and the reader exits — no shutdown
// hang on in-flight channel I/O. The invocation's own context is not cancelled
// here: the connection close is what releases it, modelling the real shutdown
// path rather than the invocation-timeout path.
func TestManagerShutdownReleasesInflightBlockedChannel(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	h := newExecutionContainerHarness(t)

	// Lease one container so it is BUSY (not idle) when shutdown arrives.
	lease, err := m.containers.acquire(context.Background(), "fn", "img", 1,
		func() (reusableContainer, error) { return h.c, nil })
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	defer lease.release()

	// Start the invocation and wait until it is demonstrably blocked in the
	// connection Write (the peer never reads, so the write can never complete).
	invokeDone := make(chan error, 1)
	go func() {
		invokeDone <- lease.invoke(context.Background(), "index.run", []byte(`{}`), nil)
	}()
	select {
	case <-h.writeEntered:
	case <-time.After(execChannelWatchdog):
		t.Fatal("timed out waiting for the in-flight invocation to enter the connection write")
	}

	// Shutdown while that invocation is still blocked. CloseContext must not
	// hang, and the teardown's connection close must release the blocked write.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	start := time.Now()
	go func() { closeDone <- m.CloseContext(ctx) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("CloseContext hung while an invocation was blocked in channel I/O")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v, want prompt return", elapsed)
	}

	// The blocked invocation must also return (released by the connection
	// close), and the reader must exit. Its error is a shutdown interruption,
	// not a success; the exact text is not part of the shutdown contract.
	if err := waitInvoke(t, invokeDone); err == nil {
		t.Fatal("the invocation interrupted by shutdown must not report success")
	}
	if !h.c.dead() {
		t.Fatal("shutdown did not discard the busy container")
	}
	waitReaderExit(t, h)
	if h.killsDone() < 1 || h.removesDone() < 1 {
		t.Fatalf("shutdown teardown did not kill/remove the busy container: kills=%d removes=%d",
			h.killsDone(), h.removesDone())
	}
}

// TestExecutionContainerRemovalConfirmedOnSuccess pins the removal outcome: a
// teardown whose Docker remove succeeds (or finds the container already gone)
// reports removalConfirmed, and removalDone closes, so the pool can safely
// return the container's warm-budget slot.
func TestExecutionContainerRemovalConfirmedOnSuccess(t *testing.T) {
	h := newExecutionContainerHarness(t)

	if h.c.removalConfirmed() {
		t.Fatal("a live container must not report confirmed removal")
	}
	if !h.c.discard(reasonShutdown) {
		t.Fatal("the first discard must perform the teardown")
	}
	if !h.c.removalConfirmed() {
		t.Fatal("a successful remove must confirm removal")
	}
	select {
	case <-h.c.removalDone():
	default:
		t.Fatal("removalDone must be closed once the teardown recorded its outcome")
	}
	h.assertTornDown(t)
	// discard is idempotent: a race loser performs no teardown and does not
	// change the recorded outcome.
	if h.c.discard(reasonShutdown) {
		t.Fatal("a second discard must be a no-op")
	}
}

// TestExecutionContainerRemovalUnconfirmedOnFailure pins the failure half: when
// the Docker remove genuinely fails, the container is still poisoned (never
// reusable) and its removalDone closes, but removalConfirmed stays false so the
// pool keeps its warm-budget slot reserved rather than admitting a fresh
// container on phantom capacity.
func TestExecutionContainerRemovalUnconfirmedOnFailure(t *testing.T) {
	h := newExecutionContainerHarnessWithRemove(t, http.StatusInternalServerError)

	if !h.c.discard(reasonShutdown) {
		t.Fatal("the first discard must perform the teardown")
	}
	if !h.c.dead() {
		t.Fatal("a failed removal must still poison the container against reuse")
	}
	if h.c.removalConfirmed() {
		t.Fatal("a genuine remove failure must NOT confirm removal")
	}
	select {
	case <-h.c.removalDone():
	default:
		t.Fatal("removalDone must close even when the removal failed")
	}
	// Kill was still attempted, and exactly one remove call was made.
	if h.killsDone() < 1 || h.removesDone() != 1 {
		t.Fatalf("teardown calls: kills=%d removes=%d, want kill>=1 remove==1",
			h.killsDone(), h.removesDone())
	}
}

// TestExecutionContainerDuplicateAfterConsumeBeforeClearIsProtocolError is the
// independent-review correlation case: a duplicate result that arrives AFTER
// Invoke consumed the first result but BEFORE the pending state is cleared. A
// detector that only checks "is the buffered response channel full" misses this
// window (the channel is empty again the instant Invoke received). The
// duplicate must still be flagged as a protocol violation, the container must
// be discarded, and it must never be handed out for reuse.
func TestExecutionContainerDuplicateAfterConsumeBeforeClearIsProtocolError(t *testing.T) {
	h := newExecutionContainerHarness(t)
	ch := make(chan invokeResponse, 1)
	h.c.setPending("dup", ch)

	// First result delivered and consumed, exactly as Invoke would drain it.
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: "dup", OK: true})
	select {
	case resp := <-ch:
		if !resp.OK {
			t.Fatalf("first result = %+v, want ok", resp)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("the first result must be delivered")
	}
	// ch is now EMPTY while pending is still set: the vulnerable window.
	// The duplicate must signal a protocol error, not be silently accepted.
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: "dup", OK: true})
	var violation error
	select {
	case violation = <-h.c.protoErr:
		if !strings.Contains(violation.Error(), "duplicate") {
			t.Errorf("signal = %v, want the duplicate-result violation", violation)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("a duplicate after the response was consumed must signal a protocol violation")
	}
	waitReaderExit(t, h)

	// The observation above drained the signal buffer; re-arm it so the
	// later-started monitor observes the same violation production wires
	// directly, then require the DISCARD (teardown completion, not the
	// fatal/dead flag) even though the in-window pending state is still set: a
	// protocol violation is never routed as a resumable invocation failure.
	h.c.protoErr <- violation
	h.startMonitor()
	h.waitDiscardComplete(t)
	// ...and a discarded container is never reused: the next exchange fails
	// closed instead of running another handler.
	h.assertRefusesUse(t)
}

// TestExecutionContainerLateResultAfterCompletionDiscardsContainer pins the
// other side of the correlation window: a result arriving after the completed
// invocation already cleared its pending state is uncorrelated — a protocol
// violation that discards the now-untrusted container, while the FIRST
// invocation's confirmed ok:true result stands as a success.
func TestExecutionContainerLateResultAfterCompletionDiscardsContainer(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()
	req := waitRead(t, readRequestAsync(h.peer))
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
	// Invoke returns successfully; its deferred clearPending has run.
	if err := waitInvoke(t, done); err != nil {
		t.Fatalf("first invocation: %v", err)
	}

	// A late duplicate for the SAME id now has no pending invocation: the
	// reader signals before any monitor can consume the observation.
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
	var violation error
	select {
	case violation = <-h.c.protoErr:
		if !strings.Contains(violation.Error(), "no pending invocation") {
			t.Errorf("signal = %v, want the uncorrelated-result violation", violation)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("a result after completion must signal a protocol violation")
	}
	waitReaderExit(t, h)

	// Re-arm the drained signal for the monitor: with nothing pending it must
	// discard the container (teardown completion proves the discard, since
	// dead() also reports the already-set fatal flag).
	h.c.protoErr <- violation
	h.startMonitor()
	h.waitDiscardComplete(t)
	h.assertRefusesUse(t)
}

// TestExecutionContainerConfirmedResponseSurvivesImmediateEOF pins the
// reviewed race: the reader confirms a valid ok:true response and THEN sees
// EOF, with the monitor's failure signal also becoming ready. Invoke must
// prefer the CONFIRMED result (report success, never a retryable transport
// failure) while the now-unhealthy container is discarded so the pool cannot
// reuse it. Cancellation still wins when the context itself is cancelled — the
// deterministic subtest below.
func TestExecutionContainerConfirmedResponseSurvivesImmediateEOF(t *testing.T) {
	h := newExecutionContainerHarness(t)
	h.startMonitor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{}`), nil)
	}()
	req := waitRead(t, readRequestAsync(h.peer))
	// The result frame is fully consumed by the reader, THEN the connection
	// dies: both the confirmed response and the EOF are in flight together.
	writeResult(t, h.peer, invokeResponse{Type: frameTypeResult, ID: req.ID, OK: true})
	if err := h.peer.Close(); err != nil {
		t.Fatalf("close peer: %v", err)
	}

	err := waitInvoke(t, done)
	if err != nil {
		t.Fatalf("a confirmed ok:true response must be reported as success, not a transport failure: %v", err)
	}
	// The container observed the loss: it must be discarded (whether the reader's
	// fatal flag or the monitor's exit grace got there first) so it is never
	// reused by the pool.
	if !pollUntil(context.Background(), 6*time.Second, h.c.dead) {
		t.Fatal("the container must be discarded after its connection died")
	}
	// Never reused: a subsequent attempt on the poisoned container fails closed
	// without running a handler (the pool-level non-reuse is pinned by
	// TestExecutionContainerCancelledInvocationNotReusedByPool).
	h.waitDiscardComplete(t)
	h.assertRefusesUse(t)
	waitReaderExit(t, h)

	t.Run("cancellation wins when the context is canceled", func(t *testing.T) {
		h2 := newExecutionContainerHarness(t)
		h2.startMonitor()
		ctx2, cancel2 := context.WithCancel(context.Background())
		done2 := make(chan error, 1)
		go func() {
			done2 <- h2.c.Invoke(ctx2, "index.run", []byte(`{}`), nil)
		}()
		req2 := waitRead(t, readRequestAsync(h2.peer))
		// Cancel first and wait until the cancellation callback has torn the
		// container down: from that point the outcome is deterministic.
		cancel2()
		if !pollUntil(context.Background(), 6*time.Second, h2.c.dead) {
			t.Fatal("cancellation must discard the container")
		}
		// A response arriving now races a cancellation that has already won; it
		// must never flip the outcome to success. The write itself may fail —
		// the connection is already closed — which is the point.
		payload, err := json.Marshal(invokeResponse{Type: frameTypeResult, ID: req2.ID, OK: true})
		if err != nil {
			t.Fatalf("marshal late result: %v", err)
		}
		_ = writeFrame(h2.peer, payload, maxResponseFrame)
		err2 := waitInvoke(t, done2)
		if !errors.Is(err2, context.Canceled) {
			t.Fatalf("invoke error = %v, want context.Canceled to win", err2)
		}
		waitReaderExit(t, h2)
	})
}

package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// attachTestWatchdog bounds every wait in these tests. It is a watchdog only
// (the tests synchronize on channels and the demuxer's pending flag, never on
// this duration), sized generously for a loaded CI machine.
const attachTestWatchdog = 5 * time.Second

// observingConn wraps the client end of the attach pipe and reports the exact
// instant the synchronous request Write ENTERS, before it can block on the pipe.
// It closes the gap between "the pending response was registered" and "Conn.Write
// was actually invoked": a test can cancel only once Invoke has demonstrably
// reached the attach Write, so the cancellation provably interrupts a blocked
// write rather than racing the call. It must be installed BEFORE
// NewHijackedResponse so the buffered reader also reads through it; all other
// net.Conn behavior (Read/Close/deadlines/addresses) is the embedded conn's.
type observingConn struct {
	net.Conn
	writeEntered chan struct{}
	once         sync.Once
}

func (o *observingConn) Write(b []byte) (int, error) {
	o.once.Do(func() { close(o.writeEntered) })
	return o.Conn.Write(b)
}

// executionContainerHarness wires a real *executionContainer to a net.Pipe
// stand-in for the hijacked attach and a scripted Docker client for the
// kill/remove teardown. The pipe lets a test block the synchronous request
// Write (the peer never reads) or the demultiplexer's response Read (the peer
// reads the request but never replies), then release it deterministically by
// cancelling the invocation context — the exact production cancellation path.
// writeEntered is closed the instant Invoke reaches the attach Write (before it
// can block), so a test never has to guess whether the write is in flight.
type executionContainerHarness struct {
	c            *executionContainer
	peer         net.Conn
	br           *bufio.Reader
	reader       <-chan struct{}
	writeEntered <-chan struct{}
	cleanup      func()

	mu     sync.Mutex
	kills  int
	remove int
}

func newExecutionContainerHarness(t *testing.T) *executionContainerHarness {
	t.Helper()
	clientEnd, peerEnd := net.Pipe()
	conn := &observingConn{Conn: clientEnd, writeEntered: make(chan struct{})}
	h := &executionContainerHarness{peer: peerEnd, writeEntered: conn.writeEntered}
	h.br = bufio.NewReader(peerEnd)

	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() {
			h.mu.Lock()
			h.kills++
			h.mu.Unlock()
		}},
		dockerRoute{method: http.MethodDelete, path: "/containers/", onMatch: func() {
			h.mu.Lock()
			h.remove++
			h.mu.Unlock()
		}},
	)

	c := &executionContainer{
		cli:      cli,
		log:      testutil.DiscardLogger(),
		fn:       "test-fn",
		image:    "test-image",
		id:       "test-container",
		attach:   &client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(conn, "application/vnd.docker.multiplexed-stream")},
		demux:    newProtocolDemuxer("test-fn"),
		fail:     make(chan failEvent, 4),
		exitInfo: make(chan exitInfo, 1),
		eof:      make(chan struct{}, 1),
		protoErr: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	c.demux.protoFail = func() { signal(c.protoErr) }
	h.c = c
	h.reader = c.startOutputReader()
	h.cleanup = func() {
		c.attach.Close()
		_ = peerEnd.Close()
		select {
		case <-h.reader:
		case <-time.After(attachTestWatchdog):
		}
	}
	t.Cleanup(h.cleanup)
	return h
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

// requestRead is the outcome of reading one request frame line from the peer
// side of the attach pipe.
type requestRead struct {
	req invokeRequest
	err error
}

// readRequestAsync reads exactly one request frame line from br on its own
// goroutine (net.Pipe reads are synchronous and must not run on the test
// goroutine while an Invoke may be blocked writing).
func readRequestAsync(br *bufio.Reader) <-chan requestRead {
	ch := make(chan requestRead, 1)
	go func() {
		line, err := br.ReadString('\n')
		if err != nil {
			ch <- requestRead{err: err}
			return
		}
		var req invokeRequest
		err = json.Unmarshal([]byte(strings.TrimSuffix(line, "\n")), &req)
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
	case <-time.After(attachTestWatchdog):
		t.Fatal("timed out reading invoke request")
		return invokeRequest{}
	}
}

func waitInvoke(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(attachTestWatchdog):
		t.Fatal("timed out waiting for Invoke to return")
		return nil
	}
}

func waitReaderExit(t *testing.T, h *executionContainerHarness) {
	t.Helper()
	select {
	case <-h.reader:
	case <-time.After(attachTestWatchdog):
		t.Fatal("output reader goroutine did not exit after the attach closed")
	}
}

// serveOne reads one request frame from the peer and answers it with an OK
// response carrying the request's id. It is used when the invocation is driven
// by the pool (not by the test goroutine), so the test cannot call Invoke
// directly.
func (h *executionContainerHarness) serveOne(t *testing.T) {
	t.Helper()
	req := waitRead(t, readRequestAsync(h.br))
	if _, err := h.peer.Write(frame(1, testResponse(req.ID, true, "")+"\n")); err != nil {
		t.Fatalf("write response frame: %v", err)
	}
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
	req := waitRead(t, readRequestAsync(h.br))
	if _, err := h.peer.Write(frame(1, testResponse(req.ID, true, "")+"\n")); err != nil {
		t.Fatalf("write response frame: %v", err)
	}
	return req, waitInvoke(t, done)
}

// TestExecutionContainerInvokeNormalRequestResponse pins the unchanged happy
// path: one request line carrying the handler is written, the OK response
// frame completes the invocation, the container stays healthy, and a second
// invocation reuses the SAME attach.
func TestExecutionContainerInvokeNormalRequestResponse(t *testing.T) {
	h := newExecutionContainerHarness(t)

	req, err := h.roundTrip(t, "index.run")
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if req.Handler != "index.run" {
		t.Errorf("request handler = %q, want index.run", req.Handler)
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

// TestExecutionContainerInvokeBlockedWriteCancelled verifies that a request
// Write blocked on the attach (the peer never reads) is released by context
// cancellation: Invoke returns the wrapped context error, the container is
// discarded with the timeout reason, kill/remove are issued, the long-lived
// reader exits, and the peer observes the connection close.
func TestExecutionContainerInvokeBlockedWriteCancelled(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	// Barrier: the observing conn closes writeEntered the instant Invoke
	// reaches the attach Write, BEFORE it can block. With the peer never reading,
	// the write cannot complete, so cancelling now provably interrupts an
	// in-flight write rather than racing the call itself.
	select {
	case <-h.writeEntered:
	case <-time.After(attachTestWatchdog):
		t.Fatal("timed out waiting for Invoke to enter the attach write")
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
	if got := h.c.discardReason(); got != reasonTimeout {
		t.Fatalf("discard reason = %q, want %q", got, reasonTimeout)
	}
	h.assertTornDown(t)
	waitReaderExit(t, h)

	// The blocked write is demonstrably released: the peer's read returns
	// promptly now that the hijacked conn was closed.
	peerRead := make(chan error, 1)
	go func() {
		_, e := h.peer.Read(make([]byte, 1))
		peerRead <- e
	}()
	select {
	case e := <-peerRead:
		if e == nil {
			t.Fatal("peer read unexpectedly succeeded after the attach closed")
		}
	case <-time.After(attachTestWatchdog):
		t.Fatal("the blocked request write was not released by cancellation")
	}
}

// TestExecutionContainerInvokeBlockedReadCancelled verifies that a demultiplexer
// Read blocked on the attach (the request was written and consumed, but the
// container never answers) is released by context cancellation with the same
// no-leak proof: Invoke returns, the reader goroutine exits, and the container is
// discarded as a timeout.
func TestExecutionContainerInvokeBlockedReadCancelled(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	// Barrier: the request line was written and consumed, so the demux reader is
	// now blocked awaiting a response that will never come.
	waitRead(t, readRequestAsync(h.br))
	cancel()

	err := waitInvoke(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("invoke error = %v, want context.Canceled", err)
	}
	if !h.c.dead() {
		t.Fatal("a cancelled invocation must discard the container")
	}
	if got := h.c.discardReason(); got != reasonTimeout {
		t.Fatalf("discard reason = %q, want %q", got, reasonTimeout)
	}
	h.assertTornDown(t)
	waitReaderExit(t, h)
}

// TestExecutionContainerInvokePreCancelledDoesNotDiscard verifies the "at/before
// write" case: an invocation whose context is already cancelled when it reaches
// the container never wrote a request, so it must report cancellation WITHOUT
// discarding the (healthy) container or issuing kill/remove.
func TestExecutionContainerInvokePreCancelledDoesNotDiscard(t *testing.T) {
	h := newExecutionContainerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
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
		done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()

	waitRead(t, readRequestAsync(h.br)) // request written; response never comes
	err := waitInvoke(t, done)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("invoke error = %v, want context.DeadlineExceeded", err)
	}
	if !strings.HasPrefix(err.Error(), "docker run: ") {
		t.Fatalf("invoke error = %q, want the one-shot wrapped form", err)
	}
	if got := h.c.discardReason(); got != reasonTimeout {
		t.Fatalf("discard reason = %q, want %q", got, reasonTimeout)
	}
	waitReaderExit(t, h)
}

// TestExecutionContainerInvokeCancellationRacesCompletion exercises the
// completion/cancellation race: the response is delivered and the context
// cancelled concurrently. Whichever side wins, the outcome must be consistent —
// a cancelled invocation discards the container, and a completed invocation must
// NOT have been closed underneath (proved by reusing the same attach afterward).
func TestExecutionContainerInvokeCancellationRacesCompletion(t *testing.T) {
	const iterations = 40
	for i := 0; i < iterations; i++ {
		h := newExecutionContainerHarness(t)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- h.c.Invoke(ctx, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
		}()
		req := waitRead(t, readRequestAsync(h.br))

		// Deliver the response and cancel back to back: the demux reader may
		// route the response just before or just after the cancellation callback
		// starts.
		if _, err := h.peer.Write(frame(1, testResponse(req.ID, true, "")+"\n")); err != nil {
			t.Fatalf("iteration %d: write response: %v", i, err)
		}
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
		// A nil result must imply the cancellation callback never closed the
		// attach: a follow-up exchange on the SAME conn must still succeed.
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

	// First execute: cancel while the container is blocked awaiting a response.
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cc.executeVersion(ctx, "fn", "img", cc.appConfig("fn"), 1,
			start, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	waitRead(t, readRequestAsync(first.br))
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
			start, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
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

// TestManagerShutdownDoesNotHangOnStuckAttach verifies pool/manager shutdown
// cannot hang on a container whose attach reader is blocked: CloseContext
// discards the stuck container through the bounded, context-aware teardown
// (which closes the attach and releases the reader) and returns promptly.
func TestManagerShutdownDoesNotHangOnStuckAttach(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	h := newExecutionContainerHarness(t)

	// Seed one idle container whose attach reader is blocked (the peer never
	// writes), then simulate shutdown.
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
	case <-time.After(attachTestWatchdog):
		t.Fatal("CloseContext hung on a stuck attach")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v; want prompt return", elapsed)
	}
	if !h.c.dead() {
		t.Fatal("pool shutdown did not discard the stuck container")
	}
	waitReaderExit(t, h)
}

// TestManagerShutdownReleasesInflightBlockedAttach covers the shutdown edge the
// idle case cannot: a container LEASED to an in-flight invocation whose request
// Write is blocked on the attach. Manager.CloseContext must tear the busy
// container down (closing the attach releases the blocked Write), so BOTH the
// invocation and shutdown return, and the reader goroutine exits — no shutdown
// hang on an in-flight attach I/O. The invocation's own context is not
// cancelled here: the attach close is what releases it, modelling the real
// shutdown path rather than the invocation-timeout path.
func TestManagerShutdownReleasesInflightBlockedAttach(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	h := newExecutionContainerHarness(t)

	// Lease one container so it is BUSY (not idle) when shutdown arrives.
	lease, err := m.containers.acquire(context.Background(), "fn", "img", 1,
		func() (reusableContainer, error) { return h.c, nil })
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.release()

	// Start the invocation and wait until it is demonstrably blocked in the
	// attach Write (the peer never reads, so the write can never complete).
	invokeDone := make(chan error, 1)
	go func() {
		invokeDone <- lease.invoke(context.Background(), "index.run", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	select {
	case <-h.writeEntered:
	case <-time.After(attachTestWatchdog):
		t.Fatal("timed out waiting for the in-flight invocation to enter the attach write")
	}

	// Shutdown while that invocation is still blocked. CloseContext must not
	// hang, and the teardown's attach close must release the blocked write.
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
	case <-time.After(attachTestWatchdog):
		t.Fatal("CloseContext hung while an invocation was blocked in attach I/O")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v; want prompt return", elapsed)
	}

	// The blocked invocation must also return (released by the attach close),
	// and the reader must exit. Its error is a shutdown interruption, not a
	// success; the exact text is not part of the shutdown contract.
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

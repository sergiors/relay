package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// shutdownRecordingManager builds a Manager whose Docker client is the scripted
// one and whose client-close is wrapped to flip closed BEFORE the client is
// closed. Every route records whether it ran after that flip, so a test can
// prove no removal/listing races a closed client.
func shutdownRecordingManager(
	t *testing.T,
	closed *atomic.Bool,
	callsAfterClose *atomic.Int32,
	routes ...dockerRoute,
) *Manager {
	t.Helper()
	guard := func() {
		if closed.Load() {
			callsAfterClose.Add(1)
		}
	}
	stamped := make([]dockerRoute, 0, len(routes))
	for _, rt := range routes {
		inner := rt.onMatch
		rt.onMatch = func() {
			guard()
			if inner != nil {
				inner()
			}
		}
		stamped = append(stamped, rt)
	}
	cli := newScriptedDockerClient(t, stamped...)
	return &Manager{
		cli:        cli,
		log:        testutil.DiscardLogger(),
		containers: newContainerCache(),
		closeClient: func(c *client.Client) error {
			closed.Store(true)
			return c.Close()
		},
	}
}

// TestRemoveImageCancelledByManagerLifecycle pins that the removal context is
// derived from the manager lifecycle as well as the coordinator shutdown gate:
// cancelling the lifecycle (the worker's signal context) alone aborts a blocked
// removal, so manager cancellation always cancels even before CloseContext runs.
func TestRemoveImageCancelledByManagerLifecycle(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	var closed atomic.Bool
	var afterClose atomic.Int32
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32
	m := shutdownRecordingManager(t, &closed, &afterClose,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`,
			onMatch: func() { listCalls.Add(1) }},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]",
			onMatch: func() { deleteCalls.Add(1) }},
	)
	m.lifecycle = lifecycle

	lease, err := m.AcquireImageLease("relay-fn-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release()

	signal := signalOnRetirement(m, "relay-fn-a:v1")
	rmDone := make(chan error, 1)
	go func() { rmDone <- m.RemoveImage(context.Background(), "relay-fn-a:v1") }()
	signal.wait(t)

	// Cancelling the lifecycle (not CloseContext) must abort the blocked removal.
	cancelLifecycle()
	select {
	case err := <-rmDone:
		if !errors.Is(err, ErrManagerShuttingDown) {
			t.Fatalf("lifecycle-cancelled RemoveImage = %v, want ErrManagerShuttingDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveImage did not abort on lifecycle cancellation")
	}

	if n := listCalls.Load(); n != 0 {
		t.Fatalf("container listings = %d, want 0", n)
	}
	if n := deleteCalls.Load(); n != 0 {
		t.Fatalf("ImageRemove calls = %d, want 0", n)
	}
	if n := afterClose.Load(); n != 0 {
		t.Fatalf("Docker calls after Close = %d, want 0", n)
	}
}

// TestCloseContextAbortsBlockedRetirementBeforeClientClose is the deterministic
// channel test for lifecycle-owned removal: a RemoveImage blocked on a held
// image lease must wake on manager shutdown, abort with ErrManagerShuttingDown,
// and never issue a container listing or an ImageRemove — neither before nor
// after the Docker client closes. It also pins that the shutdown reset no longer
// wakes a remover into executing removal: releasing the lease AFTER Close
// returns still produces no Docker call.
func TestCloseContextAbortsBlockedRetirementBeforeClientClose(t *testing.T) {
	var closed atomic.Bool
	var afterClose atomic.Int32
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32

	m := shutdownRecordingManager(t, &closed, &afterClose,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`,
			onMatch: func() { listCalls.Add(1) }},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]",
			onMatch: func() { deleteCalls.Add(1) }},
	)

	lease, err := m.AcquireImageLease("relay-fn-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	signal := signalOnRetirement(m, "relay-fn-a:v1")
	rmDone := make(chan error, 1)
	go func() { rmDone <- m.RemoveImage(context.Background(), "relay-fn-a:v1") }()

	// The removal owns the retirement gate and is blocked on the held lease.
	signal.wait(t)

	// Shutdown begins: it must cancel and join the blocked removal.
	closeDone := make(chan error, 1)
	go func() { closeDone <- m.CloseContext(context.Background()) }()

	select {
	case err := <-rmDone:
		if !errors.Is(err, ErrManagerShuttingDown) {
			t.Fatalf("blocked RemoveImage on shutdown = %v, want ErrManagerShuttingDown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveImage did not abort on manager shutdown")
	}

	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if !closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := listCalls.Load(); n != 0 {
		t.Fatalf("container listings = %d, want 0 (removal must abort before the guard)", n)
	}
	if n := deleteCalls.Load(); n != 0 {
		t.Fatalf("ImageRemove calls = %d, want 0 (removal must never race the close)", n)
	}

	// The old reset() closed every retire waiter, which could have woken this
	// removal into executing against the just-closed client. The removal was
	// already joined before Close returned, and reset dropped its waiter, so the
	// release below is fully synchronous and cannot wake a remover: asserting
	// immediately is deterministic.
	lease.Release()
	if n := afterClose.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
	if n := listCalls.Load(); n != 0 {
		t.Fatalf("container listings after Close = %d, want 0", n)
	}
	if n := deleteCalls.Load(); n != 0 {
		t.Fatalf("ImageRemove calls after Close = %d, want 0", n)
	}
}

// TestRemoveImageAfterShutdownRefused pins the removal gate: once CloseContext
// has begun, a new manager-owned removal is refused with ErrManagerShuttingDown
// and issues no Docker call at all. This covers a runner async cleanup that
// fires after lifecycle cancellation.
func TestRemoveImageAfterShutdownRefused(t *testing.T) {
	var closed atomic.Bool
	var afterClose atomic.Int32
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32

	m := shutdownRecordingManager(t, &closed, &afterClose,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`,
			onMatch: func() { listCalls.Add(1) }},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]",
			onMatch: func() { deleteCalls.Add(1) }},
	)

	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if !closed.Load() {
		t.Fatal("the Docker client was never closed")
	}

	err := m.RemoveImage(context.Background(), "relay-fn-a:v1")
	if !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("RemoveImage after shutdown = %v, want ErrManagerShuttingDown", err)
	}
	if err := m.RemoveImageNow(context.Background(), "relay-fn-a:v1"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("RemoveImageNow after shutdown = %v, want ErrManagerShuttingDown", err)
	}

	if n := afterClose.Load(); n != 0 {
		t.Fatalf("Docker calls after Close = %d, want 0", n)
	}
	if n := listCalls.Load(); n != 0 {
		t.Fatalf("container listings = %d, want 0", n)
	}
	if n := deleteCalls.Load(); n != 0 {
		t.Fatalf("ImageRemove calls = %d, want 0", n)
	}
}

// TestRemoveImageBlockedRetirementJoinedOnShutdown drives the exact ordering the
// bug described: hold a lease, start RemoveImage (which blocks), Close, THEN
// release. Shutdown must have already joined the removal, so the release cannot
// wake a remover into a closed client. It asserts the join is what returns, not
// the release.
func TestRemoveImageBlockedRetirementJoinedOnShutdown(t *testing.T) {
	var closed atomic.Bool
	var afterClose atomic.Int32
	m := shutdownRecordingManager(t, &closed, &afterClose,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]"},
	)

	lease, err := m.AcquireImageLease("relay-fn-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	signal := signalOnRetirement(m, "relay-fn-a:v1")
	rmDone := make(chan error, 1)
	go func() { rmDone <- m.RemoveImage(context.Background(), "relay-fn-a:v1") }()
	signal.wait(t)

	// Close must return even though the lease is STILL held, because shutdown
	// cancels and joins the blocked removal.
	closeDone := make(chan error, 1)
	go func() { closeDone <- m.CloseContext(context.Background()) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseContext blocked on a lease-held removal; shutdown must cancel and join it")
	}

	// The removal concluded as a shutdown deferral, before the release.
	select {
	case err := <-rmDone:
		if !errors.Is(err, ErrManagerShuttingDown) {
			t.Fatalf("joined removal = %v, want ErrManagerShuttingDown", err)
		}
	default:
		t.Fatal("the blocked removal was not joined before Close returned")
	}

	// The release lands after Close returns. Shutdown already joined the removal
	// (asserted above) and reset dropped its waiter, so the release is
	// synchronous and cannot wake a remover into a Docker call; asserting
	// immediately is deterministic.
	lease.Release()
	if n := afterClose.Load(); n != 0 {
		t.Fatalf("Docker calls after Close = %d, want 0", n)
	}
}

// TestCloseContextIdempotentWithInFlightRemoval proves the shutdown sequence is
// idempotent and bounded in the presence of a removal: a second CloseContext
// returns the stored result without re-joining or racing the client.
func TestCloseContextIdempotentWithInFlightRemoval(t *testing.T) {
	var closed atomic.Bool
	var afterClose atomic.Int32
	m := shutdownRecordingManager(t, &closed, &afterClose,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]"},
	)

	lease, err := m.AcquireImageLease("relay-fn-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release()

	signal := signalOnRetirement(m, "relay-fn-a:v1")
	go func() { _ = m.RemoveImage(context.Background(), "relay-fn-a:v1") }()
	signal.wait(t)

	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("first CloseContext: %v", err)
	}
	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("second CloseContext: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close after CloseContext: %v", err)
	}
	if n := afterClose.Load(); n != 0 {
		t.Fatalf("Docker calls after Close = %d, want 0", n)
	}
}

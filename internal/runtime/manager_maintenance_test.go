package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// noopPing is the Docker ping seam used by the deferred-maintenance tests: it
// succeeds without a daemon so NewManager's client construction and lifecycle
// wiring can be exercised in a unit test.
func noopPing(context.Context, *client.Client) error { return nil }

// newDeferredTestManager builds a real NewManager (no daemon; the ping is
// injected) with deferred maintenance and a deterministic clock, returning the
// manager and the fake clock. Cleanup closes the manager.
func newDeferredTestManager(t *testing.T) (*Manager, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	m, err := NewManager(
		testutil.DiscardLogger(),
		nil,
		"test-host",
		withClock(clk.Now),
		withPing(noopPing),
		WithWarmContainerIdleTimeout(20*time.Millisecond),
		WithDeferredMaintenance(),
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, clk
}

// TestNewManagerDeferredMaintenanceDoesNotStartLoop pins the invariant the
// worker relies on: WithDeferredMaintenance makes NewManager return WITHOUT a
// running maintenance loop, and the loop only begins when StartMaintenance is
// called explicitly. It also proves the loop is genuinely wired to the cache:
// before StartMaintenance an over-age idle container is never evicted, and after
// it is.
func TestNewManagerDeferredMaintenanceDoesNotStartLoop(t *testing.T) {
	m, clk := newDeferredTestManager(t)

	m.maintMu.Lock()
	started := m.maintStarted
	m.maintMu.Unlock()
	if started {
		t.Fatal("NewManager started the maintenance loop despite WithDeferredMaintenance")
	}

	c := &ctxFakeContainer{}
	seedIdleCtxContainer(t, m.containers, "fn-a", "img-1", c)
	clk.Advance(time.Minute)

	if pollUntil(nil, 150*time.Millisecond, c.dead) {
		t.Fatal("container evicted before StartMaintenance")
	}

	m.StartMaintenance()
	if !pollUntil(nil, 2*time.Second, c.dead) {
		t.Fatal("maintenance loop did not evict after StartMaintenance")
	}
}

// TestNewManagerDefaultStartsMaintenanceEagerly is the regression guard for
// every non-worker caller: omitting WithDeferredMaintenance preserves the
// historical eager start.
func TestNewManagerDefaultStartsMaintenanceEagerly(t *testing.T) {
	m, err := NewManager(
		testutil.DiscardLogger(),
		nil,
		"test-host",
		withClock(newFakeClock().Now),
		withPing(noopPing),
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	m.maintMu.Lock()
	started := m.maintStarted
	m.maintMu.Unlock()
	if !started {
		t.Fatal("NewManager did not start the maintenance loop without WithDeferredMaintenance")
	}
	// Close joins the started loop.
	select {
	case <-m.maintDone:
		t.Fatal("maintDone closed before Close")
	default:
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-m.maintDone:
	default:
		t.Fatal("Close did not join the maintenance loop")
	}
}

// TestStartMaintenanceIdempotentBeforeAndAfterClose pins the handshake: repeated
// StartMaintenance calls launch exactly one loop (a double close of the stop
// channel would panic), Close is safe when the loop never started, and
// StartMaintenance after Close is a no-op that neither panics nor leaks a loop.
func TestStartMaintenanceIdempotentBeforeAndAfterClose(t *testing.T) {
	m, _ := newDeferredTestManager(t)

	m.StartMaintenance()
	m.StartMaintenance()

	m.maintMu.Lock()
	started := m.maintStarted
	m.maintMu.Unlock()
	if !started {
		t.Fatal("StartMaintenance did not start the loop")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The loop joined; a post-Close start must be refused (closed is set), not
	// launch a goroutine that would outlive the manager.
	m.StartMaintenance()
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCloseWithoutStartMaintenanceIsSafe pins cleanup correctness when network
// verification fails before the deferred loop is ever started: Close must
// converge (cancel lifecycle, drop the cache, close the client) without waiting
// on a loop that does not exist, and must remain idempotent.
func TestCloseWithoutStartMaintenanceIsSafe(t *testing.T) {
	m, _ := newDeferredTestManager(t)

	closed := make(chan error, 1)
	go func() { closed <- m.CloseContext(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseContext blocked with no maintenance loop started")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestStartMaintenanceConcurrentWithClose runs the start/stop handshake under
// -race to prove the maintenance mutex makes the race benign: the loop is either
// started exactly once and joined, or never started; Close never blocks and
// never double-closes.
func TestStartMaintenanceConcurrentWithClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		m, _ := newDeferredTestManager(t)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); m.StartMaintenance() }()
		go func() { defer wg.Done(); _ = m.Close() }()
		wg.Wait()
		// A post-race start is refused and the manager stays closed.
		m.StartMaintenance()
		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

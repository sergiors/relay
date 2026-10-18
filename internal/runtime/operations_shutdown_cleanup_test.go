package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// TestShutdownCleanupAdmissionGatedOnLifecycleCancellation pins the two-sided
// shutdown-cleanup gate: the shutdown-only authorization is refused while the
// manager is NOT shutting down (so it can never be used to bypass ordinary
// lifecycle cancellation in steady state), is admitted once the lifecycle is
// cancelled, and ordinary work is refused at that same point.
func TestShutdownCleanupAdmissionGatedOnLifecycleCancellation(t *testing.T) {
	// Steady state (lifecycle live): shutdown cleanup is unavailable, ordinary
	// work is admitted.
	steady := newManagerOps(nil)
	if _, _, err := steady.beginShutdownCleanup(context.Background()); !errors.Is(err, errShutdownCleanupUnavailable) {
		t.Fatalf("beginShutdownCleanup with a live lifecycle = %v, want errShutdownCleanupUnavailable", err)
	}
	_, release, err := steady.begin(context.Background())
	if err != nil {
		t.Fatalf("ordinary begin with a live lifecycle = %v, want admitted", err)
	}
	release()

	// Lifecycle cancelled: ordinary work is refused terminally, shutdown cleanup
	// is admitted on its own (uncancelled) operation context.
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	o := newManagerOps(lifecycle)
	cancelLifecycle()

	if _, _, err := o.begin(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ordinary begin after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}
	opCtx, releaseCleanup, err := o.beginShutdownCleanup(context.Background())
	if err != nil {
		t.Fatalf("beginShutdownCleanup after lifecycle cancel = %v, want admitted", err)
	}
	if opCtx.Err() != nil {
		t.Fatalf("shutdown cleanup op context = %v, want an uncancelled operation context", opCtx.Err())
	}
	if n := o.count(); n != 1 {
		t.Fatalf("admitted operations during shutdown cleanup = %d, want 1 (tracked by the ordinary count)", n)
	}
	releaseCleanup()
	if n := o.count(); n != 0 {
		t.Fatalf("admitted operations after shutdown cleanup release = %d, want 0", n)
	}
}

// TestShutdownCleanupRefusedOnceCloseBegins pins the upper bound of the
// shutdown-only authorization: once CloseContext has closed ordinary admission,
// even the shutdown cleanup is refused terminally, so no cleanup can be admitted
// under the drains and the client close.
func TestShutdownCleanupRefusedOnceCloseBegins(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	o := newManagerOps(lifecycle)
	cancelLifecycle()
	o.closeNormal()

	if _, _, err := o.beginShutdownCleanup(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("beginShutdownCleanup after close began = %v, want ErrManagerShuttingDown", err)
	}
	if _, _, err := o.begin(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ordinary begin after close began = %v, want ErrManagerShuttingDown", err)
	}
}

// TestManagerCloseDrainsInFlightShutdownCleanupBeforeClientClose is the F-028
// shutdown-service-cleanup regression at the manager boundary: a shutdown-only
// service cleanup whose Docker call has not yet unwound (it ignores manager
// cancellation, here by blocking until the test releases its own caller
// context) is STRICTLY joined by CloseContext before the Docker client closes.
// The cleanup was admitted after the lifecycle was cancelled (the production
// shutdown ordering), so CloseContext's normal cancellation does not reach it —
// only the drain can keep the client from closing beneath it.
func TestManagerCloseDrainsInFlightShutdownCleanupBeforeClientClose(t *testing.T) {
	block := newBlockUntilCancelRoute()
	ot := newOpsTestManager(t, block.route("POST", "/stop"))
	m := ot.manager

	// The worker cancels its lifecycle before running the ordered teardown steps,
	// so by the time the service cleanup runs ordinary admission is already
	// refused. Model that here: root the operation authority at a cancellable
	// lifecycle and cancel it BEFORE the cleanup is admitted.
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	m.lifecycle = lifecycle
	cancelLifecycle()

	// Ordinary service work is terminally refused once the lifecycle is gone.
	if _, err := m.ServiceContainerList(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ServiceContainerList after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}

	cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
	defer cancelCleanup()
	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- m.ShutdownStopServiceContainers(cleanupCtx, []ServiceContainer{
			{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
		})
	}()
	block.waitEntered(t)
	if n := m.opsCoord().count(); n < 1 {
		t.Fatalf("admitted operations at shutdown cleanup entry = %d, want >= 1", n)
	}

	// Close begins while the cleanup's Docker call is still in flight.
	closeDone := make(chan error, 1)
	go func() { closeDone <- m.CloseContext(expiringCloseContext()) }()

	select {
	case err := <-closeDone:
		t.Fatalf("CloseContext returned (err=%v) while a shutdown cleanup was in flight; the Docker client closed beneath it", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Once close has begun, the shutdown-only authorization is closed too.
	if _, err := m.ShutdownServiceContainerList(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ShutdownServiceContainerList after close began = %v, want ErrManagerShuttingDown", err)
	}

	// Release the in-flight cleanup; the drain then lets CloseContext finish.
	cancelCleanup()
	if err := <-cleanupDone; err == nil {
		t.Fatal("the in-flight shutdown cleanup should have surfaced its caller cancellation")
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0 (Close must join the shutdown cleanup first)", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

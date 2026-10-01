package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestImageCoordinatorAcquireRetireDrain pins the core ownership invariant: a
// new independent lease is admitted while the image is live; once retirement is
// committed no new independent lease is admitted; the drain closes only after
// every admitted lease is released.
func TestImageCoordinatorAcquireRetireDrain(t *testing.T) {
	c := newImageCoordinator()

	l1, err := c.acquire("relay-app-a:x")
	if err != nil || l1 == nil {
		t.Fatalf("first acquire = (%v, %v), want a lease", l1, err)
	}

	drained, owner := c.beginRetire("relay-app-a:x")
	if !owner {
		t.Fatal("first beginRetire must own removal")
	}
	select {
	case <-drained:
		t.Fatal("drain closed while a lease is held")
	default:
	}

	// A new independent lease is rejected once retirement is committed.
	if _, err := c.acquire("relay-app-a:x"); !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("acquire after retire = %v, want ErrImageRetiring", err)
	}

	// An existing lease may still share authority for already-started work.
	shared := l1.Share()
	if shared == nil {
		t.Fatal("Share of a held lease must succeed")
	}
	l1.Release()
	select {
	case <-drained:
		t.Fatal("drain closed while a shared lease is still held")
	default:
	}
	shared.Release()
	<-drained
}

// TestImageCoordinatorDuplicateRetireNotOwner pins that a second beginRetire does
// not own removal, so two removers can never race an ImageRemove.
func TestImageCoordinatorDuplicateRetireNotOwner(t *testing.T) {
	c := newImageCoordinator()
	if _, owner := c.beginRetire("relay-app-a:x"); !owner {
		t.Fatal("first beginRetire must own")
	}
	if _, owner := c.beginRetire("relay-app-a:x"); owner {
		t.Fatal("second beginRetire must NOT own removal")
	}
}

// TestImageCoordinatorFailedRetireRetryable pins that a failed removal clears the
// retirement gate, so the image is reusable and a later pass can retry.
func TestImageCoordinatorFailedRetireRetryable(t *testing.T) {
	c := newImageCoordinator()
	drained, owner := c.beginRetire("relay-app-a:x")
	if !owner {
		t.Fatal("beginRetire must own")
	}
	<-drained
	c.finishRetire("relay-app-a:x", false)

	if c.retired("relay-app-a:x") {
		t.Fatal("a failed retirement must clear the gate")
	}
	if _, err := c.acquire("relay-app-a:x"); err != nil {
		t.Fatalf("image must be reusable after a failed removal, got %v", err)
	}
}

// TestImageCoordinatorShareAfterReleaseRefused pins that Share cannot resurrect
// authority from a released lease (which would extend an image's life past its
// drain point).
func TestImageCoordinatorShareAfterReleaseRefused(t *testing.T) {
	c := newImageCoordinator()
	l, err := c.acquire("relay-app-a:x")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	l.Release()
	if got := l.Share(); got != nil {
		t.Fatal("Share of a released lease must return nil")
	}
}

// TestImageCoordinatorConcurrentShareRelease hammers Share/Release under -race to
// prove no reference is stranded or resurrected.
func TestImageCoordinatorConcurrentShareRelease(t *testing.T) {
	c := newImageCoordinator()
	l, err := c.acquire("relay-app-a:x")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s := l.Share(); s != nil {
				s.Release()
			}
		}()
	}
	wg.Wait()
	l.Release()

	drained, owner := c.beginRetire("relay-app-a:x")
	if !owner {
		t.Fatal("beginRetire must own")
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not close: a reference was stranded")
	}
}

// TestManagerExecuteRetiringImageRejected pins that a direct Execute against a
// retiring image is rejected with a retryable error rather than racing removal.
func TestManagerExecuteRetiringImageRejected(t *testing.T) {
	m := &Manager{hostname: "test-host"}
	// Commit retirement with no holders, then wait for the (empty) drain is not
	// needed: acquire must reject immediately.
	m.leaseCoord().beginRetire("relay-app-a:x")

	err := m.Execute(context.Background(), &Prepared{Name: "a", Image: "relay-app-a:x"}, "h", nil, nil)
	if err == nil || !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("Execute against a retiring image = %v, want wrapped ErrImageRetiring", err)
	}
}

// TestManagerSnapshotLeaseSharesUnderRetirement pins that an execution admitted
// before retirement (carrying the snapshot lease on ctx) can still SHARE the
// admitted authority while the image is retiring, so Manager.Execute would not
// be rejected by the gate. It exercises the exact admission helper Execute uses
// without needing a Docker daemon.
func TestManagerSnapshotLeaseSharesUnderRetirement(t *testing.T) {
	m := &Manager{}
	lease, err := m.AcquireImageLease("relay-app-a:x")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lease.Release()

	drained, owner := m.leaseCoord().beginRetire("relay-app-a:x")
	if !owner {
		t.Fatal("beginRetire must own")
	}
	select {
	case <-drained:
		t.Fatal("drain closed while the snapshot lease is held")
	default:
	}

	// A snapshot share is admitted even while retiring; a fresh independent
	// acquire is not. This is exactly what Execute's context-lease path relies on.
	if shared := lease.Share(); shared == nil {
		t.Fatal("snapshot share must be admitted while retiring")
	} else {
		shared.Release()
	}
	if _, err := m.AcquireImageLease("relay-app-a:x"); !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("fresh acquire while retiring = %v, want ErrImageRetiring", err)
	}
}

// TestManagerIsImageRetiring pins the diagnostic accessor.
func TestManagerIsImageRetiring(t *testing.T) {
	m := &Manager{}
	if m.IsImageRetiring("relay-app-a:x") {
		t.Fatal("a fresh image must not be retiring")
	}
	m.leaseCoord().beginRetire("relay-app-a:x")
	if !m.IsImageRetiring("relay-app-a:x") {
		t.Fatal("a committed image must report retiring")
	}
}

// TestManagerLeaseCoordSingleAuthority pins the single-per-manager invariant:
// a zero-value Manager lazily owns exactly ONE coordinator, so every admission
// and every removal is gated by the same authority, and a nil Manager is a
// programming error that panics rather than silently minting an unowned
// coordinator no shutdown would ever join.
func TestManagerLeaseCoordSingleAuthority(t *testing.T) {
	m := &Manager{}
	first := m.leaseCoord()
	if first == nil {
		t.Fatal("a zero-value Manager must lazily own a coordinator")
	}
	if again := m.leaseCoord(); again != first {
		t.Fatal("leaseCoord must return the one coordinator it owns, not a fresh one per call")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("leaseCoord on a nil Manager must panic, not mint an unowned coordinator")
		}
	}()
	(*Manager)(nil).leaseCoord()
}

// TestImageCoordinatorBeginShutdownGatesRemoval pins the lifecycle gate at the
// primitive level: before shutdown beginRemoval owns the removal; after
// beginShutdown it is refused with ErrManagerShuttingDown, so no new removal can
// begin.
func TestImageCoordinatorBeginShutdownGatesRemoval(t *testing.T) {
	c := newImageCoordinator()
	if _, owner, err := c.beginRemoval("relay-app-a:x"); err != nil || !owner {
		t.Fatalf("beginRemoval before shutdown = (owner=%v, err=%v), want owned", owner, err)
	}
	c.finishRemoval("relay-app-a:x", true)

	c.beginShutdown()
	if _, _, err := c.beginRemoval("relay-app-a:y"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("beginRemoval after shutdown = %v, want ErrManagerShuttingDown", err)
	}
	if err := c.beginRemovalOp(); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("beginRemovalOp after shutdown = %v, want ErrManagerShuttingDown", err)
	}
}

// TestImageCoordinatorWaitRetirementsJoins pins that waitRetirements blocks on a
// registered operation and returns once it concludes, and that beginShutdown
// cancels the operation's derived context (the shutdown context).
func TestImageCoordinatorWaitRetirementsJoins(t *testing.T) {
	c := newImageCoordinator()
	_, owner, err := c.beginRemoval("relay-app-a:x")
	if err != nil || !owner {
		t.Fatalf("beginRemoval = (owner=%v, err=%v), want owned", owner, err)
	}

	// The test-only hook fires the instant waitRetirements has snapshotted the
	// join channel and is about to block, so the "still parked" assertion below
	// is deterministic rather than delay-based.
	entered := make(chan struct{})
	var enteredOnce sync.Once
	c.setTestHooks(&coordinatorHooks{
		waitEntered: func() { enteredOnce.Do(func() { close(entered) }) },
	})

	waiting := make(chan struct{})
	go func() {
		c.beginShutdown()
		c.waitRetirements(context.Background())
		close(waiting)
	}()

	// Shutdown must cancel the operation's derived context.
	select {
	case <-c.shutdownContext().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("beginShutdown did not cancel the shutdown context")
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("waitRetirements never entered its join wait")
	}
	// waitRetirements parked (the operation is still in flight), so it cannot
	// have returned yet.
	select {
	case <-waiting:
		t.Fatal("waitRetirements returned before the operation concluded")
	default:
	}

	c.finishRemoval("relay-app-a:x", false)
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("waitRetirements did not return after the operation concluded")
	}
}

// TestImageCoordinatorResetDoesNotWakeWaiter pins the core regression: reset no
// longer closes retirement waiters, so it cannot wake a remover into executing
// after the manager has begun closing the Docker client. A waiter registered by
// beginRetire (a test-simulated in-progress retirement) stays open across reset.
func TestImageCoordinatorResetDoesNotWakeWaiter(t *testing.T) {
	c := newImageCoordinator()
	lease, err := c.acquire("relay-app-a:x")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	drained, owner := c.beginRetire("relay-app-a:x")
	if !owner {
		t.Fatal("beginRetire must own")
	}
	select {
	case <-drained:
		t.Fatal("drain closed while the lease is held")
	default:
	}

	c.reset()

	select {
	case <-drained:
		t.Fatal("reset must not close a retirement waiter (it could wake a removal into a closed client)")
	default:
	}
	// The lease release still closes its (now reset-removed) waiter only if the
	// waiter survived; the important invariant is that reset itself did not.
	lease.Release()
}

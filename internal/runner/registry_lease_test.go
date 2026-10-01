package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// leaseExecutor is a test Executor whose prepared-app publications carry
// REAL admitted leases from a runtime.Manager's lease coordinator, so the
// runner's snapshot pinning can be asserted against the authoritative gate
// without Docker.
type leaseExecutor struct{ mgr *runtime.Manager }

func newLeaseExecutor() *leaseExecutor { return &leaseExecutor{mgr: &runtime.Manager{}} }

func (e *leaseExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// preparedWithLease builds a PreparedApp whose publication carries a real
// admitted lease on image, mirroring the production Prepare→NewPrepared transfer.
func preparedWithLease(t *testing.T, name, image string, exec *leaseExecutor) *PreparedApp {
	t.Helper()
	pf, err := leasePrepared(exec, name, image)
	if err != nil {
		t.Fatalf("acquire lease for %s: %v", image, err)
	}
	return pf
}

// TestRegistrySnapshotPinHoldsUntilRelease is the channel-barrier test: a
// registry snapshot taken before a Replace keeps the published image's
// admission lease held, so the image's reference count stays > 0 and cannot
// drain for removal until the snapshot is released. It asserts the exact
// invariant the removal path observes (the drain channel stays open while the
// count is non-zero).
func TestRegistrySnapshotPinHoldsUntilRelease(t *testing.T) {
	exec := newLeaseExecutor()
	reg := New(nil, testutil.DiscardLogger()).Registry()

	pf := preparedWithLease(t, "a", "relay-app-a:v1", exec)
	reg.Set([]*PreparedApp{pf})

	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 1 {
		t.Fatalf("publication lease count = %d, want 1", got)
	}

	// Take a snapshot and hold its pin (as Handle does for the whole call).
	snap := reg.snapshotPinned()
	if snap.pinFor(pf) == nil {
		t.Fatal("snapshot must pin the published image")
	}
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 2 {
		t.Fatalf("lease count after snapshot = %d, want 2 (publication + snapshot pin)", got)
	}

	// Supersede the entry: its publication lease is released after the swap, but
	// the snapshot's shared pin keeps the image admitted.
	reg.Replace("a", nil)
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 1 {
		t.Fatalf("lease count after supersede = %d, want 1 (snapshot pin only)", got)
	}

	// Releasing the snapshot drops the last reference, so the image can drain.
	snap.release()
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 0 {
		t.Fatalf("lease count after snapshot release = %d, want 0", got)
	}
}

// TestRegistryReplaceReleasesSupersededPublication pins that once no snapshot
// pins a superseded entry, its publication lease is released.
func TestRegistryReplaceReleasesSupersededPublication(t *testing.T) {
	exec := newLeaseExecutor()
	reg := New(nil, testutil.DiscardLogger()).Registry()

	reg.Set([]*PreparedApp{preparedWithLease(t, "a", "relay-app-a:v1", exec)})
	reg.Replace("a", nil)
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 0 {
		t.Fatalf("lease count after supersede with no snapshot = %d, want 0", got)
	}
}

// TestHandlePinsImageAcrossDelivery is the end-to-end channel-barrier test: a
// blocking Handle holds its snapshot pin for the whole delivery, so the
// published image's lease count stays > 0 while the handler runs and returns to
// its publication-only count when Handle completes.
func TestHandlePinsImageAcrossDelivery(t *testing.T) {
	exec := &blockingExec{mgr: &runtime.Manager{}, release: make(chan struct{}), entered: make(chan struct{})}
	r := New(nil, testutil.DiscardLogger())
	reg := r.Registry()

	lease, err := exec.mgr.AcquireImageLease("relay-app-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	pf := NewPrepared(app.App{
		Name:     "a",
		Template: &app.Template{Runtime: "node24", Events: []app.EventRule{alwaysMatchRule(time.Second)}},
	}, &runtime.Prepared{Name: "a", Image: "relay-app-a:v1"}, exec)
	pf.lease = lease
	reg.Set([]*PreparedApp{pf})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Handle(context.Background(), "m", map[string]any{"status": "ok"})
	}()
	exec.waitEntered()

	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 2 {
		t.Fatalf("lease count during Handle = %d, want 2 (publication + delivery pin)", got)
	}

	close(exec.release)
	<-done
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 1 {
		t.Fatalf("lease count after Handle = %d, want 1 (publication only)", got)
	}
}

// blockingExec blocks Execute until release closes, so Handle's snapshot pin is
// observable for the duration of the delivery.
type blockingExec struct {
	mgr     *runtime.Manager
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (e *blockingExec) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *blockingExec) waitEntered() { <-e.entered }

// leasePrepared is preparedWithLease without *testing.T, so a concurrent churn
// goroutine can build a publication carrying a real admitted lease and report an
// acquire failure without calling t.Fatalf off the test goroutine.
func leasePrepared(exec *leaseExecutor, name, image string) (*PreparedApp, error) {
	lease, err := exec.mgr.AcquireImageLease(image)
	if err != nil {
		return nil, err
	}
	pf := NewPrepared(app.App{
		Name:     name,
		Template: &app.Template{Runtime: "node24", Events: []app.EventRule{alwaysMatchRule(time.Second)}},
	}, &runtime.Prepared{Name: name, Image: image}, exec)
	pf.lease = lease
	return pf, nil
}

// TestRegistrySnapshotPinConcurrentChurn runs snapshots, replacements, and
// releases concurrently under -race to prove the pin discipline never strands or
// double-releases a lease. Interleaving is by construction, not by wall clock:
// every goroutine starts at one barrier and does a BOUNDED iteration count, and
// the test joins on a WaitGroup. The final drain is asserted deterministically —
// exactly the last publication lease remains, and removing the entry releases
// it, so the image reaches zero.
func TestRegistrySnapshotPinConcurrentChurn(t *testing.T) {
	exec := newLeaseExecutor()
	r := New([]*PreparedApp{preparedWithLease(t, "a", "relay-app-a:v1", exec)}, testutil.DiscardLogger())
	reg := r.Registry()

	const iterations = 200
	start := make(chan struct{})
	buildErr := make(chan error, 1)
	var wg sync.WaitGroup

	// Concurrent snapshotters: each takes a pinned snapshot and releases it, so a
	// pin can race a supersede and a release.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				snap := reg.snapshotPinned()
				snap.release()
			}
		}()
	}

	// One superseder: every Replace installs a fresh publication with its own
	// admitted lease and releases the superseded entry's lease after the swap.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for j := 0; j < iterations; j++ {
			pf, err := leasePrepared(exec, "a", "relay-app-a:v1")
			if err != nil {
				buildErr <- err
				return
			}
			reg.Replace("a", pf)
		}
	}()

	close(start)
	wg.Wait()
	select {
	case err := <-buildErr:
		t.Fatalf("acquire lease during churn: %v", err)
	default:
	}

	// Churn is over and every snapshot pin has been released: exactly the final
	// publication lease remains.
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 1 {
		t.Fatalf("lease count after churn = %d, want 1 (last publication only)", got)
	}
	// Removing the entry releases the last publication, so the image drains
	// completely: no reference was stranded or double-released.
	reg.Replace("a", nil)
	if got := exec.mgr.LeaseCount("relay-app-a:v1"); got != 0 {
		t.Fatalf("lease count after final removal = %d, want 0 (no stranded pin)", got)
	}
}

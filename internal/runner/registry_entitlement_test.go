package runner

import (
	"context"
	"errors"
	"testing"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestRegistryPostSwapAdmissionPinsNewImageOnly is the runner-side half of the
// "A paused before image/container use, then B publication/retirement" race: a
// delivery admitted against image A holds a pinned snapshot for A's whole
// delivery; after B supersedes A in the registry, a NEW snapshot pins B's image
// and never A's, and a non-blocking retirement of A (the runner's
// RemoveImageNow shape, also used by the startup sweep and function removal) is
// DEFERRED while the in-flight snapshot still pins A. The pins are real admitted
// references from the runtime lease coordinator (via leaseExecutor), so the
// assertions are on the authoritative admission gate, not a fake counter.
func TestRegistryPostSwapAdmissionPinsNewImageOnly(t *testing.T) {
	exec := newLeaseExecutor()
	reg := New(nil, testutil.DiscardLogger()).Registry()

	const imgA = "relay-fn-a:v1"
	const imgB = "relay-fn-a:v2"

	pfA := preparedWithLease(t, "a", imgA, exec)
	reg.Set([]*PreparedFunction{pfA})

	// Delivery A takes its snapshot and pins A's image (as Handle does).
	snapA := reg.snapshotPinned()
	if snapA.pinFor(pfA) == nil {
		t.Fatal("the pre-swap snapshot must pin A's published image")
	}
	if got := exec.mgr.LeaseCount(imgA); got != 2 {
		t.Fatalf("lease count on A after the snapshot = %d, want 2 (publication + snapshot pin)", got)
	}

	// B supersedes A. A's publication lease is released after the swap, but the
	// in-flight snapshot keeps A admitted.
	pfB := preparedWithLease(t, "a", imgB, exec)
	reg.Replace("a", pfB)
	if got := exec.mgr.LeaseCount(imgA); got != 1 {
		t.Fatalf("lease count on A after the swap = %d, want 1 (the in-flight snapshot pin only)", got)
	}
	if got := exec.mgr.LeaseCount(imgB); got != 1 {
		t.Fatalf("lease count on B after the swap = %d, want 1 (B's publication)", got)
	}

	// A NEW, post-swap admission pins B ONLY.
	snapB := reg.snapshotPinned()
	if snapB.pinFor(pfB) == nil {
		t.Fatal("the post-swap snapshot must pin B's published image")
	}
	if snapB.pinFor(pfA) != nil {
		t.Fatal("the post-swap snapshot must not pin the superseded A image")
	}
	if got := exec.mgr.LeaseCount(imgA); got != 1 {
		t.Fatalf("lease count on A after the post-swap snapshot = %d, want 1 (unchanged: only the old snapshot pin)", got)
	}
	if got := exec.mgr.LeaseCount(imgB); got != 2 {
		t.Fatalf("lease count on B after the post-swap snapshot = %d, want 2 (publication + snapshot pin)", got)
	}

	// A non-blocking retirement of A (RemoveImageNow: the startup-sweep and
	// function-removal shape) must DEFER while the in-flight snapshot still
	// pins A, and must leave the image reusable.
	if err := exec.mgr.RemoveImageNow(context.Background(), imgA); !errors.Is(err, runtime.ErrImageRetiring) {
		t.Fatalf("RemoveImageNow(A) with the in-flight snapshot = %v, want ErrImageRetiring", err)
	}
	if got := exec.mgr.LeaseCount(imgA); got != 1 {
		t.Fatalf("lease count on A after the deferred removal = %d, want 1", got)
	}
	if exec.mgr.IsImageRetiring(imgA) {
		t.Fatal("a deferred non-blocking removal must clear the retirement gate")
	}

	// The old delivery completes: releasing snapA drops A's last reference, so
	// A is now removable, while B's pins are untouched by A's retirement.
	snapA.release()
	if got := exec.mgr.LeaseCount(imgA); got != 0 {
		t.Fatalf("lease count on A after the old snapshot released = %d, want 0", got)
	}
	if got := exec.mgr.LeaseCount(imgB); got != 2 {
		t.Fatalf("lease count on B after A was released = %d, want 2 (A's retirement must not touch B)", got)
	}

	snapB.release()
	if got := exec.mgr.LeaseCount(imgB); got != 1 {
		t.Fatalf("lease count on B after snapB released = %d, want 1 (B's publication)", got)
	}
	reg.Replace("a", nil)
	if got := exec.mgr.LeaseCount(imgB); got != 0 {
		t.Fatalf("lease count on B after removal = %d, want 0", got)
	}
}

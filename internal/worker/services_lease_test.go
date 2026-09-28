package worker

import (
	"testing"

	"relay/internal/runtime"
)

// TestAcquireServiceLeaseOwnershipScope pins the worker's service-lease policy:
// a Relay-owned image is leased (and the lease is traceable on the manager), an
// external service image is never leased, and a nil manager (test wiring) is a
// safe no-op. This is the "obtain a managed image lease BEFORE enqueue" seam.
func TestAcquireServiceLeaseOwnershipScope(t *testing.T) {
	mgr := &runtime.Manager{}

	if got := acquireServiceLease(nil, "fn", "relay-fn-fn:abc"); got != nil {
		t.Fatal("a nil manager must not lease an image")
	}

	if got := acquireServiceLease(mgr, "fn", "ghcr.io/acme/api:1.2"); got != nil {
		t.Fatal("an external image must never be leased")
	}
	if got := mgr.LeaseCount("ghcr.io/acme/api:1.2"); got != 0 {
		t.Fatalf("external image lease count = %d, want 0", got)
	}

	lease := acquireServiceLease(mgr, "fn", "relay-fn-fn:abcdef0123456789")
	if lease == nil {
		t.Fatal("a Relay-owned image must be leased before enqueue")
	}
	if got := mgr.LeaseCount("relay-fn-fn:abcdef0123456789"); got != 1 {
		t.Fatalf("service lease count = %d, want 1", got)
	}
	lease.Release()
	if got := mgr.LeaseCount("relay-fn-fn:abcdef0123456789"); got != 0 {
		t.Fatalf("service lease count after release = %d, want 0", got)
	}
}

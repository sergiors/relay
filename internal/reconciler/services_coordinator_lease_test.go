package reconciler

import (
	"context"
	"testing"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestServiceCoordinatorLeasedRequestHoldsLeaseThroughRun pins that an admitted
// image lease enqueued with a desired state is held across the worker pass and
// released once the operation completes.
func TestServiceCoordinatorLeasedRequestHoldsLeaseThroughRun(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	mgr := &runtime.Manager{}
	lease, err := mgr.AcquireImageLease("relay-fn-a:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got := mgr.LeaseCount("relay-fn-a:v1"); got != 1 {
		t.Fatalf("lease count after acquire = %d, want 1", got)
	}

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.EnqueueLeased("alpha", tmpl, "relay-fn-a:v1", nil, lease)
	<-docker.entered

	// The running request still holds the lease.
	if got := mgr.LeaseCount("relay-fn-a:v1"); got != 1 {
		t.Fatalf("lease count while running = %d, want 1", got)
	}

	close(docker.release)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	// The request completed: its lease was released.
	if got := mgr.LeaseCount("relay-fn-a:v1"); got != 0 {
		t.Fatalf("lease count after run = %d, want 0", got)
	}
}

// TestServiceCoordinatorCoalescedRequestReleasesLease pins that a newer desired
// state supersedes a not-yet-started pending request AND releases the superseded
// request's admitted lease, so a coalesced-away service pass never strands an
// image.
func TestServiceCoordinatorCoalescedRequestReleasesLease(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}

	// Occupy BOTH workers so this function's request stays pending.
	mgr := &runtime.Manager{}
	hold1, _ := mgr.AcquireImageLease("relay-fn-hold1:v1")
	hold2, _ := mgr.AcquireImageLease("relay-fn-hold2:v1")
	coordinator.EnqueueLeased("hold1", tmpl, "relay-fn-hold1:v1", nil, hold1)
	<-docker.entered
	coordinator.EnqueueLeased("hold2", tmpl, "relay-fn-hold2:v1", nil, hold2)
	<-docker.entered

	// alpha's first request is pending (both workers busy).
	alpha1, _ := mgr.AcquireImageLease("relay-fn-alpha:v1")
	coordinator.EnqueueLeased("alpha", tmpl, "relay-fn-alpha:v1", nil, alpha1)
	// A newer desired state coalesces it away: the superseded lease must release.
	alpha2, _ := mgr.AcquireImageLease("relay-fn-alpha:v2")
	coordinator.EnqueueLeased("alpha", tmpl, "relay-fn-alpha:v2", nil, alpha2)

	if got := mgr.LeaseCount("relay-fn-alpha:v1"); got != 0 {
		t.Fatalf("superseded request lease count = %d, want 0 (released on coalesce)", got)
	}
	if got := mgr.LeaseCount("relay-fn-alpha:v2"); got != 1 {
		t.Fatalf("newest request lease count = %d, want 1", got)
	}

	close(docker.release)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := mgr.LeaseCount("relay-fn-alpha:v2"); got != 0 {
		t.Fatalf("newest request lease count after run = %d, want 0", got)
	}
	hold1.Release()
	hold2.Release()
}

// TestServiceCoordinatorCancellationReleasesPendingLease pins that lifecycle
// cancellation releases a pending request's admitted lease.
func TestServiceCoordinatorCancellationReleasesPendingLease(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}

	mgr := &runtime.Manager{}
	hold1, _ := mgr.AcquireImageLease("relay-fn-hold1:v1")
	hold2, _ := mgr.AcquireImageLease("relay-fn-hold2:v1")
	coordinator.EnqueueLeased("hold1", tmpl, "relay-fn-hold1:v1", nil, hold1)
	<-docker.entered
	coordinator.EnqueueLeased("hold2", tmpl, "relay-fn-hold2:v1", nil, hold2)
	<-docker.entered

	pending, _ := mgr.AcquireImageLease("relay-fn-pending:v1")
	coordinator.EnqueueLeased("pending", tmpl, "relay-fn-pending:v1", nil, pending)

	cancel()
	close(docker.release)
	if err := coordinator.Join(context.Background()); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := mgr.LeaseCount("relay-fn-pending:v1"); got != 0 {
		t.Fatalf("cancelled pending request lease count = %d, want 0 (released)", got)
	}
	hold1.Release()
	hold2.Release()
}

package reconciler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/testutil"
)

// orderLog records ordered lifecycle events across a docker fake and the
// coordinator callbacks so a test can assert their happens-before relation.
type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *orderLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// gatedRemovalDocker parks RemoveAppServiceContainers until release is closed,
// recording the conclusion order around it.
type gatedRemovalDocker struct {
	*fakeDocker
	entered      chan struct{}
	release      chan struct{}
	enteredOnce  sync.Once
	order        *orderLog
	removalStart atomic.Int32
}

func newGatedRemovalDocker() *gatedRemovalDocker {
	return &gatedRemovalDocker{
		fakeDocker: newFakeDocker(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
		order:      &orderLog{},
	}
}

func (d *gatedRemovalDocker) RemoveAppServiceContainers(
	ctx context.Context, fn string,
) (int, error) {
	d.removalStart.Add(1)
	d.enteredOnce.Do(func() { close(d.entered) })
	select {
	case <-d.release:
		n, err := d.fakeDocker.RemoveAppServiceContainers(ctx, fn)
		d.order.add("remove-done " + fn)
		return n, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// The worker's app-removal hook never blocks the serial pump: the removal is
// handed off asynchronously, another app reconciles to completion WHILE the
// removal is still parked on Docker, and the image-retirement settlement runs
// only after the removal has actually concluded — in that order.
func TestServiceCoordinatorRemovalSettlementFollowsRemovalWhilePumpStaysFree(t *testing.T) {
	docker := newGatedRemovalDocker()
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{},
		testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	var settleCount atomic.Int32
	coordinator.EnqueueRemoveWithSettlement("alpha", func() {
		settleCount.Add(1)
		docker.order.add("settle alpha")
	})
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the removal never started")
	}

	// The pump handed the removal off: a second app's reconcile runs to
	// terminal completion while alpha's removal is parked.
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	betaDone := make(chan error, 1)
	coordinator.EnqueueWithStatus("beta", tmpl, "img-b", nil, nil,
		func(err error) {
			docker.order.add("beta-complete")
			select {
			case betaDone <- err:
			default:
			}
		},
	)
	select {
	case err := <-betaDone:
		if err != nil {
			t.Fatalf("beta reconcile err = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("beta never completed while the removal was parked: the pump is blocked")
	}
	if settleCount.Load() != 0 {
		t.Fatal("the settlement ran before the removal concluded")
	}

	close(docker.release)
	testutil.WaitFor(t, 3*time.Second, "settlement after the removal concluded",
		func() bool { return settleCount.Load() == 1 })

	order := docker.order.all()
	betaAt := orderIndexOf(order, "beta-complete")
	removeAt := orderIndexOf(order, "remove-done alpha")
	settleAt := orderIndexOf(order, "settle alpha")
	if betaAt < 0 || removeAt < 0 || settleAt < 0 {
		t.Fatalf("order = %v, want beta-complete, remove-done alpha, settle alpha", order)
	}
	if !(betaAt < removeAt && removeAt < settleAt) {
		t.Fatalf("order = %v, want the pump free during removal and the settlement "+
			"strictly after the removal concluded", order)
	}
	if got := docker.removalStart.Load(); got != 1 {
		t.Fatalf("removal starts = %d, want exactly 1", got)
	}
}

// A removal superseded by a re-add before any worker picks it up concludes
// WITHOUT running: its settlement fires exactly once (the reconciler's
// freshness gate then decides retirement), and no container removal is ever
// issued for the re-added app.
func TestServiceCoordinatorRemovalSupersededByReAddSettlesWithoutRemoving(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{},
		testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Errorf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})

	// Occupy both fixed workers so the removal stays queued behind them.
	coordinator.Enqueue("beta", tmpl, "img-b", nil)
	coordinator.Enqueue("gamma", tmpl, "img-g", nil)
	<-docker.entered
	<-docker.entered

	var settleCount atomic.Int32
	coordinator.EnqueueRemoveWithSettlement("alpha", func() { settleCount.Add(1) })
	// The re-add supersedes the queued removal before any worker touched it.
	coordinator.Enqueue("alpha", tmpl, "img-a", nil)
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls after the re-add = %d, want exactly 1 "+
			"(the superseded removal concludes without running)", got)
	}

	close(docker.release)
	waitCoordinator(t, coordinator)

	if got := docker.removals.Load(); got != 0 {
		t.Fatalf("container removals = %d, want 0 (a re-add supersedes the queued "+
			"removal; the reconciler's freshness gate owns retirement)", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls = %d, want exactly 1", got)
	}
}

// orderIndexOf returns the first index of event, or -1.
func orderIndexOf(events []string, event string) int {
	for i, got := range events {
		if got == event {
			return i
		}
	}
	return -1
}

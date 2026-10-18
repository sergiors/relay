package reconciler

import (
	"context"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// shutdownCapableDocker decorates fakeDocker with the optional ShutdownDocker
// capability (as the production runtime Manager has) and counts which admission
// path each shutdown-cleanup half used: the shutdown-scoped methods or the
// ordinary ones.
type shutdownCapableDocker struct {
	*fakeDocker
	ordinaryLists int
	ordinaryStops int
	shutdownLists int
	shutdownStops int
}

func (d *shutdownCapableDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.ordinaryLists++
	return d.fakeDocker.ServiceContainerList(ctx)
}

func (d *shutdownCapableDocker) StopServiceContainers(ctx context.Context, containers []runtime.ServiceContainer) error {
	d.ordinaryStops++
	return d.fakeDocker.StopServiceContainers(ctx, containers)
}

func (d *shutdownCapableDocker) ShutdownServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.shutdownLists++
	return d.fakeDocker.ServiceContainerList(ctx)
}

func (d *shutdownCapableDocker) ShutdownStopServiceContainers(ctx context.Context, containers []runtime.ServiceContainer) error {
	d.shutdownStops++
	return d.fakeDocker.StopServiceContainers(ctx, containers)
}

// ordinaryOnlyDocker decorates fakeDocker WITHOUT the ShutdownDocker capability
// and counts ordinary calls, so the fallback path can be observed.
type ordinaryOnlyDocker struct {
	*fakeDocker
	ordinaryLists int
	ordinaryStops int
}

func (d *ordinaryOnlyDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.ordinaryLists++
	return d.fakeDocker.ServiceContainerList(ctx)
}

func (d *ordinaryOnlyDocker) StopServiceContainers(ctx context.Context, containers []runtime.ServiceContainer) error {
	d.ordinaryStops++
	return d.fakeDocker.StopServiceContainers(ctx, containers)
}

// seedService adds one w-owned service container to a fakeDocker, mirroring how
// the existing shutdown tests seed other workers' containers directly.
func seedService(f *fakeDocker, id, hostname string) {
	f.ctrs[id] = &fakeContainer{
		id: id, appName: "fn", entrypoint: "svc.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning, hostname: hostname,
	}
}

// TestShutdownCleanupSelectsShutdownCapabilityWhenAvailable pins that when the
// configured Docker implements the optional ShutdownDocker capability (the
// production runtime Manager), the shutdown cleanup's discovery and removal
// halves are admitted through the shutdown-scoped methods and never fall back to
// the ordinary ones. This is what lets the cleanup run after the worker
// lifecycle was cancelled.
func TestShutdownCleanupSelectsShutdownCapabilityWhenAvailable(t *testing.T) {
	f := &shutdownCapableDocker{fakeDocker: newFakeDocker()}
	seedService(f.fakeDocker, "w1-a", "w1")
	seedService(f.fakeDocker, "w1-b", "w1")
	seedService(f.fakeDocker, "w2-a", "w2")

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	own, err := c.DiscoverShutdownContainers(context.Background(), "w1")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(own) != 2 {
		t.Fatalf("discovered %+v, want the two w1 containers", own)
	}
	if f.shutdownLists != 1 || f.ordinaryLists != 0 {
		t.Fatalf("discovery admission: shutdown=%d ordinary=%d, want shutdown=1 ordinary=0",
			f.shutdownLists, f.ordinaryLists)
	}

	if err := c.RemoveShutdownContainers(context.Background(), own); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if f.shutdownStops != 1 || f.ordinaryStops != 0 {
		t.Fatalf("removal admission: shutdown=%d ordinary=%d, want shutdown=1 ordinary=0",
			f.shutdownStops, f.ordinaryStops)
	}
	if got := f.countForApp("fn"); got != 1 {
		t.Fatalf("remaining containers = %d, want 1 (only w2 preserved)", got)
	}
}

// TestShutdownCleanupFallsBackWithoutCapability pins the other side of the
// type-assertion: a Docker that does NOT implement ShutdownDocker (a minimal
// fake or an older implementation) keeps using the ordinary list/remove methods,
// so its behavior is unchanged.
func TestShutdownCleanupFallsBackWithoutCapability(t *testing.T) {
	f := &ordinaryOnlyDocker{fakeDocker: newFakeDocker()}
	seedService(f.fakeDocker, "w1-a", "w1")

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	own, err := c.DiscoverShutdownContainers(context.Background(), "w1")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(own) != 1 {
		t.Fatalf("discovered %+v, want the single w1 container", own)
	}
	if f.ordinaryLists != 1 {
		t.Fatalf("ordinary list calls = %d, want 1 (no capability to use)", f.ordinaryLists)
	}

	if err := c.RemoveShutdownContainers(context.Background(), own); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if f.ordinaryStops != 1 {
		t.Fatalf("ordinary stop calls = %d, want 1 (no capability to use)", f.ordinaryStops)
	}
	if got := f.countForApp("fn"); got != 0 {
		t.Fatalf("remaining containers = %d, want 0", got)
	}
}

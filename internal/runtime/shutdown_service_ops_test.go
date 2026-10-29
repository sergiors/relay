package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/testutil"
)

// newLifecycleTestManager builds a Manager whose operation authority is rooted
// at lifecycle, over the scripted Docker client. lifecycle must be set before
// the first operation so opsCoord() derives its normal shutdown context from it,
// exactly as NewManager wires the worker's signal context; a cancelled
// lifecycle then refuses every ordinary entry point terminally. containers.ops
// is wired the way NewManager wires it (mgr.containers.ops = mgr.ops), so the
// execution cache shares the same single admission gate.
func newLifecycleTestManager(t *testing.T, lifecycle context.Context, routes ...dockerRoute) *Manager {
	t.Helper()
	m := &Manager{
		cli:        newScriptedDockerClient(t, routes...),
		log:        testutil.DiscardLogger(),
		containers: newContainerCache(),
		lifecycle:  lifecycle,
	}
	m.containers.ops = m.opsCoord()
	return m
}

// TestManagerOrdinaryEntryPointsRefusedAfterLifecycleCancelled pins that once
// the manager lifecycle is cancelled (what the worker does before running its
// ordered teardown steps) EVERY ordinary Manager entry point is refused
// terminally with ErrManagerShuttingDown and issues no Docker call at all (the
// scripted daemon would fail the test on any unhandled request). The shutdown
// service cleanup is the only advertised exception, and it is covered
// separately.
func TestManagerOrdinaryEntryPointsRefusedAfterLifecycleCancelled(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	m := newLifecycleTestManager(t, lifecycle) // no routes: any Docker call fails the test
	cancelLifecycle()

	ctx := context.Background()
	calls := []struct {
		name string
		call func() error
	}{
		{"NetworkExists", func() error { _, err := m.NetworkExists(ctx, "net"); return err }},
		{"ServiceContainerList", func() error { _, err := m.ServiceContainerList(ctx); return err }},
		{"StopServiceContainers", func() error {
			return m.StopServiceContainers(ctx, []ServiceContainer{
				{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
			})
		}},
		{"StartService", func() error {
			_, err := m.StartService(ctx, ServiceSpec{App: "a", Name: "web", Port: 80, Image: "alpine:3"}, 0)
			return err
		}},
		{"RestartServiceContainer", func() error { return m.RestartServiceContainer(ctx, "svc-1") }},
		{"SweepOrphanContainers", func() error { _, err := m.SweepOrphanContainers(ctx, "host"); return err }},
		{"RemoveImage", func() error { return m.RemoveImage(ctx, "relay-app-a:v1") }},
		{"Execute", func() error {
			return m.Execute(ctx, &Prepared{Name: "a", Image: "relay-app-a:x"}, "h", nil, nil)
		}},
	}
	for _, tc := range calls {
		if err := tc.call(); !errors.Is(err, ErrManagerShuttingDown) {
			t.Errorf("%s after lifecycle cancel = %v, want ErrManagerShuttingDown", tc.name, err)
		}
	}
	if n := m.opsCoord().count(); n != 0 {
		t.Fatalf("admitted operations after refused calls = %d, want 0", n)
	}
}

// TestManagerExecuteRefusesWarmCachedContainerAfterLifecycleCancelled is the
// regression for the Execute-level ordinaryRefused guard: a warm-cache acquire
// can complete WITHOUT issuing a Docker call, so it is not covered by the
// per-operation admission inside the cold create path. With a matching idle
// container already pooled, Execute after lifecycle cancellation must refuse
// with the terminal ErrManagerShuttingDown BEFORE the pooled container is handed
// back — not fall through to the cache's own admission, which reports the
// distinct (and to callers, retryable-looking) errPoolClosed.
func TestManagerExecuteRefusesWarmCachedContainerAfterLifecycleCancelled(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	m := newLifecycleTestManager(t, lifecycle)

	// Resolve a concrete generation identity without a daemon, then seed one
	// idle warm container under exactly the (image, config) version Execute
	// would otherwise lease.
	m.resolveImageIdentityFn = func(context.Context, string, string) (resolvedImage, error) {
		return resolvedImage{id: "sha256:abc", fingerprint: "fp"}, nil
	}
	prepared := &Prepared{Name: "fn", Image: "relay-app-fn:tag", Fingerprint: "fp", Concurrency: 1}
	img := m.resolveImageIdentity(context.Background(), prepared.Image, prepared.Fingerprint)
	config := m.containers.appResources(prepared.Name).Fingerprint()

	warm := &fakeContainer{}
	seedLease, err := m.containers.acquireVersion(
		context.Background(), prepared.Name, img.identity(), config, 1,
		func() (reusableContainer, error) { return warm, nil },
	)
	if err != nil {
		t.Fatalf("seed warm container: %v", err)
	}
	seedLease.release()

	cancelLifecycle()

	err = m.Execute(context.Background(), prepared, "h", nil, nil)
	if !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("Execute with a warm cached container after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}
	if errors.Is(err, errPoolClosed) {
		t.Fatal("Execute reached the cache admission instead of refusing before the warm-cache acquire")
	}
	if got := warm.calls(); got != 0 {
		t.Fatalf("warm container handled %d invocation(s), want 0 (must not be handed back)", got)
	}
	if n := m.opsCoord().count(); n != 0 {
		t.Fatalf("admitted operations after refused warm-cache execute = %d, want 0", n)
	}
}

// TestManagerShutdownServiceOpsSucceedAfterLifecycleCancelled pins the positive
// shutdown path: after the lifecycle is cancelled (ordinary admission refused),
// the shutdown-only discovery and removal run on the caller's FRESH context
// through the tracked shutdown gate — the container is listed and removed, each
// call is observed holding a non-zero operation count, and the count returns to
// zero when the call concludes.
func TestManagerShutdownServiceOpsSucceedAfterLifecycleCancelled(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	var m *Manager
	var listOps, stopOps, removeOps atomic.Int32
	m = newLifecycleTestManager(t, lifecycle,
		dockerRoute{method: http.MethodGet, path: "/containers/json",
			body:    `[{"Id":"svc-1","State":"running","Labels":{"relay.type":"service","relay.app":"a","relay.node_id":"w1"}}]`,
			onMatch: func() { listOps.Store(int32(m.opsCoord().count())) }},
		dockerRoute{method: http.MethodPost, path: "/stop",
			onMatch: func() { stopOps.Store(int32(m.opsCoord().count())) }},
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}",
			onMatch: func() { removeOps.Store(int32(m.opsCoord().count())) }},
	)
	cancelLifecycle()

	// Ordinary discovery is already refused at this point.
	if _, err := m.ServiceContainerList(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ServiceContainerList after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}

	// Shutdown discovery succeeds on the fresh step context.
	own, err := m.ShutdownServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("ShutdownServiceContainerList after lifecycle cancel: %v", err)
	}
	if len(own) != 1 || own[0].ID != "svc-1" || own[0].NodeID != "w1" {
		t.Fatalf("discovered %+v, want the single svc-1 owned by w1", own)
	}
	if n := listOps.Load(); n < 1 {
		t.Fatalf("operations at shutdown discovery = %d, want >= 1 (tracked gate)", n)
	}
	if n := m.opsCoord().count(); n != 0 {
		t.Fatalf("operations after shutdown discovery = %d, want 0", n)
	}

	// Shutdown removal stops/removes exactly the captured container.
	if err := m.ShutdownStopServiceContainers(context.Background(), own); err != nil {
		t.Fatalf("ShutdownStopServiceContainers after lifecycle cancel: %v", err)
	}
	if n := stopOps.Load(); n < 1 {
		t.Fatalf("operations at shutdown stop = %d, want >= 1 (tracked gate)", n)
	}
	if n := removeOps.Load(); n < 1 {
		t.Fatalf("operations at shutdown remove = %d, want >= 1 (tracked gate)", n)
	}
	if n := m.opsCoord().count(); n != 0 {
		t.Fatalf("operations after shutdown removal = %d, want 0", n)
	}
}

// TestManagerShutdownServiceOpsRefusedWhileLifecycleLive pins the lower bound of
// the shutdown-only authorization: while the manager is NOT shutting down, both
// shutdown service entry points are refused with errShutdownCleanupUnavailable
// and issue no Docker call, so they can never be used to bypass ordinary
// lifecycle cancellation in steady state.
func TestManagerShutdownServiceOpsRefusedWhileLifecycleLive(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	m := newLifecycleTestManager(t, lifecycle) // no routes: any Docker call fails the test

	if _, err := m.ShutdownServiceContainerList(context.Background()); !errors.Is(err, errShutdownCleanupUnavailable) {
		t.Fatalf("ShutdownServiceContainerList with a live lifecycle = %v, want errShutdownCleanupUnavailable", err)
	}
	if err := m.ShutdownStopServiceContainers(context.Background(), []ServiceContainer{
		{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
	}); !errors.Is(err, errShutdownCleanupUnavailable) {
		t.Fatalf("ShutdownStopServiceContainers with a live lifecycle = %v, want errShutdownCleanupUnavailable", err)
	}
	if n := m.opsCoord().count(); n != 0 {
		t.Fatalf("admitted operations after refused shutdown calls = %d, want 0", n)
	}
}

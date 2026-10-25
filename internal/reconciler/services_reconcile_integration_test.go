//go:build integration

// This file drives the service reconciler end to end through the reconciler's
// unexported reconcileApp against a real Docker daemon: it creates a temp
// app dir with a services template, builds/reconciles 2 replicas, scales to
// 1, then removes the dir and asserts 0 containers + the image is removed. It
// mirrors reconciler_integration_test.go's dockerManagerAdapter pattern.
package reconciler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// serviceTestHostname is the stable manager identity the service integration
// tests use. The Manager stamps it as relay.hostname on every service container
// it creates, and cleanup requires that exact label value, so it can never
// select another worker's container even on a shared daemon.
const serviceTestHostname = "test-host"

// serviceTestCleanupTimeout bounds the entire service integration cleanup: the
// container list/stop round trips, app-image listing, image removal, and the
// Manager close. It is generous for a real daemon but never unbounded.
const serviceTestCleanupTimeout = 30 * time.Second

// TestServicesReconcileIntegration reconciles an app's services end to end.
func TestServicesReconcileIntegration(t *testing.T) {
	cli := testutil.RequireDocker(t)

	root := t.TempDir()
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	// The name is derived from the test name + a nanosecond stamp so concurrent
	// runs on one daemon do not collide on app, container, or image names;
	// testutil.UniqueName keeps it within app.ValidName's 63-char cap even
	// for a long test name. It is also the ownership scope of cleanup.
	name := testutil.UniqueName(t, "svc-rec")
	m, err := runtime.NewManager(logger, nil, serviceTestHostname)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	// Register cleanup before any app directory or container exists, so a
	// partial setup still tears down whatever this test started. The helper
	// closes the Manager last, after its own Docker cleanup, so the Manager
	// APIs remain usable for the entire cleanup.
	registerServiceTestCleanup(t, m, cli, name, serviceTestHostname)

	// Manager is both the reconcile Builder and the services Docker seam.
	adapter := dockerManagerAdapter{m}

	// Write a node app dir that declares one service (port 3000).
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(serviceReconcileTemplate(3000, 2)), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "service.js"), []byte("process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}

	reg := &runner.Registry{}
	reg.Set(nil)

	// Wire the reconciler's UpdateServices/RemoveServices hooks to the services
	// reconciler, so discovery/update converge service containers and removal
	// stops them (before the images would be retired by the worker's removal
	// hook, which this test does not wire).
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger, testReconcileTimeout)
	rec := New(
		Config{
			Root:     root,
			Debounce: 20 * time.Millisecond,
			Interval: time.Hour,
			UpdateServices: func(fnName string, tmpl *app.Template, image string) {
				ctx := context.Background()
				svcCtrl.Apply(ctx, fnName, tmpl, image, nil)
			},
			RemoveServices: func(fnName string) {
				svcCtrl.Remove(context.Background(), fnName)
			},
		},
		reg,
		adapter,
		logger,
	)

	// a) Discover + prepare; reconcile starts 2 running service replicas.
	rec.reconcileApp(name)
	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("function should be discovered and prepared")
	}
	assertServiceCounts(t, m, name, 2)

	// b) Scale down: edit template replicas 2 -> 1, re-reconcile.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(serviceReconcileTemplate(3000, 1)), 0o644); err != nil {
		t.Fatalf("write scaled template: %v", err)
	}
	rec.reconcileApp(name)
	assertServiceCounts(t, m, name, 1)

	// c) Remove the dir; reconcile drops the app and the RemoveServices
	// hook stops+removes its service containers. The images would then be
	// retired by the worker's RemoveApp hook (not wired here), so this test
	// asserts the container side converges to zero.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	rec.reconcileApp(name)
	if reg.GetByName(name) != nil {
		t.Fatal("function should be removed from the registry")
	}
	assertServiceCounts(t, m, name, 0)
}

// serviceReconcileTemplate renders a node24 template declaring one service.
func serviceReconcileTemplate(port, replicas int) string {
	return "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\nservices:\n  - name: app-service\n    entrypoint: app/service.js\n    port: 3000\n    replicas: " + strconv.Itoa(replicas) + "\n"
}

// TestServicesReconcileEnvChangeReplacesContainer proves against a real daemon
// that a template `env` change replaces the persistent service container (the
// running container's Config.Env would otherwise stay stale forever, since the
// entrypoint image reference is unchanged): the container id changes and the
// replacement's Config.Env carries the new value and the matching relay.env_hash.
func TestServicesReconcileEnvChangeReplacesContainer(t *testing.T) {
	cli := testutil.RequireDocker(t)

	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	name := testutil.UniqueName(t, "svc-env")
	m, err := runtime.NewManager(logger, nil, serviceTestHostname)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	// Register cleanup before any app directory or container exists, so a
	// partial setup or an early t.Fatalf still tears down whatever started. The
	// helper selects only this unique app's containers on this manager identity,
	// removes only its label-classified app images, and closes the Manager last
	// so its APIs stay usable throughout cleanup.
	registerServiceTestCleanup(t, m, cli, name, serviceTestHostname)

	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTemplate := func(version string) {
		tmpl := "runtime: node24\nenv:\n  APP_VERSION: " + version + "\nservices:\n  - name: app-service\n    entrypoint: app/service.js\n    port: 3000\n    replicas: 1\n"
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
			t.Fatalf("write template: %v", err)
		}
	}
	writeTemplate("v1")
	if err := os.WriteFile(filepath.Join(dir, "app", "service.js"), []byte("process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}

	reg := &runner.Registry{}
	reg.Set(nil)
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger, testReconcileTimeout)

	// apply builds/prepares the app (through the adapter) and converges its
	// service containers, so each pass observes the current template's env.
	rec := New(
		Config{
			Root: root, Debounce: 20 * time.Millisecond, Interval: time.Hour,
			UpdateServices: func(fnName string, tmpl *app.Template, image string) {
				ctx := context.Background()
				svcCtrl.Apply(ctx, fnName, tmpl, image, nil)
			},
			RemoveServices: func(fnName string) {
				svcCtrl.Remove(context.Background(), fnName)
			},
		},
		reg, dockerManagerAdapter{m}, logger,
	)

	rec.reconcileApp(name)
	assertServiceCounts(t, m, name, 1)

	// Find the running container and capture its id + env.
	findContainer := func() (id string, env []string, envHash string) {
		t.Helper()
		list, err := m.ServiceContainerList(context.Background())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, c := range list {
			if c.App != name || c.State != container.StateRunning {
				continue
			}
			insp, err := cli.ContainerInspect(context.Background(), c.ID, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatalf("inspect: %v", err)
			}
			var e []string
			if insp.Container.Config != nil {
				e = insp.Container.Config.Env
			}
			return c.ID, e, c.EnvHash
		}
		t.Fatal("no running container found")
		return "", nil, ""
	}
	firstID, firstEnv, firstHash := findContainer()
	if !containsEnv(firstEnv, "APP_VERSION=v1") {
		t.Fatalf("first container env = %v, want APP_VERSION=v1", firstEnv)
	}

	// Change only the template env value; re-reconcile.
	writeTemplate("v2")
	rec.reconcileApp(name)
	assertServiceCounts(t, m, name, 1)

	secondID, secondEnv, secondHash := findContainer()
	if secondID == firstID {
		t.Fatalf("container was not replaced on an env change (same id %s); env=%v", secondID, secondEnv)
	}
	if !containsEnv(secondEnv, "APP_VERSION=v2") {
		t.Fatalf("replacement env = %v, want APP_VERSION=v2", secondEnv)
	}
	if secondHash == firstHash || secondHash == "" {
		t.Fatalf("relay.env_hash unchanged across an env change (%q); want a new non-empty hash", secondHash)
	}
}

// containsEnv reports whether env carries the exact "K=V" entry.
func containsEnv(env []string, want string) bool {
	for _, e := range env {
		if e == want {
			return true
		}
	}
	return false
}

// assertServiceCounts waits (up to 20s) until exactly want running service
// containers exist for name. The poll cadence is short because the condition is
// what matters; the 20s budget is a generous upper bound for daemon latency,
// never a delay normally paid.
func assertServiceCounts(t *testing.T, m *runtime.Manager, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		list, err := m.ServiceContainerList(context.Background())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		n := 0
		allRunning := true
		for _, c := range list {
			if c.App == name {
				if c.State != container.StateRunning {
					allRunning = false
				}
				n++
			}
		}
		if n == want && allRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	list, _ := m.ServiceContainerList(context.Background())
	got := 0
	for _, c := range list {
		if c.App == name {
			got++
		}
	}
	t.Fatalf("expected %d running service containers for %s, got %d", want, name, got)
}

// registerServiceTestCleanup registers the complete teardown for a service
// integration test. Call it immediately after creating the Manager and before
// creating any app directory, image, or container, so a partial setup or early
// t.Fatalf still cleans up whatever the test managed to start.
//
// The cleanup selects containers STRICTLY from the labels the daemon returned:
// relay.type=service proves Relay ownership, while relay.app (this test's unique
// app) and relay.hostname (this manager's identity) scope it to this test. App
// or container names alone are not ownership proof, so another test's or
// worker's service containers can never be stopped or removed. Images are
// selected through AppImageTags, which is likewise label-derived (relay.type=app
// plus a matching relay.app), so a foreign image that merely shares the
// relay-app-<name> prefix is never removed.
//
// After stopping, cleanup re-lists and verifies that no service container with
// the same strict ownership labels remains. App images are removed only when the
// initial listing succeeded, every owned container was stopped/removed without
// error, and post-stop verification found no survivor. If any of those steps
// fails, an image may still be referenced by a surviving container, so cleanup
// reports the failure and leaves the images alone instead of force-removing
// possibly-in-use references.
//
// The Manager close is deferred INSIDE the cleanup, after container and image
// cleanup and on the same bounded context, so every Manager API used here is
// still admitted. A test-scoped defer m.Close() would run before every t.Cleanup
// and leave this cleanup talking to a shut-down Manager. Every failure is
// reported with t.Errorf: cleanup problems are observable without masking an
// original test failure or aborting the rest of the cleanup.
func registerServiceTestCleanup(t *testing.T, m *runtime.Manager, cli *client.Client, name, hostname string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), serviceTestCleanupTimeout)
		defer cancel()
		defer func() {
			if err := m.CloseContext(ctx); err != nil {
				t.Errorf("cleanup: close manager: %v", err)
			}
		}()

		listed, err := m.ServiceContainerList(ctx)
		if err != nil {
			t.Errorf("cleanup: list service containers: %v", err)
			return
		}
		var owned []runtime.ServiceContainer
		for _, ct := range listed {
			if serviceContainerOwnedBy(ct.Labels, name, hostname) {
				owned = append(owned, ct)
			}
		}
		var stopErr error
		if len(owned) > 0 {
			stopErr = m.StopServiceContainers(ctx, owned)
			if stopErr != nil {
				t.Errorf("cleanup: stop service containers: %v", stopErr)
			}
		}

		// Re-list after the stop so a survivor is observable even when the stop
		// was best-effort and removed only some of its containers.
		remaining, err := m.ServiceContainerList(ctx)
		if err != nil {
			t.Errorf("cleanup: verify service containers: %v", err)
			return
		}
		var survivors []runtime.ServiceContainer
		for _, ct := range remaining {
			if serviceContainerOwnedBy(ct.Labels, name, hostname) {
				survivors = append(survivors, ct)
			}
		}
		if len(survivors) > 0 {
			ids := make([]string, 0, len(survivors))
			for _, ct := range survivors {
				ids = append(ids, ct.ID)
			}
			t.Errorf("cleanup: %d service container(s) for app %q survived cleanup: %v", len(survivors), name, ids)
		}
		// Never force-remove an image reference while a container may still use
		// it, or when the stop/verification was inconclusive.
		if stopErr != nil || len(survivors) > 0 {
			return
		}

		tags, err := m.AppImageTags(ctx, name)
		if err != nil {
			t.Errorf("cleanup: list app images: %v", err)
			return
		}
		for _, tag := range tags {
			if _, err := cli.ImageRemove(ctx, tag, client.ImageRemoveOptions{Force: true}); err != nil {
				t.Errorf("cleanup: remove image %s: %v", tag, err)
			}
		}
	})
}

// TestIntegrationShutdownCleanupHostnameScoped proves graceful-shutdown cleanup
// is hostname-scoped against a real daemon AND that it survives the production
// shutdown ordering (the worker cancels its lifecycle before running the ordered
// teardown steps, so ordinary admission is refused and only the shutdown-only
// gate is left): two managers with distinct worker identities each start a
// persistent service container; w1's lifecycle is cancelled, then ShutdownCleanup
// of w1 removes only w1's container while w2's remains (and the container list
// structurally contains ONLY relay service containers, so no unrelated container
// could ever be touched by this path).
func TestIntegrationShutdownCleanupHostnameScoped(t *testing.T) {
	testutil.RequireDocker(t)

	dcli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("raw docker client: %v", err)
	}
	t.Cleanup(func() { _ = dcli.Close() })

	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	// Derive per-run worker identities and app names so concurrent runs on
	// one daemon cannot collide on the hostname-scoped container set.
	stamp := time.Now().UnixNano()
	w1host, w2host := fmt.Sprintf("relay-it-w1-%d", stamp), fmt.Sprintf("relay-it-w2-%d", stamp)
	// The worker cancels its lifecycle BEFORE running its ordered shutdown
	// steps, so only the shutdown-only admission remains when the cleanup runs.
	// w1's lifecycle is cancelled below; w2's stays live.
	lifecycle1, cancelLifecycle1 := context.WithCancel(context.Background())
	defer cancelLifecycle1()
	m1, err := runtime.NewManager(logger, nil, w1host, runtime.WithLifecycleContext(lifecycle1))
	if err != nil {
		t.Fatalf("new manager w1: %v", err)
	}
	defer m1.Close()
	m2, err := runtime.NewManager(logger, nil, w2host)
	if err != nil {
		t.Fatalf("new manager w2: %v", err)
	}
	defer m2.Close()

	// Start one persistent service container per worker (distinct apps, a
	// long-lived sleep under the shared node:24-alpine image). Unrelated
	// non-Relay containers cannot appear in ServiceContainerList structurally
	// (the strict relay.type=service filter), so hostname preservation is the
	// complete scoping assertion; a plain no-label container sub-assertion is
	// omitted because no relay-labeled start path exists for one.
	fn1, fn2 := fmt.Sprintf("int-sc-a-%d", stamp), fmt.Sprintf("int-sc-b-%d", stamp)
	// A stop-responsive long-lived process: a plain `sleep 600` as PID 1 ignores
	// SIGTERM, so each StopServiceContainers would pay the daemon's full 10s
	// grace before SIGKILL. node with a SIGTERM handler exits immediately; the
	// container is still long-lived and the hostname-scoping assertion is
	// unchanged.
	stopResponsive := []string{"node", "-e", "process.on('SIGTERM', () => process.exit(0)); setInterval(() => {}, 1000);"}
	if _, err := m1.StartService(context.Background(), runtime.ServiceSpec{
		App:  fn1,
		Name: "svc.js", SourceRef: "svc.js",
		Port:  80,
		Image: "node:24-alpine",
		Entry: stopResponsive,
	}, 0); err != nil {
		t.Fatalf("start w1 service: %v", err)
	}
	id2, err := m2.StartService(context.Background(), runtime.ServiceSpec{
		App:  fn2,
		Name: "svc.js", SourceRef: "svc.js",
		Port:  80,
		Image: "node:24-alpine",
		Entry: stopResponsive,
	}, 0)
	if err != nil {
		t.Fatalf("start w2 service: %v", err)
	}

	// Cleanup via the raw client: the managers' clients are closed by their
	// deferred Close before t.Cleanup runs, and m1's ordinary listing is refused
	// once its lifecycle is cancelled. Scope strictly to this run's two hostname
	// identities.
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(cc, client.ContainerListOptions{All: true})
		if err != nil {
			return
		}
		for _, c := range list.Items {
			if c.Labels["relay.hostname"] == w1host || c.Labels["relay.hostname"] == w2host {
				_, _ = dcli.ContainerRemove(cc, c.ID, client.ContainerRemoveOptions{Force: true})
			}
		}
	})

	// Graceful shutdown of w1: cancel its lifecycle first (ordinary admission is
	// then refused), exactly as the worker's shutdown defer does before running
	// the ordered teardown steps.
	cancelLifecycle1()

	svcW1 := NewServiceReconciler(m1, nil, routing.TraefikConfig{}, logger, testReconcileTimeout)
	n, err := svcW1.ShutdownCleanup(context.Background(), w1host)
	if err != nil {
		t.Fatalf("shutdown cleanup w1: %v", err)
	}
	if n != 1 {
		t.Fatalf("removed count = %d, want 1", n)
	}

	// Assert against the daemon directly: m1's own listing is refused once its
	// lifecycle is cancelled.
	findByHostname := func(host string) []string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("list containers: %v", err)
		}
		var ids []string
		for _, c := range list.Items {
			if c.Labels["relay.hostname"] == host {
				ids = append(ids, c.ID)
			}
		}
		return ids
	}
	testutil.WaitFor(t, 10*time.Second, "w1 container removed by shutdown cleanup", func() bool {
		return len(findByHostname(w1host)) == 0
	})
	if got := findByHostname(w2host); len(got) != 1 || got[0] != id2 {
		t.Fatalf("w2's container must survive ShutdownCleanup of w1, got %v", got)
	}
}

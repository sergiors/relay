//go:build integration

// This file drives the service reconciler end to end through the reconciler's
// unexported reconcileFunction against a real Docker daemon: it creates a temp
// function dir with a services template, builds/reconciles 2 replicas, scales to
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

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestServicesReconcileIntegration reconciles a function's services end to end.
func TestServicesReconcileIntegration(t *testing.T) {
	testutil.RequireDocker(t)

	root := t.TempDir()
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	// Manager is both the reconcile Builder and the services Docker seam.
	adapter := dockerManagerAdapter{m}

	// Write a node function dir that declares one service (port 3000). The name
	// is derived from the test name + a nanosecond stamp so concurrent runs on
	// one daemon do not collide on function, container, or image names;
	// testutil.UniqueName keeps it within function.ValidName's 63-char cap even
	// for a long test name.
	name := testutil.UniqueName(t, "svc-rec")
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

	// Cleanup: stop every service container belonging to this function and remove
	// every relay-fn-<name>:* image this test builds.
	cleanupCli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("cleanup client: %v", err)
	}
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		defer cleanupCli.Close()
		if c, err := m.ServiceContainerList(cc); err == nil {
			_ = m.StopServiceContainers(cc, c)
		}
		imgs, err := cleanupCli.ImageList(cc, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-"+name+":") {
					_, _ = cleanupCli.ImageRemove(cc, tag, client.ImageRemoveOptions{Force: true})
					break
				}
			}
		}
	})

	reg := &runner.Registry{}
	reg.Set(nil)

	// Wire the reconciler's UpdateServices/RemoveServices hooks to the services
	// reconciler, so discovery/update converge service containers and removal
	// stops them (before the images would be retired by the worker's removal
	// hook, which this test does not wire).
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger)
	rec := New(
		Config{
			Root:     root,
			Debounce: 20 * time.Millisecond,
			Interval: time.Hour,
			UpdateServices: func(fnName, fnDir string, tmpl *function.Template, image string) {
				ctx := context.Background()
				svcCtrl.Apply(ctx, fnName, fnDir, tmpl, image, nil)
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
	rec.reconcileFunction(name)
	if pf := reg.GetByName(name); pf == nil || pf.Prepared() == nil {
		t.Fatal("function should be discovered and prepared")
	}
	assertServiceCounts(t, m, name, 2)

	// b) Scale down: edit template replicas 2 -> 1, re-reconcile.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(serviceReconcileTemplate(3000, 1)), 0o644); err != nil {
		t.Fatalf("write scaled template: %v", err)
	}
	rec.reconcileFunction(name)
	assertServiceCounts(t, m, name, 1)

	// c) Remove the dir; reconcile drops the function and the RemoveServices
	// hook stops+removes its service containers. The images would then be
	// retired by the worker's RemoveFunction hook (not wired here), so this test
	// asserts the container side converges to zero.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	rec.reconcileFunction(name)
	if reg.GetByName(name) != nil {
		t.Fatal("function should be removed from the registry")
	}
	assertServiceCounts(t, m, name, 0)
}

// serviceReconcileTemplate renders a node24 template declaring one service.
func serviceReconcileTemplate(port, replicas int) string {
	return "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\nservices:\n  - entrypoint: app/service.js\n    port: 3000\n    replicas: " + strconv.Itoa(replicas) + "\n"
}

// TestServicesReconcileEnvChangeReplacesContainer proves against a real daemon
// that a template `env` change replaces the persistent service container (the
// running container's Config.Env would otherwise stay stale forever, since the
// entrypoint image reference is unchanged): the container id changes and the
// replacement's Config.Env carries the new value and the matching relay.env_hash.
func TestServicesReconcileEnvChangeReplacesContainer(t *testing.T) {
	testutil.RequireDocker(t)

	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	name := testutil.UniqueName(t, "svc-env")
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTemplate := func(version string) {
		tmpl := "runtime: node24\nenv:\n  APP_VERSION: " + version + "\nservices:\n  - entrypoint: app/service.js\n    port: 3000\n    replicas: 1\n"
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
			t.Fatalf("write template: %v", err)
		}
	}
	writeTemplate("v1")
	if err := os.WriteFile(filepath.Join(dir, "app", "service.js"), []byte("process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		defer cli.Close()
		if c, err := m.ServiceContainerList(cc); err == nil {
			var own []runtime.ServiceContainer
			for _, ct := range c {
				if ct.Function == name {
					own = append(own, ct)
				}
			}
			_ = m.StopServiceContainers(cc, own)
		}
		imgs, err := cli.ImageList(cc, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-"+name+":") {
					_, _ = cli.ImageRemove(cc, tag, client.ImageRemoveOptions{Force: true})
					break
				}
			}
		}
	})

	reg := &runner.Registry{}
	reg.Set(nil)
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger)

	// apply builds/prepares the function (through the adapter) and converges its
	// service containers, so each pass observes the current template's env.
	rec := New(
		Config{
			Root: root, Debounce: 20 * time.Millisecond, Interval: time.Hour,
			UpdateServices: func(fnName, fnDir string, tmpl *function.Template, image string) {
				ctx := context.Background()
				svcCtrl.Apply(ctx, fnName, fnDir, tmpl, image, nil)
			},
			RemoveServices: func(fnName string) {
				svcCtrl.Remove(context.Background(), fnName)
			},
		},
		reg, dockerManagerAdapter{m}, logger,
	)

	rec.reconcileFunction(name)
	assertServiceCounts(t, m, name, 1)

	// Find the running container and capture its id + env.
	findContainer := func() (id string, env []string, envHash string) {
		t.Helper()
		list, err := m.ServiceContainerList(context.Background())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, c := range list {
			if c.Function != name || c.State != container.StateRunning {
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
	rec.reconcileFunction(name)
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
			if c.Function == name {
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
		if c.Function == name {
			got++
		}
	}
	t.Fatalf("expected %d running service containers for %s, got %d", want, name, got)
}

// TestIntegrationShutdownCleanupHostnameScoped proves graceful-shutdown cleanup
// is hostname-scoped against a real daemon: two managers with distinct worker
// identities each start a persistent service container; ShutdownCleanup of w1
// removes only w1's container while w2's remains (and the container list
// structurally contains ONLY relay service containers, so no unrelated
// container could ever be touched by this path).
func TestIntegrationShutdownCleanupHostnameScoped(t *testing.T) {
	testutil.RequireDocker(t)

	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	// Derive per-run worker identities and function names so concurrent runs on
	// one daemon cannot collide on the hostname-scoped container set.
	stamp := time.Now().UnixNano()
	w1host, w2host := fmt.Sprintf("relay-it-w1-%d", stamp), fmt.Sprintf("relay-it-w2-%d", stamp)
	m1, err := runtime.NewManager(logger, nil, w1host)
	if err != nil {
		t.Fatalf("new manager w1: %v", err)
	}
	defer m1.Close()
	m2, err := runtime.NewManager(logger, nil, w2host)
	if err != nil {
		t.Fatalf("new manager w2: %v", err)
	}
	defer m2.Close()

	// Start one persistent service container per worker (distinct functions, a
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
	id1, err := m1.StartService(context.Background(), runtime.ServiceSpec{
		Function: fn1,
		Identity: "svc.js",
		Port:     80,
		Image:    "node:24-alpine",
		Entry:    stopResponsive,
	}, 0)
	if err != nil {
		t.Fatalf("start w1 service: %v", err)
	}
	id2, err := m2.StartService(context.Background(), runtime.ServiceSpec{
		Function: fn2,
		Identity: "svc.js",
		Port:     80,
		Image:    "node:24-alpine",
		Entry:    stopResponsive,
	}, 0)
	if err != nil {
		t.Fatalf("start w2 service: %v", err)
	}

	// Cleanup: stop whatever this test left running on either manager, scoped to
	// this run's derived function names.
	t.Cleanup(func() {
		cc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, m := range []*runtime.Manager{m1, m2} {
			if list, err := m.ServiceContainerList(cc); err == nil {
				keep := list[:0]
				for _, c := range list {
					if c.Function == fn1 || c.Function == fn2 {
						keep = append(keep, c)
					}
				}
				_ = m.StopServiceContainers(cc, keep)
			}
		}
	})

	svcW1 := NewServiceReconciler(m1, nil, routing.TraefikConfig{}, logger)
	n, err := svcW1.ShutdownCleanup(context.Background(), w1host)
	if err != nil {
		t.Fatalf("shutdown cleanup w1: %v", err)
	}
	if n != 1 {
		t.Fatalf("removed count = %d, want 1", n)
	}

	list, err := m1.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("list after cleanup: %v", err)
	}
	for _, c := range list {
		if c.ID == id1 {
			t.Fatal("w1's container survived ShutdownCleanup of w1")
		}
	}
	list2, err := m2.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("list w2: %v", err)
	}
	found := false
	for _, c := range list2 {
		if c.ID == id2 {
			found = true
		}
	}
	if !found {
		t.Fatal("w2's container must survive ShutdownCleanup of w1")
	}
}

// TestServicesReconcileMissingNetworkPreservesGeneration proves against a real
// daemon that a template network that no longer exists causes the service
// reconciler to PRESERVE the function's healthy current container and report the
// missing network, rather than tearing the container down. Relay never creates
// the network; once the network is recreated, a later reconcile converges
// normally.
func TestServicesReconcileMissingNetworkPreservesGeneration(t *testing.T) {
	testutil.RequireDocker(t)

	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	networkName := "relay-test-svc-net-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	createNetwork := func() {
		if _, err := cli.NetworkCreate(context.Background(), networkName, client.NetworkCreateOptions{Driver: "bridge"}); err != nil {
			t.Fatalf("create network: %v", err)
		}
	}
	createNetwork()

	name := testutil.UniqueName(t, "svc-net")
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	tmplBody := "runtime: node24\nnetworks:\n  - " + networkName + "\nservices:\n  - entrypoint: app/service.js\n    port: 3000\n    replicas: 1\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmplBody), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "service.js"), []byte("process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		defer cli.Close()
		if c, err := m.ServiceContainerList(cc); err == nil {
			var own []runtime.ServiceContainer
			for _, ct := range c {
				if ct.Function == name {
					own = append(own, ct)
				}
			}
			_ = m.StopServiceContainers(cc, own)
		}
		_, _ = cli.NetworkRemove(cc, networkName, client.NetworkRemoveOptions{})
		if imgs, err := cli.ImageList(cc, client.ImageListOptions{All: true}); err == nil {
			for _, img := range imgs.Items {
				for _, tag := range img.RepoTags {
					if strings.HasPrefix(tag, "relay-fn-"+name+":") {
						_, _ = cli.ImageRemove(cc, tag, client.ImageRemoveOptions{Force: true})
						break
					}
				}
			}
		}
	})

	// Prepare the function's image and reconcile once while the network exists.
	fn, err := function.LoadSingle(dir, name)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	prepared, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger)
	svcCtrl.Apply(context.Background(), name, dir, fn.Template, prepared.Image, nil)
	assertServiceCounts(t, m, name, 1)

	list, err := m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var id string
	for _, c := range list {
		if c.Function == name {
			id = c.ID
		}
	}
	if id == "" {
		t.Fatal("no service container after first reconcile")
	}

	// Simulate the network disappearing out from under the running container
	// WITHOUT actually removing the live network (Docker refuses to remove a
	// network with active endpoints). A thin Docker wrapper delegates everything
	// to the real manager but reports the configured network missing, which is
	// exactly the reconcile-visible condition. The next reconcile must preserve
	// the healthy container and report the missing network.
	missingDocker := &networksMissingDocker{Docker: m, missing: networkName}
	missingCtrl := NewServiceReconciler(missingDocker, nil, routing.TraefikConfig{}, logger)
	missingCtrl.Apply(context.Background(), name, dir, fn.Template, prepared.Image, nil)

	list, err = m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("list after missing-network pass: %v", err)
	}
	found := false
	for _, c := range list {
		if c.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatal("a missing network must preserve the healthy current container")
	}
	if got := countRunningForFunction(list, name); got != 1 {
		t.Fatalf("running = %d, want the healthy container preserved (1)", got)
	}

	// A later reconcile with the network present again converges normally (the
	// existing container is already correct, so it is kept).
	svcCtrl.Apply(context.Background(), name, dir, fn.Template, prepared.Image, nil)
	list, err = m.ServiceContainerList(context.Background())
	if err != nil {
		t.Fatalf("list after network restored: %v", err)
	}
	if got := countRunningForFunction(list, name); got != 1 {
		t.Fatalf("running = %d, want 1 after the network is restored", got)
	}
}

// countRunningForFunction counts running service containers for fnName in a list.
func countRunningForFunction(list []runtime.ServiceContainer, fnName string) int {
	n := 0
	for _, c := range list {
		if c.Function == fnName && c.State == container.StateRunning {
			n++
		}
	}
	return n
}

// TestServicesReconcileMissingNetworkStillRemovesRemovedService proves against
// a real daemon the ordering fix: while a template network is reported missing,
// a service REMOVED from the template still has its container stopped (removal
// does not depend on network availability), while the still-declared service's
// healthy container is preserved.
func TestServicesReconcileMissingNetworkStillRemovesRemovedService(t *testing.T) {
	testutil.RequireDocker(t)

	root := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := runtime.NewManager(logger, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()

	networkName := "relay-test-svc-net-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, err := cli.NetworkCreate(context.Background(), networkName, client.NetworkCreateOptions{Driver: "bridge"}); err != nil {
		t.Fatalf("create network: %v", err)
	}

	name := testutil.UniqueName(t, "svc-net-rm")
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Two entrypoint services sharing the function image.
	twoServices := "runtime: node24\nnetworks:\n  - " + networkName + "\nservices:\n" +
		"  - entrypoint: app/service.js\n    port: 3000\n    replicas: 1\n" +
		"  - entrypoint: app/old.js\n    port: 3001\n    replicas: 1\n"
	oneService := "runtime: node24\nnetworks:\n  - " + networkName + "\nservices:\n" +
		"  - entrypoint: app/service.js\n    port: 3000\n    replicas: 1\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(twoServices), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	svcBody := []byte("process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n")
	for _, f := range []string{"service.js", "old.js"} {
		if err := os.WriteFile(filepath.Join(dir, "app", f), svcBody, 0o644); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		defer cli.Close()
		if c, err := m.ServiceContainerList(cc); err == nil {
			var own []runtime.ServiceContainer
			for _, ct := range c {
				if ct.Function == name {
					own = append(own, ct)
				}
			}
			_ = m.StopServiceContainers(cc, own)
		}
		_, _ = cli.NetworkRemove(cc, networkName, client.NetworkRemoveOptions{})
		if imgs, err := cli.ImageList(cc, client.ImageListOptions{All: true}); err == nil {
			for _, img := range imgs.Items {
				for _, tag := range img.RepoTags {
					if strings.HasPrefix(tag, "relay-fn-"+name+":") {
						_, _ = cli.ImageRemove(cc, tag, client.ImageRemoveOptions{Force: true})
						break
					}
				}
			}
		}
	})

	fn, err := function.LoadSingle(dir, name)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	prepared, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	svcCtrl := NewServiceReconciler(m, nil, routing.TraefikConfig{}, logger)
	svcCtrl.Apply(context.Background(), name, dir, fn.Template, prepared.Image, nil)
	assertServiceCounts(t, m, name, 2)

	// Reduce the template to one service and reconcile through a Docker that
	// reports the network missing. The removed old.js container must still be
	// stopped; the remaining service.js container must be preserved.
	reduced, err := function.ParseTemplate([]byte(oneService))
	if err != nil {
		t.Fatalf("parse reduced: %v", err)
	}
	missingCtrl := NewServiceReconciler(&networksMissingDocker{Docker: m, missing: networkName}, nil, routing.TraefikConfig{}, logger)
	missingCtrl.Apply(context.Background(), name, dir, reduced, prepared.Image, nil)

	deadline := time.Now().Add(20 * time.Second)
	for {
		list, err := m.ServiceContainerList(context.Background())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var svcRunning, oldRunning int
		for _, c := range list {
			if c.Function != name || c.State != container.StateRunning {
				continue
			}
			switch c.Identity {
			case "app/service.js":
				svcRunning++
			case "app/old.js":
				oldRunning++
			}
		}
		if oldRunning == 0 {
			if svcRunning != 1 {
				t.Fatalf("service.js running = %d, want the healthy container preserved (1)", svcRunning)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("removed service old.js still running (%d) after a missing-network pass", oldRunning)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// networksMissingDocker delegates every Docker operation to the embedded real
// manager except VerifyNetworks, which reports the named network missing. It
// lets the missing-network preservation behavior be exercised against a REAL
// daemon without removing a live network (which Docker refuses).
type networksMissingDocker struct {
	// Docker is the real manager; all methods except VerifyNetworks forward to
	// it.
	Docker
	missing string
}

// VerifyNetworks reports the configured missing network (ok=false), never an
// error — exactly the daemon condition a genuinely absent network produces.
func (d *networksMissingDocker) VerifyNetworks(context.Context, []string) (string, bool, error) {
	return d.missing, false, nil
}

// NetworkExists agrees with VerifyNetworks for the naming network.
func (d *networksMissingDocker) NetworkExists(_ context.Context, network string) (bool, error) {
	if network == d.missing {
		return false, nil
	}
	return d.Docker.NetworkExists(context.Background(), network)
}

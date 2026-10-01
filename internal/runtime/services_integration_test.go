//go:build integration

// This file exercises the persistent service-container operations against a
// real Docker daemon: service container lifecycle (start/list/stop/remove),
// image retirement with in-flight safety (never remove an image a service
// container depends on), and the container env/hardening.
package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// buildServiceHost builds a tiny node24 app directory containing a
// long-lived service.js (keeps node alive with an interval) alongside a trivial
// invocation handler, then m.Prepare-builds its image and returns the app
// handle plus the prepared image ref.
func buildServiceHost(t *testing.T, ctx context.Context, fnName string) (app.App, string) {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('hi'); }\n")
	// Long-lived service entrypoint in a nested subdirectory: an interval keeps
	// node alive indefinitely. The file lives at <dir>/app/service.js, which the
	// image COPYies to /app/app/service.js (the app dir is the /app root).
	//
	// The handler stops on SIGTERM (Docker's stop signal). Without it node as PID
	// 1 ignores SIGTERM (the kernel reserves default signal actions for PID 1),
	// so every StopServiceContainers call would pay the daemon's full 10s
	// SIGKILL grace period — 10s of dead time per container per test that proves
	// nothing about Relay's lifecycle. A real long-lived service handles SIGTERM;
	// the container-state transitions under test are identical either way.
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	writeFile(t, dir, "app/service.js", `
process.on("SIGTERM", () => process.exit(0));
console.log("started");
setInterval(() => {}, 1 << 30);
`)
	fn := app.App{Name: fnName, Dir: dir, Template: &app.Template{Runtime: "node24"}}
	prepared, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare %s: %v", fnName, err)
	}
	return fn, prepared.Image
}

// TestIntegrationPythonServiceModuleExecution builds a tiny python3.14 app
// whose service entrypoint is app/main.py (a nested package module with a
// package-relative import from app/deps.py), starts one replica via the module
// execution entrypoint (python -m app.main), and asserts the container actually
// ran the module: its Config.Entrypoint is ["python","-m","app.main"] and the
// logs contain the value the module imported with a relative import — the whole
// point of module (not script) execution. No requirements.txt keeps the build to
// just the base image.
func TestIntegrationPythonServiceModuleExecution(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scope cleanup to THIS test's app: stopping every service container
		// on the daemon would also stop unrelated ones (e.g. a service a local
		// worker owns), and a container without a SIGTERM handler costs the
		// daemon's full 10s SIGKILL grace.
		_, _ = m.RemoveAppServiceContainers(cc, "svc-py-svc")
		cleanupImagePrefixes(cli, "relay-app-svc-py-svc:")()
	})

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.py", "def hi(e): return 'hi'\n")
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	// app/__init__.py marks app as a package so the relative import resolves.
	writeFile(t, dir, "app/__init__.py", "")
	writeFile(t, dir, "app/deps.py", "NAME = 'from-relative-import-ok'\n")
	// main.py imports a value with a PACKAGE-RELATIVE import and prints it before
	// keeping the process alive forever. Printing proves python executed the file
	// as an importable module, not as a bare script (a bare script run of
	// app/main.py cannot resolve `.deps`).
	//
	// SIGTERM is handled so Docker's stop is prompt: python as PID 1 ignores the
	// default SIGTERM action, so without this handler the daemon would wait its
	// full 10s grace period before SIGKILL on every stop. The module-execution
	// proof (<entrypoint> and the relative import) is independent of how the
	// process is later stopped.
	writeFile(t, dir, "app/main.py", `import signal
from .deps import NAME

def _on_sigterm(_signum, _frame):
    raise SystemExit(0)

signal.signal(signal.SIGTERM, _on_sigterm)

# stdout is block-buffered when not attached to a TTY, so flush explicitly
# before the keep-alive blocks forever -- otherwise the proof never reaches us.
print("module-entrypoint-started " + NAME, flush=True)

def keep_alive():
    import time
    while True:
        time.sleep(86400)

keep_alive()
`)
	fn := app.App{Name: "svc-py-svc", Dir: dir, Template: &app.Template{Runtime: "python3.14"}}
	prepared, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Resolve the entry through the same translation the reconciler uses.
	entry, err := ServiceEntry("python3.14", "app/main.py")
	if err != nil {
		t.Fatalf("service entry: %v", err)
	}
	if len(entry) != 3 || entry[0] != "python" || entry[1] != "-m" || entry[2] != "app.main" {
		t.Fatalf("service entry = %v, want [python -m app.main]", entry)
	}

	id, err := m.StartService(ctx, ServiceSpec{
		App:  "svc-py-svc",
		Name: "svc", SourceRef: "app/main.py",
		Port:  8000,
		Image: prepared.Image,
		Entry: entry,
		Env:   []string{"PORT=8000"},
	}, 0)
	if err != nil {
		t.Fatalf("start service: %v", err)
	}

	// Wait for the container to be running, then assert its entrypoint.
	waitForContainerRunning(t, ctx, cli, id)
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if insp.Container.Config == nil || len(insp.Container.Config.Entrypoint) != 3 ||
		insp.Container.Config.Entrypoint[0] != "python" ||
		insp.Container.Config.Entrypoint[1] != "-m" ||
		insp.Container.Config.Entrypoint[2] != "app.main" {
		t.Fatalf("entrypoint = %v, want [python -m app.main]", insp.Container.Config.Entrypoint)
	}

	// Poll the logs until the proof line appears. The entrypoint assertion
	// above already proves the container was created with the module form, but
	// State.Running flips true the instant the process starts — BEFORE the
	// interpreter has booted, imported the package, and flushed the proof line.
	// A single immediate fetch races that startup work (observed on CI: the
	// fetched log was empty while the container was healthy), so fetch on a
	// short deadline and require the proof to appear, not merely the container
	// to be running.
	const proof = "module-entrypoint-started from-relative-import-ok"
	var logs string
	if !pollUntil(ctx, 30*time.Second, func() bool {
		rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
		if err != nil {
			t.Fatalf("logs: %v", err)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, rc)
		rc.Close()
		logs = buf.String()
		return strings.Contains(logs, proof)
	}) {
		t.Fatalf("container logs do not show the module-entrypoint + relative-import proof within 30s, got:\n%s", logs)
	}
}

func TestIntegrationServiceStartListStop(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Clean up any containers this test's app ever starts, even on failure,
	// plus the images it builds.
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scope cleanup to THIS test's app (see the note in the python
		// service test): stopping unrelated daemon services can cost a 10s
		// SIGKILL grace and is not this test's to tear down.
		_, _ = m.RemoveAppServiceContainers(cc, "svc-lifecycle")
		cleanupImagePrefixes(cli, "relay-app-svc-lifecycle:")()
	})

	_, image := buildServiceHost(t, ctx, "svc-lifecycle")

	// Start ONE replica. The test's subject is the per-container contract
	// (labels, hardening, user, entrypoint, env) and the list/stop lifecycle,
	// all of which one container proves exactly; a second replica would only
	// duplicate the same inspect assertions and add another 10s-or-less
	// start/stop cycle without changing what is proven.
	const replicas = 1
	for i := 0; i < replicas; i++ {
		if _, err := m.StartService(ctx, ServiceSpec{
			App:  "svc-lifecycle",
			Name: "svc", SourceRef: "app/service.js",
			Port:  3000,
			Image: image,
			Entry: []string{"node", "/app/app/service.js"},
			Env:   []string{"PORT=3000"},
		}, i); err != nil {
			t.Fatalf("start replica %d: %v", i, err)
		}
	}

	// The replica is listed and running.
	var ids []string
	if !pollUntil(ctx, 15*time.Second, func() bool {
		list, err := m.ServiceContainerList(ctx)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		ids = ids[:0]
		allRunning := true
		for _, c := range list {
			if c.App == "svc-lifecycle" {
				ids = append(ids, c.ID)
				if c.State != container.StateRunning {
					allRunning = false
				}
			}
		}
		return len(ids) == replicas && allRunning
	}) {
		t.Fatalf("expected %d running service containers, got %d", replicas, len(ids))
	}

	// Verify labels on the created containers via inspect of the client.
	inspectFound := 0
	for _, id := range ids {
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect %s: %v", id, err)
		}
		if insp.Container.Config == nil {
			t.Fatalf("inspect %s: nil Config", id)
		}
		want := map[string]string{
			labelType:     ContainerTypeService,
			labelApp:      "svc-lifecycle",
			labelService:  "svc",
			labelIdentity: "app/service.js",
			labelImage:    image,
			labelHostname: "test-host",
			labelPort:     "3000",
			labelEnvHash:  EnvHash([]string{"PORT=3000"}),
		}
		for k, v := range want {
			if got := insp.Container.Config.Labels[k]; got != v {
				t.Errorf("container %s label %q = %q, want %q", id, k, got, v)
			}
		}
		// Service containers carry relay.service (their name) and relay.identity
		// (their source descriptor) and NO relay.handler.
		if _, ok := insp.Container.Config.Labels[labelHandler]; ok {
			t.Errorf("container %s must NOT carry relay.handler, got %v", id, insp.Container.Config.Labels)
		}
		if insp.Container.Config.Labels[labelService] != "svc" {
			t.Errorf("container %s must carry relay.service=svc, got %v", id, insp.Container.Config.Labels)
		}
		// Hardening assertions.
		assertHardenedHostConfig(t, insp.Container.HostConfig)
		if insp.Container.HostConfig.AutoRemove {
			t.Error("service container must NOT be AutoRemove (persistent, reconciler-owned)")
		}
		if insp.Container.Config.User != "10001:10001" {
			t.Errorf("config user = %q, want 10001:10001", insp.Container.Config.User)
		}
		if insp.Container.Config.Entrypoint == nil || len(insp.Container.Config.Entrypoint) != 2 ||
			insp.Container.Config.Entrypoint[0] != "node" || insp.Container.Config.Entrypoint[1] != "/app/app/service.js" {
			t.Errorf("entrypoint = %v, want [node /app/app/service.js]", insp.Container.Config.Entrypoint)
		}
		env := insp.Container.Config.Env
		foundPort := false
		for _, e := range env {
			if e == "PORT=3000" {
				foundPort = true
			}
		}
		if !foundPort {
			t.Errorf("expected PORT=3000 in env, got %v", env)
		}
		inspectFound++
	}
	if inspectFound != replicas {
		t.Fatalf("inspected %d containers, want %d", inspectFound, replicas)
	}

	// StopServiceContainers removes it.
	list, _ := m.ServiceContainerList(ctx)
	var svcList []ServiceContainer
	for _, c := range list {
		if c.App == "svc-lifecycle" {
			svcList = append(svcList, c)
		}
	}
	if len(svcList) != replicas {
		t.Fatalf("list returned %d containers for svc-lifecycle, want %d", len(svcList), replicas)
	}
	if err := m.StopServiceContainers(ctx, svcList); err != nil {
		t.Fatalf("stop: %v", err)
	}
	pollUntil(ctx, 15*time.Second, func() bool {
		return countServiceContainers(t, ctx, m, "svc-lifecycle") == 0
	})
	if remaining := countServiceContainers(t, ctx, m, "svc-lifecycle"); remaining != 0 {
		t.Errorf("expected 0 service containers after stop, got %d", remaining)
	}
}

func TestIntegrationServiceImageRetirement(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Clean up containers and images this test creates.
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scope cleanup to this test's app (see the python service test).
		_, _ = m.RemoveAppServiceContainers(cc, "svc-retire")
		cleanupImagePrefixes(cli, "relay-app-svc-retire:", "relay-app-svc-other:")()
	})

	// fn1 with v1 source.
	dir1 := t.TempDir()
	writeFile(t, dir1, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir1, "index.js", "export function hi(e){ console.log('v1'); }\n")
	// SIGTERM handler so the daemon's stop is prompt (node as PID 1 ignores the
	// default SIGTERM action; without this every stop pays the full 10s grace).
	writeFile(t, dir1, "service.js", "process.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 1 << 30);\n")
	fn1 := app.App{Name: "svc-retire", Dir: dir1, Template: &app.Template{Runtime: "node24"}}

	// Build v1, get its image ref. Reuse the test's single Manager for every
	// Prepare (mPrepare would spin a fresh client+manager per call): the
	// prepares are independent image builds, not independent daemons.
	p1, err := m.Prepare(ctx, fn1)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	v1Ref := p1.Image

	// Start ONE replica of v1 (running, references v1Ref).
	svc1, err := m.StartService(ctx, ServiceSpec{
		App:       "svc-retire",
		Name:      "svc",
		SourceRef: "service.js", Port: 3000,
		Image: v1Ref, Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("start v1 replica: %v", err)
	}
	// Wait for it to be running.
	waitForContainerRunning(t, ctx, cli, svc1)

	// Now create a SECOND fingerprint v2 (touch a file) and build it -> new image.
	writeFile(t, dir1, "index.js", "export function hi(e){ console.log('v2'); }\n")
	p2, err := m.Prepare(ctx, fn1)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	v2Ref := p2.Image
	if v1Ref == v2Ref {
		t.Fatalf("v1 and v2 refs must differ, both %s", v1Ref)
	}

	// Stop the v1 replica (so nothing references v1Ref anymore).
	if _, err := m.RemoveAppServiceContainers(ctx, "svc-retire"); err != nil {
		t.Fatalf("remove function containers: %v", err)
	}

	// p1/p2 are direct (unpublished) Prepare handles; in production the registry
	// publication owns these leases and releases them on supersede. Release them
	// here so the retirement below is governed by container references alone.
	p1.ReleaseLease()
	p2.ReleaseLease()

	// Retire with NO containers present -> v1 (and v2, since v2 isn't referenced
	// by any container either) gets removed. But v1 must be removed and v2 too
	// if unreferenced. To assert "never remove an image a running service
	// depends on", start a NEW replica of v2 and retire -> v2 kept, v1 removed.
	// Re-start v2 replica so it references v2Ref.
	svc2, err := m.StartService(ctx, ServiceSpec{
		App:       "svc-retire",
		Name:      "svc",
		SourceRef: "service.js", Port: 3000,
		Image: v2Ref, Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("start v2 replica: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, svc2)

	// Retire svc-retire's images. v2 is referenced by the running container -> kept.
	removed := retireAppImages(t, ctx, m, "svc-retire")
	// v1 is no longer referenced -> removed. v2 kept. (Other unreferenced v-refs:
	// none.) removed should be >= 1 (v1).
	if removed == 0 {
		t.Error("expected at least the unreferenced v1 image to be retired")
	}
	if !imageExistsInDaemon(cli, ctx, v2Ref) {
		t.Error("v2 (referenced by a running service container) must be kept, but was removed")
	}
	if imageExistsInDaemon(cli, ctx, v1Ref) {
		t.Error("v1 (no longer referenced) should have been removed, but still present")
	}

	// A second app's repo must be untouched. Build fn2 image.
	dir2 := t.TempDir()
	writeFile(t, dir2, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir2, "index.js", "export function hi(e){ console.log('other'); }\n")
	fn2 := app.App{Name: "svc-other", Dir: dir2, Template: &app.Template{Runtime: "node24"}}
	pOther, err := m.Prepare(ctx, fn2)
	if err != nil {
		t.Fatalf("prepare other: %v", err)
	}
	if !imageExistsInDaemon(cli, ctx, pOther.Image) {
		t.Fatalf("other function image should exist")
	}
	// Retire fn1's images again (any unreferenced leftovers); fn2's must survive.
	retireAppImages(t, ctx, m, "svc-retire")
	if !imageExistsInDaemon(cli, ctx, pOther.Image) {
		t.Error("unrelated function's image must not be touched by retirement")
	}
}

// TestIntegrationImageRetirementWaitsForServiceContainers pins the NO-force,
// skip-not-delete behavior at the Manager level: while a relay-owned (service)
// container references an image, RemoveImage must refuse (ErrImageInUse) and the
// image must still exist; only after the container is gone does removal succeed.
// A forced remove would have succeeded while the container was up, so this test
// proves the removal is force-free by construction.
func TestIntegrationImageRetirementWaitsForServiceContainers(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scope cleanup to this test's app (see the python service test).
		_, _ = m.RemoveAppServiceContainers(cc, "svc-wait")
		cleanupImagePrefixes(cli, "relay-app-svc-wait:")()
	})

	_, v1Ref := buildServiceHost(t, ctx, "svc-wait")
	if !imageExistsInDaemon(cli, ctx, v1Ref) {
		t.Fatalf("v1 image should exist after build")
	}

	// Start ONE replica of v1 (running, references v1Ref).
	if _, err := m.StartService(ctx, ServiceSpec{
		App:       "svc-wait",
		Name:      "svc",
		SourceRef: "app/service.js", Port: 3000,
		Image: v1Ref, Entry: []string{"node", "/app/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0); err != nil {
		t.Fatalf("start v1 replica: %v", err)
	}
	waitForServiceRunning(t, ctx, m, "svc-wait")

	// The new method reports the image referenced; RemoveImage refuses.
	referenced, err := m.ImageReferencedByManagedContainer(ctx, v1Ref)
	if err != nil {
		t.Fatalf("ImageReferencedByManagedContainer: %v", err)
	}
	if !referenced {
		t.Fatal("ImageReferencedByManagedContainer should report the image referenced by the running service container")
	}
	if err := m.RemoveImage(ctx, v1Ref); err == nil || !errors.Is(err, ErrImageInUse) {
		t.Fatalf("RemoveImage while referenced: err = %v, want a wrapped ErrImageInUse", err)
	}
	if !imageExistsInDaemon(cli, ctx, v1Ref) {
		t.Fatalf("image must still exist after a refused (non-forced) removal")
	}

	// Stop this test's container and wait for it to be gone. Scope the stop and
	// the "gone" poll to svc-wait: a whole-daemon empty check would wait on
	// unrelated service containers this test does not own.
	if _, err := m.RemoveAppServiceContainers(ctx, "svc-wait"); err != nil {
		t.Fatalf("remove function service containers: %v", err)
	}
	pollUntil(ctx, 30*time.Second, func() bool {
		return countServiceContainers(t, ctx, m, "svc-wait") == 0
	})

	// Now the image is removable and gone.
	if err := m.RemoveImage(ctx, v1Ref); err != nil {
		t.Fatalf("RemoveImage after container gone: %v", err)
	}
	if imageExistsInDaemon(cli, ctx, v1Ref) {
		t.Fatalf("image should have been removed once no container references it")
	}
}

// retireAppImages retires fn's superseded images through the production
// primitives the runner's app-removal path drives: AppImageTags lists
// the app's own Relay-owned tags, and each is handed to RemoveImageNow
// (whose container-reference guard refuses an image a service container still
// references). Transitional refusals (ErrImageInUse / ErrImageRetiring /
// ErrManagerShuttingDown) are the normal skip state, mirroring
// RemoveImagesExcept and the runner's cleanup retry path, and are not counted.
// It returns how many images were actually removed.
func retireAppImages(t *testing.T, ctx context.Context, m *Manager, fn string) int {
	t.Helper()
	tags, err := m.AppImageTags(ctx, fn)
	if err != nil {
		t.Fatalf("AppImageTags(%s): %v", fn, err)
	}
	removed := 0
	for _, tag := range tags {
		err := m.RemoveImageNow(ctx, tag)
		if err == nil {
			removed++
			continue
		}
		if errors.Is(err, ErrImageInUse) || errors.Is(err, ErrImageRetiring) || errors.Is(err, ErrManagerShuttingDown) {
			t.Logf("retire %s: transitional skip: %v", tag, err)
			continue
		}
		t.Fatalf("RemoveImageNow(%s): %v", tag, err)
	}
	return removed
}

// waitForServiceRunning polls until at least one of fn's service containers is
// running.
func waitForServiceRunning(t *testing.T, ctx context.Context, m *Manager, fn string) {
	t.Helper()
	if !pollUntil(ctx, 30*time.Second, func() bool {
		list, err := m.ServiceContainerList(ctx)
		if err != nil {
			return false
		}
		for _, c := range list {
			if c.App == fn && c.State == container.StateRunning {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no running service container for %s", fn)
	}
}

// countServiceContainers returns how many service containers belong to fn.
func countServiceContainers(t *testing.T, ctx context.Context, m *Manager, fn string) int {
	t.Helper()
	list, err := m.ServiceContainerList(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	n := 0
	for _, c := range list {
		if c.App == fn {
			n++
		}
	}
	return n
}

// TestIntegrationServiceJoinsExternalNetwork pins the ServiceSpec.Networks +
// ServiceSpec.Labels wiring end to end: a service started with
// Networks=[<name>] is created attached to that external Docker network (created
// by the TEST as the infra owner — Relay never creates networks), and its
// supplied extra labels land on the container next to the relay.* ownership
// labels. The negative case: starting a service against a NONEXISTENT network
// fails and leaves no container behind.
func TestIntegrationServiceJoinsExternalNetwork(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// A dedicated network with a unique name, created here (the test is the
	// infra owner) and removed in cleanup along with any service containers.
	networkName := "relay-test-traefik-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := cli.NetworkCreate(ctx, networkName, client.NetworkCreateOptions{Driver: "bridge"}); err != nil {
		t.Fatalf("create test network: %v", err)
	}
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		// Scope cleanup to this test's app (see the python service test).
		_, _ = m.RemoveAppServiceContainers(cc, "svc-network")
		_, _ = cli.NetworkRemove(cc, networkName, client.NetworkRemoveOptions{})
		cleanupImagePrefixes(cli, "relay-app-svc-network:")()
	})

	_, image := buildServiceHost(t, ctx, "svc-network")

	// Happy path: start a routed-looking service with the network + an extra
	// Traefik label; assert both reach the container.
	id, err := m.StartService(ctx, ServiceSpec{
		App:  "svc-network",
		Name: "svc", SourceRef: "app/service.js",
		Port:     3000,
		Image:    image,
		Entry:    []string{"node", "/app/app/service.js"},
		Env:      []string{"PORT=3000"},
		Labels:   map[string]string{"traefik.enable": "true"},
		Networks: []string{networkName},
	}, 0)
	if err != nil {
		t.Fatalf("start service: %v", err)
	}

	waitForContainerRunning(t, ctx, cli, id)
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if insp.Container.Config == nil {
		t.Fatal("inspect: nil Config")
	}
	labels := insp.Container.Config.Labels
	// The supplied extra label and the Relay ownership labels coexist.
	if labels["traefik.enable"] != "true" {
		t.Fatalf("extra routing label missing on container: %v", labels)
	}
	if labels[labelType] != ContainerTypeService || labels[labelApp] != "svc-network" ||
		labels[labelIdentity] != "app/service.js" || labels[labelImage] != image ||
		labels[labelHostname] != "test-host" {
		t.Fatalf("relay ownership labels missing/corrupted on container: %v", labels)
	}
	// The container joined the external network.
	if insp.Container.NetworkSettings == nil {
		t.Fatal("inspect: nil NetworkSettings")
	}
	if _, ok := insp.Container.NetworkSettings.Networks[networkName]; !ok {
		t.Fatalf("container is not attached to network %q; networks = %v",
			networkName, insp.Container.NetworkSettings.Networks)
	}

	// Negative: a nonexistent network fails and leaves NO container behind.
	_, err = m.StartService(ctx, ServiceSpec{
		App:  "svc-network",
		Name: "svc", SourceRef: "app/service.js",
		Port:     3000,
		Image:    image,
		Entry:    []string{"node", "/app/app/service.js"},
		Env:      []string{"PORT=3000"},
		Networks: []string{"relay-test-nonexistent-network"},
	}, 1)
	if err == nil {
		t.Fatal("expected an error starting a service on a nonexistent network")
	}
	remaining := countServiceContainers(t, ctx, m, "svc-network")
	pollUntil(ctx, 15*time.Second, func() bool {
		remaining = countServiceContainers(t, ctx, m, "svc-network")
		return remaining <= 1
	})
	// The extra replica must not exist: exactly the one happy-path container.
	if remaining != 1 {
		t.Fatalf("after a failed create left %d containers for the function; want 1 (the routed one only)", remaining)
	}
}

// TestIntegrationExternalImageService resolves an `image` source end to end
// against a real daemon: it inspects a locally present image, reports its
// content ID, and runs the image with its OWN entrypoint (no per-container
// override). The image it targets is a local function image the test already
// built, so the test needs no registry access; a recorded pull check suppresses
// the remote freshness check, exercising the inspect path.
func TestIntegrationExternalImageService(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "svc-external")
	})

	// Build a small app image to serve as the external reference.
	dir := t.TempDir()
	writeFile(t, dir, "index.js", "setInterval(() => {}, 1 << 30);\n")
	tmpl, err := app.ParseTemplate([]byte("runtime: node24\nevents:\n  - handler: index.handler\n    pattern:\n      event_name: [INSERT]\n"))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fn := app.App{Name: "svc-external", Dir: dir, Template: tmpl}
	prep, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare host function: %v", err)
	}
	ref := prep.Image
	if ref == "" {
		t.Fatal("prepared host function has no image")
	}

	svcTmpl := &app.Template{Services: []app.Service{{Image: ref, Port: 80, Replicas: 1}}}
	svc := svcTmpl.Services[0]
	// Suppress the remote pull check: the image is present locally.
	m.recordPullCheck("svc-external", ref, time.Now())

	got, err := m.ResolveServiceImage(ctx, "svc-external", svcTmpl, svc, "")
	if err != nil {
		t.Fatalf("resolve external service: %v", err)
	}
	if got.Ref != ref {
		t.Fatalf("ref = %q, want the external reference %q", got.Ref, ref)
	}
	if got.ID == "" {
		t.Fatal("expected the external image's local content ID")
	}
	if got.Entry != nil {
		t.Fatalf("entry = %v, want nil (preserve image ENTRYPOINT)", got.Entry)
	}

	id, err := m.StartService(ctx, ServiceSpec{
		App: "svc-external", Name: "svc", SourceRef: ref, Port: 80,
		Image: got.Ref, ImageID: got.ID, Env: []string{"PORT=80"},
	}, 0)
	if err != nil {
		t.Fatalf("start external service: %v", err)
	}
	waitForContainerRunning(t, ctx, cli, id)
}

// TestIntegrationServiceStartRunningGateAndNameOverlap proves the two runtime
// guarantees the reconciler's zero-downtime replacement depends on, against a
// real daemon:
//
//   - StartService returns only once Docker reports the container RUNNING (the
//     running gate); and
//   - two starts for the SAME logical slot coexist, because the physical name
//     is unique per start (Docker would reject a duplicate name otherwise).
//
// The image is the test's own app image with a stop-responsive long-lived
// command, so the container stays up.
func TestIntegrationServiceStartRunningGateAndNameOverlap(t *testing.T) {
	cli := testutil.RequireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = m.RemoveAppServiceContainers(cc, "svc-overlap")
		cleanupImagePrefixes(cli, "relay-app-svc-overlap:")()
	})

	_, image := buildServiceHost(t, ctx, "svc-overlap")

	// Start two replicas of the SAME logical slot: both must succeed (unique
	// physical names) and both must be running the instant StartService returns.
	first, err := m.StartService(ctx, ServiceSpec{
		App:       "svc-overlap",
		Name:      "svc",
		SourceRef: "app/service.js", Port: 3000,
		Image: image, Entry: []string{"node", "/app/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("first StartService: %v", err)
	}
	second, err := m.StartService(ctx, ServiceSpec{
		App:       "svc-overlap",
		Name:      "svc",
		SourceRef: "app/service.js", Port: 3000,
		Image: image, Entry: []string{"node", "/app/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("second StartService for the same slot: %v", err)
	}
	if first == second {
		t.Fatal("two starts for one slot returned the same container id")
	}
	for _, id := range []string{first, second} {
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect %s: %v", id, err)
		}
		if insp.Container.State == nil || !insp.Container.State.Running {
			t.Fatalf("container %s is not running immediately after StartService returned (the running gate failed)", id)
		}
	}
}

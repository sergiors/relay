//go:build integration

// This file exercises the persistent service-container operations against a
// real Docker daemon: service container lifecycle (start/list/stop/remove),
// image retirement with in-flight safety (never remove an image a service
// container depends on), and the container env/hardening.
package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/function"
)

// buildServiceHost builds a tiny node24 function directory containing a
// long-lived service.js (keeps node alive with an interval) alongside a trivial
// invocation handler, then m.Prepare-builds its image and returns the function
// handle plus the prepared image ref.
func buildServiceHost(t *testing.T, ctx context.Context, fnName string) (function.Function, string) {
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
	// image COPYies to /app/app/service.js (the function dir is the /app root).
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	writeFile(t, dir, "app/service.js", `
console.log("started");
setInterval(() => {}, 1 << 30);
`)
	fn := function.Function{Name: fnName, Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare %s: %v", fnName, err)
	}
	return fn, prepared.Image
}

func TestServiceStartListStop(t *testing.T) {
	cli := requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up any containers this test's function ever starts, even on failure,
	// plus the images it builds.
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if c, err := m.ServiceContainerList(cc); err == nil {
			_ = m.StopServiceContainers(cc, c)
		}
		cleanupImagePrefixes(cli, "relay-fn-svc-lifecycle:")()
	})

	_, image := buildServiceHost(t, ctx, "svc-lifecycle")

	// Start 2 replicas.
	for i := 0; i < 2; i++ {
		if _, err := m.StartService(ctx, ServiceSpec{
			Function:   "svc-lifecycle",
			Entrypoint: "app/service.js",
			Port:       3000,
			Image:      image,
			Entry:      []string{"node", "/app/app/service.js"},
			Env:        []string{"PORT=3000"},
		}, i); err != nil {
			t.Fatalf("start replica %d: %v", i, err)
		}
	}

	// Both replicas listed and running.
	deadline := time.Now().Add(15 * time.Second)
	var ids []string
	for time.Now().Before(deadline) {
		list, err := m.ServiceContainerList(ctx)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		ids = nil
		allRunning := true
		for _, c := range list {
			if c.Function == "svc-lifecycle" {
				ids = append(ids, c.ID)
				if c.State != container.StateRunning {
					allRunning = false
				}
			}
		}
		if len(ids) == 2 && allRunning {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 running service containers, got %d", len(ids))
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
			labelType:       ContainerTypeService,
			labelFunction:   "svc-lifecycle",
			labelEntrypoint: "app/service.js",
			labelImage:      image,
			labelHostname:   "test-host",
			labelPort:       "3000",
		}
		for k, v := range want {
			if got := insp.Container.Config.Labels[k]; got != v {
				t.Errorf("container %s label %q = %q, want %q", id, k, got, v)
			}
		}
		// Service containers must carry NO relay.handler and NO relay.service.
		if _, ok := insp.Container.Config.Labels[labelHandler]; ok {
			t.Errorf("container %s must NOT carry relay.handler, got %v", id, insp.Container.Config.Labels)
		}
		if _, ok := insp.Container.Config.Labels["relay.service"]; ok {
			t.Errorf("container %s must NOT carry relay.service, got %v", id, insp.Container.Config.Labels)
		}
		// Hardening assertions.
		hc := insp.Container.HostConfig
		if hc == nil {
			t.Fatal("nil HostConfig")
		}
		if hc.Memory != 128<<20 {
			t.Errorf("memory = %d, want 128MiB", hc.Memory)
		}
		if hc.NanoCPUs != 1_000_000_000 {
			t.Errorf("nanocpus = %d, want 1", hc.NanoCPUs)
		}
		if hc.PidsLimit == nil || *hc.PidsLimit != 128 {
			t.Errorf("pids limit = %v, want 128", hc.PidsLimit)
		}
		if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
			t.Errorf("capdrop = %v, want [ALL]", hc.CapDrop)
		}
		if !hc.ReadonlyRootfs {
			t.Error("expected read-only rootfs")
		}
		if len(hc.PortBindings) != 0 {
			t.Errorf("expected no host port bindings, got %v", hc.PortBindings)
		}
		if hc.AutoRemove {
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
	if inspectFound != 2 {
		t.Fatalf("inspected %d containers, want 2", inspectFound)
	}

	// StopServiceContainers removes them all.
	list, _ := m.ServiceContainerList(ctx)
	var svcList []ServiceContainer
	for _, c := range list {
		if c.Function == "svc-lifecycle" {
			svcList = append(svcList, c)
		}
	}
	if err := m.StopServiceContainers(ctx, svcList); err != nil {
		t.Fatalf("stop: %v", err)
	}
	deadline2 := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline2) {
		l2, _ := m.ServiceContainerList(ctx)
		remaining := 0
		for _, c := range l2 {
			if c.Function == "svc-lifecycle" {
				remaining++
			}
		}
		if remaining == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if remaining := countServiceContainers(t, ctx, m, "svc-lifecycle"); remaining != 0 {
		t.Errorf("expected 0 service containers after stop, got %d", remaining)
	}
}

func TestServiceImageRetirement(t *testing.T) {
	cli := requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean up containers and images this test creates.
	t.Cleanup(func() {
		cc, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		if c, err := m.ServiceContainerList(cc); err == nil {
			_ = m.StopServiceContainers(cc, c)
		}
		cleanupImagePrefixes(cli, "relay-fn-svc-retire:", "relay-fn-svc-other:")()
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
	writeFile(t, dir1, "service.js", "setInterval(() => {}, 1 << 30);\n")
	fn1 := function.Function{Name: "svc-retire", Dir: dir1, Template: &function.Template{Runtime: "node24"}}

	// Build v1, get its image ref.
	p1, err := mPrepare(ctx, t, fn1)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	v1Ref := p1.Image

	// Start ONE replica of v1 (running, references v1Ref).
	svc1, err := m.StartService(ctx, ServiceSpec{
		Function: "svc-retire", Entrypoint: "service.js", Port: 3000,
		Image: v1Ref, Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("start v1 replica: %v", err)
	}
	// Wait for it to be running.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		insp, err := cli.ContainerInspect(ctx, svc1, client.ContainerInspectOptions{})
		if err == nil && insp.Container.State != nil && insp.Container.State.Running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Now create a SECOND fingerprint v2 (touch a file) and build it -> new image.
	writeFile(t, dir1, "index.js", "export function hi(e){ console.log('v2'); }\n")
	p2, err := mPrepare(ctx, t, fn1)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	v2Ref := p2.Image
	if v1Ref == v2Ref {
		t.Fatalf("v1 and v2 refs must differ, both %s", v1Ref)
	}

	// Stop the v1 replica (so nothing references v1Ref anymore).
	if _, err := m.RemoveFunctionServiceContainers(ctx, "svc-retire"); err != nil {
		t.Fatalf("remove function containers: %v", err)
	}

	// Retire with NO containers present -> v1 (and v2, since v2 isn't referenced
	// by any container either) gets removed. But v1 must be removed and v2 too
	// if unreferenced. To assert "never remove an image a running service
	// depends on", start a NEW replica of v2 and retire -> v2 kept, v1 removed.
	// Re-start v2 replica so it references v2Ref.
	svc2, err := m.StartService(ctx, ServiceSpec{
		Function: "svc-retire", Entrypoint: "service.js", Port: 3000,
		Image: v2Ref, Entry: []string{"node", "/app/service.js"}, Env: []string{"PORT=3000"},
	}, 0)
	if err != nil {
		t.Fatalf("start v2 replica: %v", err)
	}
	deadline2 := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline2) {
		insp, err := cli.ContainerInspect(ctx, svc2, client.ContainerInspectOptions{})
		if err == nil && insp.Container.State != nil && insp.Container.State.Running {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Retire svc-retire's images. v2 is referenced by the running container -> kept.
	removed, err := m.RetireServiceImages(ctx, "svc-retire")
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
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

	// A second function's repo must be untouched. Build fn2 image.
	dir2 := t.TempDir()
	writeFile(t, dir2, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir2, "index.js", "export function hi(e){ console.log('other'); }\n")
	fn2 := function.Function{Name: "svc-other", Dir: dir2, Template: &function.Template{Runtime: "node24"}}
	pOther, err := mPrepare(ctx, t, fn2)
	if err != nil {
		t.Fatalf("prepare other: %v", err)
	}
	if !imageExistsInDaemon(cli, ctx, pOther.Image) {
		t.Fatalf("other function image should exist")
	}
	// Retire fn1's images again (any unreferenced leftovers); fn2's must survive.
	if _, err := m.RetireServiceImages(ctx, "svc-retire"); err != nil {
		t.Fatalf("retire 2: %v", err)
	}
	if !imageExistsInDaemon(cli, ctx, pOther.Image) {
		t.Error("unrelated function's image must not be touched by retirement")
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
		if c.Function == fn {
			n++
		}
	}
	return n
}

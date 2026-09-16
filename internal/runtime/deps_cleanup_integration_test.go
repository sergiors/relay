//go:build integration

package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
)

// TestIntegrationDependencyImageLabels verifies that a function built with a
// dependency layer stamps the managed-image labels onto BOTH the dependency
// image and the function image, and that a reuse (second Prepare of the same
// function) reuses the dependency layer without rebuilding it (identical dep
// image ID). This is the integration proof that ImageBuildOptions.Labels lands
// on the resulting image config (the build backend applies them as LABEL
// equivalents), the mechanism the dependency GC reads for ownership.
func TestIntegrationDependencyImageLabels(t *testing.T) {
	cli := requireDocker(t)
	mgr, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-labels:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	fn := function.Function{Name: "dep-labels", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	p1, err := mgr.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Exactly one NEW dependency image built.
	newDeps := newDepTagsSince(ctx, cli, depBefore)
	if len(newDeps) != 1 {
		t.Fatalf("expected exactly one new dependency image, got %v", newDeps)
	}
	depRef := newDeps[0]
	// The daemon normalizes a plain repo reference to a :latest tag; the
	// function image's relay.dependency label carries the untagged repo (what
	// depImageRef produces), so normalize the candidate before comparing.
	depRepo, _, _ := strings.Cut(depRef, ":")
	// The 16-hex fingerprint prefix the tag embeds (depImageRef truncates the
	// full fingerprint to the first 16 chars), used to check the label's full
	// fingerprint is consistent with the reference.
	depFP := strings.TrimPrefix(depRepo, depRepoPrefix)

	// Inspect the dependency image and assert its managed-image labels.
	depInsp, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("inspect dep image: %v", err)
	}
	if depInsp.Config == nil || depInsp.Config.Labels == nil {
		t.Fatal("managed dependency image must carry config labels (ImageBuildOptions.Labels did not land on the image config)")
	}
	dlbls := depInsp.Config.Labels
	if dlbls[labelType] != ImageTypeDependency {
		t.Errorf("dep relay.type = %q, want dependency", dlbls[labelType])
	}
	if dlbls[labelRuntime] != "python3.14" {
		t.Errorf("dep relay.runtime = %q, want python3.14", dlbls[labelRuntime])
	}
	// The label fingerprint must be consistent with the reference (depImageRef
	// truncates the full fingerprint to the first 16 hex; depFP strips the
	// daemon's normalized :latest suffix).
	if !strings.HasPrefix(dlbls[labelFingerprint], depFP) {
		t.Errorf("dep relay.fingerprint %q must be consistent with dep ref %q (prefix %q)", dlbls[labelFingerprint], depRef, depFP)
	}

	// The function image carries its own labels and references the dependency.
	fnInsp, err := cli.ImageInspect(ctx, p1.Image)
	if err != nil {
		t.Fatalf("inspect function image: %v", err)
	}
	if fnInsp.Config == nil || fnInsp.Config.Labels == nil {
		t.Fatal("managed function image must carry config labels (ImageBuildOptions.Labels did not land)")
	}
	flbls := fnInsp.Config.Labels
	if flbls[labelType] != ImageTypeFunction {
		t.Errorf("fn relay.type = %q, want function", flbls[labelType])
	}
	if flbls[labelFunction] != "dep-labels" {
		t.Errorf("fn relay.function = %q, want dep-labels", flbls[labelFunction])
	}
	// The function image's relay.dependency is the untagged repo reference
	// (what depImageRef / Prepare produce), which the daemon normalized to
	// depRef (with :latest) — compare to the normalized repo.
	if flbls[labelDependency] != depRepo {
		t.Errorf("fn relay.dependency = %q, want the dependency reference %q", flbls[labelDependency], depRepo)
	}

	// Reuse: a second Prepare of the same function must reuse the dependency
	// layer (identical dep image ID), never rebuild it.
	depIDBefore := depInsp.ID
	if _, err := mgr.Prepare(ctx, fn); err != nil {
		t.Fatalf("prepare (reuse): %v", err)
	}
	depIDAfter, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("re-inspect dep after reuse: %v", err)
	}
	if depIDAfter.ID != depIDBefore {
		t.Errorf("dependency layer was rebuilt across a reuse Prepare (before %s after %s), want reuse", depIDBefore, depIDAfter.ID)
	}
}

// TestIntegrationSharedDependencyGC exercises the lifecycle-driven dependency GC
// end to end: two functions sharing one dependency fingerprint are kept while
// any of them references the layer; once the LAST referencing function image is
// removed the dependency layer is pruned. It also proves unmanaged images (a
// relay-dep-* image with no relay.type label) are never touched.
func TestIntegrationSharedDependencyGC(t *testing.T) {
	cli := requireDocker(t)
	mgr, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-gc-a:", "relay-fn-dep-gc-b:", "relay-dep-evil"))

	newManaged := func(name, deps string) function.Function {
		dir := t.TempDir()
		writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
		writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
		writeFile(t, dir, "requirements.txt", deps)
		return function.Function{Name: name, Dir: dir, Template: &function.Template{Runtime: "python3.14"}}
	}

	// A and B share one dependency fingerprint (identical requirements).
	fnA := newManaged("dep-gc-a", "six==1.16.0\n")
	fnB := newManaged("dep-gc-b", "six==1.16.0\n")
	pa, err := mgr.Prepare(ctx, fnA)
	if err != nil {
		t.Fatalf("prepare A: %v", err)
	}
	if _, err := mgr.Prepare(ctx, fnB); err != nil {
		t.Fatalf("prepare B: %v", err)
	}
	newDeps := newDepTagsSince(ctx, cli, depBefore)
	if len(newDeps) != 1 {
		t.Fatalf("A+B should share exactly one dependency image, got %v", newDeps)
	}
	depX := newDeps[0]

	// Create an unmanaged image tagged relay-dep-evil: NO relay.type label. It
	// must NEVER be a GC candidate. Tag the python base (an unrelated image with
	// a relay-dep-* name but no label) under the relay-dep- namespace.
	buildTestImage(ctx, t, "relay-dep-evil:1", "FROM python:3.14-slim\nRUN echo unmanaged > /unmanaged\n")

	// Migrate A to a different manifest, then remove A's OLD function image
	// (which references depX), so depX is then referenced only by B's function
	// image.
	fnA2 := newManaged("dep-gc-a", "requests==2.32.3\n")
	pa2, err := mgr.Prepare(ctx, fnA2)
	if err != nil {
		t.Fatalf("prepare A v2: %v", err)
	}
	if pa2.Image == pa.Image {
		t.Fatalf("A v1 and v2 must be different images, both %s", pa2.Image)
	}
	if err := mgr.RemoveImage(ctx, pa.Image); err != nil {
		t.Fatalf("remove A v1 image: %v", err)
	}

	// GC now: depX is still referenced by B's function image -> kept; the
	// unmanaged relay-dep-evil image must be untouched.
	if _, err := mgr.CleanupUnusedDependencies(ctx); err != nil {
		t.Fatalf("GC (depX still referenced): %v", err)
	}
	if !imageExistsInDaemon(cli, ctx, depX) {
		t.Error("depX must be kept while B still references it")
	}
	if !imageExistsInDaemon(cli, ctx, "relay-dep-evil:1") {
		t.Error("an unmanaged relay-dep-* image (no relay.type label) must never be removed")
	}

	// Migrate B to the same new manifest, prepare, and remove B's OLD image,
	// which was the last reference to depX.
	fnB2 := newManaged("dep-gc-b", "requests==2.32.3\n")
	pb2, err := mgr.Prepare(ctx, fnB2)
	if err != nil {
		t.Fatalf("prepare B v2: %v", err)
	}
	// Find B's v1 image (the relay-fn-dep-gc-b tag that is NOT pb2.Image).
	bOld := ""
	list, err := cli.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		t.Fatalf("list images: %v", err)
	}
	for _, img := range list.Items {
		for _, tag := range img.RepoTags {
			if strings.HasPrefix(tag, "relay-fn-dep-gc-b:") && tag != pb2.Image {
				bOld = tag
			}
		}
	}
	if bOld == "" {
		t.Fatalf("could not find B's old function image")
	}
	if err := mgr.RemoveImage(ctx, bOld); err != nil {
		t.Fatalf("remove B v1 image: %v", err)
	}

	// GC now: depX has no referencing function image left -> removed. The
	// unmanaged relay-dep-evil image is STILL untouched.
	if _, err := mgr.CleanupUnusedDependencies(ctx); err != nil {
		t.Fatalf("GC (depX orphaned): %v", err)
	}
	if imageExistsInDaemon(cli, ctx, depX) {
		t.Error("depX must be removed once the last referencing function image is gone")
	}
	if !imageExistsInDaemon(cli, ctx, "relay-dep-evil:1") {
		t.Error("an unmanaged relay-dep-* image must survive GC even after a real dependency is pruned")
	}
}

//go:build integration

// Image lifecycle integration tests: fingerprint-versioned builds, dependency
// layer reuse/change, image removal, and the relay.dependency wiring.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// TestIntegrationFingerprintedImageLifecycle exercises the fingerprint-versioned
// image lifecycle against a real Docker daemon: build v1 -> build v2 (changed
// source) -> the two are distinct images and v1 still present -> removing v1
// (the superseded version) via the manager's per-image removal path retires it
// -> an unrelated (non-relay-owned) image is untouched.
func TestIntegrationFingerprintedImageLifecycle(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The test's own fn-ver images should be cleaned up even on failure so the
	// test never leaks images into another package's cleanup on the shared
	// daemon. Best-effort force-removal, like cleanupImage.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-fn-ver:") {
					cleanupImage(cli, cleanupCtx, tag)
					break
				}
			}
		}
	})

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('v1'); }\n")

	fn := function.Function{Name: "fn-ver", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	fp1, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v1: %v", err)
	}
	ref1 := ImageRef(fn.Name, fp1)
	if imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("image %s already exists before build", ref1)
	}

	p1, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	if p1.Image != ref1 {
		t.Fatalf("prepare image = %q, want %q", p1.Image, ref1)
	}
	if !imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("image %s should exist after v1 build", ref1)
	}

	// Change source -> distinct fingerprint -> distinct image.
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('v2'); }\n")
	fp2, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v2: %v", err)
	}
	ref2 := ImageRef(fn.Name, fp2)
	if ref1 == ref2 {
		t.Fatalf("v1 and v2 references must differ, both = %s", ref1)
	}

	p2, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image != ref2 {
		t.Fatalf("prepare image = %q, want %q", p2.Image, ref2)
	}
	// v1 is still around (not clobbered by v2).
	if !imageExistsInDaemon(cli, ctx, ref1) {
		t.Fatalf("v1 image %s still expected to exist alongside v2", ref1)
	}

	// An unrelated, non-relay image we create: it must never be touched by any
	// of the test's removal operations. Build a tiny tagged image ourselves (not
	// relay-namespaced) via the Manager's ImageBuild against an inline one-line
	// Dockerfile.
	unrelated := buildTestImage(ctx, t, "relay-unrelated-guard", `FROM scratch
CMD []
`)
	defer cleanupImage(cli, ctx, unrelated)

	// Retire v1 (the superseded version) through the manager's per-image
	// removal path: it is non-forced and treats not-found as benign, matching
	// production removal semantics without issuing a whole-daemon sweep.
	//
	// Note: we deliberately do NOT call (*Manager).RemoveImagesExcept here. That
	// sweep is a worker-startup operation over ALL Relay-owned images on the
	// daemon; a unit-scoped lifecycle test must not assert on whole-daemon state
	// it does not own, since it races anything else creating relay-fn-* images on
	// a shared daemon (e.g. another Go test package running concurrently).
	m1, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m1.Close()
	if err := m1.RemoveImage(ctx, ref1); err != nil {
		t.Fatalf("remove image %s: %v", ref1, err)
	}
	if imageExistsInDaemon(cli, ctx, ref1) {
		t.Errorf("v1 image %s should have been removed", ref1)
	}
	if !imageExistsInDaemon(cli, ctx, ref2) {
		t.Errorf("kept v2 image %s must survive", ref2)
	}
	if !imageExistsInDaemon(cli, ctx, unrelated) {
		t.Errorf("unrelated image %s must not be removed", unrelated)
	}
}

// TestIntegrationDependencyLayerReuse verifies the shared dependency layer is
// reused across source changes: build v1 (with requirements.txt), then change
// ONLY the handler source and build v2. The dependency image must exist
// unchanged BEFORE and AFTER (same reference and image ID), while the function
// image gets a NEW tag for the changed source.
//
// The dependency reference is computed deterministically with the production
// helpers (lookup -> engine Plan -> DependencyFingerprint -> depImageRef) rather
// than inferred from a before/after tag delta. The relay-dep-* namespace is
// content-addressed and shared daemon-wide, so the expected image may already
// exist when the test runs (an earlier run or another worker built the same
// manifest); existence must therefore be handled, not assumed. Reuse is proven
// by the image ID being identical across the source change, and a wrong/new
// layer is detected by re-deriving the reference across the source change (a
// correct dependency fingerprint must not depend on handler source), by Prepare
// reporting the same dependency reference for both versions, and by the v2
// function image's relay.dependency label naming that exact layer.
func TestIntegrationDependencyLayerReuse(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register cleanup FIRST (before any Fatalf) so a mid-test failure can never
	// leak the dep-layer and function images this test creates into the sibling
	// tests that follow on the shared daemon. We remove only the dep images this
	// test built (delta vs the snapshot below), not every relay-dep-* layer on
	// the daemon, and force-remove this test's own function image.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-reuse:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")

	fn := function.Function{Name: "dep-reuse", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	// The dependency reference is content-addressed from the manifest set +
	// runtime + arch + install command — NOT from the handler source. Derive it
	// with the production helpers so the test names the exact shared image
	// instead of inferring it from a tag delta that is empty when the image
	// already exists on the shared daemon.
	depRef := expectedDependencyRef(t, fn)

	// The dependency image may already exist (an earlier run or a concurrent
	// worker on the shared daemon). Record its identity now so reuse across the
	// source change is proven by ID, and a preexisting image is not mistaken for
	// one this test created.
	preexistingDepID := ""
	if insp, err := cli.ImageInspect(ctx, depRef); err == nil {
		preexistingDepID = insp.ID
	}

	// v1 source.
	writeFile(t, dir, "handler.py", "def run(event):\n    print('v1')\n")
	fp1, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v1: %v", err)
	}
	ref1 := ImageRef(fn.Name, fp1)

	p1, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	if p1.Image != ref1 {
		t.Fatalf("prepare v1 image = %q, want %q", p1.Image, ref1)
	}
	if p1.Dependency != depRef {
		t.Fatalf("prepare v1 dependency = %q, want %q", p1.Dependency, depRef)
	}
	if !imageExistsInDaemon(cli, ctx, depRef) {
		t.Fatalf("dependency image %s must exist after v1 build", depRef)
	}
	inspV1, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("inspect dep image after v1: %v", err)
	}
	if preexistingDepID != "" && inspV1.ID != preexistingDepID {
		t.Errorf("preexisting dependency image %s was rebuilt by v1 (before %s after %s) — "+
			"it should be reused", depRef, preexistingDepID, inspV1.ID)
	}

	// v2 source: ONLY the handler changes. The dependency fingerprint covers the
	// manifest, not the handler source, so re-deriving it must yield the SAME
	// reference. A regression that folded source into the dependency identity
	// would change it here and fail immediately (no daemon race involved).
	writeFile(t, dir, "handler.py", "def run(event):\n    print('v2')\n")
	if depRef2 := expectedDependencyRef(t, fn); depRef2 != depRef {
		t.Fatalf("dependency reference changed across a pure source change: %s -> %s", depRef, depRef2)
	}
	fp2, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint v2: %v", err)
	}
	ref2 := ImageRef(fn.Name, fp2)
	if ref1 == ref2 {
		t.Fatalf("v1 and v2 refs must differ, both %s", ref1)
	}

	p2, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image != ref2 {
		t.Fatalf("prepare v2 image = %q, want %q", p2.Image, ref2)
	}
	if p2.Dependency != depRef {
		t.Fatalf("prepare v2 dependency = %q, want %q — a source change must reuse the dependency layer",
			p2.Dependency, depRef)
	}

	// The same dependency reference still exists after v2 and inspects to the
	// SAME image ID (identical content — not rebuilt).
	inspV2, err := cli.ImageInspect(ctx, depRef)
	if err != nil {
		t.Fatalf("inspect dep image after v2: %v", err)
	}
	if inspV2.ID != inspV1.ID {
		t.Errorf("dependency layer was rebuilt across a pure source change (before %s after %s) — "+
			"it should be reused", inspV1.ID, inspV2.ID)
	}

	// Wrong/new dependency layer guard: the v2 function image must actually be
	// wired to the expected dependency layer via its relay.dependency label. The
	// label is the strict wiring the dependency GC reads, so a function image
	// built FROM the wrong dependency layer fails here even if its own image
	// reference happened to be correct. This is a deterministic, per-image check
	// against the reference derived with the production helpers; it deliberately
	// does NOT compare whole-daemon relay-dep-* counts, which race concurrent
	// builders on a shared daemon. The dependency image's OWN labels are not
	// asserted here: it may be a preexisting content-addressed image built by a
	// label-less older Relay, and Prepare reuses it on existence alone (see
	// TestIntegrationDependencyImageLabels for the label contract).
	fnInsp, err := cli.ImageInspect(ctx, p2.Image)
	if err != nil {
		t.Fatalf("inspect v2 function image: %v", err)
	}
	if fnInsp.Config == nil || fnInsp.Config.Labels == nil {
		t.Fatalf("v2 function image %s carries no config labels", p2.Image)
	}
	if got := fnInsp.Config.Labels[labelDependency]; got != depRepoName(depRef) {
		t.Errorf("v2 function image %s relay.dependency = %q, want %q", p2.Image, got, depRepoName(depRef))
	}
}

// TestIntegrationDependencyChangeProducesNewDepLayer verifies that changing the
// dependency manifest (adding a package to requirements.txt) yields a NEW
// dependency fingerprint and a NEW relay-dep-* image, with both the old and new
// dependency layers coexisting (old layers are never mutated).
//
// The dependency references are derived deterministically with the production
// helpers (lookup -> engine Plan -> DependencyFingerprint -> depImageRef) rather
// than counted from a before/after tag delta: the relay-dep-* namespace is
// content-addressed and shared daemon-wide, so a delta count races any other
// test/worker building the same manifest. Each version's layer must exist, the
// two references must differ, and Prepare must report the exact reference it
// built FROM.
func TestIntegrationDependencyChangeProducesNewDepLayer(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Register cleanup FIRST (before any Fatalf) so a mid-test failure can never
	// leak the dep-layer and function images this test creates into the sibling
	// tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-change:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	fn := function.Function{Name: "dep-change", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	// v1 manifest.
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	dep1 := expectedDependencyRef(t, fn)
	p1, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}
	if p1.Dependency != dep1 {
		t.Fatalf("prepare v1 dependency = %q, want %q", p1.Dependency, dep1)
	}
	if !imageExistsInDaemon(cli, ctx, dep1) {
		t.Fatalf("dependency image %s must exist after v1 build", dep1)
	}

	// v2 manifest: add a package. The function source and template are unchanged,
	// so only the dependencies differ. idna is a single tiny wheel with no
	// transitive dependencies, so the added package proves the invalidation
	// without pulling the multi-wheel `requests` tree.
	writeFile(t, dir, "requirements.txt", "six==1.16.0\nidna==3.10\n")
	dep2 := expectedDependencyRef(t, fn)
	if dep1 == dep2 {
		t.Fatalf("dependency reference did not change across a manifest change, both %s", dep1)
	}
	p2, err := mPrepare(ctx, t, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Dependency != dep2 {
		t.Fatalf("prepare v2 dependency = %q, want %q", p2.Dependency, dep2)
	}

	// Both layers coexist: the original is never mutated or pruned by a change.
	if !imageExistsInDaemon(cli, ctx, dep1) {
		t.Errorf("original dependency image %s must coexist with the new layer", dep1)
	}
	if !imageExistsInDaemon(cli, ctx, dep2) {
		t.Errorf("new dependency image %s must exist after the manifest change", dep2)
	}
}

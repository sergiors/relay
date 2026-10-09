package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
)

// TestPrepareReuseTransfersImageLease pins that a reuse Prepare admits the
// app image lease and transfers it to the returned handle, so the caller
// (the registry publication) owns admitted authority from before the existence
// probe through publication.
func TestPrepareReuseTransfersImageLease(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "reuse-lease", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	// The image exists (200 on inspect) and the bootstrap label matches, so
	// Prepare takes the reuse path without a build.
	fp, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	image := ImageRef(fn.Name, fp)
	bootstrap := bootstrapForTest(t, fn)

	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/" + image + "/json", body: imageInspectJSON(image, bootstrap)},
	)
	m := newLifecycleManager(t, cli, context.Background())

	prepared, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("prepare reuse: %v", err)
	}
	if prepared.Lease() == nil {
		t.Fatal("a reused Prepare must transfer an admitted image lease to the handle")
	}
	if got := m.LeaseCount(image); got != 1 {
		t.Fatalf("lease count after Prepare = %d, want 1", got)
	}
	// Dropping the handle releases the image.
	prepared.ReleaseLease()
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("lease count after ReleaseLease = %d, want 0", got)
	}
}

// TestPrepareRetiringImageRejected pins that Prepare refuses to reuse an image
// already committed to removal, with a retryable ErrImageRetiring, so it never
// re-uses an image that ImageRemove is about to delete.
func TestPrepareRetiringImageRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := app.App{Name: "retiring-lease", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	fp, err := app.FingerprintApp(dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	image := ImageRef(fn.Name, fp)

	cli := newScriptedDockerClient(t)
	m := newLifecycleManager(t, cli, context.Background())
	// Commit retirement with no holders (the drain is immediate, but the gate
	// rejects new independent leases until finishRetire).
	m.leaseCoord().beginRetire(image)

	if _, err := m.Prepare(context.Background(), fn); !errors.Is(err, ErrImageRetiring) {
		t.Fatalf("Prepare against a retiring image = %v, want wrapped ErrImageRetiring", err)
	}
}

// TestPrepareNoRuntimeTakesNoLease pins that a no-runtime app (no image)
// acquires no lease, preserving the no-Docker behavior.
func TestPrepareNoRuntimeTakesNoLease(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("services:\n  - image: nginx:alpine\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	fn := app.App{
		Name:     "no-runtime-lease",
		Dir:      dir,
		Template: &app.Template{Services: []app.Service{{Image: "nginx:alpine"}}},
	}
	cli := newScriptedDockerClient(t)
	m := newLifecycleManager(t, cli, context.Background())

	prepared, err := m.Prepare(context.Background(), fn)
	if err != nil {
		t.Fatalf("no-runtime prepare: %v", err)
	}
	if prepared.Lease() != nil {
		t.Fatal("a no-runtime Prepare must not acquire an image lease")
	}
}

// bootstrapForTest returns the bootstrap label hash Prepare computes for the
// app, by planning it exactly as Prepare does.
func bootstrapForTest(t *testing.T, fn app.App) string {
	t.Helper()
	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	eng, err := engineFor(spec)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	p, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	h, err := bootstrapHash(p, arch)
	if err != nil {
		t.Fatalf("bootstrap hash: %v", err)
	}
	return h
}

// imageInspectJSON renders a minimal ImageInspect response whose Config.Labels
// carry the given bootstrap hash so bootstrapLabelMatches accepts the reuse.
func imageInspectJSON(image, bootstrap string) string {
	return `{"Id":"sha256:abc","RepoTags":["` + image + `"],"Config":{"Labels":{"relay.bootstrap":"` + bootstrap + `"}}}`
}

package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime"
)

// resourceRecordingBuilder is a Builder that also implements the optional
// resourceSetter seam, recording every published resource configuration and
// counting Prepare calls. It models the runtime Manager without Docker.
type resourceRecordingBuilder struct {
	mu        sync.Mutex
	prepares  int
	published map[string][]function.ResourceLimits
}

func (b *resourceRecordingBuilder) Prepare(_ context.Context, fn function.Function) (*runtime.Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepares++
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (b *resourceRecordingBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

func (b *resourceRecordingBuilder) SetFunctionResources(name string, limits function.ResourceLimits) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.published == nil {
		b.published = map[string][]function.ResourceLimits{}
	}
	b.published[name] = append(b.published[name], limits)
}

func (b *resourceRecordingBuilder) prepareCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepares
}

func (b *resourceRecordingBuilder) publishedFor(name string) []function.ResourceLimits {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]function.ResourceLimits(nil), b.published[name]...)
}

// TestReconcileResourceOnlyChangeSkipsPrepareAndPublishesResources pins the core
// hot-change contract end to end through the reconciler: editing ONLY the
// template's `resources` does not move the content fingerprint, so no rebuild is
// issued (Prepare count stays put), yet the effective limits are published to the
// runtime's resource seam so the live pool rotates containers. The image
// reference also stays unchanged.
func TestReconcileResourceOnlyChangeSkipsPrepareAndPublishesResources(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "res")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	const base = `runtime: node24
resources:
  memory: 128MiB
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(base), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	b := &resourceRecordingBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)
	r.reconcileFunction("res")
	if b.prepareCount() != 1 {
		t.Fatalf("prepare count = %d, want 1", b.prepareCount())
	}
	firstImage := reg.GetByName("res").Prepared().Image

	// Change resources ONLY. The fingerprint must not move, so no rebuild.
	const changed = `runtime: node24
resources:
  memory: 1GiB
  cpus: 0.5
  pids: 64
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(changed), 0o644); err != nil {
		t.Fatalf("rewrite template: %v", err)
	}
	r.reconcileFunction("res")
	if b.prepareCount() != 1 {
		t.Fatalf("a resource-only change triggered a rebuild: prepares = %d, want 1", b.prepareCount())
	}
	// The image reference is unchanged (no image churn; ownership untouched).
	if got := reg.GetByName("res").Prepared().Image; got != firstImage {
		t.Fatalf("image changed on a resource-only edit: %q -> %q", firstImage, got)
	}
	// The effective limits were published so the pool can rotate containers.
	published := b.publishedFor("res")
	if len(published) == 0 {
		t.Fatal("resource-only change did not publish any resource configuration")
	}
	want := function.ResourceLimits{MemoryBytes: 1 << 30, NanoCPUs: 500_000_000, PidsLimit: 64}
	if got := published[len(published)-1]; got != want {
		t.Fatalf("last published resources = %+v, want %+v", got, want)
	}
}

// TestReconcileResourceOnlyChangeDoesNotChurnBuilder pins that a periodic
// reconcile of an UNCHANGED function publishes the same limits repeatedly (the
// runtime no-ops them) and never rebuilds: the reconciler's skip path is a cheap
// no-op for the image and idempotent for resources.
func TestReconcileResourceOnlyChangeDoesNotChurnBuilder(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "stable")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	const tmpl = `runtime: node24
resources:
  memory: 256MiB
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	b := &resourceRecordingBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)
	r.reconcileFunction("stable")
	r.reconcileFunction("stable")
	r.reconcileFunction("stable")
	if b.prepareCount() != 1 {
		t.Fatalf("prepare count = %d, want 1 (unchanged content never rebuilds)", b.prepareCount())
	}
	for _, got := range b.publishedFor("stable") {
		if got != (function.ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: function.DefaultResourceNanoCPUs, PidsLimit: function.DefaultResourcePidsLimit}) {
			t.Fatalf("published resources = %+v, want the configured limits", got)
		}
	}
}

// TestReconcileNonResourceEditStillRebuilds is the control: a NON-resource
// template edit still changes the fingerprint and rebuilds, so excluding
// resources from the digest did not accidentally make the template inert.
func TestReconcileNonResourceEditStillRebuilds(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "edit")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	const base = `runtime: node24
resources:
  memory: 128MiB
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(base), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	b := &resourceRecordingBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)
	r.reconcileFunction("edit")
	if b.prepareCount() != 1 {
		t.Fatalf("prepare count = %d, want 1", b.prepareCount())
	}

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(`runtime: node24
resources:
  memory: 128MiB
events:
  - handler: index.hi
    pattern:
      event_name: [MODIFY]
`), 0o644); err != nil {
		t.Fatalf("rewrite template: %v", err)
	}
	r.reconcileFunction("edit")
	if b.prepareCount() != 2 {
		t.Fatalf("a non-resource edit must rebuild: prepares = %d, want 2", b.prepareCount())
	}
}

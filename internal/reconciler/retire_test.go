package reconciler

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// retireRecorder captures the (name, image) pairs a hook is called with.
type retireRecorder struct {
	mu   sync.Mutex
	args []string // "name=image"
}

func (r *retireRecorder) add(name, image string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.args = append(r.args, name+"="+image)
}

func (r *retireRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.args...)
}

// newTestReconcilerRetire is like newTestReconciler but wires the image-lifecycle
// hooks so reconcile drives retirement.
func newTestReconcilerRetire(t *testing.T, root string, builder Builder, initial []*runner.PreparedFunction, rec *retireRecorder) (*Reconciler, *runner.Registry) {
	t.Helper()
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{
		Root:     root,
		Debounce: 10 * time.Millisecond,
		Interval: time.Hour,
	}
	if rec != nil {
		cfg.Retire = rec.add
	}
	r := New(cfg, reg, builder, log.New(os.Stderr, "test: ", 0))
	for _, pf := range initial {
		r.Seed(pf.Function())
	}
	return r, reg
}

// versionedBuilder returns a distinct image reference for each Prepare call, so
// a content change produces a genuinely different image to swap and retire.
type versionedBuilder struct {
	mu      sync.Mutex
	version int
}

func (v *versionedBuilder) Prepare(ctx context.Context, fn function.Function) (*runtime.Prepared, error) {
	v.mu.Lock()
	v.version++
	ver := v.version
	v.mu.Unlock()
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name + "-v" + strconv.Itoa(ver)}, nil
}

func (v *versionedBuilder) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	return nil
}

// When a function's image changes on reconcile, the Retire hook fires with the
// function name and the superseded (old) image, and only after the registry has
// been swapped to the new version.
func TestReconcileRetiresOldImageOnSwap(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "swap")

	// The same versioned builder serves both the startup entry and the rebuild.
	// Its counter starts at 1 to represent the already-built startup image, so
	// the reconcile prepare produces v2 — a genuinely different image to retire
	// v1 in favour of.
	b := &versionedBuilder{version: 1}
	fn := runner.NewPrepared(
		function.Function{Name: "swap", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "swap", Image: "img-swap-v1"},
		b,
	)

	rec := &retireRecorder{}
	r, reg := newTestReconcilerRetire(t, root, b, []*runner.PreparedFunction{fn}, rec)

	// Change source so the fingerprint changes and a rebuild is warranted.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	r.reconcileFunction("swap")

	if b.version != 2 {
		t.Fatalf("expected 1 prepare (to v2), got final version %d", b.version)
	}
	got := rec.all()
	if len(got) != 1 || got[0] != "swap=img-swap-v1" {
		t.Fatalf("retire calls = %v, want [swap=img-swap-v1]", got)
	}
	// The registry now serves the new image, not the retired one.
	if pf := reg.GetByName("swap"); pf == nil || pf.Prepared() == nil || pf.Prepared().Image != "img-swap-v2" {
		t.Fatalf("registry should serve the new image after swap, got %+v", reg.GetByName("swap"))
	}
}

// The Retire hook does not fire when a rebuild fails (the old version is
// retained, not retired).
func TestReconcileNoRetireOnFailedBuild(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := runner.NewPrepared(
		function.Function{Name: "flaky", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "flaky", Image: "img-flaky-v1"},
		&fakeBuilder{},
	)
	// A failing builder so the rebuild never succeeds.
	failb := &fakeBuilder{fail: true}

	rec := &retireRecorder{}
	r, _ := newTestReconcilerRetire(t, root, failb, []*runner.PreparedFunction{fn}, rec)

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	r.reconcileFunction("flaky")

	if got := rec.all(); len(got) != 0 {
		t.Fatalf("expected no retire on failed build, got %v", got)
	}
}

// The Retire hook does not fire when the fingerprint is unchanged (skip path).
func TestReconcileNoRetireOnSkipPath(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := runner.NewPrepared(
		function.Function{Name: "stable", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "stable", Image: "img-stable-v1"},
		&fakeBuilder{},
	)
	rec := &retireRecorder{}
	r, _ := newTestReconcilerRetire(t, root, &fakeBuilder{}, []*runner.PreparedFunction{fn}, rec)

	r.reconcileFunction("stable")
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("expected no retire on unchanged function, got %v", got)
	}
}

// Function removal fires the RemoveFunction hook.
func TestReconcileRemovalInvokesRemoveFunctionHook(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gone")

	fn := runner.NewPrepared(
		function.Function{Name: "gone", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "gone", Image: "img-gone-v1"},
		&fakeBuilder{},
	)
	var calls int
	r, reg := newTestReconcilerRemoval(t, root, &fakeBuilder{}, []*runner.PreparedFunction{fn}, func(string) {
		calls++
	})
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	r.reconcileFunction("gone")
	if reg.GetByName("gone") != nil {
		t.Fatal("gone should be dropped from the registry")
	}
	if calls != 1 {
		t.Fatalf("RemoveFunction hook calls = %d, want 1", calls)
	}
}

// newTestReconcilerRemoval wires the RemoveFunction hook.
func newTestReconcilerRemoval(t *testing.T, root string, builder Builder, initial []*runner.PreparedFunction, removeFn func(string)) (*Reconciler, *runner.Registry) {
	t.Helper()
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, RemoveFunction: removeFn}
	r := New(cfg, reg, builder, log.New(os.Stderr, "test: ", 0))
	for _, pf := range initial {
		r.Seed(pf.Function())
	}
	return r, reg
}

package reconciler

import (
	"context"
	"log/slog"

	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
)

var errBoom = contextCanceledSentinel{}

type contextCanceledSentinel struct{}

func (contextCanceledSentinel) Error() string { return "boom" }

// Records Prepare calls and can be configured to fail or return a prepared
// handle, without touching Docker or Redis.
type fakeBuilder struct {
	mu      sync.Mutex
	prepCnt int
	fail    bool
}

func (f *fakeBuilder) Prepare(ctx context.Context, fn function.Function) (*runtime.Prepared, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepCnt++
	if f.fail {
		return nil, errBoom
	}
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (f *fakeBuilder) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	return nil
}

func (f *fakeBuilder) prepares() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prepCnt
}

const template = "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"

func writeFnDir(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(template), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	return dir
}

func initialFn(name, dir string) *runner.PreparedFunction {
	tmpl := mustParse(template)
	return runner.NewPrepared(
		function.Function{Name: name, Dir: dir, Template: tmpl},
		&runtime.Prepared{Name: name, Image: "img-" + name},
		&fakeBuilder{},
	)
}

func mustParse(s string) *function.Template {
	t, err := function.ParseTemplate([]byte(s))
	if err != nil {
		panic(err)
	}
	return t
}

// A dir present on disk but not in the registry is built (Prepare called) and
// added.
func TestReconcileNewFunctionDiscovered(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "brand-new")

	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)

	r.reconcileFunction("brand-new")

	if b.prepares() != 1 {
		t.Fatalf("expected 1 prepare call, got %d", b.prepares())
	}
	if pf := reg.GetByName("brand-new"); pf == nil || pf.Prepared() == nil {
		t.Fatal("expected brand-new to be registered as prepared")
	}
}

// An identical function is not rebuilt.
func TestReconcileUnchangedFingerprintSkipped(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := initialFn("stable", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	r.reconcileFunction("stable")

	if b.prepares() != 0 {
		t.Fatalf("expected 0 prepare calls for unchanged function, got %d", b.prepares())
	}
	if reg.GetByName("stable") == nil {
		t.Fatal("stable should remain registered")
	}
}

// Editing content changes the fingerprint, triggering a rebuild and swap.
func TestReconcileChangedFunctionRebuilt(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "changing")

	fn := initialFn("changing", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Change source content.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	r.reconcileFunction("changing")

	if b.prepares() != 1 {
		t.Fatalf("expected 1 prepare call after change, got %d", b.prepares())
	}
	if pf := reg.GetByName("changing"); pf == nil || pf.Prepared() == nil {
		t.Fatal("changing should still be registered and prepared")
	}
}

// A broken template must NOT drop the previously-active function or rebuild.
func TestReconcileInvalidTemplateKeepsOld(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Break the template.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}

	r.reconcileFunction("guarded")

	if b.prepares() != 0 {
		t.Fatalf("expected 0 prepares for invalid template, got %d", b.prepares())
	}
	if pf := reg.GetByName("guarded"); pf == nil {
		t.Fatal("guarded must remain registered despite broken template")
	}
}

// A dir gone entirely is removed, but a dir that only lacks a template
// (mid-copy) is left alone.
func TestReconcileMissingDirsRetainsActive(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "tobe-removed")

	fn := initialFn("tobe-removed", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Remove the whole directory -> function dropped.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileFunction("tobe-removed")
	if reg.GetByName("tobe-removed") != nil {
		t.Fatal("removed dir should drop the function from the registry")
	}

	// "midcopy" dir exists without template -> must not be dropped nor removed.
	mid := filepath.Join(root, "midcopy")
	if err := os.MkdirAll(mid, 0o755); err != nil {
		t.Fatalf("mkdir midcopy: %v", err)
	}
	midFn := initialFn("midcopy", mid)
	reg.Set([]*runner.PreparedFunction{midFn})
	r.Seed(midFn.Function())

	r.reconcileFunction("midcopy")
	if reg.GetByName("midcopy") == nil {
		t.Fatal("dir without template must not be removed")
	}
	if b.prepares() != 0 {
		t.Fatalf("expected no rebuild for dir without template, got %d", b.prepares())
	}
}

// A failed rebuild keeps the old active version and does not drop the function;
// because the stored fingerprint stays the old one, a later pass retries the
// build even without another file change.
func TestReconcileFailedBuildRetainsOld(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Change content so a rebuild is warranted, then make it fail.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = true
	r.reconcileFunction("flaky")
	if b.prepares() != 1 {
		t.Fatalf("expected 1 failed prepare attempt, got %d", b.prepares())
	}
	// Old version retained (still prepared) despite the failure.
	if pf := reg.GetByName("flaky"); pf == nil || pf.Prepared() == nil {
		t.Fatal("failed rebuild must retain the previously-active version")
	}
	// A second pass retries because the stored fingerprint still matches the OLD
	// build, not the changed sources.
	r.reconcileFunction("flaky")
	if b.prepares() != 2 {
		t.Fatalf("expected a retry on the next pass, got %d", b.prepares())
	}

	// Success replaces the old version.
	b.fail = false
	r.reconcileFunction("flaky")
	if b.prepares() != 3 {
		t.Fatalf("expected a success prepare, got %d", b.prepares())
	}
	if pf := reg.GetByName("flaky"); pf == nil || pf.Prepared() == nil {
		t.Fatal("flaky should be registered and prepared after success")
	}
}

// A function whose startup build failed is retried by the reconciler without a
// source change.
func TestUnavailableFunctionRetriedOnPeriodicReconcile(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "recover")

	tmpl := mustParse(template)
	// Startup failed: registered as unavailable (no image).
	unavail := runner.NewUnavailable(function.Function{Name: "recover", Dir: dir, Template: tmpl})
	b := &fakeBuilder{}

	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{unavail})
	r := New(Config{Root: root, Debounce: time.Millisecond, Interval: time.Hour}, reg, b, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	r.Seed(unavail.Function())

	// Even with an unchanged fingerprint, the unavailable function is rebuilt.
	r.reconcileFunction("recover")
	if b.prepares() != 1 {
		t.Fatalf("expected 1 prepare to recover the unavailable function, got %d", b.prepares())
	}
	if pf := reg.GetByName("recover"); pf == nil || pf.Prepared() == nil {
		t.Fatal("recover should be prepared after successful reconcile")
	}
}

// TestReconcileAllDiscoversAndRemoves verifies the periodic backstop:
// reconcileAll reconciles every on-disk function AND every registered function
// whose directory vanished (removal). It drives the real single pump so the
// semantics match production.
func TestReconcileAllDiscoversAndRemoves(t *testing.T) {
	root := t.TempDir()
	// One live dir on disk + one registered function whose dir is gone.
	liveDir := writeFnDir(t, root, "live")
	goneDir := writeFnDir(t, root, "gone")

	live := initialFn("live", liveDir)
	gone := initialFn("gone", goneDir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedFunction{live, gone}, nil)

	// Remove "gone" only from disk; the registry still knows it.
	if err := os.RemoveAll(goneDir); err != nil {
		t.Fatalf("removeall gone: %v", err)
	}
	// Delete "live" content so the fingerprint changes and it must be rebuilt.
	if err := os.WriteFile(filepath.Join(liveDir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	go r.pump()
	r.reconcileAll()

	waitForPrepare(t, b, 1)
	if pf := reg.GetByName("live"); pf == nil || pf.Prepared() == nil {
		t.Fatal("live should remain registered and prepared")
	}
	if reg.GetByName("gone") != nil {
		t.Fatal("gone (dir vanished) should have been removed by reconcileAll")
	}
}

// waitForPrepare polls until the builder has recorded want prepare calls.
func waitForPrepare(t *testing.T, b *fakeBuilder, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b.prepares() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("prepare calls = %d, want >= %d", b.prepares(), want)
}

// TestReconcileSameImageRetireSkipped verifies the Retire hook does not fire
// when the rebuilt image reference equals the old one (a same-image
// re-prepare, or an unavailable->available retry with a stable fingerprint):
// there is nothing superseded to retire.
func TestReconcileSameImageRetireSkipped(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "same")

	fn := runner.NewPrepared(
		function.Function{Name: "same", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "same", Image: "img-same"},
		&fixedImageBuilder{image: "img-same"},
	)
	var retires int
	r, reg := newTestReconciler(t, root, &fixedImageBuilder{image: "img-same"}, []*runner.PreparedFunction{fn},
		func(cfg *Config) { cfg.Retire = func(string, string) { retires++ } })

	// Change content so a rebuild is warranted; the builder returns the SAME
	// image, so the retire must be skipped.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction("same")

	if retires != 0 {
		t.Fatalf("retire calls = %d, want 0 (same image is never retired)", retires)
	}
	if pf := reg.GetByName("same"); pf == nil || pf.Prepared() == nil || pf.Prepared().Image != "img-same" {
		t.Fatalf("registry should serve the same image, got %+v", reg.GetByName("same"))
	}
}

// fixedImageBuilder always returns the same image reference.
type fixedImageBuilder struct{ image string }

func (b *fixedImageBuilder) Prepare(_ context.Context, fn function.Function) (*runtime.Prepared, error) {
	return &runtime.Prepared{Name: fn.Name, Image: b.image}, nil
}

func (b *fixedImageBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

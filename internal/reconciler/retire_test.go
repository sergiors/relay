package reconciler

import (
	"context"
	"log/slog"

	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
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

// versionedBuilder returns a distinct image reference for each Prepare call, so
// a content change produces a genuinely different image to swap and retire.
type versionedBuilder struct {
	mu      sync.Mutex
	version int
}

func (v *versionedBuilder) Prepare(ctx context.Context, fn app.App) (*runtime.Prepared, error) {
	v.mu.Lock()
	v.version++
	ver := v.version
	v.mu.Unlock()
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name + "-v" + strconv.Itoa(ver)}, nil
}

func (v *versionedBuilder) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte, _ []string) error {
	return nil
}

// When an app's image changes on reconcile, the Retire hook fires with the
// app name and the superseded (old) image, and only after the registry has
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
		app.App{Name: "swap", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "swap", Image: "img-swap-v1"},
		b,
	)

	rec := &retireRecorder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.Retire = rec.add
	})

	// Change source so the fingerprint changes and a rebuild is warranted.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	r.reconcileApp("swap")

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
		app.App{Name: "flaky", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "flaky", Image: "img-flaky-v1"},
		&fakeBuilder{},
	)
	// A failing builder so the rebuild never succeeds.
	failb := &fakeBuilder{fail: true}

	rec := &retireRecorder{}
	r, _ := newTestReconciler(t, root, failb, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.Retire = rec.add
	})

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	r.reconcileApp("flaky")

	if got := rec.all(); len(got) != 0 {
		t.Fatalf("expected no retire on failed build, got %v", got)
	}
}

// The Retire hook runs AFTER UpdateServices on a rebuild: an image must not be
// retired while a service container still references it, and service convergence
// (which replaces that container) must happen first. This pins the
// build → swap → schedules → services → retire ordering.
func TestReconcileRetireRunsAfterServiceConvergence(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-retire-order")

	b := &versionedBuilder{version: 1}
	fn := initialServicesFn("svc-retire-order", dir, "img-svc-retire-order-v1", b)
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{fn})

	// Wire BOTH hooks to a single shared, order-preserving recorder.
	var order []string
	var mu sync.Mutex
	cfg := Config{
		Root:     root,
		Debounce: 10 * time.Millisecond,
		Interval: time.Hour,
		UpdateServices: func(name string, tmpl *app.Template, image string) {
			mu.Lock()
			order = append(order, "update "+name+"="+strconv.Itoa(len(tmpl.Services))+"@"+image)
			mu.Unlock()
		},
		Retire: func(name, oldImage string) {
			mu.Lock()
			order = append(order, "retire "+name+"="+oldImage)
			mu.Unlock()
		},
	}
	r := New(cfg, reg, b, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	seedCurrent(r, fn.App())

	// Change source so a rebuild (to v2) warrants retiring v1.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("svc-retire-order")

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "update svc-retire-order=1@img-svc-retire-order-v2" || order[1] != "retire svc-retire-order=img-svc-retire-order-v1" {
		t.Fatalf("hook order = %v, want [update svc-retire-order=1@img-svc-retire-order-v2, retire svc-retire-order=img-svc-retire-order-v1]", order)
	}
}

// The Retire hook does not fire when the fingerprint is unchanged (skip path).
func TestReconcileNoRetireOnSkipPath(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := runner.NewPrepared(
		app.App{Name: "stable", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "stable", Image: "img-stable-v1"},
		&fakeBuilder{},
	)
	rec := &retireRecorder{}
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.Retire = rec.add
	})

	r.reconcileApp("stable")
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("expected no retire on unchanged function, got %v", got)
	}
}

// App removal has ONE contract: the eager RemoveAppResources drop runs in the
// serial pump pass, then RemoveServices starts the container removal WITHOUT
// blocking and receives the settlement callback. Nothing is retired before the
// settlement runs; when it runs with no live generation again, RetireAppImages
// performs the deferred image retirement.
func TestReconcileRemovalDropsResourcesEagerlyAndSettlesRetirement(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gone")

	fn := runner.NewPrepared(
		app.App{Name: "gone", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "gone", Image: "img-gone-v1"},
		&fakeBuilder{},
	)
	var order []string
	var mu sync.Mutex
	var retired func()
	r, reg := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.RemoveAppResources = func(name string) {
			mu.Lock()
			order = append(order, "resources "+name)
			mu.Unlock()
		}
		cfg.RemoveServices = func(name string, settle func()) {
			mu.Lock()
			order = append(order, "services "+name)
			retired = settle
			mu.Unlock()
		}
		cfg.RetireAppImages = func(name string) {
			mu.Lock()
			order = append(order, "retire "+name)
			mu.Unlock()
		}
	})
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	r.reconcileApp("gone")

	if reg.GetByName("gone") != nil {
		t.Fatal("gone should be dropped from the registry")
	}
	mu.Lock()
	got := append([]string(nil), order...)
	settle := retired
	mu.Unlock()
	if len(got) != 2 || got[0] != "resources gone" || got[1] != "services gone" {
		t.Fatalf("removal order = %v, want eager resources then the non-blocking "+
			"service removal, with retirement still deferred", got)
	}
	if settle == nil {
		t.Fatal("RemoveServices received no settlement callback")
	}

	// The settlement concludes the removal: with no live generation again, the
	// deferred retirement finally runs — exactly once.
	settle()
	mu.Lock()
	got = append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 3 || got[2] != "retire gone" {
		t.Fatalf("order after settlement = %v, want [resources gone services gone retire gone]",
			got)
	}
}

// A valid re-add between removal and settlement must not lose its images: the
// freshness gate sees the live published generation again and the settlement
// performs no retirement, while the eager resource drop still happened in the
// removal pass (a later re-add re-registers its own resources).
func TestReconcileRemovalSettlementSkipsRetirementForReaddedApp(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "readded")

	fn := runner.NewPrepared(
		app.App{Name: "readded", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "readded", Image: "img-readded-v1"},
		&fakeBuilder{},
	)
	var resources, retires int
	var mu sync.Mutex
	var retired func()
	r, reg := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.RemoveAppResources = func(string) {
			mu.Lock()
			resources++
			mu.Unlock()
		}
		cfg.RemoveServices = func(_ string, settle func()) {
			mu.Lock()
			retired = settle
			mu.Unlock()
		}
		cfg.RetireAppImages = func(string) {
			mu.Lock()
			retires++
			mu.Unlock()
		}
	})

	// The app disappears; removal is enqueued with its settlement retained.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	r.reconcileApp("readded")
	mu.Lock()
	settle := retired
	mu.Unlock()
	if settle == nil {
		t.Fatal("precondition: the removal settlement was not captured")
	}

	// A valid re-add publishes a live generation BEFORE the settlement runs.
	writeFnDir(t, root, "readded")
	r.reconcileApp("readded")
	if pf := reg.GetByName("readded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("precondition: the re-added app must publish a live generation")
	}

	settle()
	mu.Lock()
	gotResources, gotRetires := resources, retires
	mu.Unlock()
	if gotResources != 1 {
		t.Fatalf("RemoveAppResources calls = %d, want 1 (the eager drop still ran)",
			gotResources)
	}
	if gotRetires != 0 {
		t.Fatalf("RetireAppImages calls = %d, want 0 (a live re-add keeps its images)",
			gotRetires)
	}
}

// scheduleRecorder records the (name, template-handler-count) pairs a hook is
// called with, so a test can assert convergence timing and frequency.
type scheduleRecorder struct {
	mu   sync.Mutex
	args []string // "name=scheduleCount"
}

func (s *scheduleRecorder) add(name string, tmpl *app.Template) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.args = append(s.args, name+"="+strconv.Itoa(len(tmpl.Schedules)))
}

func (s *scheduleRecorder) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.args...)
}

// On discovery and on update after a swap, the UpdateSchedules hook fires with
// the app name and its template. It does not fire on the skip path.
func TestReconcileUpdateSchedulesHookFires(t *testing.T) {
	root := t.TempDir()
	rec := &scheduleRecorder{}
	withSchedules := func(cfg *Config) { cfg.UpdateSchedules = rec.add }

	// Discovery of a brand-new app (not yet in the registry) fires the hook.
	writeFnDir(t, root, "brand-new")
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, nil, withSchedules)
	r.reconcileApp("brand-new")
	if got := rec.all(); len(got) != 1 || got[0] != "brand-new=0" {
		t.Fatalf("UpdateSchedules after discovery = %v, want [brand-new=0]", got)
	}

	// Update path: a registered app whose content changes fires the hook
	// again with the fresh template.
	updateDir := filepath.Join(root, "changing")
	if err := os.MkdirAll(updateDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(updateDir, "template.yaml"), []byte(template), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(updateDir, "index.js"), []byte("export function hi(e, context){ console.log('v1'); }\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	b := &versionedBuilder{version: 1}
	chgFn := runner.NewPrepared(
		app.App{Name: "changing", Dir: updateDir, Template: mustParse(template)},
		&runtime.Prepared{Name: "changing", Image: "img-changing-v1"},
		b,
	)
	r2, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{chgFn}, withSchedules)
	// Change source so an update warrants a rebuild.
	if err := os.WriteFile(filepath.Join(updateDir, "index.js"), []byte("export function hi(e, context){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r2.reconcileApp("changing")
	if got := rec.all(); len(got) != 2 || got[1] != "changing=0" {
		t.Fatalf("UpdateSchedules after update = %v, want two calls ending [changing=0]", got)
	}

	// Skip path: an unchanged app does not fire the hook.
	before := len(rec.all())
	r2.reconcileApp("changing")
	if got := len(rec.all()); got != before {
		t.Fatalf("UpdateSchedules fired on the skip path: %v", rec.all())
	}
}

// On removal, the UpdateSchedules hook does NOT fire (removal converges via
// RemoveApp).
func TestReconcileUpdateSchedulesHookNotFiredOnRemoval(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gone")

	fn := runner.NewPrepared(
		app.App{Name: "gone", Dir: dir, Template: mustParse(template)},
		&runtime.Prepared{Name: "gone", Image: "img-gone-v1"},
		&fakeBuilder{},
	)
	rec := &scheduleRecorder{}
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.UpdateSchedules = rec.add
	})

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	r.reconcileApp("gone")
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("UpdateSchedules must not fire on removal, got %v", got)
	}
}

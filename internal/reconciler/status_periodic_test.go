package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// readyServicesReconciler builds a reconciler over a function that declares one
// service, with the registry carrying an available (prepared) entry and the
// state row recorded as ready — the state of an unchanged, already-converged
// function that the periodic tick verifies. It returns the reconciler, the
// function, and the state handle.
func readyServicesReconciler(t *testing.T, root, name string) (*Reconciler, function.Function, *state.State) {
	t.Helper()
	dir := writeServicesDir(t, root, name)
	tmpl := mustParse(servicesTemplate)
	fn := function.Function{Name: name, Dir: dir, Template: tmpl}

	pf := runner.NewPrepared(fn, &runtime.Prepared{Name: name, Image: "img-" + name}, &fakeBuilder{})
	r, _, st := newTestStateReconciler(t, root, &fakeBuilder{}, []*runner.PreparedFunction{pf}, nil)
	// newTestStateReconciler's seeding records the function as discovered
	// (preparing); make it ready, the status an unchanged periodic tick verifies.
	fp, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	st.RecordReconcileSuccess(name, "img-"+name, fp, time.Now(), fn)
	return r, fn, st
}

// TestUnchangedPeriodicPathDoesNotRecordPreparing pins the focused fix: the
// unchanged, already-available skip branch must NOT write the public preparing
// status (nor any status) just because it enqueues a periodic service
// self-heal. With a converged service pass (a no-op verification completing
// without corrective work), status and every reconcile timestamp stay ready.
func TestUnchangedPeriodicPathDoesNotRecordPreparing(t *testing.T) {
	root := t.TempDir()
	r, fn, st := readyServicesReconciler(t, root, "svc-unchanged")

	// A converged pass: the hook completes without firing reconcile-start,
	// exactly as the coordinator does for a no-op verification.
	r.updateServicesWithStatus = func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
		onComplete(nil)
	}

	before, _ := st.GetFunction(fn.Name)
	r.reconcileFunction(fn.Name)
	after, ok := st.GetFunction(fn.Name)
	if !ok {
		t.Fatal("expected state row")
	}
	if after.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready (unchanged periodic path must not write preparing)", after.Status)
	}
	if after.LastReconcileStatus != before.LastReconcileStatus {
		t.Fatalf("last_reconcile_status = %q, want unchanged %q", after.LastReconcileStatus, before.LastReconcileStatus)
	}
	if after.LastReconcileAt != before.LastReconcileAt {
		t.Fatalf("last_reconcile_at = %q, want unchanged %q", after.LastReconcileAt, before.LastReconcileAt)
	}
	if after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("updated_at = %q, want unchanged %q", after.UpdatedAt, before.UpdatedAt)
	}
}

// TestNoOpPeriodicPassStaysReadyWithoutReconciling pins that a fully-converged
// periodic pass emits no reconciling transition: the service observer fires
// only for actual corrective work, so a no-op verification leaves ready intact.
func TestNoOpPeriodicPassStaysReadyWithoutReconciling(t *testing.T) {
	root := t.TempDir()
	r, fn, st := readyServicesReconciler(t, root, "svc-noop")

	r.updateServicesWithStatus = func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
		// A converged pass performs no corrective work: no onReconcileStart.
		onComplete(nil)
	}

	r.reconcileFunction(fn.Name)
	after, _ := st.GetFunction(fn.Name)
	if after.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after a no-op periodic pass", after.Status)
	}
}

// statusProbeBuilder records the persisted function status observed at the
// Prepare boundary, proving the reconciler wrote preparing BEFORE preparing the
// new generation's image.
type statusProbeBuilder struct {
	st    *state.State
	name  string
	seen  string
	image string
}

func (b *statusProbeBuilder) Prepare(_ context.Context, fn function.Function) (*runtime.Prepared, error) {
	if d, ok := b.st.GetFunction(b.name); ok {
		b.seen = d.Status
	}
	return &runtime.Prepared{Name: fn.Name, Image: b.image}, nil
}

func (b *statusProbeBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// TestActualDesiredChangeRecordsPreparingBeforePrepare pins that the rebuild
// path still writes preparing before the current generation's Prepare runs: a
// real desired-generation change is not silently treated as a periodic no-op.
func TestActualDesiredChangeRecordsPreparingBeforePrepare(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-change")
	tmpl := mustParse(servicesTemplate)
	fn := function.Function{Name: "svc-change", Dir: dir, Template: tmpl}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordReconcileSuccess(fn.Name, "img-v1", "fp-v1", time.Now(), fn)

	pf := runner.NewPrepared(fn, &runtime.Prepared{Name: fn.Name, Image: "img-v1"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{pf})

	probe := &statusProbeBuilder{st: st, name: fn.Name, image: "img-v2"}
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServices: func(string, string, *function.Template, string) {},
	}, reg, probe, testutil.DiscardLogger())
	r.Seed(fn)

	// Change content so the fingerprint changes and a rebuild is warranted.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction(fn.Name)

	if probe.seen != state.StatusPreparing {
		t.Fatalf("status observed at Prepare = %q, want preparing (recorded before the build)", probe.seen)
	}
}

// TestBuildReconcilingReadyOrderingOnChange pins the persisted lifecycle for a
// real desired-generation change with services: building (at the build
// boundary) -> reconciling (at the convergence seam) -> ready (at completion).
func TestBuildReconcilingReadyOrderingOnChange(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-order")
	tmpl := mustParse(servicesTemplate)
	fn := function.Function{Name: "svc-order", Dir: dir, Template: tmpl}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordReconcileSuccess(fn.Name, "img-v1", "fp-v1", time.Now(), fn)

	pf := runner.NewPrepared(fn, &runtime.Prepared{Name: fn.Name, Image: "img-svc-order"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{pf})

	var seen []string
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
			onBuildStart()
			if d, ok := st.GetFunction(name); ok {
				seen = append(seen, d.Status)
			}
			onReconcileStart()
			if d, ok := st.GetFunction(name); ok {
				seen = append(seen, d.Status)
			}
			onComplete(nil)
			if d, ok := st.GetFunction(name); ok {
				seen = append(seen, d.Status)
			}
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	r.Seed(fn)

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction(fn.Name)

	want := []string{state.StatusBuilding, state.StatusReconciling, state.StatusReady}
	if len(seen) != len(want) {
		t.Fatalf("persisted statuses = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("persisted statuses = %v, want %v", seen, want)
		}
	}
}

// TestCorrectivePeriodicPassCanReconcile pins that an unchanged periodic pass
// that performs real corrective service work (a crashed replica recreated)
// transitions ready -> reconciling -> ready, recording a fresh success while
// retaining the active image.
func TestCorrectivePeriodicPassCanReconcile(t *testing.T) {
	root := t.TempDir()
	r, fn, st := readyServicesReconciler(t, root, "svc-heal")

	var seen []string
	r.updateServicesWithStatus = func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
		onReconcileStart()
		if d, ok := st.GetFunction(name); ok {
			seen = append(seen, d.Status)
		}
		onComplete(nil)
		if d, ok := st.GetFunction(name); ok {
			seen = append(seen, d.Status)
		}
	}

	r.reconcileFunction(fn.Name)

	if len(seen) != 2 || seen[0] != state.StatusReconciling || seen[1] != state.StatusReady {
		t.Fatalf("persisted statuses on a corrective pass = %v, want [reconciling ready]", seen)
	}
	after, _ := st.GetFunction(fn.Name)
	if after.Image != "img-"+fn.Name {
		t.Fatalf("image = %q, want the retained active image", after.Image)
	}
}

// TestActiveBuildPlusPeriodicTickDoesNotBecomePreparing pins that a periodic
// tick for an unchanged function whose status is building (an active build's
// status) must NOT regress the public status to preparing: the skip branch no
// longer writes preparing, and a no-op verification writes nothing.
func TestActiveBuildPlusPeriodicTickDoesNotBecomePreparing(t *testing.T) {
	root := t.TempDir()
	r, fn, st := readyServicesReconciler(t, root, "svc-building")

	// An active build's status stands in the store.
	st.RecordReconcileBuilding(fn.Name)
	before, _ := st.GetFunction(fn.Name)
	if before.Status != state.StatusBuilding {
		t.Fatalf("precondition: status = %q, want building", before.Status)
	}

	// A periodic tick verifies an unchanged, available function with a no-op
	// service pass.
	r.updateServicesWithStatus = func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
	}

	r.reconcileFunction(fn.Name)

	after, _ := st.GetFunction(fn.Name)
	if after.Status != state.StatusBuilding {
		t.Fatalf("status = %q, want building (periodic tick must not become preparing)", after.Status)
	}
	if after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("updated_at = %q, want unchanged %q", after.UpdatedAt, before.UpdatedAt)
	}
}

// TestPeriodicSkipPathStaleCallbacksRemainGuarded pins the generation guard on
// the unchanged periodic path: callbacks captured for an older generation must
// be a no-op once a newer periodic generation is current, so a slow stale
// convergence can never resurrect an outdated ready/reconciling status.
func TestPeriodicSkipPathStaleCallbacksRemainGuarded(t *testing.T) {
	root := t.TempDir()
	r, fn, st := readyServicesReconciler(t, root, "svc-skipgen")

	type callbacks struct {
		onReconcile func()
		onComplete  func(error)
	}
	var captured []callbacks
	r.updateServicesWithStatus = func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
		captured = append(captured, callbacks{onReconcile: onReconcileStart, onComplete: onComplete})
	}

	r.reconcileFunction(fn.Name) // generation 1 (skip)
	r.reconcileFunction(fn.Name) // generation 2 (skip), now current

	if len(captured) != 2 {
		t.Fatalf("captured %d callback sets, want 2", len(captured))
	}
	before, _ := st.GetFunction(fn.Name)

	// The stale generation 1 begins and completes: both must be ignored.
	captured[0].onReconcile()
	captured[0].onComplete(nil)

	after, _ := st.GetFunction(fn.Name)
	if after.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready (stale callbacks must not land)", after.Status)
	}
	if after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("updated_at = %q, want unchanged %q (stale callbacks must be ignored)", after.UpdatedAt, before.UpdatedAt)
	}
	if after.LastReconcileStatus != before.LastReconcileStatus {
		t.Fatalf("last_reconcile_status = %q, want unchanged %q", after.LastReconcileStatus, before.LastReconcileStatus)
	}
}

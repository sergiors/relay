package reconciler

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/runner"
	"relay/internal/state"
)

// newStateReconciler builds a reconciler wired to a temp state DB, like the
// production path (Config.State set).
func newStateReconciler(t *testing.T, root string, builder Builder, initial []*runner.PreparedFunction) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	reg := &runner.Registry{}
	reg.Set(initial)
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	r := New(Config{
		Root:     root,
		Debounce: 10 * time.Millisecond,
		Interval: time.Hour,
		State:    st,
	}, reg, builder, log.New(os.Stderr, "test: ", 0))
	for _, pf := range initial {
		r.Seed(pf.Function())
		// Mirror production wiring: startup records each loaded function before
		// reconciliation, so reconcile hooks always find an existing row.
		st.RecordDiscovered(pf.Function())
	}
	return r, reg, st
}

// discovered -> the state DB records a ready row with handlers.
func TestReconcileStateDiscoverSuccess(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "brand-new")

	b := &fakeBuilder{}
	r, reg, st := newStateReconciler(t, root, b, nil)

	r.reconcileFunction("brand-new")

	if pf := reg.GetByName("brand-new"); pf == nil || pf.Prepared() == nil {
		t.Fatal("expected brand-new registered and prepared")
	}
	d, ok := st.GetFunction("brand-new")
	if !ok {
		t.Fatal("expected state row for brand-new")
	}
	if d.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready", d.Status)
	}
	if d.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want success", d.LastReconcileStatus)
	}
	if len(d.Handlers) != 1 {
		t.Fatalf("handlers = %d, want 1", len(d.Handlers))
	}
}

// Build failure retains the prior active state fields but records failed.
func TestReconcileStateFailureKeepsActiveAndMarksFailed(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	// Change so a rebuild is attempted, then make it fail.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	// Seed an active version first (mirrors the startup success path).
	func() {
		b.fail = false
		r.reconcileFunction("flaky") // success -> ready row
	}()
	before, _ := st.GetFunction("flaky")

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileFunction("flaky") // fails -> retained + failed

	d, ok := st.GetFunction("flaky")
	if !ok {
		t.Fatal("expected row")
	}
	if d.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready after failure", d.Status)
	}
	if d.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", d.LastReconcileStatus)
	}
	if d.Image != before.Image || d.Fingerprint != before.Fingerprint {
		t.Fatalf("failed reconcile must preserve active image/fingerprint: got %q/%q want %q/%q",
			d.Image, d.Fingerprint, before.Image, before.Fingerprint)
	}
}

// Unchanged healthy function -> state DB records skipped.
func TestReconcileStateUnchangedSkipped(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := initialFn("stable", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	r.reconcileFunction("stable") // unchanged -> skip

	d, ok := st.GetFunction("stable")
	if !ok {
		t.Fatal("expected row")
	}
	if d.LastReconcileStatus != state.ReconcileSkipped {
		t.Fatalf("last_reconcile_status = %s, want skipped", d.LastReconcileStatus)
	}
}

// Removal deletes the state row and handlers.
func TestReconcileStateRemoved(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "tobe-removed")

	fn := initialFn("tobe-removed", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileFunction("tobe-removed")

	if _, ok := st.GetFunction("tobe-removed"); ok {
		t.Fatal("state row should be removed")
	}
}

// Invalid template -> NO state write (no success/failure/skipped recorded).
func TestReconcileStateNoWriteOnInvalidTemplate(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("guarded")

	d, ok := st.GetFunction("guarded")
	if !ok {
		t.Fatal("row should exist from initial seeding")
	}
	if d.LastReconcileStatus != "" {
		t.Fatalf("invalid template must not write a reconcile outcome, got %q", d.LastReconcileStatus)
	}
}

// TestReconcileStateRemovalCleansAllTables is the live fsnotify-path regression
// for FULL cleanup on removal: reconciling a function whose directory was deleted
// must remove its functions row, function_stats, and handlers while leaving an
// unrelated function and the global stats row untouched.
func TestReconcileStateRemovalCleansAllTables(t *testing.T) {
	root := t.TempDir()
	victimDir := writeFnDir(t, root, "victim")
	bystanderDir := writeFnDir(t, root, "bystander")

	victim := initialFn("victim", victimDir)
	bystander := initialFn("bystander", bystanderDir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{victim, bystander})

	// Simulate prior activity: per-function counters for both and a cumulative
	// global row.
	st.RecordFunctionStats(state.FunctionStats{Function: "victim", EventsProcessedTotal: 5, HandlerSuccessTotal: 3})
	st.RecordFunctionStats(state.FunctionStats{Function: "bystander", EventsProcessedTotal: 7, HandlerSuccessTotal: 4})
	wantGlobal := state.Stats{EventsProcessedTotal: 12, HandlerSuccessTotal: 7}
	st.RecordStats(wantGlobal)

	// Capture bystander's handler count before the removal for the untouched check.
	bystanderBefore, ok := st.GetFunction("bystander")
	if !ok {
		t.Fatal("expected bystander row before removal")
	}

	// Remove the victim's directory and reconcile: the live removal path.
	if err := os.RemoveAll(victimDir); err != nil {
		t.Fatalf("removeall victim: %v", err)
	}
	r.reconcileFunction("victim")

	// Victim's functions row and function_stats are gone.
	if _, ok := st.GetFunction("victim"); ok {
		t.Fatal("victim functions row must be removed")
	}
	if _, ok := st.FunctionStats("victim"); ok {
		t.Fatal("victim function_stats must be removed")
	}

	// Handlers must not be resurrected: re-create the dir, reconcile, and the
	// detail must carry only the fresh template's handlers (no stale ones).
	writeFnDir(t, root, "victim")
	r.reconcileFunction("victim")
	d, ok := st.GetFunction("victim")
	if !ok {
		t.Fatal("expected victim re-discovered after re-creating dir")
	}
	if len(d.Handlers) != 1 {
		t.Fatalf("victim handlers = %d, want 1 (fresh template only)", len(d.Handlers))
	}

	// Bystander untouched: row, function_stats, and handler count unchanged.
	if _, ok := st.GetFunction("bystander"); !ok {
		t.Fatal("bystander functions row must survive")
	}
	bs, ok := st.FunctionStats("bystander")
	if !ok || bs.EventsProcessedTotal != 7 || bs.HandlerSuccessTotal != 4 {
		t.Fatalf("bystander function_stats = %+v, ok=%v; want events 7 success 4", bs, ok)
	}
	bystanderAfter, ok := st.GetFunction("bystander")
	if !ok {
		t.Fatal("expected bystander row after removal")
	}
	if len(bystanderAfter.Handlers) != len(bystanderBefore.Handlers) {
		t.Fatalf("bystander handler count changed: before=%d after=%d", len(bystanderBefore.Handlers), len(bystanderAfter.Handlers))
	}

	// Global stats row unchanged.
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected global stats row")
	}
	if gs.EventsProcessedTotal != wantGlobal.EventsProcessedTotal || gs.HandlerSuccessTotal != wantGlobal.HandlerSuccessTotal {
		t.Fatalf("global stats changed by removal: %+v, want %+v", gs, wantGlobal)
	}
}

// TestReconcileStateFailedBuildDoesNotRemoveStats is a regression that a failed
// rebuild (not a removal) never deletes state or stats: the function row stays
// ready, its function_stats row keeps its original counters and updated_at, and
// no removal is recorded.
func TestReconcileStateFailedBuildDoesNotRemoveStats(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	// Change content so a rebuild is attempted, then seed an active version and
	// per-function stats, as production would after activity.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileFunction("flaky") // success -> ready row
	st.RecordFunctionStats(state.FunctionStats{Function: "flaky", EventsProcessedTotal: 5, HandlerSuccessTotal: 3})

	before, ok := st.FunctionStats("flaky")
	if !ok {
		t.Fatal("expected flaky stats before failed rebuild")
	}

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileFunction("flaky") // fails -> retained + failed

	// Function row still exists, still ready, marked failed (not removed).
	d, ok := st.GetFunction("flaky")
	if !ok {
		t.Fatal("flaky row must survive a failed rebuild")
	}
	if d.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready after failure", d.Status)
	}
	if d.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed (no removal)", d.LastReconcileStatus)
	}

	// function_stats row still exists with identical counters and updated_at.
	after, ok := st.FunctionStats("flaky")
	if !ok {
		t.Fatal("flaky function_stats must survive a failed rebuild")
	}
	if after != before {
		t.Fatalf("function_stats changed by failed rebuild: before=%+v after=%+v", before, after)
	}
}

// TestReconcileStateInvalidTemplateDoesNotRemove is a regression that a broken
// template (unparseable) must not be mistaken for a removal: the function row
// and its function_stats survive untouched.
func TestReconcileStateInvalidTemplateDoesNotRemove(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, _, st := newStateReconciler(t, root, b, []*runner.PreparedFunction{fn})

	// Change content so a rebuild is attempted, then seed an active version and
	// per-function stats.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileFunction("guarded") // success -> ready row
	st.RecordFunctionStats(state.FunctionStats{Function: "guarded", EventsProcessedTotal: 9, HandlerSuccessTotal: 6})
	before, ok := st.FunctionStats("guarded")
	if !ok {
		t.Fatal("expected guarded stats before invalid template")
	}

	// Break the template; reconcile must retain the previous version, not remove.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("guarded")

	d, ok := st.GetFunction("guarded")
	if !ok {
		t.Fatal("guarded row must survive an invalid template")
	}
	if d.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready (retained)", d.Status)
	}

	after, ok := st.FunctionStats("guarded")
	if !ok {
		t.Fatal("guarded function_stats must survive an invalid template")
	}
	if after != before {
		t.Fatalf("function_stats changed by invalid template: before=%+v after=%+v", before, after)
	}
}

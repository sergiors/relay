//go:build integration

package reconciler

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// discovered -> the state DB records a ready row with handlers.
func TestReconcileStateDiscoverSuccess(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	writeFnDir(t, root, "brand-new")

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)

	r.reconcileApp("brand-new")

	if pf := reg.GetByName("brand-new"); pf == nil || pf.Prepared() == nil {
		t.Fatal("expected brand-new registered and prepared")
	}
	detail, ok := st.GetApp("brand-new")
	if !ok {
		t.Fatal("expected state row for brand-new")
	}
	if detail.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want success", detail.LastReconcileStatus)
	}
	if len(detail.Handlers) != 1 {
		t.Fatalf("handlers = %d, want 1", len(detail.Handlers))
	}
}

// Build failure retains the prior active state fields but records failed.
func TestReconcileStateFailureKeepsActiveAndMarksFailed(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// Change so a rebuild is attempted, then make it fail.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	// Seed an active version first (mirrors the startup success path).
	func() {
		b.fail = false
		r.reconcileApp("flaky") // success -> ready row
	}()
	before, _ := st.GetApp("flaky")

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileApp("flaky") // fails -> retained + failed

	detail, ok := st.GetApp("flaky")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.Status != state.StatusDegraded {
		t.Fatalf("status = %s, want degraded after failure with a prior image", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", detail.LastReconcileStatus)
	}
	if detail.Image != before.Image || detail.Fingerprint != before.Fingerprint {
		t.Fatalf("failed reconcile must preserve active image/fingerprint: got %q/%q want %q/%q",
			detail.Image, detail.Fingerprint, before.Image, before.Fingerprint)
	}
}

// Unchanged healthy app -> skipped periodic check persists NO reconcile
// outcome (RecordDiscovered seeds empty reconcile fields); the last meaningful
// reconcile (none yet) is untouched.
func TestReconcileStateUnchangedDoesNotRecordOutcome(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := initialFn("stable", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	r.reconcileApp("stable") // unchanged -> skip

	detail, ok := st.GetApp("stable")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.LastReconcileStatus != "" {
		t.Fatalf("last_reconcile_status = %s, want empty (unchanged check must not record an outcome)", detail.LastReconcileStatus)
	}
	if detail.LastReconcileAt != "" {
		t.Fatalf("last_reconcile_at = %s, want empty (unchanged check must not record a timestamp)", detail.LastReconcileAt)
	}
}

// Removal deletes the state row and handlers.
func TestReconcileStateRemoved(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "tobe-removed")

	fn := initialFn("tobe-removed", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileApp("tobe-removed")

	if _, ok := st.GetApp("tobe-removed"); ok {
		t.Fatal("state row should be removed")
	}
}

// An invalid template is a present-but-invalid desired definition: it records a
// failed desired view through state (unavailable, since no usable generation was
// ever prepared) and last_error, while never writing a success/ready outcome and
// never dropping the previously-loaded registry entry.
func TestReconcileStateInvalidTemplateRecordsFailureWithoutActiveGeneration(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("guarded")

	detail, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("row should exist from initial seeding")
	}
	if detail.Status != state.StatusUnavailable {
		t.Fatalf("status = %q, want unavailable (invalid desired, no usable generation)", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %q, want failed", detail.LastReconcileStatus)
	}
	if detail.LastError == "" {
		t.Fatal("last_error must record the invalid template")
	}
	// The invalid desired definition cannot be trusted: no stale desired
	// fingerprint is retained as if it were current.
	if detail.DesiredFingerprint != "" {
		t.Fatalf("desired_fingerprint = %q, want cleared for an invalid desired definition", detail.DesiredFingerprint)
	}
	// The runtime registry is untouched: an invalid template never removes or
	// replaces a previously-loaded version.
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("invalid template must not modify the live registry")
	}
}

// TestReconcileStateRemovalCleansAllTables is the live fsnotify-path regression
// for FULL cleanup on removal: reconciling an app whose directory was deleted
// must remove its apps row, app_stats, and handlers while leaving an
// unrelated app and the global stats row untouched.
func TestReconcileStateRemovalCleansAllTables(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	victimDir := writeFnDir(t, root, "victim")
	bystanderDir := writeFnDir(t, root, "bystander")

	victim := initialFn("victim", victimDir)
	bystander := initialFn("bystander", bystanderDir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{victim, bystander}, nil)

	// Simulate prior activity: per-app counters for both and a cumulative
	// global row.
	st.RecordAppStats(state.AppStats{App: "victim", EventsMatchedTotal: 5, HandlerSuccessTotal: 3})
	st.RecordAppStats(state.AppStats{App: "bystander", EventsMatchedTotal: 7, HandlerSuccessTotal: 4})
	wantGlobal := state.Stats{EventsMatchedTotal: 12, HandlerSuccessTotal: 7}
	st.RecordStats(wantGlobal)

	// Capture bystander's handler count before the removal for the untouched check.
	bystanderBefore, ok := st.GetApp("bystander")
	if !ok {
		t.Fatal("expected bystander row before removal")
	}

	// Remove the victim's directory and reconcile: the live removal path.
	if err := os.RemoveAll(victimDir); err != nil {
		t.Fatalf("removeall victim: %v", err)
	}
	r.reconcileApp("victim")

	// Victim's apps row and app_stats are gone.
	if _, ok := st.GetApp("victim"); ok {
		t.Fatal("victim functions row must be removed")
	}
	if _, ok := st.AppStats("victim"); ok {
		t.Fatal("victim app_stats must be removed")
	}

	// Handlers must not be resurrected: re-create the dir, reconcile, and the
	// detail must carry only the fresh template's handlers (no stale ones).
	writeFnDir(t, root, "victim")
	r.reconcileApp("victim")
	detail, ok := st.GetApp("victim")
	if !ok {
		t.Fatal("expected victim re-discovered after re-creating dir")
	}
	if len(detail.Handlers) != 1 {
		t.Fatalf("victim handlers = %d, want 1 (fresh template only)", len(detail.Handlers))
	}

	// Bystander untouched: row, app_stats, and handler count unchanged.
	if _, ok := st.GetApp("bystander"); !ok {
		t.Fatal("bystander functions row must survive")
	}
	bs, ok := st.AppStats("bystander")
	if !ok || bs.EventsMatchedTotal != 7 || bs.HandlerSuccessTotal != 4 {
		t.Fatalf("bystander app_stats = %+v, ok=%v; want events 7 success 4", bs, ok)
	}
	bystanderAfter, ok := st.GetApp("bystander")
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
	if gs.EventsMatchedTotal != wantGlobal.EventsMatchedTotal || gs.HandlerSuccessTotal != wantGlobal.HandlerSuccessTotal {
		t.Fatalf("global stats changed by removal: %+v, want %+v", gs, wantGlobal)
	}
}

// TestReconcileStateFailedBuildDoesNotRemoveStats is a regression that a failed
// rebuild (not a removal) never deletes state or stats: the app row stays
// ready, its app_stats row keeps its original counters and updated_at, and
// no removal is recorded.
func TestReconcileStateFailedBuildDoesNotRemoveStats(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// Change content so a rebuild is attempted, then seed an active version and
	// per-app stats, as production would after activity.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileApp("flaky") // success -> ready row
	st.RecordAppStats(state.AppStats{App: "flaky", EventsMatchedTotal: 5, HandlerSuccessTotal: 3})

	before, ok := st.AppStats("flaky")
	if !ok {
		t.Fatal("expected flaky stats before failed rebuild")
	}

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileApp("flaky") // fails -> retained + failed

	// App row still exists, degraded (prior image retained), marked failed
	// (not removed).
	detail, ok := st.GetApp("flaky")
	if !ok {
		t.Fatal("flaky row must survive a failed rebuild")
	}
	if detail.Status != state.StatusDegraded {
		t.Fatalf("status = %s, want degraded after failure with a prior image", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed (no removal)", detail.LastReconcileStatus)
	}

	// app_stats row still exists with identical counters and updated_at.
	after, ok := st.AppStats("flaky")
	if !ok {
		t.Fatal("flaky app_stats must survive a failed rebuild")
	}
	if after != before {
		t.Fatalf("app_stats changed by failed rebuild: before=%+v after=%+v", before, after)
	}
}

// TestReconcileStateInvalidTemplateDoesNotRemove is a regression that a broken
// template (unparseable) must not be mistaken for a removal: the app row
// and its app_stats survive untouched as ROWS, the prior active generation
// is retained (degraded), and the registry still serves the previous version.
func TestReconcileStateInvalidTemplateDoesNotRemove(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// Change content so a rebuild is attempted, then seed an active version and
	// per-app stats.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileApp("guarded") // success -> ready row
	st.RecordAppStats(state.AppStats{App: "guarded", EventsMatchedTotal: 9, HandlerSuccessTotal: 6})
	before, ok := st.AppStats("guarded")
	if !ok {
		t.Fatal("expected guarded stats before invalid template")
	}
	active, _ := st.GetApp("guarded")

	// Break the template; reconcile must retain the previous version, not remove.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("guarded")

	detail, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("guarded row must survive an invalid template")
	}
	if detail.Status != state.StatusDegraded {
		t.Fatalf("status = %s, want degraded (prior usable generation retained)", detail.Status)
	}
	if detail.Image != active.Image || detail.Fingerprint != active.Fingerprint {
		t.Fatalf("active generation = %q/%q, want preserved %q/%q",
			detail.Image, detail.Fingerprint, active.Image, active.Fingerprint)
	}
	// The runtime registry keeps serving the previous version.
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("invalid template must not remove or replace the live registry entry")
	}

	after, ok := st.AppStats("guarded")
	if !ok {
		t.Fatal("guarded app_stats must survive an invalid template")
	}
	if after != before {
		t.Fatalf("app_stats changed by invalid template: before=%+v after=%+v", before, after)
	}
}

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

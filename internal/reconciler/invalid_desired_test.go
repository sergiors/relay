package reconciler

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/source"
	"relay/internal/state"
)

// TestReconcileInvalidDesiredWritesFailureAndKeepsRegistry pins the live
// invalid-desired seam end to end: a function whose template becomes invalid is
// recorded as a failed desired state (degraded with its retained active
// generation) while the runtime registry keeps serving the previous version
// untouched and no rebuild is attempted. Removal is reserved for a genuinely
// missing directory.
func TestReconcileInvalidDesiredWritesFailureAndKeepsRegistry(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Seed a usable active generation (the registry already serves img-guarded;
	// the skip path deliberately records no outcome, so record it explicitly as
	// startup/reconcile would after a real success).
	st.RecordReconcileSuccess("guarded", "img-guarded", "fp-guarded", time.Now(), fn.Function())
	before, ok := st.GetFunction("guarded")
	if !ok || before.Status != state.StatusReady {
		t.Fatalf("precondition: %+v ok=%v, want ready", before, ok)
	}

	// Break the template: the desired definition is present but invalid.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("guarded")

	after, ok := st.GetFunction("guarded")
	if !ok {
		t.Fatal("guarded row must survive an invalid desired definition")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", after.Status)
	}
	if after.LastReconcileStatus != state.ReconcileFailed || after.LastError == "" {
		t.Fatalf("outcome = status=%q error=%q, want failed with an error", after.LastReconcileStatus, after.LastError)
	}
	if after.Image != before.Image || after.Fingerprint != before.Fingerprint {
		t.Fatalf("active generation = %q/%q, want preserved %q/%q",
			after.Image, after.Fingerprint, before.Image, before.Fingerprint)
	}
	if after.DesiredFingerprint != "" {
		t.Fatalf("desired_fingerprint = %q, want cleared for an invalid desired definition", after.DesiredFingerprint)
	}
	// The live registry is untouched and no rebuild was attempted: an invalid
	// template never replaces the previously-loaded version.
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("invalid template must not modify the live registry")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (an invalid template never builds)", b.prepares())
	}
}

// TestReconcileMissingTemplateRecordsInvalidButRetains pins the mid-copy case: a
// directory that exists but currently lacks template.yaml is a PRESENT but not
// yet loadable desired definition. It must not be treated as a removal (the
// registry entry survives), and its failed view is recorded with the retained
// active generation.
func TestReconcileMissingTemplateRecordsInvalidButRetains(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "midcopy")

	fn := initialFn("midcopy", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	st.RecordReconcileSuccess("midcopy", "img-midcopy", "fp-midcopy", time.Now(), fn.Function())
	if err := os.Remove(filepath.Join(dir, "template.yaml")); err != nil {
		t.Fatalf("remove template: %v", err)
	}
	r.reconcileFunction("midcopy")

	if pf := reg.GetByName("midcopy"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a missing template (mid-copy) must retain the loaded function")
	}
	after, ok := st.GetFunction("midcopy")
	if !ok {
		t.Fatal("midcopy row must survive")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (retained active generation)", after.Status)
	}
	if after.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %q, want failed", after.LastReconcileStatus)
	}
}

// TestReconcileFingerprintFailureRecordsInvalidAndKeepsRegistry pins the
// fingerprint-failure branch: when the injected identity resolver errors for a
// still-present desired definition, the reconciler records the invalid/failed
// view through state (retaining the active generation) without modifying the
// live registry or its fingerprint, and without treating the function as
// removed.
func TestReconcileFingerprintFailureRecordsInvalidAndKeepsRegistry(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "unreadable")

	fn := initialFn("unreadable", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, func(cfg *Config, _ *state.State) {
		cfg.Fingerprint = func(string, *function.Template) (*source.Selection, string, error) {
			return nil, "", errBoom
		}
	})

	// Seed an active generation so the retained view is observable.
	st.RecordReconcileSuccess("unreadable", "img-active", "fp-active", time.Now(), fn.Function())
	before, _ := st.GetFunction("unreadable")

	r.reconcileFunction("unreadable")

	if pf := reg.GetByName("unreadable"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a fingerprint failure must not drop the loaded function")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (a fingerprint failure never builds)", b.prepares())
	}
	after, ok := st.GetFunction("unreadable")
	if !ok {
		t.Fatal("row must survive a fingerprint failure")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", after.Status)
	}
	if after.Image != before.Image || after.Fingerprint != before.Fingerprint {
		t.Fatalf("active generation = %q/%q, want preserved %q/%q",
			after.Image, after.Fingerprint, before.Image, before.Fingerprint)
	}
	if after.LastReconcileStatus != state.ReconcileFailed || after.LastError == "" {
		t.Fatalf("outcome = %q/%q, want failed with an error", after.LastReconcileStatus, after.LastError)
	}
}

// TestReconcileGenuinelyMissingDirStillPrunes pins that the invalid-desired
// recording is NOT applied to a genuinely absent directory: removal semantics
// (registry drop + row delete) still win.
func TestReconcileGenuinelyMissingDirStillPrunes(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "vanished")

	fn := initialFn("vanished", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileFunction("vanished")

	if reg.GetByName("vanished") != nil {
		t.Fatal("a vanished directory must be removed from the registry")
	}
	if _, ok := st.GetFunction("vanished"); ok {
		t.Fatal("a vanished directory must have its state row pruned, not marked invalid")
	}
}

// TestReconcileInvalidThenRestoredIdenticalContentRecovers pins the recovery
// seam: an invalid desired definition forgets the reconciler's skip fingerprint,
// so when the definition is later restored byte-identically (which would
// otherwise compare equal and take the unchanged skip path that writes nothing),
// the next reconcile re-verifies it and a real success replaces the recorded
// failure. The active generation is not left falsely degraded.
func TestReconcileInvalidThenRestoredIdenticalContentRecovers(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "restored")

	fn := initialFn("restored", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	st.RecordReconcileSuccess("restored", "img-v1", "fp-v1", time.Now(), fn.Function())
	original, err := os.ReadFile(filepath.Join(dir, "template.yaml"))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}

	// Break, then restore the EXACT original content.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("restored")
	if got, _ := st.GetFunction("restored"); got.Status != state.StatusDegraded {
		t.Fatalf("precondition: status = %q, want degraded", got.Status)
	}

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), original, 0o644); err != nil {
		t.Fatalf("restore template: %v", err)
	}
	r.reconcileFunction("restored")

	got, ok := st.GetFunction("restored")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if got.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after an identical restore re-verifies", got.Status)
	}
	if got.LastReconcileStatus != state.ReconcileSuccess || got.LastError != "" {
		t.Fatalf("outcome = %q/%q, want success/cleared", got.LastReconcileStatus, got.LastError)
	}
}

// TestReconcileValidReplacementOverwritesInvalidGeneration pins that a later
// valid, successful reconcile fully replaces the invalid view: the active
// generation is overwritten, the desired fingerprint converges to the success,
// and the failure is cleared.
func TestReconcileValidReplacementOverwritesInvalidGeneration(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "recovers")

	fn := initialFn("recovers", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	st.RecordReconcileSuccess("recovers", "img-v1", "fp-v1", time.Now(), fn.Function())
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("recovers") // invalid -> degraded
	if got, _ := st.GetFunction("recovers"); got.Status != state.StatusDegraded {
		t.Fatalf("precondition: status = %q, want degraded (active generation retained)", got.Status)
	}

	// Fix the template and change source so a rebuild succeeds.
	writeFnDir(t, root, "recovers")
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction("recovers")

	got, ok := st.GetFunction("recovers")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if got.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after a valid successful replacement", got.Status)
	}
	if got.LastError != "" || got.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("outcome = error=%q status=%q, want cleared/success", got.LastError, got.LastReconcileStatus)
	}
	if got.DesiredFingerprint == "" || got.DesiredFingerprint != got.Fingerprint {
		t.Fatalf("desired/active = %q/%q, want converged non-empty", got.DesiredFingerprint, got.Fingerprint)
	}
}

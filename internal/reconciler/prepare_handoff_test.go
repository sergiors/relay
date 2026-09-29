package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime"
	"relay/internal/source"
)

// selectionSpyBuilder records the fingerprint and selection the reconciler
// hands to the selection-aware Prepare. It implements BOTH Builder and the
// optional selectionPreparer, so it stands in for the runtime Manager and lets a
// test prove the reconciler passes through the identity it already computed.
type selectionSpyBuilder struct {
	plainCalls     int
	selectionCalls int
	gotFingerprint string
	gotSelection   *source.Selection
	gotName        string
	// builtFingerprint, when non-empty, is returned as Prepared.Fingerprint
	// instead of echoing the supplied value. It stands in for the snapshot-derived
	// identity the real Manager returns, so a test can prove the reconciler
	// records and seeds the BUILT identity rather than the pre-build scan.
	builtFingerprint string
}

func (s *selectionSpyBuilder) Prepare(_ context.Context, fn function.Function) (*runtime.Prepared, error) {
	s.plainCalls++
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (s *selectionSpyBuilder) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn function.Function,
	fingerprint string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	s.selectionCalls++
	s.gotName = fn.Name
	s.gotFingerprint = fingerprint
	s.gotSelection = selection
	built := fingerprint
	if s.builtFingerprint != "" {
		built = s.builtFingerprint
	}
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name, Fingerprint: built}, nil
}

func (s *selectionSpyBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// TestReconcileHandsComputedFingerprintAndSelectionToBuilder is the live-rebuild
// handoff regression: the reconciler computes each function's fingerprint and
// resolves its source selection ONCE (SelectAndFingerprintFunction), then hands
// BOTH to the selection-aware builder so the runtime Manager never re-hashes the
// tree nor re-derives the policy. The supplied fingerprint equals an independent
// FingerprintFunction computation, and the selection is the one for the
// function directory — so the tag the reconciler compared and the selection the
// build stages are the same read.
func TestReconcileHandsComputedFingerprintAndSelectionToBuilder(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "handoff")

	b := &selectionSpyBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	r.reconcileFunction("handoff")

	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d, want 1", b.selectionCalls)
	}
	if b.plainCalls != 0 {
		t.Fatalf("plain Prepare calls = %d, want 0 when the builder supports selection", b.plainCalls)
	}
	wantFP, err := function.FingerprintFunction(dir, mustParse(template))
	if err != nil {
		t.Fatalf("reference fingerprint: %v", err)
	}
	if b.gotFingerprint != wantFP {
		t.Fatalf("supplied fingerprint = %q, want the computed %q", b.gotFingerprint, wantFP)
	}
	if b.gotSelection == nil {
		t.Fatal("a runtime-backed rebuild must supply the resolved selection")
	}
	if b.gotSelection.Dir() != dir {
		t.Fatalf("supplied selection dir = %q, want %q", b.gotSelection.Dir(), dir)
	}
	if b.gotName != "handoff" {
		t.Fatalf("supplied function = %q, want handoff", b.gotName)
	}
}

// TestReconcileResolvesIdentityOnceAndHandsExactValuesToBuilder is the
// no-duplicate-count regression with an INJECTED counter, not an inferred
// comment: the reconciler's identity resolver is called EXACTLY once for one
// reconcile, and the selection-aware builder receives the resolver's exact
// return values by identity — a distinct sentinel fingerprint and the precise
// *source.Selection pointer — never a rehash and never a second policy read. The
// spy's plain Prepare is never called.
func TestReconcileResolvesIdentityOnceAndHandsExactValuesToBuilder(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "counted")

	const sentinel = "injected-selection-aware-fingerprint"
	resolverCalls := 0
	var injected *source.Selection
	b := &selectionSpyBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, func(cfg *Config) {
		cfg.Fingerprint = func(dir string, _ *function.Template) (*source.Selection, string, error) {
			resolverCalls++
			sel, err := source.ForDir(dir)
			if err != nil {
				return nil, "", err
			}
			injected = sel
			return sel, sentinel, nil
		}
	})

	r.reconcileFunction("counted")

	if resolverCalls != 1 {
		t.Fatalf("identity resolver calls = %d, want exactly 1 for one reconcile", resolverCalls)
	}
	if b.plainCalls != 0 {
		t.Fatalf("plain Prepare calls = %d, want 0 when the builder supports selection", b.plainCalls)
	}
	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d, want 1", b.selectionCalls)
	}
	if b.gotFingerprint != sentinel {
		t.Fatalf("builder fingerprint = %q, want the resolver's exact %q (no rehash)", b.gotFingerprint, sentinel)
	}
	if b.gotSelection != injected {
		t.Fatalf("builder selection = %p, want the resolver's exact %p (no re-derivation)", b.gotSelection, injected)
	}
}

// TestReconcileRecordsBuiltIdentityNotPreBuildScan pins the live-reconcile half
// of the startup/live contract: when the builder returns the identity it actually
// built from (which can differ from the pre-build scan if the source mutated
// during the build), the reconciler records THAT identity in state and as its
// skip key. Persisting the stale pre-build scan instead would silently mark the
// mutated tree as current; recording the built value makes the next audit
// compare against what is really baked and rebuild the drift.
func TestReconcileRecordsBuiltIdentityNotPreBuildScan(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "drift")

	const builtIdentity = "snapshot-built-identity"
	b := &selectionSpyBuilder{builtFingerprint: builtIdentity}
	r, _, st := newTestStateReconciler(t, root, b, nil, nil)

	// Seed a prior successful row so the rebuild is observed as a desired change.
	st.RecordDiscovered(function.Function{Name: "drift", Dir: filepath.Join(root, "drift"), Template: mustParse(template)})

	r.reconcileFunction("drift")

	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d, want 1", b.selectionCalls)
	}
	if b.gotFingerprint == builtIdentity {
		t.Fatal("fixture precondition: the reconciler must supply the pre-build scan, not the built identity")
	}
	detail, ok := st.GetFunction("drift")
	if !ok {
		t.Fatal("expected a drift row")
	}
	if detail.Fingerprint != builtIdentity {
		t.Fatalf("persisted active fingerprint = %q, want the built identity %q (not the pre-build scan %q)",
			detail.Fingerprint, builtIdentity, b.gotFingerprint)
	}
	if detail.DesiredFingerprint != builtIdentity {
		t.Fatalf("desired fingerprint = %q, want the built identity %q", detail.DesiredFingerprint, builtIdentity)
	}

	// Because the live scan differs from the recorded built identity, a second
	// reconcile of the same tree rebuilds again: the drift is continuously
	// re-detected until the tree is restored or the image rebuilds from it. This
	// is the behavior that closes the TOCTOU window (an image is never treated as
	// current for bytes it did not bake).
	r.reconcileFunction("drift")
	if b.selectionCalls != 2 {
		t.Fatalf("selection-aware Prepare calls after the second reconcile = %d, want 2", b.selectionCalls)
	}
}

// TestReconcileFallsBackToPlainPrepareForTestBuilders pins the compatibility
// half: a Builder that does NOT implement selectionPreparer (every test fake)
// still goes through plain Prepare, unchanged.
func TestReconcileFallsBackToPlainPrepareForTestBuilders(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "fallback")

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	r.reconcileFunction("fallback")

	if b.prepares() != 1 {
		t.Fatalf("plain Prepare calls = %d, want 1 for a builder without the selection seam", b.prepares())
	}
}

// TestReconcileHandoffSelectionStagesSameTreeAsFingerprint is a coherence guard:
// the selection handed to the builder walks exactly the files the fingerprint
// hashed, so a file the fingerprint covers is present in the staged selection
// and an ignored file is absent.
func TestReconcileHandoffSelectionStagesSameTreeAsFingerprint(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "coherent")
	// An ignored file that must NOT be part of the selection.
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("*.log\n"), 0o644); err != nil {
		t.Fatalf("write gitignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "debug.log"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write ignored: %v", err)
	}

	b := &selectionSpyBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)
	r.reconcileFunction("coherent")

	if b.gotSelection == nil {
		t.Fatal("expected a resolved selection")
	}
	if !b.gotSelection.IncludesPath(filepath.Join(dir, "index.js"), false) {
		t.Fatal("selected source must include the hashed handler file")
	}
	if b.gotSelection.IncludesPath(filepath.Join(dir, "debug.log"), false) {
		t.Fatal("selected source must exclude a .gitignore-ignored file")
	}
	if b.gotSelection.Dir() != dir {
		t.Fatalf("selection dir = %q, want %q", b.gotSelection.Dir(), dir)
	}
}

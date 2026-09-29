package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
)

// diskIdentityBuilder is a Manager-like Builder: it derives the REAL content
// fingerprint and image reference from the on-disk tree at Prepare time and
// returns them as Prepared.Fingerprint/Image, exactly as runtime.Manager does
// from its one immutable source snapshot. It implements the optional
// selectionPreparer seam so the reconciler takes the same handoff path
// production uses. It lets a reconciler test observe genuinely distinct built
// generations across source edits without a Docker daemon.
type diskIdentityBuilder struct {
	selectionCalls int
	built          []string
}

func (b *diskIdentityBuilder) Prepare(_ context.Context, fn function.Function) (*runtime.Prepared, error) {
	return b.build(fn)
}

func (b *diskIdentityBuilder) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn function.Function,
	_ string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	b.selectionCalls++
	return b.build(fn)
}

func (b *diskIdentityBuilder) build(fn function.Function) (*runtime.Prepared, error) {
	fp, err := function.FingerprintFunction(fn.Dir, fn.Template)
	if err != nil {
		return nil, err
	}
	b.built = append(b.built, fp)
	return &runtime.Prepared{
		Name:        fn.Name,
		Image:       runtime.ImageRef(fn.Name, fp),
		Fingerprint: fp,
	}, nil
}

func (b *diskIdentityBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// TestReconcileRealBuiltGenerationsTrackSourceAndConverge exercises the
// startup/live shared contract end to end with real digests (not sentinels): a
// function is seeded with generation A, edited and rebuilt to a distinct
// generation B, re-reconciled unchanged (which must SKIP, proving the recorded
// built identity is the live content), then edited again and rebuilt to a third
// distinct generation C. Each rebuild records the identity the builder actually
// returned, tags the registry image with it, and retires the superseded image,
// so the recorded identity, the served image, and the source that was really
// baked stay in agreement across generations.
func TestReconcileRealBuiltGenerationsTrackSourceAndConverge(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "gen")
	fn := initialFn("gen", dir)

	genA, err := function.FingerprintFunction(dir, fn.Function().Template)
	if err != nil {
		t.Fatalf("fingerprint A: %v", err)
	}

	b := &diskIdentityBuilder{}
	var retired []string
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn},
		func(cfg *Config, _ *state.State) {
			cfg.Retire = func(_ string, oldImage string) { retired = append(retired, oldImage) }
		})

	// Generation A is unchanged from the seeded state: the reconcile must skip,
	// so no build is issued.
	r.reconcileFunction("gen")
	if b.selectionCalls != 0 {
		t.Fatalf("selection-aware Prepare calls = %d, want 0 for an unchanged generation A", b.selectionCalls)
	}

	// Edit -> distinct generation B.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('B'); }\n"), 0o644); err != nil {
		t.Fatalf("write B: %v", err)
	}
	genB, err := function.FingerprintFunction(dir, fn.Function().Template)
	if err != nil {
		t.Fatalf("fingerprint B: %v", err)
	}
	if genB == genA {
		t.Fatal("test setup: the edit must change the fingerprint")
	}

	r.reconcileFunction("gen")
	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d, want 1 after the edit to B", b.selectionCalls)
	}
	if b.built[0] != genB {
		t.Fatalf("builder built %q, want the live digest %q", b.built[0], genB)
	}
	detailB, ok := st.GetFunction("gen")
	if !ok {
		t.Fatal("expected a gen row")
	}
	if detailB.Fingerprint != genB {
		t.Fatalf("persisted active fingerprint = %q, want built B %q", detailB.Fingerprint, genB)
	}
	if detailB.DesiredFingerprint != genB {
		t.Fatalf("desired fingerprint = %q, want built B %q", detailB.DesiredFingerprint, genB)
	}
	if pf := reg.GetByName("gen"); pf == nil || pf.Prepared() == nil || pf.Prepared().Image != runtime.ImageRef("gen", genB) {
		t.Fatalf("registry must serve the B image, got %+v", pf)
	}

	// Re-reconcile with no further change: the recorded built identity equals the
	// live digest, so this must SKIP. A stale/mislabeled record would rebuild.
	r.reconcileFunction("gen")
	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d after B, want 1 (B must converge to a skip)", b.selectionCalls)
	}

	// Edit -> distinct generation C.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('C'); }\n"), 0o644); err != nil {
		t.Fatalf("write C: %v", err)
	}
	genC, err := function.FingerprintFunction(dir, fn.Function().Template)
	if err != nil {
		t.Fatalf("fingerprint C: %v", err)
	}
	if genC == genB || genC == genA {
		t.Fatalf("test setup: C must be distinct from A and B (A=%s B=%s C=%s)", genA, genB, genC)
	}

	r.reconcileFunction("gen")
	if b.selectionCalls != 2 {
		t.Fatalf("selection-aware Prepare calls = %d, want 2 after the edit to C", b.selectionCalls)
	}
	if b.built[1] != genC {
		t.Fatalf("builder built %q, want the live digest %q", b.built[1], genC)
	}
	detailC, ok := st.GetFunction("gen")
	if !ok {
		t.Fatal("expected a gen row after C")
	}
	if detailC.Fingerprint != genC {
		t.Fatalf("persisted active fingerprint = %q, want built C %q", detailC.Fingerprint, genC)
	}
	if pf := reg.GetByName("gen"); pf == nil || pf.Prepared() == nil || pf.Prepared().Image != runtime.ImageRef("gen", genC) {
		t.Fatalf("registry must serve the C image, got %+v", pf)
	}

	// The superseded B image was retired once C was published. A retire of the
	// initial hand-built image (from the seeded A entry) is expected first.
	if len(retired) != 2 || retired[len(retired)-1] != runtime.ImageRef("gen", genB) {
		t.Fatalf("retired images = %v, want the B image %q retired last", retired, runtime.ImageRef("gen", genB))
	}
}

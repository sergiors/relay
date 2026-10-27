//go:build integration

package reconciler

import (
	"context"
	"path/filepath"
	"testing"

	"relay/internal/app"
)

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
	if _, err := st.RecordDiscovered(context.Background(), app.App{Name: "drift", Dir: filepath.Join(root, "drift"), Template: mustParse(template)}); err != nil {
		t.Errorf("state write error: %v", err)
	}

	r.reconcileApp("drift")

	if b.selectionCalls != 1 {
		t.Fatalf("selection-aware Prepare calls = %d, want 1", b.selectionCalls)
	}
	if b.gotFingerprint == builtIdentity {
		t.Fatal("fixture precondition: the reconciler must supply the pre-build scan, not the built identity")
	}
	detail, ok := st.GetApp("drift")
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
	r.reconcileApp("drift")
	if b.selectionCalls != 2 {
		t.Fatalf("selection-aware Prepare calls after the second reconcile = %d, want 2", b.selectionCalls)
	}
}

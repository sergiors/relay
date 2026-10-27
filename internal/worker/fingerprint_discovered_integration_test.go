//go:build integration

package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestFingerprintDiscoveredReusedAcrossStatePhase is the state-phase regression
// for the worker's startup wiring: each loaded app is hashed exactly once
// (selectAndFingerprintApps), and that single value is persisted by both the
// fresh-database rebuild and the per-app discovery upsert as the DESIRED
// fingerprint (no usable active generation exists yet, so the active Fingerprint
// stays empty). The source is changed after the fingerprint is computed, so a
// state write that recomputed it would persist the changed digest; the
// caller-supplied value must win instead. The resolved selection is also carried
// on the record so Prepare can stage the exact policy the hash came from.
func TestFingerprintDiscoveredReusedAcrossStatePhase(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "demo", "def handler(e): return 1\n")

	loader := app.NewLoader(root, discardLogger())
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("loaded %d apps, want 1", len(fns))
	}

	startup := selectAndFingerprintApps(fns, discardLogger())
	if len(startup) != 1 {
		t.Fatalf("startup records = %d, want 1", len(startup))
	}
	computed := startup[0].Fingerprint
	if computed == "" {
		t.Fatal("selectAndFingerprintApps returned an empty fingerprint")
	}
	if startup[0].Selection == nil {
		t.Fatal("a runtime-backed function must carry its resolved selection")
	}
	discovered := discoveredFromStartup(startup)
	if len(discovered) != 1 {
		t.Fatalf("discovered %d apps, want 1", len(discovered))
	}

	// Mutate the source after the fingerprint was computed: any recompute now
	// yields a different digest.
	if err := os.WriteFile(filepath.Join(root, "demo", "main.py"), []byte("def handler(e): return 2\n"), 0o644); err != nil {
		t.Fatalf("rewrite source: %v", err)
	}
	changed, err := app.Fingerprint(filepath.Join(root, "demo"))
	if err != nil {
		t.Fatalf("recompute fingerprint: %v", err)
	}
	if changed == computed {
		t.Fatal("source edit did not change the fingerprint; test setup is broken")
	}

	// The state phase as Run wires it: seed an empty DB from the pair, then
	// upsert the discovery pair. Both must persist the pre-change fingerprint.
	st := testutil.NewState(t)

	if err := st.RebuildFromApps(context.Background(), discovered); err != nil {
		t.Fatalf("rebuild from apps: %v", err)
	}
	for _, d := range discovered {
		if _, err := st.RecordDiscoveredWithFingerprint(context.Background(), d.App, d.Fingerprint); err != nil {
			t.Errorf("state write error: %v", err)
		}
	}

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected demo row")
	}
	if detail.DesiredFingerprint != computed {
		t.Fatalf("persisted desired fingerprint = %q, want the once-computed %q (not recomputed %q)",
			detail.DesiredFingerprint, computed, changed)
	}
	if detail.Fingerprint != "" {
		t.Fatalf("active fingerprint = %q, want empty on a freshly discovered function", detail.Fingerprint)
	}
}

// TestStartupBuiltFingerprintIsReturnedIdentity pins the startup state contract:
// the fingerprint persisted after a successful prepare is the identity the image
// was ACTUALLY built from (prep.Fingerprint), NOT a post-build rescan of the
// live tree. A source edit landing after the build must not be recorded as the
// built generation (that would claim the image serves content it does not); the
// reconciler's audit detects the drift instead.
func TestStartupBuiltFingerprintIsReturnedIdentity(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "runtime-fn", "def handler(e): return 1\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	if len(startup) != 1 {
		t.Fatalf("startup records = %d, want 1", len(startup))
	}

	// The identity the preparer returns is a distinct sentinel, standing in for
	// the snapshot-derived fingerprint Prepare would return; the on-disk tree is
	// then mutated so any rescan would differ.
	const builtIdentity = "snapshot-built-identity"
	spy := &startupSpyPreparer{overrideFingerprint: builtIdentity}
	if err := os.WriteFile(filepath.Join(root, "runtime-fn", "main.py"), []byte("def handler(e): return 2\n"), 0o644); err != nil {
		t.Fatalf("rewrite source: %v", err)
	}
	onDisk, err := app.FingerprintApp(filepath.Join(root, "runtime-fn"), startup[0].App.Template)
	if err != nil {
		t.Fatalf("on-disk fingerprint: %v", err)
	}
	if onDisk == builtIdentity {
		t.Fatal("test setup: the on-disk digest must differ from the built identity")
	}

	st := testutil.NewState(t)
	fencingTokens := map[string]state.FencingToken{
		startup[0].App.Name: seedFencingToken(t, st, startup[0].App),
	}

	prepareApps(context.Background(), spy, startup, st, fencingTokens, discardLogger())

	detail, ok := st.GetApp("runtime-fn")
	if !ok {
		t.Fatal("expected a runtime-fn row")
	}
	if detail.Fingerprint != builtIdentity {
		t.Fatalf("persisted active fingerprint = %q, want the built identity %q (not a post-build rescan %q)",
			detail.Fingerprint, builtIdentity, onDisk)
	}
}

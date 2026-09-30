package worker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/state"
)

// TestPersistStartupDiscoveryIgnoresStagingDir pins the startup state phase
// against a Relay-owned staging directory (git's ".sync-*"): it is never
// recorded as a discovery or an invalid desired definition, and a stale reserved
// row left by a previous bad discovery is pruned. Valid neighbors and genuinely
// invalid user directories keep their normal behavior.
func TestPersistStartupDiscoveryIgnoresStagingDir(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	// A valid neighbor and a genuinely invalid user directory on disk.
	writeWorkerFunction(t, root, "good", "def handler(e): return 1\n")
	badDir := filepath.Join(root, "bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatalf("mkdir bad: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write bad template: %v", err)
	}
	// A staging directory with a valid-looking template: must be invisible.
	stageDir := filepath.Join(root, ".sync-abc")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}
	// A stale reserved row from a previous bad discovery.
	st.RecordReconcileSuccess(".sync-old", "img-stage", "fp-stage", time.Now(), stateFunction(".sync-old", filepath.Join(root, ".sync-old")))

	good := stateFunction("good", filepath.Join(root, "good"))
	discovered := []state.DiscoveredFunction{{Function: good, Fingerprint: "fp-good"}}
	// A reserved issue injected directly (defense in depth) must also be ignored.
	issues := []function.LoadIssue{
		{Name: "bad", Err: errors.New(`function "bad": template must contain at least one event rule or one service`)},
		{Name: ".sync-abc", Err: errors.New(`function ".sync-abc": read template: no such file`)},
	}

	if err := persistStartupDiscovery(st, root, discovered, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetFunction(".sync-abc"); ok {
		t.Fatal("a staging directory must never be persisted (not even invalid)")
	}
	if _, ok := st.GetFunction(".sync-old"); ok {
		t.Fatal("a stale reserved row must be pruned by the startup sweep")
	}
	if got, ok := st.GetFunction("good"); !ok || got.Status != state.StatusPreparing {
		t.Fatalf("good = %+v ok=%v, want preparing", got, ok)
	}
	if got, ok := st.GetFunction("bad"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad = %+v ok=%v, want unavailable (invalid user behavior preserved)", got, ok)
	}
}

// TestLoaderIgnoresStagingDirAtStartup pins the worker's startup loader path: a
// staging directory alongside a valid function yields exactly the valid function
// with no issue for the staging name.
func TestLoaderIgnoresStagingDirAtStartup(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "good", "def handler(e): return 1\n")
	if err := os.MkdirAll(filepath.Join(root, ".sync-xyz"), 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}

	loader := function.NewLoader(root, discardLogger())
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "good" {
		t.Fatalf("valid functions = %+v, want only good", fns)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %+v, want none (staging is not an invalid desired definition)", issues)
	}
}

// TestPersistStartupDiscoveryRecordsInvalidPresent pins the worker startup state
// phase: valid discovery persists as before, and each PRESENT-but-invalid desired
// definition from the loader diagnostics is persisted as an invalid view
// (unavailable with no generation, or degraded preserving a prior active
// generation) with an error. A fresh invalid function has no active generation.
func TestPersistStartupDiscoveryRecordsInvalidPresent(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	// Both functions (and their directories) are present on disk, so prune must
	// not remove them; only their desired definitions differ in validity, and a
	// fresh invalid name is present but has no prior row.
	for _, name := range []string{"prior", "now-invalid", "brand-new-bad"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
	}

	// A prior active generation for "prior" (valid on disk) and for "now-invalid"
	// (whose desired definition is about to be reported invalid).
	prior := stateFunction("prior", filepath.Join(root, "prior"))
	invalid := stateFunction("now-invalid", filepath.Join(root, "now-invalid"))
	st.RecordReconcileSuccess(prior.Name, "img-prior", "fp-prior", time.Now(), prior)
	st.RecordReconcileSuccess(invalid.Name, "img-invalid", "fp-invalid", time.Time{}.Add(time.Hour), invalid)

	discovered := []state.DiscoveredFunction{{Function: prior, Fingerprint: "fp-prior-next"}}
	issues := []function.LoadIssue{
		{Name: "now-invalid", Err: errors.New("function \"now-invalid\": template must contain at least one event rule or one service")},
		{Name: "brand-new-bad", Err: errors.New("function \"brand-new-bad\": read template: no such file")},
	}

	if err := persistStartupDiscovery(st, root, discovered, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	// The valid function is rediscovered as preparing with its active image
	// preserved.
	got, ok := st.GetFunction("prior")
	if !ok || got.Status != state.StatusPreparing {
		t.Fatalf("prior = %+v ok=%v, want preparing", got, ok)
	}
	if got.Image != "img-prior" {
		t.Fatalf("prior image = %q, want preserved img-prior", got.Image)
	}

	// The invalid function with a prior active generation is degraded and
	// preserves that generation.
	inv, ok := st.GetFunction("now-invalid")
	if !ok {
		t.Fatal("invalid present function must be persisted")
	}
	if inv.Status != state.StatusDegraded {
		t.Fatalf("now-invalid status = %q, want degraded", inv.Status)
	}
	if inv.Image != "img-invalid" || inv.Fingerprint != "fp-invalid" {
		t.Fatalf("now-invalid active generation = %q/%q, want preserved img-invalid/fp-invalid", inv.Image, inv.Fingerprint)
	}
	if inv.LastReconcileStatus != state.ReconcileFailed || inv.LastError == "" {
		t.Fatalf("now-invalid outcome = %q/%q, want failed with an error", inv.LastReconcileStatus, inv.LastError)
	}
	if inv.DesiredFingerprint != "" {
		t.Fatalf("now-invalid desired_fingerprint = %q, want cleared", inv.DesiredFingerprint)
	}

	// A fresh invalid function has no active generation and is unavailable.
	fresh, ok := st.GetFunction("brand-new-bad")
	if !ok {
		t.Fatal("fresh invalid present function must be persisted")
	}
	if fresh.Status != state.StatusUnavailable {
		t.Fatalf("brand-new-bad status = %q, want unavailable", fresh.Status)
	}
	if fresh.Image != "" || fresh.Fingerprint != "" || fresh.PreparedAt != "" {
		t.Fatalf("brand-new-bad must have no active generation: %q/%q/%q", fresh.Image, fresh.Fingerprint, fresh.PreparedAt)
	}
}

// TestPersistStartupDiscoveryInvalidAbsentFromRegistrySet pins that invalid
// desired definitions never enter the loaded/discovery set: only the valid
// discovered function is written as a discovery, and the invalid ones are not
// returned among the valid functions the loader produced.
func TestPersistStartupDiscoveryInvalidAbsentFromRegistrySet(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "good", "def handler(e): return 1\n")
	writeWorkerFunction(t, root, "bad", "def handler(e): return 1\n")
	// Break "bad"'s template so the loader reports it invalid while "good" loads.
	if err := os.WriteFile(filepath.Join(root, "bad", "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write bad template: %v", err)
	}

	loader := function.NewLoader(root, discardLogger())
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "good" {
		t.Fatalf("valid functions = %+v, want only good", fns)
	}
	if len(issues) != 1 || issues[0].Name != "bad" {
		t.Fatalf("issues = %+v, want one for bad", issues)
	}

	st := openTempState(t)
	startup := selectAndFingerprintFunctions(fns, discardLogger())
	discovered := discoveredFromStartup(startup)
	if err := persistStartupDiscovery(st, root, discovered, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	// Only "good" is in the discovered (registry) set; "bad" is state-only and
	// unavailable.
	for _, d := range discovered {
		if d.Function.Name == "bad" {
			t.Fatal("an invalid function must never enter the discovery/registry set")
		}
	}
	if got, ok := st.GetFunction("bad"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad = %+v ok=%v, want unavailable state row", got, ok)
	}
}

// TestPersistStartupDiscoveryInvalidImageRetainedInKeepSet pins the GC
// conservatism: an invalid PRESENT function's prior active image is still
// returned by ActiveImages and therefore retained by the startup image keep-set,
// even though the function is not part of the loaded set.
func TestPersistStartupDiscoveryInvalidImageRetainedInKeepSet(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	// The function is PRESENT on disk (just invalid), so prune keeps its row and
	// its recorded active image.
	if err := os.MkdirAll(filepath.Join(root, "now-invalid"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	invalid := stateFunction("now-invalid", filepath.Join(root, "now-invalid"))
	st.RecordReconcileSuccess(invalid.Name, "relay-fn-now-invalid:deadbeef", "fp", time.Time{}.Add(time.Hour), invalid)

	issues := []function.LoadIssue{{Name: "now-invalid", Err: errors.New("function \"now-invalid\": invalid template")}}
	if err := persistStartupDiscovery(st, root, nil, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	active := st.ActiveImages()
	keep := startupImageKeepSet(nil, nil, []string{active["now-invalid"]})
	if !keep["relay-fn-now-invalid:deadbeef"] {
		t.Fatalf("keep set = %v, want the invalid present function's prior image retained", keep)
	}
}

// TestPersistStartupDiscoveryPrunesRemovedNotInvalid pins the boundary between
// removal and invalidity: a row whose directory is genuinely absent is pruned by
// the same call, while an invalid present entry is preserved as invalid. A row
// with no on-disk directory and no loader issue is a removal.
func TestPersistStartupDiscoveryPrunesRemovedNotInvalid(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	removed := stateFunction("removed", filepath.Join(root, "removed"))
	present := stateFunction("present", filepath.Join(root, "present"))
	st.RecordReconcileSuccess(removed.Name, "img-removed", "fp", time.Now(), removed)
	st.RecordReconcileSuccess(present.Name, "img-present", "fp", time.Now(), present)

	// Create only the present directory on disk.
	if err := os.MkdirAll(filepath.Join(root, "present"), 0o755); err != nil {
		t.Fatalf("mkdir present: %v", err)
	}

	issues := []function.LoadIssue{{Name: "present", Err: errors.New("present invalid")}}
	if err := persistStartupDiscovery(st, root, nil, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetFunction("removed"); ok {
		t.Fatal("a vanished directory must be pruned, not kept")
	}
	got, ok := st.GetFunction("present")
	if !ok || got.Status != state.StatusDegraded {
		t.Fatalf("present = %+v ok=%v, want degraded (invalid present retained)", got, ok)
	}
}

// TestPersistStartupDiscoveryPruneDropsVanishedInvalid pins the ordering
// invariant: an invalid issue recorded for a directory that vanished before the
// prune is removed by the filesystem-authoritative prune rather than left as an
// invalid row. A disappearance is a removal, never an invalid desired state.
func TestPersistStartupDiscoveryPruneDropsVanishedInvalid(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	// The loader reported "vanished" as present-invalid, but by the time the
	// state phase runs its directory is gone (no directory is created here).
	issues := []function.LoadIssue{{Name: "vanished", Err: errors.New("transient")}}
	if err := persistStartupDiscovery(st, root, nil, issues, discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetFunction("vanished"); ok {
		t.Fatal("a vanished directory must not be resurrected as an invalid row")
	}
}

//go:build integration

package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/state"
)

// TestPersistStartupDiscoveryIgnoresStagingDir pins the startup state phase
// against a Relay-owned staging directory (git's ".sync-*"): it is never
// recorded as a discovery or an invalid desired definition, and a stale reserved
// row left by a previous bad discovery is pruned. Valid neighbors and genuinely
// invalid user directories keep their normal behavior.
// observeStartupIssues takes the pre-read state snapshot the startup parse gate
// would capture for every diagnosed app. An absent row is a valid observation
// (Found=false); a read error is fatal to the test.
func observeStartupIssues(
	t *testing.T,
	st *state.State,
	issues []app.LoadIssue,
) map[string]state.AppObservation {
	t.Helper()
	observed := make(map[string]state.AppObservation, len(issues))
	for _, issue := range issues {
		observation, err := st.ObserveApp(context.Background(), issue.Name)
		if err != nil {
			t.Fatalf("ObserveApp(%q): %v", issue.Name, err)
		}
		observed[issue.Name] = observation
	}
	return observed
}

func TestPersistStartupDiscoveryIgnoresStagingDir(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	// A valid neighbor and a genuinely invalid user directory on disk.
	writeWorkerApp(t, root, "good", "def handler(e): return 1\n")
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
	stage := stateApp(".sync-old", filepath.Join(root, ".sync-old"))
	stageFencingToken := seedFencingToken(t, st, stage)
	if _, err := st.RecordReconcileSuccess(context.Background(), ".sync-old", "img-stage", "fp-stage", time.Now(), stage, stageFencingToken); err != nil {
		t.Errorf("state write error: %v", err)
	}

	good := stateApp("good", filepath.Join(root, "good"))
	discovered := []state.DiscoveredApp{{App: good, Fingerprint: "fp-good"}}
	// A reserved issue injected directly (defense in depth) must also be ignored.
	issues := []app.LoadIssue{
		{Name: "bad", Err: errors.New(`function "bad": template must contain at least one event rule or one service`)},
		{Name: ".sync-abc", Err: errors.New(`function ".sync-abc": read template: no such file`)},
	}

	if _, err := persistStartupDiscovery(context.Background(), st, root, discovered, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetApp(".sync-abc"); ok {
		t.Fatal("a staging directory must never be persisted (not even invalid)")
	}
	if _, ok := st.GetApp(".sync-old"); ok {
		t.Fatal("a stale reserved row must be pruned by the startup sweep")
	}
	if got, ok := st.GetApp("good"); !ok || got.Status != state.StatusPreparing {
		t.Fatalf("good = %+v ok=%v, want preparing", got, ok)
	}
	if got, ok := st.GetApp("bad"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad = %+v ok=%v, want unavailable (invalid user behavior preserved)", got, ok)
	}
}

// TestPersistStartupDiscoveryTemplateLessDirLeavesNoState pins that a fresh
// directory without template.yaml is not an app candidate: the startup phase
// writes no apps row, no app_stats, and no transition fencing token for it.
func TestPersistStartupDiscoveryTemplateLessDirLeavesNoState(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	draftDir := filepath.Join(root, "draft")
	if err := os.MkdirAll(draftDir, 0o755); err != nil {
		t.Fatalf("mkdir draft: %v", err)
	}
	if err := os.WriteFile(filepath.Join(draftDir, "README.md"), []byte("notes"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}

	fencingTokens, err := persistStartupDiscovery(context.Background(), st, root, nil, nil, nil, discardLogger())
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if len(fencingTokens) != 0 {
		t.Fatalf("fencing tokens = %v, want none for a template-less directory", fencingTokens)
	}
	if _, ok := st.GetApp("draft"); ok {
		t.Fatal("a template-less directory must not create an apps row")
	}
	if _, ok := st.AppStats("draft"); ok {
		t.Fatal("a template-less directory must not create app_stats")
	}
}

// TestPersistStartupDiscoveryPrunesTemplateLessKnownApp pins the startup prune
// boundary: a persisted app whose directory still exists but no longer has
// template.yaml is removed from apps and app_stats, and its stale transition
// fencing token is never returned to the caller.
func TestPersistStartupDiscoveryPrunesTemplateLessKnownApp(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	appDir := filepath.Join(root, "draft")
	known := stateApp("draft", appDir)
	recordSuccess(t, st, known, "img-draft", "fp-draft", time.Now())
	st.RecordAppStats(state.AppStats{App: "draft", EventsMatchedTotal: 4})

	if err := os.MkdirAll(appDir, 0o755); err != nil {
		t.Fatalf("mkdir draft: %v", err)
	}

	fencingTokens, err := persistStartupDiscovery(context.Background(), st, root, nil, nil, nil, discardLogger())
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetApp("draft"); ok {
		t.Fatal("an app directory without template.yaml must be pruned at startup")
	}
	if _, ok := st.AppStats("draft"); ok {
		t.Fatal("a template-less app's app_stats must be pruned at startup")
	}
	if _, ok := fencingTokens["draft"]; ok {
		t.Fatal("a pruned app must not surface a transition fencing token")
	}
}

// TestPersistStartupDiscoveryRecordsInvalidPresent pins the worker startup state
// phase: valid discovery persists as before, and each PRESENT-but-invalid desired
// definition from the loader diagnostics is persisted as an invalid view
// (unavailable with no generation, or degraded preserving a prior active
// generation) with an error. A fresh invalid app has no active generation.
func TestPersistStartupDiscoveryRecordsInvalidPresent(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()

	// Both invalid apps (and their directories) are present on disk with a
	// malformed template.yaml, so prune must not remove them; only their desired
	// definitions are invalid, and a fresh invalid name has no prior row.
	writeWorkerApp(t, root, "prior", "def handler(e): return 1\n")
	for _, name := range []string{"now-invalid", "brand-new-bad"} {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
			t.Fatalf("write %s template: %v", name, err)
		}
	}

	// A prior active generation for "prior" (valid on disk) and for "now-invalid"
	// (whose desired definition is about to be reported invalid).
	prior := stateApp("prior", filepath.Join(root, "prior"))
	invalid := stateApp("now-invalid", filepath.Join(root, "now-invalid"))
	recordSuccess(t, st, prior, "img-prior", "fp-prior", time.Now())
	recordSuccess(t, st, invalid, "img-invalid", "fp-invalid", time.Time{}.Add(time.Hour))

	discovered := []state.DiscoveredApp{{App: prior, Fingerprint: "fp-prior-next"}}
	issues := []app.LoadIssue{
		{Name: "now-invalid", Err: errors.New(`function "now-invalid": unknown runtime "python9.9"`)},
		{Name: "brand-new-bad", Err: errors.New(`function "brand-new-bad": unknown runtime "python9.9"`)},
	}

	if _, err := persistStartupDiscovery(context.Background(), st, root, discovered, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	// The valid app is rediscovered as preparing with its active image
	// preserved.
	got, ok := st.GetApp("prior")
	if !ok || got.Status != state.StatusPreparing {
		t.Fatalf("prior = %+v ok=%v, want preparing", got, ok)
	}
	if got.Image != "img-prior" {
		t.Fatalf("prior image = %q, want preserved img-prior", got.Image)
	}

	// The invalid app with a prior active generation is degraded and
	// preserves that generation.
	inv, ok := st.GetApp("now-invalid")
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

	// A fresh invalid app has no active generation and is unavailable.
	fresh, ok := st.GetApp("brand-new-bad")
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
// discovered app is written as a discovery, and the invalid ones are not
// returned among the valid apps the loader produced.
func TestPersistStartupDiscoveryInvalidAbsentFromRegistrySet(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "good", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "bad", "def handler(e): return 1\n")
	// Break "bad"'s template so the loader reports it invalid while "good" loads.
	if err := os.WriteFile(filepath.Join(root, "bad", "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write bad template: %v", err)
	}

	loader := app.NewLoader(root, discardLogger())
	fns, issues, err := loader.LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "good" {
		t.Fatalf("valid apps = %+v, want only good", fns)
	}
	if len(issues) != 1 || issues[0].Name != "bad" {
		t.Fatalf("issues = %+v, want one for bad", issues)
	}

	st := openTempState(t)
	startup := selectAndFingerprintApps(fns, discardLogger())
	discovered := discoveredFromStartup(startup)
	if _, err := persistStartupDiscovery(context.Background(), st, root, discovered, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	// Only "good" is in the discovered (registry) set; "bad" is state-only and
	// unavailable.
	for _, d := range discovered {
		if d.App.Name == "bad" {
			t.Fatal("an invalid function must never enter the discovery/registry set")
		}
	}
	if got, ok := st.GetApp("bad"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad = %+v ok=%v, want unavailable state row", got, ok)
	}
}

// TestPersistStartupDiscoveryInvalidImageRetainedInKeepSet pins the GC
// conservatism: an invalid PRESENT app's prior active image is still
// returned by ActiveImages and therefore retained by the startup image keep-set,
// even though the app is not part of the loaded set.
func TestPersistStartupDiscoveryInvalidImageRetainedInKeepSet(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	// The app is PRESENT on disk with a malformed template, so prune keeps its
	// row and its recorded active image.
	invalidDir := filepath.Join(root, "now-invalid")
	if err := os.MkdirAll(invalidDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(invalidDir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	invalid := stateApp("now-invalid", invalidDir)
	recordSuccess(t, st, invalid, "relay-app-now-invalid:deadbeef", "fp", time.Time{}.Add(time.Hour))

	issues := []app.LoadIssue{{Name: "now-invalid", Err: errors.New("function \"now-invalid\": invalid template")}}
	if _, err := persistStartupDiscovery(context.Background(), st, root, nil, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	active := st.ActiveImages()
	keep := startupImageKeepSet(nil, nil, []string{active["now-invalid"]})
	if !keep["relay-app-now-invalid:deadbeef"] {
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
	removed := stateApp("removed", filepath.Join(root, "removed"))
	present := stateApp("present", filepath.Join(root, "present"))
	recordSuccess(t, st, removed, "img-removed", "fp", time.Now())
	recordSuccess(t, st, present, "img-present", "fp", time.Now())

	// Create only the present directory on disk, with a malformed template so it
	// remains a candidate (and therefore an invalid desired definition).
	if err := os.MkdirAll(filepath.Join(root, "present"), 0o755); err != nil {
		t.Fatalf("mkdir present: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "present", "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write present template: %v", err)
	}

	issues := []app.LoadIssue{{Name: "present", Err: errors.New("present invalid")}}
	if _, err := persistStartupDiscovery(context.Background(), st, root, nil, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetApp("removed"); ok {
		t.Fatal("a vanished directory must be pruned, not kept")
	}
	got, ok := st.GetApp("present")
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
	issues := []app.LoadIssue{{Name: "vanished", Err: errors.New("transient")}}
	if _, err := persistStartupDiscovery(context.Background(), st, root, nil, issues, observeStartupIssues(t, st, issues), discardLogger()); err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}

	if _, ok := st.GetApp("vanished"); ok {
		t.Fatal("a vanished directory must not be resurrected as an invalid row")
	}
}

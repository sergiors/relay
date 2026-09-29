package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// TestFingerprintDiscoveredReusedAcrossStatePhase is the state-phase regression
// for the worker's startup wiring: each loaded function is hashed exactly once
// (selectAndFingerprintFunctions), and that single value is persisted by both the
// fresh-database rebuild and the per-function discovery upsert as the DESIRED
// fingerprint (no usable active generation exists yet, so the active Fingerprint
// stays empty). The source is changed after the fingerprint is computed, so a
// state write that recomputed it would persist the changed digest; the
// caller-supplied value must win instead. The resolved selection is also carried
// on the record so Prepare can stage the exact policy the hash came from.
func TestFingerprintDiscoveredReusedAcrossStatePhase(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "demo", "def handler(e): return 1\n")

	loader := function.NewLoader(root, discardLogger())
	fns, err := loader.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 1 {
		t.Fatalf("loaded %d functions, want 1", len(fns))
	}

	startup := selectAndFingerprintFunctions(fns, discardLogger())
	if len(startup) != 1 {
		t.Fatalf("startup records = %d, want 1", len(startup))
	}
	computed := startup[0].Fingerprint
	if computed == "" {
		t.Fatal("selectAndFingerprintFunctions returned an empty fingerprint")
	}
	if startup[0].Selection == nil {
		t.Fatal("a runtime-backed function must carry its resolved selection")
	}
	discovered := discoveredFromStartup(startup)
	if len(discovered) != 1 {
		t.Fatalf("discovered %d functions, want 1", len(discovered))
	}

	// Mutate the source after the fingerprint was computed: any recompute now
	// yields a different digest.
	if err := os.WriteFile(filepath.Join(root, "demo", "main.py"), []byte("def handler(e): return 2\n"), 0o644); err != nil {
		t.Fatalf("rewrite source: %v", err)
	}
	changed, err := function.Fingerprint(filepath.Join(root, "demo"))
	if err != nil {
		t.Fatalf("recompute fingerprint: %v", err)
	}
	if changed == computed {
		t.Fatal("source edit did not change the fingerprint; test setup is broken")
	}

	// The state phase as Run wires it: seed an empty DB from the pair, then
	// upsert the discovery pair. Both must persist the pre-change fingerprint.
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("state open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.RebuildFromFunctions(discovered); err != nil {
		t.Fatalf("rebuild from functions: %v", err)
	}
	for _, d := range discovered {
		st.RecordDiscoveredWithFingerprint(d.Function, d.Fingerprint)
	}

	detail, ok := st.GetFunction("demo")
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

// TestFingerprintDiscoveredNamespacedByFunction pins that the startup
// fingerprints can be keyed by function name for the Prepare/reconciler seed
// without a further scan: each loaded function contributes exactly one entry,
// and the value is the same one the state phase persists.
func TestFingerprintDiscoveredNamespacedByFunction(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "alpha", "def handler(e): return 1\n")
	writeWorkerFunction(t, root, "beta", "def handler(e): return 2\n")

	fns, err := function.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintFunctions(fns, discardLogger())
	if len(startup) != 2 {
		t.Fatalf("startup records = %d, want 2", len(startup))
	}
	byName := make(map[string]string, len(startup))
	for _, s := range startup {
		byName[s.Function.Name] = s.Fingerprint
	}
	for _, name := range []string{"alpha", "beta"} {
		fp, ok := byName[name]
		if !ok || fp == "" {
			t.Fatalf("missing/empty fingerprint for %q: %v", name, byName)
		}
	}
	if byName["alpha"] == byName["beta"] {
		t.Fatal("distinct functions must not share a fingerprint")
	}
}

// TestDiscoveredFromStartupDropsSelection pins the package boundary: the state
// phase receives the narrow (function, fingerprint) pair and never sees the
// filesystem selection, which stays worker-local. The fingerprint is carried
// verbatim.
func TestDiscoveredFromStartupDropsSelection(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "demo", "def handler(e): return 1\n")
	fns, err := function.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintFunctions(fns, discardLogger())
	discovered := discoveredFromStartup(startup)
	if len(discovered) != 1 {
		t.Fatalf("discovered = %d, want 1", len(discovered))
	}
	if discovered[0].Function.Name != "demo" || discovered[0].Fingerprint != startup[0].Fingerprint {
		t.Fatalf("conversion lost identity: %+v vs %+v", discovered[0], startup[0])
	}
}

// TestSelectAndFingerprintFunctionsNoRuntimeYieldsNoSelection pins that a
// no-runtime function carries a nil selection (it builds no image) while a
// runtime-backed one carries the selection it hashed.
func TestSelectAndFingerprintFunctionsNoRuntimeYieldsNoSelection(t *testing.T) {
	root := t.TempDir()
	writeWorkerFunction(t, root, "runtime-fn", "def handler(e): return 1\n")
	writeWorkerFunction(t, root, "external-fn", "def handler(e): return 1\n")
	fns, err := function.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for i := range fns {
		if fns[i].Name == "external-fn" {
			fns[i].Template.Runtime = ""
			fns[i].Template.Events = nil
		}
	}
	startup := selectAndFingerprintFunctions(fns, discardLogger())
	byName := make(map[string]startupFunction, len(startup))
	for _, s := range startup {
		byName[s.Function.Name] = s
	}
	if ext := byName["external-fn"]; ext.Selection != nil {
		t.Fatalf("no-runtime function must carry no selection, got %v", ext.Selection)
	}
	if rt := byName["runtime-fn"]; rt.Selection == nil {
		t.Fatal("runtime-backed function must carry its resolved selection")
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
	writeWorkerFunction(t, root, "runtime-fn", "def handler(e): return 1\n")

	fns, err := function.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintFunctions(fns, discardLogger())
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
	onDisk, err := function.FingerprintFunction(filepath.Join(root, "runtime-fn"), startup[0].Function.Template)
	if err != nil {
		t.Fatalf("on-disk fingerprint: %v", err)
	}
	if onDisk == builtIdentity {
		t.Fatal("test setup: the on-disk digest must differ from the built identity")
	}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("state open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordDiscovered(startup[0].Function)

	prepareFunctions(context.Background(), spy, startup, st, discardLogger())

	detail, ok := st.GetFunction("runtime-fn")
	if !ok {
		t.Fatal("expected a runtime-fn row")
	}
	if detail.Fingerprint != builtIdentity {
		t.Fatalf("persisted active fingerprint = %q, want the built identity %q (not a post-build rescan %q)",
			detail.Fingerprint, builtIdentity, onDisk)
	}
}

// TestStartupSeedFingerprintsPrefersBuiltIdentity pins the seed-selection rule as
// a pure function: the identity the image was actually built from wins over the
// pre-prepare scan, and the scan is only the fallback when no built identity
// exists (unavailable/no-runtime/hand-built). This is the rule Run uses to seed
// the reconciler, factored out so it is asserted directly rather than inferred
// from Run's inline wiring.
func TestStartupSeedFingerprintsPrefersBuiltIdentity(t *testing.T) {
	builtFn := function.Function{Name: "built"}
	unavailFn := function.Function{Name: "unavailable"}
	noRuntimeFn := function.Function{Name: "no-runtime"}

	prepared := []*runner.PreparedFunction{
		// A successfully built function: its built identity is authoritative.
		runner.NewPrepared(builtFn, &runtime.Prepared{Name: "built", Image: "img-built", Fingerprint: "built-identity"}, nil),
		// An unavailable build: no prepared handle, so the scan stands.
		runner.NewUnavailable(unavailFn),
		// A no-runtime/external-image function: no image identity, scan stands.
		runner.NewPrepared(noRuntimeFn, &runtime.Prepared{Name: "no-runtime", Image: ""}, nil),
	}
	functions := []function.Function{builtFn, unavailFn, noRuntimeFn}
	fingerprints := map[string]string{
		"built":       "pre-prepare-scan",
		"unavailable": "scan-unavail",
		"no-runtime":  "scan-no-runtime",
	}

	seeds := startupSeedFingerprints(functions, fingerprints, prepared)

	if seeds["built"] != "built-identity" {
		t.Fatalf("built function seed = %q, want the built identity %q (not the pre-prepare scan)", seeds["built"], "built-identity")
	}
	if seeds["unavailable"] != "scan-unavail" {
		t.Fatalf("unavailable function seed = %q, want the scan fallback %q", seeds["unavailable"], "scan-unavail")
	}
	if seeds["no-runtime"] != "scan-no-runtime" {
		t.Fatalf("no-runtime function seed = %q, want the scan fallback %q", seeds["no-runtime"], "scan-no-runtime")
	}
	if len(seeds) != len(functions) {
		t.Fatalf("seed map size = %d, want one entry per function %d", len(seeds), len(functions))
	}
}

// writeWorkerFunction creates root/name/template.yaml and a source file so the
// loader and fingerprint have real inputs.
func writeWorkerFunction(t *testing.T, root, name, source string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	const tmpl = "runtime: python3.14\nevents:\n  - handler: main.handler\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.py"), []byte(source), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
}

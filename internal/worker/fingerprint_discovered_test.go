package worker

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// TestFingerprintDiscoveredNamespacedByApp pins that the startup
// fingerprints can be keyed by app name for the Prepare/reconciler seed
// without a further scan: each loaded app contributes exactly one entry,
// and the value is the same one the state phase persists.
func TestFingerprintDiscoveredNamespacedByApp(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "beta", "def handler(e): return 2\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	if len(startup) != 2 {
		t.Fatalf("startup records = %d, want 2", len(startup))
	}
	byName := make(map[string]string, len(startup))
	for _, s := range startup {
		byName[s.App.Name] = s.Fingerprint
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
// phase receives the narrow (app, fingerprint) pair and never sees the
// filesystem selection, which stays worker-local. The fingerprint is carried
// verbatim.
func TestDiscoveredFromStartupDropsSelection(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "demo", "def handler(e): return 1\n")
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	discovered := discoveredFromStartup(startup)
	if len(discovered) != 1 {
		t.Fatalf("discovered = %d, want 1", len(discovered))
	}
	if discovered[0].App.Name != "demo" || discovered[0].Fingerprint != startup[0].Fingerprint {
		t.Fatalf("conversion lost identity: %+v vs %+v", discovered[0], startup[0])
	}
}

// TestSelectAndFingerprintAppsNoRuntimeYieldsNoSelection pins that a
// no-runtime app carries a nil selection (it builds no image) while a
// runtime-backed one carries the selection it hashed.
func TestSelectAndFingerprintAppsNoRuntimeYieldsNoSelection(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "runtime-fn", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "external-fn", "def handler(e): return 1\n")
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for i := range fns {
		if fns[i].Name == "external-fn" {
			fns[i].Template.Runtime = ""
			fns[i].Template.Events = nil
		}
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	byName := make(map[string]startupApp, len(startup))
	for _, s := range startup {
		byName[s.App.Name] = s
	}
	if ext := byName["external-fn"]; ext.Selection != nil {
		t.Fatalf("no-runtime function must carry no selection, got %v", ext.Selection)
	}
	if rt := byName["runtime-fn"]; rt.Selection == nil {
		t.Fatal("runtime-backed function must carry its resolved selection")
	}
}

// TestStartupSeedFingerprintsPrefersBuiltIdentity pins the seed-selection rule as
// a pure function: the identity the image was actually built from wins over the
// pre-prepare scan, and the scan is only the fallback when no built identity
// exists (unavailable/no-runtime/hand-built). This is the rule Run uses to seed
// the reconciler, factored out so it is asserted directly rather than inferred
// from Run's inline wiring.
func TestStartupSeedFingerprintsPrefersBuiltIdentity(t *testing.T) {
	builtFn := app.App{Name: "built"}
	unavailFn := app.App{Name: "unavailable"}
	noRuntimeFn := app.App{Name: "no-runtime"}

	prepared := []*runner.PreparedApp{
		// A successfully built app: its built identity is authoritative.
		runner.NewPrepared(builtFn, &runtime.Prepared{Name: "built", Image: "img-built", Fingerprint: "built-identity"}, nil),
		// An unavailable build: no prepared handle, so the scan stands.
		runner.NewUnavailable(unavailFn),
		// A no-runtime/external-image app: no image identity, scan stands.
		runner.NewPrepared(noRuntimeFn, &runtime.Prepared{Name: "no-runtime", Image: ""}, nil),
	}
	functions := []app.App{builtFn, unavailFn, noRuntimeFn}
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

// writeWorkerApp creates root/name/template.yaml and a source file so the
// loader and fingerprint have real inputs.
func writeWorkerApp(t *testing.T, root, name, source string) {
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

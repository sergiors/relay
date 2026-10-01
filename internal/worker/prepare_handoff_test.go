package worker

import (
	"context"
	"testing"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/source"
)

// handoffCapture is one recorded selection-aware Prepare handoff.
type handoffCapture struct {
	fingerprint string
	selection   *source.Selection
}

// startupSpyPreparer implements the worker's narrow appPreparer seam (and
// runner.Executor) and records what prepareApps hands to the
// selection-aware Prepare, per app name. It stands in for the runtime
// Manager so the startup handoff is provable without Docker: the fingerprint and
// selection must be the ones the startup resolver produced, not a recomputation.
type startupSpyPreparer struct {
	calls     int
	callsByFn map[string]handoffCapture
	// overrideFingerprint, when non-empty, is returned as the Prepared.Fingerprint
	// instead of echoing the supplied value. It stands in for the snapshot-derived
	// identity the real Prepare returns, so a test can prove callers persist the
	// RETURNED identity rather than the value they supplied.
	overrideFingerprint string
}

func (s *startupSpyPreparer) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn app.App,
	fingerprint string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	s.calls++
	if s.callsByFn == nil {
		s.callsByFn = make(map[string]handoffCapture)
	}
	s.callsByFn[fn.Name] = handoffCapture{fingerprint: fingerprint, selection: selection}
	built := fingerprint
	if s.overrideFingerprint != "" {
		built = s.overrideFingerprint
	}
	return &runtime.Prepared{
		Name:        fn.Name,
		Image:       "img-" + fn.Name,
		Fingerprint: built,
	}, nil
}

func (s *startupSpyPreparer) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

// TestSelectAndFingerprintAppsResolvesOncePerApp is the injected
// counter proof for the startup identity pass: the resolver is called EXACTLY
// once per loaded app, and each returned startup record carries the
// resolver's exact fingerprint and selection pointer — the values later stages
// reuse rather than recompute. No comment inference: resolverCalls is asserted.
func TestSelectAndFingerprintAppsResolvesOncePerApp(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "beta", "def handler(e): return 2\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(fns) != 2 {
		t.Fatalf("loaded %d apps, want 2", len(fns))
	}

	resolverCalls := 0
	// wantByDir lets the resolver return a per-app sentinel and remember
	// the exact selection it handed back, so the test can compare by identity.
	type identity struct {
		fp  string
		sel *source.Selection
	}
	wantByDir := make(map[string]identity, len(fns))
	startup := selectAndFingerprintAppsWith(fns, discardLogger(),
		func(dir string, _ *app.Template) (*source.Selection, string, error) {
			resolverCalls++
			sel, err := source.ForDir(dir)
			if err != nil {
				return nil, "", err
			}
			fp := "sentinel:" + dir
			wantByDir[dir] = identity{fp: fp, sel: sel}
			return sel, fp, nil
		})

	if resolverCalls != len(fns) {
		t.Fatalf("resolver calls = %d, want exactly %d (one per app)", resolverCalls, len(fns))
	}
	if len(startup) != len(fns) {
		t.Fatalf("startup records = %d, want %d", len(startup), len(fns))
	}
	for _, s := range startup {
		w, ok := wantByDir[s.App.Dir]
		if !ok {
			t.Fatalf("startup record for unrequested dir %q", s.App.Dir)
		}
		if s.Fingerprint != w.fp {
			t.Fatalf("%s fingerprint = %q, want the resolver's %q", s.App.Name, s.Fingerprint, w.fp)
		}
		if s.Selection != w.sel {
			t.Fatalf("%s selection = %p, want the resolver's exact %p", s.App.Name, s.Selection, w.sel)
		}
	}

	// The narrow state/seed conversion carries the SAME fingerprints verbatim:
	// no later stage may recompute an identity the startup pass already resolved.
	discovered := discoveredFromStartup(startup)
	if len(discovered) != len(startup) {
		t.Fatalf("discovered = %d, want %d", len(discovered), len(startup))
	}
	for i, d := range discovered {
		if d.Fingerprint != startup[i].Fingerprint {
			t.Fatalf("discovered[%d] fingerprint = %q, want startup %q", i, d.Fingerprint, startup[i].Fingerprint)
		}
	}
}

// TestPrepareAppsHandsStartupIdentityToPrepare is the startup handoff proof
// for prepareApps: each startup record's resolved fingerprint and selection
// are passed to the selection-aware Prepare VERBATIM (fingerprint string equal,
// selection pointer identical), exactly one Prepare call per record, and the
// returned runner handles are available with the spy's image. This pins that
// startup does not re-scan the tree nor re-derive the selection policy.
func TestPrepareAppsHandsStartupIdentityToPrepare(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "beta", "def handler(e): return 2\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	startup := selectAndFingerprintApps(fns, discardLogger())
	if len(startup) != len(fns) {
		t.Fatalf("startup records = %d, want %d", len(startup), len(fns))
	}
	startupByName := make(map[string]startupApp, len(startup))
	for _, s := range startup {
		if s.Selection == nil {
			t.Fatalf("%s must carry a resolved selection", s.App.Name)
		}
		startupByName[s.App.Name] = s
	}

	spy := &startupSpyPreparer{}
	prepared := prepareApps(context.Background(), spy, startup, nil, discardLogger())

	if spy.calls != len(startup) {
		t.Fatalf("selection-aware Prepare calls = %d, want exactly %d (one per startup record)", spy.calls, len(startup))
	}
	if len(spy.callsByFn) != len(startup) {
		t.Fatalf("captured %d handoffs, want %d", len(spy.callsByFn), len(startup))
	}
	for name, got := range spy.callsByFn {
		want := startupByName[name]
		if got.fingerprint != want.Fingerprint {
			t.Fatalf("%s Prepare fingerprint = %q, want startup %q (no rehash)", name, got.fingerprint, want.Fingerprint)
		}
		if got.selection != want.Selection {
			t.Fatalf("%s Prepare selection = %p, want startup's exact %p (no re-derivation)", name, got.selection, want.Selection)
		}
	}

	// The returned handles are available and name the image the spy produced.
	if len(prepared) != len(startup) {
		t.Fatalf("prepared = %d, want %d", len(prepared), len(startup))
	}
	for _, pf := range prepared {
		if pf.Prepared() == nil {
			t.Fatalf("%s should be available", pf.Name())
		}
		if want := "img-" + pf.Name(); pf.Prepared().Image != want {
			t.Fatalf("%s image = %q, want %q", pf.Name(), pf.Prepared().Image, want)
		}
	}
}

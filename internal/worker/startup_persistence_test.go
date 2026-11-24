package worker

import (
	"context"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/state"
)

// TestPrepareAppsWithoutStateSinkNeedsNoToken preserves the historical test/
// standalone shape: without a state sink there is no persistence to fence, so a
// nil token map is not a reason to suppress preparation.
func TestPrepareAppsWithoutStateSinkNeedsNoToken(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "solo", "def handler(e, context): return 1\n")
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())

	spy := &startupSpyPreparer{}
	prepared := prepareApps(context.Background(), spy, startup, nil, nil, discardLogger())
	if spy.calls != 1 || len(prepared) != 1 {
		t.Fatalf("calls/prepared = %d/%d, want 1/1 without a state sink", spy.calls, len(prepared))
	}
}

// TestPrepareAppsMissingFencingTokenKeepsUnavailableCandidate pins the
// missing-token handoff at unit level: an app whose discovery write produced
// no token is never prepared, but it KEEPS its slot as an unavailable
// candidate in startup order — a nil slot would silently drop a matched app
// and let its deliveries be ACKed unmatched. Publishing then installs that
// placeholder in the registry with its desired rules intact, so a matching
// delivery classifies matched-but-unavailable at the runner boundary.
func TestPrepareAppsMissingFencingTokenKeepsUnavailableCandidate(t *testing.T) {
	// given two loaded apps where only alpha has fencing authority
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e, context): return 1\n")
	writeWorkerApp(t, root, "beta", "def handler(e, context): return 2\n")
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	if len(startup) != 2 {
		t.Fatalf("startup records = %d, want 2", len(startup))
	}
	tokens := map[string]state.FencingToken{"alpha": 7} // beta deliberately absent

	// a non-nil state sink is what activates the fail-closed token gate; the
	// zero value is safe because nothing on this path writes state
	spy := &startupSpyPreparer{}
	prepared := prepareApps(context.Background(), spy, startup, &state.State{},
		tokens, discardLogger())

	// then only alpha was prepared, while beta keeps an unavailable slot in
	// startup order — no nil-slot omission
	if spy.calls != 1 {
		t.Fatalf("Prepare calls = %d, want only alpha (beta has no token)",
			spy.calls)
	}
	if _, ok := spy.callsByFn["beta"]; ok {
		t.Fatal("beta must never reach Prepare without a fencing token")
	}
	if len(prepared) != len(startup) {
		t.Fatalf("prepared = %d entries, want every startup app to keep a slot",
			len(prepared))
	}
	byName := make(map[string]*runner.PreparedApp, len(prepared))
	for i, pf := range prepared {
		if pf == nil {
			t.Fatalf("prepared[%d] = nil; a missing slot would drop the matched "+
				"app", i)
		}
		if pf.Name() != startup[i].App.Name {
			t.Fatalf("prepared[%d] = %q, want startup order %q", i, pf.Name(),
				startup[i].App.Name)
		}
		byName[pf.Name()] = pf
	}
	if alpha := byName["alpha"]; alpha == nil || alpha.Prepared() == nil {
		t.Fatal("alpha (which has a token) should be prepared")
	}
	beta := byName["beta"]
	if beta == nil {
		t.Fatal("beta has no slot; a missing fencing token must keep it " +
			"matched-but-unavailable")
	}
	if beta.Prepared() != nil {
		t.Fatalf("beta candidate = %+v, want unavailable", beta.Prepared())
	}
	if beta.App().Template == nil || len(beta.App().Template.Events) == 0 {
		t.Fatal("beta's unavailable candidate lost its desired event rules; " +
			"matching would drop it")
	}

	// and at the registry boundary beta stays a matched-but-unavailable
	// candidate while alpha publishes normally
	registry := &runner.Registry{}
	published, retry := publishStartupApps(context.Background(), prepared,
		registry, &startupPublicationFake{}, &startupPublicationStateFake{},
		startupAppSet{fencingTokens: tokens}, discardLogger())

	if len(published) != 2 {
		t.Fatalf("published view = %d entries, want both candidates", len(published))
	}
	if built := registry.GetByName("alpha"); built == nil ||
		built.Prepared() == nil {
		t.Fatalf("alpha registry entry = %+v, want the prepared generation", built)
	}
	betaPlaceholder := requireUnavailableRegistryCandidate(t, registry, "beta")
	requireMatchedNotUnmatched(t, betaPlaceholder)
	if len(retry) != 0 {
		t.Fatalf("retry names = %v, want none (nothing was rejected)", retry)
	}
}

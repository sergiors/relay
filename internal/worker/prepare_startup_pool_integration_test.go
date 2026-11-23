//go:build integration

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// acceptStartupPublisher stands in for a selection-current runtime manager: it
// exercises the same registry callback and post-publication refresh path while
// leaving the PostgreSQL fencing-token writes real.
type acceptStartupPublisher struct{}

func (acceptStartupPublisher) PublishPrepared(_ context.Context, _ *runtime.Prepared, publish func() error) error {
	return publish()
}

func (acceptStartupPublisher) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	return nil, nil
}

func (acceptStartupPublisher) RuntimeImageSelectionIdentity(string) string { return "" }

// TestPrepareAppsStartupUsesExactPerAppFencingToken pins the per-app fencing
// contract at the state boundary: each app's guarded write carries that app's
// OWN discovery token. A token superseded by a later desired write (another
// worker bumped the generation) must NOT publish anything — no success write —
// while the app carrying a valid token still publishes, so one stale app never
// blocks its neighbor's convergence.
func TestPrepareAppsStartupUsesExactPerAppFencingToken(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "fence-alpha", "def handler(e, context): return 1\n")
	writeWorkerApp(t, root, "fence-beta", "def handler(e, context): return 2\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	byName := make(map[string]startupApp, len(startup))
	for _, s := range startup {
		byName[s.App.Name] = s
	}

	st := testutil.NewState(t)
	alphaToken := seedFencingToken(t, st, byName["fence-alpha"].App)
	betaToken := seedFencingToken(t, st, byName["fence-beta"].App)
	if alphaToken == betaToken {
		t.Fatalf("alpha and beta discovered tokens are equal (%d); tokens must be per-app", alphaToken)
	}

	// Supersede alpha's desired generation AFTER its token was captured, so the
	// value in the fencing-token map is stale while beta's stays exact.
	const newerDesiredFingerprint = "newer-alpha-desired"
	newerAlpha, err := st.RecordDiscoveredWithFingerprint(
		context.Background(), byName["fence-alpha"].App, newerDesiredFingerprint)
	if err != nil {
		t.Fatalf("supersede alpha: %v", err)
	}
	if newerAlpha <= alphaToken {
		t.Fatalf("alpha token did not advance: %d -> %d", alphaToken, newerAlpha)
	}

	parks := map[string]*poolPark{
		"fence-alpha": newPoolPark(),
		"fence-beta":  newPoolPark(),
	}
	spy := newBlockingPreparer(2, parks)

	tokens := map[string]state.FencingToken{
		"fence-alpha": alphaToken, // now stale
		"fence-beta":  betaToken,  // exact and valid
	}
	done := make(chan []*runner.PreparedApp, 1)
	go func() { done <- prepareApps(context.Background(), spy, startup, st, tokens, discardLogger()) }()

	waitArrivedIn(t, parks, "fence-alpha", "fence-beta")
	parks["fence-alpha"].free()
	parks["fence-beta"].free()
	candidates := collectPrepared(t, done, []string{"fence-alpha", "fence-beta"}, nil)
	for _, name := range []string{"fence-alpha", "fence-beta"} {
		detail, ok := st.GetApp(name)
		if !ok || detail.Fingerprint != "" || detail.Status == state.StatusReady {
			t.Fatalf("%s was marked successful before startup publication: %+v (found=%v)", name, detail, ok)
		}
	}
	registry := &runner.Registry{}
	published, retry := publishStartupApps(
		context.Background(), candidates, registry, acceptStartupPublisher{}, st,
		startupAppSet{fencingTokens: tokens}, discardLogger(),
	)
	if len(published) != 2 || len(retry) != 0 {
		t.Fatalf("published/retry = %d/%v, want two candidates accepted and no retries", len(published), retry)
	}
	for _, pf := range candidates {
		if registry.GetByName(pf.Name()) != pf {
			t.Fatalf("accepted %s candidate was not published into the registry", pf.Name())
		}
	}

	for _, name := range []string{"fence-alpha", "fence-beta"} {
		detail, ok := st.GetApp(name)
		if !ok {
			t.Fatalf("expected a %s row after publication", name)
		}
		if detail.Fingerprint != "" || detail.Status == state.StatusReady ||
			detail.LastReconcileStatus == state.ReconcileSuccess {
			t.Fatalf("%s startup publication recorded success before service completion: %+v", name, detail)
		}
	}

	docker := newCtxRecordDocker()
	serviceReconciler := reconciler.NewServiceReconciler(
		docker, nil, discardLogger(), reconcileTimeout,
	)
	coordinator, stop := startCoordinatorFixture(t, serviceReconciler)
	defer stop()
	enqueueStartupServices(
		context.Background(), published, coordinator, st, tokens, discardLogger(),
	)
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait for startup service completion: %v", err)
	}

	alpha, ok := st.GetApp("fence-alpha")
	if !ok {
		t.Fatal("expected a fence-alpha row after service completion")
	}
	if alpha.DesiredFingerprint != newerDesiredFingerprint {
		t.Fatalf("stale alpha callback changed desired fingerprint to %q, want newer identity %q",
			alpha.DesiredFingerprint, newerDesiredFingerprint)
	}
	if alpha.Fingerprint != "" {
		t.Fatalf("stale alpha callback published active fingerprint %q; it must affect no row", alpha.Fingerprint)
	}
	if alpha.Status != state.StatusPreparing {
		t.Fatalf("stale alpha callback changed status to %q, want the newer discovered row unchanged", alpha.Status)
	}
	if alpha.LastReconcileStatus != "" {
		t.Fatalf("stale alpha callback changed reconcile outcome to %q, want unchanged empty outcome",
			alpha.LastReconcileStatus)
	}

	beta, ok := st.GetApp("fence-beta")
	if !ok {
		t.Fatal("expected a fence-beta row")
	}
	if beta.Fingerprint != "built-fence-beta" {
		t.Fatalf("beta active fingerprint = %q, want the built identity %q (published with its exact token)",
			beta.Fingerprint, "built-fence-beta")
	}
	if beta.Status != state.StatusReady {
		t.Fatalf("beta status = %s, want ready", beta.Status)
	}
}

// TestPrepareAppsStartupStaleFencingTokenSuppressesFailureWrite adds the
// failure half: a preparation that fails while carrying a stale token must not
// record a failure either — a superseded preparation never overwrites a
// generation it already lost.
func TestPrepareAppsStartupStaleFencingTokenSuppressesFailureWrite(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "fence-gamma", "def handler(e, context): return 3\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())

	st := testutil.NewState(t)
	gammaToken := seedFencingToken(t, st, startup[0].App)

	// Supersede gamma's desired generation so the captured token goes stale.
	if _, err := st.RecordDiscovered(context.Background(), startup[0].App); err != nil {
		t.Fatalf("supersede gamma: %v", err)
	}

	spy := newBlockingPreparer(1, nil)
	spy.setFail("fence-gamma", errors.New("injected stale-token failure"))
	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startup, st,
			map[string]state.FencingToken{"fence-gamma": gammaToken}, discardLogger())
	}()

	unavailable := map[string]bool{"fence-gamma": true}
	collectPrepared(t, done, []string{"fence-gamma"}, unavailable)

	detail, ok := st.GetApp("fence-gamma")
	if !ok {
		t.Fatal("expected a fence-gamma row (the later desired write created it)")
	}
	if detail.LastError != "" {
		t.Fatalf("stale failure published last_error %q; it must affect no row", detail.LastError)
	}
	if detail.LastReconcileStatus == state.ReconcileFailed {
		t.Fatalf("stale failure rewrote last_reconcile_status to %q; it must be rejected", detail.LastReconcileStatus)
	}
	if detail.Status == state.StatusUnavailable || detail.Status == state.StatusDegraded {
		t.Fatalf("stale failure published status %s; the owning generation must stay untouched", detail.Status)
	}
}

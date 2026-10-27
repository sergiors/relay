//go:build integration

package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestPersistStartupDiscoveryCriticalDiscoveryFailure pins the fencing-authority
// boundary: a valid app whose discovery write fails has no safe token, so the
// phase returns an error even though another app's token landed. A partial token
// map is never treated as success.
func TestPersistStartupDiscoveryCriticalDiscoveryFailure(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	root := t.TempDir()
	writeWorkerApp(t, root, "good", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "failing", "def handler(e): return 2\n")

	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load apps: %v", err)
	}
	startup := selectAndFingerprintApps(fns, discardLogger())
	discovered := discoveredFromStartup(startup)
	// Put the healthy app first so a token lands before the injected failure;
	// the returned error must still prevent startup from using the partial map.
	for i := range discovered {
		if discovered[i].App.Name == "good" {
			discovered[0], discovered[i] = discovered[i], discovered[0]
			break
		}
	}

	failAppWrites(t, dsn, "failing")
	tokens, err := persistStartupDiscovery(
		context.Background(), st, root, discovered, nil, nil, discardLogger())
	if err == nil {
		t.Fatal("a valid app's discovery failure must be fatal, not silently skipped")
	}
	if tokens["good"] <= 0 {
		t.Fatalf("good token = %d, want the earlier write to have landed", tokens["good"])
	}
	if _, ok := tokens["failing"]; ok {
		t.Fatal("a failed discovery write must not surface a fencing token")
	}
}

// TestPersistStartupDiscoveryInvalidWriteIsIsolated pins that an invalid app's
// operational write failure is recoverable and does not hide other invalid
// observations: the remaining issues are recorded, and startup does not abort.
func TestPersistStartupDiscoveryInvalidWriteIsIsolated(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	root := t.TempDir()
	issues := []app.LoadIssue{
		invalidIssue(t, root, "bad-write"),
		invalidIssue(t, root, "good-write"),
	}
	observed := observeStartupIssues(t, st, issues)

	failAppWrites(t, dsn, "bad-write")
	tokens, err := persistStartupDiscovery(
		context.Background(), st, root, nil, issues, observed, discardLogger())
	if err != nil {
		t.Fatalf("invalid-app write failure must not be fatal: %v", err)
	}
	if _, ok := tokens["bad-write"]; ok {
		t.Fatal("a failed invalid write must not surface a fencing token")
	}
	if tokens["good-write"] <= 0 {
		t.Fatalf("good-write token = %d, want it recorded despite the neighbor failure", tokens["good-write"])
	}
	if _, ok := st.GetApp("bad-write"); ok {
		t.Fatal("the injected failure must not create the bad-write row")
	}
	if got, ok := st.GetApp("good-write"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("good-write = %+v ok=%v, want unavailable", got, ok)
	}
}

// TestPersistStartupDiscoveryMissingObservationFailsClosed pins that a failed
// state observation is not an absent row: the issue write is skipped instead of
// bypassing the optimistic concurrency guard.
func TestPersistStartupDiscoveryMissingObservationFailsClosed(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	issues := []app.LoadIssue{invalidIssue(t, root, "broken")}

	tokens, err := persistStartupDiscovery(
		context.Background(), st, root, nil, issues,
		map[string]state.AppObservation{}, discardLogger())
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}
	if _, ok := tokens["broken"]; ok {
		t.Fatal("a missing observation must not produce a fencing token")
	}
	if _, ok := st.GetApp("broken"); ok {
		t.Fatal("a missing observation must not write a row")
	}
}

// TestPersistStartupDiscoveryStaleObservationIsIsolated pins the cross-worker
// startup case: a valid correction from another worker invalidates this
// worker's pre-read observation for one app, while a second invalid observation
// is still recorded normally. Stale rejection is recoverable, not an error.
func TestPersistStartupDiscoveryStaleObservationIsIsolated(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	older := testutil.OpenState(t, dsn)
	newer := testutil.OpenState(t, dsn)
	root := t.TempDir()

	staleIssue := invalidIssue(t, root, "stale")
	freshIssue := invalidIssue(t, root, "fresh")
	seedStartupInvalid(t, older, "stale")
	staleObserved, err := older.ObserveApp(context.Background(), "stale")
	if err != nil {
		t.Fatalf("observe stale: %v", err)
	}
	freshObserved, err := older.ObserveApp(context.Background(), "fresh")
	if err != nil {
		t.Fatalf("observe fresh: %v", err)
	}
	observed := map[string]state.AppObservation{
		"stale": staleObserved,
		"fresh": freshObserved,
	}

	// Another worker's valid correction lands after the observations above.
	if _, err := newer.RecordDiscoveredWithFingerprint(context.Background(),
		stateApp("stale", filepath.Join(root, "stale")), "fp-corrected"); err != nil {
		t.Fatalf("newer valid correction: %v", err)
	}

	tokens, err := persistStartupDiscovery(context.Background(), older, root, nil,
		[]app.LoadIssue{staleIssue, freshIssue}, observed, discardLogger())
	if err != nil {
		t.Fatalf("stale rejection must be recoverable: %v", err)
	}
	if _, ok := tokens["stale"]; ok {
		t.Fatal("a stale observation must not surface a fencing token")
	}
	if tokens["fresh"] <= 0 {
		t.Fatalf("fresh token = %d, want the independent issue recorded", tokens["fresh"])
	}
	corrected, ok := older.GetApp("stale")
	if !ok {
		t.Fatal("stale observation must not remove the newer row")
	}
	if corrected.InvalidSource != "" || corrected.Status != state.StatusPreparing {
		t.Fatalf("stale observation overwrote the newer correction: %+v", corrected)
	}
	if got, ok := older.GetApp("fresh"); !ok || got.InvalidSource != freshIssue.Identity {
		t.Fatalf("fresh row = %+v ok=%v, want its invalid identity recorded", got, ok)
	}
}

// TestPersistStartupDiscoveryRebuildFailureKeepsObservationsConsistent pins that
// a failed rebuild does not invalidate the pre-read observations: it writes
// nothing, the independent invalid issue is still recorded, and the later
// discovery failure is propagated as critical.
func TestPersistStartupDiscoveryRebuildFailureKeepsObservationsConsistent(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	st := testutil.OpenState(t, dsn)
	root := t.TempDir()
	writeWorkerApp(t, root, "seed-fail", "def handler(e): return 1\n")
	broken := invalidIssue(t, root, "broken")

	fn, err := app.LoadSingle(root, "seed-fail")
	if err != nil {
		t.Fatalf("load seed-fail: %v", err)
	}
	discovered := []state.DiscoveredApp{{App: fn, Fingerprint: "fp-seed"}}
	observed := observeStartupIssues(t, st, []app.LoadIssue{broken})
	if observed["broken"].Found {
		t.Fatal("precondition: broken must not exist")
	}

	failAppWrites(t, dsn, "seed-fail")
	tokens, err := persistStartupDiscovery(context.Background(), st, root,
		discovered, []app.LoadIssue{broken}, observed, discardLogger())
	if err == nil {
		t.Fatal("the discovery failure after a failed rebuild must be critical")
	}
	if tokens["broken"] <= 0 {
		t.Fatalf("broken token = %d, want the independent invalid write recorded", tokens["broken"])
	}
	if got, ok := st.GetApp("broken"); !ok || got.InvalidSource != broken.Identity {
		t.Fatalf("broken row = %+v ok=%v, want its invalid identity", got, ok)
	}
	if _, ok := st.GetApp("seed-fail"); ok {
		t.Fatal("a failed rebuild/discovery must not create the valid app's row")
	}
}

// TestPrepareAppsFailsClosedWithoutFencingToken pins the startup handoff: an app
// without the exact token from its discovery write is not prepared at all, while
// an app that has one is prepared normally.
func TestPrepareAppsFailsClosedWithoutFencingToken(t *testing.T) {
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e): return 1\n")
	writeWorkerApp(t, root, "beta", "def handler(e): return 2\n")

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
	tokens := map[string]state.FencingToken{
		"alpha": seedFencingToken(t, st, byName["alpha"].App),
		// beta is deliberately absent/zero: no fencing authority.
	}

	spy := &startupSpyPreparer{}
	prepared := prepareApps(context.Background(), spy, startup, st, tokens, discardLogger())
	if spy.calls != 1 {
		t.Fatalf("Prepare calls = %d, want only alpha (beta has no token)", spy.calls)
	}
	if _, ok := spy.callsByFn["beta"]; ok {
		t.Fatal("beta must never reach Prepare without a fencing token")
	}
	if len(prepared) != 1 || prepared[0].Name() != "alpha" {
		t.Fatalf("prepared = %+v, want only alpha", prepared)
	}
}

// TestStartupServicePublicationFailsClosedWithoutToken pins the second
// publication boundary: a prepared app cannot enqueue service convergence
// without the exact fencing token returned by its discovery write.
func TestStartupServicePublicationFailsClosedWithoutToken(t *testing.T) {
	st := testutil.NewState(t)
	fn := app.App{Name: "alpha", Template: &app.Template{}}
	prepared := []*runner.PreparedApp{
		runner.NewPrepared(fn, &runtime.Prepared{Image: "img-alpha"}, nil),
	}

	// A nil coordinator makes any accidental publication fail the test. The
	// missing token must be detected before the coordinator is touched.
	enqueueStartupServicesWithState(
		context.Background(), prepared, nil, st, nil, discardLogger())
}

// failAppWrites installs a schema-local trigger that rejects INSERT/UPDATE of
// exactly one app name. It lets a test fail one app's state write while every
// other app still succeeds, proving per-app isolation without faking the whole
// database handle.
func failAppWrites(t *testing.T, dsn, name string) {
	t.Helper()
	db := newRawStateConn(t, dsn)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION relay_test_fail_app_write() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.name = TG_ARGV[0] THEN
				RAISE EXCEPTION 'injected app write failure for %', NEW.name;
			END IF;
			RETURN NEW;
		END;
		$$`); err != nil {
		t.Fatalf("create write-failure function: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		DROP TRIGGER IF EXISTS relay_test_fail_app_write ON apps;
		CREATE TRIGGER relay_test_fail_app_write
		BEFORE INSERT OR UPDATE ON apps
		FOR EACH ROW EXECUTE FUNCTION relay_test_fail_app_write('`+name+`')`); err != nil {
		t.Fatalf("create write-failure trigger: %v", err)
	}
}

func invalidIssue(t *testing.T, root, name string) app.LoadIssue {
	t.Helper()
	writeStartupTemplate(t, root, name, startupInvalidTemplate)
	return app.LoadIssue{
		Name:     name,
		Err:      errors.New(`app "` + name + `": unknown runtime "python9.9"`),
		Identity: app.TemplateIdentity([]byte(startupInvalidTemplate)),
	}
}

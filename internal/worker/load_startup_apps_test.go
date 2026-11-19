package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// startupDiscoveryWrite records one RecordDiscoveredWithSourceGeneration call.
type startupDiscoveryWrite struct {
	fn               app.App
	fingerprint      string
	sourceGeneration int64
}

// startupInvalidWrite records one RecordInvalidDesiredWithSource call.
// hasObservation distinguishes a passed observation from a nil pointer.
type startupInvalidWrite struct {
	name           string
	identity       string
	observed       state.AppObservation
	hasObservation bool
}

// fakeStartupState is the startupState seam: it records the discovery phase's
// state writes and serves a fixed stats snapshot so loadStartupApps runs
// without PostgreSQL. Discovered apps get a positive token unless a failure is
// injected, mirroring the state package's happy path.
type fakeStartupState struct {
	snapshot state.OwnStatsSnapshot

	rebuildErr  error
	pruneErr    error
	discoverErr error
	invalidErr  error

	observeCalls []string
	rebuilds     []state.DiscoveredApp
	pruneDirs    []string
	discoveries  []startupDiscoveryWrite
	invalids     []startupInvalidWrite
}

func (f *fakeStartupState) OwnStatsSnapshot(
	context.Context,
) (state.OwnStatsSnapshot, error) {
	return f.snapshot, nil
}

func (f *fakeStartupState) ObserveApp(
	_ context.Context, name string,
) (state.AppObservation, error) {
	f.observeCalls = append(f.observeCalls, name)
	// A first startup observes absent rows; the zero observation is valid.
	return state.AppObservation{}, nil
}

func (f *fakeStartupState) RebuildFromApps(
	_ context.Context, discovered []state.DiscoveredApp,
) error {
	f.rebuilds = append(f.rebuilds, discovered...)
	return f.rebuildErr
}

func (f *fakeStartupState) PruneRemoved(_ context.Context, dir string) error {
	f.pruneDirs = append(f.pruneDirs, dir)
	return f.pruneErr
}

func (f *fakeStartupState) RecordDiscoveredWithSourceGeneration(
	_ context.Context, fn app.App, fingerprint string, sourceGeneration int64,
) (state.FencingToken, error) {
	f.discoveries = append(f.discoveries, startupDiscoveryWrite{
		fn:               fn,
		fingerprint:      fingerprint,
		sourceGeneration: sourceGeneration,
	})
	if f.discoverErr != nil {
		return 0, f.discoverErr
	}
	return 7, nil
}

func (f *fakeStartupState) RecordInvalidDesiredWithSource(
	_ context.Context, name, sourceIdentity string, _ error,
	observed *state.AppObservation,
) (state.InvalidDesiredResult, error) {
	write := startupInvalidWrite{name: name, identity: sourceIdentity}
	if observed != nil {
		write.observed = *observed
		write.hasObservation = true
	}
	f.invalids = append(f.invalids, write)
	if f.invalidErr != nil {
		return state.InvalidDesiredResult{}, f.invalidErr
	}
	return state.InvalidDesiredResult{FencingToken: 42, Wrote: true}, nil
}

// writeBrokenWorkerApp creates root/name with an unparseable template so the
// loader diagnoses a present-but-invalid definition.
func writeBrokenWorkerApp(t *testing.T, root, name, template string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(template), 0o644); err != nil {
		t.Fatalf("write %s template: %v", name, err)
	}
}

func loadedAppNames(set startupAppSet) []string {
	names := make([]string, 0, len(set.loaded))
	for _, fn := range set.loaded {
		names = append(names, fn.Name)
	}
	return names
}

// TestLoadStartupAppsStandaloneDiscoversRestoresAndRecordsInvalid pins the
// standalone startup phase over the startupState seam: only the valid app
// loads, the present-but-invalid definition is recorded with its pre-read
// observation, the filesystem prune runs, and the atomic stats snapshot both
// seeds the registry and supplies the returned reset generations.
func TestLoadStartupAppsStandaloneDiscoversRestoresAndRecordsInvalid(t *testing.T) {
	// given one valid app and one present-but-invalid definition
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e, context): return 1\n")
	const brokenTemplate = "runtime: python9.9\n"
	writeBrokenWorkerApp(t, root, "broken", brokenTemplate)

	snapshot := state.OwnStatsSnapshot{
		Stats: state.Stats{
			EventsReceivedTotal: 5,
			HandlerSuccessTotal: 2,
		},
		AppStats:    []state.AppStats{{App: "alpha", EventsMatchedTotal: 4}},
		Generations: state.StatsGenerations{Global: 11, Node: 13},
	}
	store := &fakeStartupState{snapshot: snapshot}
	m := metrics.New()

	// when the startup app phase runs in standalone mode
	set, err := loadStartupApps(
		context.Background(), discardLogger(), store, root, m,
		0, nil, nil, false,
	)
	if err != nil {
		t.Fatalf("loadStartupApps: %v", err)
	}

	// then only the valid app is loaded, with the invalid one kept as an issue
	if names := loadedAppNames(set); len(names) != 1 || names[0] != "alpha" {
		t.Fatalf("loaded apps = %v, want only alpha", names)
	}
	if len(set.issues) != 1 || set.issues[0].Name != "broken" {
		t.Fatalf("issues = %+v, want one for broken", set.issues)
	}
	if len(set.fingerprints) != 1 || set.fingerprints["alpha"] == "" {
		t.Fatalf("fingerprints = %v, want alpha with a non-empty value",
			set.fingerprints)
	}

	// and the state phase saw exactly the alpha discovery pair, stamped with
	// the standalone zero source generation
	if len(store.rebuilds) != 1 || store.rebuilds[0].App.Name != "alpha" ||
		store.rebuilds[0].Fingerprint == "" {
		t.Fatalf("rebuild pairs = %+v, want alpha with its fingerprint",
			store.rebuilds)
	}
	if len(store.discoveries) != 1 {
		t.Fatalf("discovery writes = %d, want 1", len(store.discoveries))
	}
	disc := store.discoveries[0]
	if disc.fn.Name != "alpha" ||
		disc.fingerprint != store.rebuilds[0].Fingerprint {
		t.Fatalf("discovery write = %+v, want alpha with the rebuilt fingerprint",
			disc)
	}
	if disc.sourceGeneration != 0 {
		t.Fatalf("standalone source generation = %d, want 0",
			disc.sourceGeneration)
	}
	if len(store.pruneDirs) != 1 || store.pruneDirs[0] != root {
		t.Fatalf("prune dirs = %v, want the app root %q", store.pruneDirs, root)
	}

	// and the invalid definition was recorded with its pre-read observation
	if len(store.invalids) != 1 {
		t.Fatalf("invalid writes = %d, want 1", len(store.invalids))
	}
	inv := store.invalids[0]
	if inv.name != "broken" {
		t.Fatalf("invalid write app = %q, want broken", inv.name)
	}
	if want := app.TemplateIdentity([]byte(brokenTemplate)); inv.identity != want {
		t.Fatalf("invalid identity = %q, want %q", inv.identity, want)
	}
	if !inv.hasObservation {
		t.Fatal("invalid write must carry the pre-read observation")
	}
	if inv.observed.Found {
		t.Fatalf("observation = %+v, want an absent row on first startup",
			inv.observed)
	}
	observed := make(map[string]bool, len(store.observeCalls))
	for _, name := range store.observeCalls {
		observed[name] = true
	}
	if !observed["alpha"] || !observed["broken"] {
		t.Fatalf("parse-gate observations = %v, want alpha and broken",
			store.observeCalls)
	}

	// and the stats snapshot seeded the registry and returned its generations
	if set.statsRestore.Generations !=
		(state.StatsGenerations{Global: 11, Node: 13}) {
		t.Fatalf("restored generations = %+v, want (11, 13)",
			set.statsRestore.Generations)
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 5 {
		t.Fatalf("seeded events received = %d, want 5", got)
	}
	if got := m.Counter(metrics.MetricHandlerSuccess); got != 2 {
		t.Fatalf("seeded handler success = %d, want 2", got)
	}
	if got := snapshotAppStats(m, nil); len(got) != 1 ||
		got[0].App != "alpha" || got[0].EventsMatchedTotal != 4 {
		t.Fatalf("seeded app stats = %+v, want alpha at 4", got)
	}

	// and both writes surfaced their fencing authority
	if set.fencingTokens["alpha"] != 7 {
		t.Fatalf("alpha token = %d, want the discovery token 7",
			set.fencingTokens["alpha"])
	}
	if set.fencingTokens["broken"] != 42 {
		t.Fatalf("broken token = %d, want the invalid-write token 42",
			set.fencingTokens["broken"])
	}
}

// TestLoadStartupAppsAuthoritativeKeepsOnlyMaterializedApps pins the
// coordinated startup contract: only apps whose fingerprint matches the
// authorized generation load and persist, the source generation stamps every
// discovery write, and only materialized invalid definitions are recorded —
// local drift (additions, edits, and local parse failures) is ignored.
func TestLoadStartupAppsAuthoritativeKeepsOnlyMaterializedApps(t *testing.T) {
	// given a root with one materialized app, a local addition, an edited app,
	// and two present-but-invalid definitions (one materialized, one drift)
	root := t.TempDir()
	writeWorkerApp(t, root, "materialized",
		"def handler(e, context): return 1\n")
	writeWorkerApp(t, root, "local-add", "def handler(e, context): return 2\n")
	writeWorkerApp(t, root, "edited", "def handler(e, context): return 3\n")
	const brokenTemplate = "runtime: python9.9\n"
	writeBrokenWorkerApp(t, root, "mat-bad", brokenTemplate)
	writeBrokenWorkerApp(t, root, "drift-bad", brokenTemplate)

	// expected fingerprints come from the same real loader inputs the phase
	// uses, so the authorized map holds the true materialized identity
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	fingerprints := map[string]string{}
	for _, record := range selectAndFingerprintApps(fns, discardLogger()) {
		fingerprints[record.App.Name] = record.Fingerprint
	}
	if fingerprints["materialized"] == "" {
		t.Fatal("precondition: materialized fingerprint must not be empty")
	}
	if fingerprints["edited"] == fingerprints["materialized"] {
		t.Fatal("precondition: edited must differ from materialized")
	}
	authorized := map[string]string{
		"materialized": fingerprints["materialized"],
		// A local edit that no longer matches any materialized identity.
		"edited": "fp-of-the-previous-revision",
	}
	materializedInvalid := map[string]string{"mat-bad": ""}
	store := &fakeStartupState{snapshot: state.OwnStatsSnapshot{
		Generations: state.StatsGenerations{Global: 2, Node: 4},
	}}
	m := metrics.New()

	// when the startup app phase runs coordinator-authoritative
	set, err := loadStartupApps(
		context.Background(), discardLogger(), store, root, m,
		42, authorized, materializedInvalid, true,
	)
	if err != nil {
		t.Fatalf("loadStartupApps: %v", err)
	}

	// then only the fingerprint-matching app loads, with the materialized
	// invalid kept and both drift candidates dropped
	if names := loadedAppNames(set); len(names) != 1 ||
		names[0] != "materialized" {
		t.Fatalf("loaded apps = %v, want only materialized", names)
	}
	if len(set.issues) != 1 || set.issues[0].Name != "mat-bad" {
		t.Fatalf("issues = %+v, want only the materialized mat-bad", set.issues)
	}
	if len(set.records) != 1 || len(set.fingerprints) != 1 {
		t.Fatalf("startup records/fingerprints = %d/%d, want 1/1",
			len(set.records), len(set.fingerprints))
	}

	// and the single discovery write carries the authorized fingerprint under
	// the coordinated source generation
	if len(store.rebuilds) != 1 || store.rebuilds[0].App.Name != "materialized" {
		t.Fatalf("rebuild pairs = %+v, want only materialized", store.rebuilds)
	}
	if len(store.discoveries) != 1 {
		t.Fatalf("discovery writes = %d, want 1 (local drift never persists)",
			len(store.discoveries))
	}
	disc := store.discoveries[0]
	if disc.fn.Name != "materialized" ||
		disc.fingerprint != authorized["materialized"] {
		t.Fatalf("discovery write = %+v, want materialized with the authorized "+
			"fingerprint", disc)
	}
	if disc.sourceGeneration != 42 {
		t.Fatalf("source generation = %d, want the coordinated 42",
			disc.sourceGeneration)
	}

	// and only the materialized invalid was recorded
	if len(store.invalids) != 1 || store.invalids[0].name != "mat-bad" {
		t.Fatalf("invalid writes = %+v, want only mat-bad", store.invalids)
	}

	// while the drift candidates carry no fencing authority at all
	if set.fencingTokens["materialized"] != 7 {
		t.Fatalf("materialized token = %d, want 7",
			set.fencingTokens["materialized"])
	}
	if set.fencingTokens["mat-bad"] != 42 {
		t.Fatalf("mat-bad token = %d, want 42", set.fencingTokens["mat-bad"])
	}
	for _, name := range []string{"local-add", "edited", "drift-bad"} {
		if _, ok := set.fencingTokens[name]; ok {
			t.Fatalf("%s must not surface a token in authoritative mode", name)
		}
	}

	// and the stats snapshot generations still reach the caller
	if set.statsRestore.Generations !=
		(state.StatsGenerations{Global: 2, Node: 4}) {
		t.Fatalf("restored generations = %+v, want (2, 4)",
			set.statsRestore.Generations)
	}
}

// TestLoadStartupAppsDiscoveryFailureIsCritical pins that a valid app's failed
// discovery write aborts startup: without the persisted fencing token the app
// must never be prepared, so the phase fails closed.
func TestLoadStartupAppsDiscoveryFailureIsCritical(t *testing.T) {
	// given a valid app whose discovery write will fail
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e, context): return 1\n")
	injected := errors.New("injected discovery failure")
	store := &fakeStartupState{discoverErr: injected}

	// when the startup app phase runs
	_, err := loadStartupApps(
		context.Background(), discardLogger(), store, root, metrics.New(),
		0, nil, nil, false,
	)

	// then the phase fails closed on the discovery write
	if err == nil {
		t.Fatal("a failed discovery write must abort startup, err = nil")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("error = %v, want it to wrap the injected failure", err)
	}
	if !strings.Contains(err.Error(), "record discovered") {
		t.Fatalf("error = %v, want the critical discovery context", err)
	}
}

// TestLoadStartupAppsIsolatesInvalidWriteFailure pins that an invalid app's
// failed state write never blocks startup: the phase still succeeds, the valid
// app keeps its token, and the failed invalid app gets none.
func TestLoadStartupAppsIsolatesInvalidWriteFailure(t *testing.T) {
	// given a valid app and an invalid definition whose write will fail
	root := t.TempDir()
	writeWorkerApp(t, root, "alpha", "def handler(e, context): return 1\n")
	writeBrokenWorkerApp(t, root, "broken", "runtime: python9.9\n")
	store := &fakeStartupState{invalidErr: errors.New("invalid write refused")}

	// when the startup app phase runs
	set, err := loadStartupApps(
		context.Background(), discardLogger(), store, root, metrics.New(),
		0, nil, nil, false,
	)

	// then startup succeeds, the valid app keeps its authority, and the
	// failed invalid write surfaces no token
	if err != nil {
		t.Fatalf("an invalid write failure must not be fatal: %v", err)
	}
	if set.fencingTokens["alpha"] != 7 {
		t.Fatalf("alpha token = %d, want 7 despite the neighbor failure",
			set.fencingTokens["alpha"])
	}
	if _, ok := set.fencingTokens["broken"]; ok {
		t.Fatal("a failed invalid write must not surface a fencing token")
	}
	if len(store.invalids) != 1 || store.invalids[0].name != "broken" ||
		!store.invalids[0].hasObservation {
		t.Fatalf("invalid writes = %+v, want the observed broken attempt",
			store.invalids)
	}
}

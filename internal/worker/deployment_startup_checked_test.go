package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/deployment"
	"relay/internal/observability/metrics"
)

// computedAuthorization returns the fingerprint and raw-template identity maps
// a compatible authorization snapshot needs, computed exactly as the startup
// phase computes them from one loaded parse. Both are required for every
// authorized valid candidate.
func computedAuthorization(
	t *testing.T,
	root string,
) (map[string]string, map[string]string) {
	t.Helper()
	fns, err := app.NewLoader(root, discardLogger()).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	fingerprints := map[string]string{}
	identities := map[string]string{}
	for _, record := range selectAndFingerprintApps(fns, discardLogger()) {
		fingerprints[record.App.Name] = record.Fingerprint
	}
	for _, fn := range fns {
		if fn.TemplateIdentity == "" {
			t.Fatalf("%s: loaded record carries no template identity", fn.Name)
		}
		identities[fn.Name] = fn.TemplateIdentity
	}
	return fingerprints, identities
}

// checkedStartupLoad runs the production startup shape over root: the REAL
// deploymentStartupValidation check against this load's own scan, with the
// same authorization parameters worker.Run wires from the observed snapshot.
func checkedStartupLoad(
	t *testing.T,
	root string,
	snapshot deployment.Snapshot,
	active bool,
	store *fakeStartupState,
) (startupAppSet, error) {
	t.Helper()
	return loadStartupAppsChecked(context.Background(), discardLogger(),
		store, root, metrics.New(),
		snapshot.Generation,
		snapshot.Fingerprints,
		snapshot.Invalid,
		snapshot.Authoritative || active,
		deploymentStartupValidation(context.Background(), snapshot, active),
	)
}

// requireNoStartupStateWrites pins that a rejected authorization check aborts
// the load BEFORE any rebuild, discovery, prune, or invalid write reaches
// state — a rejected scan is never partially persisted.
func requireNoStartupStateWrites(t *testing.T, store *fakeStartupState) {
	t.Helper()
	if len(store.rebuilds) != 0 || len(store.discoveries) != 0 ||
		len(store.pruneDirs) != 0 || len(store.invalids) != 0 {
		t.Fatalf("state writes reached before the check finished: rebuilds=%d "+
			"discoveries=%d prunes=%d invalids=%d — the check must run before "+
			"any persistence",
			len(store.rebuilds), len(store.discoveries),
			len(store.pruneDirs), len(store.invalids))
	}
}

// TestLoadStartupAppsCheckedConsumesCompleteCompatibleAuthorization pins the
// complete-compatible case through the REAL deploymentStartupValidation check:
// a tree that fully matches the published generation loads completely — every
// authorized app present with its content fingerprint AND its carried
// raw-template identity (required in ordinary startup as well as while
// recovering a failed materialization), the intended invalid app kept as an
// issue, and the returned authorization set carrying the exact fingerprint map
// startup publication later checks prepared candidates against.
func TestLoadStartupAppsCheckedConsumesCompleteCompatibleAuthorization(t *testing.T) {
	// given one authorized app and one materialized-invalid definition
	const broken = "runtime: python9.9\n"
	build := func(t *testing.T) (string, deployment.Snapshot) {
		t.Helper()
		root := t.TempDir()
		fingerprint, identity := writeValidationApp(t, root, "alpha",
			validationTemplate)
		writeBrokenWorkerApp(t, root, "mat-bad", broken)
		snapshot := emptyAuthorization()
		snapshot.Fingerprints["alpha"] = fingerprint
		snapshot.TemplateIdentities["alpha"] = identity
		snapshot.Invalid["mat-bad"] = "template invalid at the published revision"
		snapshot.InvalidTemplates["mat-bad"] = app.TemplateIdentity([]byte(broken))
		return root, snapshot
	}

	for _, tc := range []struct {
		name    string
		partial bool
	}{
		{name: "ordinary startup"},
		{name: "failed-materialization recovery", partial: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, snapshot := build(t)
			if tc.partial {
				snapshot.Error = errors.New(
					"deployment: materialize commit: fetch failed")
			}
			store := &fakeStartupState{}

			// when the startup load runs the real deployment check
			set, err := checkedStartupLoad(t, root, snapshot, true, store)

			// then the compatible authorization is consumed completely
			if err != nil {
				t.Fatalf("compatible authorization = %v, want the load to proceed",
					err)
			}
			if names := loadedAppNames(set); len(names) != 1 ||
				names[0] != "alpha" {
				t.Fatalf("loaded apps = %v, want [alpha]", names)
			}
			if len(set.issues) != 1 || set.issues[0].Name != "mat-bad" {
				t.Fatalf("issues = %+v, want the intended mat-bad issue",
					set.issues)
			}

			// and the returned set carries the exact authorized fingerprint map
			// publication checks prepared candidates against
			if set.authorized == nil || len(set.authorized) != 1 ||
				set.authorized["alpha"] != snapshot.Fingerprints["alpha"] {
				t.Fatalf("authorized set = %v, want the snapshot's fingerprint map",
					set.authorized)
			}

			// and both writes persisted under the snapshot's generation with the
			// intended invalid app recorded
			if len(store.discoveries) != 1 ||
				store.discoveries[0].fn.Name != "alpha" ||
				store.discoveries[0].fingerprint !=
					snapshot.Fingerprints["alpha"] ||
				store.discoveries[0].sourceGeneration != snapshot.Generation {
				t.Fatalf("discovery writes = %+v, want alpha with its authorized "+
					"fingerprint under generation %d",
					store.discoveries, snapshot.Generation)
			}
			if len(store.invalids) != 1 || store.invalids[0].name != "mat-bad" {
				t.Fatalf("invalid writes = %+v, want only mat-bad",
					store.invalids)
			}
			if set.fencingTokens["alpha"] != 7 ||
				set.fencingTokens["mat-bad"] != 42 {
				t.Fatalf("fencing tokens = %v, want alpha=7 and mat-bad=42",
					set.fencingTokens)
			}
		})
	}
}

// TestLoadStartupAppsCheckedRejectsMissingAuthorizedApp pins the missing-app
// case: an authorized app absent from the scanned tree fails the load closed
// with the deployment diagnostic — otherwise its matched deliveries would be
// consumed and ACKed as unmatched — and nothing reaches state.
func TestLoadStartupAppsCheckedRejectsMissingAuthorizedApp(t *testing.T) {
	// given an empty tree against a generation that authorizes an app
	root := t.TempDir()
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["absent"] = "fp-of-the-materialized-revision"
	snapshot.TemplateIdentities["absent"] = "id-of-the-materialized-revision"
	store := &fakeStartupState{}

	// when the startup load runs the real deployment check
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then startup fails closed before any persistence
	if err == nil {
		t.Fatal("missing authorized app = nil, want a fail-closed rejection")
	}
	for _, want := range []string{
		"deployment: startup authorization incomplete",
		`authorized app "absent" is missing from the local tree`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}
	if len(set.loaded) != 0 || set.fencingTokens != nil {
		t.Fatalf("returned set = %+v, want empty on a rejected check", set)
	}
	requireNoStartupStateWrites(t, store)
}

// TestLoadStartupAppsCheckedRejectsChangedAuthorizedFingerprint pins the
// changed-content case: an authorized app whose local bytes no longer produce
// the materialized content fingerprint fails the load closed before anything
// is persisted — consuming it would serve content the shared generation never
// authorized.
func TestLoadStartupAppsCheckedRejectsChangedAuthorizedFingerprint(t *testing.T) {
	// given a materialized app that is locally edited after publication
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "alpha",
		validationTemplate)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["alpha"] = fingerprint
	snapshot.TemplateIdentities["alpha"] = identity

	// the comment flows into the stripped-template digest byte-for-byte, so
	// this edit CHANGES the content fingerprint
	edited, _ := writeValidationApp(t, root, "alpha",
		validationTemplate+"# local edit\n")
	if edited == fingerprint {
		t.Fatal("precondition: the local edit must change the content fingerprint")
	}
	store := &fakeStartupState{}

	// when the startup load runs the real deployment check
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then startup fails closed on the fingerprint gate before any persistence
	if err == nil {
		t.Fatal("fingerprint-changed authorized app = nil, want a fail-closed " +
			"rejection")
	}
	if want := `authorized app "alpha" no longer matches its materialized ` +
		"fingerprint"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
	if len(set.loaded) != 0 || set.fencingTokens != nil {
		t.Fatalf("returned set = %+v, want empty on a rejected check", set)
	}
	requireNoStartupStateWrites(t, store)
}

// TestLoadStartupAppsCheckedRejectsChangedTemplateIdentity pins the
// changed-template-identity case: the check proves each authorized app's
// recorded raw template identity against the identity CARRIED from the exact
// bytes this load parsed, so a runtime-only edit (which leaves the content
// fingerprint unchanged) fails closed in BOTH modes — ordinary startup, where
// the edit can arrive after successful materialization but before this scan,
// and failed-materialization recovery. An unedited tree validates in both
// modes, so the rejection is attributable to the carried identity alone.
func TestLoadStartupAppsCheckedRejectsChangedTemplateIdentity(t *testing.T) {
	// given a materialized app and a failed materialization being recovered
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "alpha",
		validationTemplate)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["alpha"] = fingerprint
	snapshot.TemplateIdentities["alpha"] = identity
	matErr := errors.New("deployment: materialize commit: fetch failed")
	partial := snapshot
	partial.Error = matErr

	// positive control: the unedited tree satisfies the recovery proof
	store := &fakeStartupState{}
	set, err := checkedStartupLoad(t, root, partial, true, store)
	if err != nil {
		t.Fatalf("unedited tree during recovery = %v, want the load to proceed",
			err)
	}
	if names := loadedAppNames(set); len(names) != 1 || names[0] != "alpha" {
		t.Fatalf("loaded apps = %v, want [alpha]", names)
	}

	// positive control: and it validates in ordinary startup as well
	if _, err := checkedStartupLoad(t, root, snapshot, true,
		&fakeStartupState{}); err != nil {
		t.Fatalf("unedited tree at ordinary startup = %v, want the load to "+
			"proceed", err)
	}

	// when a runtime-only template edit changes the raw identity but not the
	// content fingerprint (runtime-only fields are stripped from the digest)
	editedFingerprint, editedIdentity := writeValidationApp(t, root, "alpha",
		validationTemplate+"resources:\n  memory: 1GiB\n")
	if editedFingerprint != fingerprint {
		t.Fatalf("precondition: runtime-only edit changed the content "+
			"fingerprint %s -> %s", fingerprint, editedFingerprint)
	}
	if editedIdentity == identity {
		t.Fatal("precondition: runtime-only edit must change the raw template " +
			"identity")
	}
	recoveryStore := &fakeStartupState{}
	set, err = checkedStartupLoad(t, root, partial, true, recoveryStore)

	// then the recovery proof fails closed over the carried identity and wraps
	// the materialization cause, before anything is persisted
	if err == nil {
		t.Fatal("changed template bytes during recovery = nil, want a " +
			"fail-closed rejection")
	}
	if want := "template bytes no longer match the materialized revision"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
	if !errors.Is(err, matErr) {
		t.Fatalf("error = %v, want it to wrap the materialization failure", err)
	}
	if len(set.loaded) != 0 || set.fencingTokens != nil {
		t.Fatalf("returned set = %+v, want empty on a rejected check", set)
	}
	requireNoStartupStateWrites(t, recoveryStore)

	// and ordinary startup with the same drift fails closed too — the edit can
	// land after successful materialization, before this scan, and the
	// asynchronous reconciler would be too late to stop activation
	ordinaryStore := &fakeStartupState{}
	_, err = checkedStartupLoad(t, root, snapshot, true, ordinaryStore)
	if err == nil {
		t.Fatal("ordinary startup with identity-only drift = nil, want a " +
			"fail-closed rejection")
	}
	if want := "template bytes no longer match the materialized revision"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
	requireNoStartupStateWrites(t, ordinaryStore)
}

// TestLoadStartupAppsCheckedRejectsMissingAuthorizedIdentity pins the
// missing-identity case at the load boundary: an authorized app whose
// recorded template identity is absent cannot be proven from its content
// fingerprint alone (runtime-only template fields are excluded from it), so
// coordinated startup fails closed before any state write — in ordinary
// startup, not only while recovering a failed materialization.
func TestLoadStartupAppsCheckedRejectsMissingAuthorizedIdentity(t *testing.T) {
	// given an authorized fingerprint with no recorded template identity
	root := t.TempDir()
	fingerprint, _ := writeValidationApp(t, root, "alpha", validationTemplate)
	snapshot := emptyAuthorization()
	snapshot.Fingerprints["alpha"] = fingerprint
	store := &fakeStartupState{}

	// when the startup load runs the real deployment check
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then startup fails closed before any persistence
	if err == nil {
		t.Fatal("missing recorded template identity = nil, want a fail-closed " +
			"rejection")
	}
	if want := "has no recorded template identity"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
	if len(set.loaded) != 0 || set.fencingTokens != nil {
		t.Fatalf("returned set = %+v, want empty on a rejected check", set)
	}
	requireNoStartupStateWrites(t, store)
}

// TestLoadStartupAppsCheckedFiltersLocalOnlyCandidate pins the local-only
// case: a candidate the generation never authorized (and an app broken only by
// local drift) passes the ordinary check as expected drift but is filtered out
// of the loaded AND persisted set — never prepared, never recorded as desired,
// and never surfaced with a fencing token.
func TestLoadStartupAppsCheckedFiltersLocalOnlyCandidate(t *testing.T) {
	// given an authorized app plus a local addition and a local parse-breaking
	// edit of a second app
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "authorized",
		validationTemplate)
	writeValidationApp(t, root, "local-add", validationTemplate)
	writeBrokenWorkerApp(t, root, "drift-bad", "runtime: python9.9\n")

	snapshot := emptyAuthorization()
	snapshot.Fingerprints["authorized"] = fingerprint
	snapshot.TemplateIdentities["authorized"] = identity
	store := &fakeStartupState{}

	// when the startup load runs the real deployment check
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then the check allows ordinary drift and the authoritative filter drops
	// it from the loaded and persisted set
	if err != nil {
		t.Fatalf("local-only candidate at ordinary startup = %v, want filtering, "+
			"not failure", err)
	}
	if names := loadedAppNames(set); len(names) != 1 ||
		names[0] != "authorized" {
		t.Fatalf("loaded apps = %v, want only authorized", names)
	}
	if len(set.issues) != 0 {
		t.Fatalf("issues = %+v, want none (drift-bad was never materialized "+
			"invalid)", set.issues)
	}
	if len(store.discoveries) != 1 ||
		store.discoveries[0].fn.Name != "authorized" {
		t.Fatalf("discovery writes = %+v, want only authorized",
			store.discoveries)
	}
	if len(store.invalids) != 0 {
		t.Fatalf("invalid writes = %+v, want none (a local breakage is drift)",
			store.invalids)
	}
	for _, name := range []string{"local-add", "drift-bad"} {
		if _, ok := set.fencingTokens[name]; ok {
			t.Fatalf("%s surfaced a fencing token; it must stay filtered", name)
		}
	}
	if set.authorized == nil || len(set.authorized) != 1 ||
		set.authorized["authorized"] != fingerprint {
		t.Fatalf("authorized set = %v, want the snapshot's fingerprint map",
			set.authorized)
	}
}

// TestLoadStartupAppsCheckedKeepsIntendedInvalidAndDropsRemovedApp pins the
// intended-invalid/removed cases: an app the generation materialized as
// invalid keeps its issue (and is recorded), while a name the generation
// REMOVED that exists locally again is filtered without failing ordinary
// startup — yet during failed-materialization recovery that reappeared name
// fails closed, because the partially synced target may have restored it.
func TestLoadStartupAppsCheckedKeepsIntendedInvalidAndDropsRemovedApp(t *testing.T) {
	// given the authorized app, an intended-invalid app, and a removed name
	// present locally again
	const broken = "runtime: python9.9\n"
	root := t.TempDir()
	fingerprint, identity := writeValidationApp(t, root, "authorized",
		validationTemplate)
	writeBrokenWorkerApp(t, root, "intended-bad", broken)
	writeValidationApp(t, root, "removed-app", validationTemplate)

	snapshot := emptyAuthorization()
	snapshot.Fingerprints["authorized"] = fingerprint
	snapshot.TemplateIdentities["authorized"] = identity
	snapshot.Invalid["intended-bad"] = "template invalid at the published revision"
	snapshot.InvalidTemplates["intended-bad"] = app.TemplateIdentity([]byte(broken))
	snapshot.Removed = []string{"removed-app"}

	// when ordinary startup runs the real deployment check
	store := &fakeStartupState{}
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then the intended invalid app stays an issue and the removed name is
	// filtered out of the loaded and persisted set
	if err != nil {
		t.Fatalf("intended invalid plus reappeared removed name at ordinary "+
			"startup = %v, want the load to proceed", err)
	}
	if names := loadedAppNames(set); len(names) != 1 ||
		names[0] != "authorized" {
		t.Fatalf("loaded apps = %v, want only authorized", names)
	}
	if len(set.issues) != 1 || set.issues[0].Name != "intended-bad" {
		t.Fatalf("issues = %+v, want the intended invalid app kept", set.issues)
	}
	if len(store.invalids) != 1 || store.invalids[0].name != "intended-bad" {
		t.Fatalf("invalid writes = %+v, want only intended-bad", store.invalids)
	}
	if len(store.discoveries) != 1 ||
		store.discoveries[0].fn.Name != "authorized" {
		t.Fatalf("discovery writes = %+v, want only authorized",
			store.discoveries)
	}
	if set.fencingTokens["authorized"] != 7 ||
		set.fencingTokens["intended-bad"] != 42 {
		t.Fatalf("fencing tokens = %v, want authorized=7 and intended-bad=42",
			set.fencingTokens)
	}
	if _, ok := set.fencingTokens["removed-app"]; ok {
		t.Fatal("the reappeared removed name surfaced a fencing token; it must " +
			"stay filtered")
	}

	// when the same tree is consumed while recovering a failed materialization
	matErr := errors.New("deployment: materialize commit: fetch failed")
	partial := snapshot
	partial.Error = matErr
	removeStore := &fakeStartupState{}
	_, err = checkedStartupLoad(t, root, partial, true, removeStore)

	// then the reappeared removed name fails closed before any persistence
	if err == nil {
		t.Fatal("reappeared removed name during recovery = nil, want a " +
			"fail-closed rejection")
	}
	if want := "is not part of the previous authorized generation"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
	requireNoStartupStateWrites(t, removeStore)

	// and once the name is gone again, the intended invalid identity still
	// satisfies the recovery proof
	if err := os.RemoveAll(filepath.Join(root, "removed-app")); err != nil {
		t.Fatalf("remove reappeared app: %v", err)
	}
	recoveryStore := &fakeStartupState{}
	if _, err := checkedStartupLoad(t, root, partial, true, recoveryStore); err != nil {
		t.Fatalf("recovery over the intended generation = %v, want the load to "+
			"proceed", err)
	}
}

// TestLoadStartupAppsCheckedEmptyDeploymentAuthorizesNothing pins the empty
// deployment case: a coordinated worker with no authorized apps loads nothing
// and — unlike standalone startup — returns a NON-NIL empty authorization set,
// so publication refuses any candidate a local /apps edit could introduce.
func TestLoadStartupAppsCheckedEmptyDeploymentAuthorizesNothing(t *testing.T) {
	// when a generation authorizes nothing over an empty tree
	root := t.TempDir()
	snapshot := emptyAuthorization()
	store := &fakeStartupState{}
	set, err := checkedStartupLoad(t, root, snapshot, true, store)

	// then startup proceeds with nothing loaded and an empty, non-nil
	// authorization set
	if err != nil {
		t.Fatalf("empty deployment over an empty tree = %v, want the load to "+
			"proceed", err)
	}
	if len(set.loaded) != 0 || len(set.issues) != 0 {
		t.Fatalf("loaded/issues = %v/%+v, want nothing loaded",
			loadedAppNames(set), set.issues)
	}
	if set.authorized == nil || len(set.authorized) != 0 {
		t.Fatalf("authorized set = %v, want a non-nil empty set in coordinated "+
			"mode", set.authorized)
	}
	if len(store.discoveries) != 0 || len(store.invalids) != 0 {
		t.Fatalf("state writes = discoveries %d / invalids %d, want none",
			len(store.discoveries), len(store.invalids))
	}

	// and a local app against that empty authorization stays inert: filtered
	// from the loaded set and persisted nowhere
	writeValidationApp(t, root, "local-only", validationTemplate)
	localStore := &fakeStartupState{}
	local, err := checkedStartupLoad(t, root, snapshot, true, localStore)
	if err != nil {
		t.Fatalf("local app against an empty authorization = %v, want filtering, "+
			"not failure", err)
	}
	if len(local.loaded) != 0 {
		t.Fatalf("loaded apps = %v, want nothing authorized", loadedAppNames(local))
	}
	if len(localStore.discoveries) != 0 {
		t.Fatalf("discovery writes = %+v, want none", localStore.discoveries)
	}
	if _, ok := local.fencingTokens["local-only"]; ok {
		t.Fatal("the local-only app surfaced a fencing token; it must stay " +
			"inert")
	}
	if local.authorized == nil || len(local.authorized) != 0 {
		t.Fatalf("authorized set = %v, want a non-nil empty set", local.authorized)
	}
}

// TestLoadStartupAppsCheckedStandaloneSkipsDeploymentValidation pins the
// standalone case: without a configured coordinator there is no deployment
// check at all — local /apps stays authoritative, the load succeeds against a
// zero snapshot, and the returned authorization set is NIL so startup
// publication keeps each app's built identity.
func TestLoadStartupAppsCheckedStandaloneSkipsDeploymentValidation(t *testing.T) {
	// given a local app and no coordinated deployment
	root := t.TempDir()
	writeWorkerApp(t, root, "solo", "def handler(e, context): return 1\n")
	check := deploymentStartupValidation(context.Background(),
		deployment.Snapshot{}, false)

	// when the standalone startup load runs
	if check != nil {
		t.Fatal("standalone startup must receive no deployment check, got a " +
			"non-nil callback")
	}
	store := &fakeStartupState{}
	set, err := loadStartupAppsChecked(context.Background(), discardLogger(),
		store, root, metrics.New(), 0, nil, nil, false, check)

	// then the local app loads and persists with no coordinated authorization
	if err != nil {
		t.Fatalf("standalone load = %v, want it to proceed unvalidated", err)
	}
	if names := loadedAppNames(set); len(names) != 1 || names[0] != "solo" {
		t.Fatalf("loaded apps = %v, want [solo]", names)
	}
	if set.fencingTokens["solo"] != 7 {
		t.Fatalf("solo token = %d, want the discovery token 7",
			set.fencingTokens["solo"])
	}
	if len(store.discoveries) != 1 ||
		store.discoveries[0].sourceGeneration != 0 {
		t.Fatalf("discovery writes = %+v, want one stamped with generation 0",
			store.discoveries)
	}
	if set.authorized != nil {
		t.Fatalf("authorized set = %v, want nil so publication keeps built "+
			"identities", set.authorized)
	}
}

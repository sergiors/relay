//go:build integration

package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/state"
	"relay/internal/testutil"
)

// startupInvalidTemplate is a deterministic parse/validation failure whose
// identity the startup gate can recognize across restarts.
const startupInvalidTemplate = "runtime: python9.9\n"

func writeStartupTemplate(t *testing.T, root, name, data string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, app.TemplateFile), []byte(data), 0o644); err != nil {
		t.Fatalf("write %s template: %v", name, err)
	}
	return dir
}

func newStartupGate(
	ctx context.Context,
	st *state.State,
	logger *slog.Logger,
) *startupParseGate {
	return &startupParseGate{
		ctx:      ctx,
		st:       st,
		logger:   logger,
		observed: make(map[string]state.AppObservation),
	}
}

func seedStartupInvalid(
	t *testing.T,
	st *state.State,
	name string,
) (state.FencingToken, string) {
	t.Helper()
	identity := app.TemplateIdentity([]byte(startupInvalidTemplate))
	parseErr := errors.New(`app "` + name + `": unknown runtime "python9.9"`)
	observed, err := st.ObserveApp(context.Background(), name)
	if err != nil {
		t.Fatalf("ObserveApp(%q): %v", name, err)
	}
	result, err := st.RecordInvalidDesiredWithSource(
		context.Background(), name, identity, parseErr, &observed)
	if err != nil {
		t.Fatalf("RecordInvalidDesiredWithSource(%q): %v", name, err)
	}
	if !result.Wrote {
		t.Fatalf("seed invalid %q: wrote = false, want a new row", name)
	}
	return result.FencingToken, identity
}

// TestStartupParseGateSkipsPersistedInvalidIdentity pins the startup fast path:
// when the exact persisted invalid identity is on disk, the loader does not parse
// or warn again; the state phase reuses the existing token and writes nothing.
func TestStartupParseGateSkipsPersistedInvalidIdentity(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	writeStartupTemplate(t, root, "broken", startupInvalidTemplate)
	token, identity := seedStartupInvalid(t, st, "broken")
	before, _ := st.GetApp("broken")

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	gate := newStartupGate(context.Background(), st, logger)
	loader := app.NewLoader(root, logger)
	apps, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("apps = %+v, want none (the invalid template must not load)", apps)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want one for broken", issues)
	}
	issue := issues[0]
	if issue.Name != "broken" || !issue.Recorded {
		t.Fatalf("issue = %+v, want a Recorded issue", issue)
	}
	if issue.Identity != identity {
		t.Fatalf("issue identity = %q, want %q", issue.Identity, identity)
	}
	if issue.Err == nil || !strings.Contains(issue.Err.Error(), "python9.9") {
		t.Fatalf("issue error = %v, want the persisted diagnostic", issue.Err)
	}
	if logs.Len() != 0 {
		t.Fatalf("a Recorded issue must not be logged again, logs = %q", logs.String())
	}
	observation, ok := gate.observed["broken"]
	if !ok || !observation.Found || observation.Detail.InvalidSource != identity {
		t.Fatalf("startup observation = %+v ok=%v, want the persisted invalid row", observation, ok)
	}

	tokens, err := persistStartupDiscovery(
		context.Background(), st, root, nil, issues, gate.observed, logger)
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}
	if got := tokens["broken"]; got != token {
		t.Fatalf("startup token = %d, want the existing %d (no allocation)", got, token)
	}
	after, ok := st.GetApp("broken")
	if !ok {
		t.Fatal("expected the invalid row to survive startup persistence")
	}
	if before.UpdatedAt != after.UpdatedAt || before.LastError != after.LastError ||
		before.InvalidSource != after.InvalidSource {
		t.Fatalf("startup fast path changed the row:\n before: %+v\n  after: %+v", before, after)
	}
	if got := persistedObservationForTest(t, st, "broken").FencingToken; got != token {
		t.Fatalf("persisted token = %d, want unchanged %d", got, token)
	}
}

// TestStartupParseGateReparsesChangedInvalidIdentity pins that changed invalid
// bytes are not skipped: they are parsed, recorded as a new transition, and a
// later valid startup correction clears the identity.
func TestStartupParseGateReparsesChangedInvalidIdentity(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	dir := writeStartupTemplate(t, root, "broken", startupInvalidTemplate)
	tokenA, _ := seedStartupInvalid(t, st, "broken")

	changed := startupInvalidTemplate + "# changed bytes\n"
	if err := os.WriteFile(filepath.Join(dir, app.TemplateFile), []byte(changed), 0o644); err != nil {
		t.Fatalf("write changed template: %v", err)
	}
	logger := discardLogger()
	gate := newStartupGate(context.Background(), st, logger)
	loader := app.NewLoader(root, logger)
	_, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want one", issues)
	}
	if issues[0].Recorded {
		t.Fatal("changed invalid bytes must be parsed, not treated as recorded")
	}
	identityB := app.TemplateIdentity([]byte(changed))
	if issues[0].Identity != identityB {
		t.Fatalf("issue identity = %q, want the changed %q", issues[0].Identity, identityB)
	}

	tokens, err := persistStartupDiscovery(
		context.Background(), st, root, nil, issues, gate.observed, logger)
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}
	if tokens["broken"] <= tokenA {
		t.Fatalf("changed-bytes token = %d, want it to advance past %d", tokens["broken"], tokenA)
	}
	invalid, _ := st.GetApp("broken")
	if invalid.InvalidSource != identityB {
		t.Fatalf("invalid_source = %q, want %q", invalid.InvalidSource, identityB)
	}

	// A valid correction at the next startup parses and clears the identity.
	if err := os.WriteFile(filepath.Join(dir, app.TemplateFile), []byte(
		"runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"), 0o644); err != nil {
		t.Fatalf("write valid template: %v", err)
	}
	gate = newStartupGate(context.Background(), st, logger)
	apps, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing(valid): %v", err)
	}
	if len(apps) != 1 || apps[0].Name != "broken" || len(issues) != 0 {
		t.Fatalf("apps/issues = %+v/%+v, want the valid app and no issues", apps, issues)
	}
	discovered := []state.DiscoveredApp{{App: apps[0], Fingerprint: "fp-valid"}}
	if _, err := persistStartupDiscovery(
		context.Background(), st, root, discovered, nil, gate.observed, logger); err != nil {
		t.Fatalf("persistStartupDiscovery(valid): %v", err)
	}
	recovered, _ := st.GetApp("broken")
	if recovered.InvalidSource != "" {
		t.Fatalf("invalid_source = %q, want cleared by a valid startup discovery", recovered.InvalidSource)
	}
	if recovered.Status != state.StatusPreparing {
		t.Fatalf("status = %q, want preparing after startup discovery", recovered.Status)
	}
}

// concurrentCorrectionGate performs another worker's valid write after the
// pre-read Observe but before SkipParse re-reads state, modeling a correction
// that lands while this worker still holds the older observation.
type concurrentCorrectionGate struct {
	*startupParseGate
	correct func()
	done    bool
}

func (g *concurrentCorrectionGate) SkipParse(name, identity string) (bool, error) {
	if !g.done {
		g.done = true
		g.correct()
	}
	return g.startupParseGate.SkipParse(name, identity)
}

// TestStartupParseGateStaleObservationDoesNotOverwriteCorrection pins the
// startup cross-worker guard: a valid correction from another worker invalidates
// the older pre-read observation, so this worker re-parses instead of skipping and
// the stale invalid write is rejected (leaving the newer valid state intact).
func TestStartupParseGateStaleObservationDoesNotOverwriteCorrection(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	older := testutil.OpenState(t, dsn)
	newer := testutil.OpenState(t, dsn)

	root := t.TempDir()
	writeStartupTemplate(t, root, "shared", startupInvalidTemplate)
	oldToken, identity := seedStartupInvalid(t, older, "shared")

	// corrected is the full post-correction row captured synchronously by the
	// callback, after newerWrite commits and before the stale persistence phase.
	var corrected state.AppObservation
	newerWrite := func() {
		fn := stateApp("shared", filepath.Join(root, "shared"))
		correctedToken, err := newer.RecordDiscoveredWithFingerprint(
			context.Background(), fn, "fp-corrected")
		if err != nil {
			t.Fatalf("newer valid correction: %v", err)
		}
		if correctedToken <= oldToken {
			t.Fatalf("corrected token = %d, want it past the invalid %d",
				correctedToken, oldToken)
		}
		corrected, err = newer.ObserveApp(context.Background(), "shared")
		if err != nil {
			t.Fatalf("observe corrected row: %v", err)
		}
		if !corrected.Found {
			t.Fatal("newer valid correction left no row")
		}
		if corrected.FencingToken != correctedToken {
			t.Fatalf("observed corrected token = %d, want the written %d",
				corrected.FencingToken, correctedToken)
		}
		assertCorrectedStartupRow(t, corrected.Detail)
	}

	logger := discardLogger()
	inner := newStartupGate(context.Background(), older, logger)
	gate := &concurrentCorrectionGate{startupParseGate: inner, correct: newerWrite}
	loader := app.NewLoader(root, logger)
	apps, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(apps) != 0 || len(issues) != 1 {
		t.Fatalf("apps/issues = %+v/%+v, want the invalid issue re-parsed", apps, issues)
	}
	if issues[0].Recorded {
		t.Fatal("a stale observation must not authorize a skip")
	}
	if issues[0].Identity != identity {
		t.Fatalf("issue identity = %q, want %q", issues[0].Identity, identity)
	}

	oldObserved, ok := inner.observed["shared"]
	if !ok || !oldObserved.Found {
		t.Fatalf("pre-read observation = %+v ok=%v, want the seeded invalid row",
			oldObserved, ok)
	}
	if oldObserved.FencingToken != oldToken {
		t.Fatalf("pre-read observation token = %d, want the old %d",
			oldObserved.FencingToken, oldToken)
	}
	if oldObserved.Detail.InvalidSource != identity {
		t.Fatalf("pre-read invalid source = %q, want %q",
			oldObserved.Detail.InvalidSource, identity)
	}
	if !corrected.Found {
		t.Fatal("the concurrent correction callback did not run")
	}

	tokens, err := persistStartupDiscovery(
		context.Background(), older, root, nil, issues, inner.observed, logger)
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}
	if token, ok := tokens["shared"]; ok {
		t.Fatalf("rejected stale observation surfaced token %d", token)
	}
	after, err := older.ObserveApp(context.Background(), "shared")
	if err != nil {
		t.Fatalf("observe row after stale persistence: %v", err)
	}
	if !after.Found {
		t.Fatal("stale persistence removed the corrected row")
	}
	if after.FencingToken != corrected.FencingToken {
		t.Fatalf("fencing token after stale persistence = %d, want the corrected %d",
			after.FencingToken, corrected.FencingToken)
	}
	if !reflect.DeepEqual(after.Detail, corrected.Detail) {
		t.Fatalf("stale write changed the corrected row:\n got: %+v\nwant: %+v",
			after.Detail, corrected.Detail)
	}
}

// assertCorrectedStartupRow pins the valid correction's persisted fields. The
// non-empty timestamp keeps the later full-row comparison meaningful rather than
// comparing zero values.
func assertCorrectedStartupRow(t *testing.T, detail state.Detail) {
	t.Helper()
	if detail.Name != "shared" {
		t.Fatalf("corrected name = %q, want shared", detail.Name)
	}
	if detail.Runtime != "node24" {
		t.Fatalf("corrected runtime = %q, want node24", detail.Runtime)
	}
	if detail.Status != state.StatusPreparing {
		t.Fatalf("corrected status = %q, want preparing", detail.Status)
	}
	if detail.DesiredFingerprint != "fp-corrected" {
		t.Fatalf("corrected desired fingerprint = %q, want fp-corrected",
			detail.DesiredFingerprint)
	}
	if detail.InvalidSource != "" {
		t.Fatalf("corrected invalid source = %q, want cleared",
			detail.InvalidSource)
	}
	if detail.UpdatedAt == "" {
		t.Fatal("corrected row has no updated_at; baseline comparison is meaningless")
	}
}

// TestStartupParseGateFallsBackWhenStateReadFails pins that an unreadable state
// view never authorizes a skip: the loader parses normally and reports the issue
// so the caller can retry/record it.
func TestStartupParseGateFallsBackWhenStateReadFails(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	writeStartupTemplate(t, root, "broken", startupInvalidTemplate)
	_ = st.Close() // simulate a transient/unavailable state view

	gate := newStartupGate(context.Background(), st, discardLogger())
	loader := app.NewLoader(root, discardLogger())
	_, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want the unrecorded invalid issue", issues)
	}
	if issues[0].Recorded {
		t.Fatal("a failed state observation must not authorize a skip")
	}
	if want := app.TemplateIdentity([]byte(startupInvalidTemplate)); issues[0].Identity != want {
		t.Fatalf("issue identity = %q, want %q", issues[0].Identity, want)
	}
	if len(gate.observed) != 0 {
		t.Fatalf("observations = %+v, want none after a state read failure", gate.observed)
	}
}

// TestStartupParseGateRecognizesIdentityAcrossRestart pins the restart case:
// the persisted row is the authority, so a fresh worker handle recognizes the same
// invalid bytes without re-parsing, re-writing, or allocating a token.
func TestStartupParseGateRecognizesIdentityAcrossRestart(t *testing.T) {
	dsn := testutil.NewStateDSN(t)
	first := testutil.OpenState(t, dsn)
	root := t.TempDir()
	writeStartupTemplate(t, root, "broken", startupInvalidTemplate)
	token, identity := seedStartupInvalid(t, first, "broken")
	if err := first.Close(); err != nil {
		t.Fatalf("close first handle: %v", err)
	}

	// A fresh handle models a restarted worker process on the same database.
	restarted := testutil.OpenState(t, dsn)
	logger := discardLogger()
	gate := newStartupGate(context.Background(), restarted, logger)
	loader := app.NewLoader(root, logger)
	_, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(issues) != 1 || !issues[0].Recorded {
		t.Fatalf("issues = %+v, want the persisted identity recognized after restart", issues)
	}
	if issues[0].Identity != identity {
		t.Fatalf("issue identity = %q, want %q", issues[0].Identity, identity)
	}

	tokens, err := persistStartupDiscovery(
		context.Background(), restarted, root, nil, issues, gate.observed, logger)
	if err != nil {
		t.Fatalf("persistStartupDiscovery: %v", err)
	}
	if got := tokens["broken"]; got != token {
		t.Fatalf("restart token = %d, want the persisted %d", got, token)
	}
	if got := persistedObservationForTest(t, restarted, "broken").FencingToken; got != token {
		t.Fatalf("restart persisted token = %d, want unchanged %d", got, token)
	}
	detail, _ := restarted.GetApp("broken")
	if detail.InvalidSource != identity {
		t.Fatalf("invalid_source = %q, want %q after restart", detail.InvalidSource, identity)
	}
}

func persistedObservationForTest(t *testing.T, st *state.State, name string) state.AppObservation {
	t.Helper()
	observation, err := st.ObserveApp(context.Background(), name)
	if err != nil {
		t.Fatalf("ObserveApp(%q): %v", name, err)
	}
	return observation
}

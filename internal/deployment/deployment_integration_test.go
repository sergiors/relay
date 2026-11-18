//go:build integration

package deployment

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/git"
	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	deployRepo = "git@github.com:acme/r.git"
	commitOne  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	commitTwo  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

const coordinatorTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"

// coordinatorEnv bundles one coordinator test's state handle and config.
type coordinatorEnv struct {
	st  *state.State
	cfg Config
	log *bytes.Buffer
}

// newCoordinatorEnv builds ONE state handle shared by the test and the
// coordinator, plus a config over fresh temp directories and a capturing
// logger.
func newCoordinatorEnv(t *testing.T) *coordinatorEnv {
	t.Helper()
	gitDir := t.TempDir()
	logBuf := &bytes.Buffer{}
	st := testutil.NewState(t)
	return &coordinatorEnv{
		st: st,
		cfg: Config{
			State:          st,
			NodeID:         "node-1",
			InstanceID:     "inst-1",
			GitConfigPath:  filepath.Join(gitDir, "source.json"),
			GitCheckoutDir: filepath.Join(gitDir, "checkout"),
			GitSSHDir:      filepath.Join(gitDir, "ssh"),
			AppsDir:        t.TempDir(),
			Logger: slog.New(slog.NewTextHandler(logBuf,
				&slog.HandlerOptions{Level: slog.LevelDebug})),
		},
		log: logBuf,
	}
}

// publishDesired commits one operator revision as the shared desired row.
func publishDesired(t *testing.T, st *state.State, commit, ref string) {
	t.Helper()
	result, err := st.PublishDeploymentDesired(context.Background(),
		state.DeploymentPublishRequest{
			Desired: state.DeploymentDesired{
				Commit:     commit,
				Repository: deployRepo,
				Ref:        ref,
				Publisher:  "test",
			},
			ExpectedGeneration: -1,
			AllowPinned:        true,
		})
	if err != nil || !result.Published {
		t.Fatalf("publish desired = (%+v, %v), want published", result, err)
	}
}

// seededApp is one per-app progress row to plant for the applied generation.
type seededApp struct {
	name        string
	fingerprint string
	templateID  string
	phase       string
	errMessage  string
}

// seedAppliedNode plants a node whose applied generation is fully materialized
// with the given per-app rows, mirroring the progress a previous run recorded.
func seedAppliedNode(
	t *testing.T,
	st *state.State,
	nodeID, instance string,
	generation int64,
	commit string,
	apps ...seededApp,
) {
	t.Helper()
	ctx := context.Background()
	acquired, err := st.AcquireDeploymentNode(ctx, nodeID, instance, generation,
		state.DeploymentNodeProgress{
			NodeID: nodeID, Generation: generation,
			Phase: state.DeploymentNodeMaterialized, Commit: commit,
		})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if landed, err := st.RecordDeploymentNodeProgress(
		ctx, nodeID, generation, acquired.Incarnation,
		state.DeploymentNodeProgress{
			NodeID: nodeID, Generation: generation,
			AppliedGeneration:  generation,
			AppliedIncarnation: acquired.Incarnation,
			Phase:              state.DeploymentNodeMaterialized,
			Commit:             commit, AppliedCommit: commit,
		}); err != nil || !landed {
		t.Fatalf("seed node progress = (%v, %v)", landed, err)
	}
	for _, a := range apps {
		if landed, err := st.RecordDeploymentAppProgress(
			ctx, nodeID, a.name, generation, acquired.Incarnation,
			state.DeploymentAppProgress{
				NodeID: nodeID, App: a.name, Generation: generation,
				Instance: instance, Phase: a.phase,
				Fingerprint: a.fingerprint, TemplateIdentity: a.templateID,
				Commit: commit, Error: a.errMessage,
			}); err != nil || !landed {
			t.Fatalf("seed app progress %q = (%v, %v)", a.name, landed, err)
		}
	}
}

// writeCoordinatorApp materializes one valid app into dir and returns its
// content fingerprint plus template identity — the values authorize() computes
// from the same tree.
func writeCoordinatorApp(t *testing.T, appsDir, name string) (fingerprint, templateID string) {
	t.Helper()
	dir := filepath.Join(appsDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(coordinatorTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(coordinatorTemplate))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	fp, err := app.FingerprintApp(dir, tmpl)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	src, err := app.ReadTemplateSource(appsDir, name)
	if err != nil {
		t.Fatalf("template source: %v", err)
	}
	return fp, src.Identity
}

// TestObserveStartupWithoutDesiredRevision pins that a source-driven
// participating node (valid local Git source) that starts before anything was
// ever published observes no authorization and PUBLISHES nothing: startup
// succeeds, the shared row stays absent, and the snapshot is not
// authoritative — so cluster reconciliation authorizes no local /apps.
func TestObserveStartupWithoutDesiredRevision(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	c := New(env.cfg)

	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Authoritative || snapshot.Generation != 0 || snapshot.Error != nil {
		t.Fatalf("snapshot = %+v, want a non-authoritative empty observation", snapshot)
	}
	// Observation never publishes: the shared row must still be absent.
	if _, found, err := env.st.ReadDeploymentDesired(context.Background()); err != nil || found {
		t.Fatalf("ReadDeploymentDesired = (_, %v, %v), want no row (observation publishes nothing)",
			found, err)
	}
}

// TestObserveStartupMissingGitSourceReportsAndFailsClosed pins that a node
// with a shared revision but no local Git source cannot materialize: the
// failure is reported in the snapshot (startup continues), no authorization is
// granted, and the node's progress row records the failed attempt.
func TestObserveStartupMissingGitSourceReportsAndFailsClosed(t *testing.T) {
	env := newCoordinatorEnv(t)
	publishDesired(t, env.st, commitOne, "main")
	c := New(env.cfg)

	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup = %v, want a non-fatal reported failure", err)
	}
	if snapshot.Error == nil || !strings.Contains(snapshot.Error.Error(), "load local git source") {
		t.Fatalf("snapshot.Error = %v, want the missing-git-source failure", snapshot.Error)
	}
	if snapshot.Authoritative {
		t.Fatal("a failed startup must not authorize cluster reconciliation")
	}

	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.Phase != state.DeploymentNodeFailed || node.Generation != 1 ||
		node.AppliedGeneration != 0 {
		t.Fatalf("node = %+v, want a failed generation-1 attempt with nothing applied", node)
	}
	if !strings.Contains(node.Error, "load local git source") {
		t.Fatalf("node error = %q, want the missing-git-source diagnosis", node.Error)
	}
}

// TestObserveStartupSourceMismatchRetainsPreviousGeneration pins that a shared
// revision whose source coordinates disagree with the local Git source fails
// closed BEFORE any network work, while the previously materialized generation
// stays authoritative and serving — the core "failure retains the previous
// generation" contract.
func TestObserveStartupSourceMismatchRetainsPreviousGeneration(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: "fp-1", templateID: "tid-1",
			phase: state.DeploymentAppPending})
	// The shared revision tracks a different branch than this node's source.
	publishDesired(t, env.st, commitTwo, "dev")
	c := New(env.cfg)

	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup = %v, want a non-fatal reported failure", err)
	}
	if snapshot.Error == nil ||
		!strings.Contains(snapshot.Error.Error(), "does not match the shared revision") {
		t.Fatalf("snapshot.Error = %v, want the source-mismatch diagnosis", snapshot.Error)
	}
	if !snapshot.Authoritative || snapshot.Generation != 1 ||
		snapshot.Commit != commitOne {
		t.Fatalf("snapshot = %+v, want the previous generation 1 still authoritative", snapshot)
	}
	if snapshot.Fingerprints["fn"] != "fp-1" {
		t.Fatalf("fingerprints = %v, want the previous fn authorization", snapshot.Fingerprints)
	}

	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.Phase != state.DeploymentNodeFailed {
		t.Fatalf("node phase = %q, want %q", node.Phase, state.DeploymentNodeFailed)
	}
	if node.AppliedGeneration != 1 || node.AppliedCommit != commitOne {
		t.Fatalf("node = %+v, want applied generation 1 (%s) preserved", node, commitOne)
	}
	if !strings.Contains(node.Error, "ref is") {
		t.Fatalf("node error = %q, want the mismatched ref detail", node.Error)
	}
	// The mismatch is validated BEFORE any fetch: no checkout was created,
	// nothing was materialized into the local tree, and no local sync ran.
	if _, err := os.Stat(env.cfg.GitCheckoutDir); !os.IsNotExist(err) {
		t.Fatalf("checkout dir stat = %v, want no clone (mismatch must fail before fetch)", err)
	}
	entries, err := os.ReadDir(env.cfg.AppsDir)
	if err != nil {
		t.Fatalf("read apps dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("apps dir = %v, want untouched (no local sync ran)", entries)
	}
}

// TestRestartRecoversAuthorizationAndReportsProgress pins restart recovery:
// the applied generation, its fingerprints, invalid apps, and removals are
// reconstructed from persisted progress; the restart takes ownership with a
// fresh incarnation; and activation reports from the recovered coordinator
// land against the applied generation.
func TestRestartRecoversAuthorizationAndReportsProgress(t *testing.T) {
	env := newCoordinatorEnv(t)
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 2, commitTwo,
		seededApp{name: "fn", fingerprint: "fp-2", templateID: "tid-2",
			phase: state.DeploymentAppPending},
		seededApp{name: "bad", phase: state.DeploymentAppFailed,
			errMessage: "template invalid"},
		seededApp{name: "gone", phase: state.DeploymentAppRemoved},
	)
	before, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", before, found, err)
	}

	// No desired row: the restart keeps serving exactly what was applied.
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if !snapshot.Authoritative || snapshot.Generation != 2 ||
		snapshot.Commit != commitTwo {
		t.Fatalf("snapshot = %+v, want the recovered generation 2 authorization", snapshot)
	}
	if snapshot.Fingerprints["fn"] != "fp-2" {
		t.Fatalf("fingerprints = %v, want fn authorized", snapshot.Fingerprints)
	}
	if snapshot.Invalid["bad"] != "template invalid" {
		t.Fatalf("invalid = %v, want the materialized bad-app diagnostic", snapshot.Invalid)
	}
	if len(snapshot.Removed) != 1 || snapshot.Removed[0] != "gone" {
		t.Fatalf("removed = %v, want [gone]", snapshot.Removed)
	}

	after, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode after restart = (%+v, %v, %v)", after, found, err)
	}
	if after.Incarnation <= before.Incarnation {
		t.Fatalf("incarnation = %d, want a fresh one after %d (restart takes ownership)",
			after.Incarnation, before.Incarnation)
	}
	if after.Instance != env.cfg.InstanceID {
		t.Fatalf("instance = %q, want %q", after.Instance, env.cfg.InstanceID)
	}

	// Activation reports from the recovered coordinator land on the applied
	// generation with the authorized fingerprint.
	c.ReportApp("fn", state.DeploymentAppActive, "")
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 2)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var fnRow *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "fn" {
			fnRow = &rows[i]
		}
	}
	if fnRow == nil || fnRow.Phase != state.DeploymentAppActive {
		t.Fatalf("fn row = %+v, want phase active", fnRow)
	}
	if fnRow.Fingerprint != "fp-2" || fnRow.Instance != env.cfg.InstanceID {
		t.Fatalf("fn row = %+v, want the authorized fingerprint and this instance", fnRow)
	}
}

// TestReportAppBeforeStartupIsNoop pins that a coordinator that has not
// observed any authorization yet writes no per-app progress: a stale
// coordinator can never attribute progress to a generation it does not hold.
func TestReportAppBeforeStartupIsNoop(t *testing.T) {
	env := newCoordinatorEnv(t)
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 2, commitTwo,
		seededApp{name: "fn", fingerprint: "fp-2", templateID: "tid-2",
			phase: state.DeploymentAppPending})

	c := New(env.cfg) // no ObserveStartup: applied generation is zero
	c.ReportApp("never-reported", state.DeploymentAppActive, "")

	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	for _, row := range rows {
		if row.App == "never-reported" {
			t.Fatalf("stale coordinator wrote progress: %+v", row)
		}
	}
}

// TestReportDriftRecordsProgressAndKicks pins the drift seam: a local /apps
// change is recorded as drifted progress for the applied generation and
// requests an immediate re-materialization — the drift is reported, never
// published as desired.
func TestReportDriftRecordsProgressAndKicks(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprint, templateID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fingerprint, templateID: templateID,
			phase: state.DeploymentAppPending})
	c := New(env.cfg)
	if _, err := c.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}

	c.ReportDrift("fn", "content drifted")

	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var fnRow *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "fn" {
			fnRow = &rows[i]
		}
	}
	if fnRow == nil || fnRow.Phase != state.DeploymentAppDrifted ||
		fnRow.Error != "content drifted" {
		t.Fatalf("fn row = %+v, want phase drifted with the reason", fnRow)
	}
	select {
	case <-c.kick:
	default:
		t.Fatal("ReportDrift did not request an immediate re-materialization")
	}
}

// TestCheckDriftKicksOnlyOnLocalChange pins the periodic drift backstop: a
// tree that still matches the authorized fingerprints requests nothing, and a
// local edit triggers exactly the immediate re-materialization request.
func TestCheckDriftKicksOnlyOnLocalChange(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprint, templateID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fingerprint, templateID: templateID,
			phase: state.DeploymentAppPending})
	c := New(env.cfg)
	if _, err := c.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	ctx := context.Background()

	// The live tree matches the authorization: no rematerialization request.
	c.checkDrift(ctx)
	select {
	case <-c.kick:
		t.Fatal("checkDrift requested rematerialization for a matching tree")
	default:
	}

	// A local edit diverges from the authorized fingerprints.
	indexPath := filepath.Join(env.cfg.AppsDir, "fn", "index.js")
	if err := os.WriteFile(indexPath,
		[]byte("export function hi(e, context){ /* drifted */ }\n"), 0o644); err != nil {
		t.Fatalf("edit app: %v", err)
	}
	c.checkDrift(ctx)
	select {
	case <-c.kick:
	default:
		t.Fatal("checkDrift did not request rematerialization after a local edit")
	}
}

// TestSyncOnceRetriesUnchangedGeneration pins the retry bypass: after a failed
// attempt whose APPLIED generation already equals the shared desired revision,
// the unchanged-generation fast path must not suppress the next attempt — the
// coordinator retries (and reports the failure again) instead of silently
// staying failed.
func TestSyncOnceRetriesUnchangedGeneration(t *testing.T) {
	env := newCoordinatorEnv(t)
	// An unreadable persisted source fails every attempt before any network work.
	if err := os.WriteFile(env.cfg.GitConfigPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write broken config: %v", err)
	}
	publishDesired(t, env.st, commitOne, "main")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: "fp-1", templateID: "tid-1",
			phase: state.DeploymentAppPending})
	ctx := context.Background()

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Error == nil {
		t.Fatal("precondition: the startup attempt must fail against the broken config")
	}
	if !strings.Contains(env.log.String(), "startup materialization failed") {
		t.Fatalf("startup failure was not reported:\n%s", env.log.String())
	}

	// desired == applied (generation and commit): without the retry bypass the
	// fast path would swallow this pass and log nothing.
	c.syncOnce(ctx, func(string) {}, false)
	if !strings.Contains(env.log.String(),
		"materialization failed; retaining active generation") {
		t.Fatalf("syncOnce did not retry the unchanged generation:\n%s", env.log.String())
	}
	node, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.Phase != state.DeploymentNodeFailed || node.AppliedGeneration != 1 {
		t.Fatalf("node = %+v, want a failed retry that keeps generation 1 applied", node)
	}
}

// TestKickCoalescesAndIsNilSafe pins the kick channel contract: repeated kicks
// before the loop drains the channel coalesce into one pending request, and a
// nil coordinator never panics.
func TestKickCoalescesAndIsNilSafe(t *testing.T) {
	var nilCoordinator *Coordinator
	nilCoordinator.Kick()
	nilCoordinator.ReportDrift("fn", "reason")

	c := New(Config{})
	c.Kick()
	c.Kick()
	select {
	case <-c.kick:
	default:
		t.Fatal("Kick did not enqueue a rematerialization request")
	}
	select {
	case <-c.kick:
		t.Fatal("repeated kicks before a drain must coalesce into one request")
	default:
	}
}

// TestObserveStartupNilStateErrors pins the configuration contract: a
// coordinator without state cannot observe anything and reports it instead of
// pretending nothing was published.
func TestObserveStartupNilStateErrors(t *testing.T) {
	c := New(Config{NodeID: "node-1"})
	if _, err := c.ObserveStartup(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "state is not configured") {
		t.Fatalf("ObserveStartup = %v, want the missing-state configuration error", err)
	}
	// The nil-receiver startup path stays total.
	var nilCoordinator *Coordinator
	if _, err := nilCoordinator.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("nil coordinator ObserveStartup = %v, want a no-op", err)
	}
}

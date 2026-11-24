//go:build integration

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/git"
	"relay/internal/state"
)

// TestObserveStartupKeepsFailedAppWithFingerprintAuthorized pins the startup
// recovery split for failed per-app rows: a row that failed ACTIVATION (build
// or service failure) but carries the materialized content fingerprint stays
// AUTHORIZED — startup retains it so the normal prepare path installs the
// unavailable matched placeholder instead of ACKing events unmatched — while a
// failed row with an EMPTY fingerprint (materialized but unloadable) stays in
// Invalid and is never authorized to activate.
func TestObserveStartupKeepsFailedAppWithFingerprintAuthorized(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 2, commitTwo,
		seededApp{name: "build-fail", fingerprint: "fp-bf", templateID: "tid-bf",
			phase: state.DeploymentAppFailed, errMessage: "build failed"},
		seededApp{name: "unloadable", fingerprint: "", templateID: "tid-x",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
		seededApp{name: "ok", fingerprint: "fp-ok", templateID: "tid-ok",
			phase: state.DeploymentAppPending},
	)

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if !snapshot.Authoritative || snapshot.Generation != 2 {
		t.Fatalf("snapshot = %+v, want the recovered generation 2 authorization", snapshot)
	}

	// The activation failure with a materialized fingerprint is authorized.
	if snapshot.Fingerprints["build-fail"] != "fp-bf" {
		t.Fatalf("fingerprints = %v, want build-fail retained with fp-bf", snapshot.Fingerprints)
	}
	if snapshot.TemplateIdentities["build-fail"] != "tid-bf" {
		t.Fatalf("template identities = %v, want the build-fail identity",
			snapshot.TemplateIdentities)
	}
	if _, ok := snapshot.Invalid["build-fail"]; ok {
		t.Fatalf("invalid = %v, want build-fail OUT of Invalid (it is a "+
			"build/service failure, not an unloadable materialization)", snapshot.Invalid)
	}
	if len(snapshot.Removed) != 0 {
		t.Fatalf("removed = %v, want none (a failed app is not a removal)", snapshot.Removed)
	}

	// The unloadable materialization stays invalid and is never authorized.
	if snapshot.Invalid["unloadable"] != "template invalid" {
		t.Fatalf("invalid = %v, want the unloadable diagnostic", snapshot.Invalid)
	}
	if snapshot.InvalidTemplates["unloadable"] != "tid-x" {
		t.Fatalf("invalid templates = %v, want the materialized invalid identity",
			snapshot.InvalidTemplates)
	}
	if _, ok := snapshot.Fingerprints["unloadable"]; ok {
		t.Fatalf("fingerprints = %v, want unloadable excluded", snapshot.Fingerprints)
	}

	// A healthy pending app round-trips as authorized.
	if snapshot.Fingerprints["ok"] != "fp-ok" {
		t.Fatalf("fingerprints = %v, want ok authorized", snapshot.Fingerprints)
	}

	// The recovered authorization is usable: activation progress for the
	// failed-but-authorized app lands against the applied generation.
	c.ReportApp("build-fail", state.DeploymentAppActivating, "")
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 2)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var row *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "build-fail" {
			row = &rows[i]
		}
	}
	if row == nil || row.Phase != state.DeploymentAppActivating ||
		row.Fingerprint != "fp-bf" {
		t.Fatalf("build-fail row = %+v, want phase activating with fp-bf", row)
	}
	// And nothing wrote the apps tree: recovery is read-only.
	entries, err := os.ReadDir(env.cfg.AppsDir)
	if err != nil {
		t.Fatalf("read apps dir: %v", err)
	}
	for _, e := range entries {
		t.Fatalf("apps dir entry %q: startup recovery must not touch the tree", e.Name())
	}
}

// TestMaterializationAbortRetainsPreviousAndDoesNotPrune pins the abort
// contract when an attempt fails before authorization (here: the transport
// cannot authenticate, so no fetch runs): the attempt fails closed, the
// previous applied generation stays authorized and its per-app progress rows
// are NOT pruned, the node records a failed attempt against the new
// generation while keeping the previous applied generation, and the failing
// generation is retried — never recorded as a valid empty deployment.
func TestMaterializationAbortRetainsPreviousAndDoesNotPrune(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: "fp-1", templateID: "tid-1",
			phase: state.DeploymentAppPending})
	// Two publications so the observed generation (2) differs from the applied
	// one (1) in the assertions below.
	publishDesired(t, env.st, commitOne, "main")
	publishDesired(t, env.st, commitTwo, "main")

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Error == nil ||
		!strings.Contains(snapshot.Error.Error(), "materialize commit") {
		t.Fatalf("snapshot.Error = %v, want the aborted-materialization diagnosis",
			snapshot.Error)
	}

	// The previous generation remains the authorization — never an empty set.
	if !snapshot.Authoritative || snapshot.Generation != 1 ||
		snapshot.Commit != commitOne {
		t.Fatalf("snapshot = %+v, want the previous generation 1 still authoritative",
			snapshot)
	}
	if snapshot.Fingerprints["fn"] != "fp-1" {
		t.Fatalf("fingerprints = %v, want the previous fn authorization intact",
			snapshot.Fingerprints)
	}
	if len(snapshot.Removed) != 0 {
		t.Fatalf("removed = %v, want none (the abort must not classify removals)",
			snapshot.Removed)
	}

	// Node progress: failed attempt on the NEW generation, previous applied
	// generation preserved.
	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.Phase != state.DeploymentNodeFailed || node.Generation != 2 ||
		node.AppliedGeneration != 1 || node.AppliedCommit != commitOne {
		t.Fatalf("node = %+v, want a failed generation-2 attempt still applying "+
			"generation 1", node)
	}

	// The previous generation's per-app rows were NOT pruned by the abort.
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	if len(rows) != 1 || rows[0].App != "fn" || rows[0].Fingerprint != "fp-1" {
		t.Fatalf("rows = %+v, want the previous fn row retained (not pruned)", rows)
	}
	// And the aborted attempt wrote NO per-app progress of its own.
	all, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress(all): %v", err)
	}
	for _, row := range all {
		if row.Generation != 1 {
			t.Fatalf("row = %+v, want no per-app progress for the aborted generation",
				row)
		}
	}

	// The failing generation is retried: the background pass attempts it again
	// (and reports the failure again) instead of going quiet.
	c.syncOnce(context.Background(), func(string) {}, false)
	if !strings.Contains(env.log.String(),
		"materialization failed; retaining active generation") {
		t.Fatalf("retry attempt was not reported:\n%s", env.log.String())
	}
}

// TestStartEnqueuesInvalidAppsWhenGenerationUnchanged pins the M1 startup
// catch-up: when the shared desired revision already equals the applied
// generation (the initial syncOnce takes the unchanged fast path and runs no
// new materialization), Start still immediately enqueues the recovered
// INVALID app names so their pending rules are published without waiting for
// the drift ticker or a filesystem event — while already-valid apps are NOT
// needlessly re-enqueued by the catch-up.
func TestStartEnqueuesInvalidAppsWhenGenerationUnchanged(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	// Desired equals the APPLIED generation and commit: the first syncOnce can
	// only take the unchanged fast path.
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "ok", fingerprint: "fp-ok", templateID: "tid-ok",
			phase: state.DeploymentAppPending},
		seededApp{name: "broken", fingerprint: "", templateID: "",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
	)
	publishDesired(t, env.st, commitOne, "main")

	// Long intervals: only the initial pass can enqueue within this test.
	env.cfg.PollInterval = time.Hour
	env.cfg.DriftInterval = time.Hour
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if !snapshot.Authoritative || snapshot.Invalid["broken"] == "" ||
		snapshot.Fingerprints["ok"] != "fp-ok" {
		t.Fatalf("snapshot = %+v, want the recovered valid and invalid authorizations",
			snapshot)
	}

	ctx, cancel := context.WithCancel(context.Background())
	enqueued := make(chan string, 8)
	done := c.Start(ctx, nil, func(name string) { enqueued <- name })

	select {
	case name := <-enqueued:
		if name != "broken" {
			t.Fatalf("first enqueued = %q, want the invalid app %q", name, "broken")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start never enqueued the recovered invalid app on the " +
			"unchanged-generation initial pass")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not stop after cancellation")
	}

	// The catch-up is invalid-only: no valid app (and no duplicate invalid
	// entry) may arrive after the initial enqueue.
	select {
	case name := <-enqueued:
		t.Fatalf("extra enqueue %q: the unchanged-generation catch-up must not "+
			"needlessly re-enqueue valid apps", name)
	default:
	}
}

// TestLoadAppliedDoesNotAuthorizeEmptyFingerprintRows pins the R1 read-back
// contract: a persisted progress row with an EMPTY fingerprint in a default/
// active/activating/degraded phase (for example a drifted local-only app) must
// NOT become authorization after a restart — the reconciler would otherwise
// compare local bytes against "" and accept anything. Failed-empty rows remain
// Invalid, failed rows with a materialized fingerprint remain authorized, and
// Removed rows remain removals.
func TestLoadAppliedDoesNotAuthorizeEmptyFingerprintRows(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		// Empty-fingerprint rows across every authorization-relevant phase.
		seededApp{name: "drifted-local", phase: state.DeploymentAppDrifted},
		seededApp{name: "active-empty", phase: state.DeploymentAppActive},
		seededApp{name: "activating-empty", phase: state.DeploymentAppActivating},
		seededApp{name: "degraded-empty", phase: state.DeploymentAppDegraded},
		seededApp{name: "pending-empty", phase: state.DeploymentAppPending},
		// Controls across the row classes that keep their meaning.
		seededApp{name: "failed-empty", phase: state.DeploymentAppFailed,
			errMessage: "template invalid"},
		seededApp{name: "failed-fp", fingerprint: "fp-bf", templateID: "tid-bf",
			phase: state.DeploymentAppFailed, errMessage: "build failed"},
		seededApp{name: "ok", fingerprint: "fp-ok", templateID: "tid-ok",
			phase: state.DeploymentAppPending},
		seededApp{name: "gone", phase: state.DeploymentAppRemoved},
	)

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if !snapshot.Authoritative || snapshot.Generation != 1 {
		t.Fatalf("snapshot = %+v, want the recovered generation 1 authorization",
			snapshot)
	}

	for _, name := range []string{
		"drifted-local", "active-empty", "activating-empty",
		"degraded-empty", "pending-empty",
	} {
		if fp, ok := snapshot.Fingerprints[name]; ok {
			t.Fatalf("%q was authorized with fingerprint %q: an empty-fingerprint "+
				"row must never grant authorization after restart", name, fp)
		}
	}
	if len(snapshot.Removed) != 1 || snapshot.Removed[0] != "gone" {
		t.Fatalf("removed = %v, want [gone] preserved", snapshot.Removed)
	}
	if snapshot.Invalid["failed-empty"] != "template invalid" {
		t.Fatalf("invalid = %v, want failed-empty retained as Invalid",
			snapshot.Invalid)
	}
	if _, ok := snapshot.Fingerprints["failed-empty"]; ok {
		t.Fatalf("fingerprints = %v, want failed-empty excluded", snapshot.Fingerprints)
	}
	if snapshot.Fingerprints["failed-fp"] != "fp-bf" {
		t.Fatalf("fingerprints = %v, want the fingerprinted failure still authorized",
			snapshot.Fingerprints)
	}
	if _, ok := snapshot.Invalid["failed-fp"]; ok {
		t.Fatalf("invalid = %v, want failed-fp out of Invalid", snapshot.Invalid)
	}
	if snapshot.Fingerprints["ok"] != "fp-ok" {
		t.Fatalf("fingerprints = %v, want the healthy app authorized", snapshot.Fingerprints)
	}
}

// TestReportAppSkipsEmptyFingerprintActivationWrites pins the ReportApp half
// of R1: active/activating/degraded describe AUTHORIZED content, so a
// coordinator whose applied snapshot holds no fingerprint for the name must not
// write them (the row would persist an empty expected identity that startup
// must not authorize). Failure and drift diagnostics for the same name still
// write — they are observability, and the read-back test above keeps them from
// ever granting authorization.
func TestReportAppSkipsEmptyFingerprintActivationWrites(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "ok", fingerprint: "fp-ok", templateID: "tid-ok",
			phase: state.DeploymentAppPending})
	c := New(env.cfg)
	if _, err := c.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}

	// "local-only" is not in the applied fingerprints: these activation-phase
	// writes must be no-ops.
	for _, phase := range []string{
		state.DeploymentAppActive,
		state.DeploymentAppActivating,
		state.DeploymentAppDegraded,
	} {
		c.ReportApp("local-only", phase, "should not persist")
	}
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	for _, row := range rows {
		if row.App == "local-only" {
			t.Fatalf("ReportApp persisted %+v: an empty-fingerprint %q write must "+
				"be a no-op", row, row.Phase)
		}
	}

	// Control: an authorized name with a materialized fingerprint still writes.
	c.ReportApp("ok", state.DeploymentAppActive, "")
	rows, err = env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var okRow *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "ok" {
			okRow = &rows[i]
		}
	}
	if okRow == nil || okRow.Phase != state.DeploymentAppActive ||
		okRow.Fingerprint != "fp-ok" {
		t.Fatalf("ok row = %+v, want the authorized active write", okRow)
	}

	// Diagnostics for the fingerprint-less name still write — first the failure
	// (its own persisted row), then the drift diagnostic, which upserts the SAME
	// (node, app, generation) row per the table's primary key.
	c.ReportApp("local-only", state.DeploymentAppFailed, "boom")
	rows, err = env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	foundFailed := false
	for _, row := range rows {
		if row.App == "local-only" && row.Phase == state.DeploymentAppFailed &&
			row.Error == "boom" {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Fatalf("rows = %+v, want the failed diagnostic written for local-only", rows)
	}

	c.ReportApp("local-only", state.DeploymentAppDrifted, "content changed")
	rows, err = env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 0)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	foundDrifted := false
	for _, row := range rows {
		if row.App == "local-only" && row.Phase == state.DeploymentAppDrifted &&
			row.Error == "content changed" {
			foundDrifted = true
		}
	}
	if !foundDrifted {
		t.Fatalf("rows = %+v, want the drift diagnostic written for local-only", rows)
	}
}

// TestReportAppPreservesTemplateIdentity pins R3: ReportApp's per-app upsert
// carries the materialization TemplateIdentity from the applied snapshot, so
// activation progress never erases it. After a restart whose desired revision
// fails to fetch, loadApplied therefore reconstructs a NON-EMPTY identity
// alongside the fingerprint — letting the reconciler's identity gate verify an
// unchanged local template instead of reporting false drift.
func TestReportAppPreservesTemplateIdentity(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "gated", fingerprint: "fp-g", templateID: "tid-g",
			phase: state.DeploymentAppPending},
	)

	// First life: activation progress flows through ReportApp — both writes
	// must carry the identity forward instead of upserting it away.
	c := New(env.cfg)
	if _, err := c.ObserveStartup(context.Background()); err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	c.ReportApp("gated", state.DeploymentAppActivating, "")
	c.ReportApp("gated", state.DeploymentAppActive, "")

	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var row *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "gated" {
			row = &rows[i]
		}
	}
	if row == nil || row.Phase != state.DeploymentAppActive {
		t.Fatalf("row = %+v, want the active progress report", row)
	}
	if row.TemplateIdentity != "tid-g" {
		t.Fatalf("TemplateIdentity = %q, want the materialized identity %q "+
			"preserved across ReportApp upserts", row.TemplateIdentity, "tid-g")
	}
	if row.Fingerprint != "fp-g" {
		t.Fatalf("Fingerprint = %q, want fp-g preserved", row.Fingerprint)
	}

	// Restart with a shared desired revision that cannot be fetched: the
	// attempt fails (pre-network, SSH auth) and the previous authorization is
	// returned — its identity must have survived the ReportApp round trip.
	publishDesired(t, env.st, commitTwo, "main")
	restarted := New(env.cfg)
	snapshot, err := restarted.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup after restart: %v", err)
	}
	if snapshot.Error == nil {
		t.Fatal("precondition: the restart fetch must fail so the recovered " +
			"snapshot comes from loadApplied")
	}
	if !snapshot.Authoritative || snapshot.Generation != 1 {
		t.Fatalf("snapshot = %+v, want the previous generation 1 still authoritative",
			snapshot)
	}
	if snapshot.Fingerprints["gated"] != "fp-g" {
		t.Fatalf("fingerprints = %v, want the fingerprint reconstructed",
			snapshot.Fingerprints)
	}
	if snapshot.TemplateIdentities["gated"] != "tid-g" {
		t.Fatalf("template identities = %v, want the NON-EMPTY identity %q "+
			"reconstructed — an empty one makes the reconciler report false "+
			"drift for an unchanged local template after a restart/fetch failure",
			snapshot.TemplateIdentities, "tid-g")
	}
}

// TestReportAppPreservesInvalidTemplateIdentity pins the final Low residual:
// an invalid app's materialized identity lives in Snapshot.InvalidTemplates
// (not TemplateIdentities), so ReportApp's failed diagnostics — which write
// with an empty fingerprint — must fall back to it instead of upserting "".
// A stale Drifted report for the same desired-invalid app is skipped entirely
// by the coordinator's narrow guard (see
// TestReportAppSkipsStaleDriftedOverwriteForDesiredInvalid). Restart recovery
// then still reconstructs InvalidTemplates from the row and can never
// misclassify an unchanged invalid app as local drift during a fetch outage.
func TestReportAppPreservesInvalidTemplateIdentity(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "broken", fingerprint: "", templateID: "tid-x",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
	)

	// First life: reconstruction puts the identity in InvalidTemplates (the
	// precondition the ReportApp fallback reads), then the failure diagnostic
	// flows through ReportApp and must not erase it.
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.InvalidTemplates["broken"] != "tid-x" {
		t.Fatalf("precondition: invalid templates = %v, want tid-x", snapshot.InvalidTemplates)
	}
	c.ReportApp("broken", state.DeploymentAppFailed, "template invalid")

	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	var row *state.DeploymentAppProgress
	for i := range rows {
		if rows[i].App == "broken" {
			row = &rows[i]
		}
	}
	if row == nil || row.Phase != state.DeploymentAppFailed {
		t.Fatalf("row = %+v, want the failed diagnostic", row)
	}
	if row.TemplateIdentity != "tid-x" {
		t.Fatalf("TemplateIdentity = %q, want the invalid-app identity %q preserved "+
			"across the ReportApp upsert", row.TemplateIdentity, "tid-x")
	}
	if row.Fingerprint != "" {
		t.Fatalf("Fingerprint = %q, want empty (invalid materializations have none)",
			row.Fingerprint)
	}

	// Restart recovery with no desired row: loadApplied must reconstruct BOTH
	// the diagnostic and the identity from the persisted row.
	restarted := New(env.cfg)
	recovered, err := restarted.ObserveStartup(context.Background())
	if err != nil {
		t.Fatalf("ObserveStartup after restart: %v", err)
	}
	if recovered.Invalid["broken"] != "template invalid" {
		t.Fatalf("invalid = %v, want the diagnostic reconstructed", recovered.Invalid)
	}
	if recovered.InvalidTemplates["broken"] != "tid-x" {
		t.Fatalf("invalid templates = %v, want tid-x reconstructed after restart — "+
			"an empty identity makes the reconciler misclassify the unchanged app "+
			"as local drift during a fetch outage", recovered.InvalidTemplates)
	}

	// A STALE Drifted diagnostic for this desired-invalid app is skipped by
	// the coordinator's narrow guard: the Failed row and its identity must
	// survive rather than being replaced by an empty-fingerprint Drifted row
	// that loadApplied would drop after the next restart.
	restarted.ReportApp("broken", state.DeploymentAppDrifted, "content changed")
	rows, err = env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	row = nil
	for i := range rows {
		if rows[i].App == "broken" {
			row = &rows[i]
		}
	}
	if row == nil || row.Phase != state.DeploymentAppFailed ||
		row.Error != "template invalid" || row.TemplateIdentity != "tid-x" {
		t.Fatalf("row = %+v, want the Failed phase, diagnostic, and tid-x preserved "+
			"across a stale Drifted report", row)
	}
}

// TestResolveUncertainAppliedAdoptsEqualGenerationReplacement pins the
// same-generation half of ambiguous-commit recovery: when a re-materialization
// of the ALREADY applied generation landed without its acknowledgement, the
// persisted row set is the complete authorization that transaction committed,
// while the in-memory snapshot may still hold the previous (for example legacy
// partial) authorization for the SAME generation. The uncertain path must adopt
// the persisted snapshot rather than keep the stale set, and a later
// same-generation failure report must preserve the committed applied generation,
// commit, and rows instead of reintroducing the stale authorization.
func TestResolveUncertainAppliedAdoptsEqualGenerationReplacement(t *testing.T) {
	env := newCoordinatorEnv(t)
	ctx := context.Background()

	// Legacy state: generation 1 is applied with an INCOMPLETE authorization
	// ("kept" only), as an earlier non-atomic writer could leave behind.
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "kept", fingerprint: "fp-kept", templateID: "tid-kept",
			phase: state.DeploymentAppPending})

	acquired, err := env.st.AcquireDeploymentNode(ctx, env.cfg.NodeID,
		env.cfg.InstanceID, 1, state.DeploymentNodeProgress{
			NodeID: env.cfg.NodeID, Generation: 1,
			Phase: state.DeploymentNodeMaterializing, Commit: commitOne,
		})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// The same-generation replacement whose commit landed before the client lost
	// the acknowledgement: the complete authorization for generation 1.
	landed, err := env.st.CommitDeploymentMaterialization(ctx,
		state.DeploymentMaterialization{
			NodeID:      env.cfg.NodeID,
			Generation:  1,
			Instance:    env.cfg.InstanceID,
			Incarnation: acquired.Incarnation,
			Commit:      commitOne,
			Apps: []state.DeploymentAppAuthorization{
				{App: "kept", Phase: state.DeploymentAppPending,
					Fingerprint: "fp-kept", TemplateIdentity: "tid-kept"},
				{App: "repaired", Phase: state.DeploymentAppPending,
					Fingerprint: "fp-repaired", TemplateIdentity: "tid-repaired"},
			},
		})
	if err != nil || !landed {
		t.Fatalf("commit same-generation replacement = (%v, %v)", landed, err)
	}

	c := New(env.cfg)
	stale := Snapshot{
		Generation:         1,
		Commit:             commitOne,
		Fingerprints:       map[string]string{"kept": "fp-kept"},
		TemplateIdentities: map[string]string{"kept": "tid-kept"},
		Invalid:            map[string]string{},
		InvalidTemplates:   map[string]string{},
		Authoritative:      true,
	}
	c.setApplied(stale)
	c.setAppliedIncarnation(acquired.Incarnation)
	c.setAppliedUncertain(true)

	recovered := stale
	resolved, err := c.resolveUncertainApplied(ctx, &recovered)
	if err != nil {
		t.Fatalf("resolveUncertainApplied: %v", err)
	}
	if !resolved {
		t.Fatal("resolveUncertainApplied reported no persisted re-read after an " +
			"uncertain outcome")
	}
	if recovered.Fingerprints["kept"] != "fp-kept" ||
		recovered.Fingerprints["repaired"] != "fp-repaired" {
		t.Fatalf("recovered = %+v, want the complete persisted same-generation "+
			"authorization adopted", recovered)
	}
	c.mu.Lock()
	inMemory := c.applied
	c.mu.Unlock()
	if inMemory.Fingerprints["repaired"] != "fp-repaired" {
		t.Fatalf("in-memory applied = %+v, want the repaired same-generation "+
			"authorization in place of the stale set", inMemory)
	}

	// A retry of the same generation that cannot materialize reports an observed
	// failure only: the committed applied generation, commit, and complete rows
	// survive, and the stale set is not written back anywhere.
	desired := state.DeploymentDesired{
		Generation: 1, Commit: commitOne, Repository: deployRepo, Ref: "main",
	}
	if _, err := c.materialize(ctx, recovered, desired); err == nil {
		t.Fatal("materialize without a local Git source = nil, want a reported failure")
	}
	node, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.AppliedGeneration != 1 || node.AppliedCommit != commitOne ||
		node.Phase != state.DeploymentNodeFailed {
		t.Fatalf("node = %+v, want a failed retry still applying generation 1 (%s)",
			node, commitOne)
	}
	rows, err := env.st.ReadDeploymentAppProgress(ctx, env.cfg.NodeID, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want the complete committed authorization retained", rows)
	}
}

// seedSameGenerationReplacement plants the state of a same-generation
// re-materialization that committed without its acknowledgement: the applied
// generation carries the COMPLETE authorization, while the caller passes the
// stale in-memory snapshot that predates it. It returns the committed snapshot's
// fingerprints/identities and the incarnation the commit was fenced by.
func seedSameGenerationReplacement(
	t *testing.T, env *coordinatorEnv,
) (fingerprints, templateIdentities map[string]string) {
	t.Helper()
	ctx := context.Background()
	fingerprints = map[string]string{}
	templateIdentities = map[string]string{}
	for _, name := range []string{"kept", "repaired"} {
		fp, tid := writeCoordinatorApp(t, env.cfg.AppsDir, name)
		fingerprints[name], templateIdentities[name] = fp, tid
	}

	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "kept", fingerprint: fingerprints["kept"],
			templateID: templateIdentities["kept"], phase: state.DeploymentAppPending})
	acquired, err := env.st.AcquireDeploymentNode(ctx, env.cfg.NodeID,
		env.cfg.InstanceID, 1, state.DeploymentNodeProgress{
			NodeID: env.cfg.NodeID, Generation: 1,
			Phase: state.DeploymentNodeMaterializing, Commit: commitOne,
		})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	apps := make([]state.DeploymentAppAuthorization, 0, len(fingerprints))
	for name, fingerprint := range fingerprints {
		apps = append(apps, state.DeploymentAppAuthorization{
			App: name, Phase: state.DeploymentAppPending,
			Fingerprint: fingerprint, TemplateIdentity: templateIdentities[name],
		})
	}
	landed, err := env.st.CommitDeploymentMaterialization(ctx,
		state.DeploymentMaterialization{
			NodeID:      env.cfg.NodeID,
			Generation:  1,
			Instance:    env.cfg.InstanceID,
			Incarnation: acquired.Incarnation,
			Commit:      commitOne,
			Apps:        apps,
		})
	if err != nil || !landed {
		t.Fatalf("commit same-generation replacement = (%v, %v)", landed, err)
	}
	publishDesired(t, env.st, commitOne, "main")
	return fingerprints, templateIdentities
}

// staleCoordinator mirrors the running coordinator after an ambiguous
// same-generation commit: it still holds the older authorization for the applied
// generation, the retry of that attempt is armed, and the outcome is uncertain.
func staleCoordinator(
	env *coordinatorEnv,
	fingerprints, templateIdentities map[string]string,
) *Coordinator {
	env.cfg.RetryInterval = time.Hour
	env.cfg.PollInterval = time.Hour
	c := New(env.cfg)
	c.setApplied(Snapshot{
		Generation:         1,
		Commit:             commitOne,
		Fingerprints:       map[string]string{"kept": fingerprints["kept"]},
		TemplateIdentities: map[string]string{"kept": templateIdentities["kept"]},
		Invalid:            map[string]string{},
		InvalidTemplates:   map[string]string{},
		Authoritative:      true,
	})
	c.setAppliedUncertain(true)
	c.mu.Lock()
	c.retryGeneration = 1
	c.retryCommit = commitOne
	c.mu.Unlock()
	return c
}

// TestMaterializeConfirmsAmbiguousSameGenerationCommit pins the live retry path
// after an ambiguous same-generation commit: when the persisted applied
// generation/commit equals the requested desired revision and the local /apps tree
// still matches its authorization, materialize confirms that persisted snapshot as
// a success without any Git work. syncOnce then publishes the confirmed
// authorization to the reconciler and enqueues its apps instead of leaving the
// reconciler on the stale set while a fetch keeps failing. No Git source is
// configured, so any fall-through to the normal path would fail materialization.
func TestMaterializeConfirmsAmbiguousSameGenerationCommit(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprints, templateIdentities := seedSameGenerationReplacement(t, env)
	c := staleCoordinator(env, fingerprints, templateIdentities)
	c.setAppliedIncarnation(1) // any positive fence; the short-circuit never acquires

	var notified Snapshot
	c.mu.Lock()
	c.onMaterialized = func(snapshot Snapshot) { notified = snapshot }
	c.mu.Unlock()
	enqueued := make(chan string, 8)
	c.syncOnce(context.Background(), func(name string) { enqueued <- name }, false)

	if notified.Generation != 1 ||
		notified.Fingerprints["kept"] != fingerprints["kept"] ||
		notified.Fingerprints["repaired"] != fingerprints["repaired"] {
		t.Fatalf("notified = %+v, want the confirmed persisted authorization "+
			"(kept=%q repaired=%q)", notified,
			fingerprints["kept"], fingerprints["repaired"])
	}
	names := map[string]bool{}
	for len(enqueued) > 0 {
		names[<-enqueued] = true
	}
	if !names["kept"] || !names["repaired"] {
		t.Fatalf("enqueued = %v, want both confirmed apps", names)
	}
	if !strings.Contains(env.log.String(),
		"confirmed materialized revision after an ambiguous commit") {
		t.Fatalf("confirmation was not reported:\n%s", env.log.String())
	}

	// The persisted authorization is unchanged by the confirmation.
	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.AppliedGeneration != 1 || node.AppliedCommit != commitOne {
		t.Fatalf("node = %+v, want applied generation 1 (%s) preserved", node, commitOne)
	}
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows = (%+v, %v), want the complete committed authorization", rows, err)
	}
}

// TestMaterializeDoesNotPublishUnprovenSameGenerationCommit pins the other side:
// a confirmed same-generation commit is NOT published when the local tree can no
// longer be proven to match it (here an authorized app is missing). The retry
// falls through to the normal path, the reconciler is not notified, and the
// persisted applied generation, commit, and rows are left untouched.
func TestMaterializeDoesNotPublishUnprovenSameGenerationCommit(t *testing.T) {
	env := newCoordinatorEnv(t)
	fingerprints, templateIdentities := seedSameGenerationReplacement(t, env)
	if err := os.RemoveAll(filepath.Join(env.cfg.AppsDir, "repaired")); err != nil {
		t.Fatalf("remove authorized app: %v", err)
	}
	c := staleCoordinator(env, fingerprints, templateIdentities)
	c.setAppliedIncarnation(1)

	notified := false
	c.mu.Lock()
	c.onMaterialized = func(Snapshot) { notified = true }
	c.mu.Unlock()
	enqueued := make(chan string, 8)
	c.syncOnce(context.Background(), func(name string) { enqueued <- name }, false)

	if notified {
		t.Fatal("an unproven same-generation authorization was published")
	}
	if len(enqueued) != 0 {
		t.Fatalf("enqueued %d apps, want none for an unproven authorization",
			len(enqueued))
	}
	if !strings.Contains(env.log.String(),
		"materialization failed; retaining active generation") {
		t.Fatalf("fall-through failure was not reported:\n%s", env.log.String())
	}
	node, found, err := env.st.ReadDeploymentNode(context.Background(), env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.AppliedGeneration != 1 || node.AppliedCommit != commitOne {
		t.Fatalf("node = %+v, want applied generation 1 (%s) preserved", node, commitOne)
	}
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, 1)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows = (%+v, %v), want the complete committed authorization", rows, err)
	}
}

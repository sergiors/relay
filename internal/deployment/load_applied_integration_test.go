//go:build integration

package deployment

import (
	"context"
	"os"
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
// (not TemplateIdentities), so ReportApp's failed/drift diagnostics — which
// write with an empty fingerprint — must fall back to it instead of upserting
// "". Restart recovery then still reconstructs InvalidTemplates from the row
// and can never misclassify an unchanged invalid app as local drift during a
// fetch outage.
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

	// A drift diagnostic from the recovered coordinator preserves it too.
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
	if row == nil || row.Phase != state.DeploymentAppDrifted ||
		row.TemplateIdentity != "tid-x" {
		t.Fatalf("row = %+v, want the drift diagnostic preserving tid-x", row)
	}
}

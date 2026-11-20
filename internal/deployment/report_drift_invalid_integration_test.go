//go:build integration

package deployment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// noopBuildBuilder satisfies the reconciler's Builder seam for tests that
// never reach a build (drift is decided before preparation).
type noopBuildBuilder struct{}

func (noopBuildBuilder) Prepare(_ context.Context, _ app.App) (*runtime.Prepared, error) {
	return &runtime.Prepared{}, nil
}

func (noopBuildBuilder) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

// TestInvalidIdentityDriftPreservesFailedRowAcrossRestart pins the crash/restart
// hole end to end through the REAL callback chain (Reconciler DeploymentProgress
// → Coordinator.ReportApp, DeploymentDrift → Coordinator.ReportDrift): a local
// invalid identity drift on a desired-invalid app requests the restore (Kick)
// but leaves the persisted Failed row with its T0 materialized identity
// untouched, while valid and local-only drift keep writing Drifted. Deleting the
// app directory and restarting then still reconstructs the desired-invalid
// authorization from the preserved row.
func TestInvalidIdentityDriftPreservesFailedRowAcrossRestart(t *testing.T) {
	env := newCoordinatorEnv(t)
	fnFP, fnTID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	badDir := filepath.Join(env.cfg.AppsDir, "bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatalf("mkdir bad: %v", err)
	}
	// Local T1 bytes: invalid content whose identity hashes differently from
	// the recorded T0 materialized identity.
	if err := os.WriteFile(filepath.Join(badDir, "template.yaml"),
		[]byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write local invalid template: %v", err)
	}
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fnFP, templateID: fnTID,
			phase: state.DeploymentAppPending},
		seededApp{name: "bad", fingerprint: "", templateID: "sha256:t0",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
	)
	ctx := context.Background()

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Invalid["bad"] != "template invalid" ||
		snapshot.InvalidTemplates["bad"] != "sha256:t0" {
		t.Fatalf("startup snapshot = %+v, want bad as desired-invalid with T0 identity",
			snapshot)
	}

	kicked := func(what string) {
		t.Helper()
		select {
		case <-c.kick:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no Kick requested", what)
		}
	}

	// The real reconciler reports the local invalid identity drift (T1 != T0)
	// through both coordinator seams. reconcileApp is unexported, so the test
	// drives it through the production Enqueue/pump path (a short debounce keeps
	// the wait deterministic; failure bounds are only failure limits).
	rec := reconciler.New(reconciler.Config{
		Root:               env.cfg.AppsDir,
		Debounce:           5 * time.Millisecond,
		Interval:           time.Hour,
		DeploymentProgress: c.ReportApp,
		DeploymentDrift:    c.ReportDrift,
	}, &runner.Registry{}, noopBuildBuilder{}, testutil.DiscardLogger())
	rec.SetDeploymentAuthorizationWithInvalid(1,
		map[string]string{"fn": fnFP},
		map[string]string{"fn": fnTID},
		map[string]string{"bad": "template invalid"},
		map[string]string{"bad": "sha256:t0"}, nil)

	recCtx, recCancel := context.WithCancel(ctx)
	recDone := make(chan error, 1)
	go func() { recDone <- rec.Start(recCtx) }()
	t.Cleanup(func() {
		recCancel()
		select {
		case <-recDone:
		case <-time.After(5 * time.Second):
			t.Error("reconciler did not stop after cancellation")
		}
	})

	rec.Enqueue("bad")
	kicked("invalid identity drift")

	// The persisted row keeps the Failed phase, the materialization
	// diagnostic, and the T0 identity — no empty-fingerprint Drifted overwrite.
	badRow := findAppRow(t, env, 1, "bad")
	if badRow.Phase != state.DeploymentAppFailed {
		t.Fatalf("bad row = %+v, want the Failed phase preserved", badRow)
	}
	if !strings.Contains(badRow.Error, "template invalid") {
		t.Fatalf("bad row error = %q, want the Failed diagnostic preserved", badRow.Error)
	}
	if badRow.TemplateIdentity != "sha256:t0" {
		t.Fatalf("bad row identity = %q, want the T0 identity preserved",
			badRow.TemplateIdentity)
	}
	if badRow.Fingerprint != "" {
		t.Fatalf("bad row fingerprint = %q, want empty", badRow.Fingerprint)
	}

	// Valid-app drift reporting is unchanged: content drift still writes a
	// Drifted row (with the authorized fingerprint) and Kicks.
	if err := os.WriteFile(filepath.Join(env.cfg.AppsDir, "fn", "index.js"),
		[]byte("export function hi(e, context){ /* local edit */ }\n"), 0o644); err != nil {
		t.Fatalf("edit fn: %v", err)
	}
	rec.Enqueue("fn")
	kicked("valid-app drift")
	fnRow := findAppRow(t, env, 1, "fn")
	if fnRow.Phase != state.DeploymentAppDrifted ||
		fnRow.Error != "materialized content changed locally" {
		t.Fatalf("fn row = %+v, want phase drifted with the content-edit reason", fnRow)
	}
	if fnRow.Fingerprint != fnFP {
		t.Fatalf("fn row fingerprint = %q, want the authorized fingerprint kept", fnFP)
	}

	// Local-only drift reporting is unchanged: the diagnostic is persisted.
	c.ReportDrift("local-only", "not part of the deployment")
	kicked("local-only drift")
	localRow := findAppRow(t, env, 1, "local-only")
	if localRow.Phase != state.DeploymentAppDrifted ||
		localRow.Error != "not part of the deployment" {
		t.Fatalf("local-only row = %+v, want the drift diagnostic written", localRow)
	}

	// The crash hole: the drifted app's directory is gone and the process
	// restarts. The preserved Failed row must still reconstruct the
	// desired-invalid authorization.
	if err := os.RemoveAll(badDir); err != nil {
		t.Fatalf("remove bad: %v", err)
	}
	restarted := New(env.cfg)
	recovered, err := restarted.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup after restart: %v", err)
	}
	if recovered.Invalid["bad"] != "template invalid" {
		t.Fatalf("recovered invalid = %v, want bad retained across drift + restart",
			recovered.Invalid)
	}
	if recovered.InvalidTemplates["bad"] != "sha256:t0" {
		t.Fatalf("recovered invalid templates = %v, want the T0 identity retained",
			recovered.InvalidTemplates)
	}
	if _, ok := recovered.Fingerprints["bad"]; ok {
		t.Fatalf("recovered fingerprints = %v, want bad never authorized", recovered.Fingerprints)
	}
	if recovered.Fingerprints["fn"] != fnFP {
		t.Fatalf("recovered fingerprints = %v, want fn still authorized", recovered.Fingerprints)
	}
}

// TestReportAppSkipsStaleDriftedOverwriteForDesiredInvalid pins the narrow
// ReportApp guard for the authorization-update race: a stale reconciler calling
// phase Drifted directly for a name the coordinator's CURRENT applied snapshot
// classifies as desired-invalid must not replace the persisted Failed row with
// an empty-fingerprint Drifted row, while a legitimate Failed update for the
// same app and Drifted reports for valid apps keep writing.
func TestReportAppSkipsStaleDriftedOverwriteForDesiredInvalid(t *testing.T) {
	env := newCoordinatorEnv(t)
	fnFP, fnTID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fnFP, templateID: fnTID,
			phase: state.DeploymentAppPending},
		seededApp{name: "bad", fingerprint: "", templateID: "sha256:t0",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
	)
	ctx := context.Background()

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if _, ok := snapshot.Invalid["bad"]; !ok {
		t.Fatalf("precondition: snapshot = %+v, want bad classified desired-invalid",
			snapshot)
	}

	// The race: a stale reconciler reports Drifted directly against the
	// coordinator's current applied snapshot. The guard must skip that
	// empty-fingerprint write and preserve the Failed row.
	c.ReportApp("bad", state.DeploymentAppDrifted, "stale drift reason")

	row := findAppRow(t, env, 1, "bad")
	if row.Phase != state.DeploymentAppFailed {
		t.Fatalf("bad row = %+v, want the Failed phase preserved (an empty-fingerprint "+
			"Drifted row would be dropped by loadApplied after restart)", row)
	}
	if row.Error != "template invalid" {
		t.Fatalf("bad row error = %q, want the materialization diagnostic preserved, "+
			"not the stale reason", row.Error)
	}
	if row.TemplateIdentity != "sha256:t0" {
		t.Fatalf("bad row identity = %q, want the T0 identity preserved", row.TemplateIdentity)
	}
	if row.Fingerprint != "" {
		t.Fatalf("bad row fingerprint = %q, want empty", row.Fingerprint)
	}

	// A legitimate Failed update for the same desired-invalid app still writes:
	// the guard is narrow to Drifted.
	c.ReportApp("bad", state.DeploymentAppFailed, "renewed materialization diagnostic")
	row = findAppRow(t, env, 1, "bad")
	if row.Phase != state.DeploymentAppFailed ||
		row.Error != "renewed materialization diagnostic" {
		t.Fatalf("bad row = %+v, want the Failed update persisted", row)
	}
	if row.TemplateIdentity != "sha256:t0" {
		t.Fatalf("bad row identity = %q, want T0 preserved across the Failed update",
			row.TemplateIdentity)
	}

	// Valid Drifted reporting keeps writing with its authorized fingerprint.
	c.ReportApp("fn", state.DeploymentAppDrifted, "content changed")
	fnRow := findAppRow(t, env, 1, "fn")
	if fnRow.Phase != state.DeploymentAppDrifted || fnRow.Error != "content changed" {
		t.Fatalf("fn row = %+v, want the Drifted report persisted", fnRow)
	}
	if fnRow.Fingerprint != fnFP {
		t.Fatalf("fn row fingerprint = %q, want the authorized fingerprint kept", fnFP)
	}
}

// findAppRow returns the persisted progress row for name at generation.
func findAppRow(
	t *testing.T, env *coordinatorEnv, generation int64, name string,
) state.DeploymentAppProgress {
	t.Helper()
	rows, err := env.st.ReadDeploymentAppProgress(
		context.Background(), env.cfg.NodeID, generation)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	for _, row := range rows {
		if row.App == name {
			return row
		}
	}
	t.Fatalf("no progress row for %q at generation %d (rows = %+v)", name, generation, rows)
	return state.DeploymentAppProgress{}
}

// TestReportDriftPreservesDesiredInvalidFailureDiagnostic pins the restart-safe
// drift reporting contract for a desired-invalid app: when the applied snapshot
// already holds the app in Invalid, ReportDrift must NOT overwrite the
// persisted Failed row with an empty-fingerprint Drifted row (loadApplied drops
// such a row on restart, silently losing the desired-invalid authorization) —
// the Failed diagnostic stays intact while the kick still requests
// re-materialization. Valid-app and local-only drift reporting keep their
// previous Drifted-row behavior, and a restart reconstructs Invalid and
// InvalidTemplates from the preserved row.
func TestReportDriftPreservesDesiredInvalidFailureDiagnostic(t *testing.T) {
	env := newCoordinatorEnv(t)
	fnFP, fnTID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fnFP, templateID: fnTID,
			phase: state.DeploymentAppPending},
		seededApp{name: "bad", fingerprint: "", templateID: "tid-bad",
			phase: state.DeploymentAppFailed, errMessage: "template invalid"},
	)
	ctx := context.Background()

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup: %v", err)
	}
	if snapshot.Invalid["bad"] != "template invalid" ||
		snapshot.InvalidTemplates["bad"] != "tid-bad" {
		t.Fatalf("startup snapshot = %+v, want bad recovered as a desired-invalid app",
			snapshot)
	}

	kicked := func(what string) {
		t.Helper()
		select {
		case <-c.kick:
		default:
			t.Fatalf("%s: ReportDrift did not request re-materialization", what)
		}
	}

	// The reconciler's re-verification request for the desired-invalid app:
	// Kick fires, but the persisted Failed row must survive untouched.
	c.ReportDrift("bad", "unverifiable local content")
	kicked("desired-invalid drift report")

	badRow := findAppRow(t, env, 1, "bad")
	if badRow.Phase != state.DeploymentAppFailed {
		t.Fatalf("bad row = %+v, want the Failed phase preserved (an empty-fingerprint "+
			"Drifted row would be dropped by loadApplied after restart)", badRow)
	}
	if !strings.Contains(badRow.Error, "template invalid") {
		t.Fatalf("bad row error = %q, want the materialization diagnostic preserved",
			badRow.Error)
	}
	if badRow.Fingerprint != "" {
		t.Fatalf("bad row fingerprint = %q, want empty (invalid materializations "+
			"have no fingerprint)", badRow.Fingerprint)
	}
	if badRow.TemplateIdentity != "tid-bad" {
		t.Fatalf("bad row identity = %q, want tid-bad preserved", badRow.TemplateIdentity)
	}

	// A valid authorized app keeps the previous behavior: Drifted with reason.
	c.ReportDrift("fn", "content drifted")
	kicked("valid-app drift report")
	fnRow := findAppRow(t, env, 1, "fn")
	if fnRow.Phase != state.DeploymentAppDrifted || fnRow.Error != "content drifted" {
		t.Fatalf("fn row = %+v, want phase drifted with the reason", fnRow)
	}
	if fnRow.Fingerprint != fnFP {
		t.Fatalf("fn row fingerprint = %q, want the authorized fingerprint kept", fnFP)
	}

	// A local-only app keeps the previous behavior: an explicit Drifted
	// diagnostic is recorded even without an authorized fingerprint.
	c.ReportDrift("local-only", "not part of the deployment")
	kicked("local-only drift report")
	localRow := findAppRow(t, env, 1, "local-only")
	if localRow.Phase != state.DeploymentAppDrifted ||
		localRow.Error != "not part of the deployment" {
		t.Fatalf("local-only row = %+v, want the drift diagnostic written", localRow)
	}

	// Restart: loadApplied reconstructs the desired-invalid authorization from
	// the preserved Failed row instead of losing it.
	restarted := New(env.cfg)
	recovered, err := restarted.ObserveStartup(ctx)
	if err != nil {
		t.Fatalf("ObserveStartup after restart: %v", err)
	}
	if recovered.Invalid["bad"] != "template invalid" {
		t.Fatalf("recovered invalid = %v, want the diagnostic retained across the "+
			"drift report and restart", recovered.Invalid)
	}
	if recovered.InvalidTemplates["bad"] != "tid-bad" {
		t.Fatalf("recovered invalid templates = %v, want tid-bad retained", recovered.InvalidTemplates)
	}
	if _, ok := recovered.Fingerprints["bad"]; ok {
		t.Fatalf("recovered fingerprints = %v, want bad never authorized", recovered.Fingerprints)
	}
	if recovered.Fingerprints["fn"] != fnFP {
		t.Fatalf("recovered fingerprints = %v, want fn still authorized", recovered.Fingerprints)
	}
}

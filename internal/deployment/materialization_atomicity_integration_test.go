//go:build integration

package deployment

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/git"
	"relay/internal/state"
)

// The coordinator's persistence stage writes the node's applied generation and
// the COMPLETE per-app authorization in one transaction. These tests pin what a
// database failure inside that transaction means for the coordinator's view of
// what is applied. Every test runs on its own isolated schema (via
// newCoordinatorEnv) and fails rather than skips without PostgreSQL.

// installCoordinatorAppInsertFailure arms a schema-local BEFORE INSERT trigger
// that rejects exactly one named app, so a failure lands AFTER earlier app rows
// of the same transaction were already written and the rollback is decided by
// the database itself. The state package's equivalent helper is not exported;
// the DDL is duplicated here because the fixture schema belongs to this test.
func installCoordinatorAppInsertFailure(t *testing.T, db *sql.DB, app string) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		`CREATE FUNCTION relay_test_fail_deployment_app_insert() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
		 IF NEW.app_name = TG_ARGV[0] THEN
		  RAISE EXCEPTION 'injected deployment app insert failure for %', NEW.app_name;
		 END IF;
		 RETURN NEW;
		 END;
		 $$`,
		`CREATE TRIGGER relay_test_fail_deployment_app_insert
		 BEFORE INSERT ON deployment_apps
		 FOR EACH ROW EXECUTE FUNCTION relay_test_fail_deployment_app_insert(` +
			`'` + strings.ReplaceAll(app, "'", "''") + `')`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("install deployment app insert failure: %v", err)
		}
	}
}

func removeCoordinatorAppInsertFailure(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`DROP TRIGGER IF EXISTS relay_test_fail_deployment_app_insert
		 ON deployment_apps`,
		`DROP FUNCTION IF EXISTS relay_test_fail_deployment_app_insert()`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("remove deployment app insert failure: %v", err)
		}
	}
}

// TestAppProgressPersistenceFailureKeepsPriorGenerationApplied pins the
// atomic persistence boundary the coordinator builds on: a database failure
// while persisting generation 2's COMPLETE app authorization (after Git has
// already changed /apps) must NOT mark the new generation applied — the
// coordinator's own reconstruction still serves generation 1, with no partial
// generation-2 rows and nothing pruned — and once the fault is removed the
// identical retry commits the full valid/invalid/removed set, after which the
// coordinator reconstructs generation 2 as the applied authorization.
//
// The fault is a test-local PostgreSQL trigger in the fixture's schema, so the
// production transaction itself decides the rollback. The persistence stage is
// driven through the SAME state APIs, in the SAME order, that
// Coordinator.materialize uses after Git materialization returns (acquire with
// the still-applied generation carried in the observed progress, then the
// atomic commit): the coordinator exposes no local-transport seam for Git —
// the configured repository must remain the SSH URL — so materialize itself can
// never reach its persistence stage inside this fixture.
func TestAppProgressPersistenceFailureKeepsPriorGenerationApplied(t *testing.T) {
	env := newCoordinatorEnv(t)
	if err := git.SetSource(env.cfg.GitConfigPath, deployRepo, "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	ctx := context.Background()

	// Given: generation 1 applied with a real authorized app; generation 2
	// published as the desired revision.
	fingerprint, templateID := writeCoordinatorApp(t, env.cfg.AppsDir, "fn")
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: fingerprint, templateID: templateID,
			phase: state.DeploymentAppPending})
	publishDesired(t, env.st, commitTwo, "main")

	db, err := sql.Open("pgx", env.dsn)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	installCoordinatorAppInsertFailure(t, db, "boom")

	// When: the persistence stage runs for generation 2 — the acquire carries
	// the still-applied generation exactly as materialize does, and the commit
	// fails after the first target row was already inserted.
	acquired, err := env.st.AcquireDeploymentNode(ctx, env.cfg.NodeID,
		env.cfg.InstanceID, 2, state.DeploymentNodeProgress{
			NodeID:            env.cfg.NodeID,
			Generation:        2,
			Instance:          env.cfg.InstanceID,
			AppliedGeneration: 1,
			AppliedCommit:     commitOne,
			Phase:             state.DeploymentNodeMaterializing,
			Commit:            commitTwo,
		})
	if err != nil {
		t.Fatalf("acquire generation 2: %v", err)
	}
	landed, err := env.st.CommitDeploymentMaterialization(ctx,
		state.DeploymentMaterialization{
			NodeID:      env.cfg.NodeID,
			Generation:  2,
			Instance:    env.cfg.InstanceID,
			Incarnation: acquired.Incarnation,
			Commit:      commitTwo,
			Apps: []state.DeploymentAppAuthorization{
				{App: "fn", Phase: state.DeploymentAppPending,
					Fingerprint: fingerprint, TemplateIdentity: templateID},
				{App: "boom", Phase: state.DeploymentAppFailed,
					TemplateIdentity: "tid-x", Error: "template invalid"},
				{App: "legacy", Phase: state.DeploymentAppRemoved},
			},
		})
	if err == nil || landed {
		t.Fatalf("commit with an injected persistence failure = (%v, %v), want "+
			"an error with nothing landed", landed, err)
	}

	// Then: the failed persistence marked nothing applied — the coordinator's
	// own reconstruction still serves generation 1, no partial generation-2
	// rows exist, and the prior authorization was not pruned.
	c := New(env.cfg)
	recovered, err := c.loadApplied(ctx)
	if err != nil {
		t.Fatalf("loadApplied after the failed persistence: %v", err)
	}
	if !recovered.Authoritative || recovered.Generation != 1 ||
		recovered.Commit != commitOne {
		t.Fatalf("recovered = %+v, want generation 1 (%s) still the applied "+
			"authorization", recovered, commitOne)
	}
	if recovered.Fingerprints["fn"] != fingerprint ||
		recovered.TemplateIdentities["fn"] != templateID {
		t.Fatalf("fingerprints = %v / identities = %v, want the generation-1 fn "+
			"authorization intact", recovered.Fingerprints,
			recovered.TemplateIdentities)
	}
	node, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", node, found, err)
	}
	if node.AppliedGeneration != 1 || node.AppliedCommit != commitOne {
		t.Fatalf("node = %+v, want applied generation 1 (%s) — the failed "+
			"persistence must not mark generation 2 applied", node, commitOne)
	}
	if rows, err := env.st.ReadDeploymentAppProgress(ctx, env.cfg.NodeID, 2); err != nil ||
		len(rows) != 0 {
		t.Fatalf("generation-2 rows = (%+v, %v), want none (no partial target "+
			"rows)", rows, err)
	}
	if rows, err := env.st.ReadDeploymentAppProgress(ctx, env.cfg.NodeID, 1); err != nil ||
		len(rows) != 1 || rows[0].Fingerprint != fingerprint {
		t.Fatalf("generation-1 rows = (%+v, %v), want the prior authorization "+
			"retained", rows, err)
	}

	// When the fault is removed, the identical retry commits the full set and
	// the coordinator reconstructs generation 2 as the applied authorization.
	removeCoordinatorAppInsertFailure(t, db)
	landed, err = env.st.CommitDeploymentMaterialization(ctx,
		state.DeploymentMaterialization{
			NodeID:      env.cfg.NodeID,
			Generation:  2,
			Instance:    env.cfg.InstanceID,
			Incarnation: acquired.Incarnation,
			Commit:      commitTwo,
			Apps: []state.DeploymentAppAuthorization{
				{App: "fn", Phase: state.DeploymentAppPending,
					Fingerprint: fingerprint, TemplateIdentity: templateID},
				{App: "boom", Phase: state.DeploymentAppFailed,
					TemplateIdentity: "tid-x", Error: "template invalid"},
				{App: "legacy", Phase: state.DeploymentAppRemoved},
			},
		})
	if err != nil || !landed {
		t.Fatalf("retry after removing the fault = (%v, %v), want committed",
			landed, err)
	}
	recovered, err = c.loadApplied(ctx)
	if err != nil {
		t.Fatalf("loadApplied after the successful retry: %v", err)
	}
	if !recovered.Authoritative || recovered.Generation != 2 ||
		recovered.Commit != commitTwo {
		t.Fatalf("recovered = %+v, want generation 2 (%s) applied after the "+
			"successful retry", recovered, commitTwo)
	}
	if recovered.Fingerprints["fn"] != fingerprint ||
		recovered.TemplateIdentities["fn"] != templateID {
		t.Fatalf("fingerprints = %v / identities = %v, want the generation-2 fn "+
			"authorization", recovered.Fingerprints, recovered.TemplateIdentities)
	}
	if recovered.Invalid["boom"] != "template invalid" ||
		recovered.InvalidTemplates["boom"] != "tid-x" {
		t.Fatalf("invalid = %v / invalid templates = %v, want the materialized "+
			"boom diagnostic with its identity", recovered.Invalid,
			recovered.InvalidTemplates)
	}
	if len(recovered.Removed) != 1 || recovered.Removed[0] != "legacy" {
		t.Fatalf("removed = %v, want [legacy]", recovered.Removed)
	}
}

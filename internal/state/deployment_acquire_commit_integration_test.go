//go:build integration

package state

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// installDeferredNodeCommitFailure arms a schema-local DEFERRED constraint
// trigger on deployment_nodes that fails the acquisition statement's implicit
// COMMIT with the given SQLSTATE (the autocommit statement's implicit
// transaction reaches commit before QueryRowContext returns). PostgreSQL then
// rolls the whole allocation back — from the server's side this is a KNOWN
// outcome; how the CLIENT must classify the failed commit depends on the
// state it reports:
//   - "P0001" (a raised exception) is an ordinary server-reported failure the
//     production classifier treats as a definite rollback;
//   - "40003" (serialization failure) is classified as an outcome the client
//     cannot prove — ErrDeploymentNodeOutcomeUnknown.
//
// removeDeferredNodeCommitFailure disarms it so a retry in the same test runs
// against the untouched production path.
func installDeferredNodeCommitFailure(t *testing.T, st *State, sqlState string) {
	t.Helper()
	ctx := context.Background()
	function := fmt.Sprintf(
		`CREATE FUNCTION relay_test_fail_deployment_node_commit() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
		 RAISE EXCEPTION 'injected deployment node commit failure'
		   USING ERRCODE = '%s';
		 RETURN NEW;
		 END;
		 $$`, sqlState)
	stmts := []string{
		function,
		`CREATE CONSTRAINT TRIGGER relay_test_fail_deployment_node_commit
		 AFTER INSERT OR UPDATE ON deployment_nodes
		 DEFERRABLE INITIALLY DEFERRED
		 FOR EACH ROW EXECUTE FUNCTION relay_test_fail_deployment_node_commit()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("install deferred deployment node commit failure: %v", err)
		}
	}
}

func removeDeferredNodeCommitFailure(t *testing.T, st *State) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		`DROP TRIGGER IF EXISTS relay_test_fail_deployment_node_commit
		 ON deployment_nodes`,
		`DROP FUNCTION IF EXISTS relay_test_fail_deployment_node_commit()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("remove deferred deployment node commit failure: %v", err)
		}
	}
}

// TestAcquireDeploymentNodeKnownCommitFailureRollsBack pins the DEFINITE
// commit-failure contract of the owner-pinned autocommit acquisition
// statement: a server-reported error from its implicit commit (SQLSTATE
// P0001) is returned WITHOUT the outcome-unknown sentinel — PostgreSQL rolled
// the whole allocation back — proved on BOTH write paths (a first-ever insert
// leaves no row; a generation advance leaves the stored generation and
// incarnation untouched) and by a successful acquisition on the same pinned
// connection once the fault is removed. A sequence value consumed by the
// rolled-back attempt is
// deliberately not asserted: incarnation gaps are allowed, only the row's
// state is authoritative.
func TestAcquireDeploymentNodeKnownCommitFailureRollsBack(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-commit-fail")
	progress := func(generation int64, commit string) DeploymentNodeProgress {
		return DeploymentNodeProgress{
			NodeID: nodeID, Generation: generation,
			Phase: DeploymentNodeMaterializing, Commit: commit,
		}
	}

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")

	// Insert path: the first-ever acquisition cannot commit, so no row may
	// exist afterwards — and the error must classify as a KNOWN failure, not
	// as an outcome the client cannot prove.
	installDeferredNodeCommitFailure(t, owner, "P0001")
	if _, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		progress(1, deployCommitA)); err == nil {
		t.Fatal("acquisition with a failing commit = nil, want an error")
	} else {
		if errors.Is(err, ErrDeploymentNodeOutcomeUnknown) {
			t.Fatalf("definite commit failure error = %v, must not carry "+
				"ErrDeploymentNodeOutcomeUnknown", err)
		}
	}
	if _, found, err := owner.ReadDeploymentNode(ctx, nodeID); err != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row after the "+
			"rolled-back commit", found, err)
	}

	// Update path: baseline a healthy row, then prove a failing commit cannot
	// move the generation or the incarnation.
	removeDeferredNodeCommitFailure(t, owner)
	first, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		progress(1, deployCommitA))
	if err != nil || first.Incarnation <= 0 {
		t.Fatalf("baseline acquisition = (%+v, %v), want a fresh incarnation",
			first, err)
	}
	installDeferredNodeCommitFailure(t, owner, "P0001")
	if _, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 2,
		progress(2, deployCommitB)); err == nil {
		t.Fatal("generation-2 acquisition with a failing commit = nil, want an error")
	}
	after, found, err := owner.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", after, found, err)
	}
	if after.Generation != 1 || after.Incarnation != first.Incarnation ||
		after.AppliedGeneration != 0 || after.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want generation 1 untouched under incarnation %d "+
			"(the failed commit must roll back)", after, first.Incarnation)
	}

	// Recovery: with the fault removed, the SAME pinned connection still
	// allocates — a definite commit failure must not poison the ownership
	// backend — and the generation advance lands exactly once.
	removeDeferredNodeCommitFailure(t, owner)
	acquired, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 2,
		progress(2, deployCommitB))
	if err != nil || acquired.Generation != 2 ||
		acquired.Incarnation <= first.Incarnation {
		t.Fatalf("acquisition after removing the fault = (%+v, %v), want "+
			"generation 2 under a fresh incarnation", acquired, err)
	}
}

// TestAcquireDeploymentNodeSerializationCommitFailureReportsOutcomeUnknown
// pins the AMBIGUOUS-class commit failure: a commit that fails with SQLSTATE
// 40003 — the serialization-failure class the production classifier cannot
// prove the outcome of — is returned wrapped in
// ErrDeploymentNodeOutcomeUnknown, while this test (unlike the caller) proves
// through the row that the allocation in fact rolled back, and the pinned
// connection stays healthy for a later acquisition once the fault is removed.
//
// A true connection loss racing the statement's implicit COMMIT itself cannot
// be simulated deterministically; the 40003 class exercises the same classifier
// branch with a server-observed failure, keeping the test free of sleeps and
// timing windows.
func TestAcquireDeploymentNodeSerializationCommitFailureReportsOutcomeUnknown(
	t *testing.T,
) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-commit-unknown")
	progress := DeploymentNodeProgress{
		NodeID: nodeID, Generation: 1,
		Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
	}

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	installDeferredNodeCommitFailure(t, owner, "40003")
	_, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1, progress)
	if err == nil {
		t.Fatal("acquisition with an unprovable commit outcome = nil, want an error")
	}
	if !errors.Is(err, ErrDeploymentNodeOutcomeUnknown) {
		t.Fatalf("commit failure error = %v, want the wrapped "+
			"ErrDeploymentNodeOutcomeUnknown", err)
	}

	// The caller cannot prove the outcome — but the test can: the row is
	// absent, so the commit was rolled back even though production must treat
	// it as unknown.
	if _, found, rerr := owner.ReadDeploymentNode(ctx, nodeID); rerr != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"rolled-back commit", found, rerr)
	}

	// Recovery: the fault removed, the same pinned connection still acquires.
	removeDeferredNodeCommitFailure(t, owner)
	acquired, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1, progress)
	if err != nil || acquired.Incarnation <= 0 {
		t.Fatalf("acquisition after removing the fault = (%+v, %v), want success "+
			"on a healthy pinned connection", acquired, err)
	}
}

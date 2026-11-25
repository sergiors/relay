//go:build integration

package deployment

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"relay/internal/state"
	"relay/internal/testutil"
)

// Startup ownership contracts for ObserveStartup, exercised against a real
// PostgreSQL server where a pinned session advisory lock lives on a specific
// backend that can be terminated. The incarnation the initial acquisition
// allocates is the root of every later progress fence, so startup must FAIL
// with the ownership error instead of consuming as an identity it can no
// longer prove — while an ordinary Git/materialization failure stays a
// retryable reported snapshot error. Every test fails rather than skips when
// PostgreSQL is unreachable.

// terminateStateBackend kills the state handle's pinned ownership backend from
// a separate administrative connection — the production ownership-loss event
// (a terminated backend or a dropped session) — while the handle's ordinary
// pool stays usable.
func terminateStateBackend(t *testing.T, env *coordinatorEnv) {
	t.Helper()
	pid := env.st.OwnerBackendPID()
	if pid <= 0 {
		t.Fatalf("owner backend PID = %d, want a pinned ownership backend", pid)
	}
	admin, err := sql.Open("pgx", env.dsn)
	if err != nil {
		t.Fatalf("open administrative connection: %v", err)
	}
	defer admin.Close()
	var terminated bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT pg_terminate_backend($1)`, pid,
	).Scan(&terminated); err != nil {
		t.Fatalf("terminate pinned ownership backend %d: %v", pid, err)
	}
	if !terminated {
		t.Fatalf("pg_terminate_backend(%d) = false, want the backend gone", pid)
	}
}

// requireZeroStartupSnapshot fails unless ObserveStartup returned no snapshot
// at all: an ownership or read failure must surface as an error, never as a
// snapshot (with or without an Error) that startup could consume or retry.
func requireZeroStartupSnapshot(t *testing.T, snapshot Snapshot) {
	t.Helper()
	if snapshot.Generation != 0 || snapshot.Commit != "" ||
		snapshot.Error != nil || snapshot.Authoritative ||
		len(snapshot.Fingerprints) != 0 || len(snapshot.Invalid) != 0 {
		t.Fatalf("snapshot = %+v, want the zero value (a failure must be an "+
			"error, not a consumable snapshot)", snapshot)
	}
}

// TestObserveStartupReturnsNodeReadError pins that a node-progress READ
// failure is propagated instead of being treated as "nothing to claim": with
// prior progress seeded and a desired revision published, a state handle whose
// pool died beneath the coordinator fails the read, ObserveStartup returns
// that error (no snapshot), and the stored incarnation/applied authorization
// remain untouched.
func TestObserveStartupReturnsNodeReadError(t *testing.T) {
	env := newCoordinatorEnv(t)
	ctx := context.Background()
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: "fp-1", templateID: "tid-1",
			phase: state.DeploymentAppPending})
	publishDesired(t, env.st, commitTwo, "main")
	before, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("seeded ReadDeploymentNode = (%+v, %v, %v)", before, found, err)
	}

	// When: the state handle can no longer read at all.
	if err := env.st.Close(); err != nil {
		t.Fatalf("close state handle: %v", err)
	}
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)

	// Then: the read failure is returned — not swallowed into a snapshot that
	// startup would consume as an unowned or empty view.
	if err == nil || !strings.Contains(err.Error(), "read deployment node") {
		t.Fatalf("ObserveStartup = (snapshot %+v, %v), want the node-read failure",
			snapshot, err)
	}
	requireZeroStartupSnapshot(t, snapshot)

	// And: nothing about the stored progress changed. A fresh reader observes
	// the exact incarnation and applied authorization the seed left behind.
	reader := testutil.OpenState(t, env.dsn)
	after, found, err := reader.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode after failed startup = (%+v, %v, %v)",
			after, found, err)
	}
	if after.Incarnation != before.Incarnation ||
		after.AppliedGeneration != before.AppliedGeneration ||
		after.Instance != before.Instance || after.Phase != before.Phase {
		t.Fatalf("node progress changed across a failed startup: before %+v, after %+v",
			before, after)
	}
}

// TestObserveStartupReturnsOwnershipLossBeforeConsumption pins the initial
// acquisition's ownership proof: after the pinned ownership backend is
// terminated while the pool survives, a restart of a node WITH prior progress
// must fail ObserveStartup with ErrOwnerLockLost instead of returning a
// retryable previous-snapshot result — and the rejected acquisition must not
// move the incarnation or the applied authorization.
func TestObserveStartupReturnsOwnershipLossBeforeConsumption(t *testing.T) {
	env := newCoordinatorEnv(t)
	ctx := context.Background()
	seedAppliedNode(t, env.st, env.cfg.NodeID, env.cfg.InstanceID, 1, commitOne,
		seededApp{name: "fn", fingerprint: "fp-1", templateID: "tid-1",
			phase: state.DeploymentAppPending})
	publishDesired(t, env.st, commitTwo, "main")
	before, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("seeded ReadDeploymentNode = (%+v, %v, %v)", before, found, err)
	}

	// When: ownership is lost while the handle's ordinary pool still works —
	// the residual case the ownership proof inside the acquisition exists for.
	terminateStateBackend(t, env)
	// The pool recovers on a fresh backend, so any rejection below is the lost
	// ownership, not an unreachable database.
	if _, found, err := env.st.ReadDeploymentDesired(ctx); err != nil || !found {
		t.Fatalf("state pool after termination = (found %v, %v), want the "+
			"published row still readable through the ordinary pool", found, err)
	}

	// Then: startup fails with the ownership error before consuming as this
	// identity; no snapshot (previous or otherwise) is returned to consume.
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err == nil || !errors.Is(err, state.ErrOwnerLockLost) {
		t.Fatalf("ObserveStartup = (snapshot %+v, %v), want ErrOwnerLockLost",
			snapshot, err)
	}
	requireZeroStartupSnapshot(t, snapshot)

	// And: the rejected acquisition wrote nothing — the successor-incarnation
	// fence keeps the exact stored progress.
	after, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode after rejected startup = (%+v, %v, %v)",
			after, found, err)
	}
	if after.Incarnation != before.Incarnation ||
		after.AppliedGeneration != before.AppliedGeneration ||
		after.Instance != before.Instance || after.Phase != before.Phase {
		t.Fatalf("node progress changed across a rejected startup: before %+v, after %+v",
			before, after)
	}
}

// TestObserveStartupOwnershipLossDuringMaterializationIsFatal pins the
// no-prior-row path: with only a published desired revision, the materialize
// stage's acquisition must propagate ErrOwnerLockLost as an error instead of
// degrading into the retryable "previous snapshot with an Error" result. No
// node row (and therefore no incarnation or authorization) may appear, the
// shared desired revision stays untouched, and no retry cadence is armed for
// what is a fatal identity failure rather than retryable work.
func TestObserveStartupOwnershipLossDuringMaterializationIsFatal(t *testing.T) {
	env := newCoordinatorEnv(t)
	ctx := context.Background()
	publishDesired(t, env.st, commitOne, "main")

	// When: the pinned ownership backend dies before the very first
	// acquisition this node ever makes.
	terminateStateBackend(t, env)

	// Then: the ownership error surfaces as an error, with no snapshot.
	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err == nil || !errors.Is(err, state.ErrOwnerLockLost) {
		t.Fatalf("ObserveStartup = (snapshot %+v, %v), want ErrOwnerLockLost",
			snapshot, err)
	}
	requireZeroStartupSnapshot(t, snapshot)

	// And: no incarnation was allocated, nothing was applied, the retry
	// cadence is not armed for a fatal ownership failure, and the shared
	// desired revision is unchanged.
	if _, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID); err != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no node row after a "+
			"rejected acquisition", found, err)
	}
	if c.retryPending() {
		t.Fatal("an ownership failure must not arm the retry cadence: it is a " +
			"fatal startup failure, not retryable materialization work")
	}
	desired, found, err := env.st.ReadDeploymentDesired(ctx)
	if err != nil || !found || desired.Generation != 1 ||
		desired.Commit != commitOne {
		t.Fatalf("desired after failed startup = (%+v, %v, %v), want the "+
			"published generation 1 unchanged", desired, found, err)
	}
}

// installOutcomeUnknownCommitFailure arms a schema-local DEFERRED constraint
// trigger on deployment_nodes that fails the acquisition statement's implicit
// COMMIT with SQLSTATE 40003 — the serialization-failure class production
// classifies as an outcome the client cannot prove
// (state.ErrDeploymentNodeOutcomeUnknown).
// This is real server-side failure injection through test-local DDL (the same
// fixture style as the F-01 atomicity triggers), not a fake database seam;
// cleanup rides the schema drop.
func installOutcomeUnknownCommitFailure(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open trigger connection: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stmts := []string{
		`CREATE FUNCTION relay_test_fail_deployment_node_commit() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
		 RAISE EXCEPTION 'injected deployment node commit failure'
		   USING ERRCODE = '40003';
		 RETURN NEW;
		 END;
		 $$`,
		`CREATE CONSTRAINT TRIGGER relay_test_fail_deployment_node_commit
		 AFTER INSERT OR UPDATE ON deployment_nodes
		 DEFERRABLE INITIALLY DEFERRED
		 FOR EACH ROW EXECUTE FUNCTION relay_test_fail_deployment_node_commit()`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("install outcome-unknown commit failure: %v", err)
		}
	}
}

// TestObserveStartupOutcomeUnknownAcquisitionIsFatal pins that an AMBIGUOUS
// acquisition outcome during materialization is never degraded into a
// retryable snapshot: a commit failure whose outcome the client cannot prove
// (state.ErrDeploymentNodeOutcomeUnknown) must propagate as an error with no
// snapshot to consume, arm no retry cadence, and leave no node row — startup
// may not continue as if the acquisition definitely failed and consume with an
// incarnation it never received. The failure is injected as a real 40003 at
// the statement's implicit COMMIT; a genuine network drop racing that implicit
// COMMIT cannot be reproduced deterministically without a proxy.
func TestObserveStartupOutcomeUnknownAcquisitionIsFatal(t *testing.T) {
	env := newCoordinatorEnv(t)
	ctx := context.Background()
	// Only a published desired revision: materialize's acquisition is the
	// first node-progress write this startup attempts.
	publishDesired(t, env.st, commitOne, "main")
	installOutcomeUnknownCommitFailure(t, env.dsn)

	c := New(env.cfg)
	snapshot, err := c.ObserveStartup(ctx)
	if err == nil || !errors.Is(err, state.ErrDeploymentNodeOutcomeUnknown) {
		t.Fatalf("ObserveStartup = (snapshot %+v, %v), want the outcome-unknown "+
			"sentinel propagated as an error", snapshot, err)
	}
	requireZeroStartupSnapshot(t, snapshot)
	if c.retryPending() {
		t.Fatal("an unknown acquisition outcome must not arm the retry cadence: " +
			"startup must not treat it as ordinary retryable materialization work")
	}
	// No incarnation was received and no node row may exist for a startup
	// that could not prove its acquisition committed.
	if _, found, err := env.st.ReadDeploymentNode(ctx, env.cfg.NodeID); err != nil ||
		found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no node row after an "+
			"outcome-unknown acquisition", found, err)
	}
	// The shared desired revision is untouched by the failed startup.
	desired, found, err := env.st.ReadDeploymentDesired(ctx)
	if err != nil || !found || desired.Generation != 1 ||
		desired.Commit != commitOne {
		t.Fatalf("desired after failed startup = (%+v, %v, %v), want the "+
			"published generation 1 unchanged", desired, found, err)
	}
}

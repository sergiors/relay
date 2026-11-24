//go:build integration

package state

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

// readRawDeploymentAppFor returns the stored instance, updated_at instant, and
// raw JSON payload for one arbitrary (node, app, generation) deployment_apps
// row. It reports found=false when the row is absent, which is the raw-level
// proof that a complete-set replacement physically removed an omitted app —
// something the read API cannot distinguish from a never-written row.
func readRawDeploymentAppFor(
	t *testing.T, st *State, nodeID, app string, generation int64,
) (instance string, updatedAt time.Time, data string, found bool) {
	t.Helper()
	err := st.db.QueryRowContext(context.Background(),
		`SELECT instance, updated_at, data::text
		 FROM deployment_apps
		 WHERE node_id = $1 AND app_name = $2 AND generation = $3`,
		nodeID, app, generation,
	).Scan(&instance, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, "", false
	}
	if err != nil {
		t.Fatalf("deployment app %q/%q generation %d: %v",
			nodeID, app, generation, err)
	}
	return instance, updatedAt, data, true
}

// sqlStringLiteral quotes s as a SQL string literal for test-local DDL.
func sqlStringLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, "'", "''") + `'`
}

// installDeploymentAppInsertFailure arms a schema-local BEFORE INSERT trigger
// that rejects exactly one named app. The rejection fires after earlier rows of
// the same transaction were already inserted, so any failure at that point in
// the production write becomes a whole-transaction rollback decided by the
// database itself. removeDeploymentAppInsertFailure disarms it so a retry in
// the same test runs against the untouched production write path.
func installDeploymentAppInsertFailure(t *testing.T, st *State, app string) {
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
			sqlStringLiteral(app) + `)`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("install deployment app insert failure: %v", err)
		}
	}
}

func removeDeploymentAppInsertFailure(t *testing.T, st *State) {
	t.Helper()
	stmts := []string{
		`DROP TRIGGER IF EXISTS relay_test_fail_deployment_app_insert
		 ON deployment_apps`,
		`DROP FUNCTION IF EXISTS relay_test_fail_deployment_app_insert()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("remove deployment app insert failure: %v", err)
		}
	}
}

// installDeploymentNodeApplyFailure arms a schema-local BEFORE UPDATE trigger
// that rejects any change to the node's applied generation — the final write of
// a materialization commit, after every app row of that transaction already
// landed.
func installDeploymentNodeApplyFailure(t *testing.T, st *State) {
	t.Helper()
	ctx := context.Background()
	stmts := []string{
		`CREATE FUNCTION relay_test_fail_deployment_node_apply() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
		 IF NEW.applied_generation IS DISTINCT FROM OLD.applied_generation THEN
		  RAISE EXCEPTION 'injected deployment node apply failure';
		 END IF;
		 RETURN NEW;
		 END;
		 $$`,
		`CREATE TRIGGER relay_test_fail_deployment_node_apply
		 BEFORE UPDATE ON deployment_nodes
		 FOR EACH ROW EXECUTE FUNCTION relay_test_fail_deployment_node_apply()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("install deployment node apply failure: %v", err)
		}
	}
}

func removeDeploymentNodeApplyFailure(t *testing.T, st *State) {
	t.Helper()
	stmts := []string{
		`DROP TRIGGER IF EXISTS relay_test_fail_deployment_node_apply
		 ON deployment_nodes`,
		`DROP FUNCTION IF EXISTS relay_test_fail_deployment_node_apply()`,
	}
	for _, stmt := range stmts {
		if _, err := st.db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("remove deployment node apply failure: %v", err)
		}
	}
}

// acquireDeployNodeObserving claims the shared test node at the NEXT generation
// the way the coordinator does: the observed progress still carries the
// PREVIOUS applied generation and commit, so the acquisition itself never
// resets the payload fields that describe the authorization still serving.
func acquireDeployNodeObserving(
	t *testing.T, st *State, instance string,
	generation int64, commit string,
	appliedGeneration int64, appliedCommit string,
) DeploymentNodeProgress {
	t.Helper()
	node, err := st.AcquireDeploymentNode(context.Background(), deployProgressNode,
		instance, generation, DeploymentNodeProgress{
			NodeID:            deployProgressNode,
			Generation:        generation,
			AppliedGeneration: appliedGeneration,
			AppliedCommit:     appliedCommit,
			Phase:             DeploymentNodeMaterializing,
			Commit:            commit,
		})
	if err != nil {
		t.Fatalf("acquire deployment node %q generation %d: %v",
			deployProgressNode, generation, err)
	}
	return node
}

// TestCommitDeploymentMaterializationIdempotentRecommit pins the atomic
// materialization commit's write semantics: the complete set is inserted once,
// an identical re-commit of the SAME generation is an accepted no-op that leaves
// every app row (including updated_at) untouched, a later same-generation
// COMPLETE set physically removes the rows it no longer contains while the
// retained row stays byte-for-byte unchanged, and changed content for a
// retained app lands as exactly one real UPDATE under the advanced clock. The
// physical INSERT/UPDATE counters prove the database itself performed no write
// for the identical re-commit; the validated node-row fence, not an upsert row
// count, is the transaction's authority.
func TestCommitDeploymentMaterializationIdempotentRecommit(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	start := deployProgressBase
	pinDeploymentClock(t, st, start)

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	first := DeploymentAppAuthorization{
		App: deployProgressApp, Phase: DeploymentAppPending,
		Fingerprint: "fp-1", TemplateIdentity: "tid-1",
	}
	second := DeploymentAppAuthorization{
		App: "other", Phase: DeploymentAppPending,
		Fingerprint: "fp-2", TemplateIdentity: "tid-2",
	}
	materialization := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps:        []DeploymentAppAuthorization{first, second},
	}
	landed, err := st.CommitDeploymentMaterialization(ctx, materialization)
	if err != nil || !landed {
		t.Fatalf("first commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 2, 0)
	instance, updatedAt, data := readRawDeploymentApp(t, st, 1)
	if instance != deployInstFirst {
		t.Fatalf("instance = %q, want %q", instance, deployInstFirst)
	}
	otherInstance, otherUpdatedAt, otherData, otherFound := readRawDeploymentAppFor(
		t, st, deployProgressNode, "other", 1)
	if !otherFound {
		t.Fatalf("row for %q missing after the first commit", "other")
	}

	// An identical re-commit at a LATER clock instant must not rewrite anything.
	pinDeploymentClock(t, st, start.Add(time.Minute))
	landed, err = st.CommitDeploymentMaterialization(ctx, materialization)
	if err != nil || !landed {
		t.Fatalf("identical re-commit = (%v, %v), want committed (idempotent no-op)",
			landed, err)
	}
	requireDeployAppWrites(t, st, 2, 0)
	gotInstance, gotUpdatedAt, gotData := readRawDeploymentApp(t, st, 1)
	if gotInstance != instance || !gotUpdatedAt.Equal(updatedAt) || gotData != data {
		t.Fatalf("row changed across an identical re-commit: instance %q -> %q, "+
			"updated_at %s -> %s, data %s -> %s",
			instance, gotInstance, updatedAt, gotUpdatedAt, data, gotData)
	}

	// A same-generation COMPLETE set that omits a previously committed app
	// physically removes exactly that row, while the retained app's row is not
	// rewritten at all (no UPDATE, original updated_at, original payload).
	pinDeploymentClock(t, st, start.Add(2*time.Minute))
	landed, err = st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps:        []DeploymentAppAuthorization{second},
	})
	if err != nil || !landed {
		t.Fatalf("reduced re-commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 2, 0)
	rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	if len(rows) != 1 || rows[0].App != "other" ||
		rows[0].Fingerprint != "fp-2" {
		t.Fatalf("rows = %+v, want only the retained app %q", rows, "other")
	}
	if removedInstance, removedUpdatedAt, removedData, found :=
		readRawDeploymentAppFor(t, st, deployProgressNode, deployProgressApp, 1); found {
		t.Fatalf("row for the app absent from the complete set survived: "+
			"instance %q, updated_at %s, data %s",
			removedInstance, removedUpdatedAt, removedData)
	}
	gotInstance, gotUpdatedAt, gotData, found := readRawDeploymentAppFor(
		t, st, deployProgressNode, "other", 1)
	if !found {
		t.Fatal("retained row missing after a reduced re-commit")
	}
	if gotInstance != otherInstance || !gotUpdatedAt.Equal(otherUpdatedAt) ||
		gotData != otherData {
		t.Fatalf("retained row was rewritten by a reduced re-commit: instance "+
			"%q -> %q, updated_at %s -> %s, data %s -> %s",
			otherInstance, gotInstance, otherUpdatedAt, gotUpdatedAt,
			otherData, gotData)
	}

	// Changed content for a retained app replaces the stale payload in place:
	// exactly one physical UPDATE, stamped with the advanced clock instant.
	pinDeploymentClock(t, st, start.Add(3*time.Minute))
	replaced := second
	replaced.Fingerprint = "fp-3"
	landed, err = st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps:        []DeploymentAppAuthorization{replaced},
	})
	if err != nil || !landed {
		t.Fatalf("changed-content re-commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 2, 1)
	rows, err = st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(rows) != 1 || rows[0].App != "other" ||
		rows[0].Fingerprint != "fp-3" {
		t.Fatalf("rows = (%+v, %v), want the replaced payload for %q",
			rows, err, "other")
	}
	_, gotUpdatedAt, _, found = readRawDeploymentAppFor(
		t, st, deployProgressNode, "other", 1)
	if !found || !gotUpdatedAt.Equal(start.Add(3*time.Minute)) {
		t.Fatalf("replaced row updated_at = (%s, %v), want %s (exactly one "+
			"physical UPDATE under the advanced clock)",
			gotUpdatedAt, found, start.Add(3*time.Minute))
	}

	// The node progressed atomically with the complete set.
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 1 || stored.AppliedCommit != deployCommitA ||
		stored.Phase != DeploymentNodeMaterialized {
		t.Fatalf("node = %+v, want generation 1 (%s) materialized", stored, deployCommitA)
	}
}

// TestCommitDeploymentMaterializationInstanceFence pins the instance half of the
// commit fence: the expected instance is mandatory, and a value that does not
// match the locked node row writes nothing, leaving the previous applied
// generation and the per-app rows untouched.
func TestCommitDeploymentMaterializationInstanceFence(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)
	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)

	if _, err := st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
	}); err == nil {
		t.Fatal("commit with an empty instance = nil, want an error")
	}

	landed, err := st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstSecond,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-1", TemplateIdentity: "tid-1",
		}},
	})
	if err != nil || landed {
		t.Fatalf("mismatched instance = (%v, %v), want a fenced no-op", landed, err)
	}
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 0 {
		t.Fatalf("applied generation = %d, want 0 (the fenced commit wrote nothing)",
			stored.AppliedGeneration)
	}
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 0); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none for the fenced commit", rows, err)
	}
}

// TestCommitDeploymentMaterializationStaleIncarnationWritesNothing pins the
// incarnation half of the commit fence: after a restart supersedes the node
// with a fresh incarnation, a commit fenced on the STALE incarnation is
// rejected with no row changes — zero physical writes, no app rows, and the
// node still at its acquired state — while a control commit from the current
// incarnation still lands, proving the refusal came from the fence.
func TestCommitDeploymentMaterializationStaleIncarnationWritesNothing(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)

	stale := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	current := acquireDeployNode(t, st, deployInstSecond, 1, deployCommitA)
	if current.Incarnation <= stale.Incarnation {
		t.Fatalf("re-acquired incarnation %d, want a fresh one after %d",
			current.Incarnation, stale.Incarnation)
	}

	landed, err := st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: stale.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-9", TemplateIdentity: "tid-9",
		}},
	})
	if err != nil || landed {
		t.Fatalf("stale-incarnation commit = (%v, %v), want a fenced no-op",
			landed, err)
	}
	requireDeployAppWrites(t, st, 0, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none from the stale-incarnation commit",
			rows, err)
	}
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.Incarnation != current.Incarnation || stored.AppliedGeneration != 0 {
		t.Fatalf("node = %+v, want incarnation %d and applied generation 0 "+
			"(the stale commit changed nothing)",
			stored, current.Incarnation)
	}

	// Control: the current incarnation's own commit still lands.
	landed, err = st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstSecond,
		Incarnation: current.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-1", TemplateIdentity: "tid-1",
		}},
	})
	if err != nil || !landed {
		t.Fatalf("current-incarnation commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)
}

// TestCommitDeploymentMaterializationOrderedAgainstSupersedingAcquire pins the
// commit's fence ordering against a concurrent superseding incarnation: a
// commit whose node-row lock request begins while the supersession is
// uncommitted must be ordered against it, so once the supersession commits the
// pre-supersession incarnation lands nothing (false, zero physical writes, no
// rows) — and only then the superseding incarnation's own commit is proven to
// still land. A fence that read a pre-supersession snapshot would accept the
// commit the acquire superseded.
//
// The interleaving is controlled with two connections and no arbitrary sleeps:
// an explicit transaction applies the acquire's row effect (a fresh
// incarnation) without committing, the production commit starts from the
// committed pre-supersession state, and pg_stat_activity + pg_blocking_pids
// polling proves the commit's statement is in flight — its snapshot therefore
// predates the commit — before the transaction commits. Physical write counts
// come from schema-local triggers. Every test runs on its own isolated schema
// (openTestState) and fails rather than skips without PostgreSQL.
func TestCommitDeploymentMaterializationOrderedAgainstSupersedingAcquire(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)

	// Independent monitor: the state pool's connections may be held by the
	// blocked commit and the open supersession transaction while polling.
	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)

	// A dedicated connection applies the acquire's row effect — a fresh
	// incarnation — without committing, so the test owns the commit point.
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin supersession transaction: %v", err)
	}
	var supersessionPID int
	if err := tx.QueryRowContext(ctx,
		`SELECT pg_backend_pid()`).Scan(&supersessionPID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("read supersession backend pid: %v", err)
	}
	var superseded int64
	if err := tx.QueryRowContext(ctx, `
		UPDATE deployment_nodes
		SET incarnation = nextval('deployment_incarnation_seq')
		WHERE node_id = $1
		RETURNING incarnation`,
		deployProgressNode).Scan(&superseded); err != nil {
		_ = tx.Rollback()
		t.Fatalf("apply superseding incarnation: %v", err)
	}
	if superseded == node.Incarnation {
		_ = tx.Rollback()
		t.Fatalf("superseding incarnation = %d, want a fresh one after %d",
			superseded, node.Incarnation)
	}

	// When: the superseded incarnation's materialization commit starts while
	// the supersession transaction is still open.
	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writes := make(chan appProgressWriteResult, 1)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		landed, err := st.CommitDeploymentMaterialization(writeCtx,
			DeploymentMaterialization{
				NodeID:      deployProgressNode,
				Generation:  1,
				Instance:    deployInstFirst,
				Incarnation: node.Incarnation,
				Commit:      deployCommitA,
				Apps: []DeploymentAppAuthorization{{
					App: deployProgressApp, Phase: DeploymentAppPending,
					Fingerprint: "fp-1", TemplateIdentity: "tid-1",
				}},
			})
		writes <- appProgressWriteResult{landed: landed, err: err}
	}()

	// Cleanup unblocks every participant on any failure path: cancel the
	// commit, roll back an uncommitted supersession (a no-op after commit),
	// then join the goroutine. The buffered channel keeps the goroutine from
	// blocking on send when the test has already failed.
	t.Cleanup(func() {
		cancelWrite()
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("release supersession transaction: %v", err)
		}
		select {
		case <-writeDone:
		case <-time.After(5 * time.Second):
			t.Log("fenced materialization commit did not return during cleanup")
		}
	})

	// Then: observe that the commit's statement is in flight BEFORE the
	// supersession commits — it waits on the supersession backend, or (with a
	// lock-free fence) completed outright. Either observation proves the
	// statement snapshot predates the commit below.
	early := awaitFenceOrdering(t, monitor, supersessionPID, writes)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit supersession: %v", err)
	}

	got := appProgressWriteResult{}
	if early != nil {
		got = *early
	} else {
		select {
		case got = <-writes:
		case <-time.After(5 * time.Second):
			t.Fatal("fenced materialization commit did not return after the " +
				"supersession committed")
		}
	}
	if got.err != nil {
		t.Fatalf("CommitDeploymentMaterialization: %v", got.err)
	}
	if got.landed {
		if early != nil {
			t.Fatal("the commit completed while the supersession was still " +
				"uncommitted and reported landed=true; want false — its fence " +
				"read a pre-supersession snapshot (the commit must be ordered " +
				"against the supersession)")
		}
		t.Fatal("the commit reported landed=true after the supersession " +
			"committed; want false (the fence must re-check the incarnation " +
			"against the newest node row)")
	}

	// The refused commit persisted nothing: schema-local triggers counted
	// zero physical INSERT/UPDATE statements, no row exists, and the node
	// shows only the supersession's effects.
	requireDeployAppWrites(t, st, 0, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none from the refused commit", rows, err)
	}
	current, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v), want the node",
			current, found, err)
	}
	if current.Incarnation != superseded || current.Generation != 1 ||
		current.AppliedGeneration != 0 {
		t.Fatalf("node = %+v, want incarnation %d still at generation 1 with "+
			"nothing applied", current, superseded)
	}

	// Control: the superseding incarnation's own commit still lands, so the
	// refusal above came from the supersession, not a dead fence. The
	// supersession only changed the incarnation; the instance is untouched.
	landed, err := st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: superseded,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-1", TemplateIdentity: "tid-1",
		}},
	})
	if err != nil || !landed {
		t.Fatalf("superseding-incarnation commit = (%v, %v), want committed",
			landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)
	rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(rows) != 1 || rows[0].Phase != DeploymentAppPending {
		t.Fatalf("rows = (%+v, %v), want the pending row from the current "+
			"incarnation", rows, err)
	}
}

// TestCommitDeploymentMaterializationCancelledDuringLockWaitWritesNothing pins
// context cancellation: a commit whose context is cancelled while its statement
// is provably waiting on the node-row lock fails with context.Canceled and
// writes nothing — zero physical writes, no app rows, no node advancement —
// while the same commit from a live context still lands afterwards. The
// interleaving is proven through pg_stat_activity/pg_blocking_pids polling, so
// the cancellation never races an arbitrary sleep.
func TestCommitDeploymentMaterializationCancelledDuringLockWaitWritesNothing(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)

	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	materialization := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-1", TemplateIdentity: "tid-1",
		}},
	}

	// A dedicated connection holds the node-row lock so the commit below is
	// provably in flight when its context is cancelled.
	holder, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock holder transaction: %v", err)
	}
	var holderPID int
	if err := holder.QueryRowContext(ctx,
		`SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
		_ = holder.Rollback()
		t.Fatalf("read lock holder backend pid: %v", err)
	}
	var locked string
	if err := holder.QueryRowContext(ctx,
		`SELECT node_id FROM deployment_nodes WHERE node_id = $1 FOR UPDATE`,
		deployProgressNode).Scan(&locked); err != nil {
		_ = holder.Rollback()
		t.Fatalf("lock deployment node row: %v", err)
	}

	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writes := make(chan appProgressWriteResult, 1)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		landed, err := st.CommitDeploymentMaterialization(writeCtx, materialization)
		writes <- appProgressWriteResult{landed: landed, err: err}
	}()

	t.Cleanup(func() {
		cancelWrite()
		if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("release node lock holder: %v", err)
		}
		select {
		case <-writeDone:
		case <-time.After(5 * time.Second):
			t.Log("materialization commit did not return during cleanup")
		}
	})

	// The commit must be observably WAITING on the holder's backend before the
	// cancellation — a completed commit here would mean it never took the lock
	// at all.
	if early := awaitFenceOrdering(t, monitor, holderPID, writes); early != nil {
		t.Fatalf("commit completed (%v, %v) while the node row lock was held: "+
			"want it waiting on the holder", early.landed, early.err)
	}

	cancelWrite()
	got := appProgressWriteResult{}
	select {
	case got = <-writes:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled materialization commit did not return")
	}
	if got.landed {
		t.Fatal("cancelled commit landed = true, want false")
	}
	if got.err == nil || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancelled commit error = %v, want a context.Canceled failure",
			got.err)
	}

	// Release the holder for the assertions and the control below.
	if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("release node lock holder: %v", err)
	}

	requireDeployAppWrites(t, st, 0, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none from the cancelled commit", rows, err)
	}
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 0 || stored.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want no advancement from the cancelled commit",
			stored)
	}

	// Control: the same commit from a live context still lands, so the
	// refusal above came from the cancellation, not a poisoned transaction.
	landed, err := st.CommitDeploymentMaterialization(context.Background(),
		materialization)
	if err != nil || !landed {
		t.Fatalf("commit after cancellation = (%v, %v), want committed",
			landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)
}

// TestCommitDeploymentMaterializationEmptyDeploymentIsCompleteSet pins the
// legitimately empty deployment: an empty set commits and applies the
// generation with zero rows (an accepted empty state, never an error), an
// identical empty re-commit stays a no-op, and a later EMPTY same-generation
// set removes previously committed rows — the complete-set contract at its
// extreme — while the node keeps the applied generation.
func TestCommitDeploymentMaterializationEmptyDeploymentIsCompleteSet(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	start := deployProgressBase
	pinDeploymentClock(t, st, start)

	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	empty := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
	}

	landed, err := st.CommitDeploymentMaterialization(ctx, empty)
	if err != nil || !landed {
		t.Fatalf("empty commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 0, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none for the empty deployment", rows, err)
	}
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 1 || stored.AppliedCommit != deployCommitA ||
		stored.Phase != DeploymentNodeMaterialized {
		t.Fatalf("node = %+v, want generation 1 (%s) materialized from an empty set",
			stored, deployCommitA)
	}

	// An identical empty re-commit at a later clock is still a no-op.
	pinDeploymentClock(t, st, start.Add(time.Minute))
	landed, err = st.CommitDeploymentMaterialization(ctx, empty)
	if err != nil || !landed {
		t.Fatalf("identical empty re-commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 0, 0)

	// A set with content lands first, so the following empty commit has a real
	// row set to replace.
	pinDeploymentClock(t, st, start.Add(2*time.Minute))
	landed, err = st.CommitDeploymentMaterialization(ctx, DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: node.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{{
			App: deployProgressApp, Phase: DeploymentAppPending,
			Fingerprint: "fp-1", TemplateIdentity: "tid-1",
		}},
	})
	if err != nil || !landed {
		t.Fatalf("populated re-commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)

	// The EMPTY complete set removes every row the generation still holds.
	pinDeploymentClock(t, st, start.Add(3*time.Minute))
	landed, err = st.CommitDeploymentMaterialization(ctx, empty)
	if err != nil || !landed {
		t.Fatalf("empty replacement commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("rows = (%+v, %v), want none after the empty replacement", rows, err)
	}
	stored, found, err = st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 1 || stored.Phase != DeploymentNodeMaterialized {
		t.Fatalf("node = %+v, want generation 1 still materialized", stored)
	}
}

// TestCommitDeploymentMaterializationRollsBackAtomicSetAfterPartialInsert pins
// the atomicity invariant at its hardest point: a persistence failure raised
// AFTER at least one earlier app row of the SAME transaction was inserted rolls
// back the whole materialization. The previous applied generation and its
// authorization keep serving (rows retained, applied generation and commit
// untouched, nothing pruned), no partial target rows survive, and the write
// counters roll back WITH the rows — the database's own record of the partial
// inserts disappearing. Removing the fault and retrying the identical commit
// lands the full valid/invalid/removed set and advances the node atomically.
func TestCommitDeploymentMaterializationRollsBackAtomicSetAfterPartialInsert(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)

	// Given: generation 1 is applied with its complete authorization.
	first := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	prior := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  1,
		Instance:    deployInstFirst,
		Incarnation: first.Incarnation,
		Commit:      deployCommitA,
		Apps: []DeploymentAppAuthorization{
			{App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-1", TemplateIdentity: "tid-1"},
			{App: "legacy", Phase: DeploymentAppPending,
				Fingerprint: "fp-0", TemplateIdentity: "tid-0"},
		},
	}
	if landed, err := st.CommitDeploymentMaterialization(ctx, prior); err != nil ||
		!landed {
		t.Fatalf("prior commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 2, 0)

	// When: generation 2's persistence fails after its first target row was
	// already inserted inside the transaction.
	next := acquireDeployNodeObserving(t, st, deployInstFirst, 2, deployCommitB,
		1, deployCommitA)
	target := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  2,
		Instance:    deployInstFirst,
		Incarnation: next.Incarnation,
		Commit:      deployCommitB,
		Apps: []DeploymentAppAuthorization{
			{App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-2", TemplateIdentity: "tid-2"},
			{App: "boom", Phase: DeploymentAppFailed,
				TemplateIdentity: "tid-x", Error: "template invalid"},
			{App: "legacy", Phase: DeploymentAppRemoved},
		},
	}
	installDeploymentAppInsertFailure(t, st, "boom")

	landed, err := st.CommitDeploymentMaterialization(ctx, target)
	if err == nil || landed {
		t.Fatalf("commit with an injected insert failure = (%v, %v), want an "+
			"error with nothing landed", landed, err)
	}

	// Then: everything rolls back together. The counter increments for the
	// partial inserts were part of the aborted transaction and disappeared
	// with the rows.
	requireDeployAppWrites(t, st, 2, 0)
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.Generation != 2 || stored.AppliedGeneration != 1 ||
		stored.AppliedCommit != deployCommitA ||
		stored.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want observed generation 2 still applying "+
			"generation 1 (%s)", stored, deployCommitA)
	}
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 2); err != nil ||
		len(rows) != 0 {
		t.Fatalf("generation-2 rows = (%+v, %v), want none (no partial target "+
			"rows)", rows, err)
	}
	priorRows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(priorRows) != 2 {
		t.Fatalf("generation-1 rows = (%+v, %v), want both prior rows retained "+
			"(nothing pruned by the failure)", priorRows, err)
	}

	// When the fault is removed, the identical retry commits the full
	// valid/invalid/removed set and advances the node atomically.
	removeDeploymentAppInsertFailure(t, st)
	landed, err = st.CommitDeploymentMaterialization(ctx, target)
	if err != nil || !landed {
		t.Fatalf("retry after removing the fault = (%v, %v), want committed",
			landed, err)
	}
	requireDeployAppWrites(t, st, 5, 0)

	targetRows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 2)
	if err != nil {
		t.Fatalf("ReadDeploymentAppProgress: %v", err)
	}
	byName := make(map[string]DeploymentAppProgress, len(targetRows))
	for _, row := range targetRows {
		byName[row.App] = row
	}
	if len(targetRows) != 3 {
		t.Fatalf("target rows = %+v, want the full three-app set", targetRows)
	}
	if row := byName[deployProgressApp]; row.Phase != DeploymentAppPending ||
		row.Fingerprint != "fp-2" || row.TemplateIdentity != "tid-2" {
		t.Fatalf("valid row = %+v, want the pending target authorization", row)
	}
	if row := byName["boom"]; row.Phase != DeploymentAppFailed ||
		row.Error != "template invalid" || row.TemplateIdentity != "tid-x" {
		t.Fatalf("invalid row = %+v, want the failed diagnostic with its "+
			"template identity", row)
	}
	if row := byName["legacy"]; row.Phase != DeploymentAppRemoved {
		t.Fatalf("removed row = %+v, want the removal", row)
	}
	stored, found, err = st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 2 || stored.AppliedCommit != deployCommitB ||
		stored.Phase != DeploymentNodeMaterialized {
		t.Fatalf("node = %+v, want generation 2 (%s) materialized after the retry",
			stored, deployCommitB)
	}
}

// TestCommitDeploymentMaterializationRollsBackAppsWithNodeProgressFailure pins
// the other half of the atomic write: when the node-progress UPDATE (the final
// write of the transaction) fails AFTER every app row was already inserted,
// the app writes and the node advancement roll back TOGETHER — no generation-2
// rows, no applied-generation change, and the counter increments for those
// inserts rolled back with them — while the prior generation keeps serving.
// Removing the fault and retrying the identical commit lands both halves.
func TestCommitDeploymentMaterializationRollsBackAppsWithNodeProgressFailure(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()
	pinDeploymentClock(t, st, deployProgressBase)

	// Given: generation 1 is applied with one authorized app.
	first := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	if landed, err := st.CommitDeploymentMaterialization(ctx,
		DeploymentMaterialization{
			NodeID:      deployProgressNode,
			Generation:  1,
			Instance:    deployInstFirst,
			Incarnation: first.Incarnation,
			Commit:      deployCommitA,
			Apps: []DeploymentAppAuthorization{{
				App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-1", TemplateIdentity: "tid-1",
			}},
		}); err != nil || !landed {
		t.Fatalf("prior commit = (%v, %v), want committed", landed, err)
	}
	requireDeployAppWrites(t, st, 1, 0)

	// When: generation 2's app rows are written first, then the node-progress
	// update fails inside the same transaction.
	next := acquireDeployNodeObserving(t, st, deployInstFirst, 2, deployCommitB,
		1, deployCommitA)
	target := DeploymentMaterialization{
		NodeID:      deployProgressNode,
		Generation:  2,
		Instance:    deployInstFirst,
		Incarnation: next.Incarnation,
		Commit:      deployCommitB,
		Apps: []DeploymentAppAuthorization{
			{App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-2", TemplateIdentity: "tid-2"},
			{App: "other", Phase: DeploymentAppPending,
				Fingerprint: "fp-3", TemplateIdentity: "tid-3"},
		},
	}
	installDeploymentNodeApplyFailure(t, st)

	landed, err := st.CommitDeploymentMaterialization(ctx, target)
	if err == nil || landed {
		t.Fatalf("commit with an injected node-apply failure = (%v, %v), want "+
			"an error with nothing landed", landed, err)
	}

	// Then: the app inserts rolled back together with the node advancement.
	requireDeployAppWrites(t, st, 1, 0)
	if rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 2); err != nil ||
		len(rows) != 0 {
		t.Fatalf("generation-2 rows = (%+v, %v), want none (app writes rolled "+
			"back with the node update)", rows, err)
	}
	priorRows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(priorRows) != 1 ||
		priorRows[0].Fingerprint != "fp-1" {
		t.Fatalf("generation-1 rows = (%+v, %v), want the prior authorization "+
			"intact", priorRows, err)
	}
	stored, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.Generation != 2 || stored.AppliedGeneration != 1 ||
		stored.AppliedCommit != deployCommitA ||
		stored.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want observed generation 2 still applying "+
			"generation 1 (%s) — the node advancement rolled back with the app "+
			"writes", stored, deployCommitA)
	}

	// When the fault is removed, the identical retry commits both halves.
	removeDeploymentNodeApplyFailure(t, st)
	landed, err = st.CommitDeploymentMaterialization(ctx, target)
	if err != nil || !landed {
		t.Fatalf("retry after removing the fault = (%v, %v), want committed",
			landed, err)
	}
	requireDeployAppWrites(t, st, 3, 0)
	rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 2)
	if err != nil || len(rows) != 2 {
		t.Fatalf("generation-2 rows = (%+v, %v), want both target rows", rows, err)
	}
	stored, found, err = st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", stored, found, err)
	}
	if stored.AppliedGeneration != 2 || stored.AppliedCommit != deployCommitB ||
		stored.Phase != DeploymentNodeMaterialized {
		t.Fatalf("node = %+v, want generation 2 (%s) materialized after the retry",
			stored, deployCommitB)
	}
}

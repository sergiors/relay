//go:build integration

package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// supersessionLockWaitersQuery counts the backends currently waiting on a lock
// held by the backend with pid $1. It is the server-side proof that the
// app-progress write's statement is in flight while the supersession
// transaction is still open: a client-side timer alone cannot distinguish a
// waiting write from a slow one. pg_blocking_pids derives the answer from the
// lock manager, so no pg_locks lock-shape assumption is involved.
const supersessionLockWaitersQuery = `
	SELECT COUNT(*)
	FROM pg_stat_activity
	WHERE wait_event_type = 'Lock'
	  AND $1 = ANY(pg_blocking_pids(pid))`

// appProgressWriteResult is one RecordDeploymentAppProgress outcome observed
// from the coordinating goroutine.
type appProgressWriteResult struct {
	landed bool
	err    error
}

// awaitFenceOrdering proves the fenced write started before the supersession
// commits: it polls pg_stat_activity (bounded deadline, fixed cadence — never
// a blind sleep) until the write is observably waiting on the supersession
// backend, or the write completes without waiting, which only a fence without
// a lock on the node row can do. A non-nil result is the completed write; nil
// means the lock wait was observed. It fails the test when neither appears
// before the deadline; cleanup unblocks the participants on every path.
func awaitFenceOrdering(
	t *testing.T,
	monitor *sql.DB,
	supersessionPID int,
	writes <-chan appProgressWriteResult,
) *appProgressWriteResult {
	t.Helper()
	const (
		pollInterval = 10 * time.Millisecond
		pollDeadline = 10 * time.Second
	)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(pollDeadline)
	defer timeout.Stop()
	for {
		select {
		case got := <-writes:
			return &got
		case <-timeout.C:
			t.Fatalf("neither a lock wait on supersession backend %d nor a "+
				"completed write appeared after %s",
				supersessionPID, pollDeadline)
		case <-ticker.C:
			var waiters int
			queryCtx, cancel := context.WithTimeout(
				context.Background(), time.Second)
			err := monitor.QueryRowContext(queryCtx,
				supersessionLockWaitersQuery, supersessionPID).Scan(&waiters)
			cancel()
			if err != nil {
				t.Fatalf("query supersession lock waiters: %v", err)
			}
			if waiters > 0 {
				return nil
			}
		}
	}
}

// TestRecordDeploymentAppProgressOrderedAgainstSupersedingAcquire pins the
// FOR SHARE fence ordering against a concurrent superseding incarnation: a
// write whose statement begins while the supersession is uncommitted must be
// ordered against it, so once the supersession commits the pre-supersession
// incarnation lands nothing (false, zero physical INSERT/UPDATE, no row), and
// only then the superseding incarnation's own write is proven to still land.
// A plain snapshot fence read would evaluate its incarnation check against
// the pre-supersession state and accept the write the acquire superseded.
//
// The interleaving is controlled with two connections and no arbitrary
// sleeps: an explicit transaction applies the acquire's row effect (a fresh
// incarnation) without committing, the production write starts from the
// committed pre-supersession state, and pg_stat_activity + pg_blocking_pids
// polling proves the write's statement is in flight — its statement snapshot
// therefore predates the commit — before the transaction commits. Physical
// write counts come from schema-local triggers. Every test runs on its own
// isolated schema (openTestState) and fails rather than skips without
// PostgreSQL.
func TestRecordDeploymentAppProgressOrderedAgainstSupersedingAcquire(t *testing.T) {
	st := openTestState(t)
	installDeploymentAppWriteCounters(t, st)
	ctx := context.Background()

	// Independent monitor: the state pool's connections may be held by the
	// blocked write and the open supersession transaction while polling.
	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	// Given: the node runs at generation 1 under incarnation I1.
	node := acquireDeployNode(t, st, deployInstFirst, 1, deployCommitA)
	progress := deployAppProgress(1, deployInstFirst,
		DeploymentAppPending, "fp-1", "tid-1", deployCommitA, "")

	// A dedicated connection applies the acquire's row effect — a fresh
	// incarnation — without committing, so the test owns the commit point.
	// Only the incarnation changes: generation and applied_generation stay
	// put so the generation predicate still matches and any refusal can only
	// come from the incarnation re-check against the newest row version.
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

	// When: the superseded incarnation's app-progress write starts while the
	// supersession transaction is still open.
	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writes := make(chan appProgressWriteResult, 1)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		landed, err := st.RecordDeploymentAppProgress(writeCtx,
			deployProgressNode, deployProgressApp, 1, node.Incarnation,
			progress)
		writes <- appProgressWriteResult{landed: landed, err: err}
	}()

	// Cleanup unblocks every participant on any failure path: cancel the
	// write, roll back an uncommitted supersession (a no-op after commit),
	// then join the goroutine. The buffered channel keeps the goroutine from
	// blocking on send when the test has already failed.
	t.Cleanup(func() {
		cancelWrite()
		if err := tx.Rollback(); err != nil &&
			!errors.Is(err, sql.ErrTxDone) {
			t.Logf("release supersession transaction: %v", err)
		}
		select {
		case <-writeDone:
		case <-time.After(5 * time.Second):
			t.Log("fenced app-progress write did not return during cleanup")
		}
	})

	// Then: observe that the write's statement is in flight BEFORE the
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
			t.Fatal("fenced write did not return after the supersession " +
				"committed")
		}
	}
	if got.err != nil {
		t.Fatalf("RecordDeploymentAppProgress: %v", got.err)
	}
	if got.landed {
		if early != nil {
			t.Fatal("the write completed while the supersession was still " +
				"uncommitted and reported landed=true; want false — its " +
				"fence read a pre-supersession snapshot (the write must be " +
				"ordered against the supersession)")
		}
		t.Fatal("the write reported landed=true after the supersession " +
			"committed; want false (the fence must re-check the incarnation " +
			"against the newest node row)")
	}

	// The refused write persisted nothing: schema-local triggers counted
	// zero physical INSERT/UPDATE statements, and neither a stored row nor
	// presented progress exists for the generation.
	requireDeployAppWrites(t, st, 0, 0)
	if count := deploymentAppRowCount(t, st); count != 0 {
		t.Fatalf("row count = %d, want 0 (a write refused by the fence "+
			"persists nothing)", count)
	}
	rows, err := st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("ReadDeploymentAppProgress = (%d rows, %v), want no rows",
			len(rows), err)
	}

	// The supersession is committed and visible as the node's state.
	current, found, err := st.ReadDeploymentNode(ctx, deployProgressNode)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v), want the node",
			current, found, err)
	}
	if current.Incarnation != superseded || current.Generation != 1 {
		t.Fatalf("node = %+v, want incarnation %d still at generation 1",
			current, superseded)
	}

	// Control: the superseding incarnation's own write still lands, so the
	// refusal above came from the supersession, not a dead fence.
	if landed := writeDeployApp(t, st, 1, superseded, progress); !landed {
		t.Fatal("write from the superseding incarnation = false, want true")
	}
	requireDeployAppWrites(t, st, 1, 0)
	if count := deploymentAppRowCount(t, st); count != 1 {
		t.Fatalf("row count = %d, want 1 (the current incarnation's write)",
			count)
	}
	rows, err = st.ReadDeploymentAppProgress(ctx, deployProgressNode, 1)
	if err != nil || len(rows) != 1 || rows[0].Phase != DeploymentAppPending {
		t.Fatalf("ReadDeploymentAppProgress = (%+v, %v), want the pending row",
			rows, err)
	}
}

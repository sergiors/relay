package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// advisoryLockWaitersQuery counts the backends currently waiting on the
// transaction-scoped advisory lock held by the backend with pid $1. It is the
// server-side proof that a write reached the app-lock wait: a client-side timer
// alone cannot distinguish a blocked write from a slow one.
const advisoryLockWaitersQuery = `
	SELECT COUNT(*)
	FROM pg_stat_activity
	WHERE wait_event_type = 'Lock'
	  AND wait_event = 'advisory'
	  AND $1 = ANY(pg_blocking_pids(pid))`

// TestStatusTransitionCanceledWhileWaitingForAppLock is the F-03 regression:
// a lifecycle-canceled status write that is already blocked on the per-app
// advisory lock must return promptly with context.Canceled instead of waiting
// for the lock holder. The canceled write must persist nothing and must not
// notify the status observer, and its transaction/connection must be released
// once it returns.
func TestStatusTransitionCanceledWhileWaitingForAppLock(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	const name = "canceled-status"
	fn := fnFor(t, name, tmpl)
	fencingToken := seedFencingTokenWithFingerprint(t, c, fn, "fp")

	seeded, ok := c.GetApp(name)
	if !ok {
		t.Fatal("seeded app row is missing")
	}
	if seeded.Status != StatusPreparing {
		t.Fatalf("seeded status = %q, want %q", seeded.Status, StatusPreparing)
	}
	beforeData, beforeUpdatedAt, _ := rawAppData(t, c, name)

	// Install the observer only after seeding, so the only notification the
	// test can see is the transition under test.
	observer, events := recordingObserver()
	c.SetStatusObserver(observer)

	// monitor is an independent connection: the state handle's pool may be
	// fully occupied by the blocked write while the test inspects
	// pg_stat_activity.
	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	lockCtx := context.Background()
	tx, err := c.db.BeginTx(lockCtx, nil)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	var blockerPID int
	if err := tx.QueryRowContext(lockCtx,
		`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("read lock backend pid: %v", err)
	}
	// Hold the exact per-app lock the production write path takes.
	if err := lockAppTx(lockCtx, tx, name); err != nil {
		_ = tx.Rollback()
		t.Fatalf("lock app: %v", err)
	}

	lifecycle, cancel := context.WithCancel(context.Background())
	type transitionResult struct {
		landed bool
		err    error
	}
	results := make(chan transitionResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		landed, err := c.RecordReconcileBuilding(lifecycle, name, fencingToken)
		results <- transitionResult{landed: landed, err: err}
	}()

	released := false
	releaseLock := func() {
		if released {
			return
		}
		released = true
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("release app lock: %v", err)
		}
	}
	// Cleanup is safe on every failure path: cancel the owning context,
	// release the external lock, then join the transition if it is still
	// running. The buffered result channel keeps the goroutine from blocking on
	// send when the test has already failed and stopped receiving.
	t.Cleanup(func() {
		cancel()
		releaseLock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("canceled status transition did not return during cleanup")
		}
	})

	waitForAdvisoryLockWait(t, monitor, blockerPID)

	start := time.Now()
	cancel()
	var got transitionResult
	select {
	case got = <-results:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled status transition did not return promptly")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("canceled transition returned after %s, want prompt cancellation",
			elapsed)
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled transition error = %v, want context.Canceled", got.err)
	}
	if got.landed {
		t.Fatal("canceled transition reported a landed write")
	}

	// The write never committed: the persisted snapshot and its timestamp are
	// exactly the seeded state, and no observer saw a transition.
	afterData, afterUpdatedAt, _ := rawAppData(t, c, name)
	if afterData != beforeData || afterUpdatedAt != beforeUpdatedAt {
		t.Fatalf("canceled transition mutated the snapshot: before=%s/%s after=%s/%s",
			beforeData, beforeUpdatedAt, afterData, afterUpdatedAt)
	}
	if len(*events) != 0 {
		t.Fatalf("canceled transition notified the observer: %+v", *events)
	}

	// The canceled transaction released its server wait: no backend still waits
	// on the lock. Releasing the external transaction then leaves the pool with
	// no checked-out connection, and makes the same app writable again: the next
	// context-aware transition acquires the lock and lands. Together these prove
	// the canceled attempt left neither its transaction nor the app lock behind.
	waitForAdvisoryLockWaitClear(t, monitor, blockerPID)
	releaseLock()
	waitForPoolIdle(t, c)
	writeCtx, writeCancel := context.WithTimeout(
		context.Background(), 10*time.Second)
	defer writeCancel()
	landed, err := c.RecordReconcileBuilding(writeCtx, name, fencingToken)
	if err != nil {
		t.Fatalf("transition after lock release: %v", err)
	}
	if !landed {
		t.Fatal("transition after lock release did not land")
	}
	detail, ok := c.GetApp(name)
	if !ok {
		t.Fatal("app row missing after the post-release transition")
	}
	if detail.Status != StatusBuilding {
		t.Fatalf("status after lock release = %q, want %q",
			detail.Status, StatusBuilding)
	}
	want := statusEvent{name: name, status: StatusBuilding}
	if len(*events) != 1 || (*events)[0] != want {
		t.Fatalf("observer events after lock release = %+v, want one building event",
			*events)
	}
}

// waitForAdvisoryLockWait polls pg_stat_activity until a backend is blocked on
// the advisory lock held by blockerPID. The bounded deadline makes the test
// fail instead of hanging when the write never reaches the lock wait.
func waitForAdvisoryLockWait(t *testing.T, monitor *sql.DB, blockerPID int) {
	t.Helper()
	waitForAdvisoryLockWaiters(t, monitor, blockerPID, func(waiters int) bool {
		return waiters > 0
	})
}

// waitForAdvisoryLockWaitClear polls until no backend waits on the advisory
// lock held by blockerPID, proving the canceled transaction released its wait.
func waitForAdvisoryLockWaitClear(
	t *testing.T,
	monitor *sql.DB,
	blockerPID int,
) {
	t.Helper()
	waitForAdvisoryLockWaiters(t, monitor, blockerPID, func(waiters int) bool {
		return waiters == 0
	})
}

// waitForPoolIdle polls until the state pool has no checked-out connection,
// proving the canceled transaction was rolled back and returned to the pool.
func waitForPoolIdle(t *testing.T, c *State) {
	t.Helper()
	const (
		pollInterval = 10 * time.Millisecond
		pollDeadline = 2 * time.Second
	)
	deadline := time.Now().Add(pollDeadline)
	for {
		if inUse := c.db.Stats().InUse; inUse == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state pool still has %d connection(s) in use after %s",
				c.db.Stats().InUse, pollDeadline)
		}
		time.Sleep(pollInterval)
	}
}

// waitForAdvisoryLockWaiters polls the server until satisfied reports true for
// the current advisory-lock waiter count, or the deadline expires. Polling the
// actual server state (rather than sleeping for a fixed duration) keeps the
// test deterministic on both fast and loaded machines.
func waitForAdvisoryLockWaiters(
	t *testing.T,
	monitor *sql.DB,
	blockerPID int,
	satisfied func(int) bool,
) {
	t.Helper()
	const (
		pollInterval = 10 * time.Millisecond
		pollDeadline = 10 * time.Second
	)
	deadline := time.Now().Add(pollDeadline)
	for {
		queryCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		var waiters int
		err := monitor.QueryRowContext(queryCtx, advisoryLockWaitersQuery,
			blockerPID).Scan(&waiters)
		cancel()
		if err != nil {
			t.Fatalf("query advisory lock waiters: %v", err)
		}
		if satisfied(waiters) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("advisory lock waiter count = %d after %s, want a state "+
				"matching the transition under test", waiters, pollDeadline)
		}
		time.Sleep(pollInterval)
	}
}

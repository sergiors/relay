//go:build integration

// This file exercises the ownership-verification API against a real PostgreSQL
// server, where a session advisory lock lives on a specific backend that can be
// terminated server-side. It is excluded from the default suite by the
// integration build tag and REQUIRES a reachable PostgreSQL at
// TEST_DATABASE_URL (default localhost:5432, user/password/database relay, the
// address the CI service container exposes). A missing dependency FAILS rather
// than skips, matching the Redis/Docker integration convention.
package state

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestIntegrationVerifyOwnerLockDetectsTerminatedBackend proves the residual
// ownership-loss case the monitor exists for: the pinned lock connection is lost
// while the process keeps running. Terminating the pinned backend from a
// SEPARATE connection must make VerifyOwnerLock fail closed with
// ErrOwnerLockLost — even though the pool itself recovers on a fresh backend
// (so the failure is the LOST LOCK, not merely an unreachable database) — and a
// replacement incarnation of the same node must then be able to take the lock.
//
// It also pins that the check does NOT consult a replacement connection: after
// termination the pool hands out a NEW backend whose pg_backend_pid differs, yet
// VerifyOwnerLock still reports the loss because it queries only the pinned
// connection recorded at open.
func TestIntegrationVerifyOwnerLockDetectsTerminatedBackend(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()

	owner := uniqueNodeID(t, "worker-monitor")
	live := openWithIdentity(t, dsn, owner, "inst-1")
	ownerPID := live.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner OwnerBackendPID = %d, want > 0", ownerPID)
	}
	if err := live.VerifyOwnerLock(ctx); err != nil {
		t.Fatalf("VerifyOwnerLock before loss = %v, want nil", err)
	}

	// Terminate the pinned lock backend from a SEPARATE connection. In production
	// this is a terminated backend, a network partition that drops the session, or
	// an operator releasing the lock out from under the worker.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	// The pool recovers on a fresh backend (the pinned connection is checked out,
	// so a new query opens a new one), proving PostgreSQL itself is reachable and
	// the loss below is the LOCK, not connectivity.
	var freshPID int
	if err := live.db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&freshPID); err != nil {
		t.Fatalf("pool query after termination: %v", err)
	}
	if freshPID == ownerPID {
		t.Fatalf("pool reused the terminated backend pid %d; the test cannot distinguish a lost lock from a reachable pool", freshPID)
	}

	// VerifyOwnerLock queries ONLY the pinned connection, so it must fail closed
	// and must not be fooled by the pool's recovered backend.
	err = live.VerifyOwnerLock(ctx)
	if !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after backend termination = %v, want ErrOwnerLockLost", err)
	}
	// Typed classification: the query cannot run on the dead pinned backend, so
	// the failure is an UNVERIFIED connection error — it must never claim a
	// confirmed loss (lock observed gone / pid observed changed) it did not
	// observe.
	if !errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("VerifyOwnerLock after backend termination = %v, want the connection sentinel (unverified)", err)
	}
	if errors.Is(err, ErrOwnerLockNotHeld) || errors.Is(err, ErrOwnerBackendPIDChanged) {
		t.Fatalf("VerifyOwnerLock after backend termination = %v, must not claim a confirmed loss", err)
	}

	// The session advisory lock died with the backend, so a replacement
	// incarnation of the same NODE_ID can open and verify.
	replacement := openWithIdentity(t, dsn, owner, "inst-2")
	if err := replacement.VerifyOwnerLock(ctx); err != nil {
		t.Fatalf("replacement VerifyOwnerLock = %v, want nil", err)
	}
}

// TestIntegrationConcurrentVerifyOwnerLockOnPinnedConn is the regression stress
// test for sharing ONE pinned lock connection. Many concurrent VerifyOwnerLock
// callers — the ownership monitor and every startup/shutdown ownership proof run
// exactly this path — must all succeed while the backend is healthy. Before the
// connection guard, the callers interleaved database/sql's per-call driver
// accesses (a new query started while the previous result set was still being
// read), and pgx failed the check with "conn busy", which the worker misreads as
// a lost lock.
func TestIntegrationConcurrentVerifyOwnerLockOnPinnedConn(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, uniqueNodeID(t, "worker-stress"), "inst-1")

	const (
		callers = 8
		rounds  = 100
	)
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := st.VerifyOwnerLock(ctx)
				cancel()
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent VerifyOwnerLock reported %d spurious failures (first: %v) while the backend was healthy", len(errs), errs[0])
	}
}

// TestIntegrationConcurrentVerifyOwnerLockAndClose proves the connection guard
// also serializes Close against in-flight checks: closing the handle while many
// verifications run must never corrupt the connection or panic, every result is
// either a successful check (before the close) or a fail-closed
// ErrOwnerLockLost (after), and a later check still fails closed. Run with -race
// it also guards the ownerConn field access Close and VerifyOwnerLock share.
func TestIntegrationConcurrentVerifyOwnerLockAndClose(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	st := openWithIdentity(t, dsn, "worker-close", "inst-1")
	if err := st.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("initial VerifyOwnerLock = %v, want nil", err)
	}

	const callers = 8
	start := make(chan struct{})
	errCh := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := st.VerifyOwnerLock(ctx)
				cancel()
				// nil before the close, ErrOwnerLockLost after; a different
				// error would mean protocol corruption.
				if err != nil && !errors.Is(err, ErrOwnerLockLost) {
					errCh <- err
					return
				}
			}
		}()
	}
	close(start)
	if err := st.Close(); err != nil {
		t.Fatalf("close = %v, want nil", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent VerifyOwnerLock under close = %v, want nil or ErrOwnerLockLost", err)
	}
	if err := st.VerifyOwnerLock(context.Background()); !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after close = %v, want ErrOwnerLockLost", err)
	}
}

// TestOwnerBackendPIDCapture pins the backend-PID capture Open performs right
// after taking the node lock: a worker handle records the pinned connection's
// PostgreSQL backend PID, while an ownerless admin/CLI handle (which never takes
// the lock) records none.
func TestOwnerBackendPIDCapture(t *testing.T) {
	dsn := newTestDSN(t)

	owner := openWithIdentity(t, dsn, "worker-pid", "inst-1")
	if pid := owner.OwnerBackendPID(); pid <= 0 {
		t.Fatalf("owner OwnerBackendPID = %d, want > 0", pid)
	}
	if err := owner.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("VerifyOwnerLock on a held lock = %v, want nil", err)
	}

	admin := testOpenOrFail(t, dsn)
	if pid := admin.OwnerBackendPID(); pid != 0 {
		t.Fatalf("ownerless OwnerBackendPID = %d, want 0", pid)
	}
	// An ownerless handle never takes the lock, so it is never monitored and its
	// verification is a nil no-op.
	if err := admin.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("ownerless VerifyOwnerLock = %v, want nil", err)
	}
}

// TestVerifyOwnerLockFailsClosedAfterClose pins that verification fails closed
// once the handle's pinned lock connection is gone (a closed handle): the lock
// must be treated as lost, never silently assumed held.
func TestVerifyOwnerLockFailsClosedAfterClose(t *testing.T) {
	dsn := newTestDSN(t)
	owner := openWithIdentity(t, dsn, "worker-closed", "inst-1")
	if err := owner.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	err := owner.VerifyOwnerLock(context.Background())
	if !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after close = %v, want ErrOwnerLockLost", err)
	}
	// The released handle is an UNVERIFIED connection failure: no query ran, so
	// no confirmed loss was observed.
	if !errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("VerifyOwnerLock after close = %v, want the connection sentinel (unverified)", err)
	}
	if errors.Is(err, ErrOwnerLockNotHeld) || errors.Is(err, ErrOwnerBackendPIDChanged) {
		t.Fatalf("VerifyOwnerLock after close = %v, must not claim a confirmed loss", err)
	}
}

// TestIntegrationVerifyOwnerLockDetectsReleasedLock proves the CONFIRMED
// absent-lock path against a real server: releasing the session advisory lock
// on the pinned connection (an out-of-band release while the process keeps
// running) makes the next verification answer held=false and report the
// confirmed ErrOwnerLockNotHeld — while the backend and its pid stay intact,
// so no unverified or pid-change sentinel appears.
func TestIntegrationVerifyOwnerLockDetectsReleasedLock(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()

	owner := uniqueNodeID(t, "worker-released")
	live := openWithIdentity(t, dsn, owner, "inst-1")
	if err := live.VerifyOwnerLock(ctx); err != nil {
		t.Fatalf("VerifyOwnerLock on a held lock = %v, want nil", err)
	}

	// Only the holding session can release a session advisory lock, so the
	// release runs ON the pinned connection, keeping the backend and pid alive.
	live.ownerMu.Lock()
	_, unlockErr := live.ownerConn.ExecContext(ctx,
		`SELECT pg_advisory_unlock($1)`, nodeLockKey(live.nodeID))
	live.ownerMu.Unlock()
	if unlockErr != nil {
		t.Fatalf("release node lock: %v", unlockErr)
	}

	err := live.VerifyOwnerLock(ctx)
	if !errors.Is(err, ErrOwnerLockLost) || !errors.Is(err, ErrOwnerLockNotHeld) {
		t.Fatalf("VerifyOwnerLock after releasing the lock = %v, want ErrOwnerLockLost + ErrOwnerLockNotHeld", err)
	}
	if errors.Is(err, ErrOwnerBackendPIDChanged) || errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("VerifyOwnerLock after releasing the lock = %v, want a confirmed loss, not an unverified failure", err)
	}
	if !strings.Contains(err.Error(), "is no longer held") {
		t.Fatalf("error %q must name the absent lock", err)
	}
}

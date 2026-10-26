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
// replacement incarnation of the same worker must then be able to take the lock.
//
// It also pins that the check does NOT consult a replacement connection: after
// termination the pool hands out a NEW backend whose pg_backend_pid differs, yet
// VerifyOwnerLock still reports the loss because it queries only the pinned
// connection recorded at open.
func TestIntegrationVerifyOwnerLockDetectsTerminatedBackend(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()

	owner := uniqueWorkerID(t, "worker-monitor")
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
	if err := live.VerifyOwnerLock(ctx); !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after backend termination = %v, want ErrOwnerLockLost", err)
	}

	// The session advisory lock died with the backend, so a replacement
	// incarnation of the same WORKER_ID can open and verify.
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
	st := openWithIdentity(t, dsn, uniqueWorkerID(t, "worker-stress"), "inst-1")

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

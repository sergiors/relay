package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrOwnerLockLost reports that a State handle no longer holds the exclusive
// session advisory lock for its worker identity (see Open/acquireWorkerLock). It
// is fail-closed: the caller must treat the identity as no longer owned — stop
// acting as that worker (stop scheduling, cancel the lifecycle) and must not
// perform any cleanup scoped to the identity, because a replacement process may
// already own it.
var ErrOwnerLockLost = errors.New("worker ownership lock lost")

// Ownership-verification defaults. The owner lock is the cross-process guarantee
// that two workers never share a WORKER_ID; verifying it periodically detects
// the residual case where the pinned lock connection is lost while the process
// keeps running (a terminated backend, a broken link) or the lock is released
// out from under it. Detection is INTERVAL-BOUNDED, not instantaneous: the check
// runs once immediately at startup and then once per OwnerLockCheckInterval, each
// bounded by OwnerLockCheckTimeout, so a loss is normally detected within about
// interval + timeout (~3s) after it occurs, plus scheduling delay. A side effect
// scoped to the identity can therefore still happen in the window between the
// actual loss and its detection; the protection is lifecycle cancellation plus
// skipping identity-scoped cleanup AFTER detection (with the existing per-
// instance stats fence as defense in depth), not instantaneous fencing.
const (
	// OwnerLockCheckInterval is how often the ownership monitor re-verifies the
	// lock.
	OwnerLockCheckInterval = 2 * time.Second
	// OwnerLockCheckTimeout bounds a single verification query so a stalled
	// connection cannot delay detection (or shutdown) indefinitely.
	OwnerLockCheckTimeout = time.Second
)

// verifyOwnerLockQuery re-checks, server-side and on the pinned lock connection,
// that this worker identity's session advisory lock is STILL held by this exact
// backend. pg_backend_pid() is evaluated on the pinned connection, so the
// subquery is scoped to the backend this check actually ran on; the advisory key
// is reconstructed from pg_locks' classid/objid (the single-bigint
// pg_advisory_lock form stores the high 32 bits in classid and the low 32 in
// objid, with objsubid = 1). It never re-acquires the lock.
const verifyOwnerLockQuery = `
	SELECT pg_backend_pid(),
	       EXISTS (
	         SELECT 1 FROM pg_locks
	          WHERE locktype = 'advisory'
	            AND pid = pg_backend_pid()
	            AND objsubid = 1
	            AND (classid::bigint << 32) | objid::bigint = $1
	       )`

// backendPID reads the PostgreSQL backend PID of conn. It is captured once,
// immediately after the owner lock is acquired, so VerifyOwnerLock can detect a
// silently replaced connection.
func backendPID(ctx context.Context, conn *sql.Conn) (int, error) {
	var pid int
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		return 0, fmt.Errorf("read backend pid: %w", err)
	}
	return pid, nil
}

// VerifyOwnerLock performs ONE ownership check and returns nil while this handle
// still owns its worker identity's exclusive session advisory lock. It queries
// ONLY the pinned lock connection acquired by Open (ownerConn) — never the pool,
// a replacement connection, or pg_try_advisory_lock, any of which could
// re-acquire the lock and mask a loss. It fails closed with ErrOwnerLockLost on
// ANY ambiguity: a query error (the pinned backend is gone or the link is
// broken), a backend PID that differs from the one recorded at open, or the
// expected advisory lock being absent from pg_locks.
//
// It is safe to call concurrently, and concurrent calls are SERIALIZED: the
// periodic ownership monitor and every startup/shutdown ownership proof share
// ONE pinned *sql.Conn. database/sql serializes INDIVIDUAL driver calls on that
// connection but not a whole check, so without serialization two checks would
// interleave their calls and pgx would fail the overlap with "conn busy" — a
// spurious ownership loss. ownerMu makes the whole check (reading ownerConn and
// running the query) mutually exclusive with other checks and with Close, so no
// two checks overlap and the connection is never closed mid-check. The mutex
// guards only ownerConn; unrelated State operations are untouched.
//
// The check is bounded and FAILS CLOSED. A caller that is queued behind an
// in-flight check waits for the mutex (the mutex itself is not context-aware);
// the in-flight check is bounded by the context ITS OWN caller supplied, so the
// queue waits at most that timeout. Once it acquires the mutex, the query runs
// under the caller's context: if that context has already expired (queued until
// the timeout) the query fails immediately and the check fails closed with
// ErrOwnerLockLost. A caller should therefore pass a bounded context (see
// OwnerLockCheckTimeout).
//
// It returns nil for an ownerless CLI/admin handle, which never takes the lock
// and is therefore never monitored. This method performs a single query and does
// not itself retry or sleep.
func (st *State) VerifyOwnerLock(ctx context.Context) error {
	if st == nil || st.owner == "" {
		return nil
	}
	// Serialize ALL access to the pinned lock connection: concurrent checks must
	// never query the same *sql.Conn at once, and the read of ownerConn must not
	// race Close clearing it. The mutex is released only after the query returns.
	st.ownerMu.Lock()
	defer st.ownerMu.Unlock()
	conn := st.ownerConn
	if conn == nil {
		return fmt.Errorf("%w: lock connection already released", ErrOwnerLockLost)
	}
	var (
		pid  int
		held bool
	)
	if err := conn.QueryRowContext(ctx, verifyOwnerLockQuery, workerLockKey(st.owner)).Scan(&pid, &held); err != nil {
		return fmt.Errorf("%w: verify on lock connection: %w", ErrOwnerLockLost, err)
	}
	if st.ownerPID > 0 && pid != st.ownerPID {
		return fmt.Errorf("%w: lock-connection backend pid changed from %d to %d", ErrOwnerLockLost, st.ownerPID, pid)
	}
	if !held {
		return fmt.Errorf("%w: session advisory lock for worker %q is no longer held", ErrOwnerLockLost, st.owner)
	}
	return nil
}

// OwnerBackendPID returns the PostgreSQL backend PID (pg_backend_pid) of the
// handle's pinned lock connection, captured when Open acquired the ownership
// lock, or 0 for an ownerless handle. It is exposed for diagnostics and for an
// operator/caller that needs to correlate the lock with a server-side backend
// (for example, to release it administratively).
func (st *State) OwnerBackendPID() int {
	if st == nil {
		return 0
	}
	return st.ownerPID
}

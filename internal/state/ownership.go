package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Ownership verification failures all wrap ErrOwnerLockLost, so existing
// errors.Is callers keep failing closed. The narrower identities below let a
// caller distinguish a CONFIRMED loss (the lock is proven gone or the pinned
// backend changed) from an UNVERIFIED failure (a timeout, a broken connection,
// or any other query error) without inspecting error text.
var (
	// ErrOwnerLockLost reports that a State handle no longer holds the exclusive
	// session advisory lock for its node identity (see Open/acquireNodeLock). It
	// is fail-closed: the caller must treat the identity as no longer owned — stop
	// acting as that worker (stop scheduling, cancel the lifecycle) and must not
	// perform any cleanup scoped to the identity, because a replacement process may
	// already own it.
	ErrOwnerLockLost = errors.New("node ownership lock lost")
	// ErrOwnerLockNotHeld is a CONFIRMED loss: the pinned backend answered and
	// reported that the session advisory lock is no longer held.
	ErrOwnerLockNotHeld = errors.New("node ownership lock not held")
	// ErrOwnerBackendPIDChanged is a CONFIRMED loss: the pinned connection now
	// reports a different PostgreSQL backend PID, so it no longer holds the lock
	// this handle acquired.
	ErrOwnerBackendPIDChanged = errors.New("node ownership backend pid changed")
	// ErrOwnerLockConnection marks an UNVERIFIED failure: the check could not
	// query the pinned connection because it is closed or failed with a
	// PostgreSQL/network connection error. It never means the lock is known lost.
	ErrOwnerLockConnection = errors.New("node ownership lock connection error")
)

// isOwnerLockConnectionError reports whether err is a PostgreSQL or network
// connection failure. It uses typed errors/error interfaces only: no message
// matching. SQLSTATE class 08 (connection exception) and the shutdown states
// 57P01-57P03 are the server-reported connection failures.
func isOwnerLockConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var sqlState interface{ SQLState() string }
	if errors.As(err, &sqlState) {
		state := sqlState.SQLState()
		if strings.HasPrefix(state, "08") ||
			state == "57P01" || state == "57P02" || state == "57P03" {
			return true
		}
	}
	for _, errno := range []error{
		syscall.ECONNREFUSED,
		syscall.ECONNRESET,
		syscall.EPIPE,
		syscall.ETIMEDOUT,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

// Ownership-verification defaults. The node lock is the cross-process guarantee
// that two nodes never share a NODE_ID; verifying it periodically detects
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
// that this node identity's session advisory lock is STILL held by this exact
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
// immediately after the node lock is acquired, so VerifyOwnerLock can detect a
// silently replaced connection.
func backendPID(ctx context.Context, conn *sql.Conn) (int, error) {
	var pid int
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		return 0, fmt.Errorf("read backend pid: %w", err)
	}
	return pid, nil
}

// VerifyOwnerLock performs ONE ownership check and returns nil while this handle
// still owns its node identity's exclusive session advisory lock. It queries
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
// The check is bounded and FAILS CLOSED. It waits for ownerMu only while the
// caller's context remains live, so time queued behind another check or the
// bounded deployment-acquisition statement is charged to this check's own
// budget. The statement includes its implicit commit and uses a bounded query
// context; a queued check nevertheless returns at its own deadline instead of
// blocking indefinitely. Once it acquires the mutex, the query also runs under
// that context. A caller should therefore pass a bounded context (see
// OwnerLockCheckTimeout).
//
// It returns nil for an ownerless CLI/admin handle, which never takes the lock
// and is therefore never monitored. This method performs a single query and does
// not itself retry or sleep. AcquireDeploymentNode performs an equivalent
// pg_locks proof in the same autocommit statement as its write on the pinned
// backend.
//
// Every failure wraps ErrOwnerLockLost so existing errors.Is callers keep
// failing closed. Callers that need to distinguish a CONFIRMED loss from an
// unverified failure inspect the narrower identities: ErrOwnerLockNotHeld and
// ErrOwnerBackendPIDChanged accompany a proven loss, while
// ErrOwnerLockConnection accompanies a closed/broken PostgreSQL connection and a
// timeout remains visible as context.DeadlineExceeded. Other query errors carry
// only ErrOwnerLockLost and are unverified.
func (st *State) VerifyOwnerLock(ctx context.Context) error {
	if st == nil || st.nodeID == "" {
		return nil
	}
	// Serialize ALL access to the pinned lock connection: concurrent checks must
	// never query the same *sql.Conn at once, and the read of ownerConn must not
	// race Close clearing it. The mutex is released only after the query returns.
	if err := lockOwnerConnection(ctx, &st.ownerMu); err != nil {
		return fmt.Errorf("%w: verify on lock connection: %w", ErrOwnerLockLost, err)
	}
	defer st.ownerMu.Unlock()
	conn := st.ownerConn
	if conn == nil {
		return fmt.Errorf(
			"%w: %w: lock connection already released",
			ErrOwnerLockLost, ErrOwnerLockConnection,
		)
	}
	var (
		pid  int
		held bool
	)
	if err := conn.QueryRowContext(
		ctx, verifyOwnerLockQuery, nodeLockKey(st.nodeID),
	).Scan(&pid, &held); err != nil {
		// A query failure is never a confirmed lock loss: classify a known
		// PostgreSQL/network failure as a connection error and leave every other
		// query error as an unverified failure. The cause stays wrapped so a
		// caller can still classify a timeout before the connection sentinel.
		if isOwnerLockConnectionError(err) {
			return fmt.Errorf(
				"%w: %w: verify on lock connection: %w",
				ErrOwnerLockLost, ErrOwnerLockConnection, err,
			)
		}
		return fmt.Errorf("%w: verify on lock connection: %w", ErrOwnerLockLost, err)
	}
	if st.ownerPID > 0 && pid != st.ownerPID {
		return fmt.Errorf(
			"%w: %w: lock-connection backend pid changed from %d to %d",
			ErrOwnerLockLost, ErrOwnerBackendPIDChanged, st.ownerPID, pid,
		)
	}
	if !held {
		return fmt.Errorf("%w: %w: session advisory lock for node %q is no longer held",
			ErrOwnerLockLost, ErrOwnerLockNotHeld, st.nodeID)
	}
	return nil
}

func lockOwnerConnection(ctx context.Context, mu *sync.Mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if mu.TryLock() {
				return nil
			}
		}
	}
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

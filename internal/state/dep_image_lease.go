package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// dependencyImageLockNamespace is the high-order key of the per-dependency-image
// advisory locks. It partitions them from appLockNamespace (1) and from the
// single-bigint schema/worker locks, so a dependency reference can never alias
// an app's snapshot lock or a worker's ownership lock. The pair
// (dependencyImageLockNamespace, dependencyImageLockKey) is the two-int form of
// the PostgreSQL advisory locks; PostgreSQL stores it in pg_locks with
// objsubid = 2 (the single-bigint form uses objsubid = 1).
const dependencyImageLockNamespace int32 = 2

// DependencyImageUseContentionTimeout bounds how long a shared dependency-image
// use acquisition waits for an exclusive GC hold to clear before failing with
// ErrDependencyImageBusy. It is deliberately short: a caller that builds an app
// image retries the whole preparation, so a bounded retryable failure is
// preferable to wedging a reconcile worker (or startup preparation) behind a
// slow GC pass. The wait is always the smaller of this bound and the caller's
// context deadline.
const DependencyImageUseContentionTimeout = 5 * time.Second

// Contention polling for shared use acquisition and the outer context bound for
// lease release. PostgreSQL's advisory-lock functions have no context-aware
// blocking form through database/sql, so acquisition polls a non-blocking
// try-lock and checks ctx between attempts. Each query has the shorter
// dependencyQueryTimeout below. The query itself is never held under a lock
// while the caller waits, so a slow database cannot stall the caller's other
// coordination work.
const (
	dependencyImageAcquireMinBackoff = 5 * time.Millisecond
	dependencyImageAcquireMaxBackoff = 100 * time.Millisecond
	dependencyImageReleaseTimeout    = 5 * time.Second
)

// dependencyQueryTimeout bounds ONE dependency-image coordination query, and the
// lazy ownerless connection allocation (dial + backend-PID read), to the same
// short bound the ownership check already uses. Every dependency query runs on
// the worker's pinned ownership connection while ownerMu is held (or on an
// ownerless handle's pinned connection while depConnMu is held), so an unbounded
// query against a wedged database or network would block VerifyOwnerLock and
// delay ownership-loss detection, or hold depConnMu indefinitely. The bound is
// always the smaller of this and the caller's context deadline, and it is never
// held across a contention wait: the acquisition poll sleeps outside the mutexes.
//
// It does NOT change the shared-acquisition budget: an acquisition refused by
// contention keeps polling until DependencyImageUseContentionTimeout, while a
// query that hits this bound is a coordination failure and fails closed
// immediately (a build retries; GC removes nothing). A query canceled exactly as
// the server executed it can leave a session advisory lock unaccounted for; that
// is always conservative (a layer stays pinned until the handle closes, never
// removed early) and does not affect cross-process safety.
const dependencyQueryTimeout = OwnerLockCheckTimeout

// Dependency-image coordination errors. Every one of them is FAIL-CLOSED for
// garbage collection: a GC pass must remove nothing when it cannot prove it
// holds the exclusive view.
var (
	// ErrDependencyImageCoordinationUnavailable reports that dependency-image
	// coordination cannot run on this handle: the handle is closed, an ownerful
	// handle's pinned ownership connection is gone (the existing ownership
	// monitor cancels the lifecycle in that case), or a connection could not be
	// established. A build fails retryably; a GC pass fails closed.
	ErrDependencyImageCoordinationUnavailable = errors.New(
		"state: dependency image coordination unavailable")
	// ErrDependencyImageBusy reports that a shared use acquisition could not be
	// granted within DependencyImageUseContentionTimeout because an exclusive GC
	// hold (local or cross-process) is active. It is retryable.
	ErrDependencyImageBusy = errors.New("state: dependency image use is busy")
	// ErrDependencyImageLeaseLost reports that a lease no longer protects its
	// reference: it was released, its handle closed, the backend changed, or the
	// session advisory lock is no longer held. Callers must fail closed.
	ErrDependencyImageLeaseLost = errors.New("state: dependency image lease lost")
)

// DependencyImageLease is one admitted cross-process reference to a dependency
// image reference. It is the database-backed counterpart of the runtime's
// per-Manager image lease: a shared lease keeps the layer from being
// garbage-collected by any Relay process sharing this database, and an exclusive
// lease gives the GC one process's exclusive view for a removal decision.
//
// It is safe for concurrent use. Verify reports whether the underlying session
// advisory lock is still held by the exact backend that acquired it; Release is
// idempotent. Neither claims instantaneous fencing: PostgreSQL releases session
// locks when the backend disappears, and the residual window between a Verify
// and the following Docker call cannot be closed from the database side.
type DependencyImageLease interface {
	Verify(ctx context.Context) error
	Release() error
}

// dependencyImageLeaseMode is the lock mode a lease was admitted with.
type dependencyImageLeaseMode int

const (
	dependencyImageUseLease dependencyImageLeaseMode = iota
	dependencyImageGCLease
)

// advisoryMode is the pg_locks mode string for a lease mode.
func (m dependencyImageLeaseMode) advisoryMode() string {
	if m == dependencyImageGCLease {
		return "ExclusiveLock"
	}
	return "ShareLock"
}

// dependencyImageLockKey maps a full dependency image reference to the low-order
// half of its advisory-lock key. The mapping only needs to be stable across
// processes and restarts. It is a 32-bit hash, so two distinct references CAN
// collide onto one key; that is safe because the advisory lock (namespace + key)
// — not the reference string — is the coordination identity: collisions make the
// two references share one lock, so uses of both serialize against one GC hold
// and all local bookkeeping is keyed by that same lock identity. The reference is
// content-addressed by construction, so the full reference is the stable input.
func dependencyImageLockKey(depRef string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(depRef))
	return int32(h.Sum32())
}

// dependencyImageLockID is the PostgreSQL advisory-lock identity a dependency
// reference maps to. Local lease bookkeeping and the local use/exclusive conflict
// check are keyed by it, NOT by the full reference, because the lock is the
// coordination identity: two colliding references share one advisory lock, and
// PostgreSQL grants a session a conflicting mode on a lock it already holds. If
// local bookkeeping were keyed by the reference string, a colliding pair would
// bypass the local gate and let the same session take an exclusive GC hold while
// it still holds a shared use of the colliding reference.
type dependencyImageLockID struct {
	namespace int32
	key       int32
}

// dependencyImageLockIDFor maps a dependency reference to its advisory-lock
// identity.
func dependencyImageLockIDFor(depRef string) dependencyImageLockID {
	return dependencyImageLockID{
		namespace: dependencyImageLockNamespace,
		key:       dependencyImageLockKey(depRef),
	}
}

// dependencyLeaseState is one handle's local view of the dependency-image locks
// it holds on its coordination session, keyed by advisory-lock identity (see
// dependencyImageLockID) so colliding references conservatively serialize
// locally exactly as they do in PostgreSQL.
//
// PostgreSQL grants a session a CONFLICTING advisory-lock mode on a key it
// already holds (verified: exclusive-then-shared and shared-then-exclusive both
// succeed within one session), so the database alone cannot stop one handle from
// using a layer and garbage-collecting it at the same time. The local view is
// therefore load-bearing, and it is consulted and mutated only while the
// handle's depGateMu is held, which is what makes "no local exclusive while
// local uses exist" and "no local use while a local exclusive is held"
// race-free. PostgreSQL remains the CROSS-process authority: every use lease
// takes its own counted shared session lock and every GC lease the exclusive one.
type dependencyLeaseState struct {
	// uses counts the shared use leases this handle currently holds per lock
	// identity. Each one owns one counted session lock.
	uses map[dependencyImageLockID]int
	// gc marks the lock identities for which this handle holds the exclusive GC
	// lock.
	gc map[dependencyImageLockID]bool
}

// AcquireDependencyImageUse admits a shared use lease for depRef.
//
// It fails closed on any coordination error. Contention with an exclusive hold
// (local or cross-process) is waited out for at most
// DependencyImageUseContentionTimeout and then reported as a retryable
// ErrDependencyImageBusy. An already-cancelled context grants nothing. The wait
// happens outside the handle's coordination gate, so one contended reference
// never blocks local coordination of another.
//
// The returned lease is independent: two acquisitions for the same reference
// (even on this same handle) each hold one counted session lock, so releasing
// one never releases the other. The caller MUST release it on every path.
func (st *State) AcquireDependencyImageUse(
	ctx context.Context,
	depRef string,
) (DependencyImageLease, error) {
	if depRef == "" {
		return nil, fmt.Errorf("acquire dependency image use: empty dependency reference")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(DependencyImageUseContentionTimeout)
	backoff := dependencyImageAcquireMinBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lease, err := st.tryAcquireDependencyImageUse(ctx, depRef)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("%w: %s", ErrDependencyImageBusy, depRef)
		}
		if backoff > remaining {
			backoff = remaining
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff < dependencyImageAcquireMaxBackoff {
			backoff *= 2
			if backoff > dependencyImageAcquireMaxBackoff {
				backoff = dependencyImageAcquireMaxBackoff
			}
		}
	}
}

// tryAcquireDependencyImageUse runs ONE gated shared acquisition attempt. It
// returns a lease on success and (nil, nil) when an exclusive hold is active.
//
// The handle gate serializes the local decision with the session query, which is
// required because PostgreSQL grants this session a shared lock even while the
// same session holds the exclusive one. Both the local check and the bookkeeping
// use the advisory-lock identity, so a colliding reference is treated as the same
// lock locally too.
func (st *State) tryAcquireDependencyImageUse(
	ctx context.Context,
	depRef string,
) (DependencyImageLease, error) {
	lockID := dependencyImageLockIDFor(depRef)
	st.depGateMu.Lock()
	defer st.depGateMu.Unlock()
	localGC, err := st.localDependencyGCHeld(lockID)
	if err != nil {
		return nil, err
	}
	if localGC {
		return nil, nil
	}
	locked, pid, err := st.tryDependencyLock(ctx, lockID, true)
	if err != nil {
		return nil, fmt.Errorf("acquire dependency image use %s: %w", depRef, err)
	}
	if !locked {
		return nil, nil
	}
	if err := st.noteDependencyUse(lockID); err != nil {
		// The handle closed between the query and the bookkeeping: drop the
		// just-taken lock immediately rather than leaking it.
		st.dropDependencyLock(lockID, true)
		return nil, err
	}
	return &dependencyImageLease{
		st:      st,
		ref:     depRef,
		lockID:  lockID,
		mode:    dependencyImageUseLease,
		backend: pid,
	}, nil
}

// TryAcquireDependencyImageGC takes the exclusive dependency-image view for one
// candidate removal. It NEVER waits: acquired == false with a nil error means
// either this handle or another coordination session still uses the layer, and
// the caller (GC) must skip the candidate and retry at the next natural
// lifecycle point. Any error is fail-closed: GC must remove nothing.
//
// The local decision and the session query run under the handle gate, so a local
// use cannot be admitted between them (PostgreSQL would otherwise grant the
// exclusive lock to the same session that already holds a shared one). The
// decision is keyed by advisory-lock identity, so a use of a reference that
// collides onto the same key also refuses this GC hold.
//
// The returned lease must be released on every path. A caller must not run any
// blocking wait on other work while holding it.
func (st *State) TryAcquireDependencyImageGC(
	ctx context.Context,
	depRef string,
) (DependencyImageLease, bool, error) {
	if depRef == "" {
		return nil, false, fmt.Errorf("acquire dependency image GC: empty dependency reference")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	lockID := dependencyImageLockIDFor(depRef)
	st.depGateMu.Lock()
	defer st.depGateMu.Unlock()

	st.depMu.Lock()
	if st.depClosed {
		st.depMu.Unlock()
		return nil, false, ErrDependencyImageCoordinationUnavailable
	}
	localUses := 0
	localGC := false
	if st.depLeases != nil {
		localUses = st.depLeases.uses[lockID]
		localGC = st.depLeases.gc[lockID]
	}
	st.depMu.Unlock()
	if localUses > 0 || localGC {
		return nil, false, nil
	}

	locked, pid, err := st.tryDependencyLock(ctx, lockID, false)
	if err != nil {
		return nil, false, fmt.Errorf("acquire dependency image GC %s: %w", depRef, err)
	}
	if !locked {
		return nil, false, nil
	}
	if err := st.noteDependencyGC(lockID); err != nil {
		st.dropDependencyLock(lockID, false)
		return nil, false, err
	}
	return &dependencyImageLease{
		st:      st,
		ref:     depRef,
		lockID:  lockID,
		mode:    dependencyImageGCLease,
		backend: pid,
	}, true, nil
}

// dependencyImageLease is the concrete lease. It tracks only its own released
// state; the shared session lock count is tracked by its State under the lease's
// advisory-lock identity (lockID). ref is retained for diagnostics only.
type dependencyImageLease struct {
	st      *State
	ref     string
	lockID  dependencyImageLockID
	mode    dependencyImageLeaseMode
	backend int

	mu       sync.Mutex
	released bool
}

// Verify reports whether this lease still protects its reference: it must not
// have been released, its handle must still be open, and the session advisory
// lock must still be held by the exact backend that acquired it. Any ambiguity
// is an error (fail closed).
func (l *dependencyImageLease) Verify(ctx context.Context) error {
	if l == nil {
		return ErrDependencyImageLeaseLost
	}
	l.mu.Lock()
	released := l.released
	l.mu.Unlock()
	if released {
		return fmt.Errorf("%w: released", ErrDependencyImageLeaseLost)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var (
		pid  int
		held bool
	)
	err := l.st.withDependencyCoordConn(ctx, func(queryCtx context.Context, conn *sql.Conn, _ int) error {
		// pg_locks stores the two-int advisory key in classid/objid as unsigned
		// OIDs, while the FNV hash is a signed int32. Compare in the unsigned
		// space the catalog uses, or any negative key would never verify.
		return conn.QueryRowContext(queryCtx, dependencyImageVerifyQuery,
			int64(uint32(l.lockID.namespace)),
			int64(uint32(l.lockID.key)),
			l.mode.advisoryMode(),
		).Scan(&pid, &held)
	})
	if err != nil {
		return fmt.Errorf("%w: verify on coordination connection: %w",
			ErrDependencyImageLeaseLost, err)
	}
	if l.backend > 0 && pid != l.backend {
		return fmt.Errorf("%w: coordination backend pid changed from %d to %d",
			ErrDependencyImageLeaseLost, l.backend, pid)
	}
	if !held {
		return fmt.Errorf("%w: session advisory lock for %s is no longer held",
			ErrDependencyImageLeaseLost, l.ref)
	}
	return nil
}

// Release drops this lease's session lock and local bookkeeping. It is
// idempotent and nil-safe. A handle that has already closed has released every
// session lock with its connection, so Release then returns nil without a query.
//
// A release that cannot reach the database is reported: the layer may stay
// pinned on a live session until the handle closes, so the caller must not treat
// the layer as free. The local view is always cleared so a later Close is clean.
func (l *dependencyImageLease) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	l.mu.Unlock()

	st := l.st
	if st == nil {
		return nil
	}
	// The handle gate makes the release's session unlock atomic with the local
	// bookkeeping, so no local acquisition can interleave and observe a count the
	// session does not hold (or vice versa).
	st.depGateMu.Lock()
	defer st.depGateMu.Unlock()
	if st.dependencyCoordinationClosed() {
		// The handle closed: its connection already released every lock.
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), dependencyImageReleaseTimeout)
	defer cancel()
	err := st.unlockDependencyLock(ctx, l.lockID, l.mode == dependencyImageUseLease)
	st.forgetDependencyLease(l.lockID, l.mode)
	if err != nil {
		// The coordination connection is gone (or closing): PostgreSQL released
		// the lock with the session, so this is not an outstanding lease. Every
		// other failure is reported because the layer may stay pinned until the
		// handle closes.
		if errors.Is(err, ErrDependencyImageCoordinationUnavailable) ||
			errors.Is(err, sql.ErrConnDone) {
			return nil
		}
		return fmt.Errorf("release dependency image lease %s: %w", l.ref, err)
	}
	return nil
}

// dependencyImageVerifyQuery re-checks, server-side, that the current session
// still holds the two-int dependency-image advisory lock for the supplied
// (namespace, key) pair in the supplied mode. pg_backend_pid() is evaluated on
// the coordination connection itself, so the check is scoped to the backend it
// actually ran on. It never re-acquires the lock.
const dependencyImageVerifyQuery = `
	SELECT pg_backend_pid(),
	       EXISTS (
	         SELECT 1 FROM pg_locks
	          WHERE locktype = 'advisory'
	            AND pid = pg_backend_pid()
	            AND objsubid = 2
	            AND classid::bigint = $1
	            AND objid::bigint = $2
	            AND mode = $3
	            AND granted
	       )`

// tryDependencyLock attempts one non-blocking advisory lock for lockID and
// reports whether it was granted, with the coordination backend's PID.
func (st *State) tryDependencyLock(
	ctx context.Context,
	lockID dependencyImageLockID,
	shared bool,
) (bool, int, error) {
	query := `SELECT pg_try_advisory_lock($1, $2)`
	if shared {
		query = `SELECT pg_try_advisory_lock_shared($1, $2)`
	}
	var locked bool
	pid := 0
	err := st.withDependencyCoordConn(ctx, func(queryCtx context.Context, conn *sql.Conn, backend int) error {
		if err := conn.QueryRowContext(queryCtx, query,
			lockID.namespace, lockID.key).Scan(&locked); err != nil {
			return err
		}
		pid = backend
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	return locked, pid, nil
}

// unlockDependencyLock releases one counted session lock for lockID. The
// returned boolean is deliberately ignored: a false result means the session no
// longer held the lock (the connection was lost or already released it), which
// Release treats as success. Real query failures are returned so the caller can
// report that the layer may still be pinned.
func (st *State) unlockDependencyLock(ctx context.Context, lockID dependencyImageLockID, shared bool) error {
	query := `SELECT pg_advisory_unlock($1, $2)`
	if shared {
		query = `SELECT pg_advisory_unlock_shared($1, $2)`
	}
	return st.withDependencyCoordConn(ctx, func(queryCtx context.Context, conn *sql.Conn, _ int) error {
		var unlocked bool
		if err := conn.QueryRowContext(queryCtx, query,
			lockID.namespace, lockID.key).Scan(&unlocked); err != nil {
			return err
		}
		return nil
	})
}

// dropDependencyLock releases a just-acquired lock whose local bookkeeping could
// not be recorded. It is best-effort: if the unlock query fails, the lock stays
// on the session until the handle closes, which is fail-closed (a layer stays
// pinned, never removed).
func (st *State) dropDependencyLock(lockID dependencyImageLockID, shared bool) {
	ctx, cancel := context.WithTimeout(context.Background(), dependencyImageReleaseTimeout)
	defer cancel()
	_ = st.unlockDependencyLock(ctx, lockID, shared)
}

// withDependencyCoordConn runs fn on the connection dependency-image
// coordination uses and reports that connection's backend PID.
//
// An ownerful handle reuses the worker's pinned owner-lock connection, so
// dependency leases live on the same session whose loss the ownership monitor
// already treats as fatal; fn runs under ownerMu, so it is serialized with
// VerifyOwnerLock and Close. An ownerless handle (CLI, tests) lazily pins its own
// connection, and ALL of its queries and its closure are serialized by
// depConnMu: database/sql serializes individual driver calls but not a whole
// query-and-scan, so two overlapping calls — a Verify that does not take the
// lease gate racing another Verify, an Acquire, or Close — make pgx fail with
// "conn busy" and can poison the pinned connection. In both cases the mutex is
// held only for the individual query: callers must never wait while holding it.
//
// The context handed to fn is bounded by dependencyQueryTimeout (and by the
// caller's deadline, if shorter), so a wedged database or network can hold
// ownerMu — and therefore delay VerifyOwnerLock and ownership-loss detection — or
// depConnMu for at most that short bound. The bound starts before the mutex is
// acquired, so a call queued behind another query is bounded end-to-end: if the
// queue wait consumes the budget the query fails closed immediately instead of
// starting. The lazy ownerless connection allocation runs under the same bound.
// The mutex is never held across the acquisition poll/backoff, which lives in the
// caller.
func (st *State) withDependencyCoordConn(
	ctx context.Context,
	fn func(queryCtx context.Context, conn *sql.Conn, backendPID int) error,
) error {
	queryCtx, cancel := context.WithTimeout(ctx, dependencyQueryTimeout)
	defer cancel()
	if st.owner != "" {
		st.ownerMu.Lock()
		defer st.ownerMu.Unlock()
		if st.ownerConn == nil {
			return ErrDependencyImageCoordinationUnavailable
		}
		return fn(queryCtx, st.ownerConn, st.ownerPID)
	}
	st.depConnMu.Lock()
	defer st.depConnMu.Unlock()
	conn, pid, err := st.ownerlessDependencyConn(queryCtx)
	if err != nil {
		return err
	}
	return fn(queryCtx, conn, pid)
}

// ownerlessDependencyConn returns the handle's pinned coordination connection,
// allocating it on first use. The caller holds depConnMu, which serializes this
// lookup, the connection's queries, and its closure. ctx is already bounded by
// dependencyQueryTimeout, so the dial and the backend-PID read cannot hold
// depConnMu indefinitely. The connection is pinned for the handle's lifetime so a
// session advisory lock can never be handed back to the pool with a recycled
// connection; Close releases it.
func (st *State) ownerlessDependencyConn(ctx context.Context) (*sql.Conn, int, error) {
	st.depMu.Lock()
	if st.depClosed {
		st.depMu.Unlock()
		return nil, 0, ErrDependencyImageCoordinationUnavailable
	}
	conn, pid := st.depConn, st.depPID
	st.depMu.Unlock()
	if conn != nil {
		return conn, pid, nil
	}

	// Dial outside depMu so a slow connect does not block local bookkeeping for
	// other references. The double-check below discards a racing loser.
	fresh, err := st.db.Conn(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("dependency image coordination: pin connection: %w", err)
	}
	freshPID, err := backendPID(ctx, fresh)
	if err != nil {
		_ = fresh.Close()
		return nil, 0, fmt.Errorf("dependency image coordination: %w", err)
	}

	st.depMu.Lock()
	defer st.depMu.Unlock()
	if st.depClosed {
		_ = fresh.Close()
		return nil, 0, ErrDependencyImageCoordinationUnavailable
	}
	if st.depConn != nil {
		winner, winnerPID := st.depConn, st.depPID
		_ = fresh.Close()
		return winner, winnerPID, nil
	}
	st.depConn, st.depPID = fresh, freshPID
	return fresh, freshPID, nil
}

// localDependencyGCHeld reports whether this handle holds the exclusive GC lock
// for lockID, and fails when the handle is closed.
func (st *State) localDependencyGCHeld(lockID dependencyImageLockID) (bool, error) {
	st.depMu.Lock()
	defer st.depMu.Unlock()
	if st.depClosed {
		return false, ErrDependencyImageCoordinationUnavailable
	}
	if st.depLeases == nil {
		return false, nil
	}
	return st.depLeases.gc[lockID], nil
}

// noteDependencyUse records one local shared use of lockID. It fails when the
// handle closed while the lock query ran, so the caller can drop the lock.
func (st *State) noteDependencyUse(lockID dependencyImageLockID) error {
	st.depMu.Lock()
	defer st.depMu.Unlock()
	if st.depClosed {
		return ErrDependencyImageCoordinationUnavailable
	}
	if st.depLeases == nil {
		st.depLeases = newDependencyLeaseState()
	}
	st.depLeases.uses[lockID]++
	return nil
}

// noteDependencyGC records the exclusive GC hold on lockID, failing when the
// handle closed while the lock query ran.
func (st *State) noteDependencyGC(lockID dependencyImageLockID) error {
	st.depMu.Lock()
	defer st.depMu.Unlock()
	if st.depClosed {
		return ErrDependencyImageCoordinationUnavailable
	}
	if st.depLeases == nil {
		st.depLeases = newDependencyLeaseState()
	}
	st.depLeases.gc[lockID] = true
	return nil
}

// forgetDependencyLease clears the local record for one released lease. The
// caller holds the handle gate, so no concurrent acquisition can observe the
// intermediate state. It is keyed by advisory-lock identity, so releasing a
// lease for one of two colliding references decrements their shared count.
func (st *State) forgetDependencyLease(lockID dependencyImageLockID, mode dependencyImageLeaseMode) {
	st.depMu.Lock()
	defer st.depMu.Unlock()
	if st.depClosed || st.depLeases == nil {
		return
	}
	switch mode {
	case dependencyImageUseLease:
		if uses := st.depLeases.uses[lockID]; uses > 1 {
			st.depLeases.uses[lockID] = uses - 1
		} else {
			delete(st.depLeases.uses, lockID)
		}
	case dependencyImageGCLease:
		delete(st.depLeases.gc, lockID)
	}
}

// dependencyCoordinationClosed reports whether Close has released this handle's
// dependency coordination. The caller holds no other dependency mutex.
func (st *State) dependencyCoordinationClosed() bool {
	st.depMu.Lock()
	defer st.depMu.Unlock()
	return st.depClosed
}

// closeDependencyCoordination is the Close hook: it marks coordination closed,
// clears the local view, and closes an ownerless pinned connection (an ownerful
// handle's locks are released with ownerConn). It never fails the caller; Close
// reports only the pool close.
//
// The ownerless connection is extracted and closed under depConnMu, the same
// mutex every ownerless query holds, so Close waits for an in-flight query and no
// later query can start on the closed connection. Lock order is depConnMu ->
// depMu (the same order withDependencyCoordConn uses through
// ownerlessDependencyConn); no path holds depMu and then takes depConnMu.
func (st *State) closeDependencyCoordination() {
	st.depMu.Lock()
	st.depClosed = true
	st.depLeases = nil
	st.depMu.Unlock()

	st.depConnMu.Lock()
	defer st.depConnMu.Unlock()
	st.depMu.Lock()
	conn := st.depConn
	st.depConn = nil
	st.depPID = 0
	st.depMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// newDependencyLeaseState returns an empty local dependency-lease view.
func newDependencyLeaseState() *dependencyLeaseState {
	return &dependencyLeaseState{
		uses: map[dependencyImageLockID]int{},
		gc:   map[dependencyImageLockID]bool{},
	}
}

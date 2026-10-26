//go:build integration

// Contract tests for the State-backed dependency-image lease API (IR-004).
// They exercise real PostgreSQL session advisory locks across TWO independent
// State handles on the SAME database, so process-local coordination cannot
// satisfy them: hold one shared lease, the exclusive attempt from the other
// handle must lose; release, and it must win. Release, Verify, cancellation,
// and Close/lifecycle hygiene are covered at the same boundary.
//
// Method names follow the landed production contract (state.State's
// dependency-image lease API in internal/state/dep_image_lease.go); any
// future production drift adjusts only the call sites in this file.
//
// Like the rest of the state integration suite, these tests require a reachable
// PostgreSQL at TEST_DATABASE_URL and FAIL rather than skip without one
// (see testutil.RequireStateDatabase).
package state_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"relay/internal/state"
	"relay/internal/testutil"
)

// ir004RunToken is unique per test-binary process: PostgreSQL advisory locks
// are database-wide (NewStateDSN's per-test schemas do NOT namespace them), so
// without this token two concurrent `go test` binaries could contend on one of
// these references. It is generated once via crypto/rand and appended by
// leaseRefFor; each test derives references ONLY through it, so intra-test
// sharing (same-ref coordination across handles/processes) is unchanged while
// cross-run collisions become impossible in practice.
var ir004RunToken = func() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; failing loudly is
		// strictly better than a silent shared-key flake risk.
		panic("state_test: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}()

// leaseRefFor builds a dependency reference unique per scenario AND per
// test-binary run: "relay-dep-ir004-<scenario>-<ir004RunToken>".
func leaseRefFor(suffix string) string {
	return "relay-dep-ir004-" + suffix + "-" + ir004RunToken
}

// newSharedStatePair opens two independent State handles on ONE fresh schema,
// so every assertion below runs against real cross-handle PostgreSQL state.
func newSharedStatePair(t *testing.T) (*state.State, *state.State) {
	t.Helper()
	dsn := testutil.NewStateDSN(t)
	return testutil.OpenState(t, dsn), testutil.OpenState(t, dsn)
}

// TestIntegrationDependencyLeaseSharedConcurrency proves two independent
// handles on one DB can each hold a shared use lease on the SAME dependency
// image simultaneously, and both must be released before anything else's
// exclusive view exists.
func TestIntegrationDependencyLeaseSharedConcurrency(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("shared")

	// Both handles acquire concurrently (real advisory-lock contention; the
	// outcome must be success for BOTH, no serialization), alternating between
	// the two independent handles so the grant is never handle-local.
	const holders = 4
	handles := []*state.State{a, b}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		leases   []state.DependencyImageLease
		firstErr error
	)
	for i := 0; i < holders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l, err := handles[i%len(handles)].AcquireDependencyImageUse(ctx, ref)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			leases = append(leases, l)
		}(i)
	}
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("concurrent shared acquisition failed: %v", firstErr)
	}
	if len(leases) != holders {
		t.Fatalf("granted leases = %d, want %d", len(leases), holders)
	}
	// Every granted lease verifies while held.
	for i, l := range leases {
		if err := l.Verify(ctx); err != nil {
			t.Fatalf("lease %d Verify while held: %v", i, err)
		}
	}
	// All shared holders must release without error, and release is idempotent.
	for i, l := range leases {
		if err := l.Release(); err != nil {
			t.Fatalf("lease %d Release: %v", i, err)
		}
		if err := l.Release(); err != nil {
			t.Fatalf("lease %d idempotent Release: %v", i, err)
		}
	}
}

// TestIntegrationDependencyLeaseExclusiveContended proves the cross-process
// gate: while a shared use lease is held on one handle, another handle's
// TryAcquireGC reports acquired=false with no error and must NOT leak the
// exclusive lease; after the shared holder releases, the SAME handle acquires.
func TestIntegrationDependencyLeaseExclusiveContended(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("(contended)")

	// Hold a shared use lease through handle A.
	l, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}

	// Handle B's exclusive attempt is refused while A holds the shared lease.
	gc, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil {
		t.Fatalf("TryAcquireDependencyImageGC while use lease held: %v", err)
	}
	if ok {
		t.Fatal("TryAcquireDependencyImageGC must report acquired=false while a shared use lease is held")
	}
	if gc != nil {
		t.Fatal("refused GC attempt must not hand back a lease token")
	}
	if err := l.Verify(ctx); err != nil {
		t.Fatalf("shared use lease must still verify while contention refused the exclusive attempt: %v", err)
	}

	// After A releases, the same exclusive attempt SUCCEEDS.
	if err := l.Release(); err != nil {
		t.Fatalf("Release shared use lease: %v", err)
	}
	gc, ok, err = b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil {
		t.Fatalf("TryAcquireDependencyImageGC after release: %v", err)
	}
	if !ok {
		t.Fatal("TryAcquireDependencyImageGC must succeed once no shared use lease is held")
	}
	if gc == nil {
		t.Fatal("acquired GC attempt must hand back a non-nil lease")
	}
	if err := gc.Verify(ctx); err != nil {
		t.Fatalf("acquired GC lease Verify: %v", err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
}

// TestIntegrationDependencyLeaseExclusiveBlocksShared proves the reverse
// direction: an exclusive GC lease on one handle REFUSES a shared use
// acquisition from the other; both succeed once the exclusive lease releases.
func TestIntegrationDependencyLeaseExclusiveBlocksShared(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("reverse")

	gc, ok, err := a.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("first TryAcquireDependencyImageGC = (%v, %v), want acquired", ok, err)
	}

	if _, err := b.AcquireDependencyImageUse(ctx, ref); err == nil {
		t.Fatal("AcquireDependencyImageUse must fail while an exclusive GC lease is held")
	}

	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
	use, err := b.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse after GC release: %v", err)
	}
	if err := use.Release(); err != nil {
		t.Fatalf("Release use lease: %v", err)
	}
}

// TestIntegrationDependencyLeaseBusyRetryable pins the retryable-busy path:
// while an exclusive GC hold is active on another handle, a shared acquisition
// that cannot wait long enough fails with ErrDependencyImageBusy (retryable),
// never with something a caller would treat as a lost layer.
func TestIntegrationDependencyLeaseBusyRetryable(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ref := leaseRefFor("busy")

	gc, ok, err := a.TryAcquireDependencyImageGC(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("first TryAcquireDependencyImageGC = (%v, %v), want acquired", ok, err)
	}

	// The contention budget (DependencyImageUseContentionTimeout) expires
	// inside a longer caller context: the refusal is the retryable busy error,
	// not a context deadline. A full-context retry below then succeeds.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = b.AcquireDependencyImageUse(ctx, ref)
	if !errors.Is(err, state.ErrDependencyImageBusy) {
		t.Fatalf("AcquireDependencyImageUse under an exclusive hold = %v, want ErrDependencyImageBusy", err)
	}

	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
	// Full-context retry now succeeds (the busy error was nothing terminal).
	use, err := b.AcquireDependencyImageUse(context.Background(), ref)
	if err != nil {
		t.Fatalf("retry after GC release: %v", err)
	}
	if err := use.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestIntegrationDependencyLeaseBusyWaitThenGrant pins the bounded-wait path:
// a shared acquisition overlapping a soon-released exclusive hold waits out
// the contention and is granted (no timing sleep in the TEST: the waiting side
// runs on a goroutine and the release happens deterministically before the
// acquisition's contention budget expires).
func TestIntegrationDependencyLeaseBusyWaitThenGrant(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ref := leaseRefFor("wait-grant")

	gc, ok, err := a.TryAcquireDependencyImageGC(context.Background(), ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC = (%v, %v), want acquired", ok, err)
	}

	granted := make(chan error, 1)
	go func() {
		l, err := b.AcquireDependencyImageUse(context.Background(), ref)
		if err == nil {
			_ = l.Release()
		}
		granted <- err
	}()

	// Release while (or before) the acquisition is retrying: every attempt
	// after this finds the gate open, so the acquire must return nil.
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
	select {
	case err := <-granted:
		if err != nil {
			t.Fatalf("bounded-wait acquisition under soon-released contention = %v, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the bounded-wait acquisition never returned")
	}
}

// TestIntegrationDependencyLeaseClosedHandleFailsClosed pins the closed-handle
// semantics: once a handle closed, coordination is unavailable, so acquisitions
// fail closed with ErrDependencyImageCoordinationUnavailable (a build retries,
// a GC refuses to remove). This is the state half of the supported lifecycle
// order: State.Close is a LATE lifecycle step — a Manager using its coordinator
// must close and join BEFORE the State handle closes, never the reverse, and a
// closed handle's locks are the process-cleanup release.
func TestIntegrationDependencyLeaseClosedHandleFailsClosed(t *testing.T) {
	testutil.RequireStateDatabase(t)
	st, _ := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("closed-handle")

	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := st.AcquireDependencyImageUse(ctx, ref); !errors.Is(err, state.ErrDependencyImageCoordinationUnavailable) {
		t.Fatalf("acquisition on a closed handle = %v, want ErrDependencyImageCoordinationUnavailable", err)
	}
	if _, ok, err := st.TryAcquireDependencyImageGC(ctx, ref); !errors.Is(err, state.ErrDependencyImageCoordinationUnavailable) {
		t.Fatalf("GC acquisition on a closed handle = (%v, %v), want ErrDependencyImageCoordinationUnavailable", ok, err)
	}
}

// newOwnerfulStatePair opens two independent OWNERFUL State handles on ONE
// fresh schema, each with a unique run-unique worker identity — the exact
// production connection branch (dependency leases run on each handle's pinned
// owner-lock connection) and the proof that different WORKER_IDs coexist.
func newOwnerfulStatePair(t *testing.T) (*state.State, *state.State) {
	t.Helper()
	dsn := testutil.NewStateDSN(t)
	a := testutil.OpenState(t, dsn, state.Options{
		Owner:    "ir004-own-a-" + ir004RunToken,
		Instance: "inst-a-" + ir004RunToken,
	})
	b := testutil.OpenState(t, dsn, state.Options{
		Owner:    "ir004-own-b-" + ir004RunToken,
		Instance: "inst-b-" + ir004RunToken,
	})
	return a, b
}

// advisoryKeyFor replicates the production dependency-image advisory-key
// mapping (FNV-1a 32 over the reference, namespace 2, objsubid=2 two-int form)
// so the test can address the exact pg_locks row / target backend from an
// ADMIN connection. The low-order half must match
// state.dependencyImageLockKey byte-for-byte or the test would be probing a
// non-existent lock — the initial pg_locks assertion below pins that.
func advisoryKeyFor(depRef string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(depRef))
	return int64(uint32(h.Sum32()))
}

// TestIntegrationDependencyLeaseBackendLossDetected proves PostgreSQL session
// loss closes the lease: while an ownerful shared use lease is held, an ADMIN
// connection terminates the exact owning backend (pg_terminate_backend on the
// pg_locks row for this lease's advisory key); the lease Verify must then fail
// closed (lost, never "claimed safe"), and a second handle on the same database
// can only then acquire the exclusive GC view — after the backend is gone.
// No Docker work is involved anywhere.
func TestIntegrationDependencyLeaseBackendLossDetected(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newOwnerfulStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref := leaseRefFor("backend-loss")

	l, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}
	if err := l.Verify(ctx); err != nil {
		t.Fatalf("lease Verify while healthy: %v", err)
	}

	// ADMIN connection (no leases of its own): address the exact backend that
	// holds this lease's advisory ShareLock. The row MUST exist — the key
	// mapping mirrors the production hash, and its absence is an immediate
	// fixture bug, not a wait.
	db, err := sql.Open("pgx", testutil.TestDatabaseURL())
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	defer func() { _ = db.Close() }()
	adminCtx, adminCancel := context.WithTimeout(ctx, 10*time.Second)
	defer adminCancel()

	var pid int
	err = db.QueryRowContext(adminCtx, `
		SELECT pid FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND granted
		   AND objsubid = 2
		   AND classid::bigint = $1
		   AND objid::bigint  = $2
		   AND mode = 'ShareLock'`,
		int64(uint32(2)), advisoryKeyFor(ref)).Scan(&pid)
	if err != nil {
		t.Fatalf("the use lease's advisory lock row must be visible (key map mismatch?): %v", err)
	}

	terminated, err := db.ExecContext(adminCtx, `SELECT pg_terminate_backend($1)`, pid)
	if err != nil {
		t.Fatalf("terminate backend %d: %v", pid, err)
	}
	if n, _ := terminated.RowsAffected(); n != 1 {
		t.Fatalf("pg_terminate_backend(%d) returned %d rows, want 1", pid, n)
	}

	// The lease no longer protects anything: Verify must FAIL CLOSED.
	if err := l.Verify(ctx); err == nil {
		t.Fatal("Verify must fail after the coordinating backend was terminated")
	} else if !errors.Is(err, state.ErrDependencyImageLeaseLost) {
		t.Fatalf("Verify error = %v, want ErrDependencyImageLeaseLost", err)
	}

	// The released session's lock is gone from the database: handle B — a
	// different, still-healthy worker identity — acquires the exclusive view.
	if _, ok, err := b.TryAcquireDependencyImageGC(ctx, ref); err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after the holding backend was terminated = (%v, %v), want acquired", ok, err)
	}
}

// TestIntegrationDependencyLeaseExclusiveExclusive proves the exclusive view is
// EXCLUSIVE per reference: two independent ownerful handles on the same
// database cannot both hold the GC lease for one dep — the second try is
// refused (acquired=false, nil error = contended, skip candidate) while the
// first is held, and the SAME try succeeds once the first releases. Fully
// deterministic single calls, run-unique refs, no sleeps, no Docker.
func TestIntegrationDependencyLeaseExclusiveExclusive(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newOwnerfulStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref := leaseRefFor("gc-vs-gc")

	first, ok, err := a.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("first TryAcquireDependencyImageGC = (acquired=%v, err=%v), want acquired", ok, err)
	}

	// While the first is held, the second handle must be refused the same
	// reference — and must NOT leak a lease token of its own.
	second, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil {
		t.Fatalf("second TryAcquireDependencyImageGC error: %v", err)
	}
	if ok {
		t.Fatal("two handles on the same database must not both hold an exclusive GC lease for one dep")
	}
	if second != nil {
		t.Fatal("a refused GC acquisition must not hand back a lease token")
	}
	// The sterile refusal also stays sterile under repetition.
	if _, ok, _ := b.TryAcquireDependencyImageGC(ctx, ref); ok {
		t.Fatal("the refusal must hold across repeated tries while the first lease is held")
	}

	if err := first.Release(); err != nil {
		t.Fatalf("Release first GC lease: %v", err)
	}
	third, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after release = (acquired=%v, err=%v), want acquired", ok, err)
	}
	if err := third.Release(); err != nil {
		t.Fatalf("Release third GC lease: %v", err)
	}
}

// TestIntegrationDependencyLeasePerReferenceIsolation proves coordination is
// PER dependency reference, not global: a shared use lock on dep A held on one
// handle must not prevent an exclusive GC of dep B on the other handle. This
// is what keeps one contended build from blocking unrelated GC candidates.
func TestIntegrationDependencyLeasePerReferenceIsolation(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	refA := leaseRefFor("isolate-a")
	refB := leaseRefFor("isolate-b")

	// Hold A's shared use lease through handle a.
	l, err := a.AcquireDependencyImageUse(ctx, refA)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse (dep A): %v", err)
	}

	// An exclusive GC for the DIFFERENT dependency B is granted freely.
	gc, ok, err := b.TryAcquireDependencyImageGC(ctx, refB)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC (dep B) while dep A is use-locked = (%v, %v), want acquired (per-reference coordination)", ok, err)
	}

	// A stays protected: B's GC lease must not have disturbed it.
	if err := l.Verify(ctx); err != nil {
		t.Fatalf("dep A's use lease must still verify while dep B is GC-held: %v", err)
	}

	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease (dep B): %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release use lease (dep A): %v", err)
	}
	// And the cross-dep gate works the other way: GC of A is now grantable.
	gc, ok, err = b.TryAcquireDependencyImageGC(ctx, refA)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC (dep A) after release = (%v, %v), want acquired", ok, err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease (dep A): %v", err)
	}
}

// TestIntegrationDependencyLeaseReleaseUnblocks proves Release on the last
// holder unblocks the cross-handle exclusive gate deterministically
// (release → try succeeds without any timing assistance).
func TestIntegrationDependencyLeaseReleaseUnblocks(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("release")

	// Two shared holders from the SAME handle (nested consumption) plus one
	// from the other handle: the gate must open only after ALL references
	// release.
	h1, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("first use lease: %v", err)
	}
	h2, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("nested use lease: %v", err)
	}
	h3, err := b.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("other-handle use lease: %v", err)
	}

	mustRefuse := func(when string) {
		t.Helper()
		if _, ok, err := b.TryAcquireDependencyImageGC(ctx, ref); err != nil || ok {
			t.Fatalf("TryAcquireDependencyImageGC %s = (%v, %v), want refusal", when, ok, err)
		}
	}
	mustRefuse("with three held references")

	if err := h2.Release(); err != nil {
		t.Fatalf("nested Release: %v", err)
	}
	mustRefuse("with two held references")
	if err := h1.Release(); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	mustRefuse("with the other handle's reference still held")
	if err := h3.Release(); err != nil {
		t.Fatalf("other-handle Release: %v", err)
	}

	gc, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after every release = (%v, %v), want acquired", ok, err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
}

// TestIntegrationDependencyLeaseCanceledAcquisition proves a canceled
// acquisition grants nothing and leaves no residue: the immediately
// following full-context acquisition succeeds as if nothing happened.
func TestIntegrationDependencyLeaseCanceledAcquisition(t *testing.T) {
	testutil.RequireStateDatabase(t)
	st, _ := newSharedStatePair(t)
	ref := leaseRefFor("canceled")

	// An ALREADY-cancelled context: deterministic, no timing element at all.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.AcquireDependencyImageUse(canceled, ref); err == nil {
		t.Fatal("canceled AcquireDependencyImageUse must fail")
	}

	ctx, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	// Also ensure the REFUSED path leaves nothing: exclusive try after the
	// canceled attempt must succeed.
	gc, ok, err := st.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after canceled acquisition = (%v, %v), want acquired", ok, err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// TestIntegrationDependencyLeaseReleaseLeavesVerifyDead proves a released
// lease no longer verifies: Verify must report an error, not success, once the
// holder released its reference.
func TestIntegrationDependencyLeaseReleaseLeavesVerifyDead(t *testing.T) {
	testutil.RequireStateDatabase(t)
	a, b := newSharedStatePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("verify-dead")

	l, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}
	if err := l.Verify(ctx); err != nil {
		t.Fatalf("Verify while held: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := l.Verify(ctx); err == nil {
		t.Fatal("Verify must fail after Release")
	}

	// And the gate is genuinely open for the other handle.
	gc, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after release = (%v, %v), want acquired", ok, err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC: %v", err)
	}
}

// TestIntegrationDependencyLeaseCloseLifecycle proves Close is the terminal
// lifecycle: after a handle closes, its leases are gone AND the closed handle
// itself is inert — a fresh lease cannot be acquired and a surviving lease
// token from before the Close no longer verifies, while a sibling handle
// regains the full gate.
func TestIntegrationDependencyLeaseCloseLifecycle(t *testing.T) {
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)
	open := func() *state.State { return testutil.OpenState(t, dsn) }
	a := open()
	b := open()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ref := leaseRefFor("close")

	l, err := a.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse: %v", err)
	}

	// Closing A releases its lease: B's exclusive try now succeeds.
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	gc, ok, err := b.TryAcquireDependencyImageGC(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("TryAcquireDependencyImageGC after A closed = (%v, %v), want acquired", ok, err)
	}
	if err := l.Verify(ctx); err == nil {
		t.Fatal("a lease token from a closed handle must no longer verify")
	}

	// B's held exclusive lease blocks a NEW handle (simulating another
	// process/worker starting on the same DB) while B keeps it.
	fresh := open()
	if _, err := fresh.AcquireDependencyImageUse(ctx, ref); err == nil {
		t.Fatal("new handle must not acquire while B holds the exclusive GC lease")
	}

	// B closes, releasing everything it held; the fresh handle acquires.
	if err := gc.Release(); err != nil {
		t.Fatalf("Release GC lease: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close B: %v", err)
	}
	_, err = fresh.AcquireDependencyImageUse(ctx, ref)
	if err != nil {
		t.Fatalf("AcquireDependencyImageUse after B closed: %v", err)
	}
}

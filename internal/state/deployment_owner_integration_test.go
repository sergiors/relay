//go:build integration

package state

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Ownership contracts for AcquireDeploymentNode against a real PostgreSQL
// server, where the node's exclusive session advisory lock lives on a specific
// backend that can be terminated from elsewhere. The incarnation the
// acquisition allocates is the ROOT of every deployment progress fence, so it
// is handed out only to a handle that proves, on its own pinned backend, that
// it still owns the node identity — while that handle's ordinary pool may keep
// working after the pinned backend is gone.
//
// Node identities here are unique per test (uniqueNodeID): advisory locks are
// cluster-global even though every test's rows live in an isolated schema. The
// interleaving tests synchronize through pg_stat_activity/pg_blocking_pids and
// explicit row locks — never arbitrary sleeps — and every test FAILS rather
// than skips when PostgreSQL is unreachable.

// terminatePinnedBackend kills a handle's pinned ownership backend from a
// SEPARATE connection — the production ownership-loss event (a terminated
// backend or a dropped session) — and confirms the server reported it gone.
func terminatePinnedBackend(t *testing.T, dsn string, owner *State) {
	t.Helper()
	pid := owner.OwnerBackendPID()
	if pid <= 0 {
		t.Fatalf("owner backend PID = %d, want a pinned ownership backend", pid)
	}
	admin, err := sql.Open("pgx", dsn)
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

// firstCheckedContext signals once, with nil error, when its Err method is
// first read. AcquireDeploymentNode reads the caller's context BEFORE touching
// the ownership mutex, so the signal proves that first check has passed; the
// test then cancels strictly after the signal and before the mutex is
// released, ordering the cancellation between the two documented checks.
type firstCheckedContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *firstCheckedContext) Err() error {
	err := c.Context.Err()
	if err == nil {
		c.once.Do(func() { close(c.checked) })
	}
	return err
}

// TestAcquireDeploymentNodeRequiresLiveOwnership is THE stale-pool scenario:
// owner A acquires deployment progress; A's pinned lock backend is terminated
// from elsewhere while A's ORDINARY POOL keeps working; owner B then opens the
// same NODE_ID and acquires newer progress; A's later acquisition is rejected,
// B's incarnation and progress remain untouched by the rejection, and B can
// still write.
func TestAcquireDeploymentNodeRequiresLiveOwnership(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-owner")

	// Given: owner A holds the node identity and acquired generation 1.
	a := openWithIdentity(t, dsn, nodeID, "inst-1")
	first, err := a.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err != nil {
		t.Fatalf("owner A acquire: %v", err)
	}
	if first.Incarnation <= 0 || first.AppliedGeneration != 0 {
		t.Fatalf("first acquire = %+v, want a fresh incarnation, applied 0", first)
	}

	// When: A's pinned lock backend dies while the process keeps running.
	terminatePinnedBackend(t, dsn, a)

	// Then: A's ordinary POOL still works on a fresh backend — so A's
	// rejection below is the lost ownership, not an unreachable database.
	var freshPID int
	if err := a.db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).
		Scan(&freshPID); err != nil {
		t.Fatalf("stale handle pool query after termination: %v", err)
	}
	if freshPID == a.OwnerBackendPID() {
		t.Fatalf("pool reused the terminated backend pid %d", freshPID)
	}

	// When: owner B opens the same NODE_ID (the session lock died with A's
	// backend) and acquires NEWER progress (generation 2).
	b := openWithIdentity(t, dsn, nodeID, "inst-2")
	acquired, err := b.AcquireDeploymentNode(ctx, nodeID, "inst-2", 2,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
		})
	if err != nil {
		t.Fatalf("successor B acquire: %v", err)
	}
	if acquired.Incarnation <= first.Incarnation {
		t.Fatalf("successor incarnation %d, want a fresh one after %d",
			acquired.Incarnation, first.Incarnation)
	}
	if acquired.AppliedGeneration != first.AppliedGeneration {
		t.Fatalf("successor applied generation = %d, want the preserved %d",
			acquired.AppliedGeneration, first.AppliedGeneration)
	}

	// Then: A's LATER acquisition is rejected — its handle can no longer prove
	// ownership — and it must not read as a generation rejection.
	_, err = a.AcquireDeploymentNode(ctx, nodeID, "inst-1", 3,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 3,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitC,
		})
	if err == nil {
		t.Fatal("stale owner's acquisition = nil, want a rejection")
	}
	if !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("stale owner's acquisition error = %v, want ErrOwnerLockLost", err)
	}
	if errors.Is(err, ErrDeploymentNodeSuperseded) {
		t.Fatalf("stale owner's acquisition error = %v, must not claim a "+
			"generation rejection", err)
	}

	// Then: B's incarnation and progress are exactly what B acquired — the
	// rejected attempt changed nothing.
	after, found, err := b.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", after, found, err)
	}
	if after.Generation != 2 || after.Incarnation != acquired.Incarnation ||
		after.Instance != "inst-2" || after.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want B's generation-2 progress with incarnation %d",
			after, acquired.Incarnation)
	}

	// Then: B can still write under its own incarnation.
	landed, err := b.RecordDeploymentNodeProgress(ctx, nodeID, 2,
		acquired.Incarnation, DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			AppliedGeneration: 2, AppliedIncarnation: acquired.Incarnation,
			Phase: DeploymentNodeMaterialized, Commit: deployCommitB,
			AppliedCommit: deployCommitB,
		})
	if err != nil || !landed {
		t.Fatalf("successor node write = (%v, %v), want landed", landed, err)
	}
	landed, err = b.RecordDeploymentAppProgress(ctx, nodeID, "fn", 2,
		acquired.Incarnation, DeploymentAppProgress{
			NodeID: nodeID, App: "fn", Generation: 2, Instance: "inst-2",
			Phase: DeploymentAppPending, Fingerprint: "fp-2",
			TemplateIdentity: "tid-2", Commit: deployCommitB,
		})
	if err != nil || !landed {
		t.Fatalf("successor app write = (%v, %v), want landed", landed, err)
	}
}

// TestAcquireDeploymentNodeFailsClosedWhenOwnershipLockReleased pins the
// in-statement ownership proof with the backend ALIVE: releasing the session
// advisory lock out of band makes the very same handle's acquisition fail with
// the CONFIRMED loss (the proof ran inside the acquisition statement and
// observed the absent lock), no row appears, and re-taking the lock on the
// pinned connection restores acquisition — so the refusal came from the proof,
// not a poisoned handle.
func TestAcquireDeploymentNodeFailsClosedWhenOwnershipLockReleased(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-released")
	progress := DeploymentNodeProgress{
		NodeID: nodeID, Generation: 1,
		Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
	}

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	// Only the holding session can release its own session advisory lock, so
	// the release runs ON the pinned connection: the backend and its pid stay
	// intact while the lock is gone.
	owner.ownerMu.Lock()
	_, unlockErr := owner.ownerConn.ExecContext(ctx,
		`SELECT pg_advisory_unlock($1)`, nodeLockKey(nodeID))
	owner.ownerMu.Unlock()
	if unlockErr != nil {
		t.Fatalf("release node lock: %v", unlockErr)
	}

	_, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1, progress)
	if !errors.Is(err, ErrOwnerLockLost) || !errors.Is(err, ErrOwnerLockNotHeld) {
		t.Fatalf("acquisition after releasing the lock = %v, want "+
			"ErrOwnerLockLost + ErrOwnerLockNotHeld", err)
	}
	if errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("acquisition after releasing the lock = %v, want a confirmed "+
			"loss, not an unverified connection failure", err)
	}

	// The pool is untouched by the rejection: no row was written.
	if _, found, err := owner.ReadDeploymentNode(ctx, nodeID); err != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"rejected acquisition", found, err)
	}

	// Control: re-taking the lock on the same pinned connection makes the very
	// same acquisition succeed, proving the refusal came from the ownership
	// proof rather than a dead handle.
	owner.ownerMu.Lock()
	_, relockErr := owner.ownerConn.ExecContext(ctx,
		`SELECT pg_advisory_lock($1)`, nodeLockKey(nodeID))
	owner.ownerMu.Unlock()
	if relockErr != nil {
		t.Fatalf("re-take node lock: %v", relockErr)
	}
	acquired, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1, progress)
	if err != nil || acquired.Incarnation <= 0 {
		t.Fatalf("acquisition after re-taking the lock = (%+v, %v), want success",
			acquired, err)
	}
}

// TestAcquireDeploymentNodeRejectsUnownedIdentityHandle pins that a State
// handle scoped to a node identity but WITHOUT a pinned ownership backend (it
// never took the node's session lock) can never acquire: identity alone is not
// ownership. Its ordinary pool working proves the refusal is the missing
// backend, and no row appears.
func TestAcquireDeploymentNodeRejectsUnownedIdentityHandle(t *testing.T) {
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-unowned")

	st := openUnlockedState(t, dsn, nodeID, "inst-1")
	_, err := st.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err == nil || !strings.Contains(err.Error(),
		"requires a node-owned state handle") {
		t.Fatalf("acquisition without a lock backend = %v, want the node-owned "+
			"handle rejection", err)
	}
	if _, found, rerr := st.ReadDeploymentNode(ctx, nodeID); rerr != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"rejected acquisition", found, rerr)
	}
	var one int
	if err := st.db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("pool query = (%d, %v), want the ordinary pool operational", one, err)
	}
}

// TestAcquireDeploymentNodeFailsClosedForClosedOwner pins the closed-handle
// boundary: once Close released the pinned lock connection, the acquisition
// fails closed with the ownership-loss sentinels instead of falling back to
// the (still closable) pool, and writes no row.
func TestAcquireDeploymentNodeFailsClosedForClosedOwner(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-closed-owner")

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	if err := owner.Close(); err != nil {
		t.Fatalf("close owner handle: %v", err)
	}
	_, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if !errors.Is(err, ErrOwnerLockLost) || !errors.Is(err, ErrOwnerLockConnection) {
		t.Fatalf("acquisition after Close = %v, want ErrOwnerLockLost + "+
			"ErrOwnerLockConnection", err)
	}
	reader := testOpenOrFail(t, dsn)
	if _, found, rerr := reader.ReadDeploymentNode(ctx, nodeID); rerr != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"closed handle's rejected acquisition", found, rerr)
	}
}

// TestAcquireDeploymentNodePreCanceledContextReturnsBeforeMutex pins the FIRST
// cancellation boundary: a call whose context is already canceled returns
// context.Canceled before ever queueing for the ownership mutex — proven by
// holding that mutex for the whole call, which would deadlock a check that ran
// only after the mutex.
func TestAcquireDeploymentNodePreCanceledContextReturnsBeforeMutex(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-precancel")

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	owner.ownerMu.Lock()
	mutexHeld := true
	release := func() {
		if mutexHeld {
			owner.ownerMu.Unlock()
			mutexHeld = false
		}
	}
	// Cleanup must never leave the mutex held: Close serializes on it.
	defer release()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	results := make(chan error, 1)
	go func() {
		_, err := owner.AcquireDeploymentNode(canceled, nodeID, "inst-1", 1,
			DeploymentNodeProgress{
				NodeID: nodeID, Generation: 1,
				Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
			})
		results <- err
	}()
	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pre-canceled acquisition = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a pre-canceled acquisition blocked on the ownership mutex: " +
			"the context must be checked before the mutex is taken")
	}

	if _, found, err := owner.ReadDeploymentNode(ctx, nodeID); err != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"canceled acquisition", found, err)
	}
}

// TestAcquireDeploymentNodeCancellationWhileQueuedRunsNoStatement pins the
// SECOND cancellation boundary: a call that passes the first context check and
// then QUEUES behind the held ownership mutex (behind a verification or
// another acquisition) must observe the cancellation immediately after
// acquiring that mutex and return without running the acquisition statement —
// no row, no incarnation — while the handle stays healthy for a later
// live-context acquisition. The interleaving is deterministic: the first check
// signals, the test cancels, then the mutex is released; no sleeps are
// involved.
func TestAcquireDeploymentNodeCancellationWhileQueuedRunsNoStatement(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-queued-cancel")
	progress := DeploymentNodeProgress{
		NodeID: nodeID, Generation: 1,
		Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
	}

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	// Given: the ownership mutex is held (an ownership check or another
	// acquisition in flight), so no acquisition statement can start.
	owner.ownerMu.Lock()
	mutexHeld := true
	release := func() {
		if mutexHeld {
			owner.ownerMu.Unlock()
			mutexHeld = false
		}
	}
	defer release()

	// When: the acquisition passes its first context check (the signal below),
	// queues behind the held mutex, and its context is then canceled before
	// the mutex is granted.
	queued, cancelQueued := context.WithCancel(ctx)
	observed := &firstCheckedContext{Context: queued, checked: make(chan struct{})}
	results := make(chan error, 1)
	go func() {
		_, err := owner.AcquireDeploymentNode(observed, nodeID, "inst-1", 1, progress)
		results <- err
	}()
	select {
	case <-observed.checked:
	case <-time.After(5 * time.Second):
		t.Fatal("the acquisition never reached its first context check")
	}
	cancelQueued()
	release()

	// Then: the queued call returns context.Canceled instead of running the
	// acquisition statement on an already-canceled context.
	select {
	case err := <-results:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation while queued = %v, want context.Canceled (the "+
				"context must be re-checked after the mutex is acquired)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the queued acquisition did not return after the mutex was released")
	}

	if _, found, err := owner.ReadDeploymentNode(ctx, nodeID); err != nil || found {
		t.Fatalf("ReadDeploymentNode = (found %v, %v), want no row from the "+
			"canceled acquisition", found, err)
	}
	// Control: the canceled attempts poisoned neither the pinned connection
	// nor the handle — a live-context acquisition still succeeds.
	acquired, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1, progress)
	if err != nil || acquired.Incarnation <= 0 {
		t.Fatalf("control acquisition = (%+v, %v), want the handle usable after "+
			"canceled attempts", acquired, err)
	}
}

// TestAcquireDeploymentNodeIgnoresCancellationWhileStatementInFlight pins
// the THIRD cancellation boundary, distinguished from the two pre-start ones:
// once the acquisition statement has begun on the pinned connection, the
// caller's cancellation no longer aborts it — a racing cancellation would
// cancel the statement's implicit transaction mid-commit, whose outcome the
// client cannot prove and which can make the driver discard ownerConn. The
// in-flight state is proven through pg_stat_activity/pg_blocking_pids against
// an explicit row lock; the acquisition then completes successfully under the
// still-live bound.
func TestAcquireDeploymentNodeIgnoresCancellationWhileStatementInFlight(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-inflight-cancel")

	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	first, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err != nil {
		t.Fatalf("initial acquisition: %v", err)
	}

	// A dedicated pool connection holds the node-row lock so the acquisition
	// below is provably in flight when its context is cancelled.
	holder, err := owner.db.BeginTx(ctx, nil)
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
		nodeID).Scan(&locked); err != nil {
		_ = holder.Rollback()
		t.Fatalf("lock deployment node row: %v", err)
	}

	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	acquires := make(chan appProgressWriteResult, 1)
	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		acquired, aerr := owner.AcquireDeploymentNode(acquireCtx, nodeID, "inst-1", 2,
			DeploymentNodeProgress{
				NodeID: nodeID, Generation: 2,
				Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
			})
		acquires <- appProgressWriteResult{
			landed: aerr == nil && acquired.Incarnation > 0, err: aerr}
	}()
	t.Cleanup(func() {
		cancelAcquire()
		if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("release node lock holder: %v", err)
		}
		select {
		case <-acquireDone:
		case <-time.After(5 * time.Second):
			t.Log("acquisition did not return during cleanup")
		}
	})

	// The acquisition must be observably WAITING on the holder's backend —
	// its statement (implicit transaction) has begun — before the cancellation
	// below. The fast cadence fits inside deploymentNodeLockTimeout's 50ms
	// server-side wait bound with room for CI scheduling jitter.
	early := awaitFenceOrderingFast(t, monitor, holderPID, acquires)
	if early != nil {
		t.Fatalf("acquisition completed (%v, %v) while the node row lock was "+
			"held: want it in flight", early.landed, early.err)
	}

	// When: the caller cancels while the statement is provably in flight.
	cancelAcquire()
	if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("release node lock holder: %v", err)
	}

	// Then: the in-flight statement finishes under its own bound instead of
	// failing with the cancellation.
	got := appProgressWriteResult{}
	select {
	case got = <-acquires:
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not return after the row lock was released")
	}
	if got.err != nil {
		t.Fatalf("in-flight cancellation aborted the acquisition: %v, want the "+
			"bounded statement to complete (cancellation is honored only "+
			"before it starts)", got.err)
	}

	after, found, err := owner.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", after, found, err)
	}
	if after.Generation != 2 || after.Incarnation <= first.Incarnation {
		t.Fatalf("node = %+v, want generation 2 under a fresh incarnation", after)
	}

	// And: the ownership lock survived the raced cancellation together with
	// the pinned connection — VerifyOwnerLock queries ONLY that pinned
	// backend, so a cancellation that disturbed it would fail closed here.
	if err := owner.VerifyOwnerLock(ctx); err != nil {
		t.Fatalf("owner lock after the raced cancellation = %v, want it intact",
			err)
	}

	// And: the pinned connection survived the raced cancellation — a
	// cancellation that reached the in-flight statement would have failed it
	// with an unknown outcome and may have discarded the pinned connection,
	// so the next acquisition would not run clean.
	next, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 2,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
		})
	if err != nil || next.Incarnation <= after.Incarnation {
		t.Fatalf("acquisition after the raced cancellation = (%+v, %v), want a "+
			"healthy pinned connection allocating a fresh incarnation", next, err)
	}
}

// TestStaleOwnerCannotWriteProgressAfterSuccessor pins the stale handle's
// WRITE surface after a successor takes over: with owner A's backend gone and
// owner B holding the identity, A's materialization commit, failure report,
// and app-progress writes are all fenced no-ops — zero physical row writes per
// the schema-local F-01 counters — while B's own materialization and failure
// report land, and B's failure report preserves the applied authorization.
func TestStaleOwnerCannotWriteProgressAfterSuccessor(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-stale-writes")

	// Given: owner A acquired generation 1, then lost its pinned backend.
	a := openWithIdentity(t, dsn, nodeID, "inst-1")
	stale, err := a.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err != nil {
		t.Fatalf("owner A acquire: %v", err)
	}
	terminatePinnedBackend(t, dsn, a)

	// Given: successor B claims the same identity and recovers the SAME
	// generation under a fresh incarnation.
	b := openWithIdentity(t, dsn, nodeID, "inst-2")
	current, err := b.AcquireDeploymentNode(ctx, nodeID, "inst-2", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err != nil {
		t.Fatalf("successor B acquire: %v", err)
	}
	if current.Incarnation <= stale.Incarnation {
		t.Fatalf("successor incarnation %d, want a fresh one after %d",
			current.Incarnation, stale.Incarnation)
	}
	installDeploymentAppWriteCounters(t, b)

	// When: A's stale handle reports the work it believed it still owned.
	landedMaterialization, err := a.CommitDeploymentMaterialization(ctx,
		DeploymentMaterialization{
			NodeID: nodeID, Generation: 1, Instance: "inst-1",
			Incarnation: stale.Incarnation, Commit: deployCommitA,
			Apps: []DeploymentAppAuthorization{{
				App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-stale", TemplateIdentity: "tid-stale",
			}},
		})
	if err != nil || landedMaterialization {
		t.Fatalf("stale materialization commit = (%v, %v), want a fenced no-op",
			landedMaterialization, err)
	}
	landedFailure, err := a.RecordDeploymentNodeFailure(ctx, nodeID, 1,
		stale.Incarnation, deployCommitA, "stale fetch failed")
	if err != nil || landedFailure {
		t.Fatalf("stale failure report = (%v, %v), want a fenced no-op",
			landedFailure, err)
	}
	landedApp, err := a.RecordDeploymentAppProgress(ctx, nodeID, deployProgressApp,
		1, stale.Incarnation, DeploymentAppProgress{
			NodeID: nodeID, App: deployProgressApp, Generation: 1,
			Instance: "inst-1", Phase: DeploymentAppFailed,
			Fingerprint: "fp-stale", TemplateIdentity: "tid-stale",
			Commit: deployCommitA, Error: "stale fetch failed",
		})
	if err != nil || landedApp {
		t.Fatalf("stale app write = (%v, %v), want a fenced no-op", landedApp, err)
	}

	// Then: the database recorded ZERO physical writes and B's row is exactly
	// the progress B acquired.
	requireDeployAppWrites(t, b, 0, 0)
	if rows, err := b.ReadDeploymentAppProgress(ctx, nodeID, 1); err != nil ||
		len(rows) != 0 {
		t.Fatalf("app rows = (%+v, %v), want none from the stale writes", rows, err)
	}
	after, found, err := b.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode = (%+v, %v, %v)", after, found, err)
	}
	if after.Incarnation != current.Incarnation || after.Generation != 1 ||
		after.AppliedGeneration != 0 ||
		after.Phase != DeploymentNodeMaterializing {
		t.Fatalf("node = %+v, want B's untouched generation-1 progress under "+
			"incarnation %d", after, current.Incarnation)
	}

	// Control: B's own materialization and failure report land — the fence
	// refused A specifically, not a dead fence — and B's failure report
	// preserves the applied authorization.
	landed, err := b.CommitDeploymentMaterialization(ctx,
		DeploymentMaterialization{
			NodeID: nodeID, Generation: 1, Instance: "inst-2",
			Incarnation: current.Incarnation, Commit: deployCommitA,
			Apps: []DeploymentAppAuthorization{{
				App: deployProgressApp, Phase: DeploymentAppPending,
				Fingerprint: "fp-b", TemplateIdentity: "tid-b",
			}},
		})
	if err != nil || !landed {
		t.Fatalf("successor materialization commit = (%v, %v), want committed",
			landed, err)
	}
	requireDeployAppWrites(t, b, 1, 0)
	landed, err = b.RecordDeploymentNodeFailure(ctx, nodeID, 1,
		current.Incarnation, deployCommitA, "real fetch failed")
	if err != nil || !landed {
		t.Fatalf("successor failure report = (%v, %v), want landed", landed, err)
	}
	final, found, err := b.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("final ReadDeploymentNode = (%+v, %v, %v)", final, found, err)
	}
	if final.Phase != DeploymentNodeFailed ||
		final.AppliedGeneration != 1 || final.AppliedCommit != deployCommitA {
		t.Fatalf("node = %+v, want the failed phase with applied generation 1 "+
			"(%s) preserved", final, deployCommitA)
	}
}

// TestAcquireDeploymentNodeOrderedAgainstOwnershipLossWhileBlocked pins the
// in-flight acquisition against an ownership change, with PostgreSQL as the
// only clock: an acquisition whose statement is provably waiting on an
// explicit node-row lock cannot be claimed by a successor while the owner's
// acquisition statement is still in flight on that locked backend;
// terminating that backend mid-wait (the ownership change) aborts the
// allocation so nothing is written; and the successor then claims the
// identity, acquires, and writes normally. Waiters are observed through
// pg_stat_activity/pg_blocking_pids — no sleeps.
func TestAcquireDeploymentNodeOrderedAgainstOwnershipLossWhileBlocked(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	ctx := context.Background()
	nodeID := uniqueNodeID(t, "worker-deploy-blocked")

	monitor, err := sql.Open("pgx", testDatabaseURL())
	if err != nil {
		t.Fatalf("open activity monitor: %v", err)
	}
	t.Cleanup(func() { _ = monitor.Close() })

	// Given: owner A's node row exists and a holder transaction locks it.
	owner := openWithIdentity(t, dsn, nodeID, "inst-1")
	first, err := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 1,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 1,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitA,
		})
	if err != nil {
		t.Fatalf("owner A initial acquire: %v", err)
	}
	holder, err := owner.db.BeginTx(ctx, nil)
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
		nodeID).Scan(&locked); err != nil {
		_ = holder.Rollback()
		t.Fatalf("lock deployment node row: %v", err)
	}

	// When: A's next acquisition (generation 2) starts while the row is locked.
	acquires := make(chan appProgressWriteResult, 1)
	acquireDone := make(chan struct{})
	go func() {
		defer close(acquireDone)
		acquired, aerr := owner.AcquireDeploymentNode(ctx, nodeID, "inst-1", 2,
			DeploymentNodeProgress{
				NodeID: nodeID, Generation: 2,
				Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
			})
		acquires <- appProgressWriteResult{
			landed: aerr == nil && acquired.Incarnation > 0, err: aerr}
	}()
	t.Cleanup(func() {
		if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Logf("release node lock holder: %v", err)
		}
		select {
		case <-acquireDone:
		case <-time.After(5 * time.Second):
			t.Log("acquisition did not return during cleanup")
		}
	})

	// Then: the acquisition is observably in flight — waiting on the holder's
	// backend — before anything below changes. The fast cadence fits inside
	// deploymentNodeLockTimeout's 50ms server-side wait bound with room for
	// CI scheduling jitter.
	early := awaitFenceOrderingFast(t, monitor, holderPID, acquires)
	if early != nil {
		t.Fatalf("acquisition completed (%v, %v) while the node row lock was "+
			"held: want it waiting", early.landed, early.err)
	}

	// Then: a successor CANNOT claim the identity while the owner's
	// acquisition statement is still in flight — the session advisory lock
	// lives on that backend throughout the statement's implicit transaction.
	if successor, err := Open(ctx, Options{DSN: dsn, NodeID: nodeID,
		Instance: "inst-2"}); err == nil {
		_ = successor.Close()
		t.Fatal("a successor opened the same NODE_ID while the owner's " +
			"acquisition statement was still in flight")
	}

	// When: ownership changes — A's pinned backend is terminated from a
	// separate connection, which releases the session lock and aborts the
	// in-flight statement on that backend.
	terminatePinnedBackend(t, dsn, owner)
	if err := holder.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("release node lock holder: %v", err)
	}

	// Then: the in-flight acquisition failed and wrote NOTHING.
	got := appProgressWriteResult{}
	select {
	case got = <-acquires:
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight acquisition did not return after the ownership change")
	}
	if got.err == nil {
		t.Fatal("the in-flight acquisition succeeded after its owner backend " +
			"was terminated, want a rejection")
	}
	if errors.Is(got.err, ErrDeploymentNodeSuperseded) {
		t.Fatalf("in-flight acquisition error = %v, want an ownership/connection "+
			"failure, not a generation rejection", got.err)
	}
	// The stale handle's ordinary pool still serves reads, and the row shows
	// exactly A's original progress.
	after, found, err := owner.ReadDeploymentNode(ctx, nodeID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode through the stale pool = (%+v, %v, %v)",
			after, found, err)
	}
	if after.Generation != 1 || after.Incarnation != first.Incarnation ||
		after.AppliedGeneration != 0 {
		t.Fatalf("node = %+v, want generation 1 untouched under incarnation %d "+
			"(the aborted acquisition wrote nothing)", after, first.Incarnation)
	}

	// Control: the successor now claims the identity, acquires generation 2
	// with a fresh incarnation, and writes under it.
	b := openWithIdentity(t, dsn, nodeID, "inst-2")
	acquired, err := b.AcquireDeploymentNode(ctx, nodeID, "inst-2", 2,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
		})
	if err != nil || acquired.Incarnation <= first.Incarnation {
		t.Fatalf("successor acquire = (%+v, %v), want generation 2 under a "+
			"fresh incarnation", acquired, err)
	}
	landed, err := b.RecordDeploymentNodeProgress(ctx, nodeID, 2,
		acquired.Incarnation, DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			AppliedGeneration: 2, AppliedIncarnation: acquired.Incarnation,
			Phase: DeploymentNodeMaterialized, Commit: deployCommitB,
			AppliedCommit: deployCommitB,
		})
	if err != nil || !landed {
		t.Fatalf("successor write = (%v, %v), want landed", landed, err)
	}
}

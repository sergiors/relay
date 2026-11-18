//go:build integration

package state

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// Desired-revision notification contracts, on a real PostgreSQL. Notifications
// are transactional hints: PostgreSQL delivers one only after the publishing
// transaction commits, only when the generation actually advances, and never
// carries state — every listener re-reads the authoritative
// deployment_desired row instead of trusting the hint stream.

// notifyQuietWindow bounds requireNoNotification. Committed notifications are
// delivered immediately to a subscribed session, so any pending hint would
// arrive well within this deadline; anything later is not this contract.
const notifyQuietWindow = 300 * time.Millisecond

func listenDesired(t *testing.T, st *State) *DeploymentDesiredListener {
	t.Helper()
	listener, err := st.ListenDeploymentDesired(context.Background())
	if err != nil {
		t.Fatalf("ListenDeploymentDesired: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

// newNotificationTestDSN creates a fresh, per-test DATABASE and returns a DSN
// for it, dropping the database on cleanup. Notification tests need a whole
// database rather than a schema because PostgreSQL NOTIFY channels are
// database-wide: every package's parallel integration suite publishes to the
// same channel name in the shared test database, and no payload or schema
// boundary can separate those hints from the ones under test. Owning the
// database makes "no hint arrived" and exact delivery counts deterministic.
// It fails (never skips) when the server cannot create a database, matching
// the suite's dependency convention; the DSN must be a postgres:// URL so its
// database path can be rewritten.
func newNotificationTestDSN(t *testing.T) string {
	t.Helper()
	base := testDatabaseURL()
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatalf("notification integration tests need a postgres:// URL in " +
			"TEST_DATABASE_URL to isolate NOTIFY channels; got a non-URL DSN")
	}
	database := "relayn_" + randHex(t)
	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		_ = admin.Close()
		t.Fatalf("notification integration test needs permission to CREATE DATABASE "+
			"on the server at %s (from TEST_DATABASE_URL): %v", testDatabaseEndpoint(), err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		if _, err := admin.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+database+" WITH (FORCE)"); err != nil {
			t.Logf("drop notification test database %s: %v", database, err)
		}
		_ = admin.Close()
	})
	u.Path = "/" + database
	query := u.Query()
	query.Del("search_path") // a fresh database uses the default search path
	u.RawQuery = query.Encode()
	return u.String()
}

// openNotificationTestState opens a State handle on a fresh notification-
// isolated database (see newNotificationTestDSN) with a debug logger
// installed, mirroring openTestState.
func openNotificationTestState(t *testing.T) *State {
	t.Helper()
	st := testOpenOrFail(t, newNotificationTestDSN(t))
	st.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return st
}

// requireNotification waits for one delivered hint.
func requireNotification(t *testing.T, listener *DeploymentDesiredListener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	notified, err := listener.Wait(ctx)
	if err != nil || !notified {
		t.Fatalf("Wait = (%v, %v), want a delivered notification", notified, err)
	}
}

// requireNoNotification pins the ABSENCE of a hint: the wait ends only at its
// own short deadline, and no notification is reported.
func requireNoNotification(t *testing.T, listener *DeploymentDesiredListener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), notifyQuietWindow)
	defer cancel()
	notified, err := listener.Wait(ctx)
	if notified || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = (%v, %v), want no notification before the deadline", notified, err)
	}
}

// listeningOnChannel reports whether conn's session currently LISTENs on the
// desired channel, straight from the server's view of session state.
// listenerBackendPID reads the backend PID of a pinned listener session,
// reusing the package's existing backendPID seam.
func listenerBackendPID(t *testing.T, conn *sql.Conn) int {
	t.Helper()
	pid, err := backendPID(context.Background(), conn)
	if err != nil {
		t.Fatalf("read listener backend pid: %v", err)
	}
	return pid
}

func listeningOnChannel(t *testing.T, conn *sql.Conn) bool {
	t.Helper()
	var listening bool
	if err := conn.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_listening_channels() AS listening(channel)
		 WHERE channel = $1)`,
		deploymentNotifyChannel).Scan(&listening); err != nil {
		t.Fatalf("pg_listening_channels: %v", err)
	}
	return listening
}

// TestPublishDeploymentDesiredNotifiesOnlyOnGenerationAdvance pins the
// transactional hint contract: a committed generation advance delivers exactly
// one notification, while a repeated revision, a rejected automatic push, and
// a compare-and-swap conflict — writes that do NOT advance the generation —
// enqueue nothing. A rollback PUBLICATION (an older commit as a new higher
// generation) does notify; it is unrelated to a transaction rollback, which
// never delivers (covered by the direct-helper test below).
func TestPublishDeploymentDesiredNotifiesOnlyOnGenerationAdvance(t *testing.T) {
	st := openNotificationTestState(t)
	listener := listenDesired(t, st)
	ctx := context.Background()

	first := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "first", false))
	if !first.Published || first.Desired.Generation != 1 {
		t.Fatalf("first publish = %+v, want published generation 1", first)
	}
	requireNotification(t, listener)

	// Unchanged revision: no generation advance, no hint.
	repeat := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "again", false))
	if repeat.Published || repeat.Reason != DeploymentPublishReasonUnchanged {
		t.Fatalf("repeat publish = %+v, want an unchanged no-op", repeat)
	}
	requireNoNotification(t, listener)

	// Rejected automatic push (the before SHA does not chain from the
	// persisted branch-tip anchor): presentation-only rejection, no hint.
	rejected := publishOrFatal(t, st, autoDeployRequest(
		deployCommitZ, deployCommitB, sourceNew, testRepository, testRef, ""))
	if rejected.Published {
		t.Fatalf("stale publish = %+v, want rejected", rejected)
	}
	requireNoNotification(t, listener)

	// A compare-and-swap conflict errors before any write or hint.
	_, err := st.PublishDeploymentDesired(ctx, DeploymentPublishRequest{
		Desired: DeploymentDesired{
			Commit:     deployCommitB,
			Repository: testRepository,
			Ref:        testRef,
		},
		ExpectedGeneration: 7,
		AllowPinned:        true,
	})
	if !errors.Is(err, ErrDeploymentConflict) {
		t.Fatalf("stale-CAS publish err = %v, want ErrDeploymentConflict", err)
	}
	requireNoNotification(t, listener)

	// A genuinely new generation notifies again.
	second := publishOrFatal(t, st, manualDeployRequest(deployCommitB, "second", false))
	if !second.Published || second.Desired.Generation != 2 {
		t.Fatalf("second publish = %+v, want published generation 2", second)
	}
	requireNotification(t, listener)

	// A rollback publication is a NEW generation carrying the older commit.
	rollback := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "rollback", false))
	if !rollback.Published || rollback.Desired.Generation != 3 {
		t.Fatalf("rollback publish = %+v, want published generation 3", rollback)
	}
	requireNotification(t, listener)
}

// TestPinPolicyChangesNeverNotify pins that pin/unpin is policy-only: it never
// advances the generation and never enqueues a hint, and while pinned even a
// perfectly chained automatic push is refused silently. Unpinning resumes the
// branch chain, and only the ACCEPTED publication notifies.
func TestPinPolicyChangesNeverNotify(t *testing.T) {
	st := openNotificationTestState(t)
	listener := listenDesired(t, st)
	ctx := context.Background()

	publishOrFatal(t, st, manualDeployRequest(deployCommitA, "base", false))
	requireNotification(t, listener)

	setPin := func(pinned bool) {
		t.Helper()
		if _, found, err := st.SetDeploymentPinned(ctx, pinned); err != nil || !found {
			t.Fatalf("SetDeploymentPinned(%v) = (_, %v, %v), want found", pinned, found, err)
		}
	}

	// Pin and unpin change the automatic-publication policy only.
	setPin(true)
	requireNoNotification(t, listener)
	setPin(false)
	requireNoNotification(t, listener)

	// While pinned, a chained automatic push (before == anchor) is refused
	// without a hint.
	setPin(true)
	pinned := publishOrFatal(t, st, autoDeployRequest(
		deployCommitA, deployCommitB, sourceNew, testRepository, testRef, ""))
	if pinned.Published || pinned.Reason != DeploymentPublishReasonPinned {
		t.Fatalf("pinned push = %+v, want a pinned rejection", pinned)
	}
	requireNoNotification(t, listener)

	// Unpinning resumes the branch chain; only the accepted push notifies.
	setPin(false)
	accepted := publishOrFatal(t, st, autoDeployRequest(
		deployCommitA, deployCommitB, sourceNew, testRepository, testRef, ""))
	if !accepted.Published || accepted.Desired.Generation != 2 {
		t.Fatalf("chained push = %+v, want published generation 2", accepted)
	}
	requireNotification(t, listener)
}

// TestRolledBackPublicationTransactionDoesNotNotify pins that the hint is
// transactional: pg_notify enqueued inside the publication transaction is
// delivered only when that transaction commits. A transaction that writes the
// generation, enqueues the hint, and then aborts leaves no row and no
// notification behind.
func TestRolledBackPublicationTransactionDoesNotNotify(t *testing.T) {
	st := openNotificationTestState(t)
	listener := listenDesired(t, st)
	ctx := context.Background()
	forced := errors.New("forced rollback after the hint was enqueued")

	err := st.deploymentTx(ctx, func(tx *sql.Tx) error {
		desired := DeploymentDesired{
			Generation: 1,
			Commit:     deployCommitA,
			Repository: testRepository,
			Ref:        testRef,
		}
		if err := writeDeploymentDesiredTx(ctx, tx, desired, st.nowTime().UTC(), false); err != nil {
			return err
		}
		if err := notifyDeploymentDesiredTx(ctx, tx); err != nil {
			return err
		}
		return forced
	})
	if !errors.Is(err, forced) {
		t.Fatalf("deploymentTx err = %v, want the forced rollback to propagate", err)
	}

	if _, found, rerr := st.ReadDeploymentDesired(ctx); rerr != nil || found {
		t.Fatalf("ReadDeploymentDesired = (_, %v, %v), want no row after rollback", found, rerr)
	}
	requireNoNotification(t, listener)
}

// TestListenerCloseUnlistensBeforeReturningSessionToPool is the regression for
// a LISTEN subscription leaking through the State pool: Close must UNLISTEN on
// the pinned backend before releasing it, so unrelated State work that later
// acquires the SAME physical connection is not silently subscribed. The pool
// is constrained to one connection, which makes the post-Close acquire
// provably the session the listener just released: same backend PID, no
// channel. Close is also exercised twice — the second call must be an
// idempotent no-op.
func TestListenerCloseUnlistensBeforeReturningSessionToPool(t *testing.T) {
	st, err := Open(context.Background(), Options{
		DSN:          newNotificationTestDSN(t),
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	listener, err := st.ListenDeploymentDesired(context.Background())
	if err != nil {
		t.Fatalf("ListenDeploymentDesired: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	// Precondition: the pinned session is subscribed.
	if !listeningOnChannel(t, listener.conn) {
		t.Fatal("session does not LISTEN after a successful subscribe")
	}
	pidBefore := listenerBackendPID(t, listener.conn)

	// Close issues the UNLISTEN, releases the connection, and is idempotent.
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second Close = %v, want an idempotent nil", err)
	}

	// With MaxOpenConns=1 the pool owns exactly one physical connection, so
	// this acquire MUST hand back the listener's released session.
	conn, err := st.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("reacquire from pool: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if pidAfter := listenerBackendPID(t, conn); pidAfter != pidBefore {
		t.Fatalf("pool returned backend pid %d, want the released listener session %d "+
			"(with a single connection the session must be reused)", pidAfter, pidBefore)
	}
	if listeningOnChannel(t, conn) {
		t.Fatal("released session still LISTENs on the desired channel: " +
			"Close must UNLISTEN before returning the connection to the pool")
	}
}

// TestListenerCloseCancelsOutstandingWait pins the shutdown contract: Close
// interrupts a Wait that is already blocking (reported as
// ErrDeploymentListenerClosed, never as a delivered hint), is idempotent, and
// makes every later Wait fail fast instead of starting a new blocking read on
// the released session.
func TestListenerCloseCancelsOutstandingWait(t *testing.T) {
	st := openNotificationTestState(t)
	listener := listenDesired(t, st)

	entered := make(chan struct{})
	type waitResult struct {
		notified bool
		err      error
	}
	result := make(chan waitResult, 1)
	go func() {
		close(entered)
		notified, err := listener.Wait(context.Background())
		result <- waitResult{notified, err}
	}()
	<-entered
	// No notification exists, so an outstanding Wait must still be blocking.
	select {
	case got := <-result:
		t.Fatalf("Wait returned before a notification or Close: %+v", got)
	case <-time.After(300 * time.Millisecond):
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case got := <-result:
		if got.notified || !errors.Is(got.err, ErrDeploymentListenerClosed) {
			t.Fatalf("interrupted Wait = (%v, %v), want (false, ErrDeploymentListenerClosed)",
				got.notified, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the outstanding Wait")
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("second Close = %v, want an idempotent nil", err)
	}
	if _, err := listener.Wait(context.Background()); !errors.Is(err, ErrDeploymentListenerClosed) {
		t.Fatalf("Wait after Close = %v, want ErrDeploymentListenerClosed", err)
	}
}

// TestListenerCloseNeverReleasesADeadSubscribedSession covers the cleanup
// failure path: when the pinned backend dies before Close can UNLISTEN it,
// Close must not hand the session back as healthy, still-subscribed pool
// capacity — the pool's next connection must be a different backend that does
// not listen on the channel.
func TestListenerCloseNeverReleasesADeadSubscribedSession(t *testing.T) {
	st := openNotificationTestState(t)
	listener := listenDesired(t, st)
	if !listeningOnChannel(t, listener.conn) {
		t.Fatal("precondition: the session must subscribe before the failure")
	}
	pidBefore := listenerBackendPID(t, listener.conn)

	// Kill the pinned backend out from under the listener, then wait until the
	// session is provably unusable before closing.
	killCtx, killCancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, _ = listener.conn.ExecContext(killCtx,
		"SELECT pg_terminate_backend(pg_backend_pid())")
	killCancel()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := listener.conn.ExecContext(pingCtx, "SELECT 1")
		cancel()
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener backend survived pg_terminate_backend")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Close reports the failed UNLISTEN (a session database/sql already
	// invalidated itself is the tolerated ErrConnDone outcome and may report
	// nil — the pool assertions below are the invariant either way).
	if err := listener.Close(); err != nil &&
		!strings.Contains(err.Error(), "unlisten deployment desired") {
		t.Fatalf("Close = %v, want an unlisten failure report", err)
	}

	conn, err := st.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("reacquire from pool: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if listeningOnChannel(t, conn) {
		t.Fatal("pool yielded a session still subscribed to the desired channel")
	}
	if pid := listenerBackendPID(t, conn); pid == pidBefore {
		t.Fatalf("pool reacquired the dead listener backend pid %d; it must be discarded", pid)
	}
}

// TestMissedNotificationsNeverReplayAndRowWins pins the hint semantics: a
// publication that commits while no session exists is never replayed to a
// later listener, a commit between an initial observation and the LISTEN
// statement is visible only through a fresh read, and after any pattern of
// delivered hints the authoritative row — not the hint stream — is what a
// re-read yields.
func TestMissedNotificationsNeverReplayAndRowWins(t *testing.T) {
	st := openNotificationTestState(t)
	ctx := context.Background()

	// Given an initial observation of an empty deployment...
	if _, found, err := st.ReadDeploymentDesired(ctx); err != nil || found {
		t.Fatalf("ReadDeploymentDesired = (_, %v, %v), want no row", found, err)
	}
	// ...a generation commits between that observation and the first LISTEN...
	publishOrFatal(t, st, manualDeployRequest(deployCommitA, "offline-one", false))

	listener := listenDesired(t, st)
	// ...so the hint sent while offline is never replayed...
	requireNoNotification(t, listener)
	// ...and only a fresh read observes the committed generation.
	desired, found, err := st.ReadDeploymentDesired(ctx)
	if err != nil || !found || desired.Generation != 1 {
		t.Fatalf("fresh read = (%+v, %v, %v), want generation 1", desired, found, err)
	}

	// A longer offline window: two more generations commit with no session up.
	if err := listener.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	publishOrFatal(t, st, manualDeployRequest(deployCommitB, "offline-two", false))
	publishOrFatal(t, st, manualDeployRequest(deployCommitC, "offline-three", false))

	listener = listenDesired(t, st)
	requireNoNotification(t, listener)

	// Multiple rapid publications while subscribed: whatever subset of hints
	// arrives, the row read afterwards is the current truth.
	publishOrFatal(t, st, manualDeployRequest(deployCommitD, "four", false))
	publishOrFatal(t, st, manualDeployRequest(deployCommitE, "five", false))
	requireNotification(t, listener)
	final, found, err := st.ReadDeploymentDesired(ctx)
	if err != nil || !found || final.Generation != 5 ||
		!strings.EqualFold(final.Commit, deployCommitE) {
		t.Fatalf("row after hints = (%+v, %v, %v), want generation 5 commit %s",
			final, found, err, deployCommitE)
	}
}

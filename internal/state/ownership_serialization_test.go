package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// These deterministic unit tests pin the serialization the pinned lock
// connection (ownerConn) requires. They need no PostgreSQL: a minimal
// database/sql driver hands out one connection whose every query BLOCKS until
// the test releases it, so a test can hold the State's connection guard and
// prove that VerifyOwnerLock and Close wait for it instead of touching the
// connection underneath it. (database/sql serializes individual driver calls,
// so only the cross-call interleaving matters — pgx fails such an interleaving
// with "conn busy" — which the postgres integration test proves; here we pin the
// State-level mutual exclusion deterministically.)

// fakeLockDriver is the driver behind fakeLockConnector. sql.OpenDB takes the
// connector directly, so Open is never reached and only satisfies the
// driver.Driver contract.
type fakeLockDriver struct{}

func (fakeLockDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fakeLockDriver: open via the connector")
}

// fakeLockConnector is one test's fake database. It meters query concurrency and
// controls when blocked queries proceed.
type fakeLockConnector struct {
	mu          sync.Mutex
	inFlight    int
	maxInFlight int

	entered chan struct{} // one token buffered per query entry
	release chan struct{} // closed by the test to let blocked queries proceed

	pid  int
	held bool
}

func newFakeLockConnector() *fakeLockConnector {
	return &fakeLockConnector{
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
		pid:     1,
		held:    true,
	}
}

func (c *fakeLockConnector) Driver() driver.Driver { return fakeLockDriver{} }

func (c *fakeLockConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeLockConn{c: c}, nil
}

// enter records a query starting; leave records it finishing. inFlight is how
// many queries are inside the driver at once; maxInFlight is its high-water
// mark.
func (c *fakeLockConnector) enter() {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	c.mu.Unlock()
	select {
	case c.entered <- struct{}{}:
	default:
	}
}

func (c *fakeLockConnector) leave() {
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
}

func (c *fakeLockConnector) max() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxInFlight
}

// fakeLockConn is the single driver connection the connector hands out. Its
// QueryContext honors the caller's context, so a check queued until its bounded
// context expires fails closed instead of hanging.
type fakeLockConn struct{ c *fakeLockConnector }

func (c *fakeLockConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakeLockConn: prepare unsupported")
}
func (c *fakeLockConn) Close() error { return nil }
func (c *fakeLockConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fakeLockConn: begin unsupported")
}

func (c *fakeLockConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.c.enter()
	defer c.c.leave()
	select {
	case <-c.c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &fakeLockRows{c: c.c}, nil
}

var _ driver.QueryerContext = (*fakeLockConn)(nil)

// fakeLockRows returns the single (pid, held) row VerifyOwnerLock scans.
type fakeLockRows struct {
	c    *fakeLockConnector
	done bool
}

func (r *fakeLockRows) Columns() []string { return []string{"pid", "held"} }
func (r *fakeLockRows) Close() error      { return nil }
func (r *fakeLockRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(r.c.pid)
	dest[1] = r.c.held
	return nil
}

// fakeOwnerState builds a State whose pinned lock connection is the fake
// driver's connection. The connector must be released (or the caller's context
// must expire) before the handle closes.
func fakeOwnerState(t *testing.T, c *fakeLockConnector) *State {
	t.Helper()
	db := sql.OpenDB(c)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("fake lock conn: %v", err)
	}
	st := &State{db: db, log: fallbackLogger, owner: "worker-fake", instance: "inst-fake", ownerPID: c.pid, ownerConn: conn}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestVerifyOwnerLockWaitsForConnectionGuard proves the check actually
// serializes on the State's connection guard: while the guard is held, a
// verification must NOT reach the pinned connection, and it queries (and
// succeeds) only once the guard is released. Without the guard it would query
// the connection immediately.
func TestVerifyOwnerLockWaitsForConnectionGuard(t *testing.T) {
	c := newFakeLockConnector()
	st := fakeOwnerState(t, c)

	st.ownerMu.Lock()
	errCh := make(chan error, 1)
	go func() { errCh <- st.VerifyOwnerLock(context.Background()) }()

	select {
	case <-c.entered:
		// Release the connection so the in-flight query (and the handle's
		// cleanup) can finish before failing; otherwise the cleanup Close would
		// block on the guard-protected connection.
		st.ownerMu.Unlock()
		close(c.release)
		t.Fatal("VerifyOwnerLock queried the pinned connection while the connection guard was held")
	case <-time.After(100 * time.Millisecond):
	}

	st.ownerMu.Unlock()
	select {
	case <-c.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("VerifyOwnerLock did not query the connection after the guard was released")
	}
	close(c.release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("VerifyOwnerLock = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("VerifyOwnerLock did not complete")
	}
}

// TestCloseWaitsForConnectionGuard proves Close serializes on the same guard:
// while it is held, Close must NOT close/clear the pinned connection, and it
// completes only once the guard is released (after which a verification fails
// closed). Without the guard Close would proceed immediately.
func TestCloseWaitsForConnectionGuard(t *testing.T) {
	c := newFakeLockConnector()
	st := fakeOwnerState(t, c)

	st.ownerMu.Lock()
	closeErr := make(chan error, 1)
	go func() { closeErr <- st.Close() }()

	select {
	case <-closeErr:
		st.ownerMu.Unlock()
		t.Fatal("Close completed while the connection guard was held")
	case <-time.After(100 * time.Millisecond):
	}

	st.ownerMu.Unlock()
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Close = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete after the guard was released")
	}
	if err := st.VerifyOwnerLock(context.Background()); !errors.Is(err, ErrOwnerLockLost) {
		t.Fatalf("VerifyOwnerLock after close = %v, want ErrOwnerLockLost", err)
	}
}

// TestVerifyOwnerLockQueuedBehindGuardFailsClosed pins the boundedness contract:
// a verification queued behind an in-flight one waits for the guard, then runs
// under its OWN context; if the connection never becomes usable the check fails
// closed with ErrOwnerLockLost rather than hanging.
func TestVerifyOwnerLockQueuedBehindGuardFailsClosed(t *testing.T) {
	c := newFakeLockConnector()
	st := fakeOwnerState(t, c)
	// The connection is never released: both checks must fail closed by context.

	st.ownerMu.Lock()

	type result struct {
		index int
		err   error
	}
	results := make(chan result, 2)

	// The first check queues behind the held guard and must fail closed once its
	// own (short) context expires; the second, queued behind the same guard, must
	// then also fail closed rather than hang.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		results <- result{index: 0, err: st.VerifyOwnerLock(ctx)}
	}()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		results <- result{index: 1, err: st.VerifyOwnerLock(ctx)}
	}()

	time.Sleep(150 * time.Millisecond)
	st.ownerMu.Unlock()

	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			if !errors.Is(r.err, ErrOwnerLockLost) {
				t.Fatalf("verification %d = %v, want ErrOwnerLockLost (fail closed)", r.index, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a verification did not fail closed within its context")
		}
	}
	// Below is the boundedness assertion: both checks are queued behind the held
	// guard for their whole (short) context lifetime, so at most one may reach
	// the pinned connection. database/sql may instead short-circuit an
	// already-expired context before driver dispatch, leaving the high-water
	// mark at 0, so only exceeding one violates serialization.
	if got := c.max(); got > 1 {
		t.Fatalf("max concurrent verifications = %d, want <= 1", got)
	}
}

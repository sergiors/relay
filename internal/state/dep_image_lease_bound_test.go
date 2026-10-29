package state

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// depBoundStalledDSN returns a DSN whose TCP endpoint accepts connections but
// never speaks the PostgreSQL startup protocol, so a client's connect/query
// attempt stalls until its context expires. It is a deterministic stand-in for a
// wedged database or network partition (no driver mock, no timing sleeps on the
// happy path), and it needs no real database.
func depBoundStalledDSN(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-accepted
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return "postgres://relay:relay@" + ln.Addr().String() + "/relay?sslmode=disable"
}

// depBoundStalledState returns an ownerless State whose pool can never complete
// a PostgreSQL handshake, so every coordination query stalls.
func depBoundStalledState(t *testing.T) *State {
	t.Helper()
	db, err := sql.Open("pgx", depBoundStalledDSN(t))
	if err != nil {
		t.Fatalf("open stalled pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &State{db: db, log: fallbackLogger}
}

// depBoundAssertLocksReleased proves neither coordination mutex was retained
// after a call. TryLock is immediate and therefore cannot itself block.
func depBoundAssertLocksReleased(t *testing.T, st *State) {
	t.Helper()
	if !st.depConnMu.TryLock() {
		t.Fatal("depConnMu was still held after the call returned")
	}
	st.depConnMu.Unlock()
	if !st.ownerMu.TryLock() {
		t.Fatal("ownerMu was still held after the call returned")
	}
	st.ownerMu.Unlock()
}

// TestDependencyCoordQueriesAreBounded proves every dependency-image
// coordination operation that runs on the pinned coordination connection is
// bounded to the short operation timeout instead of the caller's (potentially
// unbounded/lifecycle) context. The stalled endpoint would otherwise hold the
// caller until its own deadline — and, for an ownerful handle, hold ownerMu and
// therefore delay VerifyOwnerLock and ownership-loss detection.
func TestDependencyCoordQueriesAreBounded(t *testing.T) {
	const (
		ref = "relay-dep-bound-test"
		// A caller bound well above the internal operation bound: returning
		// materially before it proves the internal bound fired.
		callerBound = 3 * time.Second
		wantBelow   = 2500 * time.Millisecond
	)
	st := depBoundStalledState(t)

	lease := func() *dependencyImageLease {
		return &dependencyImageLease{
			st:     st,
			ref:    ref,
			lockID: dependencyImageLockIDFor(ref),
			mode:   dependencyImageUseLease,
		}
	}

	cases := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{
			name: "shared use acquisition",
			call: func(ctx context.Context) error {
				_, err := st.AcquireDependencyImageUse(ctx, ref)
				return err
			},
		},
		{
			name: "exclusive GC acquisition",
			call: func(ctx context.Context) error {
				_, _, err := st.TryAcquireDependencyImageGC(ctx, ref)
				return err
			},
		},
		{
			name: "lease verify",
			call: func(ctx context.Context) error {
				return lease().Verify(ctx)
			},
		},
		{
			name: "lease release (bounded unlock query)",
			call: func(context.Context) error {
				return lease().Release()
			},
		},
		{
			name: "lazy coordination connection",
			call: func(ctx context.Context) error {
				return st.withDependencyCoordConn(ctx,
					func(context.Context, *sql.Conn, int) error {
						t.Error("query closure ran despite a stalled connection allocation")
						return nil
					})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), callerBound)
			defer cancel()
			start := time.Now()
			err := tc.call(ctx)
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("a stalled coordination query must fail closed")
			}
			if elapsed >= wantBelow {
				t.Fatalf("call took %s, want it bounded by the internal operation timeout (< %s), not the caller bound",
					elapsed, wantBelow)
			}
			depBoundAssertLocksReleased(t, st)
		})
	}
}

// TestDependencyCoordQueryContextIsCappedByCaller proves the bound is the SMALLER
// of the internal operation timeout and the caller's deadline, and that the
// context handed to the query (which is what the ownerMu/depConnMu hold is
// bounded by) carries that deadline.
func TestDependencyCoordQueryContextIsCappedByCaller(t *testing.T) {
	// Ownerful handle: fn inspects the bounded context it is handed and records
	// the backend PID. The connection is never queried, so a zero-value *sql.Conn
	// is sufficient — the helper only requires ownerConn to be non-nil to use the
	// ownerful path.
	st := &State{
		nodeID:    "dep-bound-worker",
		ownerConn: new(sql.Conn),
		ownerPID:  4242,
		log:       fallbackLogger,
	}
	errSentinel := errors.New("closure sentinel")

	check := func(t *testing.T, callerCtx context.Context, maxRemaining time.Duration) {
		t.Helper()
		err := st.withDependencyCoordConn(callerCtx,
			func(queryCtx context.Context, conn *sql.Conn, pid int) error {
				if conn != st.ownerConn {
					t.Error("closure did not receive the owner connection")
				}
				if pid != st.ownerPID {
					t.Errorf("backend pid = %d, want %d", pid, st.ownerPID)
				}
				deadline, ok := queryCtx.Deadline()
				if !ok {
					t.Fatal("query context carries no deadline, so the ownerMu hold would be unbounded")
				}
				if remaining := time.Until(deadline); remaining > maxRemaining {
					t.Fatalf("query deadline is %s away, want at most %s", remaining, maxRemaining)
				}
				return errSentinel
			})
		if !errors.Is(err, errSentinel) {
			t.Fatalf("closure error = %v, want the sentinel", err)
		}
		depBoundAssertLocksReleased(t, st)
	}

	t.Run("internal operation bound", func(t *testing.T) {
		check(t, context.Background(), dependencyQueryTimeout+100*time.Millisecond)
	})
	t.Run("caller deadline is shorter", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		check(t, ctx, 300*time.Millisecond)
	})
}

// TestDependencyCoordCallerDeadlineWinsOnStalledQuery proves the caller's shorter
// deadline caps a real stalled query too (not just the context handed to the
// closure), and that an already-expired context grants nothing.
func TestDependencyCoordCallerDeadlineWinsOnStalledQuery(t *testing.T) {
	st := depBoundStalledState(t)
	ref := "relay-dep-bound-caller"

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := st.AcquireDependencyImageUse(ctx, ref); err == nil {
		t.Fatal("a stalled acquisition must fail closed")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("acquisition took %s, want the caller's 250ms deadline to cap it", elapsed)
	}
	depBoundAssertLocksReleased(t, st)

	expired, expiredCancel := context.WithCancel(context.Background())
	expiredCancel()
	if _, err := st.AcquireDependencyImageUse(expired, ref); err == nil {
		t.Fatal("an already-cancelled context must grant nothing")
	}
	if !errors.Is(expired.Err(), context.Canceled) {
		t.Fatal("test context was not cancelled")
	}
}

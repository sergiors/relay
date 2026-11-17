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
	"syscall"
	"testing"
)

// These deterministic unit tests pin how VerifyOwnerLock TYPED-classifies a
// failed verification on the pinned lock connection — no PostgreSQL and no
// message matching: the fake driver from ownership_serialization_test.go
// scripts the query result or failure. Every case fails closed with
// ErrOwnerLockLost; the narrower sentinels separate a CONFIRMED loss (the lock
// is proven gone or the pinned backend changed) from an UNVERIFIED failure (a
// timeout, a typed PostgreSQL/network connection error, or any other query
// error). The worker's structured ownership warning reports exactly this
// distinction.

// fakeSQLStateError is a server-reported failure carrying its SQLSTATE through
// the standard interface, the shape of a pgconn.PgError.
type fakeSQLStateError struct {
	sqlState string
	msg      string
}

func (e fakeSQLStateError) Error() string    { return e.msg }
func (e fakeSQLStateError) SQLState() string { return e.sqlState }

// assertOwnershipClassification pins which sentinels a fail-closed verification
// error must and must not carry.
func assertOwnershipClassification(t *testing.T, err error, want, not []error) {
	t.Helper()
	if err == nil {
		t.Fatal("verification = nil, want a fail-closed error")
	}
	for _, sentinel := range want {
		if !errors.Is(err, sentinel) {
			t.Errorf("verification %v must wrap %v", err, sentinel)
		}
	}
	for _, sentinel := range not {
		if errors.Is(err, sentinel) {
			t.Errorf("verification %v must not wrap %v", err, sentinel)
		}
	}
}

// TestVerifyOwnerLockClassifiesConfirmedLosses pins the CONFIRMED-loss paths:
// the pinned backend answers and proves the identity is gone — either the
// session advisory lock is absent from pg_locks or the connection reports a
// different backend PID than the one captured at open. Both carry
// ErrOwnerLockLost for existing fail-closed callers plus the specific
// confirmed sentinel, and never the connection sentinel.
func TestVerifyOwnerLockClassifiesConfirmedLosses(t *testing.T) {
	t.Run("absent session advisory lock", func(t *testing.T) {
		c := newFakeLockConnector()
		c.held = false
		close(c.release) // the query answers; the row reports the lock absent
		st := fakeOwnerState(t, c)

		err := st.VerifyOwnerLock(context.Background())
		assertOwnershipClassification(t, err,
			[]error{ErrOwnerLockLost, ErrOwnerLockNotHeld},
			[]error{ErrOwnerBackendPIDChanged, ErrOwnerLockConnection})
		if !strings.Contains(err.Error(), "is no longer held") ||
			!strings.Contains(err.Error(), st.nodeID) {
			t.Fatalf("error %q must name the absent lock and its node", err)
		}
	})

	t.Run("backend pid changed", func(t *testing.T) {
		c := newFakeLockConnector()
		close(c.release) // the query answers; the row reports a different pid
		st := fakeOwnerState(t, c)
		// The connection now answers from a different backend than the one
		// recorded when the lock was acquired.
		st.ownerPID = 99

		err := st.VerifyOwnerLock(context.Background())
		assertOwnershipClassification(t, err,
			[]error{ErrOwnerLockLost, ErrOwnerBackendPIDChanged},
			[]error{ErrOwnerLockNotHeld, ErrOwnerLockConnection})
		if !strings.Contains(err.Error(), "pid changed from 99 to 1") {
			t.Fatalf("error %q must name both backend pids", err)
		}
	})

	t.Run("released lock connection", func(t *testing.T) {
		st := fakeOwnerState(t, newFakeLockConnector())
		if err := st.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		err := st.VerifyOwnerLock(context.Background())
		assertOwnershipClassification(t, err,
			[]error{ErrOwnerLockLost, ErrOwnerLockConnection},
			[]error{ErrOwnerLockNotHeld, ErrOwnerBackendPIDChanged})
		if !strings.Contains(err.Error(), "already released") {
			t.Fatalf("error %q must name the released connection", err)
		}
	})
}

// TestVerifyOwnerLockClassifiesUnverifiedFailures pins the UNVERIFIED paths:
// the check could not prove anything, so it fails closed with ErrOwnerLockLost
// but must never claim a confirmed loss. Typed PostgreSQL/network connection
// failures carry the connection sentinel; any other driver/server error carries
// only the base sentinel; a timed-out check preserves the deadline so the
// worker can report it as a timeout (the worker checks timeouts first, so the
// connection sentinel a deadline also triggers never surfaces as the reason).
func TestVerifyOwnerLockClassifiesUnverifiedFailures(t *testing.T) {
	cases := []struct {
		name       string
		queryErr   error
		want       []error
		not        []error
		wantInText string
	}{
		{
			name:     "typed network connection failure",
			queryErr: &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET},
			want:     []error{ErrOwnerLockLost, ErrOwnerLockConnection},
			not:      []error{ErrOwnerLockNotHeld, ErrOwnerBackendPIDChanged},
		},
		{
			name:       "server connection exception SQLSTATE",
			queryErr:   fakeSQLStateError{sqlState: "08006", msg: "connection failure during simple query"},
			want:       []error{ErrOwnerLockLost, ErrOwnerLockConnection},
			not:        []error{ErrOwnerLockNotHeld, ErrOwnerBackendPIDChanged},
			wantInText: "connection failure during simple query",
		},
		{
			name:     "server admin-shutdown SQLSTATE",
			queryErr: fakeSQLStateError{sqlState: "57P01", msg: "terminating connection due to administrator command"},
			want:     []error{ErrOwnerLockLost, ErrOwnerLockConnection},
			not:      []error{ErrOwnerLockNotHeld, ErrOwnerBackendPIDChanged},
		},
		{
			name:     "bounded check timed out",
			queryErr: context.DeadlineExceeded,
			want: []error{
				ErrOwnerLockLost,
				// A deadline satisfies net.Error, so it also matches the
				// connection sentinel; the worker orders the timeout check
				// first and reports verification_timeout.
				ErrOwnerLockConnection,
				context.DeadlineExceeded,
			},
			not:        []error{ErrOwnerLockNotHeld, ErrOwnerBackendPIDChanged},
			wantInText: "context deadline exceeded",
		},
		{
			name:     "server error that is not a connection failure",
			queryErr: fakeSQLStateError{sqlState: "22001", msg: "value too long for target column"},
			want:     []error{ErrOwnerLockLost},
			not: []error{
				ErrOwnerLockNotHeld,
				ErrOwnerBackendPIDChanged,
				ErrOwnerLockConnection,
			},
			wantInText: "value too long",
		},
		{
			name:     "unknown driver error",
			queryErr: errors.New("driver returned an unexpected response"),
			want:     []error{ErrOwnerLockLost},
			not: []error{
				ErrOwnerLockNotHeld,
				ErrOwnerBackendPIDChanged,
				ErrOwnerLockConnection,
			},
			wantInText: "unexpected response",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newFakeLockConnector()
			c.queryErr = tc.queryErr
			st := fakeOwnerState(t, c)

			err := st.VerifyOwnerLock(context.Background())
			assertOwnershipClassification(t, err, tc.want, tc.not)
			if tc.wantInText != "" && !strings.Contains(err.Error(), tc.wantInText) {
				t.Fatalf("error %q must preserve the cause text %q", err, tc.wantInText)
			}
			// The scripted failure must stay errors.Is-reachable through the
			// wrap, so callers can still classify the underlying cause.
			if !errors.Is(err, tc.queryErr) {
				t.Fatalf("error %v must preserve the query error %v", err, tc.queryErr)
			}
		})
	}
}

// TestIsOwnerLockConnectionErrorTypedClassification pins the typed connection
// classifier itself as a pure function: it recognizes the standard
// database/sql, network, syscall, and SQLSTATE failure identities and nothing
// else — never message text. driver.ErrBadConn is included here rather than
// end to end because database/sql owns that sentinel's dispatch.
func TestIsOwnerLockConnectionErrorTypedClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "driver.ErrBadConn", err: driver.ErrBadConn, want: true},
		{name: "sql.ErrConnDone", err: sql.ErrConnDone, want: true},
		{name: "net.ErrClosed", err: net.ErrClosed, want: true},
		{name: "io.EOF", err: io.EOF, want: true},
		{name: "io.ErrUnexpectedEOF", err: io.ErrUnexpectedEOF, want: true},
		{
			name: "network dial error",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: true,
		},
		{
			name: "wrapped syscall errno",
			err:  fmt.Errorf("write: %w", syscall.EPIPE),
			want: true,
		},
		{
			name: "SQLSTATE class 08 connection exception",
			err:  fakeSQLStateError{sqlState: "08P01", msg: "protocol violation"},
			want: true,
		},
		{
			name: "SQLSTATE 57P03 cannot connect now",
			err:  fakeSQLStateError{sqlState: "57P03", msg: "the database system is starting up"},
			want: true,
		},
		{
			name: "other SQLSTATE is not a connection failure",
			err:  fakeSQLStateError{sqlState: "40001", msg: "serialization failure"},
			want: false,
		},
		{
			name: "context deadline is a network timeout",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{name: "message text alone never classifies", err: errors.New("connection refused"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOwnerLockConnectionError(tc.err); got != tc.want {
				t.Fatalf("isOwnerLockConnectionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

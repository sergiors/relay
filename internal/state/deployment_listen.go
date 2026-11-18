package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// deploymentNotifyChannel is the PostgreSQL NOTIFY channel carrying desired
// revision hints. There is exactly one shared desired revision, so one channel
// is sufficient. Notifications are HINTS only: the payload is empty and every
// listener must re-read deployment_desired, because a hint can be delivered
// after a later revision already replaced it and no notification is ever
// replayed to a listener that was disconnected when it was sent.
const deploymentNotifyChannel = "relay_deployment_desired"

// deploymentListenerCloseTimeout bounds the UNLISTEN issued by Close so a slow
// or wedged cleanup can never hold the worker's shutdown barrier indefinitely.
const deploymentListenerCloseTimeout = 5 * time.Second

// ErrDeploymentListenerClosed reports that a listener session was closed while
// a wait was outstanding. It is a normal shutdown outcome, not a database
// failure.
var ErrDeploymentListenerClosed = errors.New("deployment desired listener closed")

// DeploymentDesiredListener is one dedicated PostgreSQL LISTEN session for
// desired revision hints.
//
// LISTEN is session state: PostgreSQL delivers a notification to the exact
// backend that subscribed, so the subscription must run on its own pinned
// physical connection rather than a pooled one. This listener therefore owns a
// *sql.Conn acquired from the pool for its whole lifetime (one reserved pool
// slot, like the pinned ownership connection). It never uses the ownership or
// dependency-image coordination connections, so a long blocking wait can never
// serialize unrelated state work.
//
// Deployment requires either a direct connection or a session-pooling proxy.
// A transaction-pooling proxy (for example PgBouncer in transaction mode) does
// not preserve the session between statements, so LISTEN is lost and delivery
// silently degrades to the coordinator's fallback poll. Relay treats every
// subscription failure as non-fatal for this reason (see the worker
// supervisor): notifications only reduce convergence latency and are never
// required for correctness.
type DeploymentDesiredListener struct {
	conn *sql.Conn
	// closed is canceled by Close so an outstanding Wait returns even when the
	// caller's context is still live. Wait derives its blocking context from
	// both.
	closed      context.Context
	closeCancel context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
}

// ListenDeploymentDesired opens a dedicated connection from the state pool and
// subscribes it to the desired revision channel. The supplied context bounds
// only connection acquisition and the LISTEN statement; the returned listener's
// lifetime is controlled by Wait and Close.
//
// The listener is not required for correctness: a failure to open it leaves the
// coordinator's periodic reconciliation in place.
func (st *State) ListenDeploymentDesired(ctx context.Context) (*DeploymentDesiredListener, error) {
	if st == nil || st.db == nil {
		return nil, fmt.Errorf("listen deployment desired: state is not open")
	}
	conn, err := st.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("listen deployment desired: acquire connection: %w", err)
	}
	listener := &DeploymentDesiredListener{conn: conn}
	listener.closed, listener.closeCancel = context.WithCancel(context.Background())
	if _, err := conn.ExecContext(ctx,
		"LISTEN "+pgx.Identifier{deploymentNotifyChannel}.Sanitize()); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("listen deployment desired: subscribe: %w", err)
	}
	return listener, nil
}

// Wait blocks until the server delivers a notification or ctx is done. It
// reports whether a hint arrived; a false result with a nil error is never
// returned. Any received notification is a hint to re-read the authoritative
// desired row — its payload carries no state.
//
// Wait runs the pgx wait on the pinned physical connection through
// database/sql's Raw seam so no other caller can use that session concurrently.
// A connection-level failure is returned for the caller to reconnect; context
// cancellation is reported as the context error and does not poison the
// session.
func (l *DeploymentDesiredListener) Wait(ctx context.Context) (bool, error) {
	if l == nil {
		return false, fmt.Errorf("wait deployment desired: listener is nil")
	}
	if l.closed.Err() != nil {
		// Never start a wait on a session that Close is tearing down; the
		// UNLISTEN and pool release must not race a fresh blocking read.
		return false, ErrDeploymentListenerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(l.closed, cancel)
	defer stop()

	var (
		notified bool
		waitErr  error
	)
	rawErr := l.conn.Raw(func(driverConn any) error {
		std, ok := driverConn.(*stdlib.Conn)
		if !ok {
			waitErr = fmt.Errorf("unexpected driver connection %T", driverConn)
			return nil
		}
		conn := std.Conn()
		if conn == nil {
			waitErr = errors.New("driver connection is closed")
			return nil
		}
		notification, err := conn.WaitForNotification(waitCtx)
		if err != nil {
			waitErr = err
			if l.closed.Err() != nil ||
				errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				// Caller cancellation or an explicit Close leaves the session
				// healthy (pgx restores the socket deadline), so it must not be
				// discarded as broken.
				return nil
			}
			// A real wait failure means the session is unusable. Returning
			// driver.ErrBadConn makes database/sql mark this pinned *sql.Conn
			// done and close the physical connection instead of returning it to
			// the idle pool as healthy for the next acquire.
			return driver.ErrBadConn
		}
		notified = notification != nil
		return nil
	})
	if rawErr != nil && !errors.Is(rawErr, driver.ErrBadConn) {
		return false, fmt.Errorf("wait deployment desired: %w", rawErr)
	}
	if waitErr == nil {
		return notified, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if l.closed.Err() != nil {
		return false, ErrDeploymentListenerClosed
	}
	return false, fmt.Errorf("wait deployment desired: %w", waitErr)
}

// Close cancels an outstanding Wait, removes this session's subscription, and
// releases the dedicated connection back to the pool. It is idempotent and safe
// on a nil receiver.
//
// LISTEN is session state on the exact backend that subscribed, so simply
// returning the *sql.Conn to the pool would leak the subscription into unrelated
// State work that later acquires that physical connection. Close therefore
// issues UNLISTEN on the same pinned backend first. If UNLISTEN cannot be
// confirmed (an unhealthy or unresponsive session), the physical connection is
// forced to be discarded instead of reused.
func (l *DeploymentDesiredListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeCancel()
		l.closeErr = l.release()
	})
	if errors.Is(l.closeErr, sql.ErrConnDone) {
		return nil
	}
	return l.closeErr
}

// release performs the one-time teardown guarded by closeOnce.
func (l *DeploymentDesiredListener) release() error {
	ctx, cancel := context.WithTimeout(context.Background(), deploymentListenerCloseTimeout)
	defer cancel()
	if _, err := l.conn.ExecContext(ctx,
		"UNLISTEN "+pgx.Identifier{deploymentNotifyChannel}.Sanitize()); err != nil {
		// The subscription cannot be proven gone. Never return this backend to
		// the idle pool while it may still be listening: force database/sql to
		// discard the physical connection instead.
		l.discard()
		_ = l.conn.Close()
		return fmt.Errorf("unlisten deployment desired: %w", err)
	}
	return l.conn.Close()
}

// discard marks the pinned driver connection bad so database/sql closes it
// rather than returning it to the pool. It is best-effort: an already-done
// connection needs no further action.
func (l *DeploymentDesiredListener) discard() {
	_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
}

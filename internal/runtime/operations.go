package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// managerOps is the Manager's single admission, tracking, cancellation, and
// drain authority for every Manager-owned Docker operation.
//
// It has exactly TWO phases, and the phase boundary is what makes shutdown
// safe:
//
//   - The NORMAL phase admits ordinary Manager work (preparation, service
//     start/stop/list, sweeps, image inspect/pull/listing/removal, readiness
//     Ping, execution-container create/acquire, execution-container teardown and
//     the exit-wait watcher). Closing normal admission cancels every normal
//     operation's context, and drainNormal then waits for all of them to
//     release. No normal operation may be admitted once normal admission has
//     closed.
//
//   - The TEARDOWN phase is opened by CloseContext only AFTER normal operations
//     are fully drained, and it owns Manager cache teardown. Closing teardown
//     admission is atomic, and drainTeardown then waits for teardown operations
//     to release. No operation of either kind may be admitted after that final
//     drain starts.
//
// The phases are deliberately separate: an ordinary discard that used teardown
// admission could be admitted after drainNormal observed zero and race the
// cache teardown. Normal work is therefore never admitted through the teardown
// phase; only Manager cache teardown is.
//
// Ordinary admission is closed in TWO senses, and both are checked under this
// coordinator's lock so the decision is atomic with the registration:
//
//   - once CloseContext has begun (normalClosed), and
//   - once the normal shutdown context is cancelled — i.e. as soon as the
//     manager lifecycle is cancelled (the worker's signal context, or Close).
//     An operation admitted after that point would receive an already-cancelled
//     operation context, so it is refused terminally instead: cancelled work
//     must not be admitted and must issue no Docker call.
//
// The one narrow exception is beginShutdownCleanup: a shutdown-only
// authorization available ONLY while the manager is already shutting down and
// CloseContext has NOT begun. It is what lets the worker's hostname-scoped
// service-container cleanup run after the lifecycle is cancelled (the worker
// cancels it before running the ordered teardown steps, so ordinary admission
// is already refused by then) while remaining tracked by the ordinary count,
// so CloseContext still joins every such operation before the client closes.
type managerOps struct {
	mu sync.Mutex

	// --- normal phase ---
	// normalClosed is set once by closeNormal: the admission gate for ordinary
	// work.
	normalClosed bool
	// normalCtx is the manager shutdown context, cancelled by closeNormal (and by
	// the manager lifecycle it is derived from). Every normal operation's
	// admission context is derived from it, so close cancels in-flight normal
	// work. Builds are the one exception: the Prepare operation is admitted here
	// (so close refuses a new preparation), but its Docker build calls run on the
	// lifecycle-rooted buildCtx (see buildContext), which CloseContext's
	// lifecycle cancellation stops BEFORE the drains rather than through this
	// normal operation context.
	normalCtx    context.Context
	normalCancel context.CancelFunc
	// normalOps counts admitted normal operations from admission to release;
	// normalIdle is closed whenever it reaches zero, so drainNormal can block on
	// a stable snapshot. Guarded by mu.
	normalOps  int
	normalIdle chan struct{}

	// --- teardown phase ---
	// teardownOpen is set by openTeardown and cleared by closeTeardown; admission
	// requires it. teardownClosed records that teardown admission has been closed
	// (so an accidental re-open is refused).
	teardownOpen   bool
	teardownClosed bool
	// teardownOps counts admitted teardown operations; teardownIdle is closed
	// whenever it reaches zero. Guarded by mu.
	teardownOps  int
	teardownIdle chan struct{}
}

// newManagerOps returns a coordinator whose normal shutdown context is a child
// of parent (the manager lifecycle), so cancelling the lifecycle also cancels
// every admitted normal operation, independent of Close. A nil parent roots the
// shutdown context at context.Background, which supports a Manager constructed
// directly by tests.
func newManagerOps(parent context.Context) *managerOps {
	if parent == nil {
		parent = context.Background()
	}
	o := &managerOps{}
	o.normalCtx, o.normalCancel = context.WithCancel(parent)
	o.normalIdle = make(chan struct{})
	close(o.normalIdle)
	o.teardownIdle = make(chan struct{})
	close(o.teardownIdle)
	return o
}

// lazyInitLocked makes a zero-valued coordinator usable. It must be called with
// o.mu held.
func (o *managerOps) lazyInitLocked() {
	if o.normalCtx == nil {
		o.normalCtx, o.normalCancel = context.WithCancel(context.Background())
	}
	if o.normalIdle == nil {
		o.normalIdle = make(chan struct{})
		close(o.normalIdle)
	}
	if o.teardownIdle == nil {
		o.teardownIdle = make(chan struct{})
		close(o.teardownIdle)
	}
}

// enterNormalLocked registers one normal operation. The caller must hold o.mu
// and must have decided the operation is admitted.
func (o *managerOps) enterNormalLocked() {
	if o.normalOps == 0 {
		o.normalIdle = make(chan struct{})
	}
	o.normalOps++
}

// leaveNormalLocked discharges one normal operation and closes normalIdle when
// the count returns to zero. The caller must hold o.mu.
func (o *managerOps) leaveNormalLocked() {
	if o.normalOps > 0 {
		o.normalOps--
	}
	if o.normalOps == 0 {
		closeIfOpen(o.normalIdle)
	}
}

// closeIfOpen closes ch unless it is already closed.
func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
		// Already closed: leave it closed.
	default:
		close(ch)
	}
}

// errOrdinaryRefused is the terminal admission refusal for ordinary Manager
// work: either CloseContext has begun or the manager is already shutting down.
func errOrdinaryRefused() error {
	return fmt.Errorf("manager docker operation: %w", ErrManagerShuttingDown)
}

// errShutdownCleanupUnavailable is returned when the shutdown-only cleanup
// authorization is requested while the manager is NOT shutting down: the
// ordinary gate is the correct path there, so this path can never be used to
// bypass lifecycle cancellation in steady state.
var errShutdownCleanupUnavailable = errors.New(
	"runtime: shutdown cleanup refused: manager is not shutting down")

// ordinaryRefused returns the terminal refusal for ordinary Manager work when
// the manager is shutting down, or nil when ordinary work may proceed. It is
// the single predicate behind the ordinary admission gate, exposed so a Manager
// entry point that can complete WITHOUT issuing a Docker call — a warm-cache
// execution acquire — still refuses lifecycle-cancelled work rather than
// handing back a pooled container. o.mu must be held by the caller.
func (o *managerOps) ordinaryRefusedLocked() error {
	if o.normalClosed || o.normalCtx.Err() != nil {
		return errOrdinaryRefused()
	}
	return nil
}

// begin admits one NORMAL Manager-owned operation. It returns the operation's
// context and a release that MUST be called exactly once when the operation
// concludes. It refuses with ErrManagerShuttingDown once closeNormal has closed
// admission OR once the normal shutdown context is already cancelled (a
// lifecycle-cancelled worker), so ordinary work is never admitted onto an
// already-cancelled operation context. It never starts an operation whose
// caller context is already cancelled (it returns that context error and holds
// no registration).
func (o *managerOps) begin(ctx context.Context) (context.Context, func(), error) {
	return o.admitOrdinary(ctx, false)
}

// beginShutdownCleanup admits one shutdown-only Manager-owned cleanup operation
// (the worker's hostname-scoped service-container removal during graceful
// shutdown), returning its context and a release that MUST be called exactly
// once.
//
// It exists because the worker cancels its lifecycle BEFORE it runs the ordered
// shutdown steps, so by the time the service-cleanup step runs ordinary
// admission is already refused. The authorization is deliberately narrow:
//
//   - it is refused once CloseContext has begun (normalClosed), so no cleanup
//     operation can be admitted once the drains and the client close are under
//     way; and
//   - it is refused unless the normal shutdown context is ALREADY cancelled, so
//     it is available only during shutdown and never bypasses lifecycle
//     cancellation in steady state.
//
// The operation is registered in the SAME ordinary count, so CloseContext's
// drainNormal joins it (and any in-flight Docker call it owns) before the
// Docker client closes, even when the caller's shutdown step has already timed
// out and moved on. Its context is derived from the caller's ctx ALONE: the
// normal shutdown context is already cancelled, so merging it would abort
// immediately the very cleanup this path exists to perform. The caller still
// owns the bound (the shutdown registry's fresh, bounded step context).
func (o *managerOps) beginShutdownCleanup(ctx context.Context) (context.Context, func(), error) {
	return o.admitOrdinary(ctx, true)
}

// admitOrdinary implements begin and beginShutdownCleanup. shutdownCleanup
// selects the narrow shutdown-only authorization described on
// beginShutdownCleanup; the ordinary path additionally cancels the operation
// when the normal shutdown context is cancelled.
func (o *managerOps) admitOrdinary(ctx context.Context, shutdownCleanup bool) (context.Context, func(), error) {
	o.mu.Lock()
	o.lazyInitLocked()
	if o.normalClosed {
		o.mu.Unlock()
		return nil, nil, errOrdinaryRefused()
	}
	shutdown := o.normalCtx
	if shutdownCleanup {
		if shutdown.Err() == nil {
			o.mu.Unlock()
			return nil, nil, errShutdownCleanupUnavailable
		}
	} else if err := o.ordinaryRefusedLocked(); err != nil {
		o.mu.Unlock()
		return nil, nil, err
	}
	o.enterNormalLocked()
	o.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		// The caller's bound already fired: do not hold a registration for an
		// operation that will never run.
		o.mu.Lock()
		o.leaveNormalLocked()
		o.mu.Unlock()
		return nil, nil, ctx.Err()
	}
	var extra context.Context
	if !shutdownCleanup {
		extra = shutdown
	}
	opCtx, cancel := deriveOpContext(ctx, extra)
	once := &sync.Once{}
	release := func() {
		once.Do(func() {
			cancel()
			o.mu.Lock()
			o.lazyInitLocked()
			o.leaveNormalLocked()
			o.mu.Unlock()
		})
	}
	return opCtx, release, nil
}

// closeNormal closes admission for ordinary operations and cancels the normal
// shutdown context, so every in-flight normal operation observes cancellation
// and no new normal operation can begin. It is idempotent and does NOT wait;
// drainNormal performs the join.
func (o *managerOps) closeNormal() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	if o.normalClosed {
		return
	}
	o.normalClosed = true
	if o.normalCancel != nil {
		o.normalCancel()
	}
}

// drainNormal blocks until every admitted normal operation has released. It
// deliberately takes no context: returning early because a caller's bound
// expired is exactly the race this barrier exists to eliminate, and termination
// is already guaranteed by closeNormal cancelling every normal operation's
// derived context.
func (o *managerOps) drainNormal() {
	o.mu.Lock()
	o.lazyInitLocked()
	idle := o.normalIdle
	o.mu.Unlock()
	<-idle
}

// openTeardown opens the teardown phase. It must be called only after
// closeNormal and drainNormal, so no normal operation can be admitted while
// teardown admission is open and no normal operation is still in flight.
func (o *managerOps) openTeardown() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	if o.teardownClosed {
		return
	}
	o.teardownOpen = true
}

// beginTeardown admits one shutdown-owned operation for Manager cache teardown.
// It refuses unless the teardown phase is open (and not yet closed), so an
// operation can never be admitted after the final teardown drain starts. Its
// context is derived from the caller's ctx ONLY: the normal shutdown context is
// already cancelled, so merging it would immediately abort the teardown the
// phase exists to perform.
func (o *managerOps) beginTeardown(ctx context.Context) (context.Context, func(), error) {
	o.mu.Lock()
	o.lazyInitLocked()
	if !o.teardownOpen || o.teardownClosed {
		o.mu.Unlock()
		return nil, nil, fmt.Errorf("manager teardown operation: %w", ErrManagerShuttingDown)
	}
	if o.teardownOps == 0 {
		o.teardownIdle = make(chan struct{})
	}
	o.teardownOps++
	o.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	opCtx, cancel := deriveOpContext(ctx, nil)
	once := &sync.Once{}
	release := func() {
		once.Do(func() {
			cancel()
			o.mu.Lock()
			o.lazyInitLocked()
			if o.teardownOps > 0 {
				o.teardownOps--
			}
			if o.teardownOps == 0 {
				closeIfOpen(o.teardownIdle)
			}
			o.mu.Unlock()
		})
	}
	return opCtx, release, nil
}

// closeTeardown closes teardown admission atomically. After it returns, no
// teardown operation may be admitted.
func (o *managerOps) closeTeardown() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	o.teardownOpen = false
	o.teardownClosed = true
}

// drainTeardown blocks until every admitted teardown operation has released.
func (o *managerOps) drainTeardown() {
	o.mu.Lock()
	o.lazyInitLocked()
	idle := o.teardownIdle
	o.mu.Unlock()
	<-idle
}

// deriveOpContext builds an operation context from ctx, additionally cancelled
// when extra is done (extra nil skips the merge). A pre-cancelled parent is
// propagated synchronously rather than waiting for an AfterFunc goroutine, so a
// just-closed manager cancels the new operation before its first Docker call.
func deriveOpContext(ctx context.Context, extra context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	opCtx, cancel := context.WithCancel(ctx)
	if extra != nil {
		if extra.Err() != nil {
			cancel()
		} else {
			stop := context.AfterFunc(extra, cancel)
			return opCtx, func() { stop(); cancel() }
		}
	}
	if ctx.Err() != nil {
		cancel()
	}
	return opCtx, cancel
}

// context returns the normal-phase shutdown context. It is the single
// manager-owned cancellation signal handed to a container's exit-wait watcher,
// so the watcher is cancelled when Manager close begins.
func (o *managerOps) context() context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	return o.normalCtx
}

// isClosing reports whether closeNormal has closed ordinary admission. It lets a
// create path classify a container that completed after shutdown began as
// unusable rather than handing it back to a closing manager.
func (o *managerOps) isClosing() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	return o.normalClosed
}

// classify translates a Docker error that raced Manager shutdown into a wrapped
// ErrManagerShuttingDown, so an expected shutdown cancellation is never mistaken
// for a genuine Docker failure. A caller-owned cancellation (ctx already done)
// is returned unchanged, so a real caller deadline is never masked. It is the
// general-operation analogue of imageCoordinator.shutdownErr.
func (o *managerOps) classify(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return err
	}
	if o.isClosing() {
		return fmt.Errorf("%w: %w", ErrManagerShuttingDown, err)
	}
	return err
}

// count returns the number of currently admitted operations across both phases.
// It is a diagnostic helper for tests; production relies on the drains.
func (o *managerOps) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	return o.normalOps + o.teardownOps
}

// opsCoord returns the manager's Docker operation lifecycle, lazily initializing
// it on first use. Exactly one exists per Manager and it lives for the manager's
// whole lifetime, so the admission gates, the cancellation signal, and the
// drains all observe the same state. Lazy initialization is what lets a Manager
// built without NewManager (a zero-value/test-constructed one) still own that
// single authority. A nil receiver is a programming error, not a supported
// state.
func (m *Manager) opsCoord() *managerOps {
	if m == nil {
		panic("runtime: nil Manager")
	}
	m.opsInit.Do(func() {
		if m.ops == nil {
			m.ops = newManagerOps(m.lifecycle)
		}
	})
	return m.ops
}

// beginDockerOp admits one NORMAL Manager-owned Docker operation through the
// manager's single operation gate. Every Manager Docker method funnels through
// it — preparation (reuse probes, self-mount inspect, build), service
// start/restart/stop/list, orphan sweeps, image inspect/pull/listing/removal,
// readiness Ping, and execution-container create/acquire — so manager shutdown
// refuses new work and joins in-flight work before the Docker client closes. The
// returned release must be called exactly once. Execution-container teardown and
// the exit-wait watcher also use normal admission (they are ordinary work); the
// Manager's own cache teardown uses the teardown phase. The only Docker calls
// outside the gate are the pre-publication startup ping (NewManager, before the
// manager is returned to any caller) and the client Close itself (after both
// drains).
func (m *Manager) beginDockerOp(ctx context.Context) (context.Context, func(), error) {
	return m.opsCoord().begin(ctx)
}

// beginShutdownCleanupOp admits one shutdown-only Manager-owned cleanup
// operation through the manager's single operation gate (see
// managerOps.beginShutdownCleanup). It is the ONLY way a Manager-owned Docker
// operation may be admitted after the manager lifecycle has been cancelled, and
// only while CloseContext has not begun. It is reserved for the worker's
// graceful-shutdown service-container cleanup; every other entry point must use
// beginDockerOp and is therefore refused once shutdown starts.
func (m *Manager) beginShutdownCleanupOp(ctx context.Context) (context.Context, func(), error) {
	return m.opsCoord().beginShutdownCleanup(ctx)
}

// ordinaryRefused returns the terminal refusal for ordinary Manager work when
// the manager is shutting down, or nil when ordinary work may proceed. A
// Manager entry point that can complete WITHOUT a Docker call (a warm-cache
// execution acquire) uses it so lifecycle-cancelled work is refused rather than
// admitted with an already-cancelled operation context.
func (m *Manager) ordinaryRefused() error {
	o := m.opsCoord()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lazyInitLocked()
	return o.ordinaryRefusedLocked()
}

// classifyDockerErr converts a Docker error that raced Manager shutdown into
// ErrManagerShuttingDown (see managerOps.classify), so an expected shutdown
// cancellation is reported as the terminal deferral callers already handle
// rather than a spurious cleanup failure.
func (m *Manager) classifyDockerErr(ctx context.Context, err error) error {
	return m.opsCoord().classify(ctx, err)
}

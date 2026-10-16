package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/state"
)

// errOwnershipLost marks a worker run that ended because the pinned WORKER_ID
// lock was detected no longer held (by the periodic monitor or by a fail-closed
// shutdown verification). Run joins it into its returned error (see the override
// defer) so the supervisor treats the exit as an abnormal termination rather
// than a clean signal shutdown. By the time this error surfaces the worker has
// already cancelled its lifecycle and skipped identity-scoped cleanup; it is a
// report, not a remedy.
var errOwnershipLost = errors.New("worker ownership lost")

// ownershipLoss is the once-only latch recording a detected loss of the worker-
// ownership lock (the exclusive session advisory lock state.Open takes on
// WORKER_ID). It is set once by whichever path observes the loss first — the
// periodic monitor, a fail-closed shutdown verification, or an identity-cleanup
// guard's fresh proof — through ownershipLossAction, and read by readiness, the
// deferred error override, and the shutdown guards. It is safe for concurrent
// use and nil-safe to query.
type ownershipLoss struct {
	lost atomic.Bool
	once sync.Once
}

// lose records the loss and returns true only for the FIRST caller, so the loss
// side effects (readiness clear, warning, lifecycle cancellation) run exactly
// once even if several paths observe the same loss.
func (l *ownershipLoss) lose() bool {
	if l == nil {
		return false
	}
	first := false
	l.once.Do(func() {
		l.lost.Store(true)
		first = true
	})
	return first
}

// isLost reports whether a loss has been recorded; nil receiver is false.
func (l *ownershipLoss) isLost() bool {
	return l != nil && l.lost.Load()
}

// ownershipMonitorConfig configures the ownership monitor poll loop. Its
// `verify` seam is the production State.VerifyOwnerLock method; a unit test
// injects a fake so detection is provable without PostgreSQL. The once-only
// loss side effects deliberately live in a separate shared action
// (ownershipLossAction) rather than in this config: the periodic monitor is not
// the only detector, because a fail-closed shutdown verification runs the SAME
// action through the SAME latch.
type ownershipMonitorConfig struct {
	// verify performs ONE ownership check against the pinned lock connection. A
	// nil error means the lock is still held; any error is ambiguous/lost
	// (VerifyOwnerLock already fails closed). Required.
	verify func(context.Context) error
	// interval and timeout bound the poll loop; zero values fall back to the
	// state package defaults.
	interval time.Duration
	timeout  time.Duration
}

// ownershipLossAction returns the ONCE-ONLY side effect of a detected ownership
// loss, run through the latch: on the FIRST observation it marks the latch,
// clears readiness (so a `relay health` query stops reporting ready the moment
// the loss is known, before the deferred shutdown clear), logs one structured
// warning carrying the STABLE worker identity and this process's incarnation
// token, and cancels the lifecycle so background loops, the service
// coordinator, and rooted builds observe the loss exactly as they would a
// signal. The periodic monitor and every fail-closed shutdown verification share
// this ONE action, so the effects run exactly once no matter which path observes
// the loss first. Nil fields are tolerated, so a test can wire only the pieces
// it asserts.
func ownershipLossAction(
	loss *ownershipLoss,
	ready *readiness,
	logger *slog.Logger,
	worker string,
	instance string,
	cancel context.CancelFunc,
) func() {
	return func() {
		if !loss.lose() {
			return
		}
		if ready != nil {
			ready.setOwnershipLost()
		}
		if logger != nil {
			logger.Warn("Worker ownership lost; shutting down",
				"worker_id", worker,
				"instance_id", instance,
			)
		}
		if cancel != nil {
			cancel()
		}
	}
}

// ownershipRunError joins errOwnershipLost into runErr when the ownership latch
// is set, so Run's result is a distinctive abnormal-termination error rather
// than a clean signal-shutdown result. It is the single place the conversion is
// decided, so the contract is unit-testable without exercising Run's full
// resource wiring (mirroring startupResult). A nil runErr yields just
// errOwnershipLost.
func ownershipRunError(loss *ownershipLoss, runErr error) error {
	if loss.isLost() {
		return errors.Join(errOwnershipLost, runErr)
	}
	return runErr
}

// startOwnershipMonitor starts the ownership-polling goroutine and returns a
// channel closed when it exits. The monitor re-verifies the pinned ownership
// lock immediately and then once per interval; on a detected loss it invokes
// onLost (the shared once-only loss action) and returns. On an ordinary
// lifecycle cancellation it returns WITHOUT reporting a loss: VerifyOwnerLock
// fails once the pinned connection is closing, and that is shutdown, not a
// loss. A nil/ownerless verify is the caller's responsibility: Run only starts
// the monitor for a worker handle that holds the lock.
func startOwnershipMonitor(ctx context.Context, onLost func(), cfg ownershipMonitorConfig) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorOwnership(ctx, cfg.verify, onLost, cfg.interval, cfg.timeout)
	}()
	return done
}

// monitorOwnership is the poll loop: it checks ownership immediately so an
// already-lost lock is detected without waiting a full interval, then once per
// tick. A detected loss (check failed on a LIVE lifecycle) invokes onLost and
// returns; a check failure caused by the lifecycle being cancelled is an
// ordinary shutdown and returns without reporting a loss.
func monitorOwnership(
	ctx context.Context,
	verify func(context.Context) error,
	onLost func(),
	interval, timeout time.Duration,
) {
	if interval <= 0 {
		interval = state.OwnerLockCheckInterval
	}
	if timeout <= 0 {
		timeout = state.OwnerLockCheckTimeout
	}
	if lost, stop := ownershipCheck(ctx, verify, timeout); stop {
		if lost {
			onLost()
		}
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if lost, stop := ownershipCheck(ctx, verify, timeout); stop {
				if lost {
					onLost()
				}
				return
			}
		}
	}
}

// ownershipCheck runs one bounded verification. It returns lost=true when the
// check failed on a LIVE lifecycle (a real, detected loss) and stop=true when
// the loop must end: either a loss was detected or the check failed only because
// the lifecycle is being cancelled (an ordinary shutdown, not a loss). A
// successful check returns (false, false).
func ownershipCheck(ctx context.Context, verify func(context.Context) error, timeout time.Duration) (lost, stop bool) {
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	err := verify(checkCtx)
	cancel()
	if err == nil {
		return false, false
	}
	if ctx.Err() != nil {
		return false, true
	}
	return true, true
}

// ownershipProof is the fail-closed, fresh-context ownership proof shared by the
// paths that act on THIS worker identity (its service containers, its stats
// row) or must report a lost identity. It bundles the once-only latch, the
// pinned-session verification seam, and the shared once-only loss action, so
// "is this identity still mine?" has ONE definition: the periodic monitor, the
// final shutdown verification, the identity-cleanup guards, and the startup
// identity-scoped sweeps all agree.
type ownershipProof struct {
	// loss is the once-only latch. A proof already known lost short-circuits so
	// the identity guards never spend a round trip on cleanup they will skip.
	loss *ownershipLoss
	// verify is the pinned-session verification seam (State.VerifyOwnerLock). A
	// nil seam is an ownerless handle and proves successfully, matching
	// VerifyOwnerLock's nil/ownerless no-op contract.
	verify func(context.Context) error
	// onLost is the shared once-only loss action (ownershipLossAction). It is
	// invoked on every failed proof, but only the first observation performs the
	// side effects. Optional.
	onLost func()
}

// prove runs ONE fresh ownership check on a context rooted in
// context.Background — NEVER the worker lifecycle, which Run's shutdown defer
// has already cancelled — so a lock lost since the monitor's last tick (for
// example just before SIGTERM) is still detected during shutdown. The check is
// bounded by OwnerLockCheckTimeout (or timeout when positive), so a single
// stalled verification cannot delay shutdown indefinitely AND a stall FAILS
// CLOSED. A failure invokes the once-only loss action and returns the error; a
// nil seam proves successfully. It never re-acquires the lock: the seam queries
// only the pinned connection.
func (p ownershipProof) prove(timeout time.Duration) error {
	if p.verify == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = state.OwnerLockCheckTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := p.verify(ctx); err != nil {
		if p.onLost != nil {
			p.onLost()
		}
		return err
	}
	return nil
}

// guardIdentityCleanup wraps a shutdown step that operates on THIS worker
// identity (its service containers, its stats row) so it is SKIPPED once
// ownership is lost. Once the lock is gone a replacement process may already own
// WORKER_ID, so "this worker's" cleanup could remove the new owner's service
// containers or overwrite its stats row. The guard checks the latch AND then
// runs a fresh bounded ownership proof on the pinned connection IMMEDIATELY
// before the operation: a loss latched earlier (by the monitor or the final
// shutdown verification) or detected by the fresh proof suppresses the work,
// while a healthy lock keeps the normal cleanup. The proof uses a fresh
// background context (never the step's, and never the cancelled lifecycle),
// while the actual cleanup keeps its existing shutdown step context.
func guardIdentityCleanup(
	proof ownershipProof, logger *slog.Logger, name string, run func(context.Context) error,
) func(context.Context) error {
	return func(ctx context.Context) error {
		if proof.loss.isLost() {
			if logger != nil {
				logger.Debug("Shutdown: skipping identity-scoped cleanup after ownership loss",
					"step", name)
			}
			return nil
		}
		if err := proof.prove(0); err != nil {
			// prove already latched the loss and ran the once-only side effects.
			if logger != nil {
				logger.Debug("Shutdown: skipping identity-scoped cleanup after failed ownership verification",
					"step", name,
					"error", err)
			}
			return nil
		}
		return run(ctx)
	}
}

// ownershipBarrierStep joins the ownership monitor goroutine and then runs ONE
// final fail-closed ownership verification before any later teardown step runs
// (the state handle, and the pinned lock connection the verification reads, are
// still open here). The monitor reads that pinned lock connection, so the join
// is a quiescence barrier: its strict join guarantees the monitor has exited
// before the state handle (and that connection) is closed in a later step.
//
// The final verification closes the residual race in which the lock was lost
// after the monitor's last tick but before the lifecycle was cancelled by
// shutdown: Run's shutdown defer cancels the lifecycle first, the monitor then
// exits classifying the failing check as ordinary shutdown, and only this
// background-context verification still notices the loss. On failure it latches
// the loss (clearing readiness and cancelling the lifecycle exactly once) so
// every identity-scoped cleanup guard registered later skips its work.
func ownershipBarrierStep(done <-chan struct{}, proof ownershipProof, logger *slog.Logger) shutdownStep {
	return shutdownStep{
		name:    shutdownStepOwnership,
		timeout: shutdownStepTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			err := barrierJoin(stepCtx, func(joinCtx context.Context) error {
				return waitSignal(joinCtx, done)
			})
			// Final verification AFTER the real join. It runs on a fresh
			// background context (proof.prove), so a cancelled lifecycle cannot
			// mask a loss the monitor classified as ordinary shutdown, and it
			// FAILS CLOSED on any error (including a single stalled check).
			// A failure is logged at Debug only: the shared loss action already
			// emitted the one structured ownership-loss warning.
			if verr := proof.prove(0); verr != nil {
				if logger != nil {
					logger.Debug("Shutdown: final ownership verification failed; identity-scoped cleanup will be skipped",
						"error", verr)
				}
			}
			return err
		},
	}
}

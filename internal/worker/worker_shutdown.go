package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"relay/internal/reconciler"
	"relay/internal/runtime"
)

// shutdownServiceTimeout is the per-step bound shared by the services-join
// barrier and the service-cleanup step during graceful shutdown. For the barrier
// it is only the wait-before-diagnostic threshold: on expiry the join is
// cancelled and then strictly joined for real, so draining the coordinator and
// its in-flight Applys may take longer. For the cleanup step it bounds the
// best-effort stop/removal of this worker's persistent service containers, so a
// hung Docker call cannot hold shutdown open.
const shutdownServiceTimeout = 30 * time.Second

// shutdownStepTimeout is the per-step bound for the steps that gracefully stop a
// server (scheduler, metrics, webhook). The shutdown registry derives a fresh
// context.Background bound from it per step, so one slow step can never consume
// another step's budget. For the scheduler barrier it is only the
// wait-before-diagnostic threshold: the step is then cancelled and strictly
// joined, so it may exceed this bound. For metrics and webhook (best-effort
// cleanup) it is the actual bound.
const shutdownStepTimeout = 5 * time.Second

// shutdownAggregateTimeout is the aggregate budget for the BEST-EFFORT shutdown
// steps, an internal constant with no user knob. The shutdown registry gives
// every step a context bounded by min(its own cap, what remains of this budget),
// and runs each step in its own goroutine so a step that ignores its context can
// never hang the registry: a best-effort step that misses its deadline is logged
// and the registry moves on to the next step. The budget is deliberately
// generous (2m) relative to the per-step caps (2-30s): with cooperative steps it
// is never reached, and it is the ceiling on how long best-effort cleanup may
// take.
//
// It is NOT a hard cap on process exit. A quiescence barrier step (see
// shutdownStep.barrier) that misses its bound is cancelled and then STRICTLY
// JOINED, so a wedged dependency-holding operation may extend the shutdown past
// this budget. That is a safety-over-latency trade: a later step must never
// close the runtime manager, state DB, or Redis while a reconcile, service pass,
// sweep, loop, or scheduler publisher callback may still be using it. Every
// barrier's joined operation is rooted in the worker lifecycle and individually
// bounded, so it still terminates.
const shutdownAggregateTimeout = 2 * time.Minute

// Shutdown step names. Each names a single teardown step; together they are the
// explicit teardown order (shutdownStepOrder) and the structured `step` field
// on a failure log. Registration happens as resources are acquired — which is
// NOT teardown order (Redis is acquired first and released last) — so the order
// is declared here rather than inherited from registration/LIFO.
const (
	shutdownStepOwnership      = "ownership"
	shutdownStepSocket         = "socket"
	shutdownStepScheduler      = "scheduler"
	shutdownStepReconciler     = "reconciler"
	shutdownStepHousekeeping   = "housekeeping"
	shutdownStepServicesJoin   = "services-join"
	shutdownStepServiceCleanup = "service-cleanup"
	shutdownStepLoops          = "loops"
	shutdownStepStatsFlush     = "stats-flush"
	shutdownStepMetrics        = "metrics"
	shutdownStepWebhook        = "webhook"
	shutdownStepManager        = "manager"
	shutdownStepState          = "state"
	shutdownStepTracing        = "tracing"
	shutdownStepRedis          = "redis"
)

// shutdownStepOrder is the single source of truth for graceful-shutdown
// ordering, walked top to bottom by shutdownRegistry.run. Lifecycle
// cancellation is deliberately NOT a step: Run cancels the lifecycle before
// invoking the registry, so background loops, the coordinator, and rooted
// builds observe cancellation before teardown joins them. A step that was never
// registered (an optional resource that never started) is simply skipped.
//
// Each step is one of two kinds, not a per-step choice made at random:
//
//   - QUIESCENCE BARRIER (shutdownStep.barrier): joins a background operation
//     that holds a shared dependency a LATER step tears down. Missed bound is
//     logged, the context is cancelled, and the step is then strictly joined
//     before the registry advances, so it may exceed both its own bound and the
//     aggregate budget. Barriers:
//     ownership (joins the lock monitor and runs the final fail-closed
//     ownership verification before the state handle closes),
//     scheduler (joins publisher callbacks before Redis closes), reconciler
//     (writes state, drives the manager), housekeeping (startup sweeps use the
//     manager and state DB), services-join (coordinator workers use manager and
//     state DB), loops (the stats and retention loops use the state DB and
//     Redis).
//   - BEST-EFFORT CLEANUP (everything else): bounded, logged on timeout or
//     failure, and never joined past its bound. Cleanup:
//     socket, service-cleanup, stats-flush, metrics, webhook, manager, state,
//     tracing, redis.
//
// Only barrier steps gate shared-dependency teardown. A cleanup step that
// ignores its context is surfaced as a timeout and left running by design; none
// of them is a join of a resource-holding operation.
var shutdownStepOrder = []string{
	shutdownStepOwnership,
	shutdownStepSocket,
	shutdownStepScheduler,
	shutdownStepReconciler,
	shutdownStepHousekeeping,
	shutdownStepServicesJoin,
	shutdownStepServiceCleanup,
	shutdownStepLoops,
	shutdownStepStatsFlush,
	shutdownStepMetrics,
	shutdownStepWebhook,
	shutdownStepManager,
	shutdownStepState,
	shutdownStepTracing,
	shutdownStepRedis,
}

// shutdownStep is one teardown action. run receives a fresh context derived
// from context.Background by the registry and bounded by min(its own timeout,
// the remaining aggregate budget): a non-positive timeout means the step is
// bounded only by the aggregate budget (never unbounded). Its error, when
// non-nil, is logged with the structured name and never aborts the remaining
// steps. A step that ignores its context is surfaced as a timeout rather than
// allowed to hang the registry (the registry runs each step in its own
// goroutine).
//
// A step is either a best-effort cleanup step (barrier false) or a quiescence
// barrier (barrier true); see shutdownStepOrder for the full classification.
type shutdownStep struct {
	name    string
	timeout time.Duration
	// barrier marks a QUIESCENCE BARRIER: a join of a background operation that
	// holds a shared dependency the LATER steps tear down (the runtime manager,
	// the state DB, or Redis). A best-effort cleanup step that times out is left
	// running while teardown continues; a barrier step that times out has its
	// context cancelled and is then joined for real before the registry
	// advances. Cancellation alone is insufficient: a non-cooperative reconcile
	// or service pass would keep using the dependency while it is closed. Only
	// steps that gate shared-dependency teardown set this.
	barrier bool
	run     func(context.Context) error
}

// kind labels the step in diagnostics: a quiescence barrier (strictly joined
// past its bound) versus a best-effort cleanup step (logged and skipped on
// timeout). It keeps the timeout log actionable without a second log line.
func (s shutdownStep) kind() string {
	if s.barrier {
		return "barrier"
	}
	return "cleanup"
}

// shutdownRegistry is the small ordered teardown the worker runs on every
// return path once resources exist. Steps are registered as each resource is
// successfully acquired/started, but run strictly by shutdownStepOrder, so
// ordering is explicit rather than defer/LIFO. It is not safe for concurrent
// registration (Run registers from its single startup goroutine).
type shutdownRegistry struct {
	steps []shutdownStep
	// budget overrides shutdownAggregateTimeout when > 0. It exists so tests can
	// exercise the aggregate cap deterministically without waiting minutes;
	// production leaves it zero.
	budget time.Duration
}

// barrierJoin is the per-step half of the registry's barrier contract. It runs
// join under stepCtx; when the bound expires (join returns its context error)
// it runs join AGAIN with context.Background, so the step does not return until
// the operation it guards is actually quiescent. The registry cancels the
// step's context at the deadline and then joins the step goroutine, so this is
// what makes that join wait for the real operation rather than an early
// timeout return — a later step may close the runtime manager or state DB only
// once the reconcile/service pass is done using it.
//
// The second join is deliberately unbounded: every joint operation is rooted in
// the worker lifecycle and bounded by its own per-operation timeout, so it
// terminates. Waiting longer is the point — safety over teardown overlap. The
// original (timeout) error is returned so the registry keeps its normal timeout
// logging.
//
// The second join runs only when the first failed because stepCtx expired: any
// other error means the join already reported completion (a non-context error is
// a genuine join outcome), so a later step may proceed. This mirrors Join's own
// contract — it returns nil once the operation has exited and its context error
// only while still waiting.
func barrierJoin(stepCtx context.Context, join func(context.Context) error) error {
	err := join(stepCtx)
	if err == nil || stepCtx.Err() == nil {
		return err
	}
	_ = join(context.Background())
	return err
}

// waitSignal waits for done under ctx, returning ctx.Err() if the bound fires
// first.
func waitSignal(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reconcilerBarrierStep is the reconciler shutdown barrier: it joins the
// reconciler's Start (reconcilerDone is closed once its pump/ticker/eventLoop
// have exited). The reconciler writes the state DB and drives the runtime
// manager, so this is a quiescence barrier ahead of manager/state teardown: the
// strict join waits for the real reconcile to exit even past the step bound.
func reconcilerBarrierStep(reconcilerDone <-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepReconciler,
		timeout: 10 * time.Second,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				return waitSignal(joinCtx, reconcilerDone)
			})
		},
	}
}

// loopsBarrierStep joins the worker-owned background loops (their done channels
// close on shutdown). The 5s stats loop writes the state DB, so this is a
// quiescence barrier ahead of the state close.
func loopsBarrierStep(loopDones []<-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepLoops,
		timeout: 5 * time.Second,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				for _, done := range loopDones {
					if err := waitSignal(joinCtx, done); err != nil {
						return err
					}
				}
				return nil
			})
		},
	}
}

// servicesJoinBarrierStep joins the service coordinator (join is
// ServiceCoordinator.Join; it drains queued/active passes and joins its
// workers). Its workers use the runtime manager and the state DB, so this is a
// quiescence barrier ahead of manager/state teardown.
func servicesJoinBarrierStep(join func(context.Context) error) shutdownStep {
	return shutdownStep{
		name:    shutdownStepServicesJoin,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, join)
		},
	}
}

// housekeepingBarrierStep joins the background startup housekeeping pass (done
// closes when its exclusive sweeps finish). The sweeps use the runtime manager
// and the state DB, so this is a quiescence barrier ahead of manager/state
// teardown.
func housekeepingBarrierStep(done <-chan struct{}) shutdownStep {
	return shutdownStep{
		name:    shutdownStepHousekeeping,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(joinCtx context.Context) error {
				return waitSignal(joinCtx, done)
			})
		},
	}
}

// schedulerBarrierStep is the scheduler shutdown barrier: it stops the gocron
// scheduler and then strictly joins every in-flight Relay publisher callback.
// A callback calls Publisher.PublishOccurrence, which touches Redis, and
// gocron's own bounded Shutdown can return (ErrStopJobsTimedOut) while a task
// goroutine — and therefore that callback — is still running. Redis is
// released in a later step, so this must be a quiescence barrier: the strict
// join (barrierJoin's unbounded second call) waits for Stop's real completion
// even past the step bound, guaranteeing no callback can touch Redis after
// this step returns. Stop itself is idempotent and joins the same shutdown on
// re-invocation, so the second call converges rather than restarting anything.
func schedulerBarrierStep(stop func(context.Context) error) shutdownStep {
	return shutdownStep{
		name:    shutdownStepScheduler,
		timeout: shutdownStepTimeout,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, stop)
		},
	}
}

// register adds a teardown step. Registering a name that is not in
// shutdownStepOrder would silently never run, so callers must use the
// shutdownStep* constants.
func (r *shutdownRegistry) register(step shutdownStep) {
	r.steps = append(r.steps, step)
}

// run executes every registered step in shutdownStepOrder, then logs the
// completion marker. Each step gets a fresh context bounded by min(its own
// timeout, the remaining aggregate budget), so one slow step cannot consume
// another's budget. Every step is invoked in its own goroutine and the registry
// selects on its result vs. the step deadline, so a step whose real operation
// ignores context cannot hang the shutdown: the timeout is observed, logged at
// Warn as "Shutdown: step timed out", and the remaining steps still run. A step
// failure is logged as "Shutdown: step failed" and likewise never stops the
// sequence. The result channel is buffered so a non-cooperative step that later
// returns can always send without leaking on a blocked send. A step that PANICS
// is recovered at this boundary and converted into a step failure (with the step
// name and duration), so a broken cleanup cannot crash the process and skip the
// remaining steps: ordered continuation is a core shutdown contract.
//
// The aggregate budget therefore bounds the BEST-EFFORT cleanup steps, not the
// whole teardown: every step is ATTEMPTED, the budget is never a reason to skip
// one, and a barrier (below) may still extend the shutdown past it.
//
// A barrier step (shutdownStep.barrier) is the exception to the
// move-on-after-timeout policy: when its deadline fires the registry cancels
// the step's context and then WAITS for the step's goroutine to actually return
// before advancing, so a later step can never tear down a shared dependency
// (manager, state DB, Redis) while a reconcile, sweep, loop, or publisher
// callback is still using it. The barrier's timeout is logged exactly as a
// cleanup timeout — with `kind=barrier` and the exhausted `bound` — so the
// diagnostics stay actionable and there is no duplicate log, then the strict
// join may extend the shutdown past that bound, deliberately trading teardown
// latency for safety.
//
// Every step logs exactly one timeout line, carrying `kind` (barrier vs
// cleanup) and `bound` (whether the step's own cap or the aggregate budget was
// what expired); a genuine error after a barrier timeout is the one additional
// line, and only when the joined operation does not merely report its context
// error.
func (r *shutdownRegistry) run(logger *slog.Logger) {
	steps := make(map[string]shutdownStep, len(r.steps))
	for _, step := range r.steps {
		steps[step.name] = step
	}
	started := time.Now()
	budgetDuration := shutdownAggregateTimeout
	if r.budget > 0 {
		budgetDuration = r.budget
	}
	budget := time.Now().Add(budgetDuration)
	for _, name := range shutdownStepOrder {
		step, ok := steps[name]
		if !ok {
			continue
		}
		// Every registered step is ATTEMPTED, even once the aggregate budget is
		// exhausted: the step is still invoked (with an already-expired context
		// if the budget is gone) so a cooperative cleanup gets its chance, and
		// the registry logs it as timed out rather than silently skipping it.
		// A barrier is never skipped by exhaustion either; it is still joined.
		deadline := budget
		bound := "aggregate"
		if step.timeout > 0 {
			if own := time.Now().Add(step.timeout); own.Before(deadline) {
				deadline = own
				bound = "step"
			}
		}
		stepCtx, cancel := context.WithDeadline(context.Background(), deadline)
		stepStart := time.Now()
		done := make(chan error, 1)
		go func() {
			// Recover at the step boundary: a panicking cleanup is converted to
			// an error (logged as a step failure with step/duration) so it can
			// never crash the process or abort the ordered sequence. The send is
			// safe on the buffered channel even if the registry already moved on
			// after a timeout.
			defer func() {
				if pv := recover(); pv != nil {
					done <- fmt.Errorf("panic: %v", pv)
				}
			}()
			done <- step.run(stepCtx)
		}()
		select {
		case err := <-done:
			// A cooperative step that returns exactly when its context fires is
			// reported as the timeout it is; a step that returns a context error
			// is likewise a timeout (its bound was reached), not a genuine
			// failure. Any other error is a real step failure.
			ctxErr := stepCtx.Err()
			cancel()
			switch {
			case err == nil:
			case ctxErr != nil && errors.Is(err, ctxErr):
				logger.Warn("Shutdown: step timed out",
					"step", step.name,
					"kind", step.kind(),
					"bound", bound,
					"error", err,
					"duration", time.Since(stepStart),
				)
			default:
				logger.Warn("Shutdown: step failed",
					"step", step.name,
					"kind", step.kind(),
					"error", err,
					"duration", time.Since(stepStart),
				)
			}
		case <-stepCtx.Done():
			// Cancel first so a cooperative barrier observes the cancellation
			// promptly, then (barriers only) join the step for real.
			cancel()
			logger.Warn("Shutdown: step timed out",
				"step", step.name,
				"kind", step.kind(),
				"bound", bound,
				"error", stepCtx.Err(),
				"duration", time.Since(stepStart),
			)
			if step.barrier {
				// Strict join: do NOT advance to a later step (manager/state/
				// Redis teardown) until the operation this barrier joins has
				// actually returned. This is where shutdown may exceed both the
				// step bound and the aggregate budget, by design.
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					logger.Warn("Shutdown: barrier step failed after timeout",
						"step", step.name,
						"error", err,
						"duration", time.Since(stepStart),
					)
				}
			}
		}
	}
	logger.Info("Shutdown complete", "duration", time.Since(started))
}

// shutdownServices builds the shutdown step that stops and removes THIS worker's
// persistent service containers during graceful shutdown. It is the
// snapshot-first, proof-second, remove-third sequence (see
// guardIdentitySnapshotCleanup): the step SNAPSHOTS the worker's service
// containers, re-proves the pinned ownership lock, and stops/removes exactly the
// captured containers — never re-listing after the proof. Because a replacement
// incarnation with the SAME WORKER_ID can retain/adopt the SAME physical id (the
// reconciler is label-derived and identity-scoped, not process-scoped), the
// protection is the fail-closed
// ownership proof, not id freshness: a latched loss or a failed/ambiguous proof
// removes nothing. The step's context (the registry's fresh bounded one) is
// handed to discovery and removal; the ownership proof uses its own background
// context (proof.prove), so it still detects a loss once the lifecycle is
// cancelled. A discovery error is returned so the shutdown registry surfaces it
// with the structured step name. The detailed per-operation logs live in the
// reconciler, so no duplicate logging happens here.
func shutdownServices(
	svcCtrl *reconciler.ServiceReconciler,
	hostname string,
	proof ownershipProof,
	logger *slog.Logger,
) func(context.Context) error {
	return guardIdentitySnapshotCleanup(proof, logger, shutdownStepServiceCleanup,
		func(stepCtx context.Context) ([]runtime.ServiceContainer, error) {
			return svcCtrl.DiscoverShutdownContainers(stepCtx, hostname)
		},
		func(stepCtx context.Context, own []runtime.ServiceContainer) error {
			return svcCtrl.RemoveShutdownContainers(stepCtx, own)
		},
	)
}

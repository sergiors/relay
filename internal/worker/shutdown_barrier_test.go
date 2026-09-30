package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestShutdownRegistryBarrierStrictlyJoinsBeforeAdvancing is the audited-race
// regression at the registry level: a barrier step whose join is an IN-PROGRESS
// reconcile blocked inside a controllable dependency call (the runtime manager /
// state DB). The step's bound expires, the registry cancels the step context,
// and the barrier OBSERVES that cancellation — but the dependency call keeps
// blocking until the test releases it. The registry must not advance to
// manager/state teardown once the barrier has been cancelled; it must wait for
// the dependency call to really return (the strict join), and only then run the
// later steps. The key sequence is driven by channel signals; the bounded
// selects are deadlock guards only, never sequencing waits, and there are no
// sleeps.
func TestShutdownRegistryBarrierStrictlyJoinsBeforeAdvancing(t *testing.T) {
	// dependencyEntered: the barrier's operation has entered the controllable
	// dependency call (the reconcile holding the runtime manager / state DB).
	dependencyEntered := make(chan struct{})
	// stepCancelled: the barrier observed its step context cancelled after the
	// bound fired, i.e. the registry has declared the step timed out.
	stepCancelled := make(chan struct{})
	// release: the test releases the controllable dependency call, letting the
	// in-progress reconcile finish.
	release := make(chan struct{})
	// managerStarted / stateStarted: the later shared-dependency teardown steps
	// actually began (they must never start before release).
	managerStarted := make(chan struct{})
	stateStarted := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	// dependencyCall models the controllable dependency the in-progress
	// reconcile is blocked in. It reports entry, observes (and reports) the step
	// context cancellation the registry issues at the bound, then stays blocked
	// until the TEST releases it — it never abandons the dependency early.
	dependencyCall := func(ctx context.Context) {
		once.Do(func() { close(dependencyEntered) })
		<-ctx.Done()
		close(stepCancelled)
		<-release
	}

	// A barrier that ignores its (expired) context past the observation point:
	// exactly the non-cooperative reconcile/service operation the strict join
	// exists for.
	barrier := shutdownStep{
		name:    shutdownStepReconciler,
		timeout: 30 * time.Millisecond,
		barrier: true,
		run: func(stepCtx context.Context) error {
			dependencyCall(stepCtx)
			record(shutdownStepReconciler)
			return nil
		},
	}
	manager := shutdownStep{
		name:    shutdownStepManager,
		timeout: time.Second,
		run: func(context.Context) error {
			close(managerStarted)
			record(shutdownStepManager)
			return nil
		},
	}
	state := shutdownStep{
		name:    shutdownStepState,
		timeout: time.Second,
		run: func(context.Context) error {
			close(stateStarted)
			record(shutdownStepState)
			return nil
		},
	}

	reg := &shutdownRegistry{}
	reg.register(barrier)
	reg.register(manager)
	reg.register(state)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	// Key step 1: the barrier entered its controllable dependency call.
	select {
	case <-dependencyEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("barrier never entered its controllable dependency call")
	}

	// Key step 2: the bound fired and the registry cancelled the step; the
	// barrier observed that cancellation. Channel-signalled, not a fixed wait.
	select {
	case <-stepCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("barrier never observed its step context cancellation")
	}

	// The barrier has been cancelled and is STILL blocked in its dependency
	// call, so manager/state teardown must not have begun. A buggy registry
	// would already have advanced; the bounded select below is a deadlock guard
	// giving it that chance to be caught, not a sequencing wait.
	select {
	case <-managerStarted:
		t.Fatal("manager teardown began after cancellation but before the dependency was released")
	case <-stateStarted:
		t.Fatal("state teardown began after cancellation but before the dependency was released")
	case <-done:
		t.Fatal("shutdown registry advanced past the barrier before it was joined")
	case <-time.After(150 * time.Millisecond):
	}

	// Key step 3: release the controllable dependency; only now may the strict
	// join complete and manager/state teardown begin.
	close(release)
	select {
	case <-managerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("manager teardown did not begin after the dependency was released")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry.run did not return after the barrier operation exited")
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{shutdownStepReconciler, shutdownStepManager, shutdownStepState}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

// TestShutdownRegistryBarrierTimeoutStillLogged pins that the strict join
// preserves the normal timeout diagnostic: the barrier's expired bound is still
// logged as "Shutdown: step timed out" with the structured step name, even
// though the registry then waits for the real join.
func TestShutdownRegistryBarrierTimeoutStillLogged(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	barrier := shutdownStep{
		name:    shutdownStepServicesJoin,
		timeout: 30 * time.Millisecond,
		barrier: true,
		run: func(context.Context) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		},
	}
	later := shutdownStep{
		name:    shutdownStepManager,
		timeout: time.Second,
		run:     func(context.Context) error { return nil },
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := &shutdownRegistry{}
	reg.register(barrier)
	reg.register(later)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(logger)
	}()
	<-entered
	select {
	case <-done:
		t.Fatal("barrier step was not strictly joined")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry.run did not return after the barrier operation exited")
	}

	out := logs.String()
	if !strings.Contains(out, "Shutdown: step timed out") {
		t.Errorf("barrier timeout was not logged:\n%s", out)
	}
	if !strings.Contains(out, "step="+shutdownStepServicesJoin) {
		t.Errorf("barrier timeout log missing the structured step name:\n%s", out)
	}
	if !strings.Contains(out, "Shutdown complete") {
		t.Errorf("missing completion marker:\n%s", out)
	}
}

// TestShutdownRegistryBarrierAggregateBudgetAlsoStrict proves the strict join
// applies when the AGGREGATE budget (not a per-step timeout) is what expires.
// The later step is still attempted once the barrier is joined, with its own
// fresh per-step bound so the exhausted aggregate does not skip it.
func TestShutdownRegistryBarrierAggregateBudgetAlsoStrict(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	managerStarted := make(chan struct{})
	var once sync.Once
	barrier := shutdownStep{
		name:    shutdownStepReconciler,
		timeout: 0, // only the aggregate bounds it
		barrier: true,
		run: func(context.Context) error {
			once.Do(func() { close(entered) })
			<-release
			return nil
		},
	}
	manager := shutdownStep{
		name:    shutdownStepManager,
		timeout: 2 * time.Second,
		run: func(context.Context) error {
			close(managerStarted)
			return nil
		},
	}

	reg := &shutdownRegistry{budget: 50 * time.Millisecond}
	reg.register(barrier)
	reg.register(manager)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	<-entered
	select {
	case <-managerStarted:
		t.Fatal("manager teardown began before the aggregate-capped barrier exited")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry.run did not return after the barrier operation exited")
	}
	select {
	case <-managerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("later step did not run after the aggregate-capped barrier was joined")
	}
}

// TestShutdownRegistryOrdinaryStepDoesNotStrictlyJoin proves the strict join is
// a barrier-only behavior: an ordinary (non-barrier) step whose bound expires is
// surfaced as timed out and the registry advances WITHOUT waiting for its real
// operation, preserving the existing best-effort cleanup policy. The blocked
// step is released afterward so its goroutine does not leak.
func TestShutdownRegistryOrdinaryStepDoesNotStrictlyJoin(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	recorded := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var order []string
	blocked := shutdownStep{
		name:    shutdownStepScheduler,
		timeout: 30 * time.Millisecond,
		run: func(context.Context) error {
			once.Do(func() { close(entered) })
			<-release
			mu.Lock()
			order = append(order, shutdownStepScheduler)
			mu.Unlock()
			close(recorded)
			return nil
		},
	}
	later := shutdownStep{
		name:    shutdownStepState,
		timeout: time.Second,
		run: func(context.Context) error {
			mu.Lock()
			order = append(order, shutdownStepState)
			mu.Unlock()
			return nil
		},
	}

	reg := &shutdownRegistry{}
	reg.register(blocked)
	reg.register(later)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	<-entered

	// The ordinary step's bound expired; the registry must advance to the later
	// step without joining the blocked operation.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry did not advance past an ordinary timed-out step")
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 1 || got[0] != shutdownStepState {
		t.Fatalf("order = %v, want only [%s] (ordinary step must not be joined before advancing)",
			got, shutdownStepState)
	}

	close(release)
	select {
	case <-recorded:
	case <-time.After(2 * time.Second):
		t.Fatal("released ordinary step did not record itself")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[1] != shutdownStepScheduler {
		t.Fatalf("released ordinary step did not record itself: %v", order)
	}
}

// TestBarrierJoinWaitsForRealOperation proves the per-step half of the barrier
// contract: barrierJoin must not return while the join's real operation is still
// in flight, even after the step context expired. The first call returns
// ctx.Err() (its bound fired) while the real operation is still blocked; the
// unbounded second call blocks until the operation concludes. The helper returns
// the original timeout error so the registry keeps its timeout log.
func TestBarrierJoinWaitsForRealOperation(t *testing.T) {
	opDone := make(chan struct{})
	stepCtx, cancel := context.WithCancel(context.Background())
	cancel() // the step's bound has already expired

	returned := make(chan error, 1)
	go func() {
		returned <- barrierJoin(stepCtx, func(joinCtx context.Context) error {
			select {
			case <-opDone:
				return nil
			case <-joinCtx.Done():
				return joinCtx.Err()
			}
		})
	}()

	select {
	case <-returned:
		t.Fatal("barrierJoin returned while the real operation was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(opDone)
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("barrierJoin err = %v, want the original context.Canceled timeout error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("barrierJoin did not return after the real operation concluded")
	}
}

// TestBarrierJoinReturnsNilWhenJoinSucceeds proves the happy path is unchanged:
// a join that succeeds under its step context returns nil with a single call.
func TestBarrierJoinReturnsNilWhenJoinSucceeds(t *testing.T) {
	calls := 0
	err := barrierJoin(context.Background(), func(context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("barrierJoin = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("join called %d times, want 1 on success", calls)
	}
}

// TestBarrierStepsStrictlyJoinViaTheirRealConstructors proves the production
// step constructors are wired to the strict join: with an already-cancelled step
// context (a bound that has expired), each constructor's run must still block
// until its real join returns, then report the timeout error. Driving run
// directly avoids waiting out the production bounds.
func TestBarrierStepsStrictlyJoinViaTheirRealConstructors(t *testing.T) {
	t.Run("reconciler", func(t *testing.T) {
		reconcilerDone := make(chan struct{})
		step := reconcilerBarrierStep(reconcilerDone)
		assertBarrierRunBlocksPastCancelledContext(t, step, func() { close(reconcilerDone) })
	})
	t.Run("loops", func(t *testing.T) {
		loopDone := make(chan struct{})
		step := loopsBarrierStep([]<-chan struct{}{loopDone})
		assertBarrierRunBlocksPastCancelledContext(t, step, func() { close(loopDone) })
	})
	t.Run("services-join", func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		entered := make(chan struct{})
		step := servicesJoinBarrierStep(func(ctx context.Context) error {
			once.Do(func() { close(entered) })
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		assertBarrierRunBlocksPastCancelledContext(t, step, func() {
			// Release the join's real operation; the second (unbounded) call
			// observes the closed release.
			close(release)
		}, entered)
	})
	t.Run("housekeeping", func(t *testing.T) {
		done := make(chan struct{})
		step := housekeepingBarrierStep(done)
		assertBarrierRunBlocksPastCancelledContext(t, step, func() { close(done) })
	})
	t.Run("scheduler", func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		entered := make(chan struct{})
		// Model the real Scheduler.Stop: the bounded first call honors the
		// (already expired) step context and returns it, then the unbounded
		// second call waits for the in-flight callback to actually finish.
		step := schedulerBarrierStep(func(ctx context.Context) error {
			once.Do(func() { close(entered) })
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		assertBarrierRunBlocksPastCancelledContext(t, step, func() {
			close(release)
		}, entered)
	})
}

// assertBarrierRunBlocksPastCancelledContext drives step.run with an already
// cancelled context and asserts it does not return until release is called, then
// returns the step's timeout error.
func assertBarrierRunBlocksPastCancelledContext(t *testing.T, step shutdownStep, release func(), signals ...<-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	returned := make(chan error, 1)
	go func() { returned <- step.run(ctx) }()

	// The join may need a beat to enter on the unbounded second call; wait for
	// any entry signal the caller supplied.
	for _, entered := range signals {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("barrier join never entered")
		}
	}

	// Its bound is already expired, but the real operation is still in flight:
	// run must NOT return.
	select {
	case err := <-returned:
		t.Fatalf("%s run returned %v before its operation exited", step.name, err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	select {
	case err := <-returned:
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s run err = %v, want context.Canceled (its expired bound)", step.name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s run did not return after its operation exited", step.name)
	}
}

// TestBarrierStepConstructorsMarkBarriers pins that the shared-dependency
// steps are registered as barriers with their documented names, so a future edit
// cannot silently demote one and reopen the shutdown race. The scheduler is
// included: its Relay publisher callbacks touch Redis, which a later step
// closes, so it must be a barrier too.
func TestBarrierStepConstructorsMarkBarriers(t *testing.T) {
	reconcilerDone := make(chan struct{})
	close(reconcilerDone)
	loopDone := make(chan struct{})
	close(loopDone)

	cases := []struct {
		name string
		step shutdownStep
	}{
		{shutdownStepReconciler, reconcilerBarrierStep(reconcilerDone)},
		{shutdownStepLoops, loopsBarrierStep([]<-chan struct{}{loopDone})},
		{shutdownStepServicesJoin, servicesJoinBarrierStep(func(context.Context) error { return nil })},
		{shutdownStepHousekeeping, housekeepingBarrierStep(loopDone)},
		{shutdownStepScheduler, schedulerBarrierStep(func(context.Context) error { return nil })},
	}
	for _, tc := range cases {
		if tc.step.name != tc.name {
			t.Errorf("step name = %q, want %q", tc.step.name, tc.name)
		}
		if !tc.step.barrier {
			t.Errorf("%s step is not marked barrier; manager/state teardown could overlap it", tc.name)
		}
	}
}

// TestSchedulerBarrierStepHoldsRedisTeardownUntilCallbackJoins proves the
// scheduler barrier gates the Redis close: the scheduler's stop is wedged in a
// Relay publisher callback (the controllable Redis-facing seam) that ignores the
// step bound. The registry must NOT advance to the redis step — whose teardown
// stands in for client.Close — until the callback is released and the strict
// join completes, even though the step's own bound expired long before.
//
// The barrier replica here carries the scheduler step's identity and barrier
// flag with a short bound so the ordering is observable quickly; the production
// schedulerBarrierStep constructor is pinned to the same shape and strict-join
// behavior by TestBarrierStepsStrictlyJoinViaTheirRealConstructors.
func TestSchedulerBarrierStepHoldsRedisTeardownUntilCallbackJoins(t *testing.T) {
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	stepCancelled := make(chan struct{})
	redisClosed := make(chan struct{})
	var once sync.Once

	// stop models Scheduler.Stop blocked in its strict join of an in-flight
	// Relay publisher callback: it observes the step's expired bound (so the
	// registry logs the timeout) but keeps waiting for the real callback.
	stop := func(stepCtx context.Context) error {
		once.Do(func() { close(callbackEntered) })
		<-stepCtx.Done()
		close(stepCancelled)
		<-callbackRelease
		return nil
	}
	redisStep := shutdownStep{
		name:    shutdownStepRedis,
		timeout: time.Second,
		run: func(context.Context) error {
			close(redisClosed)
			return nil
		},
	}
	schedulerStep := shutdownStep{
		name:    shutdownStepScheduler,
		timeout: 50 * time.Millisecond,
		barrier: true,
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, stop)
		},
	}

	reg := &shutdownRegistry{}
	reg.register(redisStep)
	reg.register(schedulerStep)

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	select {
	case <-callbackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler stop never entered its callback join")
	}
	select {
	case <-stepCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler step never observed its expired bound")
	}

	// The scheduler step is cancelled and its callback is still in flight:
	// Redis must not close, and the registry must not have advanced.
	select {
	case <-redisClosed:
		t.Fatal("Redis closed after the scheduler bound expired but before the callback returned")
	case <-done:
		t.Fatal("shutdown advanced past the scheduler barrier before the callback returned")
	case <-time.After(150 * time.Millisecond):
	}

	close(callbackRelease)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete after the callback was released")
	}
	select {
	case <-redisClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("Redis did not close after the scheduler barrier was joined")
	}
}

// TestSchedulerBarrierStepStrictUnderAggregateExpiry proves the scheduler
// barrier's strict join also applies when the AGGREGATE budget (not a per-step
// timeout) expires: Redis still cannot close until the in-flight callback
// returns.
func TestSchedulerBarrierStepStrictUnderAggregateExpiry(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	redisClosed := make(chan struct{})
	var once sync.Once

	reg := &shutdownRegistry{budget: 50 * time.Millisecond}
	reg.register(shutdownStep{
		name:    shutdownStepRedis,
		timeout: 2 * time.Second,
		run: func(context.Context) error {
			close(redisClosed)
			return nil
		},
	})
	reg.register(shutdownStep{
		name:    shutdownStepScheduler,
		barrier: true,
		// No per-step cap: only the aggregate bounds it.
		run: func(stepCtx context.Context) error {
			return barrierJoin(stepCtx, func(context.Context) error {
				once.Do(func() { close(entered) })
				<-release
				return nil
			})
		},
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler stop never entered")
	}
	select {
	case <-redisClosed:
		t.Fatal("Redis closed before the aggregate-capped scheduler callback was joined")
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete after the callback was released")
	}
	select {
	case <-redisClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("Redis did not close after the aggregate-capped barrier was joined")
	}
}

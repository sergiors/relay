package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// registeredStep builds a no-op shutdownStep for the given name and timeout
// whose run appends name to order, enabling the registry's ordering and
// per-step bound to be observed without Docker, Redis, or a state DB.
func registeredStep(name string, timeout time.Duration, order *[]string) shutdownStep {
	return shutdownStep{
		name:    name,
		timeout: timeout,
		run: func(context.Context) error {
			*order = append(*order, name)
			return nil
		},
	}
}

// TestShutdownRegistryRunsInExplicitOrderAndNeverStopsOnError pins the single
// teardown contract the refactor introduced: steps run in the documented
// shutdownStepOrder regardless of REGISTRATION order (Redis is registered first
// yet released last), and a failing step is logged by name without aborting the
// rest — the same non-fatal policy the original inline shutdown tail had. It
// uses pure seams, so it needs no Docker, Redis, or state DB.
func TestShutdownRegistryRunsInExplicitOrderAndNeverStopsOnError(t *testing.T) {
	var order []string
	failing := func(name string, msg string) shutdownStep {
		return shutdownStep{
			name:    name,
			timeout: time.Second,
			run: func(context.Context) error {
				order = append(order, name)
				return errors.New(msg)
			},
		}
	}
	simple := func(name string) shutdownStep { return registeredStep(name, time.Second, &order) }

	// Register in a deliberately scrambled order, with Redis first (as Run does
	// when the client is acquired first). The explicit order must win.
	reg := &shutdownRegistry{}
	reg.register(simple(shutdownStepRedis))
	reg.register(failing(shutdownStepManager, "manager boom"))
	reg.register(simple(shutdownStepSocket))
	reg.register(failing(shutdownStepScheduler, "scheduler boom"))
	reg.register(simple(shutdownStepHousekeeping))
	reg.register(failing(shutdownStepServicesJoin, "join boom"))
	reg.register(simple(shutdownStepServiceCleanup))
	reg.register(simple(shutdownStepStatsFlush))
	reg.register(simple(shutdownStepMetrics))
	reg.register(simple(shutdownStepWebhook))
	reg.register(simple(shutdownStepState))
	reg.register(simple(shutdownStepTracing))

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg.run(logger)

	want := []string{
		shutdownStepSocket, shutdownStepScheduler, shutdownStepHousekeeping,
		shutdownStepServicesJoin, shutdownStepServiceCleanup, shutdownStepStatsFlush,
		shutdownStepMetrics, shutdownStepWebhook, shutdownStepManager,
		shutdownStepState, shutdownStepTracing, shutdownStepRedis,
	}
	if len(order) != len(want) {
		t.Fatalf("ran %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("step %d = %q, want %q (full order %v)", i, order[i], want[i], order)
		}
	}

	// Each step failure was surfaced with its structured step name, and the
	// teardown still reached its final marker.
	for _, wantStep := range []string{shutdownStepScheduler, shutdownStepServicesJoin, shutdownStepManager} {
		if !strings.Contains(logs.String(), "step="+wantStep) {
			t.Errorf("log missing structured step=%s:\n%s", wantStep, logs.String())
		}
	}
	for _, wantMsg := range []string{"scheduler boom", "join boom", "manager boom", "Shutdown complete"} {
		if !strings.Contains(logs.String(), wantMsg) {
			t.Errorf("log missing %q:\n%s", wantMsg, logs.String())
		}
	}
}

// TestShutdownRegistryFreshTimeoutPerStep proves each step gets its own fresh
// context.Background bound: a long earlier step cannot consume a later step's
// budget (a fresh bound is created immediately before each step, not once
// shared), the configured timeout is honored, and the context is canceled
// immediately after the step runs. Timeout isolation is the property that keeps
// a wedged Docker/Redis call from starving the rest of the teardown.
func TestShutdownRegistryFreshTimeoutPerStep(t *testing.T) {
	const slowBound = 400 * time.Millisecond
	const laterBound = 1500 * time.Millisecond

	type observation struct {
		startedAt time.Time
		deadline  time.Time
		hasBound  bool
		ctx       context.Context
	}
	seen := map[string]observation{}
	record := func(name string, timeout time.Duration, block time.Duration) shutdownStep {
		return shutdownStep{
			name:    name,
			timeout: timeout,
			run: func(ctx context.Context) error {
				obs := observation{startedAt: time.Now(), ctx: ctx}
				if deadline, ok := ctx.Deadline(); ok {
					obs.hasBound = true
					obs.deadline = deadline
				}
				seen[name] = obs
				// Block for part of the (short) bound so the earlier step holds
				// the sequence for a measurable interval.
				time.Sleep(block)
				return nil
			},
		}
	}

	reg := &shutdownRegistry{}
	reg.register(record(shutdownStepScheduler, slowBound, slowBound/2))
	reg.register(record(shutdownStepState, laterBound, 0))

	reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))

	sched, ok := seen[shutdownStepScheduler]
	if !ok || !sched.hasBound {
		t.Fatalf("scheduler step missing its deadline: %+v", sched)
	}
	state, ok := seen[shutdownStepState]
	if !ok || !state.hasBound {
		t.Fatalf("state step missing its deadline: %+v", state)
	}

	// Each step's deadline is its OWN timeout measured from the instant THAT
	// step started, not from registry.run's start. A context created once for the
	// whole sequence would give the later step a deadline short by the earlier
	// step's consumed budget (the scheduler blocks for slowBound/2 here), which
	// lands far outside this slop.
	const slop = 50 * time.Millisecond
	assertOwnBound := func(name string, obs observation, bound time.Duration) {
		want := obs.startedAt.Add(bound)
		delta := obs.deadline.Sub(want)
		if delta < -slop || delta > slop {
			t.Errorf("%s deadline = %v, want %v (its own %v bound from its own start; delta %v)",
				name, obs.deadline, want, bound, delta)
		}
	}
	assertOwnBound(shutdownStepScheduler, sched, slowBound)
	assertOwnBound(shutdownStepState, state, laterBound)

	// The context is canceled immediately after the step executes, so a leaked
	// step cannot keep a resource alive past its turn.
	for name, obs := range seen {
		if !errors.Is(obs.ctx.Err(), context.Canceled) {
			t.Errorf("%s ctx.Err() = %v, want context.Canceled (canceled after run)", name, obs.ctx.Err())
		}
	}
}

// TestShutdownRegistryZeroTimeoutStillBounded pins the correction that a step
// declaring no timeout (timeout <= 0) is bounded by the AGGREGATE budget, never
// unbounded: the registry derives a fresh context bounded by min(the step's own
// cap, the budget remaining). A positive timeout yields its own deadline, and
// every bounded step's fresh context is canceled immediately after it executes.
func TestShutdownRegistryZeroTimeoutStillBounded(t *testing.T) {
	type observation struct {
		deadline time.Time
		hasBound bool
		ctx      context.Context
	}
	seen := map[string]*observation{}
	record := func(name string, timeout time.Duration) shutdownStep {
		return shutdownStep{
			name:    name,
			timeout: timeout,
			run: func(ctx context.Context) error {
				obs := &observation{ctx: ctx}
				obs.deadline, obs.hasBound = ctx.Deadline()
				seen[name] = obs
				return nil
			},
		}
	}

	reg := &shutdownRegistry{}
	reg.register(record(shutdownStepSocket, 0))              // no per-step cap
	reg.register(record(shutdownStepManager, 0))             // no per-step cap
	reg.register(record(shutdownStepState, 0))               // no per-step cap
	reg.register(record(shutdownStepRedis, 0))               // no per-step cap
	reg.register(record(shutdownStepScheduler, time.Second)) // explicit 1s cap

	before := time.Now()
	reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, name := range []string{
		shutdownStepSocket, shutdownStepManager, shutdownStepState, shutdownStepRedis,
	} {
		obs, ok := seen[name]
		if !ok {
			t.Fatalf("zero-timeout step %q did not run", name)
		}
		if !obs.hasBound {
			t.Errorf("%s has no deadline; want the aggregate bound", name)
		}
		// Bounded by the aggregate, measured from registry.run's start. A small
		// slop covers the instant between `before` and the registry stamping its
		// budget.
		if got := obs.deadline.Sub(before); got <= 0 || got > shutdownAggregateTimeout+50*time.Millisecond {
			t.Errorf("%s deadline is %v from run start; want within the aggregate budget %v",
				name, got, shutdownAggregateTimeout)
		}
		if !errors.Is(obs.ctx.Err(), context.Canceled) {
			t.Errorf("%s ctx.Err() = %v, want context.Canceled (canceled after run)", name, obs.ctx.Err())
		}
	}

	bounded, ok := seen[shutdownStepScheduler]
	if !ok || !bounded.hasBound {
		t.Fatalf("bounded step scheduler missing its deadline: %+v", bounded)
	}
	// The scheduler carries its own 1s cap, which is far tighter than the
	// aggregate, so its deadline is ~1s from its own start.
	if got := time.Until(bounded.deadline); got > time.Second {
		t.Errorf("scheduler deadline is %v away; want its own 1s cap", got)
	}
	if !errors.Is(bounded.ctx.Err(), context.Canceled) {
		t.Errorf("bounded step ctx.Err() = %v, want context.Canceled (canceled after run)", bounded.ctx.Err())
	}
}

// TestShutdownRegistryPartialRegistration verifies only registered steps run and
// an unregistered one is silently skipped: optional resources register only
// when they actually started, so a step that never existed must not panic or
// emit a spurious "failed" log.
func TestShutdownRegistryPartialRegistration(t *testing.T) {
	var order []string
	reg := &shutdownRegistry{}
	reg.register(registeredStep(shutdownStepSocket, time.Second, &order))
	reg.register(registeredStep(shutdownStepState, time.Second, &order))

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg.run(logger)

	want := []string{shutdownStepSocket, shutdownStepState}
	if len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("ran %v, want %v (unregistered steps skipped)", order, want)
	}
	if strings.Contains(logs.String(), "step failed") {
		t.Errorf("unexpected failure log on a partial registry:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "Shutdown complete") {
		t.Errorf("missing completion marker:\n%s", logs.String())
	}
}

// TestShutdownRegistryOrderingInvariants pins the load-bearing sub-orderings the
// task calls out, independent of the full order test: the coordinator join
// precedes the service cleanup, which precedes the manager close (the manager
// must own live container state while containers are stopped); the reconciler
// and loops joins precede the state close (no reconcile or loop can touch the DB
// after it closes); the stats flush precedes the state close (the final snapshot
// lands before the DB closes); and Redis is released last, after every other
// resource.
func TestShutdownRegistryOrderingInvariants(t *testing.T) {
	var order []string
	reg := &shutdownRegistry{}
	for _, name := range []string{
		shutdownStepRedis, shutdownStepState, shutdownStepStatsFlush,
		shutdownStepManager, shutdownStepServiceCleanup, shutdownStepServicesJoin,
		shutdownStepReconciler, shutdownStepLoops, shutdownStepTracing,
	} {
		reg.register(registeredStep(name, time.Second, &order))
	}
	reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))

	index := func(name string) int {
		for i, got := range order {
			if got == name {
				return i
			}
		}
		t.Fatalf("step %q did not run: %v", name, order)
		return -1
	}
	if index(shutdownStepServicesJoin) >= index(shutdownStepServiceCleanup) {
		t.Errorf("services-join (%d) must precede service-cleanup (%d): %v",
			index(shutdownStepServicesJoin), index(shutdownStepServiceCleanup), order)
	}
	if index(shutdownStepServiceCleanup) >= index(shutdownStepManager) {
		t.Errorf("service-cleanup (%d) must precede manager (%d): %v",
			index(shutdownStepServiceCleanup), index(shutdownStepManager), order)
	}
	if index(shutdownStepStatsFlush) >= index(shutdownStepState) {
		t.Errorf("stats-flush (%d) must precede state (%d): %v",
			index(shutdownStepStatsFlush), index(shutdownStepState), order)
	}
	// The reconciler and the worker-owned loops are joined before the state DB
	// closes, so neither can touch it after close.
	if index(shutdownStepReconciler) >= index(shutdownStepState) {
		t.Errorf("reconciler (%d) must precede state (%d): %v",
			index(shutdownStepReconciler), index(shutdownStepState), order)
	}
	if index(shutdownStepLoops) >= index(shutdownStepState) {
		t.Errorf("loops (%d) must precede state (%d): %v",
			index(shutdownStepLoops), index(shutdownStepState), order)
	}
	// Tracing flushes after every span-producing resource has stopped (state,
	// manager, servers) and immediately before Redis is released last.
	if index(shutdownStepTracing) <= index(shutdownStepState) {
		t.Errorf("tracing (%d) must follow state (%d): %v",
			index(shutdownStepTracing), index(shutdownStepState), order)
	}
	if last := order[len(order)-1]; last != shutdownStepRedis {
		t.Errorf("last step = %q, want %q (Redis released last): %v", last, shutdownStepRedis, order)
	}
}

// TestShutdownRegistryTimedOutStepContinues proves the registry's central
// safety property for best-effort cleanup: a step whose real operation ignores
// its context (blocks past its bound) is surfaced as "Shutdown: step timed out"
// and does NOT stop the sequence — later steps still run. Its context is
// cancelled at the bound so a cooperative cleanup can observe the deadline, but
// the registry does not join it. The blocking step's goroutine is released after
// the assertion, and its buffered result channel is never leaked.
func TestShutdownRegistryTimedOutStepContinues(t *testing.T) {
	release := make(chan struct{})
	blockedEntered := make(chan struct{})
	blockedCancelled := make(chan struct{})
	blockedReturned := make(chan struct{})
	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	blocker := shutdownStep{
		name:    shutdownStepServiceCleanup,
		timeout: 40 * time.Millisecond,
		run: func(ctx context.Context) error {
			record(shutdownStepServiceCleanup)
			close(blockedEntered)
			<-ctx.Done()
			close(blockedCancelled)
			<-release // ignores the rest of the deadline
			close(blockedReturned)
			return nil
		},
	}
	later := shutdownStep{
		name:    shutdownStepState,
		timeout: time.Second,
		run: func(context.Context) error {
			record(shutdownStepState)
			return nil
		},
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := &shutdownRegistry{}
	reg.register(blocker)
	reg.register(later)

	start := time.Now()
	reg.run(logger)
	elapsed := time.Since(start)
	close(release)

	if elapsed > 2*time.Second {
		t.Fatalf("registry hung on a non-cooperative step: %v", elapsed)
	}
	select {
	case <-blockedEntered:
	default:
		t.Fatal("blocking step never entered")
	}
	// The timeout cancelled the cleanup step's context, but the registry
	// advanced without joining it.
	select {
	case <-blockedCancelled:
	default:
		t.Fatal("timed-out cleanup step's context was not cancelled")
	}
	// The later step ran despite the timeout.
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != shutdownStepServiceCleanup || got[1] != shutdownStepState {
		t.Fatalf("order = %v, want [%s %s] (later step must run after a timeout)",
			got, shutdownStepServiceCleanup, shutdownStepState)
	}
	out := logs.String()
	if !strings.Contains(out, "Shutdown: step timed out") || !strings.Contains(out, "step="+shutdownStepServiceCleanup) {
		t.Errorf("missing structured timeout log:\n%s", out)
	}
	if !strings.Contains(out, "kind=cleanup") || !strings.Contains(out, "bound=step") {
		t.Errorf("cleanup timeout log missing kind/bound tags:\n%s", out)
	}
	if !strings.Contains(out, "Shutdown complete") {
		t.Errorf("missing completion marker after a timeout:\n%s", out)
	}
	// Join the timed-out goroutine so the test does not race a late write. Its
	// buffered result channel means it never blocks on a send.
	<-blockedReturned
}

// TestShutdownRegistryAggregateBudgetCapsTotal proves the aggregate budget
// bounds BEST-EFFORT cleanup work: a cleanup step that ignores its context and
// blocks past the aggregate is cut off (kind=cleanup, bound=aggregate), the
// registry returns at (approximately) the aggregate, and later steps are still
// attempted (with the expired budget) rather than the teardown running forever.
func TestShutdownRegistryAggregateBudgetCapsTotal(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	schedulerEntered := make(chan struct{})
	var schedulerOnce sync.Once
	blockForever := shutdownStep{
		name: shutdownStepSocket,
		// No per-step cap: only the aggregate bounds it.
		run: func(context.Context) error {
			<-release
			return nil
		},
	}
	later := shutdownStep{
		name: shutdownStepScheduler,
		run: func(context.Context) error {
			schedulerOnce.Do(func() { close(schedulerEntered) })
			return nil
		},
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := &shutdownRegistry{budget: 120 * time.Millisecond}
	reg.register(blockForever)
	reg.register(later)

	start := time.Now()
	reg.run(logger)
	elapsed := time.Since(start)

	if elapsed > 1500*time.Millisecond {
		t.Fatalf("aggregate budget did not cap total shutdown: took %v", elapsed)
	}
	// The later step is attempted even though the budget is exhausted; its
	// goroutine may still be starting when run returns, so wait for its signal.
	select {
	case <-schedulerEntered:
	case <-time.After(time.Second):
		t.Fatal("a later step was never attempted after the aggregate budget was exhausted")
	}
	out := logs.String()
	if !strings.Contains(out, "Shutdown: step timed out") {
		t.Errorf("missing timeout log for the aggregate-capped step:\n%s", out)
	}
	if !strings.Contains(out, "kind=cleanup") {
		t.Errorf("aggregate-capped cleanup step not tagged kind=cleanup:\n%s", out)
	}
	if !strings.Contains(out, "bound=aggregate") {
		t.Errorf("aggregate-capped cleanup step not tagged bound=aggregate:\n%s", out)
	}
}

// TestShutdownRegistryPanickingStepContinues proves ordered continuation is a
// core contract even for a panicking step: the registry recovers the panic at
// the step boundary, logs it as a structured step failure with the step name and
// duration, and still runs every later step. Without the recovery the panic would
// crash the process (or, absent the goroutine, abort the sequence) and skip the
// remaining teardown.
func TestShutdownRegistryPanickingStepContinues(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	panicker := shutdownStep{
		name:    shutdownStepScheduler,
		timeout: time.Second,
		run: func(context.Context) error {
			record(shutdownStepScheduler)
			panic("boom")
		},
	}
	later := shutdownStep{
		name:    shutdownStepState,
		timeout: time.Second,
		run: func(context.Context) error {
			record(shutdownStepState)
			return nil
		},
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := &shutdownRegistry{}
	reg.register(panicker)
	reg.register(later)

	reg.run(logger)

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != shutdownStepScheduler || got[1] != shutdownStepState {
		t.Fatalf("order = %v, want [%s %s] (a later step must run after a panic)",
			got, shutdownStepScheduler, shutdownStepState)
	}
	out := logs.String()
	if !strings.Contains(out, "Shutdown: step failed") || !strings.Contains(out, "step="+shutdownStepScheduler) {
		t.Errorf("missing structured failure log for the panicking step:\n%s", out)
	}
	if !strings.Contains(out, "boom") {
		t.Errorf("failure log does not surface the panic value:\n%s", out)
	}
	if !strings.Contains(out, "duration=") {
		t.Errorf("failure log is missing the step duration:\n%s", out)
	}
	if !strings.Contains(out, "Shutdown complete") {
		t.Errorf("missing completion marker after a panic:\n%s", out)
	}
}

// TestRunInvalidRedisDSNReturnsError proves the worker's pre-resource failure is
// RETURNED, not os.Exit'd: config.Load succeeds once the required env vars are
// set, the DSN parse fails, and Run returns an error naming it. Run returns
// before any resource (Redis client, manager, servers) is created, so the test
// needs no Docker or Redis.
func TestRunInvalidRedisDSNReturnsError(t *testing.T) {
	t.Setenv("REDIS_URI", "redis://%zz")
	t.Setenv("REDIS_STREAM", "test-stream")
	t.Setenv("REDIS_GROUP", "test-group")

	err := Run(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("Run returned nil for an invalid REDIS_URI; want an error")
	}
	if !strings.Contains(err.Error(), "redis config invalid") {
		t.Fatalf("error = %q, want it to name the invalid redis config", err)
	}
}

// preflightSpy records the exact order in which the preflight steps ran, so the
// fixed Redis -> manager -> NETWORKS -> maintenance order and the
// stop-at-first-failure boundary are provable without Redis or Docker. Each
// step's error is scripted.
type preflightSpy struct {
	order []string
	group error
	open  error
	net   error
	// opens records how many times OpenManager was invoked; netManager records
	// the manager instance the NETWORKS step received (nil when not reached);
	// maintManager records the manager the maintenance step received (nil when
	// not reached).
	opens        int
	netManager   *runtime.Manager
	maintManager *runtime.Manager
}

// deps builds the preflightDeps the production Run wires, with each step
// recording itself. A non-nil manager is returned so a reached NETWORKS step
// receives a non-nil manager (the production contract); tests that need to
// observe the manager instance can do so via netManager/maintManager.
func (s *preflightSpy) deps() preflightDeps {
	manager := &runtime.Manager{}
	return preflightDeps{
		EnsureGroup: func(context.Context) error {
			s.order = append(s.order, "group")
			return s.group
		},
		OpenManager: func(context.Context) (*runtime.Manager, error) {
			s.order = append(s.order, "manager")
			s.opens++
			if s.open != nil {
				return nil, s.open
			}
			return manager, nil
		},
		VerifyNetworks: func(_ context.Context, m *runtime.Manager) error {
			s.order = append(s.order, "networks")
			s.netManager = m
			return s.net
		},
		StartMaintenance: func(m *runtime.Manager) {
			s.order = append(s.order, "maintenance")
			s.maintManager = m
		},
	}
}

// TestExternalPreflightOrder proves the external-dependency preflight runs its
// four steps in exactly the documented order — Redis stream/group readiness,
// then Docker runtime-manager readiness, then NETWORKS verification, then the
// manager's deferred maintenance loop — on the success path, and yields the
// manager the later phases use. A "load" sentinel appended only after the call
// succeeds stands in for function loading/fingerprinting, proving the whole
// preflight completes (including starting the manager loop LAST) before any
// later phase starts.
func TestExternalPreflightOrder(t *testing.T) {
	spy := &preflightSpy{}
	manager, err := runExternalPreflight(context.Background(), discardLogger(), spy.deps())
	if err != nil {
		t.Fatalf("runExternalPreflight = %v, want nil", err)
	}
	if manager == nil {
		t.Fatal("runExternalPreflight returned a nil manager on success")
	}
	// Function loading/preparation begins only once the preflight succeeds.
	spy.order = append(spy.order, "load")
	want := []string{"group", "manager", "networks", "maintenance", "load"}
	if len(spy.order) != len(want) {
		t.Fatalf("preflight order = %v, want %v", spy.order, want)
	}
	for i := range want {
		if spy.order[i] != want[i] {
			t.Fatalf("preflight step %d = %q, want %q (full order %v)", i, spy.order[i], want[i], spy.order)
		}
	}
	if spy.netManager != manager {
		t.Fatalf("NETWORKS step received manager %p, want the opened %p", spy.netManager, manager)
	}
	if spy.maintManager != manager {
		t.Fatalf("maintenance step received manager %p, want the opened %p", spy.maintManager, manager)
	}
}

// TestExternalPreflightShortCircuitsOnFailure proves each preflight failure
// stops every later step: a Redis/group failure never opens the manager, a
// manager failure never verifies networks, and a network failure never starts
// the manager's maintenance loop. Each failure is returned (not merely logged).
// This is the boundary that keeps function loading, every later phase, and any
// manager background loop from running against a broken external dependency.
func TestExternalPreflightShortCircuitsOnFailure(t *testing.T) {
	cases := []struct {
		name       string
		spy        *preflightSpy
		wantSteps  []string
		wantErrSub string
	}{
		{
			name:       "redis group failure stops at group",
			spy:        &preflightSpy{group: errors.New("dial tcp 127.0.0.1:6379: connection refused")},
			wantSteps:  []string{"group"},
			wantErrSub: "ensure consumer group failed",
		},
		{
			name:       "manager failure stops at manager",
			spy:        &preflightSpy{open: errors.New("cannot connect to Docker daemon")},
			wantSteps:  []string{"group", "manager"},
			wantErrSub: "runtime: new manager failed",
		},
		{
			name:       "network failure stops at networks",
			spy:        &preflightSpy{net: errors.New("verify NETWORKS: daemon exploded")},
			wantSteps:  []string{"group", "manager", "networks"},
			wantErrSub: "verify NETWORKS",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manager, err := runExternalPreflight(context.Background(), discardLogger(), tc.spy.deps())
			if err == nil {
				t.Fatal("runExternalPreflight = nil, want an error")
			}
			if manager != nil {
				t.Fatalf("runExternalPreflight returned manager %p on failure, want nil", manager)
			}
			if len(tc.spy.order) != len(tc.wantSteps) {
				t.Fatalf("preflight steps = %v, want %v (no later step may run)", tc.spy.order, tc.wantSteps)
			}
			for i := range tc.wantSteps {
				if tc.spy.order[i] != tc.wantSteps[i] {
					t.Fatalf("preflight step %d = %q, want %q", i, tc.spy.order[i], tc.wantSteps[i])
				}
			}
			if !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErrSub)
			}
			if got := startupResult(err); got == nil {
				t.Fatal("startupResult(genuine preflight failure) = nil, want the error")
			}
		})
	}
}

// TestExternalPreflightCancellationIsGraceful proves a lifecycle cancellation at
// the Redis/group step is classified as errStartupInterrupted (which
// startupResult turns into a graceful nil) and that it likewise short-circuits
// the manager and network steps. The manager and network steps are covered by
// TestExternalPreflightNetworkCancellationIsGraceful (shared startupInterrupted
// predicate).
func TestExternalPreflightCancellationIsGraceful(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	spy := &preflightSpy{group: fmt.Errorf("xgroup create: %w", context.Canceled)}
	manager, err := runExternalPreflight(ctx, logger, spy.deps())
	if !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("preflight cancellation = %v, want errStartupInterrupted", err)
	}
	if manager != nil {
		t.Fatalf("preflight cancellation returned manager %p, want nil", manager)
	}
	if got := startupResult(err); got != nil {
		t.Fatalf("startupResult(cancellation) = %v, want nil (graceful shutdown)", got)
	}
	if len(spy.order) != 1 || spy.order[0] != "group" {
		t.Fatalf("steps after a cancelled group step = %v, want only [group]", spy.order)
	}
	if !strings.Contains(logs.String(), "consumer group creation interrupted by shutdown") {
		t.Errorf("expected an Info log recording the interrupted startup, got:\n%s", logs.String())
	}
}

// TestExternalPreflightNetworkCancellationIsGraceful proves the same graceful
// classification when the lifecycle is cancelled during the LAST preflight step:
// group and manager succeed, NETWORKS returns an error wrapping
// context.Canceled while the lifecycle context is cancelled, and the result is
// errStartupInterrupted with a nil manager — never a NETWORKS failure. The
// recorded step order is exactly the three preflight steps, proving no later
// phase (function loading/fingerprinting) continued after the interrupted
// network step.
func TestExternalPreflightNetworkCancellationIsGraceful(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// group and manager succeed; only the network verify observes cancellation,
	// so the test isolates the last step's classification rather than an early
	// short-circuit.
	spy := &preflightSpy{net: fmt.Errorf("verify NETWORKS: %w", context.Canceled)}
	manager, err := runExternalPreflight(ctx, logger, spy.deps())
	if !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("preflight network cancellation = %v, want errStartupInterrupted", err)
	}
	if manager != nil {
		t.Fatalf("preflight network cancellation returned manager %p, want nil", manager)
	}
	if got := startupResult(err); got != nil {
		t.Fatalf("startupResult(network cancellation) = %v, want nil (graceful shutdown)", got)
	}
	// The cancellation happened at the last step, so all three steps ran; a
	// later phase appending after the call would show up here as a fourth entry.
	if len(spy.order) != 3 || spy.order[0] != "group" || spy.order[1] != "manager" || spy.order[2] != "networks" {
		t.Fatalf("steps before the interrupted network step = %v, want [group manager networks]", spy.order)
	}
	if !strings.Contains(logs.String(), "NETWORKS verification interrupted by shutdown") {
		t.Errorf("expected an Info log recording the interrupted NETWORKS step, got:\n%s", logs.String())
	}
}

// TestStartupInterruptedClassification pins the predicate that separates a
// lifecycle cancellation from a genuine startup failure: it is true only when
// the lifecycle is actually cancelled AND the operation error wraps a context
// error. A genuine error merely racing a cancelled lifecycle stays a failure, so
// a real problem is never silently swallowed as graceful shutdown.
func TestStartupInterruptedClassification(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"nil error", cancelled, nil, false},
		{"cancelled ctx, wrapped canceled", cancelled, fmt.Errorf("read: %w", context.Canceled), true},
		{"cancelled ctx, wrapped deadline", cancelled, fmt.Errorf("read: %w", context.DeadlineExceeded), true},
		{"live ctx, wrapped canceled", live, fmt.Errorf("read: %w", context.Canceled), false},
		{"cancelled ctx, genuine error", cancelled, errors.New("connection refused"), false},
		{"live ctx, genuine error", live, errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		if got := startupInterrupted(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: startupInterrupted = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStartupCleanupFailureLogLevel proves the startup cleanup failures are
// logged at Warn for a genuine error but demoted to Debug when the lifecycle is
// being cancelled, so an interrupted sweep does not pollute the shutdown trace
// with warnings. The message text is identical in both cases.
func TestStartupCleanupFailureLogLevel(t *testing.T) {
	const msg = "Image cleanup: startup sweep failed"

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Interrupted by shutdown: Debug only.
	logStartupCleanupFailure(ctx, logger, msg, fmt.Errorf("sweep: %w", context.Canceled))
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("interrupted cleanup logged at WARN; want DEBUG:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "level=DEBUG") || !strings.Contains(buf.String(), msg) {
		t.Errorf("interrupted cleanup missing DEBUG log %q:\n%s", msg, buf.String())
	}

	// Genuine failure on a live lifecycle: Warn.
	buf.Reset()
	logStartupCleanupFailure(context.Background(), logger, msg, errors.New("docker unavailable"))
	if !strings.Contains(buf.String(), "level=WARN") || !strings.Contains(buf.String(), msg) {
		t.Errorf("genuine cleanup failure missing WARN log %q:\n%s", msg, buf.String())
	}
}

// TestStartupHousekeepingCancelledLifecycleSkipsSweepsAndLogsDebug drives the
// existing startStartupHousekeeping seam with a cancelled lifecycle and a
// barrier that reports ctx.Err(), pinning the async startup-cleanup shutdown
// contract: no sweep runs against a shutting-down world, and the stop is logged
// at Debug (not a barrier-failure Warn). This uses the real seam the worker
// wires, so it exercises the production path without Docker or Redis.
func TestStartupHousekeepingCancelledLifecycleSkipsSweepsAndLogsDebug(t *testing.T) {
	buf := &testutil.SyncBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()

	var ran []string
	done := startStartupHousekeeping(lifecycle, logger, startupHousekeeper{
		exclusive: func(ctx context.Context, _ func(context.Context)) error {
			return ctx.Err()
		},
		sweep:  func(context.Context) { ran = append(ran, "sweep") },
		images: func(context.Context) { ran = append(ran, "images") },
		deps:   func(context.Context) { ran = append(ran, "deps") },
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not return on a cancelled lifecycle")
	}
	if len(ran) != 0 {
		t.Fatalf("sweeps ran on a cancelled lifecycle: %v", ran)
	}
	if strings.Contains(buf.String(), "barrier failed") {
		t.Errorf("cancelled lifecycle logged a barrier failure:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "housekeeping stopped by shutdown") {
		t.Errorf("expected the Debug 'stopped by shutdown' line, got:\n%s", buf.String())
	}
}

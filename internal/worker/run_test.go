package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

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

// TestShutdownRegistryZeroTimeoutIsUnbounded pins the correction that a step
// declaring no timeout (timeout <= 0) MUST run on a bare context.Background:
// the socket/manager/state/Redis closes historically took no context, so the
// registry must not impose a new 5s bound on them. A positive timeout still
// yields its own deadline, while a zero-timeout step both has no deadline and
// is NOT canceled after run (there is no bound to release), unlike a bounded
// step whose fresh context is canceled immediately after it executes.
func TestShutdownRegistryZeroTimeoutIsUnbounded(t *testing.T) {
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
	reg.register(record(shutdownStepSocket, 0))              // historically unbounded
	reg.register(record(shutdownStepManager, 0))             // historically unbounded
	reg.register(record(shutdownStepState, 0))               // historically unbounded
	reg.register(record(shutdownStepRedis, 0))               // historically unbounded
	reg.register(record(shutdownStepScheduler, time.Second)) // bounded 5s in Run; 1s here

	reg.run(slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, name := range []string{
		shutdownStepSocket, shutdownStepManager, shutdownStepState, shutdownStepRedis,
	} {
		obs, ok := seen[name]
		if !ok {
			t.Fatalf("zero-timeout step %q did not run", name)
		}
		if obs.hasBound {
			t.Errorf("%s has a deadline %v; want an unbounded context.Background", name, obs.deadline)
		}
		if obs.ctx.Err() != nil {
			t.Errorf("%s ctx.Err() = %v after run; want nil (no bound to cancel)", name, obs.ctx.Err())
		}
	}

	bounded, ok := seen[shutdownStepScheduler]
	if !ok || !bounded.hasBound {
		t.Fatalf("bounded step scheduler missing its deadline: %+v", bounded)
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
// must own live container state while containers are stopped); the stats flush
// precedes the state close (the final snapshot lands before the DB closes); and
// Redis is released last, after every other resource.
func TestShutdownRegistryOrderingInvariants(t *testing.T) {
	var order []string
	reg := &shutdownRegistry{}
	for _, name := range []string{
		shutdownStepRedis, shutdownStepState, shutdownStepStatsFlush,
		shutdownStepManager, shutdownStepServiceCleanup, shutdownStepServicesJoin,
		shutdownStepTracing,
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

// fakeEnsurer is the consumerGroupEnsurer seam: it returns a canned EnsureGroup
// error so the cancellation-vs-genuine classification is exercised without a
// Redis server.
type fakeEnsurer struct {
	err   error
	calls int
}

func (f *fakeEnsurer) EnsureGroup(context.Context) error {
	f.calls++
	return f.err
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

// TestEnsureGroupCancellationIsGraceful proves the core lifecycle contract: an
// EnsureGroup failure caused by the lifecycle being cancelled during startup is
// classified as errStartupInterrupted, which startupResult converts to a nil
// return, so Run converges through its deferred cleanup and reports success
// rather than a startup error.
func TestEnsureGroupCancellationIsGraceful(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeEnsurer{err: fmt.Errorf("xgroup create: %w", context.Canceled)}
	err := ensureGroup(ctx, c, logger)
	if !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("ensureGroup cancellation error = %v, want errStartupInterrupted", err)
	}
	if got := startupResult(err); got != nil {
		t.Fatalf("startupResult(cancellation) = %v, want nil (graceful shutdown)", got)
	}
	if !strings.Contains(logs.String(), "consumer group creation interrupted by shutdown") {
		t.Errorf("expected an Info log recording the interrupted startup, got:\n%s", logs.String())
	}
}

// TestEnsureGroupGenuineFailureIsReturned proves a real prerequisite failure is
// NOT masked by a coincident cancellation: an error that does not wrap a context
// error is wrapped and returned, and startupResult leaves it non-nil for the CLI
// boundary to print and exit on.
func TestEnsureGroupGenuineFailureIsReturned(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Even with a cancelled lifecycle, a genuine (non-context) error is a real
	// failure, not a graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &fakeEnsurer{err: errors.New("dial tcp 127.0.0.1:6379: connection refused")}
	err := ensureGroup(ctx, c, logger)
	if err == nil {
		t.Fatal("ensureGroup genuine failure = nil, want an error")
	}
	if errors.Is(err, errStartupInterrupted) {
		t.Fatalf("genuine failure was classified as a shutdown: %v", err)
	}
	if !strings.Contains(err.Error(), "ensure consumer group failed") {
		t.Fatalf("error = %q, want it to name the consumer-group failure", err)
	}
	if got := startupResult(err); !errors.Is(got, c.err) {
		t.Fatalf("startupResult(genuine) = %v, want the original error %v", got, c.err)
	}
	// The failure ran exactly once and was not retried by the classification.
	if c.calls != 1 {
		t.Fatalf("EnsureGroup calls = %d, want 1", c.calls)
	}
}

// TestEnsureGroupSuccessPassesThrough proves a successful group creation is a
// plain nil, unaffected by the classification.
func TestEnsureGroupSuccessPassesThrough(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &fakeEnsurer{}
	if err := ensureGroup(context.Background(), c, logger); err != nil {
		t.Fatalf("ensureGroup(nil) = %v, want nil", err)
	}
	if got := startupResult(nil); got != nil {
		t.Fatalf("startupResult(nil) = %v, want nil", got)
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

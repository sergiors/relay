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

	"relay/internal/function"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// svcDeadlineDocker is a minimal reconciler.Docker fake that records the
// deadline of every ctx passed to ServiceContainerList. Apply drives Reconcile,
// which lists containers once per call; Reconcile now derives its own fresh
// per-operation bound from the LIFECYCLE context the worker passes, so each
// per-function Apply is still observable as its OWN bounded (~reconcileTimeout)
// listing, not one shared deadline consumed across functions.
type svcDeadlineDocker struct {
	mu        sync.Mutex
	deadlines []time.Time // deadline (local time) of each ServiceContainerList call; zero = no deadline
}

func (f *svcDeadlineDocker) ResolveServiceImage(
	_ context.Context, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}

func (f *svcDeadlineDocker) StartService(_ context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	return "id-1", nil
}

func (f *svcDeadlineDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, d)
	} else {
		f.deadlines = append(f.deadlines, time.Time{})
	}
	return nil, nil
}

func (f *svcDeadlineDocker) StopServiceContainers(_ context.Context, _ []runtime.ServiceContainer) error {
	return nil
}

func (f *svcDeadlineDocker) RemoveFunctionServiceContainers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (f *svcDeadlineDocker) NetworkExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (f *svcDeadlineDocker) recordedDeadlines() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.deadlines...)
}

// TestReconcileTimeoutIsShortAndIndependentOfBuildTimeout pins the worker's
// normal-operation budget: reconcileTimeout is exactly 30s for service
// operations and is deliberately far shorter than the runtime's independent 10m
// Dockerfile-build bound (runtime.buildTimeout). Builds must NOT be limited by
// this constant; normal service operations MUST retain it.
func TestReconcileTimeoutIsShortAndIndependentOfBuildTimeout(t *testing.T) {
	if reconcileTimeout != 30*time.Second {
		t.Fatalf("reconcileTimeout = %v, want exactly 30s for normal service operations", reconcileTimeout)
	}
	const runtimeBuildTimeout = 10 * time.Minute
	if reconcileTimeout >= runtimeBuildTimeout {
		t.Fatalf("reconcileTimeout %v must be far shorter than the buildTimeout %v", reconcileTimeout, runtimeBuildTimeout)
	}
}

// blockingListDocker is a reconciler.Docker fake whose ServiceContainerList
// blocks until the context is done, then reports the context error. It models a
// slow daemon call so the lifecycle-cancellation behavior of the reconcile
// bound is observable.
type blockingListDocker struct {
	entered chan struct{}
	once    sync.Once
}

func (f *blockingListDocker) enteredOnce() {
	f.once.Do(func() { close(f.entered) })
}

func (f *blockingListDocker) ResolveServiceImage(
	_ context.Context, _ string, _ *function.Template, _ function.Service, _ string,
) (runtime.ServiceImage, error) {
	return runtime.ServiceImage{Ref: "img"}, nil
}

func (f *blockingListDocker) StartService(_ context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	return "id-1", nil
}

func (f *blockingListDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	f.enteredOnce()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *blockingListDocker) StopServiceContainers(_ context.Context, _ []runtime.ServiceContainer) error {
	return nil
}

func (f *blockingListDocker) RemoveFunctionServiceContainers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (f *blockingListDocker) NetworkExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// TestEnqueueStartupServicesRootedInLifecycle proves the enqueued per-function
// converge contexts are rooted in the worker lifecycle: cancelling that
// lifecycle cancels an in-flight service converge promptly, rather than waiting
// out the 30s reconcileTimeout. It uses a Docker fake whose list blocks until
// ctx is done.
func TestEnqueueStartupServicesRootedInLifecycle(t *testing.T) {
	fake := &blockingListDocker{entered: make(chan struct{})}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha"}, nil),
	}

	enqueueStartupServices(prepared, coordinator, logger)

	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("service converge was not entered")
	}

	start := time.Now()
	cancelLifecycle()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join after lifecycle cancel: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("service converge took %v to cancel after lifecycle cancel; want prompt", elapsed)
	}
}

// TestEnqueueStartupServicesBoundedPerFunction pins the per-function timeout
// guarantee through the new mechanism: enqueueStartupServices enqueues each
// desired state, the coordinator converges it with its lifecycle context, and
// the ServiceReconciler derives a FRESH per-operation ~reconcileTimeout bound
// from it, so each function's container listing observes its own distinct
// deadline rather than one shared (potentially already-consumed) context.
func TestEnqueueStartupServicesBoundedPerFunction(t *testing.T) {
	fake := &svcDeadlineDocker{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)
	defer stop()

	tmpl := &function.Template{
		Runtime: "node24",
		Services: []function.Service{
			{Entrypoint: "service.js", Port: 80, Replicas: 1},
		},
	}
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha", Env: nil}, nil),
		runner.NewPrepared(function.Function{Name: "beta", Template: tmpl}, &runtime.Prepared{Image: "img-beta", Env: nil}, nil),
	}

	enqueueStartupServices(prepared, coordinator, logger)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	deadlines := fake.recordedDeadlines()
	// Two per-function Applys, each with its own bounded listing. The orphan
	// sweep is no longer part of this helper (it is the housekeeping pass).
	if len(deadlines) != 2 {
		t.Fatalf("ServiceContainerList saw %d calls, want 2 (two Applys): %v", len(deadlines), deadlines)
	}
	// Both Applys must be bounded and each must have a DISTINCT deadline (a
	// fresh ~reconcileTimeout bound per function, not one shared context).
	for i, d := range deadlines {
		if d.IsZero() {
			t.Fatalf("Apply %d carried no deadline; want a fresh reconcileTimeout bound", i)
		}
		if got := time.Until(d); got <= 0 || got > reconcileTimeout {
			t.Fatalf("Apply %d deadline %v is not bounded to reconcileTimeout=%v (until=%v)", i, d, reconcileTimeout, got)
		}
	}
	if deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("Apply deadlines are identical (%v); want distinct per-function bounds", deadlines)
	}
}

// TestStartupHousekeepingBarrierOrderAndNonBlocking is the deterministic seam
// test for the background startup housekeeping. It proves:
//   - startStartupHousekeeping returns immediately (Run never blocks on it);
//   - the exclusive barrier runs FIRST and nothing runs until the callback
//     completes;
//   - the passes then run in the safe order sweep → images → deps;
//   - a barrier failure skips every sweep (cleanup never runs against an
//     unconverged or shutting-down world).
func TestStartupHousekeepingBarrierOrderAndNonBlocking(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	release := make(chan struct{})
	enteredWait := make(chan struct{})
	var mu sync.Mutex
	var order []string
	record := func(step string) {
		mu.Lock()
		order = append(order, step)
		mu.Unlock()
	}

	h := startupHousekeeper{
		exclusive: func(_ context.Context, fn func(context.Context)) error {
			record("wait")
			close(enteredWait)
			<-release
			fn(context.Background())
			return nil
		},
		sweep:  func(context.Context) { record("sweep") },
		images: func(context.Context) { record("images") },
		deps:   func(context.Context) { record("deps") },
	}

	// Returns immediately even though the barrier blocks.
	done := startStartupHousekeeping(context.Background(), logger, h)
	select {
	case <-done:
		t.Fatal("housekeeping returned before the barrier cleared; Run must not block on it")
	case <-enteredWait:
	}

	// Nothing but the barrier may have run yet.
	mu.Lock()
	if len(order) != 1 || order[0] != "wait" {
		mu.Unlock()
		t.Fatalf("passes ran before the barrier cleared: %v", order)
	}
	mu.Unlock()

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not finish after the barrier cleared")
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"wait", "sweep", "images", "deps"}
	if len(got) != len(want) {
		t.Fatalf("pass order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pass order = %v, want %v", got, want)
		}
	}

	// Barrier failure skips every sweep.
	var failed []string
	h2 := startupHousekeeper{
		exclusive: func(context.Context, func(context.Context)) error { return context.Canceled },
		sweep:     func(context.Context) { failed = append(failed, "sweep") },
		images:    func(context.Context) { failed = append(failed, "images") },
		deps:      func(context.Context) { failed = append(failed, "deps") },
	}
	done2 := startStartupHousekeeping(context.Background(), logger, h2)
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not return after a barrier failure")
	}
	if len(failed) != 0 {
		t.Fatalf("a failed barrier must skip every sweep, got %v", failed)
	}
}

// TestStartupHousekeepingExcludesLiveUpdates is the end-to-end wiring proof: it
// runs the real startStartupHousekeeping with the real coordinator's RunExclusive
// seam and a real orphan sweep. A live service update published while the sweep
// is parked must NOT start an Apply (no concurrent resolve) until the exclusive
// window closes, then it must run with the latest desired state.
func TestStartupHousekeepingExcludesLiveUpdates(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["list"] = true // the orphan sweep's listing parks until released
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := applyServiceTemplate()
	// Housekeeping's sweep lifecycle is separate so the test can release the
	// parked sweep without cancelling the coordinator.
	sweepCtx, cancelSweep := context.WithCancel(context.Background())
	defer cancelSweep()
	imagesRan := make(chan struct{}, 1)
	done := startStartupHousekeeping(sweepCtx, logger, startupHousekeeper{
		exclusive: coordinator.RunExclusive,
		sweep:     func(hctx context.Context) { svcCtrl.SweepOrphans(hctx, map[string]bool{"alpha": true}) },
		images: func(context.Context) {
			// Runs after the sweep and before resume; unblock the resumed Apply's
			// listing so it can complete.
			fake.mu.Lock()
			fake.block["list"] = false
			fake.mu.Unlock()
			imagesRan <- struct{}{}
		},
		deps: func(context.Context) {},
	})

	// The sweep has entered its listing (recorded before blocking): the exclusive
	// window is now open with the coordinator paused.
	fake.waitCount(t, "list", 1)

	// A live update arrives during the sweep: it must coalesce, not Apply. Give
	// the scheduler ample opportunity to (wrongly) start it; the exclusive pause
	// must keep the resolve count at zero.
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)
	time.Sleep(50 * time.Millisecond)
	if got := len(fake.forOp("resolve")); got != 0 {
		t.Fatalf("a live Apply ran during the exclusive housekeeping window: resolve calls = %d, want 0", got)
	}
	if fake.tryEntered("resolve") {
		t.Fatal("a live Apply signalled entry during the exclusive housekeeping window")
	}

	// Release the sweep: the exclusive window closes, resume schedules the update.
	cancelSweep()
	select {
	case <-imagesRan:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not proceed past the sweep")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not finish")
	}
	fake.waitEntered(t, "resolve")

	resolves := fake.forOp("resolve")
	if len(resolves) != 1 {
		t.Fatalf("resolve calls = %d, want 1 (the resumed update)", len(resolves))
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// svcShutdownDocker is a minimal reconciler.Docker fake for the graceful
// shutdown cleanup tests: it holds in-memory service containers (with worker
// hostname identities) and records which containers ShutdownCleanup sent to
// StopServiceContainers.
type svcShutdownDocker struct {
	mu         sync.Mutex
	nextID     int
	containers map[string]runtime.ServiceContainer
	stopped    []string
	stopErr    error
}

func newSvcShutdownDocker() *svcShutdownDocker {
	return &svcShutdownDocker{containers: map[string]runtime.ServiceContainer{}}
}

func (f *svcShutdownDocker) ResolveServiceImage(
	_ context.Context, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}

func (f *svcShutdownDocker) StartService(_ context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	return "", nil
}

func (f *svcShutdownDocker) NetworkExists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (f *svcShutdownDocker) RemoveFunctionServiceContainers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (f *svcShutdownDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]runtime.ServiceContainer, 0, len(f.containers))
	for _, c := range f.containers {
		out = append(out, c)
	}
	return out, nil
}

func (f *svcShutdownDocker) StopServiceContainers(_ context.Context, containers []runtime.ServiceContainer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range containers {
		f.stopped = append(f.stopped, c.ID)
		delete(f.containers, c.ID)
	}
	return f.stopErr
}

// addService inserts one service container for the given worker hostname.
func (f *svcShutdownDocker) addService(id, hostname string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.containers[id] = runtime.ServiceContainer{
		ID:       id,
		Function: "fn",
		Hostname: hostname,
		Image:    "img-1",
	}
}

func (f *svcShutdownDocker) stoppedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stopped...)
}

func (f *svcShutdownDocker) remainingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.containers)
}

// TestShutdownCleanupHostnameScopedWorkerSide: the worker-side ShutdownCleanup
// path (via shutdownServices with the same shutdownServiceTimeout bound the
// shutdown registry derives for its step) removes only the containers owned by
// this worker's hostname, preserves other workers', and returns promptly even
// with an already-cancelled context (the deadline is the caller's
// responsibility; shutdown continues anyway).
func TestWorkerShutdownServicesHostnameScopedAndBounded(t *testing.T) {
	fake := newSvcShutdownDocker()
	fake.addService("own-1", "relay-worker-a")
	fake.addService("own-2", "relay-worker-a")
	fake.addService("other-1", "relay-worker-b")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	// The shutdown registry owns the step's bound; apply the same 30s bound
	// here so the bounded cleanup path is exercised as production runs it.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	shutdownServices(cleanupCtx, svcCtrl, "relay-worker-a")
	cleanupCancel()

	consoleFake := fake // the same fake: shutdownServices already ran the cleanup
	stopped := consoleFake.stoppedIDs()
	if len(stopped) != 2 {
		t.Fatalf("stops = %v, want [own-1 own-2] (own hostname only)", stopped)
	}
	for _, id := range stopped {
		if id != "own-1" && id != "own-2" {
			t.Fatalf("stop of foreign container %s; want own hostname only: %v", id, stopped)
		}
	}
	if remaining := fake.remainingCount(); remaining != 1 {
		t.Fatalf("remaining containers = %d, want 1 (other worker preserved)", remaining)
	}

	// Already-cancelled-context tolerance: ShutdownCleanup must return promptly
	// when the fake's StopServiceContainers is instantaneous — the deadline is
	// the caller's (shutdownServices') responsibility, not the reconciler's.
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := svcCtrl.ShutdownCleanup(cancelCtx, "relay-worker-b"); err != nil {
		t.Fatalf("ShutdownCleanup with cancelled ctx: %v", err)
	}
	if elapsed := time.Since(start); elapsed > shutdownServiceTimeout {
		t.Fatalf("ShutdownCleanup took %v; must not exceed the bound", elapsed)
	}
	if remaining := fake.remainingCount(); remaining != 0 {
		t.Fatalf("remaining containers = %d, want 0 (cancelled ctx still removes via the fake)", remaining)
	}
}

// TestWorkerShutdownServiceCleanupStepFailureReachesRegistry pins the fix that a
// ShutdownCleanup error must propagate out of the service-cleanup step to
// shutdownRegistry.run, where it is logged once with the structured step name
// and error. It drives the real production step closure (shutdownServices)
// through the registry, so a regression to swallowing the error (returning nil)
// loses the "step=service-cleanup" log and fails the test.
func TestWorkerShutdownServiceCleanupStepFailureReachesRegistry(t *testing.T) {
	fake := newSvcShutdownDocker()
	fake.addService("own-1", "relay-worker-a")
	stopErr := errors.New("docker stop: daemon unavailable")
	fake.stopErr = stopErr

	var logs bytes.Buffer
	// Debug level so the lower-level ShutdownCleanup Warn is captured too; the
	// test asserts both layers, not just the registry's.
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	reg := &shutdownRegistry{}
	reg.register(shutdownStep{
		name:    shutdownStepServiceCleanup,
		timeout: shutdownServiceTimeout,
		run: func(stepCtx context.Context) error {
			return shutdownServices(stepCtx, svcCtrl, "relay-worker-a")
		},
	})
	reg.run(logger)

	out := logs.String()
	// The registry surfaced the failure with the structured step name and the
	// propagated error, rather than the old silent nil return.
	if !strings.Contains(out, "step="+shutdownStepServiceCleanup) {
		t.Errorf("registry log missing structured step=%s:\n%s", shutdownStepServiceCleanup, out)
	}
	if !strings.Contains(out, stopErr.Error()) {
		t.Errorf("registry log missing the propagated error %q:\n%s", stopErr, out)
	}
	// The lower-level ShutdownCleanup Warn is still present, and the registry
	// adds its own structured step log — exactly the two expected records, with
	// no duplicate debug line from the step closure.
	if n := strings.Count(out, stopErr.Error()); n != 2 {
		t.Errorf("error appears %d times, want 2 (reconciler Warn + registry step log):\n%s", n, out)
	}
	if strings.Contains(out, "shutdown cleanup returned error") {
		t.Errorf("shutdownServices must not re-log the error it now returns:\n%s", out)
	}
	if !strings.Contains(out, "Shutdown complete") {
		t.Errorf("a failing step must not abort the sequence; missing completion marker:\n%s", out)
	}
}

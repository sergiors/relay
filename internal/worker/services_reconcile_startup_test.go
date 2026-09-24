package worker

import (
	"context"
	"io"
	"log/slog"
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
// which lists containers once per call, so the per-function Applys in
// reconcileStartupServices are observable: each must receive its OWN bounded
// context (a distinct ~reconcileTimeout deadline), not a single shared one.
type svcDeadlineDocker struct {
	mu        sync.Mutex
	deadlines []time.Time // deadline (local time) of each ServiceContainerList call; zero = no deadline
}

func (f *svcDeadlineDocker) ResolveServiceImage(
	_ context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
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

func (f *svcDeadlineDocker) VerifyNetworks(_ context.Context, _ []string) (string, bool, error) {
	return "", true, nil
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
	_ context.Context, _, _ string, _ *function.Template, _ function.Service, _ string,
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

func (f *blockingListDocker) VerifyNetworks(_ context.Context, _ []string) (string, bool, error) {
	return "", true, nil
}

// TestReconcileStartupServicesRootedInLifecycle proves the per-function reconcile
// contexts are rooted in the worker lifecycle: cancelling that lifecycle cancels
// an in-flight service converge promptly, rather than waiting out the 30s
// reconcileTimeout. It uses a Docker fake whose list blocks until ctx is done.
func TestReconcileStartupServicesRootedInLifecycle(t *testing.T) {
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	fake := &blockingListDocker{entered: make(chan struct{})}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha"}, nil),
	}
	functions := []function.Function{{Name: "alpha", Template: tmpl}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		reconcileStartupServices(lifecycle, prepared, functions, svcCtrl, logger)
	}()

	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("service converge was not entered")
	}

	start := time.Now()
	cancelLifecycle()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the in-flight service converge")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("service converge took %v to cancel after lifecycle cancel; want prompt", elapsed)
	}
}

// TestReconcileStartupServicesBoundedPerFunction pins the per-function timeout
// fix: reconcileStartupServices must give each function's service converge its
// OWN bounded context, so one slow Docker call cannot consume the budget of the
// functions that follow. It does so by recording the deadline each Apply's
// ServiceContainerList saw: two available functions must each observe a distinct
// ~reconcileTimeout deadline. The trailing SweepOrphans call is rooted in the
// lifecycle context, which here is the unbounded context.Background, so it must
// carry no deadline.
func TestReconcileStartupServicesBoundedPerFunction(t *testing.T) {
	fake := &svcDeadlineDocker{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger)

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
	// liveNames is derived from `functions` in the helper; provide the matching
	// on-disk set so the orphan sweep finds nothing to remove.
	functions := []function.Function{
		{Name: "alpha", Template: tmpl},
		{Name: "beta", Template: tmpl},
	}

	reconcileStartupServices(context.Background(), prepared, functions, svcCtrl, logger)

	deadlines := fake.recordedDeadlines()
	// Two per-function Applys (each with a bounded deadline) + the SweepOrphans
	// call (context.Background, no deadline).
	if len(deadlines) != 3 {
		t.Fatalf("ServiceContainerList saw %d calls, want 3 (two Applys + sweep): %v", len(deadlines), deadlines)
	}

	var applyDeadlines []time.Time
	for _, d := range deadlines {
		if !d.IsZero() {
			applyDeadlines = append(applyDeadlines, d)
		} else if deadlines[len(deadlines)-1] != d {
			t.Fatalf("a non-final call carried no deadline; only SweepOrphans should: %v", deadlines)
		}
	}
	// The sweep is the final call and must be unbounded.
	if !deadlines[len(deadlines)-1].IsZero() {
		t.Fatalf("SweepOrphans call must use an unbounded context, got deadline %v", deadlines[len(deadlines)-1])
	}

	if len(applyDeadlines) != 2 {
		t.Fatalf("per-function Applys = %d, want 2: %v", len(applyDeadlines), deadlines)
	}
	// Both Applys must be bounded and each must have a DISTINCT deadline (a
	// fresh ~reconcileTimeout bound per function, not one shared context).
	for i, d := range applyDeadlines {
		if got := time.Until(d); got <= 0 || got > reconcileTimeout {
			t.Fatalf("Apply %d deadline %v is not bounded to reconcileTimeout=%v (until=%v)", i, d, reconcileTimeout, got)
		}
	}
	if applyDeadlines[0].Equal(applyDeadlines[1]) {
		t.Fatalf("Apply deadlines are identical (%v); want distinct per-function bounds", applyDeadlines)
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
}

func newSvcShutdownDocker() *svcShutdownDocker {
	return &svcShutdownDocker{containers: map[string]runtime.ServiceContainer{}}
}

func (f *svcShutdownDocker) ResolveServiceImage(
	_ context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
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

func (f *svcShutdownDocker) VerifyNetworks(_ context.Context, _ []string) (string, bool, error) {
	return "", true, nil
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
	return nil
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
// path (via shutdownServices with the bound the helper applies internally)
// removes only the containers owned by this worker's hostname, preserves other
// workers', and returns promptly even with an already-cancelled context (the
// deadline is the caller's responsibility; shutdown continues anyway).
func TestWorkerShutdownServicesHostnameScopedAndBounded(t *testing.T) {
	fake := newSvcShutdownDocker()
	fake.addService("own-1", "relay-worker-a")
	fake.addService("own-2", "relay-worker-a")
	fake.addService("other-1", "relay-worker-b")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger)

	shutdownServices(svcCtrl, "relay-worker-a", logger)

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

package reconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

type coordinatorDocker struct {
	mu       sync.Mutex
	active   int
	max      int
	resolves []string
	entered  chan struct{}
	release  chan struct{}
}

func (d *coordinatorDocker) ResolveServiceImage(_ context.Context, fnName, _ string, _ *function.Template, _ function.Service, image string) (runtime.ServiceImage, error) {
	d.mu.Lock()
	d.active++
	if d.active > d.max {
		d.max = d.active
	}
	d.resolves = append(d.resolves, fnName+"="+image)
	d.mu.Unlock()
	d.entered <- struct{}{}
	<-d.release
	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return runtime.ServiceImage{Ref: image, ID: image}, nil
}

func (d *coordinatorDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "id", nil
}
func (d *coordinatorDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *coordinatorDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *coordinatorDocker) RemoveFunctionServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *coordinatorDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }

func TestServiceCoordinatorLimitsConcurrencyAndCoalescesLatest(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", "", tmpl, "img-1", nil)
	<-docker.entered
	coordinator.Enqueue("beta", "", tmpl, "img-b", nil)
	<-docker.entered
	// A third function cannot enter while both fixed workers are occupied.
	coordinator.Enqueue("gamma", "", tmpl, "img-g", nil)
	select {
	case <-docker.entered:
		t.Fatal("more than two service reconciles entered concurrently")
	default:
	}

	// Updates while alpha is active collapse to the newest desired image.
	coordinator.Enqueue("alpha", "", tmpl, "img-2", nil)
	coordinator.Enqueue("alpha", "", tmpl, "img-3", nil)
	close(docker.release)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	cancel()
	if err := coordinator.Join(context.Background()); err != nil {
		t.Fatalf("join: %v", err)
	}

	docker.mu.Lock()
	defer docker.mu.Unlock()
	if docker.max != 2 {
		t.Fatalf("maximum concurrent reconciles = %d, want 2", docker.max)
	}
	var alpha []string
	for _, resolve := range docker.resolves {
		if len(resolve) >= len("alpha=") && resolve[:len("alpha=")] == "alpha=" {
			alpha = append(alpha, resolve)
		}
	}
	if len(alpha) != 2 || alpha[0] != "alpha=img-1" || alpha[1] != "alpha=img-3" {
		t.Fatalf("alpha resolves = %v, want [alpha=img-1 alpha=img-3]", alpha)
	}
}

// newBlockingCoordinator builds a started coordinator whose worker resolve is
// blocked until release is called, and returns the coordinator plus a release
// function (idempotent) that unblocks the worker, cancels the lifecycle, and
// joins the workers.
func newBlockingCoordinator(t *testing.T) (*ServiceCoordinator, *coordinatorDocker, func()) {
	t.Helper()
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	var once sync.Once
	release := func() {
		once.Do(func() {
			close(docker.release)
			cancel()
			joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer joinCancel()
			if err := coordinator.Join(joinCtx); err != nil {
				t.Fatalf("join: %v", err)
			}
		})
	}
	return coordinator, docker, release
}

// TestServiceCoordinatorWaitCancelsPromptlyWhileBusy proves Wait returns
// context.Canceled promptly when its ctx is cancelled while the coordinator is
// busy, rather than hanging on a missing worker broadcast. context.AfterFunc
// broadcasts the condition, so even a Wait parked with no worker progress wakes
// up.
func TestServiceCoordinatorWaitCancelsPromptlyWhileBusy(t *testing.T) {
	coordinator, docker, release := newBlockingCoordinator(t)
	defer release()

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", "", tmpl, "img-1", nil)
	<-docker.entered // the worker is now busy in ResolveServiceImage

	waitCtx, waitCancel := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() { waitDone <- coordinator.Wait(waitCtx) }()
	waitCancel()

	select {
	case err := <-waitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after its ctx was cancelled while busy")
	}
}

// TestServiceCoordinatorJoinBoundedThenDrains proves Join is bounded by its ctx
// (it returns the ctx error rather than hanging when a worker is stuck in an
// unbounded Docker call) and that once the work completes a later Join drains
// cleanly. It also exercises repeated Join/Wait calls on the same coordinator.
func TestServiceCoordinatorJoinBoundedThenDrains(t *testing.T) {
	coordinator, docker, release := newBlockingCoordinator(t)
	defer release()

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", "", tmpl, "img-1", nil)
	<-docker.entered

	// The worker is parked in ResolveServiceImage (which ignores ctx), so Join
	// must give up at its bound rather than leak a goroutine waiting forever.
	boundedCtx, boundedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer boundedCancel()
	if err := coordinator.Join(boundedCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Join err = %v, want context.DeadlineExceeded", err)
	}

	// Release the worker and cancel the lifecycle: a fresh Join now drains the
	// workers (workersDone was closed by the single waiter Start launched) and
	// returns nil.
	release()
}

// TestServiceCoordinatorEnqueueAfterShutdownIsSafe proves a request racing
// lifecycle cancellation does not hang: enqueue after cancelPending releases
// the waiter immediately (stopped is checked under the same lock that
// cancelPending holds), so RemoveAndWait returns without waiting on a worker
// that has already exited.
func TestServiceCoordinatorEnqueueAfterShutdownIsSafe(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	// Cancel with no in-flight work: the workers observe ctx.Done, run
	// cancelPending (setting stopped), and exit. Join drains them.
	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}

	// A removal after shutdown must return promptly rather than deadlock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		coordinator.RemoveAndWait("alpha")
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAndWait after shutdown hung; the waiter was not released")
	}
}

// removalBlockingDocker blocks RemoveFunctionServiceContainers until release is
// closed, then reports ctx.Err if the caller's context expired first. It lets a
// test observe whether RemoveAndWait returned before the removal actually
// completed.
type removalBlockingDocker struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *removalBlockingDocker) ResolveServiceImage(
	_ context.Context, _, _ string, _ *function.Template, _ function.Service, image string,
) (runtime.ServiceImage, error) {
	return runtime.ServiceImage{Ref: image, ID: image}, nil
}
func (d *removalBlockingDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "id", nil
}
func (d *removalBlockingDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *removalBlockingDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *removalBlockingDocker) RemoveFunctionServiceContainers(ctx context.Context, _ string) (int, error) {
	d.once.Do(func() { close(d.entered) })
	select {
	case <-d.release:
		return 1, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
func (d *removalBlockingDocker) NetworkExists(context.Context, string) (bool, error) {
	return true, nil
}

// TestServiceCoordinatorRemoveAndWaitWaitsForCompletion proves RemoveAndWait
// cannot return before the queued removal operation actually completes. It uses
// a Docker fake whose removal blocks until released; if RemoveAndWait had any
// early-return timeout path it would return while the fake is still parked, and
// the assertion would catch it.
func TestServiceCoordinatorRemoveAndWaitWaitsForCompletion(t *testing.T) {
	docker := &removalBlockingDocker{entered: make(chan struct{}), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), time.Minute)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		coordinator.RemoveAndWait("alpha")
	}()

	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("removal was never entered")
	}
	// The removal is parked; RemoveAndWait must still be waiting.
	select {
	case <-returned:
		t.Fatal("RemoveAndWait returned before the removal completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(docker.release)
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAndWait did not return after the removal completed")
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorRemoveOperationBoundedByReconcileTimeout proves the
// removal operation the coordinator runs is bounded by the reconciler's
// reconcileTimeout: a removal that never completes on its own (a wedged daemon)
// observes ctx.Done at that bound and returns, so a deterministic RemoveAndWait
// cannot hang forever. Shutdown (lifecycle cancellation) is the other release.
func TestServiceCoordinatorRemoveOperationBoundedByReconcileTimeout(t *testing.T) {
	const bound = 50 * time.Millisecond
	docker := &removalBlockingDocker{entered: make(chan struct{}), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), bound)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		coordinator.RemoveAndWait("alpha")
	}()

	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAndWait did not return after the removal bound expired")
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorEnqueueRemoveNonBlocking proves EnqueueRemove returns
// immediately without waiting for the removal to run, and that the removal is
// still executed and coalesces like any other desired state.
func TestServiceCoordinatorEnqueueRemoveNonBlocking(t *testing.T) {
	docker := &removalBlockingDocker{entered: make(chan struct{}), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), time.Minute)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		coordinator.EnqueueRemove("alpha")
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("EnqueueRemove blocked on the removal")
	}

	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("EnqueueRemove did not schedule the removal")
	}
	close(docker.release)

	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorSnapshotsTemplateAtEnqueue proves Enqueue snapshots the
// template fields the reconcile path reads, so a caller mutating its template
// after Enqueue (as the live reconciler does when it replaces a function's
// template) cannot change what the worker converges. The fake captures the
// template passed to ResolveServiceImage; the mutation must not be visible.
func TestServiceCoordinatorSnapshotsTemplateAtEnqueue(t *testing.T) {
	docker := &snapshotDocker{}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Env:      map[string]string{"A": "1"},
		Secrets:  map[string]function.SecretRef{"S": "ref-1"},
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", "", tmpl, "img", []string{"PLAN=1"})

	// Mutate the caller's template while the request is queued/in flight.
	tmpl.Env["A"] = "mutated"
	tmpl.Secrets["S"] = "mutated-ref"
	tmpl.Services[0].Port = 9999
	tmpl.Services = append(tmpl.Services, function.Service{Entrypoint: "extra.js", Port: 81, Replicas: 1})

	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	docker.mu.Lock()
	defer docker.mu.Unlock()
	if docker.seenPort != 80 {
		t.Fatalf("worker saw port %d, want the snapshotted 80", docker.seenPort)
	}
	if got := docker.seenEnv["A"]; got != "1" {
		t.Fatalf("worker saw Env[A] = %q, want the snapshotted %q", got, "1")
	}
	if got := string(docker.seenSecret["S"]); got != "ref-1" {
		t.Fatalf("worker saw Secrets[S] = %q, want the snapshotted %q", got, "ref-1")
	}
	if len(docker.seenServices) != 1 {
		t.Fatalf("worker saw %d services, want the snapshotted 1", len(docker.seenServices))
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// snapshotDocker captures the template it receives so a test can assert the
// worker converged the enqueue-time snapshot rather than the mutated original.
type snapshotDocker struct {
	mu           sync.Mutex
	seenPort     int
	seenEnv      map[string]string
	seenSecret   map[string]function.SecretRef
	seenServices []function.Service
}

func (d *snapshotDocker) ResolveServiceImage(
	_ context.Context, _, _ string, tmpl *function.Template, svc function.Service, image string,
) (runtime.ServiceImage, error) {
	d.mu.Lock()
	d.seenPort = svc.Port
	d.seenEnv = tmpl.Env
	d.seenSecret = tmpl.Secrets
	d.seenServices = append([]function.Service(nil), tmpl.Services...)
	d.mu.Unlock()
	return runtime.ServiceImage{Ref: image, ID: image}, nil
}
func (d *snapshotDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "id", nil
}
func (d *snapshotDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *snapshotDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *snapshotDocker) RemoveFunctionServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *snapshotDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }

// pauseProbeDocker records every ResolveServiceImage entry (function=image) and
// the peak concurrent count. It lets a test prove that a service pass enqueued
// during a RunExclusive housekeeping window never starts until the window ends.
type pauseProbeDocker struct {
	mu      sync.Mutex
	active  int
	max     int
	entered chan string
}

func (d *pauseProbeDocker) ResolveServiceImage(
	_ context.Context, fnName, _ string, _ *function.Template, _ function.Service, image string,
) (runtime.ServiceImage, error) {
	d.mu.Lock()
	d.active++
	if d.active > d.max {
		d.max = d.active
	}
	d.mu.Unlock()
	d.entered <- fnName + "=" + image
	d.mu.Lock()
	d.active--
	d.mu.Unlock()
	return runtime.ServiceImage{Ref: image, ID: image}, nil
}
func (d *pauseProbeDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "id", nil
}
func (d *pauseProbeDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *pauseProbeDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *pauseProbeDocker) RemoveFunctionServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *pauseProbeDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }
func (d *pauseProbeDocker) peak() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.max
}

// TestServiceCoordinatorRunExclusivePausesScheduling is the deterministic seam
// proof for the startup housekeeping barrier: an update arriving while
// RunExclusive holds its exclusive window must NOT start a service pass (it must
// not run concurrently with the sweep/GC callback), and must instead coalesce and
// be scheduled once the window closes.
func TestServiceCoordinatorRunExclusivePausesScheduling(t *testing.T) {
	docker := &pauseProbeDocker{entered: make(chan string, 8)}
	services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	// The initial desired state settles before housekeeping starts.
	coordinator.Enqueue("alpha", "", tmpl, "img-1", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := <-docker.entered; got != "alpha=img-1" {
		t.Fatalf("initial pass = %q, want alpha=img-1", got)
	}

	// Enter the exclusive housekeeping window and park inside it.
	inExclusive := make(chan struct{})
	releaseExclusive := make(chan struct{})
	exclusiveDone := make(chan error, 1)
	go func() {
		exclusiveDone <- coordinator.RunExclusive(context.Background(), func(context.Context) {
			close(inExclusive)
			<-releaseExclusive
		})
	}()
	select {
	case <-inExclusive:
	case <-time.After(2 * time.Second):
		t.Fatal("RunExclusive did not enter its callback")
	}

	// A live update arrives during housekeeping. It must coalesce as pending, not
	// start a pass that would race the sweeps.
	coordinator.Enqueue("alpha", "", tmpl, "img-2", nil)
	select {
	case got := <-docker.entered:
		t.Fatalf("a service pass ran during housekeeping: %s", got)
	case <-time.After(100 * time.Millisecond):
	}

	// Closing the window resumes scheduling; the latest desired state runs.
	close(releaseExclusive)
	select {
	case err := <-exclusiveDone:
		if err != nil {
			t.Fatalf("RunExclusive: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunExclusive did not return after the window closed")
	}
	select {
	case got := <-docker.entered:
		if got != "alpha=img-2" {
			t.Fatalf("resumed pass = %q, want the coalesced alpha=img-2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resume did not schedule the coalesced update")
	}

	if got := docker.peak(); got != 1 {
		t.Fatalf("peak concurrent passes = %d, want 1 (no pass during housekeeping)", got)
	}

	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("final wait: %v", err)
	}
	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorRunExclusiveCancelledBarrierSkipsCallback proves a
// cancelled barrier ctx (lifecycle shutdown while busy) makes RunExclusive run
// neither the callback NOR lose the pause: fn is never invoked, so the caller
// skips its sweeps, and the coordinator returns to normal scheduling afterwards.
func TestServiceCoordinatorRunExclusiveCancelledBarrierSkipsCallback(t *testing.T) {
	coordinator, docker, release := newBlockingCoordinator(t)
	defer release()

	tmpl := &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", "", tmpl, "img-1", nil)
	<-docker.entered // the worker is busy, so the barrier cannot clear

	barrierCtx, barrierCancel := context.WithCancel(context.Background())
	barrierCancel() // shutdown while the barrier is still waiting
	invoked := false
	err := coordinator.RunExclusive(barrierCtx, func(context.Context) { invoked = true })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunExclusive err = %v, want context.Canceled", err)
	}
	if invoked {
		t.Fatal("RunExclusive invoked the callback despite a cancelled barrier")
	}
}

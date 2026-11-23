package reconciler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

type coordinatorDocker struct {
	mu         sync.Mutex
	active     int
	max        int
	resolves   []string
	resolveErr error
	entered    chan struct{}
	release    chan struct{}
	removals   atomic.Int32
}

func (d *coordinatorDocker) ResolveServiceImage(_ context.Context, fnName string, _ *app.Template, _ app.Service, image string) (runtime.ServiceImage, error) {
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
	err := d.resolveErr
	d.mu.Unlock()
	if err != nil {
		return runtime.ServiceImage{}, err
	}
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
func (d *coordinatorDocker) RemoveAppServiceContainers(context.Context, string) (int, error) {
	d.removals.Add(1)
	return 0, nil
}
func (d *coordinatorDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }

func TestServiceCoordinatorLimitsConcurrencyAndCoalescesLatest(t *testing.T) {
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", tmpl, "img-1", nil)
	<-docker.entered
	coordinator.Enqueue("beta", tmpl, "img-b", nil)
	<-docker.entered
	// A third app cannot enter while both fixed workers are occupied.
	coordinator.Enqueue("gamma", tmpl, "img-g", nil)
	select {
	case <-docker.entered:
		t.Fatal("more than two service reconciles entered concurrently")
	default:
	}

	// Updates while alpha is active collapse to the newest desired image.
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)
	coordinator.Enqueue("alpha", tmpl, "img-3", nil)
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

func TestServiceCoordinatorStatusAuthority(t *testing.T) {
	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "slow success"},
		{name: "slow failure", err: errors.New("resolve failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docker := &coordinatorDocker{
				entered:    make(chan struct{}, 8),
				release:    make(chan struct{}),
				resolveErr: tc.err,
			}
			services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
			coordinator := NewServiceCoordinator(services)
			lifecycle, cancel := context.WithCancel(context.Background())
			coordinator.Start(lifecycle)
			var release sync.Once
			defer func() {
				release.Do(func() { close(docker.release) })
				cancel()
				if err := coordinator.Join(context.Background()); err != nil {
					t.Fatalf("join: %v", err)
				}
			}()

			meaningfulDone := make(chan error, 1)
			observationDone := make(chan error, 1)
			coordinator.EnqueueWithStatus("alpha", tmpl, "img-1", nil, nil, func(err error) {
				meaningfulDone <- err
			})
			<-docker.entered
			// A periodic no-op/self-heal observation shares the meaningful
			// request's authority and must not stale its slow completion.
			coordinator.EnqueueStatusObservation("alpha", tmpl, "img-1", nil, nil, func(err error) {
				observationDone <- err
			})
			release.Do(func() { close(docker.release) })
			if err := coordinator.Wait(context.Background()); err != nil {
				t.Fatalf("wait: %v", err)
			}
			select {
			case got := <-meaningfulDone:
				if (got != nil) != (tc.err != nil) {
					t.Fatalf("meaningful completion error = %v, want error=%v", got, tc.err != nil)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("meaningful completion was incorrectly invalidated by observation")
			}
		})
	}
}

func TestServiceCoordinatorNewMeaningfulRequestSupersedesStatus(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	var release sync.Once
	defer func() {
		release.Do(func() { close(docker.release) })
		cancel()
		_ = coordinator.Join(context.Background())
	}()
	tmpl := &app.Template{Runtime: "nodejs24.x", Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}}}
	oldDone := make(chan struct{}, 1)
	newDone := make(chan struct{}, 1)
	coordinator.EnqueueWithStatus("alpha", tmpl, "img-1", nil, nil, func(error) { oldDone <- struct{}{} })
	<-docker.entered
	coordinator.EnqueueWithStatus("alpha", tmpl, "img-2", nil, nil, func(error) { newDone <- struct{}{} })
	release.Do(func() { close(docker.release) })
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	select {
	case <-oldDone:
		t.Fatal("older meaningful completion was not rejected")
	default:
	}
	select {
	case <-newDone:
	case <-time.After(2 * time.Second):
		t.Fatal("newer meaningful completion did not run")
	}
}

// newBlockingCoordinator builds a started coordinator whose worker resolve is
// blocked until release is called, and returns the coordinator plus a release
// app (idempotent) that unblocks the worker, cancels the lifecycle, and
// joins the workers.
func newBlockingCoordinator(t *testing.T) (*ServiceCoordinator, *coordinatorDocker, func()) {
	t.Helper()
	docker := &coordinatorDocker{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
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

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", tmpl, "img-1", nil)
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

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", tmpl, "img-1", nil)
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
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
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

// removalBlockingDocker blocks RemoveAppServiceContainers until release is
// closed, then reports ctx.Err if the caller's context expired first. It lets a
// test observe whether RemoveAndWait returned before the removal actually
// completed.
type removalBlockingDocker struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *removalBlockingDocker) ResolveServiceImage(
	_ context.Context, _ string, _ *app.Template, _ app.Service, image string,
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
func (d *removalBlockingDocker) RemoveAppServiceContainers(ctx context.Context, _ string) (int, error) {
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
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), time.Minute)
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
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), bound)
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
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), time.Minute)
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
// after Enqueue (as the live reconciler does when it replaces an app's
// template) cannot change what the worker converges. The fake captures the
// template passed to ResolveServiceImage; the mutation must not be visible.
func TestServiceCoordinatorSnapshotsTemplateAtEnqueue(t *testing.T) {
	docker := &snapshotDocker{}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Env:      map[string]string{"A": "1"},
		Secrets:  map[string]app.SecretRef{"S": "ref-1"},
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", tmpl, "img", []string{"PLAN=1"})

	// Mutate the caller's template while the request is queued/in flight.
	tmpl.Env["A"] = "mutated"
	tmpl.Secrets["S"] = "mutated-ref"
	tmpl.Services[0].Port = 9999
	tmpl.Services = append(tmpl.Services, app.Service{Name: "extra.js", Entrypoint: "extra.js", Port: 81, Replicas: 1})

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
	seenSecret   map[string]app.SecretRef
	seenServices []app.Service
}

func (d *snapshotDocker) ResolveServiceImage(
	_ context.Context, _ string, tmpl *app.Template, svc app.Service, image string,
) (runtime.ServiceImage, error) {
	d.mu.Lock()
	d.seenPort = svc.Port
	d.seenEnv = tmpl.Env
	d.seenSecret = tmpl.Secrets
	d.seenServices = append([]app.Service(nil), tmpl.Services...)
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
func (d *snapshotDocker) RemoveAppServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *snapshotDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }

// pauseProbeDocker records every ResolveServiceImage entry (app=image) and
// the peak concurrent count. It lets a test prove that a service pass enqueued
// during a RunExclusive housekeeping window never starts until the window ends.
type pauseProbeDocker struct {
	mu      sync.Mutex
	active  int
	max     int
	entered chan string
}

func (d *pauseProbeDocker) ResolveServiceImage(
	_ context.Context, fnName string, _ *app.Template, _ app.Service, image string,
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
func (d *pauseProbeDocker) RemoveAppServiceContainers(context.Context, string) (int, error) {
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
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	// The initial desired state settles before housekeeping starts.
	coordinator.Enqueue("alpha", tmpl, "img-1", nil)
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
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)
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

	tmpl := &app.Template{
		Runtime:  "nodejs24.x",
		Services: []app.Service{{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
	coordinator.Enqueue("alpha", tmpl, "img-1", nil)
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

// pendingServiceDocker is a test-local Docker fake for the coordinator's
// pending/resume contract: StartService reports ErrServicePending on its FIRST
// call and succeeds on every later call, so the first pass of a one-replica
// service ends pending and a later pass ends terminally. removeEntered is
// closed once an app removal enters, which then parks until removeRelease, so
// a test can observe a removal in progress. resolveErr (set only while no pass
// is active) makes a later source resolution fail terminally.
type pendingServiceDocker struct {
	startCalls    atomic.Int32
	resolveCalls  atomic.Int32
	pendingSignal chan struct{}
	removeEntered chan struct{}
	removeRelease chan struct{}
	removeOnce    sync.Once
	resolveErr    error
}

func newPendingServiceDocker() *pendingServiceDocker {
	return &pendingServiceDocker{
		pendingSignal: make(chan struct{}, 1),
		removeEntered: make(chan struct{}),
		removeRelease: make(chan struct{}),
	}
}

func (d *pendingServiceDocker) ResolveServiceImage(
	_ context.Context, _ string, _ *app.Template, _ app.Service, image string,
) (runtime.ServiceImage, error) {
	d.resolveCalls.Add(1)
	if err := d.resolveErr; err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: image, ID: image}, nil
}

func (d *pendingServiceDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	if d.startCalls.Add(1) == 1 {
		select {
		case d.pendingSignal <- struct{}{}:
		default:
		}
		return "", ErrServicePending
	}
	return "id-terminal", nil
}

func (d *pendingServiceDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *pendingServiceDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *pendingServiceDocker) RemoveAppServiceContainers(ctx context.Context, _ string) (int, error) {
	d.removeOnce.Do(func() { close(d.removeEntered) })
	select {
	case <-d.removeRelease:
		return 1, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
func (d *pendingServiceDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }

// enqueueMeaningfulWithHooks publishes one meaningful leased request through
// the coordinator's single enqueue funnel and returns its done channel, so a
// test can observe exactly when the logical request concludes.
func enqueueMeaningfulWithHooks(
	c *ServiceCoordinator, name string, tmpl *app.Template, image string,
	lease *runtime.ImageLease, onComplete func(error), onSettled func(),
) chan struct{} {
	return c.enqueue(&serviceRequest{
		name: name, tmpl: cloneServiceTemplate(tmpl), image: image,
		lease: lease, onComplete: onComplete, onSettled: onSettled,
	})
}

// TestServiceCoordinatorPendingReleasesWorkerAndNotifyCompletesOnce pins the
// external-pending lifecycle: a first pass whose StartService reports
// ErrServicePending ends WITHOUT concluding the logical request (no completion,
// no settlement, done stays open) while releasing the worker (Wait returns) and
// the per-pass image lease; an explicit Notify then resumes the SAME request,
// which concludes terminally with its callback and settlement hook running
// exactly once.
func TestServiceCoordinatorPendingReleasesWorkerAndNotifyCompletesOnce(t *testing.T) {
	docker := newPendingServiceDocker()
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	mgr := &runtime.Manager{}
	lease, err := mgr.AcquireImageLease("relay-app-pending:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	var completeCount, settleCount atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "relay-app-pending:v1", lease,
		func(err error) {
			completeCount.Add(1)
			select {
			case completed <- err:
			default:
			}
		},
		func() { settleCount.Add(1) },
	)

	// The first pass ends pending and must release its worker.
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("Wait did not return while the request was pending: %v", err)
	}

	// Pending concludes nothing: the logical request, its callbacks, and its
	// waiter are retained; only the per-pass image lease is released.
	if got := docker.startCalls.Load(); got != 1 {
		t.Fatalf("StartService calls = %d, want 1 (no resume without an explicit wake)", got)
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("completion calls while pending = %d, want 0", got)
	}
	if got := settleCount.Load(); got != 0 {
		t.Fatalf("settlement calls while pending = %d, want 0 (settlement is delayed to conclusion)", got)
	}
	select {
	case <-done:
		t.Fatal("pending request's done channel was closed (it must stay open for resume)")
	default:
	}
	if got := mgr.LeaseCount("relay-app-pending:v1"); got != 0 {
		t.Fatalf("lease count after the pending pass = %d, want 0 (released per pass)", got)
	}

	// Notify resumes the retained request; only its terminal pass concludes it.
	coordinator.Notify("alpha")
	select {
	case got := <-completed:
		if got != nil {
			t.Fatalf("terminal completion err = %v, want nil", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal completion did not fire after Notify resumed the pending request")
	}
	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls = %d, want 2 (Notify ran exactly one second pass)", got)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want exactly 1", got)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done channel still open after the terminal completion")
	}
	select {
	case <-completed:
		t.Fatal("completion callback fired more than once")
	default:
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls = %d, want exactly 1 after conclusion", got)
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls after join = %d, want exactly 1", got)
	}
	if got := docker.startCalls.Load(); got != 2 {
		t.Fatalf("StartService calls after join = %d, want 2 (no stray extra pass)", got)
	}
}

// TestServiceCoordinatorMidPassDoubleEnqueueSupersedesIntermediateRequest
// pins the mid-pass double-enqueue race: while one request executes, two newer
// enqueues arrive. The intermediate queued request can never run, so it must be
// concluded exactly once at the second enqueue — done closed, admitted lease
// released, settlement hook run once — and its completion callback must never
// fire; the executing request's stale completion is likewise suppressed, and
// only the newest request's callback runs.
func TestServiceCoordinatorMidPassDoubleEnqueueSupersedesIntermediateRequest(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	var release sync.Once
	defer func() {
		release.Do(func() { close(docker.release) })
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	mgr := &runtime.Manager{}
	lease1, err := mgr.AcquireImageLease("relay-app-mid:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lease2, err := mgr.AcquireImageLease("relay-app-mid:v2")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	var complete1, complete2, settle1, settle2 atomic.Int32
	completed3 := make(chan error, 1)

	// req1 executes, parked inside its resolve.
	done1 := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "relay-app-mid:v1", lease1,
		func(error) { complete1.Add(1) }, func() { settle1.Add(1) })
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never entered")
	}

	// req2 queues behind the executing pass; req3 then supersedes req2 before
	// req2 can ever run.
	done2 := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "relay-app-mid:v2", lease2,
		func(error) { complete2.Add(1) }, func() { settle2.Add(1) })
	coordinator.EnqueueWithStatus("alpha", tmpl, "img-3", nil, nil, func(err error) {
		select {
		case completed3 <- err:
		default:
		}
	})

	// The intermediate request is concluded synchronously by the supersession:
	// done closed, lease released, settlement exactly once, completion never.
	select {
	case <-done2:
	default:
		t.Fatal("intermediate request's done channel was not closed on supersession")
	}
	if got := mgr.LeaseCount("relay-app-mid:v2"); got != 0 {
		t.Fatalf("intermediate request lease count = %d, want 0 (released on supersession)", got)
	}
	if got := settle2.Load(); got != 1 {
		t.Fatalf("intermediate settlement calls = %d, want exactly 1", got)
	}
	if got := complete2.Load(); got != 0 {
		t.Fatalf("intermediate completion calls = %d, want 0 (suppressed)", got)
	}

	// Release the executing pass: it must not fire its stale completion, and
	// the newest request runs to a terminal result.
	release.Do(func() { close(docker.release) })
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	select {
	case got := <-completed3:
		if got != nil {
			t.Fatalf("newest completion err = %v, want nil", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("newest request's completion did not fire")
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := complete1.Load(); got != 0 {
		t.Fatalf("executing request completion calls = %d, want 0 (superseded)", got)
	}
	if got := settle1.Load(); got != 1 {
		t.Fatalf("executing request settlement calls = %d, want exactly 1", got)
	}
	if got := complete2.Load(); got != 0 {
		t.Fatalf("intermediate request completion calls after join = %d, want 0", got)
	}
	if got := settle2.Load(); got != 1 {
		t.Fatalf("intermediate request settlement calls after join = %d, want exactly 1", got)
	}
	select {
	case <-done1:
	default:
		t.Fatal("superseded executing request's done channel was not closed")
	}
}

// TestServiceCoordinatorObservationDoesNotReplacePendingRequestOrAdvanceDeadline
// pins callback/deadline ownership while a meaningful request is pending: a
// periodic observation enqueued under a future wake deadline is dropped without
// replacing the pending request's callbacks, without starting a pass, and
// without advancing (or clearing) the scheduled deadline. The explicit wake
// then concludes the ORIGINAL meaningful request — through a terminal FAILURE —
// with its own callback, never the observation's, and the concluded request's
// deadline is cleared.
func TestServiceCoordinatorObservationDoesNotReplacePendingRequestOrAdvanceDeadline(t *testing.T) {
	docker := newPendingServiceDocker()
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})

	var meaningfulComplete, meaningfulSettle atomic.Int32
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-meaningful", nil,
		func(err error) {
			meaningfulComplete.Add(1)
			select {
			case completed <- err:
			default:
			}
		},
		func() { meaningfulSettle.Add(1) },
	)

	// The first pass ends pending.
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("Wait did not return while the request was pending: %v", err)
	}

	// A future wake deadline is installed for the pending request.
	deadline := time.Now().Add(time.Hour)
	coordinator.Schedule("alpha", deadline)
	deadlineOf := func() time.Time {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
		state := coordinator.states["alpha"]
		if state == nil {
			return time.Time{}
		}
		return state.deadline
	}
	if got := deadlineOf(); !got.Equal(deadline) {
		t.Fatalf("scheduled deadline = %v, want %v", got, deadline)
	}

	// A periodic observation arrives while the meaningful request is pending.
	var obsComplete, obsSettle atomic.Int32
	coordinator.EnqueueObservationWithHooksLeased("alpha", tmpl, "img-observation", nil, nil,
		ServiceRequestHooks{
			OnComplete: func(error) { obsComplete.Add(1) },
			OnSettled:  func() { obsSettle.Add(1) },
		})

	// The observation is dropped as logical work: it settles once, does not
	// complete while the request is pending, does not start a pass, and leaves
	// the pending request's deadline untouched. Its terminal callback is
	// composed onto the retained request instead of running now.
	if got := obsSettle.Load(); got != 1 {
		t.Fatalf("observation settlement calls = %d, want exactly 1 (dropped request concluded)", got)
	}
	if got := obsComplete.Load(); got != 0 {
		t.Fatalf("observation completion calls = %d, want 0 (never ran)", got)
	}
	if got := deadlineOf(); !got.Equal(deadline) {
		t.Fatalf("deadline after observation = %v, want the unchanged %v", got, deadline)
	}
	obsWaitCtx, obsWaitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := coordinator.Wait(obsWaitCtx); err != nil {
		obsWaitCancel()
		t.Fatalf("wait after observation: %v", err)
	}
	obsWaitCancel()
	if got := docker.startCalls.Load(); got != 1 {
		t.Fatalf("StartService calls after observation = %d, want 1 (observation must not resume/replace)", got)
	}
	if got := meaningfulComplete.Load(); got != 0 {
		t.Fatalf("meaningful completion calls while pending = %d, want 0", got)
	}

	// The explicit wake concludes the ORIGINAL meaningful request through a
	// terminal failure. The composed observation completion runs once with that
	// same terminal result; the observation never owned a separate status
	// transition.
	resolveErr := errors.New("registry unavailable")
	docker.resolveErr = resolveErr
	coordinator.Notify("alpha")
	select {
	case got := <-completed:
		if !errors.Is(got, resolveErr) {
			t.Fatalf("terminal completion err = %v, want the meaningful request's resolve failure", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal completion did not fire after Notify resumed the pending request")
	}
	// The waiter is released only after the callbacks and settlement finished.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("meaningful request's done channel still open after terminal completion")
	}
	// The resumed pass ran exactly once: it resolved the source (and failed
	// there), so StartService was never reached a second time and no stray
	// extra pass followed.
	if got := docker.resolveCalls.Load(); got != 2 {
		t.Fatalf("resolve attempts = %d, want 2 (pending pass + exactly one resumed pass)", got)
	}
	if got := docker.startCalls.Load(); got != 1 {
		t.Fatalf("StartService calls = %d, want 1 (the resumed pass failed before starting)", got)
	}
	if got := meaningfulComplete.Load(); got != 1 {
		t.Fatalf("meaningful completion calls = %d, want exactly 1", got)
	}
	if got := obsComplete.Load(); got != 1 {
		t.Fatalf("observation completion calls after terminal = %d, want exactly 1 (composed observer)", got)
	}
	// The concluded request's deadline belongs to it and is cleared with it.
	if got := deadlineOf(); !got.IsZero() {
		t.Fatalf("deadline after conclusion = %v, want cleared", got)
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := meaningfulSettle.Load(); got != 1 {
		t.Fatalf("meaningful settlement calls = %d, want exactly 1", got)
	}
	if got := meaningfulComplete.Load(); got != 1 {
		t.Fatalf("meaningful completion calls after join = %d, want exactly 1", got)
	}
}

// TestServiceCoordinatorRemoveAndWaitSupersedesPendingRequest pins the app
// removal edge: a removal request is non-meaningful for status authority but
// must NOT be mistaken for a redundant observation — it supersedes a pending
// meaningful request (concluding it once, without its stale completion), runs
// the Docker removal, and RemoveAndWait waits deterministically until that
// removal actually returns.
func TestServiceCoordinatorRemoveAndWaitSupersedesPendingRequest(t *testing.T) {
	docker := newPendingServiceDocker()
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), time.Minute)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	mgr := &runtime.Manager{}
	lease, err := mgr.AcquireImageLease("relay-app-removal:v1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	var completeCount, settleCount atomic.Int32
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "relay-app-removal:v1", lease,
		func(error) { completeCount.Add(1) }, func() { settleCount.Add(1) })

	// The first pass ends pending: worker and lease released, request retained.
	select {
	case <-docker.pendingSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never reached StartService")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("Wait did not return while the request was pending: %v", err)
	}
	if got := mgr.LeaseCount("relay-app-removal:v1"); got != 0 {
		t.Fatalf("lease count after the pending pass = %d, want 0", got)
	}
	select {
	case <-done:
		t.Fatal("pending request's done channel was closed before the removal")
	default:
	}

	// The removal supersedes the pending meaningful request and parks in Docker.
	removeReturned := make(chan struct{})
	go func() {
		defer close(removeReturned)
		coordinator.RemoveAndWait("alpha")
	}()
	select {
	case <-docker.removeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the removal never entered: it was mistaken for a redundant observation")
	}
	select {
	case <-done:
	default:
		t.Fatal("pending request's done channel not closed by removal supersession")
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("pending request settlement calls = %d, want exactly 1 (superseded by removal)", got)
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("pending request completion calls = %d, want 0 (suppressed by removal)", got)
	}

	// RemoveAndWait must still be waiting on the parked removal.
	select {
	case <-removeReturned:
		t.Fatal("RemoveAndWait returned while the removal was still parked in Docker")
	default:
	}
	close(docker.removeRelease)
	select {
	case <-removeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAndWait did not return after the removal completed")
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("pending request settlement calls after join = %d, want exactly 1", got)
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("pending request completion calls after join = %d, want 0", got)
	}
}

// TestServiceCoordinatorStatusLessDesiredSupersedesExecutingMeaningful pins the
// authority split: a status-less desired update advances the desired-operation
// fence without advancing meaningful status authority. An executing meaningful
// pass superseded by it must not commit its old-generation stop, and its
// completion callback must be suppressed. The new status-less desired state then
// runs and converges.
func TestServiceCoordinatorStatusLessDesiredSupersedesExecutingMeaningful(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "alpha", "service.js", "img-old", 80, 0)
	services := NewServiceReconciler(f, nil, testutil.DiscardLogger(), time.Minute)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	oldCompleted := make(chan error, 1)
	coordinator.EnqueueWithStatus("alpha", tmpl, "img-meaningful", nil, nil,
		func(err error) { oldCompleted <- err })
	coordinator.mu.Lock()
	operationID := coordinator.operationIDs["alpha"]
	coordinator.mu.Unlock()

	// Park the first pass at the post-start/pre-commit boundary, then publish a
	// status-less desired update. It must advance the desired-operation fence
	// and supersede the older request.
	firstIDs := make(chan string, 1)
	f.mu.Lock()
	f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
		select {
		case firstIDs <- id:
			coordinator.Enqueue("alpha", tmpl, "img-statusless", nil)
		default:
		}
	}
	f.mu.Unlock()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	select {
	case err := <-oldCompleted:
		t.Fatalf("superseded meaningful completion ran with err=%v, want suppression", err)
	default:
	}

	coordinator.mu.Lock()
	gotOperation := coordinator.operationIDs["alpha"]
	coordinator.mu.Unlock()
	if gotOperation == operationID {
		t.Fatalf("operation fence = %d, want the status-less desired update to advance it past %d",
			gotOperation, operationID)
	}

	var firstID string
	select {
	case firstID = <-firstIDs:
	default:
		t.Fatal("the first meaningful pass never reached StartService")
	}

	f.mu.Lock()
	stops := append([]string(nil), f.stops...)
	f.mu.Unlock()
	if !containsString(stops, firstID) {
		t.Fatalf("stops = %v, want the superseded pass's provisional %q cleaned up", stops, firstID)
	}
	if !containsString(stops, "old-1") {
		t.Fatalf("stops = %v, want the new status-less pass to replace old-1", stops)
	}
	started := f.lastStartedFor("alpha", "service.js")
	if started == nil || started.image != "img-statusless" {
		t.Fatalf("running service = %+v, want the status-less desired image", started)
	}
}

// TestServiceCoordinatorStatusLessEnqueueAfterTerminalDecisionKeepsCallback
// pins callback linearization: once the terminal decision is made under the
// mutex, a newer status-less desired update arriving before callback dispatch
// must not suppress the already-authorized completion.
func TestServiceCoordinatorStatusLessEnqueueAfterTerminalDecisionKeepsCallback(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	close(docker.release)
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	decided := make(chan struct{})
	releaseDecision := make(chan struct{})
	var decisionOnce sync.Once
	coordinator.terminalDecided = func() {
		decisionOnce.Do(func() {
			close(decided)
			<-releaseDecision
		})
	}

	tmpl := serviceTemplate("", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	completed := make(chan error, 1)
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(err error) { completed <- err }, nil)

	select {
	case <-decided:
	case <-time.After(2 * time.Second):
		t.Fatal("the terminal decision seam never fired")
	}
	// The terminal decision is fixed. A status-less desired update now advances
	// operationIDs before the callback is dispatched.
	coordinator.Enqueue("alpha", tmpl, "img-2", nil)
	close(releaseDecision)

	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("terminal completion err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the linearized terminal completion was lost")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concluded request's done channel stayed open")
	}
	docker.mu.Lock()
	resolves := append([]string(nil), docker.resolves...)
	docker.mu.Unlock()
	if !containsString(resolves, "alpha=img-2") {
		t.Fatalf("resolves = %v, want the status-less follow-up pass to run", resolves)
	}
}

// TestServiceCoordinatorShutdownAfterTerminalDecisionSuppressesCallback pins
// the shutdown side: cancellation between the terminal decision and callback
// dispatch suppresses the status callback, while the settlement cleanup still
// runs and releases the waiter.
func TestServiceCoordinatorShutdownAfterTerminalDecisionSuppressesCallback(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	close(docker.release)
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	defer func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	decided := make(chan struct{})
	releaseDecision := make(chan struct{})
	var decisionOnce sync.Once
	coordinator.terminalDecided = func() {
		decisionOnce.Do(func() {
			close(decided)
			<-releaseDecision
		})
	}

	tmpl := serviceTemplate("", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	var completeCount, settleCount atomic.Int32
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(error) { completeCount.Add(1) },
		func() { settleCount.Add(1) },
	)

	select {
	case <-decided:
	case <-time.After(2 * time.Second):
		t.Fatal("the terminal decision seam never fired")
	}
	cancel()
	close(releaseDecision)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after shutdown settlement")
	}
	if got := completeCount.Load(); got != 0 {
		t.Fatalf("completion calls after shutdown = %d, want 0", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls after shutdown = %d, want 1", got)
	}
}

// TestServiceCoordinatorShutdownDuringCompletionSuppressesComposedObserver pins
// the per-callback liveness check: a shutdown that begins while the completion
// callback is still blocked must suppress the composed observation callback
// that runs after it, while settlement and waiter closure still complete.
func TestServiceCoordinatorShutdownDuringCompletionSuppressesComposedObserver(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	completeEntered := make(chan struct{})
	releaseComplete := make(chan struct{})
	var dockerOnce, completeOnce sync.Once
	releaseDocker := func() { dockerOnce.Do(func() { close(docker.release) }) }
	releaseCompletion := func() { completeOnce.Do(func() { close(releaseComplete) }) }
	defer func() {
		cancel()
		releaseDocker()
		releaseCompletion()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	var completeCount, observerCount, settleCount atomic.Int32
	done := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil,
		func(error) {
			completeCount.Add(1)
			close(completeEntered)
			<-releaseComplete
		},
		func() { settleCount.Add(1) },
	)
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the pass never entered the backend")
	}

	// A future retry deadline keeps the coalesced observation from setting the
	// dirty-rerun bit, so this same pass is the terminal one that dispatches
	// both the completion and the composed observer callback.
	coordinator.Schedule("alpha", time.Now().Add(time.Hour))
	coordinator.EnqueueObservationWithHooksLeased("alpha", tmpl, "img-1", nil, nil,
		ServiceRequestHooks{OnComplete: func(error) { observerCount.Add(1) }})

	releaseDocker()
	select {
	case <-completeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the completion callback never ran")
	}

	// Shutdown begins while the completion callback is still blocked.
	cancel()
	releaseCompletion()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("done did not close after settlement")
	}
	if got := completeCount.Load(); got != 1 {
		t.Fatalf("completion calls = %d, want 1", got)
	}
	if got := observerCount.Load(); got != 0 {
		t.Fatalf("composed observer calls after shutdown = %d, want 0", got)
	}
	if got := settleCount.Load(); got != 1 {
		t.Fatalf("settlement calls after shutdown = %d, want 1", got)
	}
}

// TestServiceCoordinatorCancelPendingKeepsIdleBarrierThroughSettlement pins
// the shutdown race: cancelPending runs a queued request's settlement hook
// outside the mutex while another worker still executes. If that worker
// finishes and broadcasts idle during a blocking hook, Wait/RunExclusive must
// remain busy until the cancellation settlement and waiter closure finish.
func TestServiceCoordinatorCancelPendingKeepsIdleBarrierThroughSettlement(t *testing.T) {
	docker := &coordinatorDocker{entered: make(chan struct{}, 8), release: make(chan struct{})}
	services := NewServiceReconciler(docker, nil, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)

	settleEntered := make(chan struct{})
	releaseSettle := make(chan struct{})
	var releaseDockerOnce, releaseSettleOnce sync.Once
	releaseDocker := func() { releaseDockerOnce.Do(func() { close(docker.release) }) }
	releaseSettlement := func() { releaseSettleOnce.Do(func() { close(releaseSettle) }) }
	defer func() {
		cancel()
		releaseSettlement()
		releaseDocker()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}()

	tmpl := serviceTemplate("", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	// The first request executes and parks inside ResolveServiceImage.
	enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-1", nil, nil, nil)
	select {
	case <-docker.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the executing request never entered the backend")
	}

	// A queued replacement is cancelled by shutdown and its settlement hook
	// blocks, representing a real resource-release step.
	cancelledDone := enqueueMeaningfulWithHooks(coordinator, "alpha", tmpl, "img-2", nil, nil,
		func() {
			close(settleEntered)
			<-releaseSettle
		})

	cancel()
	select {
	case <-settleEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelPending did not run the queued request's settlement")
	}

	// Park a waiter while the cancellation settlement is blocked.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- coordinator.Wait(waitCtx) }()

	// Let the executing worker finish and broadcast idle. The waiter must stay
	// parked because cancellation settlement is still in flight.
	releaseDocker()
	select {
	case err := <-waitDone:
		t.Fatalf("Wait returned before cancellation settlement completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	releaseSettlement()
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancellation settlement completed")
	}
	select {
	case <-cancelledDone:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request's waiter was not released after settlement")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

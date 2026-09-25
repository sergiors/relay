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

// ctxRecordDocker is a reconciler.Docker fake that records the context every
// method received and can block selected methods until their context is done. It
// lets the worker-level tests assert the context passed DOWN into Apply and the
// standalone operations, without a daemon.
type ctxRecordDocker struct {
	mu           sync.Mutex
	observations []ctxObservation
	block        map[string]bool // method name -> block until ctx done
	entered      map[string]chan struct{}
	containers   []runtime.ServiceContainer
}

type ctxObservation struct {
	op          string
	hasDeadline bool
	deadline    time.Time
	at          time.Time
	err         error
}

func newCtxRecordDocker() *ctxRecordDocker {
	return &ctxRecordDocker{
		block:   map[string]bool{},
		entered: map[string]chan struct{}{},
	}
}

func (d *ctxRecordDocker) record(op string, ctx context.Context) {
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	if d.entered[op] == nil {
		d.entered[op] = make(chan struct{}, 1)
	}
	d.observations = append(d.observations, ctxObservation{
		op: op, hasDeadline: ok, deadline: dl, at: time.Now(), err: ctx.Err(),
	})
	ch := d.entered[op]
	d.mu.Unlock()
	// Non-blocking signal on a buffered channel: safe if record is somehow
	// called more than once for the same op (no double-close).
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (d *ctxRecordDocker) maybeBlock(method string, ctx context.Context) bool {
	d.mu.Lock()
	block := d.block[method]
	d.mu.Unlock()
	if block {
		<-ctx.Done()
		return true
	}
	return false
}

func (d *ctxRecordDocker) ResolveServiceImage(
	ctx context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	d.record("resolve", ctx)
	if d.maybeBlock("resolve", ctx) {
		return runtime.ServiceImage{}, ctx.Err()
	}
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}

func (d *ctxRecordDocker) StartService(ctx context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	d.record("start", ctx)
	if d.maybeBlock("start", ctx) {
		return "", ctx.Err()
	}
	return "id-1", nil
}

func (d *ctxRecordDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.record("list", ctx)
	if d.maybeBlock("list", ctx) {
		return nil, ctx.Err()
	}
	return append([]runtime.ServiceContainer(nil), d.containers...), nil
}

func (d *ctxRecordDocker) StopServiceContainers(ctx context.Context, _ []runtime.ServiceContainer) error {
	d.record("stop", ctx)
	return nil
}

func (d *ctxRecordDocker) RemoveFunctionServiceContainers(ctx context.Context, _ string) (int, error) {
	d.record("remove", ctx)
	if d.maybeBlock("remove", ctx) {
		return 0, ctx.Err()
	}
	return 0, nil
}

func (d *ctxRecordDocker) NetworkExists(ctx context.Context, _ string) (bool, error) {
	d.record("network", ctx)
	return true, nil
}

func (d *ctxRecordDocker) forOp(op string) []ctxObservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []ctxObservation
	for _, o := range d.observations {
		if o.op == op {
			out = append(out, o)
		}
	}
	return out
}

func (d *ctxRecordDocker) waitEntered(t *testing.T, op string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		ch := d.entered[op]
		d.mu.Unlock()
		if ch != nil {
			select {
			case <-ch:
				return
			default:
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q to be entered", op)
}

// tryEntered reports whether an un-consumed entry signal is pending for op,
// without blocking. It reads the entered channel under the same mutex record
// writes it, so it is race-free alongside concurrent Docker calls.
func (d *ctxRecordDocker) tryEntered(op string) bool {
	d.mu.Lock()
	ch := d.entered[op]
	d.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// waitCount waits until at least n calls to op have been recorded. Unlike
// waitEntered it does not consume an entry signal, so a subsequent waitEntered
// still observes the same op.
func (d *ctxRecordDocker) waitCount(t *testing.T, op string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(d.forOp(op)) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d %q call(s)", n, op)
}

// applyServiceTemplate is a single entrypoint service whose convergence touches
// resolve, env, and start — the ops the live/startup paths must bound.
func applyServiceTemplate() *function.Template {
	return &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
}

// startCoordinatorFixture builds a started coordinator over a fresh fake, and
// returns the coordinator plus a stop function that cancels the lifecycle and
// joins the workers. The worker lifecycle is the parent in production; here a
// test-owned context stands in for it.
func startCoordinatorFixture(t *testing.T, svcCtrl *reconciler.ServiceReconciler) (*reconciler.ServiceCoordinator, func()) {
	t.Helper()
	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	lifecycle, cancel := context.WithCancel(context.Background())
	coordinator.Start(lifecycle)
	stop := func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := coordinator.Join(joinCtx); err != nil {
			t.Fatalf("join: %v", err)
		}
	}
	return coordinator, stop
}

// TestEnqueueLiveServicesBoundsEachOperationToReconcileTimeout pins the live
// path: enqueueLiveServices publishes the desired state, the coordinator's
// workers converge it with the coordinator LIFECYCLE context (not a pass-wide
// budget), and the ServiceReconciler derives a fresh reconcileTimeout bound for
// each normal Docker operation. A long pre-build phase therefore cannot consume
// the post-build start's deadline.
func TestEnqueueLiveServicesBoundsEachOperationToReconcileTimeout(t *testing.T) {
	fake := newCtxRecordDocker()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)

	reg := &runner.Registry{}
	reg.Set(nil)

	enqueueLiveServices(coordinator, reg, "fn", t.TempDir(), applyServiceTemplate(), "img")
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	for _, op := range []string{"resolve", "start"} {
		obs := fake.forOp(op)
		if len(obs) == 0 {
			t.Fatalf("no %s observation; live converge did not run", op)
		}
		o := obs[0]
		if !o.hasDeadline {
			t.Fatalf("%s carried no deadline; want the reconcileTimeout bound", op)
		}
		if rem := o.deadline.Sub(o.at); rem <= 0 || rem > reconcileTimeout {
			t.Fatalf("%s remaining budget = %v, want a fresh (0, %v]", op, rem, reconcileTimeout)
		}
	}
	stop()
}

// TestEnqueueLiveServicesRootedInLifecycle proves the live path stays rooted in
// the worker lifecycle: cancelling the lifecycle (shutdown) cancels the
// in-flight operation promptly and Join drains the workers, even though each
// operation carries a 30s bound.
func TestEnqueueLiveServicesRootedInLifecycle(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["list"] = true
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)

	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	reg := &runner.Registry{}
	reg.Set(nil)
	enqueueLiveServices(coordinator, reg, "fn", t.TempDir(), applyServiceTemplate(), "img")

	fake.waitEntered(t, "list")
	start := time.Now()
	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join after lifecycle cancel: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("live converge took %v to cancel; want prompt", elapsed)
	}
}

// TestEnqueueLiveServicesUsesRegistryPreparedEnv proves the live helper still
// sources the runtime plan env from the current registry entry (its documented
// behavior, unchanged by the context rework).
func TestEnqueueLiveServicesUsesRegistryPreparedEnv(t *testing.T) {
	// A secret-free template with one env var lets the prepared plan env show up
	// in the started container's spec. The fake records nothing about env, so
	// this pins that the helper tolerates both a present and an absent registry
	// entry.
	fake := newCtxRecordDocker()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)

	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{runner.NewPrepared(
		function.Function{Name: "fn", Template: applyServiceTemplate()},
		&runtime.Prepared{Name: "fn", Image: "img", Env: []string{"PLAN=1"}},
		nil,
	)})

	enqueueLiveServices(coordinator, reg, "fn", t.TempDir(), applyServiceTemplate(), "img")
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := len(fake.forOp("start")); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}

	// Absent registry entry: nil Prepared falls back to no plan env and still
	// converges.
	fake2 := newCtxRecordDocker()
	svcCtrl2 := reconciler.NewServiceReconciler(fake2, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	coordinator2, stop2 := startCoordinatorFixture(t, svcCtrl2)
	enqueueLiveServices(coordinator2, reg, "missing", t.TempDir(), applyServiceTemplate(), "img")
	if err := coordinator2.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := len(fake2.forOp("start")); got != 1 {
		t.Fatalf("starts with no registry entry = %d, want 1", got)
	}
	stop()
	stop2()
}

// TestEnqueueStartupServicesApplyUsesLifecycleNotPassBudget proves the startup
// path enqueues convergence that stays rooted in the worker lifecycle:
// cancelling the lifecycle cancels an in-flight startup converge promptly rather
// than letting it run out the reconcileTimeout.
func TestEnqueueStartupServicesApplyUsesLifecycleNotPassBudget(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["list"] = true
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmpl := applyServiceTemplate()
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha"}, nil),
	}

	enqueueStartupServices(prepared, coordinator, logger)
	fake.waitEntered(t, "list")

	start := time.Now()
	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join after lifecycle cancel: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("startup converge took %v to cancel; want prompt", elapsed)
	}
}

// TestEnqueueStartupServicesStandaloneRemoveBounded pins the removal published
// by enqueueStartupServices: an unavailable function whose template declares no
// services is removed on ITS OWN fresh reconcileTimeout bound (derived by the
// coordinator, not the unbounded lifecycle), and startup does not block on it
// (EnqueueRemove, not RemoveAndWait).
func TestEnqueueStartupServicesStandaloneRemoveBounded(t *testing.T) {
	fake := newCtxRecordDocker()
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)
	defer stop()

	tmpl := &function.Template{Runtime: "node24"} // no services
	fn := function.Function{Name: "broken", Template: tmpl}
	prepared := []*runner.PreparedFunction{runner.NewUnavailable(fn)}

	enqueueStartupServices(prepared, coordinator, logger)

	// Wait for the published removal to run and settle.
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	obs := fake.forOp("remove")
	if len(obs) != 1 {
		t.Fatalf("remove observations = %d, want 1", len(obs))
	}
	if !obs[0].hasDeadline {
		t.Fatal("startup remove carried no deadline; want a reconcileTimeout bound")
	}
	if rem := obs[0].deadline.Sub(obs[0].at); rem <= 0 || rem > reconcileTimeout {
		t.Fatalf("startup remove remaining budget = %v, want a fresh (0, %v]", rem, reconcileTimeout)
	}
}

// TestEnqueueStartupServicesStandaloneRemoveNonBlocking proves the startup
// removal path does not block startup: enqueueStartupServices returns while a
// removal is parked in the Docker fake, and the housekeeping barrier later waits
// for it.
func TestEnqueueStartupServicesStandaloneRemoveNonBlocking(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["remove"] = true
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)
	defer stop()

	// The removal's remove call blocks until the coordinator lifecycle is
	// cancelled, so enqueueStartupServices must return immediately.
	tmpl := &function.Template{Runtime: "node24"} // no services
	fn := function.Function{Name: "broken", Template: tmpl}
	prepared := []*runner.PreparedFunction{runner.NewUnavailable(fn)}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		enqueueStartupServices(prepared, coordinator, logger)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueStartupServices blocked on the removal; startup must not wait")
	}
	fake.waitEntered(t, "remove")
}

// TestEnqueueStartupServicesUnavailableWithServicesSkips proves the other
// standalone branch: an unavailable function that still declares services is
// left alone (no remove, no enqueue), so its possibly-serving containers are
// preserved.
func TestEnqueueStartupServicesUnavailableWithServicesSkips(t *testing.T) {
	fake := newCtxRecordDocker()
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)
	defer stop()

	tmpl := applyServiceTemplate()
	fn := function.Function{Name: "broken", Template: tmpl}
	prepared := []*runner.PreparedFunction{runner.NewUnavailable(fn)}

	enqueueStartupServices(prepared, coordinator, logger)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// The unavailable-with-services branch must neither remove nor apply: the
	// coordinator has no work, so no container call happens at all (the orphan
	// sweep is the housekeeping pass' job, not this helper's).
	for _, op := range []string{"remove", "resolve", "start", "list"} {
		if got := len(fake.forOp(op)); got != 0 {
			t.Fatalf("unavailable-with-services must not call %q, got %d call(s)", op, got)
		}
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

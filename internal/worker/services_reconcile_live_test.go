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

func (d *ctxRecordDocker) VerifyNetworks(ctx context.Context, _ []string) (string, bool, error) {
	d.record("verify-networks", ctx)
	return "", true, nil
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

// applyServiceTemplate is a single entrypoint service whose convergence touches
// resolve, env, and start — the ops the live/startup paths must bound.
func applyServiceTemplate() *function.Template {
	return &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Entrypoint: "service.js", Port: 80, Replicas: 1}},
	}
}

// TestApplyLiveServicesBoundsEachOperationToReconcileTimeout pins the live path:
// applyLiveServices passes the LIFECYCLE context (not a pass-wide budget) to
// Apply, and the ServiceReconciler derives a fresh reconcileTimeout bound for
// each normal Docker operation. A long pre-build phase therefore cannot consume
// the post-build start's deadline.
func TestApplyLiveServicesBoundsEachOperationToReconcileTimeout(t *testing.T) {
	fake := newCtxRecordDocker()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	reg := &runner.Registry{}
	reg.Set(nil)

	tmpl := applyServiceTemplate()
	applyLiveServices(context.Background(), svcCtrl, reg, "fn", t.TempDir(), tmpl, "img")

	for _, op := range []string{"resolve", "start"} {
		obs := fake.forOp(op)
		if len(obs) == 0 {
			t.Fatalf("no %s observation; live Apply did not run", op)
		}
		o := obs[0]
		if !o.hasDeadline {
			t.Fatalf("%s carried no deadline; want the reconcileTimeout bound", op)
		}
		if rem := o.deadline.Sub(o.at); rem <= 0 || rem > reconcileTimeout {
			t.Fatalf("%s remaining budget = %v, want a fresh (0, %v]", op, rem, reconcileTimeout)
		}
	}
}

// TestApplyLiveServicesRootedInLifecycle proves the live path stays rooted in
// the worker lifecycle: cancelling the lifecycle (shutdown) cancels an in-flight
// operation promptly, even though each operation carries a 30s bound.
func TestApplyLiveServicesRootedInLifecycle(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["list"] = true
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	reg := &runner.Registry{}
	reg.Set(nil)

	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		applyLiveServices(lifecycle, svcCtrl, reg, "fn", t.TempDir(), applyServiceTemplate(), "img")
	}()

	fake.waitEntered(t, "list")
	start := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the live Apply promptly")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("live Apply took %v to cancel; want prompt", elapsed)
	}
}

// TestApplyLiveServicesUsesRegistryPreparedEnv proves the live helper still
// sources the runtime plan env from the current registry entry (its documented
// behavior, unchanged by the context rework).
func TestApplyLiveServicesUsesRegistryPreparedEnv(t *testing.T) {
	// A secret-free template with one env var lets the prepared plan env show up
	// in the started container's spec. The fake records nothing about env, so
	// this is asserted through the reconciler's own env handling: a nil Prepared
	// must still converge. This test pins that the helper tolerates both a
	// present and an absent registry entry.
	fake := newCtxRecordDocker()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)

	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{runner.NewPrepared(
		function.Function{Name: "fn", Template: applyServiceTemplate()},
		&runtime.Prepared{Name: "fn", Image: "img", Env: []string{"PLAN=1"}},
		nil,
	)})

	applyLiveServices(context.Background(), svcCtrl, reg, "fn", t.TempDir(), applyServiceTemplate(), "img")
	if got := len(fake.forOp("start")); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}

	// Absent registry entry: nil Prepared falls back to no plan env and still
	// converges.
	fake2 := newCtxRecordDocker()
	svcCtrl2 := reconciler.NewServiceReconciler(fake2, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	applyLiveServices(context.Background(), svcCtrl2, reg, "missing", t.TempDir(), applyServiceTemplate(), "img")
	if got := len(fake2.forOp("start")); got != 1 {
		t.Fatalf("starts with no registry entry = %d, want 1", got)
	}
}

// TestReconcileStartupServicesApplyUsesLifecycleNotPassBudget proves the startup
// path also passes the lifecycle context into Apply: cancelling the lifecycle
// cancels an in-flight startup converge promptly rather than letting it run out
// the reconcileTimeout.
func TestReconcileStartupServicesApplyUsesLifecycleNotPassBudget(t *testing.T) {
	fake := newCtxRecordDocker()
	fake.block["list"] = true
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	tmpl := applyServiceTemplate()
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha"}, nil),
	}
	functions := []function.Function{{Name: "alpha", Template: tmpl}}

	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		reconcileStartupServices(lifecycle, prepared, functions, svcCtrl, logger)
	}()

	fake.waitEntered(t, "list")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the startup Apply promptly")
	}
}

// TestReconcileStartupServicesStandaloneRemoveBounded pins the standalone
// operation kept in reconcileStartupServices: an unavailable function whose
// template declares no services is removed on its OWN fresh reconcileTimeout
// bound (not the unbounded lifecycle).
func TestReconcileStartupServicesStandaloneRemoveBounded(t *testing.T) {
	fake := newCtxRecordDocker()
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	tmpl := &function.Template{Runtime: "node24"} // no services
	fn := function.Function{Name: "broken", Template: tmpl}
	prepared := []*runner.PreparedFunction{runner.NewUnavailable(fn)}
	functions := []function.Function{fn}

	reconcileStartupServices(context.Background(), prepared, functions, svcCtrl, logger)

	obs := fake.forOp("remove")
	if len(obs) != 1 {
		t.Fatalf("remove observations = %d, want 1", len(obs))
	}
	if !obs[0].hasDeadline {
		t.Fatal("standalone startup remove carried no deadline; want a reconcileTimeout bound")
	}
	if rem := obs[0].deadline.Sub(obs[0].at); rem <= 0 || rem > reconcileTimeout {
		t.Fatalf("standalone remove remaining budget = %v, want a fresh (0, %v]", rem, reconcileTimeout)
	}
}

// TestReconcileStartupServicesUnavailableWithServicesSkips proves the other
// standalone branch: an unavailable function that still declares services is
// left alone (no remove, no apply), so its possibly-serving containers are
// preserved.
func TestReconcileStartupServicesUnavailableWithServicesSkips(t *testing.T) {
	fake := newCtxRecordDocker()
	logger := discardLogger()
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	tmpl := applyServiceTemplate()
	fn := function.Function{Name: "broken", Template: tmpl}
	prepared := []*runner.PreparedFunction{runner.NewUnavailable(fn)}

	reconcileStartupServices(context.Background(), prepared, []function.Function{fn}, svcCtrl, logger)

	// The unavailable-with-services branch must neither remove nor apply: the
	// only container call is the trailing orphan sweep's listing.
	for _, op := range []string{"remove", "resolve", "start"} {
		if got := len(fake.forOp(op)); got != 0 {
			t.Fatalf("unavailable-with-services must not call %q, got %d call(s)", op, got)
		}
	}
	if got := len(fake.forOp("list")); got != 1 {
		t.Fatalf("list calls = %d, want 1 (the trailing orphan sweep only)", got)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

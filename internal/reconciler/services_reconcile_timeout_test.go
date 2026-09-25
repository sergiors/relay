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

// ctxObservation records the lifecycle-relevant state of a context at the
// moment a fake Docker method received it: its deadline (if any), the remaining
// budget then, and its error. It is what lets the timeout lifecycle tests assert
// that each Docker operation got a FRESH bound rather than a context a prior
// operation (e.g. a long build) already consumed.
type ctxObservation struct {
	op          string
	at          time.Time
	deadline    time.Time
	hasDeadline bool
	err         error
}

func (o ctxObservation) remaining() time.Duration {
	if !o.hasDeadline {
		return 0
	}
	return o.deadline.Sub(o.at)
}

// phaseDocker is a reconciler.Docker fake that models the timeout lifecycle
// without a daemon. It records the context every method receives and lets a test
// simulate a long Dockerfile build (a ResolveServiceImage that outlives the
// pre-build budget) and lifecycle cancellation (methods that block until their
// context is done). It mirrors the production contract: a build source's real
// Dockerfile build is rooted in the runtime manager lifecycle, so a build is NOT
// cancelled by the short pre-build budget — this fake likewise ignores the
// pre-build context during buildDelay and returns its simulated ctx error only
// when blockResolve is set.
type phaseDocker struct {
	mu sync.Mutex

	// buildDelay simulates a long source resolution (a Dockerfile build). It is
	// waited without honoring the passed context, mirroring the runtime rooting
	// the real build in the manager lifecycle.
	buildDelay time.Duration
	// blockResolve makes ResolveServiceImage wait for the pre-build context to
	// finish and return its error (models lifecycle cancellation reaching the
	// build phase).
	blockResolve bool
	// blockList makes ServiceContainerList wait for its context.
	blockList bool
	// blockStart makes StartService wait for the post-build context and return
	// its error.
	blockStart bool

	obs    []ctxObservation
	starts int
}

func (d *phaseDocker) record(op string, ctx context.Context) {
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.obs = append(d.obs, ctxObservation{
		op:          op,
		at:          time.Now(),
		deadline:    dl,
		hasDeadline: ok,
		err:         ctx.Err(),
	})
}

func (d *phaseDocker) ResolveServiceImage(
	ctx context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	d.record("resolve", ctx)
	if d.blockResolve {
		<-ctx.Done()
		return runtime.ServiceImage{}, ctx.Err()
	}
	if d.buildDelay > 0 {
		// Deliberately ignore ctx: this is the runtime's long build, rooted in
		// the manager lifecycle, not the pre-build reconcile budget.
		time.Sleep(d.buildDelay)
	}
	if svc.Source() == function.ServiceSourceBuild || svc.Source() == function.ServiceSourceImage {
		return runtime.ServiceImage{Ref: functionImage, ID: "built"}, nil
	}
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}

func (d *phaseDocker) StartService(ctx context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	d.record("start", ctx)
	if d.blockStart {
		<-ctx.Done()
		return "", ctx.Err()
	}
	// Honor an already-expired context, so a post-build phase mistakenly reusing
	// a consumed pre-build context surfaces as a failure rather than passing.
	if err := ctx.Err(); err != nil {
		return "", err
	}
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	return "id-1", nil
}

func (d *phaseDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.record("list", ctx)
	if d.blockList {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}

func (d *phaseDocker) StopServiceContainers(ctx context.Context, _ []runtime.ServiceContainer) error {
	d.record("stop", ctx)
	return nil
}

func (d *phaseDocker) RemoveFunctionServiceContainers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (d *phaseDocker) NetworkExists(ctx context.Context, _ string) (bool, error) {
	d.record("network", ctx)
	return true, nil
}

func (d *phaseDocker) observationsFor(op string) []ctxObservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []ctxObservation
	for _, o := range d.obs {
		if o.op == op {
			out = append(out, o)
		}
	}
	return out
}

func (d *phaseDocker) startCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts
}

// buildServiceTemplate is a single `build`-source service, the shape whose
// resolution runs a Dockerfile build.
func buildServiceTemplate() *function.Template {
	return &function.Template{
		Runtime:  "node24",
		Services: []function.Service{{Build: "Dockerfile", Port: 80, Replicas: 1}},
	}
}

// TestReconcileLongBuildDoesNotConsumePostBuildDeadline is the core lifecycle
// regression: a Dockerfile build that outlives the pre-build reconcile budget
// must NOT leave the post-build phase (env resolution, stale stops, replica
// starts) with an expired context. The build is simulated by a ResolveServiceImage
// that sleeps well past the injected timeout; the StartService that follows must
// still see a fresh, unexpired post-build context.
func TestReconcileLongBuildDoesNotConsumePostBuildDeadline(t *testing.T) {
	const budget = 20 * time.Millisecond
	f := &phaseDocker{buildDelay: 5 * budget}

	changed, err := Reconcile(context.Background(), budget, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
	if err != nil {
		t.Fatalf("reconcile with a long build: %v", err)
	}
	if !changed || f.startCount() != 1 {
		t.Fatalf("changed=%v starts=%d, want a started replica", changed, f.startCount())
	}
	startObs := f.observationsFor("start")
	if len(startObs) != 1 {
		t.Fatalf("start observations = %d, want 1", len(startObs))
	}
	// The build consumed (and expired) the pre-build context; the start must have
	// received a FRESH one, so its recorded ctx error is nil and its remaining
	// budget is roughly the full reconcileTimeout, not the leftover of a
	// consumed context.
	if startObs[0].err != nil {
		t.Fatalf("post-build StartService ctx error = %v, want nil (fresh post-build context)", startObs[0].err)
	}
	if !startObs[0].hasDeadline {
		t.Fatal("post-build StartService ctx has no deadline; want the reconcile budget")
	}
	if r := startObs[0].remaining(); r <= 0 || r > budget {
		t.Fatalf("post-build remaining budget = %v, want a fresh (0, %v]", r, budget)
	}
}

// TestReconcilePreAndPostBuildBoundsAreFreshAndSeparate pins that the pre-build
// and post-build phases each get their OWN fresh bound: distinct deadlines, each
// approximately the full reconcile budget measured from its own creation.
func TestReconcilePreAndPostBuildBoundsAreFreshAndSeparate(t *testing.T) {
	const budget = 50 * time.Millisecond
	f := &phaseDocker{buildDelay: 3 * budget}

	if _, err := Reconcile(context.Background(), budget, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	resolve := f.observationsFor("resolve")
	start := f.observationsFor("start")
	if len(resolve) != 1 || len(start) != 1 {
		t.Fatalf("resolve/start observations = %d/%d, want 1/1", len(resolve), len(start))
	}
	if !resolve[0].hasDeadline || !start[0].hasDeadline {
		t.Fatal("both pre-build and post-build calls must carry a reconcile deadline")
	}
	for name, o := range map[string]ctxObservation{"resolve": resolve[0], "start": start[0]} {
		if r := o.remaining(); r <= 0 || r > budget {
			t.Fatalf("%s remaining budget = %v, want a fresh (0, %v]", name, r, budget)
		}
	}
	// The post-build deadline is created AFTER the pre-build one, so it is later
	// in time: proof the post-build bound is not the pre-build bound carried over.
	if !start[0].deadline.After(resolve[0].deadline) {
		t.Fatalf("post-build deadline %v must be after pre-build deadline %v", start[0].deadline, resolve[0].deadline)
	}
}

// TestReconcilePostBuildIsBoundedToReconcileTimeout proves the post-build phase
// is bounded by the injected reconcileTimeout: a StartService that blocks until
// its context is done is cut off after roughly the budget, with a deadline error.
func TestReconcilePostBuildIsBoundedToReconcileTimeout(t *testing.T) {
	const budget = 20 * time.Millisecond
	f := &phaseDocker{blockStart: true}

	start := time.Now()
	_, err := Reconcile(context.Background(), budget, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
	elapsed := time.Since(start)

	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconcile err = %v, want a post-build context.DeadlineExceeded", err)
	}
	if elapsed < budget || elapsed > 2*time.Second {
		t.Fatalf("post-build cut off after %v, want ~%v (and well under 2s)", elapsed, budget)
	}
}

// TestReconcileLifecycleCancellationCancelsNormalOperation proves a normal
// (non-build) operation is rooted in the lifecycle: cancelling the lifecycle
// cancels an in-flight container listing promptly, surfacing context.Canceled
// rather than waiting out the reconcile budget.
func TestReconcileLifecycleCancellationCancelsNormalOperation(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &phaseDocker{blockList: true}

	done := make(chan error, 1)
	go func() {
		_, err := Reconcile(lifecycle, time.Minute, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
		done <- err
	}()

	// Wait until the list call is in flight (its observation is recorded before
	// it blocks).
	waitForObservation(t, f, "list")

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcile err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the in-flight list")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("list cancelled after %v; want prompt", elapsed)
	}
}

// TestReconcileLifecycleCancellationCancelsBuildPhase proves the pre-build phase
// (source resolution, i.e. a build source's resolution) remains lifecycle-
// cancellable: cancelling the lifecycle cancels an in-flight resolve promptly
// even though the build itself is NOT bounded by the short reconcile budget.
func TestReconcileLifecycleCancellationCancelsBuildPhase(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &phaseDocker{blockResolve: true}

	done := make(chan error, 1)
	go func() {
		_, err := Reconcile(lifecycle, time.Minute, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
		done <- err
	}()

	waitForObservation(t, f, "resolve")

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcile err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the in-flight build resolve")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("build resolve cancelled after %v; want prompt", elapsed)
	}
}

// TestReconcileNonPositiveTimeoutLeavesOperationsLifecycleBounded pins the
// "no normal-operation bound" contract: a non-positive reconcileTimeout derives
// no deadline (the lifecycle bounds cancellation), rather than treating the
// zero value as an immediate expiry.
func TestReconcileNonPositiveTimeoutLeavesOperationsLifecycleBounded(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &phaseDocker{}

	if _, err := Reconcile(lifecycle, 0, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, op := range []string{"list", "resolve", "start"} {
		obs := f.observationsFor(op)
		if len(obs) == 0 {
			t.Fatalf("no %s observation recorded", op)
		}
		if obs[0].hasDeadline {
			t.Fatalf("%s carried a deadline %v with a non-positive reconcileTimeout", op, obs[0].deadline)
		}
	}

	// Cancelling the lifecycle still cancels a non-positive-timeout operation.
	f2 := &phaseDocker{blockList: true}
	done := make(chan error, 1)
	go func() {
		_, err := Reconcile(lifecycle, 0, f2, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
		done <- err
	}()
	waitForObservation(t, f2, "list")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcile err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel a non-positive-timeout operation")
	}
}

// TestReconcileBuildReuseCachedPathStillBoundsPostBuild proves the build-reuse
// (cached) path keeps the same fresh post-build bound: when a build source's
// image already exists the resolution short-circuits with no build, and the
// post-build start still gets its own fresh reconcileTimeout bound. This pins
// that the timeout lifecycle is independent of whether a build ran.
func TestReconcileBuildReuseCachedPathStillBoundsPostBuild(t *testing.T) {
	const budget = 40 * time.Millisecond
	f := &phaseDocker{} // buildDelay 0: resolution is the cached/reuse short-circuit

	if _, err := Reconcile(context.Background(), budget, f, "fn", t.TempDir(), buildServiceTemplate(), "img", nil, nil, routing.TraefikConfig{}, testutil.DiscardLogger()); err != nil {
		t.Fatalf("reconcile (cached build): %v", err)
	}
	start := f.observationsFor("start")
	if len(start) != 1 {
		t.Fatalf("start observations = %d, want 1", len(start))
	}
	if start[0].err != nil {
		t.Fatalf("cached-path post-build ctx error = %v, want nil", start[0].err)
	}
	if rem := start[0].remaining(); rem <= 0 || rem > budget {
		t.Fatalf("cached-path post-build remaining budget = %v, want a fresh (0, %v]", rem, budget)
	}
}

// waitForObservation blocks until the fake has recorded at least one observation
// for op (the method records before it blocks), so cancellation tests do not
// race the goroutine's entry.
func waitForObservation(t *testing.T, f *phaseDocker, op string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(f.observationsFor(op)) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q to be entered", op)
}

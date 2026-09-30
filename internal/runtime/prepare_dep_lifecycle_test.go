package runtime

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// countingRaceGate parks N concurrent scripted daemon calls and signals exactly
// when the Nth arrives, so a test can prove N preparations are simultaneously
// holding a lease. Each arrival gets its OWN release channel, so a test can let
// the preparations finish one at a time and observe the reference count fall.
// Every wait is a channel signal at an exact production boundary; time.After is
// a deadlock failure bound only, never a delay used to order the test.
type countingRaceGate struct {
	want int

	mu          sync.Mutex
	arrivals    int
	slots       []chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once

	releaseAllCh   chan struct{}
	releaseAllOnce sync.Once
}

func newCountingRaceGate(want int) *countingRaceGate {
	return &countingRaceGate{
		want:         want,
		entered:      make(chan struct{}),
		releaseAllCh: make(chan struct{}),
	}
}

// arrive records one arrival, returns the channel that releases THIS arrival, and
// closes entered once want have arrived.
func (g *countingRaceGate) arrive() <-chan struct{} {
	ch := make(chan struct{})
	g.mu.Lock()
	g.slots = append(g.slots, ch)
	g.arrivals++
	reached := g.arrivals >= g.want
	g.mu.Unlock()
	if reached {
		g.enteredOnce.Do(func() { close(g.entered) })
	}
	return ch
}

// waitEntered blocks until want arrivals are parked.
func (g *countingRaceGate) waitEntered(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: only %d of %d arrivals observed", what, g.count(), g.want)
	}
}

func (g *countingRaceGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.arrivals
}

// slot returns the release channel of the idx-th arrival. It is only called after
// waitEntered, so the slot exists.
func (g *countingRaceGate) slot(idx int) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.slots[idx]
}

func (g *countingRaceGate) releaseAll() {
	g.releaseAllOnce.Do(func() { close(g.releaseAllCh) })
}

// parked blocks the calling scripted request until its own slot is released, the
// whole gate is released, or the request context ends.
func (g *countingRaceGate) parked(req *http.Request, slot <-chan struct{}) {
	select {
	case <-slot:
	case <-g.releaseAllCh:
	case <-req.Context().Done():
	}
}

// dependencyLifecycleFixture writes one dependency-bearing Python function and
// returns it with the exact image and dependency references production derives,
// so a test can name both without a real daemon.
func dependencyLifecycleFixture(t *testing.T, name string) (function.Function, string, string) {
	t.Helper()
	dir := t.TempDir()
	writeRaceSource(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeRaceSource(t, dir, "requirements.txt", "six==1.16.0\n")
	fn := function.Function{Name: name, Dir: dir, Template: &function.Template{Runtime: "python3.14"}}
	fp, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint %q: %v", name, err)
	}
	return fn, ImageRef(fn.Name, fp), dependencyRefForTest(t, fn)
}

// TestPrepareDependencyLeaseReleasedOnDependencyBuildFailure pins the failure
// half of the dependency lease: when the dependency image build fails, Prepare
// must release BOTH the dependency lease (admitted before the probe/build) and
// the function lease, so a failed preparation never pins an image. The gate is
// proven clear by a fresh independent acquire succeeding.
func TestPrepareDependencyLeaseReleasedOnDependencyBuildFailure(t *testing.T) {
	fn, image, depRef := dependencyLifecycleFixture(t, "dep-build-fail")

	builtTag := ""
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			body:      `{"errorDetail":{"message":"install failed"}}`,
			onRequest: func(req *http.Request) { builtTag = req.URL.Query().Get("t") },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("Prepare must surface the dependency build failure")
	}
	if builtTag != depRef {
		t.Fatalf("the failing build must be the dependency layer %q, got %q", depRef, builtTag)
	}
	if got := m.LeaseCount(depRef); got != 0 {
		t.Fatalf("dependency lease count after a failed prepare = %d, want 0", got)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("function lease count after a failed prepare = %d, want 0", got)
	}
	// The failure must have cleared the gate: a fresh independent acquire on the
	// exact dependency succeeds (a stranded lease would leave the drain open but
	// would not reject acquisition; the count assertion above is the primary
	// proof, this confirms the coordinator is reusable).
	lease, err := m.AcquireImageLease(depRef)
	if err != nil {
		t.Fatalf("dependency must be reusable after a failed prepare, got %v", err)
	}
	lease.Release()
}

// TestPrepareDependencyLeaseReleasedOnFunctionBuildFailure pins the sibling
// failure half: the dependency build succeeds, then the function image build
// (which consumes the layer via FROM) fails. The dependency lease held across
// that build must still be released, leaving the now-orphaned dependency
// collectible — the very next GC pass removes it.
func TestPrepareDependencyLeaseReleasedOnFunctionBuildFailure(t *testing.T) {
	fn, image, depRef := dependencyLifecycleFixture(t, "fn-build-fail")

	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON(depRef)},
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			fail: func(req *http.Request) error {
				if req.URL.Query().Get("t") == image {
					return errors.New("function build failed")
				}
				return nil
			},
		},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := newLifecycleManager(t, cli, context.Background())

	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("Prepare must surface the function build failure")
	}
	if got := m.LeaseCount(depRef); got != 0 {
		t.Fatalf("dependency lease count after a failed function build = %d, want 0", got)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("function lease count after a failed function build = %d, want 0", got)
	}
	// No function image was tagged, so the dependency is genuinely orphaned.
	// The lease being released is what lets the very same GC pass remove it.
	removed, err := m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC after the failed function build: %v", err)
	}
	if removed != 1 || dels != 1 {
		t.Fatalf("GC removed %d (DELETE %d), want 1 and 1: the dependency lease must have been released", removed, dels)
	}
}

// TestPrepareDependencyLeaseReleasedOnCancellation pins the cancellation half:
// the dependency build is parked, the manager lifecycle is cancelled, and the
// preparation must fail while still releasing BOTH leases (the dependency lease
// spans the build, so its release must run on the cancellation path too).
func TestPrepareDependencyLeaseReleasedOnCancellation(t *testing.T) {
	fn, image, depRef := dependencyLifecycleFixture(t, "dep-cancel")

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	entered := make(chan struct{})
	var enteredOnce sync.Once
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(req *http.Request) {
				if req.URL.Query().Get("t") == depRef {
					enteredOnce.Do(func() { close(entered) })
				}
			},
			fail: func(req *http.Request) error {
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)
	m := newLifecycleManager(t, cli, lifecycle)

	done := make(chan error, 1)
	go func() {
		_, err := m.Prepare(context.Background(), fn)
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the dependency build was not entered")
	}
	cancelLifecycle()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Prepare = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the dependency build promptly")
	}
	if got := m.LeaseCount(depRef); got != 0 {
		t.Fatalf("dependency lease count after a cancelled prepare = %d, want 0", got)
	}
	if got := m.LeaseCount(image); got != 0 {
		t.Fatalf("function lease count after a cancelled prepare = %d, want 0", got)
	}
}

// TestPrepareMultipleDependencyLeasesSpanGC is the N-concurrent-preparations
// proof for the shared, content-addressed dependency layer. Two functions with
// identical manifests resolve to ONE dependency image; while both preparations
// are parked in its build, each holds its OWN lease, so the count is 2 and GC
// must keep the layer. Releasing one preparation leaves the other pinning it; GC
// still keeps it; only when the last releases does the same GC pass remove it.
func TestPrepareMultipleDependencyLeasesSpanGC(t *testing.T) {
	fnA, imageA, depRef := dependencyLifecycleFixture(t, "dep-multi-a")
	fnB, imageB, depRefB := dependencyLifecycleFixture(t, "dep-multi-b")
	if depRefB != depRef {
		t.Fatalf("A and B must share one dependency layer, got %s and %s", depRef, depRefB)
	}

	gate := newCountingRaceGate(2)
	defer gate.releaseAll()

	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON(depRef)},
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(req *http.Request) {
				if req.URL.Query().Get("t") == depRef {
					gate.parked(req, gate.arrive())
				}
			},
		},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := newLifecycleManager(t, cli, context.Background())

	done := make(chan error, 2)
	for _, fn := range []function.Function{fnA, fnB} {
		fn := fn
		go func() {
			_, err := m.Prepare(context.Background(), fn)
			done <- err
		}()
	}

	gate.waitEntered(t, "both preparations parked in the shared dependency build")
	if got := m.LeaseCount(depRef); got != 2 {
		t.Fatalf("dependency lease count = %d, want 2 (one per in-flight preparation)", got)
	}

	// Both preparations hold the layer: GC must keep it and issue no DELETE.
	removed, err := m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC with two held dependency leases must not surface a genuine error: %v", err)
	}
	if removed != 0 || dels != 0 {
		t.Fatalf("GC removed %d (DELETE %d) while two preparations held the dependency, want 0", removed, dels)
	}

	// Release one preparation: the other still pins the layer.
	close(gate.slot(0))
	if err := <-done; err != nil {
		t.Fatalf("first prepare failed: %v", err)
	}
	if got := m.LeaseCount(depRef); got != 1 {
		t.Fatalf("dependency lease count after one prepare returned = %d, want 1", got)
	}
	removed, err = m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC with one held dependency lease: %v", err)
	}
	if removed != 0 || dels != 0 {
		t.Fatalf("GC removed %d (DELETE %d) while one preparation still held the dependency, want 0", removed, dels)
	}

	// The last preparation releases: the layer is now collectible.
	close(gate.slot(1))
	if err := <-done; err != nil {
		t.Fatalf("second prepare failed: %v", err)
	}
	if got := m.LeaseCount(depRef); got != 0 {
		t.Fatalf("dependency lease count after both prepares returned = %d, want 0", got)
	}
	removed, err = m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC after both preparations released: %v", err)
	}
	if removed != 1 || dels != 1 {
		t.Fatalf("GC removed %d (DELETE %d) after both released, want 1 and 1", removed, dels)
	}
	// Sanity: the two function images are distinct per function name.
	if imageA == imageB {
		t.Fatalf("distinct functions must have distinct image references, both %s", imageA)
	}
}

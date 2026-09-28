package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// dependencyRefForTest computes the dependency image reference a function's
// current manifest set resolves to, using the exact production helpers Prepare
// uses (lookup -> engine Plan -> DependencyFingerprint -> depImageRef). It is the
// non-integration twin of integration_helpers_test.go's expectedDependencyRef, so
// this race test can name the shared dependency image deterministically.
func dependencyRefForTest(t *testing.T, fn function.Function) string {
	t.Helper()
	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		t.Fatalf("lookup %q: %v", fn.Template.Runtime, err)
	}
	eng, err := engineFor(spec)
	if err != nil {
		t.Fatalf("engine for %q: %v", fn.Template.Runtime, err)
	}
	p, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan %q: %v", fn.Name, err)
	}
	if p.Deps.IsZero() {
		t.Fatalf("function %q declares no dependency layer", fn.Name)
	}
	fp, err := DependencyFingerprint(arch, platform, spec, fn.Dir, p.Deps)
	if err != nil {
		t.Fatalf("dependency fingerprint %q: %v", fn.Name, err)
	}
	return depImageRef(fp)
}

// raceGate is a deterministic one-shot entry/release pair used to park a
// scripted daemon call at a precise point. entered closes when the call is first
// observed; the caller releases it (or the request context ends, so a wedged test
// cannot leak the goroutine past its bound).
type raceGate struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newRaceGate() *raceGate {
	return &raceGate{entered: make(chan struct{}), release: make(chan struct{})}
}

// wait blocks until the gate's daemon call is observed. The timeout is a deadlock
// failure bound only: on success the signal is delivered by the scripted daemon at
// the exact request entry, never by a delay.
func (g *raceGate) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s was never entered", what)
	}
}

// releaseAll unblocks the gate exactly once; it is safe to defer on every path.
func (g *raceGate) releaseAll() { g.releaseOnce.Do(func() { close(g.release) }) }

// parked blocks the calling scripted request until the test releases the gate or
// the request context ends. It is called from a route's onRequest after the gate
// has been signaled.
func (g *raceGate) parked(req *http.Request) {
	select {
	case <-g.release:
	case <-req.Context().Done():
	}
}

// TestPrepareDependencyLeaseSpansBuildVsGC is the deterministic proof of the
// "dependency GC vs active build" race. Manager.Prepare admits the dependency
// image lease BEFORE the dependency's existence probe and holds it through the
// ACTUAL dependency image build (and the function image build that consumes the
// layer via FROM). The test parks Prepare at each real daemon boundary — the
// dependency ImageInspect, the dependency /build, and the function /build — and
// at each boundary runs CleanupUnusedDependencies with an inventory that names the
// dependency as an unreferenced candidate. The GC must never issue the dependency
// DELETE while the lease is held; once Prepare returns and releases the lease, the
// very same GC pass removes it.
//
// Every synchronization point is a channel signaled at an exact production
// transition; the time.After cases are deadlock failure bounds only, never a
// delay used to order the test. There are no sleeps.
func TestPrepareDependencyLeaseSpansBuildVsGC(t *testing.T) {
	dir := t.TempDir()
	writeRaceSource(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeRaceSource(t, dir, "requirements.txt", "six==1.16.0\n")

	fn := function.Function{
		Name:     "dep-race",
		Dir:      dir,
		Template: &function.Template{Runtime: "python3.14"},
	}

	fp, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("function fingerprint: %v", err)
	}
	image := ImageRef(fn.Name, fp)
	depRef := dependencyRefForTest(t, fn)
	if image == "" || depRef == "" {
		t.Fatalf("image=%q depRef=%q must both be non-empty", image, depRef)
	}

	// Three exact production boundaries:
	//   depInspect — the dependency's existence probe (after the dep lease is
	//                admitted, before any dependency use);
	//   depBuild   — the actual dependency image build (POST /build t=<depRef>);
	//   fnBuild    — the function image build that consumes the layer via FROM.
	depInspect := newRaceGate()
	depBuild := newRaceGate()
	fnBuild := newRaceGate()
	defer depInspect.releaseAll()
	defer depBuild.releaseAll()
	defer fnBuild.releaseAll()

	dels := 0
	cli := newScriptedDockerClient(t,
		// The GC inventory: one tagged managed dependency image, unreferenced.
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON(depRef)},
		// The dependency existence probe parks the lease-gated window open, then
		// reports the layer absent so the actual dependency build path runs.
		dockerRoute{
			method: http.MethodGet, path: "/images/" + depRef + "/json",
			status: http.StatusNotFound,
			body:   `{"message":"no such image"}`,
			onRequest: func(req *http.Request) {
				depInspect.enteredOnce.Do(func() { close(depInspect.entered) })
				depInspect.parked(req)
			},
		},
		// The function image is absent, forcing the build path.
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		// A single build route dispatching on the tag query: the dependency build
		// parks on depBuild, the function build on fnBuild.
		dockerRoute{
			method: http.MethodPost, path: "/build",
			body: `{"stream":"ok"}`,
			onRequest: func(req *http.Request) {
				switch req.URL.Query().Get("t") {
				case depRef:
					depBuild.enteredOnce.Do(func() { close(depBuild.entered) })
					depBuild.parked(req)
				case image:
					fnBuild.enteredOnce.Do(func() { close(fnBuild.entered) })
					fnBuild.parked(req)
				}
			},
		},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := newLifecycleManager(t, cli, context.Background())

	prepared := make(chan *Prepared, 1)
	prepareErr := make(chan error, 1)
	go func() {
		p, err := m.Prepare(context.Background(), fn)
		if err != nil {
			prepareErr <- err
			return
		}
		prepared <- p
	}()

	// gc runs the production dependency GC and asserts the invariant at the given
	// boundary: while Prepare is parked, the dependency is leased, so GC must
	// neither remove it nor issue a DELETE. It returns once the assertion holds so
	// the test can release the boundary deterministically.
	assertGCKeepsLeasedDependency := func(stage string) {
		t.Helper()
		if got := m.LeaseCount(depRef); got != 1 {
			t.Fatalf("%s: dependency lease count = %d, want 1 (Prepare must hold it here)", stage, got)
		}
		removed, err := m.CleanupUnusedDependencies(context.Background())
		if err != nil {
			t.Fatalf("%s: GC with a held dependency lease must not surface a genuine error: %v", stage, err)
		}
		if removed != 0 {
			t.Fatalf("%s: GC removed %d while Prepare held the dependency lease", stage, removed)
		}
		if dels != 0 {
			t.Fatalf("%s: dependency DELETE issued while Prepare held the lease: %d", stage, dels)
		}
	}

	// Boundary 1: dependency existence probe. The lease is already admitted.
	depInspect.wait(t, "dependency ImageInspect")
	assertGCKeepsLeasedDependency("at dependency inspect")

	// Release the probe as "absent": the dependency image must now be built.
	depInspect.releaseAll()

	// Boundary 2: the ACTUAL dependency image build. The lease spans it.
	depBuild.wait(t, "dependency image build")
	assertGCKeepsLeasedDependency("during dependency build")

	// Let the dependency build finish; the function image build (FROM depRef)
	// starts next.
	depBuild.releaseAll()

	// Boundary 3: the function image build consuming the dependency via FROM.
	fnBuild.wait(t, "function image build")
	assertGCKeepsLeasedDependency("during function image build consuming the dependency")

	fnBuild.releaseAll()

	select {
	case err := <-prepareErr:
		t.Fatalf("Prepare failed: %v", err)
	case p := <-prepared:
		if p.Image != image {
			t.Fatalf("prepared image = %q, want %q", p.Image, image)
		}
		if p.Dependency != depRef {
			t.Fatalf("prepared dependency = %q, want %q", p.Dependency, depRef)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Prepare did not return after all builds were released")
	}

	// Prepare returned, so its deferred dependency lease release has run: the
	// layer is now collectible by the very same GC pass.
	if got := m.LeaseCount(depRef); got != 0 {
		t.Fatalf("dependency lease count after Prepare returned = %d, want 0", got)
	}
	removed, err := m.CleanupUnusedDependencies(context.Background())
	if err != nil {
		t.Fatalf("GC after Prepare released the dependency lease: %v", err)
	}
	if removed != 1 {
		t.Fatalf("GC removed %d dependencies after the lease released, want 1", removed)
	}
	if dels != 1 {
		t.Fatalf("dependency DELETE calls = %d, want 1", dels)
	}
}

// writeRaceSource writes one source file for the dependency-race test.
func writeRaceSource(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

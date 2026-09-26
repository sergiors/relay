package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestReconcileWithStatusCallbackOrdering pins the focused status-callback
// contract the reconciler wires for a function that declares services:
//
//	onReconcileStart -> reconciling (source resolved / build finished)
//	onComplete(nil)  -> ready (only after the full generation converged)
//
// and, for a Dockerfile (`build`) service, the actual build boundary fires
// onBuildStart -> building BEFORE onReconcileStart -> reconciling. This is the
// ordering the worker maps onto the persisted lifecycle: preparing -> building
// -> reconciling -> ready. An entrypoint/image service has no build, so
// onBuildStart never fires.
func TestReconcileWithStatusCallbackOrdering(t *testing.T) {
	t.Run("build service fires building before reconciling", func(t *testing.T) {
		f := newFakeDocker()
		f.buildObserver = true
		buildTmpl := serviceTemplate("", function.Service{Build: "Dockerfile", Port: 80, Replicas: 1})

		var order []string
		c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
		err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), buildTmpl, "img", nil,
			func() { order = append(order, "building") },
			func() { order = append(order, "reconciling") },
		)
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		if len(order) != 2 || order[0] != "building" || order[1] != "reconciling" {
			t.Fatalf("callback order = %v, want [building reconciling]", order)
		}
	})

	t.Run("entrypoint service never fires building", func(t *testing.T) {
		f := newFakeDocker()
		f.buildObserver = true
		entryTmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

		var order []string
		c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
		if err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), entryTmpl, "img", nil,
			func() { order = append(order, "building") },
			func() { order = append(order, "reconciling") },
		); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if len(order) != 1 || order[0] != "reconciling" {
			t.Fatalf("callback order = %v, want [reconciling] only", order)
		}
	})

	t.Run("mixed template keeps building before reconciling", func(t *testing.T) {
		f := newFakeDocker()
		f.buildObserver = true
		// The entrypoint service is iterated BEFORE the build service; the
		// global invariant (building before reconciling) must still hold, so
		// reconciling is deferred until the build resolves.
		mixed := serviceTemplate("node24",
			function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1},
			function.Service{Build: "Dockerfile", Port: 8080, Replicas: 1},
		)

		var order []string
		c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
		if err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), mixed, "img", nil,
			func() { order = append(order, "building") },
			func() { order = append(order, "reconciling") },
		); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if len(order) != 2 || order[0] != "building" || order[1] != "reconciling" {
			t.Fatalf("callback order = %v, want [building reconciling] for a mixed template", order)
		}
	})
}

// TestReconcileWithStatusNoOpVerificationDoesNotNotifyReconciling pins the
// focused correction: a fully-converged service pass is a VERIFICATION, not
// convergence work. It must not fire the reconciling callback (nor building),
// so a periodic no-op tick can never publish a spurious reconciling/building
// status for an already-ready function.
func TestReconcileWithStatusNoOpVerificationDoesNotNotifyReconciling(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["id-1"] = &fakeContainer{
		id: "id-1", function: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80),
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	var building, reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil,
		func() { building++ },
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if building != 0 {
		t.Fatalf("building fired %d times on a no-op verification, want 0", building)
	}
	if reconciling != 0 {
		t.Fatalf("reconciling fired %d times on a no-op verification, want 0", reconciling)
	}
}

// TestReconcileWithStatusCorrectiveStartNotifiesReconciling pins the other side:
// a pass that actually starts a missing replica performs corrective work, so it
// fires reconciling exactly once before converging the container.
func TestReconcileWithStatusCorrectiveStartNotifiesReconciling(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	var reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil,
		func() {},
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if reconciling != 1 {
		t.Fatalf("reconciling fired %d times on a corrective pass, want 1", reconciling)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// TestReconcileWithStatusRemovedServiceNotifiesReconciling pins that stopping a
// removed service's leftover container is corrective work: the pass must publish
// reconciling (there is no per-service loop and no build in this template).
func TestReconcileWithStatusRemovedServiceNotifiesReconciling(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["old-1"] = &fakeContainer{
		id: "old-1", function: "fn", entrypoint: "old.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
	}
	tmpl := serviceTemplate("node24") // old.js is no longer desired

	var reconciling int
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.ApplyWithStatus(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil,
		func() {},
		func() { reconciling++ },
	); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if reconciling != 1 {
		t.Fatalf("reconciling fired %d times when stopping a removed service, want 1", reconciling)
	}
	if len(f.stops) != 1 || f.stops[0] != "old-1" {
		t.Fatalf("stops = %v, want [old-1]", f.stops)
	}
}

// TestReconcileServiceStatusPersistedPreparingReconcilingReady drives the real
// reconciler over a function with services and pins the persisted lifecycle the
// worker exposes: preparing (written before the current generation's work),
// reconciling (at the convergence seam), and ready (after onComplete). It wires
// status callbacks the production worker wires and asserts the persisted status
// as each boundary runs.
func TestReconcileServiceStatusPersistedPreparingReconcilingReady(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-persist")
	tmpl := mustParse(servicesTemplate)
	baseFn := function.Function{Name: "svc-persist", Dir: dir, Template: tmpl}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordReconcileSuccess(baseFn.Name, "img-svc-persist", "fp", time.Now(), baseFn)

	// Seed an active generation so the reconcile is an update (the skip path is
	// bypassed by changing content below).
	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-svc-persist"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{pf})

	var seen []string
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
			// The reconciler wrote preparing before dispatching. Record the
			// persisted state at the convergence seam and at completion.
			onReconcileStart()
			if d, ok := st.GetFunction(name); ok {
				seen = append(seen, d.Status)
			}
			onComplete(nil)
			if d, ok := st.GetFunction(name); ok {
				seen = append(seen, d.Status)
			}
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	r.Seed(baseFn)

	// Change content so the rebuild path runs and the update-services hook fires.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction("svc-persist")

	if len(seen) != 2 || seen[0] != state.StatusReconciling || seen[1] != state.StatusReady {
		t.Fatalf("persisted status at seams = %v, want [reconciling ready]", seen)
	}
	final, _ := st.GetFunction("svc-persist")
	if final.Status != state.StatusReady {
		t.Fatalf("final status = %q, want ready", final.Status)
	}
}

// TestReconcileServiceFailureStatusRetainsGeneration pins the generation-safe
// failure semantics end to end: a service convergence failure after a successful
// build leaves a degraded (not unavailable) status when an active image exists,
// and the ready status is never reached.
func TestReconcileServiceFailureStatusRetainsGeneration(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-fail")
	tmpl := mustParse(servicesTemplate)
	baseFn := function.Function{Name: "svc-fail", Dir: dir, Template: tmpl}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordReconcileSuccess(baseFn.Name, "img-svc-fail", "fp", time.Now(), baseFn)

	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-svc-fail"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{pf})

	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
			onReconcileStart()
			onComplete(errServiceConverge)
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	r.Seed(baseFn)

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction("svc-fail")

	final, ok := st.GetFunction("svc-fail")
	if !ok {
		t.Fatal("expected svc-fail row")
	}
	if final.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded after a service failure with an active image", final.Status)
	}
	if final.Image != "img-svc-fail" {
		t.Fatalf("image = %q, want the retained active image", final.Image)
	}
}

// errServiceConverge is the sentinel service-convergence failure used above.
var errServiceConverge = &boomError{"service converge failed"}

type boomError struct{ msg string }

func (e *boomError) Error() string { return e.msg }

// TestReconcileStaleGenerationCompletionCannotOverwriteNewerStatus pins the
// generation guard: when a newer reconcile of the same function has begun, a
// stale in-flight generation's completion callback (success or failure) must be
// a no-op, so a slow service convergence from an older desired state can never
// resurrect an outdated ready/degraded status over the newer generation's
// outcome. The reconciler's callbacks are captured per generation and fired
// explicitly in the stale-then-current order.
func TestReconcileStaleGenerationCompletionCannotOverwriteNewerStatus(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-gen")
	tmpl := mustParse(servicesTemplate)
	baseFn := function.Function{Name: "svc-gen", Dir: dir, Template: tmpl}

	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.RecordReconcileSuccess(baseFn.Name, "img-gen", "fp", time.Now(), baseFn)

	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-gen"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedFunction{pf})

	type completion struct {
		onBegin  func()
		onFinish func(error)
	}
	var completions []completion
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
			completions = append(completions, completion{onBegin: onReconcileStart, onFinish: onComplete})
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	r.Seed(baseFn)

	// Generation 1: content change triggers an update; capture its callbacks.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileFunction("svc-gen")

	// Generation 2: another content change begins before generation 1 completes.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	r.reconcileFunction("svc-gen")

	if len(completions) != 2 {
		t.Fatalf("captured %d completions, want 2", len(completions))
	}

	// The stale generation 1 completes SUCCESSFULLY last: its callback must be
	// ignored because generation 2 is now current.
	completions[0].onBegin()
	completions[0].onFinish(nil)

	// Generation 2 completes with a FAILURE: only this may land.
	completions[1].onBegin()
	completions[1].onFinish(errServiceConverge)

	final, _ := st.GetFunction("svc-gen")
	if final.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (stale success must not overwrite the newer failure)", final.Status)
	}

	// Reverse order: a stale FAILURE after the current SUCCESS must not clobber
	// the ready status either.
	t.Run("stale failure after current success is ignored", func(t *testing.T) {
		st2, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
		if err != nil {
			t.Fatalf("open state: %v", err)
		}
		t.Cleanup(func() { _ = st2.Close() })
		st2.RecordReconcileSuccess(baseFn.Name, "img-gen", "fp", time.Now(), baseFn)

		var cs []completion
		reg2 := &runner.Registry{}
		reg2.Set([]*runner.PreparedFunction{runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-gen"}, &fakeBuilder{})})
		r2 := New(Config{
			Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st2,
			UpdateServicesWithStatus: func(name, fnDir string, _ *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error)) {
				cs = append(cs, completion{onBegin: onReconcileStart, onFinish: onComplete})
			},
		}, reg2, &fakeBuilder{}, testutil.DiscardLogger())
		r2.Seed(baseFn)

		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v4'); }\n"), 0o644); err != nil {
			t.Fatalf("write v4: %v", err)
		}
		r2.reconcileFunction("svc-gen")
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v5'); }\n"), 0o644); err != nil {
			t.Fatalf("write v5: %v", err)
		}
		r2.reconcileFunction("svc-gen")
		if len(cs) != 2 {
			t.Fatalf("captured %d completions, want 2", len(cs))
		}
		cs[1].onBegin()
		cs[1].onFinish(nil) // current generation succeeds
		cs[0].onBegin()     // stale generation begins...
		cs[0].onFinish(errServiceConverge)

		got, _ := st2.GetFunction("svc-gen")
		if got.Status != state.StatusReady {
			t.Fatalf("status = %q, want ready (stale failure must not overwrite the newer success)", got.Status)
		}
	})
}

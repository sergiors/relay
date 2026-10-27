//go:build integration

package reconciler

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestReconcileServiceStatusPersistedPreparingReconcilingReady drives the real
// reconciler over an app with services and pins the persisted lifecycle the
// worker exposes: preparing (written before the current generation's work),
// reconciling (at the convergence seam), and ready (after onComplete). It wires
// status callbacks the production worker wires and asserts the persisted status
// as each boundary runs.
func TestReconcileServiceStatusPersistedPreparingReconcilingReady(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-persist")
	tmpl := mustParse(servicesTemplate)
	baseFn := app.App{Name: "svc-persist", Dir: dir, Template: tmpl}

	st := testutil.NewState(t)
	fencingToken := recordSuccess(t, st, baseFn.Name, "img-svc-persist", "fp", time.Now(), baseFn)

	// Seed an active generation so the reconcile is an update (the skip path is
	// bypassed by changing content below).
	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-svc-persist"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{pf})

	var seen []string
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name string, _ *app.Template, image string, onReconcileStart func(), onComplete func(error)) {
			// The reconciler wrote preparing before dispatching. Record the
			// persisted state at the convergence seam and at completion.
			onReconcileStart()
			if d, ok := st.GetApp(name); ok {
				seen = append(seen, d.Status)
			}
			onComplete(nil)
			if d, ok := st.GetApp(name); ok {
				seen = append(seen, d.Status)
			}
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	seedCurrent(r, baseFn)
	r.SeedFencingToken(baseFn.Name, fencingToken)

	// Change content so the rebuild path runs and the update-services hook fires.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("svc-persist")

	if len(seen) != 2 || seen[0] != state.StatusReconciling || seen[1] != state.StatusReady {
		t.Fatalf("persisted status at seams = %v, want [reconciling ready]", seen)
	}
	final, _ := st.GetApp("svc-persist")
	if final.Status != state.StatusReady {
		t.Fatalf("final status = %q, want ready", final.Status)
	}
}

// TestReconcileServiceFailureStatusRetainsGeneration pins the generation-safe
// failure semantics end to end: a service convergence failure after a successful
// build leaves a degraded (not unavailable) status when an active image exists,
// and the ready status is never reached.
func TestReconcileServiceFailureStatusRetainsGeneration(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-fail")
	tmpl := mustParse(servicesTemplate)
	baseFn := app.App{Name: "svc-fail", Dir: dir, Template: tmpl}

	st := testutil.NewState(t)
	fencingToken := recordSuccess(t, st, baseFn.Name, "img-svc-fail", "fp", time.Now(), baseFn)

	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-svc-fail"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{pf})

	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name string, _ *app.Template, image string, onReconcileStart func(), onComplete func(error)) {
			onReconcileStart()
			onComplete(errServiceConverge)
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	seedCurrent(r, baseFn)
	r.SeedFencingToken(baseFn.Name, fencingToken)

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("svc-fail")

	final, ok := st.GetApp("svc-fail")
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

// TestReconcileStaleGenerationCompletionCannotOverwriteNewerStatus pins the
// generation guard: when a newer reconcile of the same app has begun, a
// stale in-flight generation's completion callback (success or failure) must be
// a no-op, so a slow service convergence from an older desired state can never
// resurrect an outdated ready/degraded status over the newer generation's
// outcome. The reconciler's callbacks are captured per generation and fired
// explicitly in the stale-then-current order.
func TestReconcileStaleGenerationCompletionCannotOverwriteNewerStatus(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-gen")
	tmpl := mustParse(servicesTemplate)
	baseFn := app.App{Name: "svc-gen", Dir: dir, Template: tmpl}

	st := testutil.NewState(t)
	fencingToken := recordSuccess(t, st, baseFn.Name, "img-gen", "fp", time.Now(), baseFn)

	pf := runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-gen"}, &fakeBuilder{})
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{pf})

	type completion struct {
		onBegin  func()
		onFinish func(error)
	}
	var completions []completion
	r := New(Config{
		Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st,
		UpdateServicesWithStatus: func(name string, _ *app.Template, image string, onReconcileStart func(), onComplete func(error)) {
			completions = append(completions, completion{onBegin: onReconcileStart, onFinish: onComplete})
		},
	}, reg, &fakeBuilder{}, testutil.DiscardLogger())
	seedCurrent(r, baseFn)
	r.SeedFencingToken(baseFn.Name, fencingToken)

	// Generation 1: content change triggers an update; capture its callbacks.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("svc-gen")

	// Generation 2: another content change begins before generation 1 completes.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	r.reconcileApp("svc-gen")

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

	final, _ := st.GetApp("svc-gen")
	if final.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (stale success must not overwrite the newer failure)", final.Status)
	}

	// Reverse order: a stale FAILURE after the current SUCCESS must not clobber
	// the ready status either.
	t.Run("stale failure after current success is ignored", func(t *testing.T) {
		st2 := testutil.NewState(t)
		fencingToken2 := recordSuccess(t, st2, baseFn.Name, "img-gen", "fp", time.Now(), baseFn)

		var cs []completion
		reg2 := &runner.Registry{}
		reg2.Set([]*runner.PreparedApp{runner.NewPrepared(baseFn, &runtime.Prepared{Name: baseFn.Name, Image: "img-gen"}, &fakeBuilder{})})
		r2 := New(Config{
			Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st2,
			UpdateServicesWithStatus: func(name string, _ *app.Template, image string, onReconcileStart func(), onComplete func(error)) {
				cs = append(cs, completion{onBegin: onReconcileStart, onFinish: onComplete})
			},
		}, reg2, &fakeBuilder{}, testutil.DiscardLogger())
		seedCurrent(r2, baseFn)
		r2.SeedFencingToken(baseFn.Name, fencingToken2)

		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v4'); }\n"), 0o644); err != nil {
			t.Fatalf("write v4: %v", err)
		}
		r2.reconcileApp("svc-gen")
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v5'); }\n"), 0o644); err != nil {
			t.Fatalf("write v5: %v", err)
		}
		r2.reconcileApp("svc-gen")
		if len(cs) != 2 {
			t.Fatalf("captured %d completions, want 2", len(cs))
		}
		cs[1].onBegin()
		cs[1].onFinish(nil) // current generation succeeds
		cs[0].onBegin()     // stale generation begins...
		cs[0].onFinish(errServiceConverge)

		got, _ := st2.GetApp("svc-gen")
		if got.Status != state.StatusReady {
			t.Fatalf("status = %q, want ready (stale failure must not overwrite the newer success)", got.Status)
		}
	})
}

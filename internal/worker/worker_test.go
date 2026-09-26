package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/state"
)

// TestStatsLoopNilStateExitsOnCancel ensures the snapshot loop is nil-safe on
// the state handle and stops promptly on cancel.
func TestStatsLoopNilStateExitsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, newStatsFlusher(nil, metrics.New()), time.Hour)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}
}

// TestManagedRuntimeBuildObserverPublishesBuildingOnlyOnActualBuild pins the
// startup preparation status boundary: the observer installed by
// managedRuntimeBuildContext only fires at the ACTUAL managed-runtime image
// build boundary, which a fake Builder models directly. A template that needs no
// runtime never fires it (Prepare is then a fast no-op), so it never flashes a
// spurious building state. The terminal transitions clear building with the
// existing failure semantics: a successful no-service preparation reaches ready,
// and a preparation failure without an active image is unavailable. A nil state
// handle yields a context without an observer.
func TestManagedRuntimeBuildObserverPublishesBuildingOnlyOnActualBuild(t *testing.T) {
	st := openTempState(t)

	runtimeFn := function.Function{
		Name:     "runtime-fn",
		Dir:      t.TempDir(),
		Template: &function.Template{Runtime: "node24"},
	}
	noRuntimeFn := function.Function{
		Name:     "no-runtime-fn",
		Dir:      t.TempDir(),
		Template: &function.Template{Services: []function.Service{{Image: "nginx:alpine"}}},
	}
	if noRuntimeFn.Template.NeedsRuntime() {
		t.Fatal("fixture precondition: no-runtime template must not need a runtime")
	}

	st.RecordDiscovered(runtimeFn)
	st.RecordDiscovered(noRuntimeFn)
	if got, _ := st.GetFunction(runtimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("discovered runtime function status = %q, want preparing", got.Status)
	}

	// A real build fires the observer, moving preparing -> building.
	runtime.FunctionBuildObserverFromContext(managedRuntimeBuildContext(context.Background(), st, runtimeFn))()
	if got, _ := st.GetFunction(runtimeFn.Name); got.Status != state.StatusBuilding {
		t.Fatalf("runtime function status = %q, want building", got.Status)
	}

	// A no-runtime preparation never fires the observer (there is no image to
	// build), so it stays preparing.
	noRuntimeCtx := managedRuntimeBuildContext(context.Background(), st, noRuntimeFn)
	if runtime.FunctionBuildObserverFromContext(noRuntimeCtx) == nil {
		t.Fatal("expected an observer to be installed for the no-runtime function too")
	}
	if got, _ := st.GetFunction(noRuntimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("no-runtime function status = %q, want preparing (observer never fires)", got.Status)
	}

	// A successful no-service preparation reaches ready, clearing building.
	st.RecordReconcileSuccess(runtimeFn.Name, "img", "fp", time.Now(), runtimeFn)
	if got, _ := st.GetFunction(runtimeFn.Name); got.Status != state.StatusReady {
		t.Fatalf("after success status = %q, want ready", got.Status)
	}

	// A preparation failure without an active image is unavailable, again
	// clearing building.
	failFn := function.Function{Name: "fail-fn", Dir: t.TempDir(), Template: &function.Template{Runtime: "node24"}}
	st.RecordDiscovered(failFn)
	runtime.FunctionBuildObserverFromContext(managedRuntimeBuildContext(context.Background(), st, failFn))()
	st.RecordReconcileFailure(failFn.Name, errors.New("build failed"))
	if got, _ := st.GetFunction(failFn.Name); got.Status != state.StatusUnavailable {
		t.Fatalf("after failure status = %q, want unavailable", got.Status)
	}

	// Nil state yields a context without an observer, not a panic.
	if runtime.FunctionBuildObserverFromContext(managedRuntimeBuildContext(context.Background(), nil, runtimeFn)) != nil {
		t.Fatal("nil state must not install a build observer")
	}
}

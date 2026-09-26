package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
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

// TestBeginManagedRuntimeBuildPublishesBuildingOnlyWhenRuntimeNeeded pins the
// startup preparation status boundary: a function whose managed runtime image is
// about to be prepared is marked building, while a template that needs no
// runtime (its services bring their own build/image sources) is left untouched
// so it never flashes a spurious building state. The terminal transitions clear
// building with the existing failure semantics: a successful no-service
// preparation reaches ready, and a preparation failure without an active image
// is unavailable. A nil state handle is a no-op.
func TestBeginManagedRuntimeBuildPublishesBuildingOnlyWhenRuntimeNeeded(t *testing.T) {
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

	beginManagedRuntimeBuild(st, runtimeFn)
	if got, _ := st.GetFunction(runtimeFn.Name); got.Status != state.StatusBuilding {
		t.Fatalf("runtime function status = %q, want building", got.Status)
	}

	beginManagedRuntimeBuild(st, noRuntimeFn)
	if got, _ := st.GetFunction(noRuntimeFn.Name); got.Status != state.StatusPending {
		t.Fatalf("no-runtime function status = %q, want pending (no spurious building)", got.Status)
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
	beginManagedRuntimeBuild(st, failFn)
	st.RecordReconcileFailure(failFn.Name, errors.New("build failed"))
	if got, _ := st.GetFunction(failFn.Name); got.Status != state.StatusUnavailable {
		t.Fatalf("after failure status = %q, want unavailable", got.Status)
	}

	// Nil state is a no-op, not a panic.
	beginManagedRuntimeBuild(nil, runtimeFn)
}

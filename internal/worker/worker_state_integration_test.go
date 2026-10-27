//go:build integration

package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/state"
)

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

	runtimeFn := app.App{
		Name:     "runtime-fn",
		Dir:      t.TempDir(),
		Template: &app.Template{Runtime: "node24"},
	}
	noRuntimeFn := app.App{
		Name:     "no-runtime-fn",
		Dir:      t.TempDir(),
		Template: &app.Template{Services: []app.Service{{Name: "web", Image: "nginx:alpine"}}},
	}
	if err := os.WriteFile(
		filepath.Join(noRuntimeFn.Dir, "template.yaml"),
		[]byte("services:\n  - name: web\n    image: nginx:alpine\n    port: 80\n"),
		0o644,
	); err != nil {
		t.Fatalf("write no-runtime template: %v", err)
	}
	if noRuntimeFn.Template.NeedsRuntime() {
		t.Fatal("fixture precondition: no-runtime template must not need a runtime")
	}

	runtimeFencingToken := seedFencingToken(t, st, runtimeFn)
	noRuntimeFencingToken := seedFencingToken(t, st, noRuntimeFn)
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("discovered runtime function status = %q, want preparing", got.Status)
	}

	// A real build fires the observer, moving preparing -> building.
	runtime.AppBuildObserverFromContext(
		managedRuntimeBuildContext(context.Background(), st, runtimeFn, runtimeFencingToken),
	)()
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusBuilding {
		t.Fatalf("runtime function status = %q, want building", got.Status)
	}

	// A no-runtime preparation never fires the observer (there is no image to
	// build), so it stays preparing.
	noRuntimeCtx := managedRuntimeBuildContext(context.Background(), st, noRuntimeFn, noRuntimeFencingToken)
	if runtime.AppBuildObserverFromContext(noRuntimeCtx) == nil {
		t.Fatal("expected an observer to be installed for the no-runtime function too")
	}
	if got, _ := st.GetApp(noRuntimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("no-runtime function status = %q, want preparing (observer never fires)", got.Status)
	}

	// A successful no-service preparation reaches ready, clearing building.
	landed, err := st.RecordReconcileSuccess(context.Background(), runtimeFn.Name, "img", "fp", time.Now(), runtimeFn, runtimeFencingToken)
	if err != nil {
		t.Fatalf("success write: %v", err)
	}
	if !landed {
		t.Fatal("success did not persist with the current fencing token")
	}
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusReady {
		t.Fatalf("after success status = %q, want ready", got.Status)
	}

	// A preparation failure without an active image is unavailable, again
	// clearing building.
	failFn := app.App{Name: "fail-fn", Dir: t.TempDir(), Template: &app.Template{Runtime: "node24"}}
	failFencingToken := seedFencingToken(t, st, failFn)
	runtime.AppBuildObserverFromContext(
		managedRuntimeBuildContext(context.Background(), st, failFn, failFencingToken),
	)()
	landed, err = st.RecordReconcileFailure(context.Background(), failFn.Name, errors.New("build failed"), failFencingToken)
	if err != nil {
		t.Fatalf("failure write: %v", err)
	}
	if !landed {
		t.Fatal("failure did not persist with the current fencing token")
	}
	if got, _ := st.GetApp(failFn.Name); got.Status != state.StatusUnavailable {
		t.Fatalf("after failure status = %q, want unavailable", got.Status)
	}

	// Nil state yields a context without an observer, not a panic.
	if runtime.AppBuildObserverFromContext(managedRuntimeBuildContext(context.Background(), nil, runtimeFn, runtimeFencingToken)) != nil {
		t.Fatal("nil state must not install a build observer")
	}
}

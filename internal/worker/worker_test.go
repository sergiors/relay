package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/state"
)

// seedToken persists fn's desired generation and returns the exact transition
// token the write stored. Generation-specific writes must carry it; token zero
// is a failed write and never persists.
func seedToken(t *testing.T, st *state.State, fn app.App) state.Token {
	t.Helper()
	token, err := st.RecordDiscovered(context.Background(), fn)
	if err != nil {
		t.Fatalf("RecordDiscovered(%q): %v", fn.Name, err)
	}
	if token <= 0 {
		t.Fatalf("RecordDiscovered(%q) returned token %d, want positive", fn.Name, token)
	}
	return token
}

// recordSuccess gives fn a usable persisted generation without reading the
// token back from apps.generation: the desired write returns the exact token and
// the success is conditioned on it.
func recordSuccess(
	t *testing.T,
	st *state.State,
	fn app.App,
	image, fingerprint string,
	preparedAt time.Time,
) state.Token {
	t.Helper()
	token := seedToken(t, st, fn)
	landed, err := st.RecordReconcileSuccess(context.Background(), fn.Name, image, fingerprint, preparedAt, fn, token)
	if err != nil {
		t.Fatalf("RecordReconcileSuccess(%q): %v", fn.Name, err)
	}
	if !landed {
		t.Fatalf("RecordReconcileSuccess(%q) did not persist with token %d",
			fn.Name, token)
	}
	return token
}

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

	runtimeToken := seedToken(t, st, runtimeFn)
	noRuntimeToken := seedToken(t, st, noRuntimeFn)
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("discovered runtime function status = %q, want preparing", got.Status)
	}

	// A real build fires the observer, moving preparing -> building.
	runtime.AppBuildObserverFromContext(
		managedRuntimeBuildContext(context.Background(), st, runtimeFn, runtimeToken),
	)()
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusBuilding {
		t.Fatalf("runtime function status = %q, want building", got.Status)
	}

	// A no-runtime preparation never fires the observer (there is no image to
	// build), so it stays preparing.
	noRuntimeCtx := managedRuntimeBuildContext(context.Background(), st, noRuntimeFn, noRuntimeToken)
	if runtime.AppBuildObserverFromContext(noRuntimeCtx) == nil {
		t.Fatal("expected an observer to be installed for the no-runtime function too")
	}
	if got, _ := st.GetApp(noRuntimeFn.Name); got.Status != state.StatusPreparing {
		t.Fatalf("no-runtime function status = %q, want preparing (observer never fires)", got.Status)
	}

	// A successful no-service preparation reaches ready, clearing building.
	landed, err := st.RecordReconcileSuccess(context.Background(), runtimeFn.Name, "img", "fp", time.Now(), runtimeFn, runtimeToken)
	if err != nil {
		t.Fatalf("success write: %v", err)
	}
	if !landed {
		t.Fatal("success did not persist with the current token")
	}
	if got, _ := st.GetApp(runtimeFn.Name); got.Status != state.StatusReady {
		t.Fatalf("after success status = %q, want ready", got.Status)
	}

	// A preparation failure without an active image is unavailable, again
	// clearing building.
	failFn := app.App{Name: "fail-fn", Dir: t.TempDir(), Template: &app.Template{Runtime: "node24"}}
	failToken := seedToken(t, st, failFn)
	runtime.AppBuildObserverFromContext(
		managedRuntimeBuildContext(context.Background(), st, failFn, failToken),
	)()
	landed, err = st.RecordReconcileFailure(context.Background(), failFn.Name, errors.New("build failed"), failToken)
	if err != nil {
		t.Fatalf("failure write: %v", err)
	}
	if !landed {
		t.Fatal("failure did not persist with the current token")
	}
	if got, _ := st.GetApp(failFn.Name); got.Status != state.StatusUnavailable {
		t.Fatalf("after failure status = %q, want unavailable", got.Status)
	}

	// Nil state yields a context without an observer, not a panic.
	if runtime.AppBuildObserverFromContext(managedRuntimeBuildContext(context.Background(), nil, runtimeFn, runtimeToken)) != nil {
		t.Fatal("nil state must not install a build observer")
	}
}

package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"relay/internal/function"
)

// TestFunctionBuildObserverFiresOnlyOnActualBuild pins the focused seam the
// worker and reconciler use to publish the persisted building status at the
// ACTUAL managed-runtime image build boundary:
//
//   - a real build (the image is absent, so Prepare issues ImageBuild) fires the
//     observer exactly once;
//   - a template that needs no runtime has no function image to build, so the
//     observer never fires (no spurious building);
//   - an observer installed on the context is only invoked by Prepare, never by
//     an unrelated call.
func TestFunctionBuildObserverFiresOnlyOnActualBuild(t *testing.T) {
	t.Run("real build fires the observer once", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		fn := function.Function{Name: "build-fires", Dir: dir, Template: &function.Template{Runtime: "node24"}}

		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			buildRoute(nil),
		)
		m := newLifecycleManager(t, cli, context.Background())

		var fired atomic.Int32
		ctx := WithFunctionBuildObserver(context.Background(), func() { fired.Add(1) })
		if _, err := m.Prepare(ctx, fn); err == nil {
			t.Fatal("expected the scripted build failure to surface")
		}
		if got := fired.Load(); got != 1 {
			t.Fatalf("observer fired %d times, want exactly 1", got)
		}
	})

	t.Run("no-runtime template never fires the observer", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("services:\n  - image: nginx:alpine\n"), 0o644); err != nil {
			t.Fatalf("write template: %v", err)
		}
		fn := function.Function{
			Name:     "no-runtime",
			Dir:      dir,
			Template: &function.Template{Services: []function.Service{{Image: "nginx:alpine"}}},
		}
		if fn.Template.NeedsRuntime() {
			t.Fatal("fixture precondition: template must not need a runtime")
		}
		// No daemon routes: a no-runtime Prepare must not touch Docker at all.
		cli := newScriptedDockerClient(t)
		m := newLifecycleManager(t, cli, context.Background())

		var fired atomic.Int32
		ctx := WithFunctionBuildObserver(context.Background(), func() { fired.Add(1) })
		if _, err := m.Prepare(ctx, fn); err != nil {
			t.Fatalf("no-runtime prepare: %v", err)
		}
		if got := fired.Load(); got != 0 {
			t.Fatalf("observer fired %d times for a no-runtime template, want 0", got)
		}
	})

	t.Run("nil observer is a no-op", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		fn := function.Function{Name: "nil-observer", Dir: dir, Template: &function.Template{Runtime: "node24"}}

		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			buildRoute(nil),
		)
		m := newLifecycleManager(t, cli, context.Background())
		if _, err := m.Prepare(context.Background(), fn); err == nil {
			t.Fatal("expected the scripted build failure to surface without an observer")
		}
	})
}

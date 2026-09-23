package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// buildDeadline captures the deadline carried by the *http.Request the moby
// client actually sent to the scripted daemon for a Dockerfile build. Because
// the client derives the request context from the ctx passed to ImageBuild, this
// is the real build deadline — not a re-derivation from a local context.
type buildDeadline struct {
	seen bool
	at   time.Time
	ok   bool
}

func (b *buildDeadline) capture(req *http.Request) {
	d, ok := req.Context().Deadline()
	b.seen = true
	b.at = d
	b.ok = ok
}

// newLifecycleManager builds a Manager directly (no daemon) whose Dockerfile
// builds are rooted in lifecycle. It is the minimal wiring buildContext needs:
// a scripted client, a logger, a container cache, and the lifecycle context.
func newLifecycleManager(t *testing.T, cli *client.Client, lifecycle context.Context) *Manager {
	t.Helper()
	m := &Manager{
		log:        testutil.DiscardLogger(),
		cli:        cli,
		hostname:   "test-host",
		lifecycle:  lifecycle,
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// buildRoute returns an ImageBuild route that fails the build with an error
// message, so the caller can capture the request deadline without needing the
// daemon to produce a real image.
func buildRoute(capture func(*http.Request)) dockerRoute {
	return dockerRoute{
		method:    http.MethodPost,
		path:      "/build",
		body:      `{"errorDetail":{"message":"boom"}}`,
		onRequest: capture,
	}
}

// TestFunctionImageBuildGetsIndependentTenMinuteDeadline proves a Dockerfile
// build issued through Manager.Prepare is bounded by buildTimeout (10m), NOT by
// the short reconcile budget the worker uses for normal service operations. The
// deadline is observed on the actual ImageBuild HTTP request.
func TestFunctionImageBuildGetsIndependentTenMinuteDeadline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	fn := function.Function{
		Name:     "build-deadline",
		Dir:      dir,
		Template: &function.Template{Runtime: "node24"},
	}

	var got buildDeadline
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		buildRoute(got.capture),
	)
	m := newLifecycleManager(t, cli, context.Background())

	// The caller passes a SHORT context (the worker's 30s reconcile budget,
	// exercised by reconciler.Prepare); the build must not inherit it.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), reconcileShort)
	defer shortCancel()
	if _, err := m.Prepare(shortCtx, fn); err == nil {
		t.Fatal("expected the scripted build failure to surface")
	}
	if !got.seen {
		t.Fatal("ImageBuild was never issued; the scripted /build route did not match")
	}
	if !got.ok {
		t.Fatalf("ImageBuild request carried no deadline; want %v", buildTimeout)
	}
	remaining := time.Until(got.at)
	if remaining <= reconcileShort {
		t.Fatalf("ImageBuild deadline bound = %v, must exceed the short reconcile budget %v", remaining, reconcileShort)
	}
	if remaining > buildTimeout {
		t.Fatalf("ImageBuild deadline bound = %v, must not exceed buildTimeout %v", remaining, buildTimeout)
	}
	if remaining < buildTimeout-time.Minute {
		t.Fatalf("ImageBuild deadline bound = %v, want ~buildTimeout %v", remaining, buildTimeout)
	}
}

// reconcileShort is the short budget the worker applies to normal service
// operations (its reconcileTimeout). runtime cannot import worker, so this test
// pins the same 30s value the worker uses; the two must agree.
const reconcileShort = 30 * time.Second

// TestServiceBuildGetsIndependentTenMinuteDeadline proves the service `build`
// source path (ResolveServiceImage for a Dockerfile service) also issues its
// ImageBuild under buildTimeout, independently of the caller's reconcile budget.
func TestServiceBuildGetsIndependentTenMinuteDeadline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}

	var got buildDeadline
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		buildRoute(got.capture),
	)
	// The lifecycle is unbounded; only the build's own buildTimeout should apply.
	m := newLifecycleManager(t, cli, context.Background())

	// The caller passes a SHORT context (the reconcile budget); the build must
	// not inherit it.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), reconcileShort)
	defer shortCancel()
	if _, err := m.ResolveServiceImage(shortCtx, "fn", dir, &function.Template{Runtime: "node24"}, svc, ""); err == nil {
		t.Fatal("expected the scripted build failure to surface")
	}
	if !got.seen {
		t.Fatal("ImageBuild was never issued; the scripted /build route did not match")
	}
	if !got.ok {
		t.Fatalf("ImageBuild request carried no deadline; want %v", buildTimeout)
	}
	if remaining := time.Until(got.at); remaining <= reconcileShort || remaining > buildTimeout {
		t.Fatalf("ImageBuild deadline bound = %v, want an independent 10m bound (short=%v build=%v)",
			remaining, reconcileShort, buildTimeout)
	}
}

// TestServiceBuildReuseKeepsCallerShortDeadline proves the non-build path does
// NOT inherit the long build timeout: when a `build` service's image already
// exists, the resolve short-circuits and its ImageInspect call carries the
// caller's short reconcile deadline, not buildTimeout.
func TestServiceBuildReuseKeepsCallerShortDeadline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}

	inspectDeadline := &buildDeadline{}
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/",
			body:      `{"Id":"sha256:abc"}`,
			onRequest: inspectDeadline.capture,
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	shortCtx, shortCancel := context.WithTimeout(context.Background(), reconcileShort)
	defer shortCancel()
	if _, err := m.ResolveServiceImage(shortCtx, "fn", dir, &function.Template{Runtime: "node24"}, svc, ""); err != nil {
		t.Fatalf("resolve (reuse): %v", err)
	}
	if !inspectDeadline.seen || !inspectDeadline.ok {
		t.Fatal("expected the reuse ImageInspect to carry a deadline")
	}
	if remaining := time.Until(inspectDeadline.at); remaining > reconcileShort {
		t.Fatalf("reuse ImageInspect deadline bound = %v, must not inherit the %v build timeout", remaining, buildTimeout)
	}
}

// TestLifecycleCancellationCancelsActiveBuild proves an active Dockerfile build
// is cancelled promptly when the manager lifecycle is cancelled (Relay
// shutdown), while the build remains independently bounded by buildTimeout.
func TestLifecycleCancellationCancelsActiveBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	entered := make(chan struct{})
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(_ *http.Request) { close(entered) },
			fail: func(req *http.Request) error {
				// Block until the build's context is cancelled, exactly like a
				// long-running daemon image build interrupted by shutdown, then
				// surface the context error.
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)
	m := newLifecycleManager(t, cli, lifecycle)

	done := make(chan error, 1)
	go func() {
		_, err := m.ResolveServiceImage(context.Background(), "fn", dir, &function.Template{Runtime: "node24"}, svc, "")
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ImageBuild was not entered")
	}

	start := time.Now()
	cancelLifecycle()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("build error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the active build promptly")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("build took %v to cancel after lifecycle cancel; want prompt cancellation", elapsed)
	}
}

// TestServiceImageSourceKeepsCallerShortDeadline proves a non-build service
// source (`image`) does NOT inherit the long build timeout: its ImageInspect is
// issued under the caller's short reconcile deadline, because only Dockerfile
// builds get the independent 10m bound.
func TestServiceImageSourceKeepsCallerShortDeadline(t *testing.T) {
	svc := function.Service{Image: "ghcr.io/acme/api:1.2", Port: 80, Replicas: 1}

	inspectDeadline := &buildDeadline{}
	cli := newScriptedDockerClient(t,
		dockerRoute{
			method: http.MethodGet, path: "/images/",
			body:      `{"Id":"sha256:cafe"}`,
			onRequest: inspectDeadline.capture,
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	// Record a successful pull check so the resolve does not attempt a pull.
	m.recordPullCheck("fn", svc.Image, time.Now())

	shortCtx, shortCancel := context.WithTimeout(context.Background(), reconcileShort)
	defer shortCancel()
	if _, err := m.ResolveServiceImage(shortCtx, "fn", t.TempDir(), &function.Template{Runtime: "node24"}, svc, ""); err != nil {
		t.Fatalf("resolve (image source): %v", err)
	}
	if !inspectDeadline.seen || !inspectDeadline.ok {
		t.Fatal("expected the image-source ImageInspect to carry a deadline")
	}
	if remaining := time.Until(inspectDeadline.at); remaining > reconcileShort {
		t.Fatalf("image-source ImageInspect deadline bound = %v, must not inherit the %v build timeout", remaining, buildTimeout)
	}
}

// TestManagerCloseCancelsOwnedBuildLifecycle proves a Manager constructed
// without WithLifecycleContext still owns a build lifecycle that Close cancels,
// so a direct caller also gets shutdown-cancellable builds.
func TestManagerCloseCancelsOwnedBuildLifecycle(t *testing.T) {
	m := &Manager{
		log:        testutil.DiscardLogger(),
		containers: newContainerCache(),
	}
	// Mirror NewManager's owned-root fallback.
	m.lifecycle, m.lifecycleCancel = context.WithCancel(context.Background())

	buildCtx, cancel := m.buildContext()
	defer cancel()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-buildCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close must cancel the manager-owned build lifecycle")
	}
	if !errors.Is(buildCtx.Err(), context.Canceled) {
		t.Fatalf("build ctx err = %v, want context.Canceled", buildCtx.Err())
	}
}

//go:build integration

// These tests drive image retirement against a real Docker daemon. They need no
// Redis: they exercise the runner's retirement chain (the in-flight refcount and
// the Relay-owned container reference guard) against real daemon state, because
// the leak they pin — an image left behind once its bounded retry window elapsed
// — only appears when the reference check consults a real daemon.
package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// imageExistsInRunnerDaemon reports whether the daemon has the exact image ref.
func imageExistsInRunnerDaemon(ctx context.Context, cli *client.Client, ref string) bool {
	_, err := cli.ImageInspect(ctx, ref)
	return err == nil
}

// buildScratchImage builds a tiny scratch image tagged ref, so a test never
// has to pull a base image. It fails the test when the image is not present
// after the build, so a silently failed build cannot make a later assertion
// vacuous.
func buildScratchImage(ctx context.Context, t *testing.T, cli *client.Client, ref string) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	dockerfile := "FROM scratch\nCMD [\"/nonexistent\"]\n"
	hdr := &tar.Header{Name: "Dockerfile", Mode: 0o644, Size: int64(len(dockerfile))}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	if _, err := tw.Write([]byte(dockerfile)); err != nil {
		t.Fatalf("tar write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	resp, err := cli.ImageBuild(ctx, bytes.NewReader(buf.Bytes()), client.ImageBuildOptions{
		Tags:       []string{ref},
		Dockerfile: "Dockerfile",
	})
	if err != nil {
		t.Fatalf("build scratch image %s: %v", ref, err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("drain scratch build %s: %v", ref, err)
	}
	if !imageExistsInRunnerDaemon(ctx, cli, ref) {
		t.Fatalf("scratch image %s was not produced by the build", ref)
	}
}

// TestIntegrationDeferredRetirementReDrivenAfterServiceContainerStops is the
// end-to-end proof of the fixed leak against a real daemon: a retired app image
// is still referenced by a real Relay-owned (service-labelled) container, so the
// bounded removal retries exhaust and the image is retained as deferred. When
// that container is replaced (removed here), a lifecycle re-drive removes the
// image — so the image is not left behind merely because the initial retry
// window elapsed.
func TestIntegrationDeferredRetirementReDrivenAfterServiceContainerStops(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const appName = "retire-drive"
	img := fmt.Sprintf("relay-app-%s:%d", appName, time.Now().UnixNano())
	buildScratchImage(ctx, t, cli, img)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = cli.ImageRemove(c, img, client.ImageRemoveOptions{Force: true})
	})

	// A real Relay-owned (service) container referencing the image, exactly the
	// state a persistent service is in while it still runs the superseded image.
	created, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image: img,
			Labels: map[string]string{
				"relay.type":     runtime.ContainerTypeService,
				"relay.image":    img,
				"relay.app":      appName,
				"relay.service":  appName,
				"relay.identity": "service.js",
				"relay.hostname": "test-host",
			},
		},
	})
	if err != nil {
		t.Fatalf("create holder container: %v", err)
	}
	holderRemoved := false
	removeHolder := func() {
		if holderRemoved {
			return
		}
		holderRemoved = true
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := cli.ContainerRemove(c, created.ID, client.ContainerRemoveOptions{Force: true}); err != nil {
			t.Logf("remove holder container: %v", err)
		}
	}
	t.Cleanup(removeHolder)

	mgr, err := runtime.NewManager(testutil.DiscardLogger(), nil, "test-host")
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// The registry serves the app's NEW image while the test retires the OLD one
	// (img), exactly as production does: the reconciler swaps the registry to the
	// new image BEFORE it retires the superseded one. A retired image the
	// registry still served would (correctly) have its retirement cancelled.
	newImage := fmt.Sprintf("relay-app-%s:%d-new", appName, time.Now().UnixNano())
	buildScratchImage(ctx, t, cli, newImage)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = cli.ImageRemove(c, newImage, client.ImageRemoveOptions{Force: true})
	})

	fn := app.App{Name: appName, Template: &app.Template{Runtime: "node24"}}
	pf := NewPrepared(fn, &runtime.Prepared{Name: appName, Image: newImage}, mgr)
	r := NewWithMetrics([]*PreparedApp{pf}, testutil.DiscardLogger(), nil)
	// Short bounded retries: the chain exhausts quickly, so the deferral is
	// reached without a long wait. The tester never sleeps: the attempt seam
	// signals each completed classification.
	r.imageCleanupRetryDelays = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}

	// The chain has 1 initial attempt + len(delays) retries before it defers,
	// then one attempt for the re-drive.
	const deferAt = 3
	deferred := make(chan struct{})
	redrivenAttempt := make(chan struct{})
	var attempts atomic.Int32
	r.imageCleanupAttemptDone = func(string) {
		switch n := attempts.Add(1); {
		case n == deferAt:
			close(deferred)
		case n == deferAt+1:
			close(redrivenAttempt)
		}
	}

	r.RetireImage(img)
	select {
	case <-deferred:
	case <-time.After(2 * time.Minute):
		t.Fatalf("the bounded retry chain never exhausted (attempts: %d)", attempts.Load())
	}

	// The image is still there (a real container references it) and its removal
	// is retained rather than abandoned.
	if !imageExistsInRunnerDaemon(ctx, cli, img) {
		t.Fatal("the image referenced by a live service container must not be removed")
	}
	if got := r.refs.deferredImages(); len(got) != 1 || got[0] != img {
		t.Fatalf("deferred images = %v, want [%s]", got, img)
	}

	// The service container is replaced: the reference clears.
	removeHolder()

	r.RetryDeferredImageRemovals()
	select {
	case <-redrivenAttempt:
	case <-time.After(2 * time.Minute):
		t.Fatalf("the re-driven removal attempt never completed (attempts: %d)", attempts.Load())
	}
	if imageExistsInRunnerDaemon(ctx, cli, img) {
		t.Fatal("the deferred image must be removed once its last service container is gone")
	}
	if got := r.refs.deferredImages(); len(got) != 0 {
		t.Fatalf("deferred images after the re-driven removal = %v, want none", got)
	}
}

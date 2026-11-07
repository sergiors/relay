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
//
// Remove is true, like production (runtime's buildImageOptions): the moby
// client emits rm=0 when Remove is false, suppressing the daemon's default
// cleanup, so every cache-missing fixture build would leave its
// classic-builder "#(nop) CMD" metadata step behind as a Created container on
// the shared daemon. ForceRemove stays unset, mirroring production: an
// intermittent fixture-build failure keeps its intermediates visible for
// debugging while the test fails.
//
// The Dockerfile is a parameter so the fixture-build contract can be tested
// with content-attributable (nonce) builds; the retirement fixtures pass
// retireScratchDockerfile.
func buildScratchImage(ctx context.Context, t *testing.T, cli *client.Client, ref, dockerfile string) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
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
		Remove:     true,
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

// retireScratchDockerfile is the fixed scratch fixture the retirement tests
// build: a bare scratch image whose CMD points at a nonexistent binary, so an
// executed attempt fails fast while Prepare/retirement logic still sees a real
// image.
const retireScratchDockerfile = "FROM scratch\nCMD [\"/nonexistent\"]\n"

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
	buildScratchImage(ctx, t, cli, img, retireScratchDockerfile)
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
				"relay.node_id":  "test-host",
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
	buildScratchImage(ctx, t, cli, newImage, retireScratchDockerfile)
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = cli.ImageRemove(c, newImage, client.ImageRemoveOptions{Force: true})
	})

	fn := app.App{Name: appName, Template: &app.Template{Runtime: "nodejs24.x"}}
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

// TestIntegrationBuildScratchImageLeavesNoIntermediateContainers pins for the
// runner's fixture helper buildScratchImage the same no-leak contract the
// runtime suite pins for its buildTestImage helper: a successful fixture build
// must issue the same rm=1 (ImageBuildOptions.Remove) cleanup the production
// builder uses, so unmanaged fixture images do not leak classic-builder
// "#(nop) CMD ..." Created containers onto the shared daemon — the exact
// intermediate shape the deferred-retirement fixtures themselves were observed
// leaving before the fix (dockerfile CMD ["/nonexistent"]).
//
// Attribution is by command CONTENT (a nonce CMD instruction), neither by
// image ancestry (a scratch intermediate commits a config-only layer, so its
// Summary.ImageID is empty and layer-set matching is blind) nor by a
// whole-daemon before/after delta (go test ./... runs packages in parallel, so
// concurrent foreign builds create their own intermediates). No other
// container can carry the nonce's value, and the nonce also makes a rerun a
// genuinely new build: the daemon caches build chains by content, and a
// cache-hit build runs no step containers at all, so reusing the fixed
// retireScratchDockerfile would leave nothing for the contract to observe —
// and pre-existing leaked intermediates of the SAME shape from runs before the
// fix carry a different command and can never match.
func TestIntegrationBuildScratchImageLeavesNoIntermediateContainers(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	nonce := fmt.Sprintf("relay-runner-noprune-%d", time.Now().UnixNano())
	ref := fmt.Sprintf("relay-runner-noprune-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = cli.ImageRemove(cleanupCtx, ref, client.ImageRemoveOptions{Force: true})
	})

	buildScratchImage(ctx, t, cli, ref, "FROM scratch\nCMD [\""+nonce+"\"]\n")
	if !imageExistsInRunnerDaemon(ctx, cli, ref) {
		t.Fatalf("scratch image %s must exist after the helper build; a vacuously failed build cannot prove cleanup", ref)
	}

	leaked, listErr := awaitNoFixtureIntermediate(ctx, cli, nonce, 30*time.Second)
	if listErr != nil {
		// Fail-closed: without a single successful container list, "no leak"
		// was never observed — an unreadable daemon must fail the test rather
		// than pass for it.
		t.Fatalf("could not verify the fixture build left no intermediate (no successful container list): %v", listErr)
	}
	if len(leaked) > 0 {
		t.Errorf("buildScratchImage leaked %d classic-builder intermediate container(s) "+
			"(a Remove:false/rm=0 regression in the helper): %v", len(leaked), leaked)
	}
}

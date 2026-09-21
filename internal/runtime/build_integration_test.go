//go:build integration

// Build integration tests: intermediate-container cleanup across rebuilds,
// failed-build behavior, and concurrent dependency builds on a shared daemon.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/testutil"
)

// isClassicBuilderIntermediate reports whether a container's Config.Cmd matches
// the shape of a classic-builder (V1) intermediate container: a metadata-only
// step (`#(nop) ...`) or a `/bin/sh -c` step container. The daemon represents
// the step command either as a single string (`/bin/sh -c <cmd>`) or split into
// separate argv elements (`["/bin/sh","-c","<cmd>"]`), so both forms are
// detected. Relay execution containers never match this shape (their Cmd is the
// handler entrypoint), so any such container that is not relay-labeled is a
// leaked build intermediate.
func isClassicBuilderIntermediate(cmd []string) bool {
	for i, part := range cmd {
		if strings.Contains(part, "#(nop)") {
			return true
		}
		if strings.Contains(part, "/bin/sh -c") {
			return true
		}
		// Split argv form: ["/bin/sh","-c","<cmd>"].
		if part == "/bin/sh" && i+1 < len(cmd) && cmd[i+1] == "-c" {
			return true
		}
	}
	return false
}

// isClassicBuilderIntermediateCmd is isClassicBuilderIntermediate for the
// flat command string a ContainerList Summary carries (Summary has no Config;
// the daemon joins the argv with spaces). Same detection, one argument.
func isClassicBuilderIntermediateCmd(command string) bool {
	return strings.Contains(command, "#(nop)") || strings.Contains(command, "/bin/sh -c")
}

// TestIntegrationRebuildLeavesNoIntermediateContainers verifies that rebuilding
// a function (v1 -> v2 -> v3) does not leak classic-builder intermediate
// containers. It snapshots the daemon's container set before each build and
// asserts that no NEW non-relay-labeled container matching the classic-builder
// intermediate shape remains after the build. It exercises the real daemon path
// (Manager.Prepare -> buildImage -> ImageBuild).
func TestIntegrationRebuildLeavesNoIntermediateContainers(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	fn := function.Function{Name: "rebuild-int", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	// Track the relay-fn-* images this test creates so t.Cleanup can remove them
	// (intermediates are expected to be gone by the fix; if the test fails they
	// are left visible for debugging). t.Cleanup runs after the test's deferred
	// cancel() has fired, so use a fresh context here rather than the cancelled
	// test ctx.
	var createdImages []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, ref := range createdImages {
			cleanupImage(cli, cleanupCtx, ref)
		}
	})

	// Build v1, then v2, then v3. After each build assert no new intermediate
	// container remains. Each version writes distinct source so every Prepare
	// produces a distinct fingerprint and a real rebuild (not a cache reuse).
	versions := []string{"v1", "v2", "v3"}
	for _, ver := range versions {
		// Write this version's distinct source before building it.
		writeFile(t, dir, "index.js", "export function hi(e){ console.log('"+ver+"'); }\n")
		// Snapshot the container set BEFORE this build, and count the
		// classic-intermediate-shaped non-relay containers so we can assert no
		// growth. The count is scoped to the intermediate SHAPE (not all
		// non-relay containers) because the daemon is global: unrelated
		// transient containers (another package's concurrent tests under
		// `go test ./...`, the daemon's own async AutoRemove of an earlier
		// test's container, buildkit helpers) can appear in the build window
		// and must never fail this assertion — only a genuine intermediate
		// leak should. The per-container shape check below is the primary
		// assertion; this count is the redundant secondary signal.
		preList, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("snapshot containers before %s: %v", ver, err)
		}
		before := make(map[string]bool, len(preList.Items))
		interBefore := 0
		for _, c := range preList.Items {
			before[c.ID] = true
			if _, ok := c.Labels[labelFunction]; !ok && isClassicBuilderIntermediateCmd(c.Command) {
				interBefore++
			}
		}

		prepared, err := mPrepare(ctx, t, fn)
		if err != nil {
			t.Fatalf("prepare %s: %v", ver, err)
		}
		createdImages = append(createdImages, prepared.Image)

		// The current function image must exist after the build.
		if !imageExistsInDaemon(cli, ctx, prepared.Image) {
			t.Fatalf("image %s should exist after %s build", prepared.Image, ver)
		}

		// After the build, list containers and classify any that are NEW (not in
		// the pre-build snapshot) and NOT relay-labeled.
		after, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Fatalf("list containers after %s: %v", ver, err)
		}
		var newNonRelay []string
		interAfter := 0
		for _, c := range after.Items {
			if _, ok := c.Labels[labelFunction]; !ok {
				if isClassicBuilderIntermediateCmd(c.Command) {
					interAfter++
				}
			}
			if before[c.ID] {
				continue // pre-existing; not ours
			}
			if _, ok := c.Labels[labelFunction]; ok {
				continue // a Relay execution container, not a build intermediate
			}
			newNonRelay = append(newNonRelay, c.ID)
		}

		// Any new non-relay container must NOT be a classic-builder intermediate.
		// Inspect each to classify it; a leaked intermediate fails the test.
		for _, id := range newNonRelay {
			insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatalf("inspect new container %s after %s: %v", id, ver, err)
			}
			var cmd []string
			if insp.Container.Config != nil {
				cmd = insp.Container.Config.Cmd
			}
			if isClassicBuilderIntermediate(cmd) {
				t.Errorf("leaked classic-builder intermediate container %s after %s build (Cmd %v)", id, ver, cmd)
			}
		}

		// The count of classic-intermediate-shaped non-relay containers must not
		// grow across the rebuild (no linear accumulation of intermediates).
		if interAfter > interBefore {
			t.Errorf("intermediate-shaped container count grew across %s build: before=%d after=%d "+
				"(leaked intermediates)", ver, interBefore, interAfter)
		}
	}
}

// TestIntegrationFailedBuildKeepsIntermediatesAndPropagatesError verifies that
// a failed build (a) surfaces the real build-stream error through Prepare and
// (b) intentionally KEEPS its classic-builder intermediate containers for
// debugging (the daemon only removes intermediates when the build completed
// successfully; Remove:true does not apply to failed builds). It uses a
// malformed package.json so the node engine's `npm install --omit=dev` step
// fails deterministically.
func TestIntegrationFailedBuildKeepsIntermediatesAndPropagatesError(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e){ console.log('hi'); }\n")
	// Malformed package.json: the engine's `npm install --omit=dev` step must
	// fail parsing it, failing the build deterministically.
	writeFile(t, dir, "package.json", "{ not json")

	fn := function.Function{Name: "failed-build-int", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	// Snapshot the container set BEFORE the failed build.
	preList, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("snapshot containers before failed build: %v", err)
	}
	before := make(map[string]bool, len(preList.Items))
	for _, c := range preList.Items {
		before[c.ID] = true
	}

	// Prepare must fail. mPrepare returns the Prepare error (it only t.Fatalfs on
	// NewManager failure), so a non-nil error here is the expected outcome.
	prepared, err := mPrepare(ctx, t, fn)
	if err == nil {
		t.Fatalf("expected Prepare to fail on malformed package.json, got success (image %s)", prepared.Image)
	}
	t.Logf("prepare error: %v", err)

	// The error must propagate the build stream: assert it carries the npm
	// failure text, proving drainBuildResponse surfaced the stream error rather
	// than a silent success. Keep to a stable substring observed on the daemon.
	if !strings.Contains(err.Error(), "npm") && !strings.Contains(err.Error(), "JSON") {
		t.Errorf("expected the build-stream error to mention npm/JSON, got: %v", err)
	}

	// After the failed build, at least one NEW classic-builder intermediate
	// container must remain (failed builds keep intermediates for debugging).
	after, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("list containers after failed build: %v", err)
	}
	var leakedIntermediates []string
	for _, c := range after.Items {
		if before[c.ID] {
			continue // pre-existing; not ours
		}
		insp, err := cli.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect new container %s after failed build: %v", c.ID, err)
		}
		var cmd []string
		if insp.Container.Config != nil {
			cmd = insp.Container.Config.Cmd
		}
		if isClassicBuilderIntermediate(cmd) {
			leakedIntermediates = append(leakedIntermediates, c.ID)
		}
	}
	if len(leakedIntermediates) == 0 {
		t.Error("expected at least one classic-builder intermediate container to remain after the " +
			"failed build (failed builds keep intermediates for debugging)")
	}
	t.Logf("failed build left %d intermediate container(s): %v", len(leakedIntermediates), leakedIntermediates)

	// Cleanup: force-remove the leftover intermediate containers and any
	// relay-fn-failed-build-int:* image the failed build created. t.Cleanup runs
	// after the test's deferred cancel() has fired, so use a fresh context.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, id := range leakedIntermediates {
			_ = removeContainer(cli, id)
		}
		// Remove any tagged relay-fn-failed-build-int:* image (the failed build
		// may or may not have produced a tagged image).
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{All: true})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-fn-failed-build-int:") {
					cleanupImage(cli, cleanupCtx, tag)
					break
				}
			}
		}
	})
}

// TestIntegrationConcurrentDepBuilds verifies two concurrent Prepare calls for
// the same function version (two Manager instances, as two worker replicas
// would) both succeed and leave exactly ONE NEW dependency image tag — the
// shared, content-addressed layer is built once even under a build race. Only
// the delta vs a snapshot taken at test start is counted, so unrelated
// relay-dep-* layers from other tests/workers on the shared daemon are ignored.
func TestIntegrationConcurrentDepBuilds(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Snapshot the pre-existing dep images so the assertions AND the cleanup
	// count only what THIS test's concurrent builds add. Register cleanup FIRST
	// (before any Fatalf) so a mid-test failure never leaks the dep-layer nor
	// function images into the sibling tests that follow on the shared daemon.
	depBefore := depTagSet(ctx, cli)
	t.Cleanup(cleanupNewDepImagesSince(cli, depBefore))
	t.Cleanup(cleanupImagePrefixes(cli, "relay-fn-dep-race:"))

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event):\n    print('ok')\n")
	writeFile(t, dir, "requirements.txt", "six==1.16.0\n")
	fn := function.Function{Name: "dep-race", Dir: dir, Template: &function.Template{Runtime: "python3.14"}}

	start := make(chan struct{})
	errs := make(chan error, 2)
	results := make(chan *Prepared, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
			if err != nil {
				errs <- err
				return
			}
			defer m.Close()
			p, err := m.Prepare(ctx, fn)
			if err != nil {
				errs <- err
				return
			}
			results <- p
		}()
	}
	close(start)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent prepare failed: %v", err)
		case p := <-results:
			// Two concurrent Prepare calls build the SAME function image
			// ref. The second build can briefly un-tag/rebuild the ref while
			// the daemon finishes, so poll for the ref to exist rather than
			// asserting at the instant this goroutine finished.
			if !pollUntil(ctx, 20*time.Second, func() bool { return imageExistsInDaemon(cli, ctx, p.Image) }) {
				t.Fatalf("concurrently prepared image %s must exist", p.Image)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for concurrent prepares")
		}
	}

	// Exactly one NEW relay-dep-* image may exist after the race (both goroutines
	// built the same fingerprint; Docker racing same-content builds => one tag).
	// Count only the delta vs the snapshot so unrelated relay-dep-* layers from
	// other tests/workers on the shared daemon are ignored.
	newDeps := newDepTagsSince(ctx, cli, depBefore)
	if len(newDeps) != 1 {
		t.Errorf("expected exactly one new dependency image after concurrent builds, got %v", newDeps)
	}
}

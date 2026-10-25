//go:build integration

// Build integration tests: intermediate-container cleanup across rebuilds,
// failed-build behavior, and concurrent dependency builds on a shared daemon.
package runtime

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/app"
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

// imageSet snapshots every local image ID -> ParentID on the daemon in ONE list
// call (including dangling intermediate images). It is taken once per test, not
// per build: ImageList(All:true) is O(all images on the daemon), so the per-build
// attribution below uses ImageHistory (one image's chain) instead.
func imageSet(ctx context.Context, cli *client.Client) map[string]string {
	list, err := cli.ImageList(ctx, client.ImageListOptions{All: true})
	if err != nil {
		return nil
	}
	parents := make(map[string]string, len(list.Items))
	for _, img := range list.Items {
		parents[img.ID] = img.ParentID
	}
	return parents
}

// buildOwnLayers returns the image layer IDs committed by the build that
// produced built: the layers in built's history that did NOT exist before the
// build (the known set), stopping at the first pre-existing layer (the FROM base
// or a cache-reused ancestor). ImageHistory is a single-image call, far cheaper
// than listing every image, and it yields the exact chain including the base's
// layers — so the same boundary logic applies without a per-build ImageList.
//
// The layer chain is the deterministic build identity: every classic-builder
// step commits a layer whose ID is the step's intermediate container's
// Summary.ImageID. Two different builds only share a layer ID when their content
// AND ancestry are byte-identical, so a concurrent build of different source can
// never alias into these IDs.
func buildOwnLayers(ctx context.Context, cli *client.Client, built string, known map[string]string) map[string]bool {
	hist, err := cli.ImageHistory(ctx, built)
	if err != nil {
		return nil
	}
	own := make(map[string]bool)
	// History is ordered newest-first; the base's layers continue to the end. We
	// include every layer that was not already known, and stop at the first known
	// layer (everything below it is pre-existing base/cache).
	for _, h := range hist.Items {
		if h.ID == "" || strings.Contains(h.ID, "<missing>") {
			continue // base layers built by the image producer carry no local ID
		}
		if _, existed := known[h.ID]; existed {
			break
		}
		own[h.ID] = true
	}
	return own
}

// rememberLayers adds every layer ID in built's history to known, so the next
// build's attribution treats the just-built image's layers as pre-existing
// (they are neither a leak nor another build's).
func rememberLayers(ctx context.Context, cli *client.Client, known map[string]string, built string) {
	if known == nil {
		return
	}
	if hist, err := cli.ImageHistory(ctx, built); err == nil {
		for _, h := range hist.Items {
			if h.ID != "" && !strings.Contains(h.ID, "<missing>") {
				known[h.ID] = ""
			}
		}
	}
}

// ourClassicIntermediates returns the IDs of classic-builder intermediate
// containers attributable to THIS build: non-relay containers whose command
// matches the intermediate shape and that ran in one of the build's own new
// layers (own). Foreign concurrent builds' intermediates ran in THEIR own
// layers, which are absent from this set.
func ourClassicIntermediates(ctx context.Context, cli *client.Client, own map[string]bool) []string {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil
	}
	var out []string
	for _, c := range list.Items {
		if _, ok := c.Labels[labelApp]; ok {
			continue // a Relay execution container, not a build intermediate
		}
		if !isClassicBuilderIntermediateCmd(c.Command) {
			continue
		}
		if own[c.ImageID] {
			out = append(out, c.ID)
		}
	}
	return out
}

// TestIntegrationRebuildLeavesNoIntermediateContainers verifies that rebuilding
// an app (v1 -> v2) does not leak classic-builder intermediate containers.
// After each build it computes the image layers that build created (from the
// built image's history, minus the pre-build layer set) and asserts no
// intermediate-shaped container ran in any of them. It exercises the real daemon
// path (Manager.Prepare -> buildImage -> ImageBuild).
//
// Two versions is the minimum that proves a REBUILD: the second, changed-source
// build is where a missing Remove:true / rm=1 regression would leave a step
// container behind. (The old three-version loop only re-ran the same assertion a
// third time.)
//
// Attribution by layer ancestry (not a whole-daemon before/after delta) is what
// makes this deterministic on a shared daemon: another package running
// concurrently under `go test ./...` creates its own classic intermediates, and
// those must never be mistaken for this build's leak. A foreign build's step
// containers run in that foreign build's layers, which are absent from this
// build's parent chain, so they are ignored. The daemon removes a successful
// classic build's intermediates asynchronously as it proceeds, so the assertion
// polls (bounded) for this build's own layers to settle rather than sampling a
// single instant.
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
	fn := app.App{Name: "rebuild-int", Dir: dir, Template: &app.Template{Runtime: "node24"}}

	// Track the relay-app-* images this test creates so t.Cleanup can remove them
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

	// Snapshot the image set ONCE before the first build so the first build's
	// own new layers can be identified; later builds extend the known set with
	// their own output (rememberLayers) rather than re-listing every image.
	known := imageSet(ctx, cli)

	// Build v1 then v2. Each version writes source carrying a per-run nonce so
	// every Prepare produces a genuinely NEW layer chain, not a cache reuse: the
	// assertion must observe a build's intermediates, and a reused image leaves
	// nothing to attribute. A fixed marker would be a cache hit on a re-run
	// (`-count>1`) or when another process builds the same content on the shared
	// daemon, making `buildOwnLayers` find no new layer and the test fail for a
	// reason unrelated to the intermediate-cleanup contract under test.
	nonce := time.Now().UnixNano()
	versions := []string{"v1", "v2"}
	for _, ver := range versions {
		writeFile(t, dir, "index.js", "export function hi(e){ console.log('"+ver+"-"+strconv.FormatInt(nonce, 10)+"'); }\n")

		// Remove this version's target image first (it is this test's own
		// relay-app-rebuild-int:* namespace) so each Prepare is a REAL build, not
		// a cache reuse: the assertion must observe a build's intermediates, and
		// a reused image would leave nothing to attribute.
		fp, err := app.Fingerprint(dir)
		if err != nil {
			t.Fatalf("fingerprint %s: %v", ver, err)
		}
		ref := ImageRef(fn.Name, fp)
		cleanupImage(cli, ctx, ref)

		prepared, err := mPrepare(ctx, t, fn)
		if err != nil {
			t.Fatalf("prepare %s: %v", ver, err)
		}
		createdImages = append(createdImages, prepared.Image)

		if !imageExistsInDaemon(cli, ctx, prepared.Image) {
			t.Fatalf("image %s should exist after %s build", prepared.Image, ver)
		}

		own := buildOwnLayers(ctx, cli, prepared.Image, known)
		if len(own) == 0 {
			t.Fatalf("could not attribute any new image layer to the %s build of %s; "+
				"cannot prove intermediates were pruned", ver, prepared.Image)
		}
		// The just-built image's layers become pre-existing for the next build.
		rememberLayers(ctx, cli, known, prepared.Image)

		// Poll (bounded) for THIS build's intermediates to be gone: the daemon
		// prunes a successful classic build's intermediates asynchronously. In
		// the healthy path they are already gone by the time ImageBuild returns,
		// so this costs a single list; the budget is an upper bound, never a
		// delay normally paid.
		var leaked []string
		if !pollUntil(ctx, 30*time.Second, func() bool {
			leaked = ourClassicIntermediates(ctx, cli, own)
			return len(leaked) == 0
		}) {
			t.Errorf("leaked %d classic-builder intermediate container(s) from the %s build "+
				"(build layers %d; a Remove:false/rm=0 regression): %v", len(leaked), ver, len(own), leaked)
		}
	}
}

// TestIntegrationFailedBuildKeepsIntermediatesAndPropagatesError verifies that
// a failed build (a) surfaces the real build-stream error through Prepare and
// (b) intentionally KEEPS its classic-builder intermediate containers for
// debugging (the daemon only removes intermediates when the build completed
// successfully; Remove:true does not apply to failed builds). It uses a
// malformed package.json so the node engine's `pnpm install --prod
// --frozen-lockfile` step fails deterministically.
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
	// A committed pnpm lock is required for a package.json to reach the install
	// step; the malformed manifest then fails `pnpm install --frozen-lockfile`
	// deterministically (instead of the engine's missing-lock plan error).
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	// Malformed package.json: the engine's `pnpm install --prod --frozen-lockfile`
	// step must fail parsing it, failing the build deterministically.
	writeFile(t, dir, "package.json", "{ not json")

	fn := app.App{Name: "failed-build-int", Dir: dir, Template: &app.Template{Runtime: "node24"}}

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

	// The error must propagate the build stream: assert it carries the pnpm/JSON
	// failure text, proving drainBuildResponse surfaced the stream error rather
	// than a silent success. Keep to a stable substring observed on the daemon.
	if !strings.Contains(err.Error(), "pnpm") && !strings.Contains(err.Error(), "JSON") && !strings.Contains(err.Error(), "package.json") {
		t.Errorf("expected the build-stream error to mention pnpm/JSON/package.json, got: %v", err)
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
	// relay-app-failed-build-int:* image the failed build created. t.Cleanup runs
	// after the test's deferred cancel() has fired, so use a fresh context.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, id := range leakedIntermediates {
			_ = removeContainer(cli, id)
		}
		// Remove any tagged relay-app-failed-build-int:* image (the failed build
		// may or may not have produced a tagged image).
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "relay-app-failed-build-int:") {
					cleanupImage(cli, cleanupCtx, tag)
					break
				}
			}
		}
	})
}

// TestIntegrationConcurrentManagersResolveSameDependencyReference verifies that
// two independent Managers concurrently preparing identical dependency content
// both succeed and resolve to the SAME valid content-addressed dependency image
// reference. It does NOT assert global physical build deduplication or
// build-once behavior: the Managers are independent and may each perform build
// work; the contract is only that identical content resolves to one shared
// reference and that reference names a valid Docker image. The dependency
// reference is derived deterministically from the manifest with the production
// helpers; a whole-daemon before/after tag delta would race any other test/worker
// building the same content-addressed manifest on the shared daemon.
func TestIntegrationConcurrentManagersResolveSameDependencyReference(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	appName := testutil.UniqueName(t, "dep-race")
	var wg sync.WaitGroup
	// One cleanup owns the concurrent test's teardown: it joins BOTH Prepare
	// goroutines (each closes its Manager before signaling done) and only then
	// removes this test's app image, so no image cleanup can race an active
	// Prepare. The app name is unique per invocation (both Managers still share
	// it), so parallel package processes cannot remove or retag each other's app
	// image. Dependency layers are content-addressed and shared daemon-wide; a
	// baseline delta is not proof of exclusive ownership, so they are
	// intentionally retained rather than deleted here.
	t.Cleanup(func() {
		wg.Wait()
		cleanupImagePrefixes(cli, "relay-app-"+appName+":")()
	})

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
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "python3.14"}}
	wantDep := expectedDependencyRef(t, fn)

	// managerOutcome keeps every failure dimension of one independent Manager
	// (construction, Prepare, close) together with its index, so diagnostics can
	// name the Manager that failed without losing the underlying error.
	type managerOutcome struct {
		manager       int
		prepared      *Prepared
		newManagerErr error
		prepareErr    error
		closeErr      error
	}
	start := make(chan struct{})
	outcomes := make(chan managerOutcome, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(manager int) {
			// Done runs after this goroutine has closed its Manager and sent its
			// outcome, so cleanup observes fully joined work.
			defer wg.Done()
			<-start
			m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
			if err != nil {
				outcomes <- managerOutcome{manager: manager, newManagerErr: err}
				return
			}
			p, prepErr := m.Prepare(ctx, fn)
			closeErr := m.Close()
			outcomes <- managerOutcome{
				manager:    manager,
				prepared:   p,
				prepareErr: prepErr,
				closeErr:   closeErr,
			}
		}(i)
	}
	close(start)
	deps := make(map[string]bool, 2)
	for i := 0; i < 2; i++ {
		select {
		case o := <-outcomes:
			if o.newManagerErr != nil {
				t.Fatalf("manager %d: NewManager failed: %v", o.manager, o.newManagerErr)
			}
			if o.prepareErr != nil {
				t.Fatalf("manager %d: Prepare(app %q) failed: %v (Close error: %v)",
					o.manager, fn.Name, o.prepareErr, o.closeErr)
			}
			if o.closeErr != nil {
				t.Fatalf("manager %d: Close after preparing app %q failed: %v", o.manager, fn.Name, o.closeErr)
			}
			p := o.prepared
			if p == nil {
				t.Fatalf("manager %d: Prepare returned no result for app %q", o.manager, fn.Name)
			}
			// Both independent Managers prepare the same app ref (same unique app
			// and fingerprint). One build can briefly retag the ref while the
			// daemon finishes, so poll rather than asserting at the instant this
			// Manager finished; report the last inspection error with the ref.
			var inspectErr error
			if !pollUntil(ctx, 20*time.Second, func() bool {
				_, inspectErr = cli.ImageInspect(ctx, p.Image)
				return inspectErr == nil
			}) {
				t.Fatalf("manager %d: prepared app image %q for app %q must exist: %v",
					o.manager, p.Image, fn.Name, inspectErr)
			}
			// Both Managers must resolve to the SAME dependency reference: identical
			// dependency content maps to one content-addressed reference.
			if p.Dependency != wantDep {
				t.Fatalf("manager %d: Prepare(app %q) resolved dependency %q, want shared content-addressed reference %q (app image %q)",
					o.manager, fn.Name, p.Dependency, wantDep, p.Image)
			}
			deps[p.Dependency] = true
		case <-ctx.Done():
			t.Fatalf("timed out waiting for both concurrent prepares of app %q: %v", fn.Name, ctx.Err())
		}
	}
	if len(deps) != 1 {
		t.Errorf("expected exactly one dependency reference after concurrent prepares, got %v", deps)
	}
	if _, err := cli.ImageInspect(ctx, wantDep); err != nil {
		t.Errorf("shared dependency image %q resolved for app %q must exist and be a valid Docker image: %v",
			wantDep, fn.Name, err)
	}
}

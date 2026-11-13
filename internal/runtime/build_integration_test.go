//go:build integration

// Build integration tests: intermediate-container cleanup across rebuilds,
// failed-build behavior, and concurrent dependency builds on a shared daemon.
package runtime

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// isClassicBuilderIntermediateCmd reports whether the flat command string of a
// container-list summary matches the shape of a classic-builder (V1)
// intermediate container: a metadata-only step (`#(nop) ...`) or a `/bin/sh -c`
// step container. Summary has no Config, so the daemon's joined command string
// is the only form available. Relay execution containers never match this shape
// (their command is the handler entrypoint), so any such container that is not
// relay-labeled is a leaked build intermediate.
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
runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	fn := app.App{Name: "rebuild-int", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}

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

	// One real Manager/State pair prepares BOTH versions: this contract is about
	// the daemon's build/intermediate-container behavior across rebuilds, not
	// manager independence, so a fresh manager (and State schema) per build adds
	// nothing the assertions can see.
	m, err := newCoordinatedRealManager(t, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

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
		writeFile(t, dir, "index.js", "export function hi(e, context){ console.log('"+ver+"-"+strconv.FormatInt(nonce, 10)+"'); }\n")

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

		prepared, err := m.Prepare(ctx, fn)
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

// TestIntegrationBuildTestImageLeavesNoIntermediateContainers pins for the
// shared fixture helper buildTestImage the same no-leak contract
// TestIntegrationRebuildLeavesNoIntermediateContainers pins for the production
// builder. The helper issues its own ImageBuild over unmanaged relay-named
// fixture images (relay-dep-evil:1, relay-unrelated-guard), so a
// Remove:false/rm=0 regression there would leave one classic-builder
// "#(nop) CMD ..." Created container on the shared daemon per cache-missing
// build — the exact intermediate shape the production path is proven not to
// leave.
//
// Attribution is by command CONTENT, neither by image ancestry nor by a
// whole-daemon before/after delta. Image ancestry cannot work for these
// scratch fixtures: a scratch intermediate commits a config-only layer and its
// Summary.ImageID is empty, so layer-set matching is blind; a whole-daemon
// delta would mistake another package's concurrently created intermediates
// (go test ./... runs packages in parallel) for this build's leak. The classic
// builder embeds a metadata step's content verbatim in the step container's
// command ("/bin/sh -c #(nop) CMD [...]"), so a nonce CMD instruction in the
// fixture Dockerfile is attributable to this one build alone — no other
// container on the daemon can carry its value.
//
// Fresh content additionally keeps the assertion non-vacuous across reruns:
// the daemon caches the build chain by content, and a cache-hit build re-runs
// no step containers at all, so reusing the shared `CMD []` fixture Dockerfile
// could silently observe nothing to clean. The nonce makes every invocation a
// genuinely new build.
func TestIntegrationBuildTestImageLeavesNoIntermediateContainers(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	nonce := fmt.Sprintf("relay-buildtest-noprune-%d", time.Now().UnixNano())
	ref := fmt.Sprintf("relay-buildtest-noprune-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, ref)
	})

	buildTestImage(ctx, t, ref, "FROM scratch\nCMD [\""+nonce+"\"]\n")
	if !imageExistsInDaemon(cli, ctx, ref) {
		t.Fatalf("fixture image %s must exist after the helper build; a vacuously failed build cannot prove cleanup", ref)
	}

	leaked, listErr := awaitNoFixtureIntermediate(ctx, cli, nonce, 30*time.Second)
	if listErr != nil {
		// Fail-closed: without an IN-WINDOW successful container read, "no
		// leak" was never observed — a stalled or unreadable daemon fails the
		// test instead of passing or hanging it past the stated window.
		t.Fatalf("could not verify the fixture build left no intermediate (no in-window container read): %v", listErr)
	}
	if len(leaked) > 0 {
		t.Errorf("buildTestImage leaked %d classic-builder intermediate container(s) "+
			"(a Remove:false/rm=0 regression in the helper): %v", len(leaked), leaked)
	}
}

// TestIntegrationFailedBuildPropagatesError verifies that a failed build
// surfaces the real build-stream error through Prepare. It uses a malformed
// package.json so the node engine's `pnpm install --prod --frozen-lockfile`
// step fails deterministically. Production builds select BuildKit, which keeps
// failed-step state in its cache rather than in classic-builder intermediate
// containers, so this test asserts the failure contract (the stream error
// reaches the caller) and does not look for container-visible leftovers.
func TestIntegrationFailedBuildPropagatesError(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function hi(e, context){ console.log('hi'); }\n")
	// A committed pnpm lock is required for a package.json to reach the install
	// step; the malformed manifest then fails `pnpm install --frozen-lockfile`
	// deterministically (instead of the engine's missing-lock plan error).
	writeFile(t, dir, "pnpm-lock.yaml", nodePnpmLockEmpty)
	// Malformed package.json: the engine's `pnpm install --prod --frozen-lockfile`
	// step must fail parsing it, failing the build deterministically.
	writeFile(t, dir, "package.json", "{ not json")

	// Per-invocation unique app identity: cleanup below removes only this run's
	// relay-app-<appName>:* tags, never another invocation's image namespace.
	appName := testutil.UniqueName(t, "failed-build-int")
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}

	// Scoped cleanup registered BEFORE the build, ownership-checked and
	// error-reporting:
	//   - app images: only tags on images whose EXISTING Relay labels prove
	//     relay.type == ImageTypeApp AND relay.app == this run's unique
	//     appName — never a repo prefix, never unlabeled or foreign images.
	//   - ImageList/ImageRemove errors are reported with t.Errorf; NotFound
	//     on an ImageRemove is benign (the failed build may tag nothing).
	// The context is fresh and bounded: t.Cleanup runs after the test's
	// deferred cancel() has fired.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		// This test's own FAILED build produced (at most) one tagged
		// relay-app-<appName>:* image; list ONLY the daemon-side label-filtered
		// candidates for this app (relay.type=app AND relay.app=<appName>), then
		// re-check the ownership proof on each listed image — never a repo
		// prefix, never an unfiltered whole-daemon enumeration.
		imgs, err := cli.ImageList(cleanupCtx, client.ImageListOptions{
			Filters: make(client.Filters).
				Add("label", labelType+"="+ImageTypeApp, labelApp+"="+appName),
		})
		if err != nil {
			t.Errorf("cleanup: list images: %v", err)
			return
		}
		for _, img := range imgs.Items {
			if img.Labels[labelType] != ImageTypeApp || img.Labels[labelApp] != appName {
				continue // not this test's app image; untouchable
			}
			for _, tag := range img.RepoTags {
				if _, err := cli.ImageRemove(cleanupCtx, tag, client.ImageRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
					t.Errorf("cleanup: remove app image tag %s: %v", tag, err)
				}
			}
		}
	})

	// Prepare must fail. mPrepare returns the Prepare error (it only t.Fatalfs on
	// NewManager failure), so a non-nil error here is the expected outcome.
	prepared, err := mPrepare(ctx, t, fn)
	if err == nil {
		t.Fatalf("expected Prepare to fail on malformed package.json, got success (image %s)", prepared.Image)
	}
	errText := fmt.Sprint(err)
	t.Logf("prepare error: %v", err)

	// The error must propagate the build stream: assert it carries the pnpm/JSON
	// failure text, proving drainBuildResponse surfaced the stream error rather
	// than a silent success. Keep to a stable substring observed on the daemon.
	if !strings.Contains(errText, "pnpm") && !strings.Contains(errText, "JSON") && !strings.Contains(errText, "package.json") {
		t.Errorf("expected the build-stream error to mention pnpm/JSON/package.json, got: %v", err)
	}
}

// TestIntegrationBuildKitResolvesRemoteBaseThroughSession covers the
// "failed to resolve source metadata: no active sessions" regression against a
// real daemon. Relay forces the BuildKit build path, which resolves a base
// image's metadata through the client session named by
// ImageBuildOptions.SessionID. Daemons whose BuildKit predates tolerating an
// absent session fail that resolution; this test removes the base locally so
// the metadata must come from the registry, then builds through the same
// session-attaching path production uses. On a newer daemon the session-less
// build also succeeds, so the negative case is not observable here; the
// session-carrying request itself is pinned by
// TestRunImageBuildNamesBuildKitSession.
func TestIntegrationBuildKitResolvesRemoteBaseThroughSession(t *testing.T) {
	cli := testutil.RequireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const base = "busybox:1.36.1"
	if _, err := cli.ImageRemove(ctx, base, client.ImageRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
		t.Fatalf("remove cached base %s: %v", base, err)
	}
	if imageExistsInDaemon(cli, ctx, base) {
		t.Fatalf("base %s still exists locally; the metadata-resolution assertion would be vacuous", base)
	}

	// Re-run the affected request without a session first. Older BuildKit
	// daemons reproduce the regression here; newer daemons tolerate the missing
	// session, so accept success only as a daemon-version difference. In either
	// case, remove the image again before the session-attached assertion below.
	ctxDir := t.TempDir()
	writeFile(t, ctxDir, "Dockerfile", "FROM "+base+"\nRUN true\n")
	withoutSessionRef := fmt.Sprintf("relay-buildkit-no-session-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, withoutSessionRef)
	})
	if err := runImageBuild(ctx, cli, "buildkit-no-session", ctxDir, withoutSessionRef, nil, nil, ""); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "no active sessions") {
			t.Fatalf("session-less BuildKit build failed for an unexpected reason: %v", err)
		}
		t.Logf("reproduced session-less BuildKit failure: %v", err)
	} else {
		t.Log("daemon tolerated a BuildKit build without a session")
	}
	if _, err := cli.ImageRemove(ctx, base, client.ImageRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
		t.Fatalf("remove base %s after session-less build: %v", base, err)
	}
	if imageExistsInDaemon(cli, ctx, base) {
		t.Fatalf("base %s is still local after the session-less build", base)
	}

	ref := fmt.Sprintf("relay-buildkit-session-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(cli, cleanupCtx, ref)
	})

	// A directly constructed Manager suffices: buildWithSession only needs the
	// client and the session hook NewManager installs in production.
	m := &Manager{cli: cli, startBuildSessionFn: connectBuildSession}
	var sessionID string
	err := m.buildWithSession(ctx, "buildkit-session", func(buildCtx context.Context, id string) error {
		sessionID = id
		return runImageBuild(buildCtx, cli, "buildkit-session", ctxDir, ref, nil, nil, id)
	})
	if err != nil {
		t.Fatalf("BuildKit build with an attached session must resolve the remote base %s: %v", base, err)
	}
	if sessionID == "" {
		t.Fatal("the production build path did not attach a BuildKit session")
	}
	if !imageExistsInDaemon(cli, ctx, ref) {
		t.Fatalf("image %s should exist after the BuildKit build", ref)
	}
}

// TestIntegrationBuildKitStreamsUnknownLengthContext verifies the streamed
// ImageBuild request does not stall when BuildKit starts its response before a
// larger context has been fully uploaded. Moby uses chunked transfer for the
// io.PipeReader context; the production Docker client sends the standard
// Expect: 100-continue header to avoid the observed HTTP/1 request/response
// stall.
func TestIntegrationBuildKitStreamsUnknownLengthContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	m, err := NewManager(
		testutil.DiscardLogger(), nil, "test-host", WithLifecycleContext(ctx),
	)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	image := fmt.Sprintf("relay-buildkit-stream:%d", time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupImage(m.cli, cleanupCtx, image)
		_ = m.Close()
	}()

	ctxDir := t.TempDir()
	writeFile(t, ctxDir, "Dockerfile", "FROM scratch\nCOPY payload.bin /payload.bin\n")
	payload, err := os.Create(filepath.Join(ctxDir, "payload.bin"))
	if err != nil {
		t.Fatalf("create build-context file: %v", err)
	}
	if err := payload.Truncate(1 << 20); err != nil {
		_ = payload.Close()
		t.Fatalf("size build-context file: %v", err)
	}
	if err := payload.Close(); err != nil {
		t.Fatalf("close build-context file: %v", err)
	}

	_, err = m.runImageBuild(ctx, "buildkit-stream", image, func(buildCtx context.Context, sessionID string) error {
		return runImageBuild(buildCtx, m.cli, "buildkit-stream", ctxDir, image, nil, nil, sessionID)
	})
	if err != nil {
		t.Fatalf("streamed BuildKit context should build successfully: %v", err)
	}
	if !imageExistsInDaemon(m.cli, ctx, image) {
		t.Fatalf("image %s should exist after the BuildKit build", image)
	}
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

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: python3.14
events:
  - handler: handler.run
    pattern:
      status: [COMPLETED]
`)
	writeFile(t, dir, "handler.py", "def run(event, context):\n    print('ok')\n")
	// The marker makes this dependency reference unique to this test invocation,
	// so a warm daemon cannot turn the concurrent dependency-build assertion into
	// an app-image-only cache-hit test.
	writeFile(t, dir, "requirements.txt", "# "+appName+"\nsix==1.16.0\n")
	fn := app.App{Name: appName, Dir: dir, Template: &app.Template{Runtime: "python3.14"}}
	wantDep := expectedDependencyRef(t, fn)
	if imageExistsInDaemon(cli, ctx, wantDep) {
		t.Fatalf("per-run dependency image %s must not pre-exist", wantDep)
	}
	t.Cleanup(func() {
		cleanupImagePrefixes(cli, "relay-app-"+appName+":", wantDep)()
	})

	// managerOutcome keeps every failure dimension of one independent Manager
	// (Prepare, close) together with its index, so diagnostics can name the
	// Manager that failed without losing the underlying error.
	type managerOutcome struct {
		manager    int
		prepared   *Prepared
		prepareErr error
		closeErr   error
	}
	// Construction happens ON THE TEST GOROUTINE, before any concurrent work:
	// newCoordinatedRealManager creates per-Manager State handles
	// (testutil.NewStateDSN/OpenState) and may t.Fatalf / register cleanup,
	// which is only safe there. The goroutines keep ONLY Prepare + Close
	// concurrent; the handles remain fully independent (own State schema, own
	// Docker client), sharing just the identical app fixture. Their lifecycle is
	// tied to ctx so a test deadline closes active BuildKit sessions before the
	// cleanup join waits for both goroutines.
	mgrs := make([]*Manager, 2)
	for i := range mgrs {
		m, err := newCoordinatedRealManager(
			t, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
			WithLifecycleContext(ctx),
		)
		if err != nil {
			t.Fatalf("manager %d: NewManager failed: %v", i, err)
		}
		mgrs[i] = m
	}
	// Registered after each manager's State cleanup so the join runs before
	// those handles close. Each Prepare goroutine closes its Manager before it
	// signals done; image cleanup runs later, after both State handles close.
	t.Cleanup(func() { wg.Wait() })

	start := make(chan struct{})
	outcomes := make(chan managerOutcome, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(manager int) {
			// Done runs after this goroutine has closed its Manager and sent its
			// outcome, so cleanup observes fully joined work.
			defer wg.Done()
			<-start
			p, prepErr := mgrs[manager].Prepare(ctx, fn)
			closeErr := mgrs[manager].Close()
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

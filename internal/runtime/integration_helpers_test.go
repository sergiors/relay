//go:build integration

// Shared helpers for the Docker integration suite (client/manager construction,
// image/container listing, cleanup, bounded polling, hardened HostConfig
// assertions). They require a real daemon reachable via client.FromEnv.
package runtime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/function"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// newManager returns a Manager wired to a logger that writes into the returned
// buffer, capturing Relay operational logs for assertions. The manager owns
// hostname "test-host" so container-ownership tests are deterministic. Close is
// registered as cleanup: Manager.Close discards the per-function reused
// execution containers it may have started.
//
// Handler stdout/stderr is NO LONGER routed through the logger (it is forwarded
// as a raw transport to the function-output sink; see output.go and
// newFunctionOutputSink), so handler-output assertions must read from that sink,
// not from this operational log buffer. The logger level is nevertheless kept at
// DEBUG here so the operational-line assertions these tests make are unaffected.
func newManager(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m, err := NewManager(l, nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, &buf
}

// cleanupImage removes an image, best-effort.
func cleanupImage(cli *client.Client, ctx context.Context, ref string) {
	_, _ = cli.ImageRemove(ctx, ref, client.ImageRemoveOptions{Force: true})
}

// findContainerByLabel scans All containers for one carrying the exact
// relay.<key>=<value> label, returning its ID or "". It is a client-side filter
// matching the sweep's own ownership predicate.
func findContainerByLabel(ctx context.Context, cli *client.Client, key, value string) string {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return ""
	}
	for _, c := range list.Items {
		if c.Labels[key] == value {
			return c.ID
		}
	}
	return ""
}

// countContainersByLabel counts All containers carrying the exact
// relay.<key>=<value> label. It complements findContainerByLabel for
// pool-size assertions (at most one container per function before Phase 2,
// up to the function's concurrency now).
func countContainersByLabel(ctx context.Context, cli *client.Client, key, value string) int {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return 0
	}
	n := 0
	for _, c := range list.Items {
		if c.Labels[key] == value {
			n++
		}
	}
	return n
}

// waitForContainersCount polls until exactly want running-or-exited containers
// carry the given label pair, or the deadline passes.
func waitForContainersCount(ctx context.Context, cli *client.Client, key, value string, want int) bool {
	return pollUntil(ctx, 15*time.Second, func() bool {
		return countContainersByLabel(ctx, cli, key, value) == want
	})
}

// waitForContainerGone polls until no running-or-exited container carries the
// given label pair, or the deadline passes. AutoRemove removes the container on
// exit asynchronously: ContainerWait delivers the exit code the moment the
// process stops, but the daemon's removal completes a beat later, so callers
// must await removal rather than assert it at the instant Execute returns.
// It returns true once the container is gone.
func waitForContainerGone(ctx context.Context, cli *client.Client, key, value string) bool {
	return pollUntil(ctx, 10*time.Second, func() bool {
		return findContainerByLabel(ctx, cli, key, value) == ""
	})
}

// waitForContainerByLabel polls until a (running) container carries the exact
// relay.<key>=<value> label, returning its ID or "". The reuse container stays
// running between invocations, so polling is only needed to bridge the start
// gap.
func waitForContainerByLabel(ctx context.Context, cli *client.Client, key, value string) string {
	var id string
	pollUntil(ctx, 15*time.Second, func() bool {
		id = findContainerByLabel(ctx, cli, key, value)
		return id != ""
	})
	return id
}

// waitForContainerRunning polls until the container with the given id reports
// State.Running, or fails the test after a bounded wait.
func waitForContainerRunning(t *testing.T, ctx context.Context, cli *client.Client, id string) {
	t.Helper()
	if !pollUntil(ctx, 30*time.Second, func() bool {
		insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		return err == nil && insp.Container.State != nil && insp.Container.State.Running
	}) {
		t.Fatalf("container %s did not reach State.Running within 30s", id)
	}
}

// assertHardenedHostConfig checks the daemon-applied HostConfig against the
// hardenedHostConfig security baseline. It is the shared assertion for the
// invocation-container hardening test and the service-container lifecycle test.
func assertHardenedHostConfig(t *testing.T, hc *container.HostConfig) {
	t.Helper()
	if hc == nil {
		t.Fatal("inspect returned nil HostConfig")
	}
	if hc.Memory != 128<<20 {
		t.Errorf("memory limit = %d, want %d", hc.Memory, 128<<20)
	}
	if hc.NanoCPUs != 1_000_000_000 {
		t.Errorf("nano cpus = %d, want %d", hc.NanoCPUs, 1_000_000_000)
	}
	if hc.PidsLimit == nil || *hc.PidsLimit != 128 {
		t.Errorf("pids limit = %v, want 128", hc.PidsLimit)
	}
	if len(hc.CapDrop) != 1 || hc.CapDrop[0] != "ALL" {
		t.Errorf("cap drop = %v, want [ALL]", hc.CapDrop)
	}
	if !hc.ReadonlyRootfs {
		t.Error("expected read-only rootfs")
	}
	if hc.Tmpfs["/tmp"] != "rw,nosuid,noexec,size=64m" {
		t.Errorf("tmpfs = %v, want /tmp rw,nosuid,noexec,size=64m", hc.Tmpfs)
	}
	if len(hc.PortBindings) != 0 {
		t.Errorf("expected no host port bindings, got %v", hc.PortBindings)
	}
}

// imageExistsInDaemon reports whether a local image carries exactly ref.
func imageExistsInDaemon(cli *client.Client, ctx context.Context, ref string) bool {
	_, err := cli.ImageInspect(ctx, ref)
	return err == nil
}

// mPrepare builds a function via a fresh Manager wired to a discard logger.
func mPrepare(ctx context.Context, t *testing.T, fn function.Function) (*Prepared, error) {
	t.Helper()
	m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.Prepare(ctx, fn)
}

// buildTestImage builds a tiny image with the given tag and an inline Dockerfile
// via the Engine API, used to create a non-relay-owned image to prove the sweep
// never touches it.
func buildTestImage(ctx context.Context, t *testing.T, ref, dockerfile string) string {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()
	ctxDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ctxDir, "Dockerfile"), []byte(dockerfile), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	reader, err := tarContext(ctxDir)
	if err != nil {
		t.Fatalf("tar context: %v", err)
	}
	resp, err := cli.ImageBuild(ctx, reader, client.ImageBuildOptions{Tags: []string{ref}, Dockerfile: "Dockerfile"})
	if err != nil {
		t.Fatalf("build unrelated image: %v", err)
	}
	defer resp.Body.Close()
	if _, err := drainBuildResponse(resp.Body); err != nil {
		t.Fatalf("build unrelated image output: %v", err)
	}
	return ref
}

// cleanupImagePrefixes force-removes every local image whose repo tag starts with
// any of the given prefixes. It is t.Cleanup glue so dependency-layer tests never
// leak relay-dep-* / relay-fn-* images onto a shared daemon.
func cleanupImagePrefixes(cli *client.Client, prefixes ...string) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Non-All (tagged images only): every prefix match below is on a
		// RepoTag, and a dangling image has none, so All:dangling would only add
		// ~5s of daemon work per call on a large shared daemon with no effect on
		// the result.
		imgs, err := cli.ImageList(ctx, client.ImageListOptions{})
		if err != nil {
			return
		}
		for _, img := range imgs.Items {
			for _, tag := range img.RepoTags {
				for _, p := range prefixes {
					if strings.HasPrefix(tag, p) {
						cleanupImage(cli, ctx, tag)
						break
					}
				}
			}
		}
	}
}

// depTags lists every local relay-dep-* image tag currently present on the
// daemon.
func depTags(ctx context.Context, cli *client.Client) []string {
	list, err := cli.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		return nil
	}
	var out []string
	for _, img := range list.Items {
		for _, tag := range img.RepoTags {
			if strings.HasPrefix(tag, depRepoPrefix) {
				out = append(out, tag)
			}
		}
	}
	return out
}

// depTagSet snapshots the CURRENT set of relay-dep-* tags on the daemon into a
// map so tests can diff their OWN additions against a baseline. The relay-dep-*
// namespace is content-addressed and shared across every test and worker
// process on a daemon (any Python build with a requirements manifest creates a
// relay-dep-* image), so asserting on whole-daemon relay-dep-* counts is
// inherently racy. Tests must snapshot at test start and assert only on the
// tags that appear SINCE that snapshot.
func depTagSet(ctx context.Context, cli *client.Client) map[string]bool {
	before := make(map[string]bool)
	for _, d := range depTags(ctx, cli) {
		before[d] = true
	}
	return before
}

// newDepTagsSince returns the relay-dep-* tags present NOW but NOT present in
// the given baseline snapshot. On a shared daemon this scopes a dep-layer
// assertion to exactly the images this test's builds created, ignoring relay-dep-*
// layers other tests or workers legitimately created.
func newDepTagsSince(ctx context.Context, cli *client.Client, before map[string]bool) []string {
	var out []string
	for _, d := range depTags(ctx, cli) {
		if !before[d] {
			out = append(out, d)
		}
	}
	return out
}

// cleanupNewDepImagesSince force-removes only the relay-dep-* images that
// appeared since the given baseline snapshot, leaving dep layers built
// concurrently by other tests/workers on the shared daemon untouched. It is
// t.Cleanup glue so dep-image-creating tests clean up after themselves and never
// leak relay-dep-* images into later sibling tests.
func cleanupNewDepImagesSince(cli *client.Client, before map[string]bool) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, d := range newDepTagsSince(ctx, cli, before) {
			cleanupImage(cli, ctx, d)
		}
	}
}

// expectedDependencyRef computes the dependency image reference a function's
// current manifest set must resolve to, using the exact production helpers
// Prepare uses (lookup -> engine Plan -> DependencyFingerprint -> depImageRef).
// Integration tests use it to name the shared, content-addressed dependency
// image deterministically, instead of inferring it from a before/after tag
// delta that is racy when the image already exists on a shared daemon. It fails
// the test if the function declares no dependency layer.
func expectedDependencyRef(t *testing.T, fn function.Function) string {
	t.Helper()
	if fn.Template == nil {
		t.Fatalf("function %q has no template", fn.Name)
	}
	spec, err := lookup(fn.Template.Runtime)
	if err != nil {
		t.Fatalf("lookup %q: %v", fn.Template.Runtime, err)
	}
	eng, err := engineFor(spec)
	if err != nil {
		t.Fatalf("engine for %q: %v", fn.Template.Runtime, err)
	}
	p, err := eng.Plan(spec, fn.Dir, templateHandlers(fn))
	if err != nil {
		t.Fatalf("plan %q: %v", fn.Name, err)
	}
	if p.Deps.IsZero() {
		t.Fatalf("function %q declares no dependency layer", fn.Name)
	}
	fp, err := DependencyFingerprint(arch, platform, spec, fn.Dir, p.Deps)
	if err != nil {
		t.Fatalf("dependency fingerprint %q: %v", fn.Name, err)
	}
	return depImageRef(fp)
}

// depRepoName normalizes a dependency image reference or daemon tag to its
// repository name (tag stripped), so an untagged depImageRef such as
// "relay-dep-<fp>" compares equal to the daemon's RepoTag
// "relay-dep-<fp>:latest".
func depRepoName(ref string) string {
	repo, _, _ := strings.Cut(ref, ":")
	return repo
}

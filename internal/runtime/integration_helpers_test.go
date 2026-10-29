//go:build integration

// Shared helpers for the Docker integration suite (client/manager construction,
// image/container listing, cleanup, bounded polling, hardened HostConfig
// assertions). They require a real daemon reachable via client.FromEnv.
package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// newManager returns a Manager wired to a logger that writes into the returned
// buffer, capturing Relay operational logs for assertions. The manager owns
// nodeID "test-host" so container-ownership tests are deterministic. Close is
// registered as cleanup: Manager.Close discards the per-app reused
// execution containers it may have started.
//
// The manager is constructed with an explicit State-backed dependency-image
// coordinator over a fresh schema on TEST_DATABASE_URL, mirroring production
// wiring: real integration tests share one Docker daemon with other tests,
// other packages, and possibly a live worker, so their dep-bearing Prepare and
// dependency GC must coordinate through the SAME database those processes use.
// A Postgres at TEST_DATABASE_URL is therefore required like the state
// integration suite; a missing database fails here rather than silently
// running an uncoordinated manager.
//
// Handler stdout/stderr is NO LONGER routed through the logger (it is forwarded
// as a raw transport to the app-output sink; see output.go and
// newAppOutputSink), so handler-output assertions must read from that sink,
// not from this operational log buffer. The logger level is nevertheless kept at
// DEBUG here so the operational-line assertions these tests make are unaffected.
func newManager(t *testing.T) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	dsn := testutil.NewStateDSN(t)
	coord := stateDepCoordinator{st: testutil.OpenState(t, dsn)}
	m, err := NewManager(l, nil, "test-host", WithDependencyImageCoordinator(coord))
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
// pool-size assertions (at most one container per app before Phase 2,
// up to the app's concurrency now).
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

// listAppServiceContainers returns every persistent Relay service container
// (relay.type=service) whose relay.app label is exactly appName, read straight
// from the RAW Docker client. It deliberately does not go through
// Manager.ServiceContainerList: a container leaked by a prior run outlives its
// Manager, and a closed Manager refuses service work, which is exactly the state
// a leaked container's owner is left in. The summaries carry the id, image,
// state, and full label set used by the scoped cleanup and the failure
// diagnostics below. A container-list failure is RETURNED, never flattened into
// an empty result: "no matching container" is a meaningful assertion outcome
// here, so an unreadable daemon must not masquerade as one.
func listAppServiceContainers(ctx context.Context, cli *client.Client, appName string) ([]container.Summary, error) {
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("list containers for %s: %w", appName, err)
	}
	var out []container.Summary
	for _, c := range list.Items {
		if isServiceContainer(c.Labels) && c.Labels[labelApp] == appName {
			out = append(out, c)
		}
	}
	return out, nil
}

// describeAppServiceContainers renders every service container for the named app
// as a leading-newline-prefixed multi-line diagnostic (id, image, image id,
// state, and the full label set), " (none)" when there are none, or the
// container-list error when the daemon could not be read. It lists ALL of the
// app's service containers, not merely the service identity an assertion filters
// on, so an unexpected extra service name is still visible in the failure
// output.
func describeAppServiceContainers(ctx context.Context, cli *client.Client, appName string) string {
	containers, err := listAppServiceContainers(ctx, cli, appName)
	if err != nil {
		return fmt.Sprintf(" (list error: %v)", err)
	}
	if len(containers) == 0 {
		return " (none)"
	}
	var b strings.Builder
	for _, c := range containers {
		fmt.Fprintf(&b, "\n  id=%s image=%s image_id=%s state=%s labels=%v",
			c.ID, c.Image, c.ImageID, c.State, c.Labels)
	}
	return b.String()
}

// countAppServiceContainers counts the service containers for one service
// identity: relay.type=service AND relay.app=appName AND relay.service=name. It
// reports a container-list failure as an error so a caller never mistakes an
// unreadable daemon for a zero count. Scoping to the service population (rather
// than every container carrying relay.app) is what makes a rerun deterministic,
// while still counting a leaked OR genuinely duplicated container for THIS
// identity: an assertion still fails on a third replica of the same service, it
// just ignores unrelated event/schedule containers that share the app label.
func countAppServiceContainers(ctx context.Context, cli *client.Client, appName, serviceName string) (int, error) {
	containers, err := listAppServiceContainers(ctx, cli, appName)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, c := range containers {
		if c.Labels[labelService] == serviceName {
			n++
		}
	}
	return n, nil
}

// waitForAppServiceContainersCount polls until exactly want service containers
// for the given app/service identity exist, or the deadline passes. A
// container-list failure never satisfies the wait — not even want == 0 — so a
// broken daemon cannot make an "expect none" assertion pass.
func waitForAppServiceContainersCount(ctx context.Context, cli *client.Client, appName, serviceName string, want int) bool {
	return pollUntil(ctx, 15*time.Second, func() bool {
		n, err := countAppServiceContainers(ctx, cli, appName, serviceName)
		return err == nil && n == want
	})
}

// appServiceContainersDiagnostic renders the current service-container state for
// an assertion failure message: the count for the given service identity plus a
// per-container listing, or the container-list error (never a bare zero) when
// the daemon could not be read.
func appServiceContainersDiagnostic(ctx context.Context, cli *client.Client, appName, serviceName string) string {
	n, err := countAppServiceContainers(ctx, cli, appName, serviceName)
	if err != nil {
		return fmt.Sprintf("unknown (list error: %v)", err)
	}
	return fmt.Sprintf("%d%s", n, describeAppServiceContainers(ctx, cli, appName))
}

// removeAppServiceContainers force-removes every service container for the named
// app through the RAW Docker client, returning the ids it removed and the first
// failure (if any). It is manager-independent, so it is safe BOTH before a
// scenario (isolating a fixed app name from containers a prior run leaked) and
// from t.Cleanup, including after Manager.Close. Removals are best-effort: a
// failure on one container does not stop the others, and because cleanup must
// never fail a test the caller logs the returned error rather than calling
// t.Fatal. A container-list failure removes nothing and is returned, so a
// cleanup can report that it could not even discover the containers.
func removeAppServiceContainers(ctx context.Context, cli *client.Client, appName string) ([]string, error) {
	containers, err := listAppServiceContainers(ctx, cli, appName)
	if err != nil {
		return nil, err
	}
	var removed []string
	var firstErr error
	for _, c := range containers {
		if c.State == container.StateRunning {
			timeout := 10
			if _, err := cli.ContainerStop(ctx, c.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("stop service container %s: %w", c.ID, err)
				}
				continue
			}
		}
		if err := removeContainerContext(ctx, cli, c.ID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove service container %s: %w", c.ID, err)
			}
			continue
		}
		removed = append(removed, c.ID)
	}
	return removed, firstErr
}

// isolateAppServiceContainers removes this test app's stale service containers
// before a scenario, failing the test when they cannot be listed or removed. A
// fixed app name plus whole-daemon container queries makes a leak from a killed
// prior run (cleanup never ran) or from a run whose cleanup raced Manager.Close
// (a closed Manager refuses service work) visible as a phantom extra container;
// this makes every run start clean.
func isolateAppServiceContainers(t *testing.T, ctx context.Context, cli *client.Client, appName string) {
	t.Helper()
	removed, err := removeAppServiceContainers(ctx, cli, appName)
	if err != nil {
		t.Fatalf("remove stale service containers before scenario: %v", err)
	}
	if len(removed) > 0 {
		t.Logf("removed %d stale service container(s) for %s before scenario: %v", len(removed), appName, removed)
	}
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

// mPrepare builds an app via a fresh Manager wired to a discard logger and an
// explicit State-backed dependency-image coordinator (per-test schema on
// TEST_DATABASE_URL), so dep-bearing prepares coordinate like production.
func mPrepare(ctx context.Context, t *testing.T, fn app.App) (*Prepared, error) {
	t.Helper()
	m, err := newCoordinatedRealManager(t, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer m.Close()
	return m.Prepare(ctx, fn)
}

// newCoordinatedRealManager constructs a real-daemon Manager with the
// State-backed dependency-image coordinator over its own schema on the shared
// TEST_DATABASE_URL database, plus the supplied options. It mirrors production
// wiring (the worker always injects the State-backed coordinator). The test
// binary must have a reachable Postgres, exactly like the state integration
// suite: a missing database fails rather than skipping.
func newCoordinatedRealManager(
	t *testing.T,
	logger *slog.Logger,
	registry *metrics.Registry,
	opts ...ManagerOption,
) (*Manager, error) {
	t.Helper()
	dsn := testutil.NewStateDSN(t)
	coord := stateDepCoordinator{st: testutil.OpenState(t, dsn)}
	return NewManager(
		logger, registry, "test-host",
		append([]ManagerOption{WithDependencyImageCoordinator(coord)}, opts...)...,
	)
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
	reader, writer := io.Pipe()
	go func() {
		_ = writer.CloseWithError(writeContextTar(ctx, ctxDir, writer))
	}()
	resp, err := cli.ImageBuild(ctx, reader, client.ImageBuildOptions{Tags: []string{ref}, Dockerfile: "Dockerfile"})
	if err != nil {
		t.Fatalf("build unrelated image: %v", err)
	}
	defer resp.Body.Close()
	if _, _, err := drainBuildResponse(resp.Body); err != nil {
		t.Fatalf("build unrelated image output: %v", err)
	}
	return ref
}

// cleanupImagePrefixes force-removes every local image whose repo tag starts with
// any of the given prefixes. It is t.Cleanup glue so dependency-layer tests never
// leak relay-dep-* / relay-app-* images onto a shared daemon.
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

// depTagSet snapshots the CURRENT set of relay-dep-* tags on the daemon. The
// relay-dep-* namespace is content-addressed and shared across every test and
// worker process on a daemon, so this snapshot is a daemon-wide observation and
// never proof of ownership: tags present since a baseline may have been built
// concurrently by another test or worker. Snapshots are therefore used only for
// non-destructive assertions; they must never drive image deletion.
func depTagSet(ctx context.Context, cli *client.Client) map[string]bool {
	before := make(map[string]bool)
	for _, d := range depTags(ctx, cli) {
		before[d] = true
	}
	return before
}

// newDepTagsSince returns the relay-dep-* tags present NOW but NOT present in
// the given baseline snapshot. It is a non-destructive observation for
// assertions (for example, proving a source-only edit did not add a layer), not
// an ownership claim: another test or worker may have concurrently built and
// tagged a layer since the snapshot. Its result must never be used to delete
// images; shared content-addressed layers are intentionally retained.
func newDepTagsSince(ctx context.Context, cli *client.Client, before map[string]bool) []string {
	var out []string
	for _, d := range depTags(ctx, cli) {
		if !before[d] {
			out = append(out, d)
		}
	}
	return out
}

// expectedDependencyRef computes the dependency image reference an app's
// current manifest set must resolve to, using the exact production helpers
// Prepare uses (lookup -> engine Plan -> DependencyFingerprint -> depImageRef).
// Integration tests use it to name the shared, content-addressed dependency
// image deterministically, instead of inferring it from a before/after tag
// delta that is racy when the image already exists on a shared daemon. It fails
// the test if the app declares no dependency layer.
func expectedDependencyRef(t *testing.T, fn app.App) string {
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

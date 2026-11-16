//go:build relayincontainer

// Targeted in-Docker integration test for the control-network topology. It
// runs the runtime.Manager test process ITSELF inside a Docker container with
// the Docker socket mounted, proving the exact production topology the design
// targets: Relay-in-container self-identifies through Docker's default
// container-ID hostname, auto-attaches itself to its per-node control network,
// and executes Python and Node handlers. Two scenarios are covered:
//
//   - NETWORKS unset: Python runs TWICE against the same prepared app — a
//     single warm container id across both calls plus a module-level call
//     counter reaching call=2 prove warm reuse of one persistent bootstrap
//     process — and Node runs once for language coverage. The execution
//     container is joined to ONLY the control network, proving execution-
//     channel connectivity is independent of any application network — no
//     published ports, no reversed connection direction, no application-
//     network IPC.
//   - NETWORKS set: a temporary operator-owned bridge network hosts a small
//     endpoint container launched from the just-built fixture app image with a
//     Python entrypoint override serving stdlib HTTP under its network alias
//     (no extra image pull — no published ports),
//     a Python handler performs the application-network HTTP request by alias
//     on TWO runs inside the execution container (same warm container id,
//     module-level counter reaching call=2), and the execution container is
//     inspected to confirm it joined BOTH the per-node control network and the
//     application network.
//
// The build tag is isolated on purpose: the normal host-run integration suite
// (-tags=integration) never compiles this file, so Linux CI is unaffected and
// the known macOS host→unpublished-bridge-IP blocker is not worked around here
// (running THESE tests on a macOS host is unsupported by design; they must run
// inside a Linux container, where the control-network address is directly
// routable in-container).
//
// Exact invocation — from the repository root on a Linux host with Docker:
//
//	docker run --rm \
//	  -v /var/run/docker.sock:/var/run/docker.sock \
//	  -v "$PWD":/src -w /src \
//	  -e GOCACHE=/tmp/go-build -e CGO_ENABLED=0 \
//	  golang:1.27-alpine \
//	  go test -tags=relayincontainer -count=1 -timeout 20m -v \
//	    ./internal/runtime -run '^TestRelayInContainerControlNetwork'
//
// Requirements: outbound network access for module/runtime base-image pulls,
// and the default container hostname (do NOT pass --hostname). No Postgres is
// needed: the fixture apps declare no dependency layer, so Prepare never
// touches the cross-process dependency coordinator. The uniquely named control
// network these tests create are removed by their own cleanup (production
// never auto-removes them; the fixture identity is per-run, so removal is pure
// test hygiene), as is the temporary application network and its endpoint
// container. The golang container is never given published ports, and the
// execution containers inherit the hardened HostConfig (asserted below).
package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	cerrdefs "github.com/containerd/errdefs"

	"relay/internal/app"
)

// inContainerTimeout bounds the whole scenario (base-image pulls included).
const inContainerTimeout = 15 * time.Minute

// requireInContainerTopology enforces the preconditions the file header
// documents — Docker's default container-ID hostname resolving to THIS very
// container plus a reachable socket — and returns the client, a
// scenario-bounded context, and this container's full id. A process outside a
// default-hostname container is a wiring error, not a skip, so it fails with
// the invocation to use; osHostname() is the same best-effort resolution the
// production Manager uses ("" on failure). The client close is registered
// FIRST so LIFO runs it last; the context cancellation runs after every
// scenario cleanup registered later.
func requireInContainerTopology(t *testing.T) (*client.Client, context.Context, string) {
	t.Helper()
	hostname := osHostname()
	if !isContainerIDLike(hostname) {
		t.Fatalf("os.Hostname() = %q is not Docker's default container-ID hostname; "+
			"run this test with the docker run command documented in this file's header", hostname)
	}
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker client (socket): %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), inContainerTimeout)
	t.Cleanup(cancel)
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf("docker socket ping: %v", err)
	}
	// Verified self identity: the default hostname must resolve to this very
	// container (hostname match + full-ID prefix) before anything inspects it.
	self, err := cli.ContainerInspect(ctx, hostname, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect this container by its default hostname %q: %v", hostname, err)
	}
	if !isSelfContainer(self.Container, hostname) {
		t.Fatalf("container %q resolved by hostname %q is not this process's container", self.Container.ID, hostname)
	}
	return cli, ctx, self.Container.ID
}

// removeNetworkWithRetry removes a test-owned network, retrying because
// AutoRemove detaches dead containers asynchronously. A not-found counts as
// removed: a partial setup that failed before creating the network (or before
// EnsureControlNetwork reached creation) must finish immediately instead of
// burning the retry bound. It is best-effort: cleanup must never fail a
// passing test, so a network that stays is reported by the caller instead of
// raised here.
func removeNetworkWithRetry(ctx context.Context, cli *client.Client, name string) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		_, err := cli.NetworkRemove(ctx, name, client.NetworkRemoveOptions{})
		if err == nil || cerrdefs.IsNotFound(err) {
			return nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return lastErr
}

// inContainerSingleExecutionID returns the id of the app's ONE warm execution
// container, failing when the pool holds anything but exactly one — with every
// id and state listed — so a second invocation that spawned a replacement
// instead of reusing surfaces as an explicit failure. BOTH labels are
// required: Docker INHERITS an image's labels onto every container created
// from it (verified against a labelled probe image), so the application-
// network endpoint — which runs the fixture app image — also carries
// relay.app=<name> and would match an app-only filter. Execution containers
// uniquely carry this run's relay.node_id (stamped from the manager's node
// identity at create); the endpoint never does. The name also deliberately
// avoids the integration-tagged reusedContainerID helper, which this file must
// compile without.
func inContainerSingleExecutionID(t *testing.T, ctx context.Context, cli *client.Client, appName, nodeID string) string {
	t.Helper()
	filters := make(client.Filters).
		Add("label", labelApp+"="+appName, labelNodeID+"="+nodeID)
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		t.Fatalf("list execution containers for %s: %v", appName, err)
	}
	if len(list.Items) != 1 {
		found := make([]string, 0, len(list.Items))
		for _, item := range list.Items {
			found = append(found, item.ID+" ("+string(item.State)+")")
		}
		t.Fatalf("execution containers for %s = %v, want exactly 1", appName, found)
	}
	return list.Items[0].ID
}

// awaitContainerLog polls one container's NATIVE logs until want appears and
// returns the full snapshot. Handler output trails the invocation (and Python
// stdout into a pipe may flush lazily), so presence is polled exactly like the
// host-run suite's log assertions — never read once.
func awaitContainerLog(t *testing.T, ctx context.Context, cli *client.Client, id, want string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var logs string
	for time.Now().Before(deadline) {
		rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{
			ShowStdout: true, ShowStderr: true,
		})
		if err == nil {
			raw, readErr := io.ReadAll(rc)
			_ = rc.Close()
			if readErr == nil {
				logs = string(raw)
				if strings.Contains(logs, want) {
					return logs
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("container %s logs never contained %q within 10s; logs:\n%s", id, want, logs)
	return logs
}

// TestRelayInContainerControlNetwork proves the full in-container topology:
// this process is a Docker container with a socket, its verified default
// hostname resolves to its own container, EnsureControlNetwork provisions the
// per-node control network and attaches THIS container to it (idempotently
// usable endpoint on the validated network id), and handler execution then
// succeeds over the control network with NETWORKS completely unset. The Python
// handler runs TWICE on the same prepared app: both calls are served by the
// SAME warm container id (the pool never grows past one) and the handler's
// module-level counter reaches call=2, proving one persistent bootstrap
// process served both invocations. The warm execution container is inspected
// afterwards to prove it joined exactly the control network — nothing else —
// and published no ports; Node repeats the same double-run with its own
// module-level counter, so BOTH languages prove warm-container reuse over the
// control connection.
func TestRelayInContainerControlNetwork(t *testing.T) {
	cli, ctx, selfID := requireInContainerTopology(t)

	// A per-run node identity keeps this fixture's control network isolated
	// from any real node (and from other concurrent runs) and lets the cleanup
	// remove exactly what the test created.
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	nodeID := "inctest-" + hex.EncodeToString(suffix[:])
	controlName := controlNetworkName(nodeID)
	pyName := "relay-ic-py-" + hex.EncodeToString(suffix[:])
	nodeName := "relay-ic-node-" + hex.EncodeToString(suffix[:])

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mgr, err := NewManager(logger, nil, nodeID) // NETWORKS deliberately unset
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	// One scenario-owned cleanup: manager close first (discards warm execution
	// containers), then app containers/images, then detach this container and
	// remove the per-run control network. Registered after cli.Close, so LIFO
	// runs it before the client closes. Everything here is best-effort —
	// cleanup must never fail a passing test — except retries, which exist only
	// because AutoRemove detaches asynchronously.
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer scancel()
		_ = mgr.Close()
		for _, appName := range []string{pyName, nodeName} {
			// relay.node_id narrows the label to THIS run's execution
			// containers; image-inherited relay.app alone is not enough (see
			// inContainerSingleExecutionID).
			filters := make(client.Filters).
				Add("label", labelApp+"="+appName, labelNodeID+"="+nodeID)
			if list, lerr := cli.ContainerList(sctx, client.ContainerListOptions{All: true, Filters: filters}); lerr == nil {
				for _, c := range list.Items {
					_, _ = cli.ContainerRemove(sctx, c.ID, client.ContainerRemoveOptions{Force: true})
				}
			}
			imgFilters := make(client.Filters).
				Add("label", labelType+"="+ImageTypeApp, labelApp+"="+appName)
			if imgs, ierr := cli.ImageList(sctx, client.ImageListOptions{Filters: imgFilters}); ierr == nil {
				for _, img := range imgs.Items {
					_, _ = cli.ImageRemove(sctx, img.ID, client.ImageRemoveOptions{Force: true})
				}
			}
		}
		_, _ = cli.NetworkDisconnect(sctx, controlName, client.NetworkDisconnectOptions{
			Container: selfID, Force: true,
		})
		if rerr := removeNetworkWithRetry(sctx, cli, controlName); rerr != nil {
			t.Logf("cleanup: control network %s was not removable (%v); left for the operator", controlName, rerr)
		}
	})

	if len(mgr.networks) != 0 {
		t.Fatalf("NETWORKS = %v, want it unset for this scenario", mgr.networks)
	}

	// The preflight-equivalent provisioning step: create-or-reuse plus
	// self-attachment for this container.
	if err := mgr.EnsureControlNetwork(ctx); err != nil {
		t.Fatalf("EnsureControlNetwork (in-container self-attach): %v", err)
	}
	if mgr.controlNetwork != controlName {
		t.Fatalf("controlNetwork = %q, want the derived %q", mgr.controlNetwork, controlName)
	}
	if mgr.controlSelfID != selfID {
		t.Fatalf("controlSelfID = %q, want this container's full id %q", mgr.controlSelfID, selfID)
	}

	// The daemon-side facts: strict ownership labels, bridge driver, and this
	// container actually attached with a usable endpoint on the validated
	// network id.
	controlInsp, err := cli.NetworkInspect(ctx, controlName, client.NetworkInspectOptions{})
	if err != nil {
		t.Fatalf("inspect provisioned control network: %v", err)
	}
	for key, want := range controlNetworkLabels(nodeID) {
		if got := controlInsp.Network.Labels[key]; got != want {
			t.Errorf("control network label %s = %q, want %q", key, got, want)
		}
	}
	if controlInsp.Network.Driver != "bridge" {
		t.Errorf("control network driver = %q, want bridge", controlInsp.Network.Driver)
	}
	selfAfter, err := cli.ContainerInspect(ctx, selfID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("re-inspect this container after attach: %v", err)
	}
	endpoint := containerNetworks(selfAfter.Container)[controlName]
	if !endpointOnControlNetwork(endpoint, controlInsp.Network) {
		t.Fatalf("this container has no usable endpoint on %s after EnsureControlNetwork: %+v",
			controlName, containerNetworks(selfAfter.Container))
	}
	if endpoint.NetworkID != controlInsp.Network.ID {
		t.Fatalf("self endpoint NetworkID = %q, want the validated %q", endpoint.NetworkID, controlInsp.Network.ID)
	}

	// Idempotency: a second provisioning pass (the restart path) attaches
	// nothing new and keeps the same identity.
	if err := mgr.EnsureControlNetwork(ctx); err != nil {
		t.Fatalf("second EnsureControlNetwork (idempotent reuse): %v", err)
	}
	if mgr.controlSelfID != selfID {
		t.Fatalf("controlSelfID changed across reuse: %q != %q", mgr.controlSelfID, selfID)
	}

	// Python runs TWICE against the same prepared app over the control network
	// with NETWORKS unset: the second call must be served by the SAME warm
	// container (inContainerSingleExecutionID requires the pool to stay at
	// exactly one), and the module-level counter in handler.py must advance to
	// call=2 — the bootstrap's persistent process caches the handler module
	// (sys.modules), so only one process can produce that marker. Node keeps
	// its single-call coverage on the same control network.
	pyPrepared := prepareFixtureApp(ctx, t, mgr, pyName, "python3.14", map[string]string{
		"template.yaml": "runtime: python3.14\nevents:\n  - handler: handler.run\n    pattern:\n      event_name: [INSERT]\n",
		"handler.py": `_calls = 0

def run(event, context):
    global _calls
    _calls += 1
    print("in-container-python-ok call=%d" % _calls)
`,
	})
	executeFixtureApp(ctx, t, mgr, pyPrepared, "handler.run")
	pyContainer := inContainerSingleExecutionID(t, ctx, cli, pyName, nodeID)
	executeFixtureApp(ctx, t, mgr, pyPrepared, "handler.run")
	if reused := inContainerSingleExecutionID(t, ctx, cli, pyName, nodeID); reused != pyContainer {
		t.Fatalf("second Python invocation ran in container %s, want warm reuse of %s", reused, pyContainer)
	}
	awaitContainerLog(t, ctx, cli, pyContainer, "in-container-python-ok call=2")

	// Node runs TWICE the same way: a single warm container id across both
	// calls plus a module-level counter in the native logs — the Node
	// bootstrap caches the dynamic import() promise per resolved module path
	// (bootstrap.mjs moduleCache), so only one persistent process can advance
	// the counter to 2. Python's double-run is above; Node keeps its own
	// language coverage on the same control network.
	nodePrepared := prepareFixtureApp(ctx, t, mgr, nodeName, "nodejs24.x", map[string]string{
		"template.yaml": "runtime: nodejs24.x\nevents:\n  - handler: index.run\n    pattern:\n      event_name: [INSERT]\n",
		"index.js": `let calls = 0;

export function run(e, context){
  calls += 1;
  console.log("in-container-node-ok call=" + calls);
}
`,
	})
	executeFixtureApp(ctx, t, mgr, nodePrepared, "index.run")
	nodeContainer := inContainerSingleExecutionID(t, ctx, cli, nodeName, nodeID)
	executeFixtureApp(ctx, t, mgr, nodePrepared, "index.run")
	if reused := inContainerSingleExecutionID(t, ctx, cli, nodeName, nodeID); reused != nodeContainer {
		t.Fatalf("second Node invocation ran in container %s, want warm reuse of %s", reused, nodeContainer)
	}
	awaitContainerLog(t, ctx, cli, nodeContainer, "in-container-node-ok call=2")

	// The Node app's execution container proves the composition end to end:
	// exactly ONE warm container exists, it joined EXACTLY the control network
	// (nothing else, because NETWORKS is unset), and it published no host
	// ports. Warm reuse itself is proven by the Python and Node double-runs
	// above (same container ids across both calls per language).
	insp, err := cli.ContainerInspect(ctx, nodeContainer, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect execution container: %v", err)
	}
	if insp.Container.HostConfig == nil || len(insp.Container.HostConfig.PortBindings) != 0 {
		t.Fatalf("execution container published ports: %+v", insp.Container.HostConfig)
	}
	joined := make([]string, 0, 2)
	for name := range containerNetworks(insp.Container) {
		joined = append(joined, name)
	}
	if len(joined) != 1 || joined[0] != controlName {
		t.Fatalf("execution container networks = %v, want exactly [%s]", joined, controlName)
	}
	joinedEndpoint := containerNetworks(insp.Container)[controlName]
	if !usableEndpointAddress(joinedEndpoint) {
		t.Fatalf("execution container has no usable control-network address: %+v", joinedEndpoint)
	}
}

// prepareFixtureApp materializes one fixture app on disk and runs Prepare
// through the given scenario manager, failing the test on any error. Splitting
// prepare from execute lets a scenario interpose daemon-side setup (the
// application-network endpoint below) between build and invocation.
func prepareFixtureApp(
	ctx context.Context, t *testing.T, mgr *Manager,
	name, runtimeID string, files map[string]string,
) *Prepared {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	for file, content := range files {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
	}
	fn := app.App{Name: name, Dir: dir, Template: &app.Template{Runtime: runtimeID}}
	prepared, err := mgr.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare %s: %v", name, err)
	}
	return prepared
}

// executeFixtureApp invokes the fixture's declared handler identity (matching
// the template's `handler:` entry); handler success is Execute returning nil.
func executeFixtureApp(
	ctx context.Context, t *testing.T, mgr *Manager, prepared *Prepared, handler string,
) {
	t.Helper()
	if err := mgr.Execute(ctx, prepared, handler,
		[]byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute %s (%s): %v", prepared.Name, handler, err)
	}
}

// awaitEndpointServing polls the endpoint container's NATIVE logs until the
// stdlib HTTP server reports it is listening (bind+listen complete), so the
// handler's first connection attempt targets a live socket. The endpoint
// entrypoint must run python unbuffered (-u) — Python block-buffers stdout
// into the log pipe otherwise, which hides the banner from a still-running
// container (verified against python:3.14-slim: no -u ⇒ empty logs while
// running; with -u ⇒ immediate banner). This is a daemon-side observation
// only: the runner container is deliberately not attached to the application
// network, so no test-side probe ever crosses it. A container that exits
// first fails immediately with its state and captured logs instead of waiting
// out the bound; so does a timeout, which now includes the logs.
func awaitEndpointServing(t *testing.T, ctx context.Context, cli *client.Client, id string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var logs string
	for time.Now().Before(deadline) {
		rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{
			ShowStdout: true, ShowStderr: true,
		})
		if err == nil {
			raw, readErr := io.ReadAll(rc)
			_ = rc.Close()
			if readErr == nil {
				logs = string(raw)
				if strings.Contains(logs, "Serving HTTP") {
					return
				}
			}
		}
		if insp, ierr := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{}); ierr == nil &&
			insp.Container.State != nil && !insp.Container.State.Running {
			t.Fatalf("endpoint container %s exited (status %s, code %d) before reporting readiness; logs:\n%s",
				id, insp.Container.State.Status, insp.Container.State.ExitCode, logs)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("endpoint container %s never reported an active HTTP listener; logs:\n%s", id, logs)
}

// TestRelayInContainerControlNetworkWithApplicationNetwork extends the
// in-container topology with the OPTIONAL NETWORKS layer: a temporary
// operator-owned bridge network (created outside the manager, exactly like
// production NETWORKS infrastructure, and never carrying the execution-control
// role) hosts
// a small endpoint container launched from the already-built fixture app image
// with a Python entrypoint override serving stdlib HTTP under an explicit
// network alias (no extra image pull: BuildKit leaves the FROM base tag
// unaddressable). A
// Python handler inside the execution container performs the
// application-network HTTP request by alias on TWO runs — same warm container
// id across both calls and a module-level counter reaching call=2 prove one
// persistent bootstrap process served both — while the execution CHANNEL still
// runs exclusively over the control network (unit-proven by
// TestConnectExecutionContainerUsesOnlyControlNetwork; here the runner
// container is additionally never attached to the application network). The
// SAME warm execution container is inspected to confirm it joined BOTH the
// per-node control network and the application network, with no other network
// and no published ports; the endpoint container publishes no ports either.
func TestRelayInContainerControlNetworkWithApplicationNetwork(t *testing.T) {
	cli, ctx, selfID := requireInContainerTopology(t)

	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	tag := hex.EncodeToString(suffix[:])
	nodeID := "inctest-" + tag
	controlName := controlNetworkName(nodeID)
	appNetName := "relay-ic-appnet-" + tag
	endpointName := "relay-ic-endpoint-" + tag
	appName := "relay-ic-appnet-py-" + tag
	const endpointAlias = "relay-app-endpoint"

	// The temporary operator-owned application network: created OUTSIDE the
	// manager (Relay never creates operator networks), with no
	// execution-control role label so the workload guard accepts it, and no
	// published ports anywhere.
	if _, err := cli.NetworkCreate(ctx, appNetName, client.NetworkCreateOptions{
		Driver: "bridge",
		Labels: map[string]string{"relay.test": "incontainer-appnet"},
	}); err != nil {
		t.Fatalf("create application network: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Registered IMMEDIATELY after the operator-owned network exists — before
	// NewManager — so every partial failure (a failed manager open above all)
	// still removes it. mgr and endpointID are captured nil/empty-safe, each
	// resource is removed by THIS one closure exactly once (no duplicate
	// removal elsewhere), and removeNetworkWithRetry treats a control network
	// that was never created (Ensure never reached creation) as already gone
	// instead of burning the retry bound. Registered after cli.Close (in the
	// topology helper), so LIFO runs it before the client closes. Everything
	// here is best-effort — cleanup must never fail a passing test — except
	// retries, which exist only because AutoRemove detaches asynchronously.
	var (
		mgr        *Manager
		err        error
		endpointID string // set after the endpoint create below; "" skips removal
	)
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer scancel()
		if mgr != nil {
			_ = mgr.Close()
		}
		// Fixture-app execution containers (label-scoped; relay.node_id keeps
		// the endpoint — which inherits relay.app from the app image — out).
		filters := make(client.Filters).
			Add("label", labelApp+"="+appName, labelNodeID+"="+nodeID)
		if list, lerr := cli.ContainerList(sctx, client.ContainerListOptions{All: true, Filters: filters}); lerr == nil {
			for _, c := range list.Items {
				_, _ = cli.ContainerRemove(sctx, c.ID, client.ContainerRemoveOptions{Force: true})
			}
		}
		// The endpoint runs from the fixture's app image, so the endpoint
		// container must go BEFORE that image is removed (a running container
		// keeps its image in use).
		if endpointID != "" {
			_, _ = cli.ContainerRemove(sctx, endpointID, client.ContainerRemoveOptions{Force: true})
		}
		imgFilters := make(client.Filters).
			Add("label", labelType+"="+ImageTypeApp, labelApp+"="+appName)
		if imgs, ierr := cli.ImageList(sctx, client.ImageListOptions{Filters: imgFilters}); ierr == nil {
			for _, img := range imgs.Items {
				_, _ = cli.ImageRemove(sctx, img.ID, client.ImageRemoveOptions{Force: true})
			}
		}
		// Then this container's control-network attachment and both test-owned
		// networks (their containers are gone by now). A disconnect/remove of a
		// control network a failed setup never created is a not-found, which
		// the retry helper treats as already gone.
		_, _ = cli.NetworkDisconnect(sctx, controlName, client.NetworkDisconnectOptions{
			Container: selfID, Force: true,
		})
		if rerr := removeNetworkWithRetry(sctx, cli, controlName); rerr != nil {
			t.Logf("cleanup: control network %s was not removable (%v); left for the operator", controlName, rerr)
		}
		if rerr := removeNetworkWithRetry(sctx, cli, appNetName); rerr != nil {
			t.Logf("cleanup: application network %s was not removable (%v); left for the operator", appNetName, rerr)
		}
	})

	mgr, err = NewManager(logger, nil, nodeID, WithNetworks([]string{appNetName}))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if len(mgr.networks) != 1 || mgr.networks[0] != appNetName {
		t.Fatalf("NETWORKS = %v, want exactly [%s]", mgr.networks, appNetName)
	}

	// Preflight-equivalent provisioning for this scenario's node identity.
	if err := mgr.EnsureControlNetwork(ctx); err != nil {
		t.Fatalf("EnsureControlNetwork (in-container self-attach): %v", err)
	}
	if mgr.controlNetwork != controlName {
		t.Fatalf("controlNetwork = %q, want the derived %q", mgr.controlNetwork, controlName)
	}
	if mgr.controlSelfID != selfID {
		t.Fatalf("controlSelfID = %q, want this container's full id %q", mgr.controlSelfID, selfID)
	}

	// Topology documentation: this runner container is attached to its control
	// network only — nothing ever attaches it to the application network, so
	// the endpoint reachability below provably originates from the EXECUTION
	// container, not from the test process.
	selfTopology, err := cli.ContainerInspect(ctx, selfID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect this container: %v", err)
	}
	if _, onAppNet := containerNetworks(selfTopology.Container)[appNetName]; onAppNet {
		t.Fatalf("this test container must not be attached to the application network %s", appNetName)
	}

	// Prepare FIRST: the endpoint below runs from THIS just-built app image,
	// so the build must precede the container create.
	prepared := prepareFixtureApp(ctx, t, mgr, appName, "python3.14", map[string]string{
		"template.yaml": "runtime: python3.14\nevents:\n  - handler: handler.run\n    pattern:\n      event_name: [INSERT]\n",
		"handler.py": `import time
import urllib.request

_calls = 0

def run(event, context):
    global _calls
    url = "http://` + endpointAlias + `:8000/"
    last = None
    for _ in range(10):
        try:
            response = urllib.request.urlopen(url, timeout=2)
            body = response.read()
            _calls += 1
            print("app-endpoint-ok call=%d status=%s bytes=%d" % (_calls, response.status, len(body)))
            return
        except Exception as exc:
            last = exc
            time.sleep(1)
    raise RuntimeError("application endpoint unreachable: %s" % last)
`,
	})
	// The small test endpoint launches from the ALREADY-BUILT fixture app
	// image with the bootstrap entrypoint fully replaced by the stdlib HTTP
	// server: Prepare's BuildKit build does NOT leave the FROM base tag
	// (python:3.14-slim) locally addressable — ContainerCreate rejects it with
	// "No such image" — while the app image it produced is guaranteed local,
	// so the endpoint needs no extra image pull and no new dependency. The
	// entrypoint runs python UNBUFFERED (-u), matching the engine's own
	// unbuffered-bootstrap rationale: Python block-buffers stdout into the log
	// pipe otherwise, hiding the readiness banner from a still-running
	// container. Cmd is explicitly cleared so the base image's inherited CMD
	// cannot append positional arguments to the overridden entrypoint. It
	// serves HTTP on the application network under an explicit alias,
	// unprivileged on port 8000 (>1024) as the image's non-root user; the
	// merged config is asserted below. HostConfig stays zero-valued — no
	// published ports, nothing privileged.
	createResp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: endpointName,
		Config: &container.Config{
			Image:      prepared.Image,
			Entrypoint: []string{"python", "-u", "-m", "http.server", "8000"},
			Cmd:        []string{},
			Labels:     map[string]string{"relay.test": "incontainer-endpoint"},
		},
		NetworkingConfig: &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				appNetName: {Aliases: []string{endpointAlias}},
			},
		},
	})
	if err != nil {
		t.Fatalf("create endpoint container: %v", err)
	}
	endpointID = createResp.ID
	if _, err := cli.ContainerStart(ctx, endpointID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start endpoint container: %v", err)
	}

	// Fail fast on the MERGED container config before waiting on readiness: the
	// override must fully replace the image bootstrap entrypoint (including the
	// -u unbuffered flag) and the inherited base CMD must be cleared so it can
	// never append positional arguments to http.server.
	endpointInsp, err := cli.ContainerInspect(ctx, endpointID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect endpoint container: %v", err)
	}
	if endpointInsp.Container.Config == nil {
		t.Fatal("endpoint inspect returned a nil Config")
	}
	wantEntrypoint := "python -u -m http.server 8000"
	if got := strings.Join(endpointInsp.Container.Config.Entrypoint, " "); got != wantEntrypoint {
		t.Fatalf("endpoint merged Entrypoint = %q, want %q", got, wantEntrypoint)
	}
	if len(endpointInsp.Container.Config.Cmd) != 0 {
		t.Fatalf("endpoint merged Cmd = %v, want it cleared (no inherited base CMD)", endpointInsp.Container.Config.Cmd)
	}
	if endpointInsp.Container.HostConfig != nil && len(endpointInsp.Container.HostConfig.PortBindings) != 0 {
		t.Fatalf("endpoint container published ports: %+v", endpointInsp.Container.HostConfig)
	}
	endpointNet := containerNetworks(endpointInsp.Container)[appNetName]
	if endpointNet == nil {
		t.Fatalf("endpoint container is not on the application network: %+v",
			containerNetworks(endpointInsp.Container))
	}
	aliasFound := false
	for _, alias := range endpointNet.Aliases {
		if alias == endpointAlias {
			aliasFound = true
		}
	}
	if !aliasFound {
		t.Fatalf("endpoint network aliases = %v, want %q registered", endpointNet.Aliases, endpointAlias)
	}
	awaitEndpointServing(t, ctx, cli, endpointID)

	// The handler connects to the endpoint BY ITS NETWORK ALIAS from inside
	// the execution container and runs TWICE: each run performs its own
	// application-network HTTP request (a raised exception — endpoint
	// unreachable — fails Execute and therefore the test), the second run must
	// be served by the SAME warm container, and the module-level counter must
	// reach call=2 with at least two successful HTTP markers — the persistent
	// bootstrap process keeps the handler module (sys.modules) across
	// invocations, so only one process can produce that pair.
	executeFixtureApp(ctx, t, mgr, prepared, "handler.run")
	appContainer := inContainerSingleExecutionID(t, ctx, cli, appName, nodeID)
	executeFixtureApp(ctx, t, mgr, prepared, "handler.run")
	if reused := inContainerSingleExecutionID(t, ctx, cli, appName, nodeID); reused != appContainer {
		t.Fatalf("second invocation ran in container %s, want warm reuse of %s", reused, appContainer)
	}
	logs := awaitContainerLog(t, ctx, cli, appContainer, "app-endpoint-ok call=2")
	if strings.Count(logs, "app-endpoint-ok") < 2 {
		t.Fatalf("want two successful HTTP runs in the warm container's logs; logs:\n%s", logs)
	}

	// The SAME warm execution container proves the composition: joined BOTH the
	// per-node control network and the application network — nothing else —
	// each with a usable address, and no published ports.
	insp, err := cli.ContainerInspect(ctx, appContainer, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect execution container: %v", err)
	}
	if insp.Container.HostConfig == nil || len(insp.Container.HostConfig.PortBindings) != 0 {
		t.Fatalf("execution container published ports: %+v", insp.Container.HostConfig)
	}
	joined := containerNetworks(insp.Container)
	if len(joined) != 2 || joined[controlName] == nil || joined[appNetName] == nil {
		names := make([]string, 0, len(joined))
		for name := range joined {
			names = append(names, name)
		}
		t.Fatalf("execution container networks = %v, want exactly [%s %s]",
			names, controlName, appNetName)
	}
	if !usableEndpointAddress(joined[controlName]) {
		t.Fatalf("execution container has no usable control-network address: %+v", joined[controlName])
	}
	if !usableEndpointAddress(joined[appNetName]) {
		t.Fatalf("execution container has no usable application-network address: %+v", joined[appNetName])
	}
}

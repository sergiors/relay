//go:build integration

// End to end: a no-healthcheck ROUTED service is reconciled and COMMITTED
// against a real Docker daemon, collected by the worker's endpoint publisher,
// persisted to the real PostgreSQL service registry under the global node
// lease, and served by the actual discovery server's GET /discovery/traefik
// handler.
//
// It is excluded from the default suite by the integration build tag, so
// running it (`go test -tags=integration ./internal/worker/`) REQUIRES a
// reachable Docker daemon (client.FromEnv) and PostgreSQL at
// TEST_DATABASE_URL; a missing dependency fails the test rather than skipping
// it. Redis is deliberately not required: nothing here touches the event
// stream.
package worker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/discovery"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// e2eNoProbeHost is the routed host the committed replica must be discoverable
// under from end to end.
const e2eNoProbeHost = "noprobe.e2e.example.com"

// e2eTraefikConfig mirrors the Traefik HTTP provider document the discovery
// server serves (its traefikConfig type is package-private to discovery):
// exactly the router and load-balancer shape this end-to-end assertion needs.
type e2eTraefikConfig struct {
	HTTP struct {
		Routers  map[string]e2eTraefikRouter  `json:"routers"`
		Services map[string]e2eTraefikService `json:"services"`
	} `json:"http"`
}

type e2eTraefikRouter struct {
	Rule    string `json:"rule"`
	Service string `json:"service"`
}

type e2eTraefikService struct {
	LoadBalancer struct {
		Servers []struct {
			URL string `json:"url"`
		} `json:"servers"`
	} `json:"loadBalancer"`
}

// Given one routed service with NO healthcheck, when the app is discovered,
// built, and reconciled through the production entry points, then the pass
// ends terminal with the replica COMMITTED (its Docker-owned marker makes the
// running container collectable under the explicit no-probe policy) and with
// no healthcheck candidate or health-driven wake. When the worker's publisher
// then syncs against real PostgreSQL under the node's global lease, then the
// SAME reader Discovery polls returns exactly that endpoint — host, container,
// port, and generation — AND the actual discovery.NewServer, started on a test
// address over the same real *state.State, answers GET /discovery/traefik with
// the Traefik router, service, and backend URL for that committed replica
// before the server is stopped.
func TestIntegrationNoProbeServiceCommittedAndPublishedToDiscovery(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	dsn := testutil.NewStateDSN(t)
	logger := testutil.DiscardLogger()

	nodeID := testutil.UniqueName(t, "noprobe")
	appName := testutil.UniqueName(t, "noprobe-app")

	// Registered FIRST so it runs LAST: the raw-client cleanup removes every
	// container this node identity created, even after the manager closed.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Logf("cleanup: list containers: %v", err)
			return
		}
		for _, ctr := range list.Items {
			if ctr.Labels["relay.node_id"] != nodeID {
				continue
			}
			if _, err := dcli.ContainerRemove(
				ctx, ctr.ID, client.ContainerRemoveOptions{Force: true},
			); err != nil {
				t.Logf("cleanup: remove container %s: %v", ctr.ID, err)
			}
		}
	})

	st := testutil.OpenState(t, dsn, state.Options{
		NodeID: nodeID, Instance: "e2e",
	})
	manager, err := runtime.NewManager(logger, nil, nodeID)
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	const bindAddr = "127.0.0.1"
	svcCtrl := reconciler.NewServiceReconciler(
		manager, nil, logger, reconcileTimeout,
		reconciler.WithNodeID(nodeID),
		reconciler.WithServiceBindAddr(bindAddr),
	)

	// The app directory: one routed service with no healthcheck plus the
	// minimal handler the nodejs24.x runtime needs to prepare an image.
	root := t.TempDir()
	dir := filepath.Join(root, appName)
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatalf("mkdir app dir: %v", err)
	}
	templateYAML := "runtime: nodejs24.x\n" +
		"events:\n  - handler: index.hi\n    pattern:\n" +
		"      event_name: [INSERT]\n" +
		"services:\n  - name: web\n    entrypoint: app/service.js\n" +
		"    port: 3000\n    replicas: 1\n    host: " + e2eNoProbeHost + "\n"
	files := map[string]string{
		filepath.Join(dir, "template.yaml"): templateYAML,
		filepath.Join(dir, "index.js"):      "export function hi(e, context){}\n",
		filepath.Join(dir, "app", "service.js"): "process.on('SIGTERM', () => " +
			"process.exit(0));\nsetInterval(() => {}, 1 << 30);\n",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	reg := &runner.Registry{}
	reg.Set(nil)

	// Drive the production reconciler through its exported Start/Enqueue loop,
	// forwarding service convergence exactly like the worker's hooks do.
	var resultMu sync.Mutex
	var lastResult reconciler.ServicePassResult
	var haveResult bool
	rec := reconciler.New(reconciler.Config{
		Root:     root,
		Debounce: 20 * time.Millisecond,
		Interval: time.Hour,
		UpdateServices: func(name string, tmpl *app.Template, image string) {
			result := svcCtrl.ApplyResult(
				context.Background(), name, tmpl, image, nil, nil)
			resultMu.Lock()
			lastResult, haveResult = result, true
			resultMu.Unlock()
		},
		RemoveServices: func(name string, _ func()) {
			svcCtrl.Remove(context.Background(), name)
		},
	}, reg, manager, logger)

	reconcileCtx, cancelReconcile := context.WithCancel(context.Background())
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- rec.Start(reconcileCtx) }()
	t.Cleanup(func() {
		cancelReconcile()
		select {
		case err := <-reconcileDone:
			if err != nil {
				t.Errorf("reconciler shutdown: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("reconciler did not stop after cancellation")
		}
	})
	rec.Enqueue(appName)

	// The no-probe policy view: no healthcheck is declared, so the effective
	// probe is nil and the committed running replica must be collectable.
	noProbePolicy := func(appName, service string) *app.ServiceHealthcheck {
		return nil
	}
	testutil.WaitFor(t, 5*time.Minute,
		"no-probe service prepared, running, and committed",
		func() bool {
			if pf := reg.GetByName(appName); pf == nil || pf.Prepared() == nil {
				return false
			}
			endpoints, err := manager.RegistryServiceEndpoints(
				context.Background(), bindAddr, noProbePolicy)
			return err == nil && len(endpoints) == 1
		})

	resultMu.Lock()
	committed, seen := lastResult, haveResult
	resultMu.Unlock()
	if !seen {
		t.Fatal("UpdateServices never ran for the discovered app")
	}
	if committed.Err != nil || committed.Pending() {
		t.Fatalf("service pass = %+v, want a terminal successful convergence",
			committed)
	}
	if len(committed.Candidates) != 0 || !committed.RetryNotBefore.IsZero() ||
		!committed.StartupDeadline.IsZero() {
		t.Fatalf("service pass = %+v, want no healthcheck candidate and no "+
			"health-driven wake for a no-probe service", committed)
	}

	endpoints, err := manager.RegistryServiceEndpoints(
		context.Background(), bindAddr, noProbePolicy)
	if err != nil || len(endpoints) != 1 {
		t.Fatalf("collectable endpoints = (%+v, %v), want exactly one committed "+
			"replica", endpoints, err)
	}
	committedEndpoint := endpoints[0]
	if committedEndpoint.App != appName || committedEndpoint.Service != "web" ||
		committedEndpoint.Replica != 0 ||
		committedEndpoint.HostIP != bindAddr ||
		committedEndpoint.HostPort < 1 ||
		committedEndpoint.InternalPort != 3000 ||
		committedEndpoint.Generation == "" {
		t.Fatalf("committed endpoint = %+v, want this node's bound, "+
			"generation-stamped replica", committedEndpoint)
	}

	// When: the worker's publisher syncs against the real registry under the
	// node's global lease. Its probe policy derives from the SAME current
	// template snapshot (no healthcheck declared -> nil).
	ctx := context.Background()
	lease, err := st.ClaimNodeLease(ctx, state.NodeLeaseMetadata{
		BindAddr: bindAddr, AdvertiseAddr: bindAddr,
	}, time.Minute)
	if err != nil {
		t.Fatalf("claim node lease: %v", err)
	}
	t.Cleanup(func() {
		_ = st.ReleaseNodeLease(context.Background(), lease)
	})
	publisher := &serviceEndpointPublisher{
		st:            st,
		manager:       manager,
		apps:          reg,
		nodes:         &staticLeaseSource{lease: &lease},
		log:           logger,
		reporter:      newRegistryFailureReporter(logger),
		bindAddr:      bindAddr,
		sourceMounted: manager.ServiceSourceMounted,
	}
	publisher.ctx, publisher.cancel = context.WithCancel(ctx)
	t.Cleanup(publisher.cancel)
	publisher.syncOnce()

	// Then: the SAME reader the discovery handler polls returns exactly that
	// endpoint — the published route resolves to this node's committed
	// container.
	read, err := st.ReadEligibleServiceRegistryEndpoints(ctx,
		state.ServiceRegistryFilter{App: appName})
	if err != nil {
		t.Fatalf("read service registry (the Discovery reader): %v", err)
	}
	if len(read) != 1 {
		t.Fatalf("discovered endpoints = %+v, want exactly the one committed "+
			"replica", read)
	}
	served := read[0]
	if served.Host != e2eNoProbeHost || served.Service != "web" ||
		served.NodeID != nodeID ||
		served.ContainerID != committedEndpoint.ContainerID ||
		served.HostPort != committedEndpoint.HostPort ||
		served.HostIP != bindAddr ||
		served.Generation != committedEndpoint.Generation ||
		served.AdvertiseAddr != bindAddr {
		t.Fatalf("discovered endpoint = %+v, want host %q routing to this "+
			"node's committed container %s on %s (generation %q)",
			served, e2eNoProbeHost, committedEndpoint.ContainerID, bindAddr,
			committedEndpoint.Generation)
	}

	// And: the ACTUAL discovery server — production NewServer over this same
	// real *state.State — starts on a reserved test address and serves the
	// route to GET /discovery/traefik.
	addr := testutil.FreeAddr(t)
	discoveryServer := discovery.NewServer(addr, st, logger)
	if discoveryServer == nil {
		t.Fatalf("discovery.NewServer(%q) = nil, want a server for a "+
			"configured address", addr)
	}
	// Stop is idempotent; the cleanup covers failures before the explicit
	// Stop below.
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(),
			5*time.Second)
		defer cancel()
		_ = discoveryServer.Stop(stopCtx)
	})
	if err := discoveryServer.Start(); err != nil {
		t.Fatalf("discovery server start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var resp *http.Response
	for {
		resp, err = http.Get("http://" + addr + "/discovery/traefik")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET /discovery/traefik never became reachable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /discovery/traefik = %d, want 200", resp.StatusCode)
	}
	var cfg e2eTraefikConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatalf("decode Traefik configuration: %v", err)
	}

	// Then: exactly one host-only router for the committed replica, and its
	// load-balancer backend points at this node's advertise address and the
	// committed replica's Docker-assigned host port.
	if len(cfg.HTTP.Routers) != 1 {
		t.Fatalf("routers = %+v, want exactly one for the single committed "+
			"replica", cfg.HTTP.Routers)
	}
	var router e2eTraefikRouter
	for _, candidate := range cfg.HTTP.Routers {
		router = candidate
	}
	wantRule := "Host(`" + e2eNoProbeHost + "`)"
	if router.Rule != wantRule {
		t.Fatalf("router rule = %q, want %q", router.Rule, wantRule)
	}
	backend, ok := cfg.HTTP.Services[router.Service]
	if !ok {
		t.Fatalf("router service %q missing from services: %+v",
			router.Service, cfg.HTTP.Services)
	}
	wantURL := "http://" + net.JoinHostPort(
		bindAddr, strconv.Itoa(served.HostPort))
	servers := backend.LoadBalancer.Servers
	if len(servers) != 1 || servers[0].URL != wantURL {
		t.Fatalf("backend servers = %+v, want exactly [%s] (this node's "+
			"advertise address and the committed replica's host port)",
			servers, wantURL)
	}

	// Stop the server: the provider endpoint shuts down cleanly.
	stopCtx, stopCancel := context.WithTimeout(context.Background(),
		5*time.Second)
	defer stopCancel()
	if err := discoveryServer.Stop(stopCtx); err != nil {
		t.Fatalf("discovery server stop: %v", err)
	}
}

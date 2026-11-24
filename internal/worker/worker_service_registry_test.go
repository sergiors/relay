package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// Unit tests for the worker's service ENDPOINT PUBLISHER: it starts only when
// host-port publication is configured, consumes the GLOBAL node lease owned by
// the node registry (never claiming or heartbeating a lease itself), and
// publishes the exact verified batch. Deterministic fakes only.

// fakeServiceEndpointStore is an in-memory serviceEndpointStore recording
// every publish with the lease it was given.
type fakeServiceEndpointStore struct {
	mu      sync.Mutex
	leases  []state.NodeLease
	batches [][]state.ServiceRegistryEndpoint
	err     error

	// publishHook runs at publish ENTRY, before the store lock, so a test can
	// deliver lifecycle cancellation WHILE the publish is in flight.
	publishHook func()

	firstPublish chan struct{}
	publishOnce  sync.Once
}

func newFakeServiceEndpointStore() *fakeServiceEndpointStore {
	return &fakeServiceEndpointStore{firstPublish: make(chan struct{})}
}

func (f *fakeServiceEndpointStore) PublishServiceRegistryEndpoints(
	_ context.Context, lease state.NodeLease,
	endpoints []state.ServiceRegistryEndpoint,
) error {
	if f.publishHook != nil {
		f.publishHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = append(f.leases, lease)
	f.batches = append(f.batches,
		append([]state.ServiceRegistryEndpoint(nil), endpoints...))
	f.publishOnce.Do(func() { close(f.firstPublish) })
	return f.err
}

func (f *fakeServiceEndpointStore) publishCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func (f *fakeServiceEndpointStore) lastPublish() (
	state.NodeLease, []state.ServiceRegistryEndpoint,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batches) == 0 {
		return state.NodeLease{}, nil
	}
	last := len(f.batches) - 1
	return f.leases[last], f.batches[last]
}

func (f *fakeServiceEndpointStore) leaseAt(index int) state.NodeLease {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leases[index]
}

// fakeServiceEndpointSource is an in-memory serviceEndpointSource recording the
// bind address and EFFECTIVE probe policy each collection received.
type fakeServiceEndpointSource struct {
	mu        sync.Mutex
	binds     []string
	policy    runtime.ServiceProbePolicy
	endpoints []runtime.RegistryServiceEndpoint
	err       error

	// onCall runs at collection ENTRY, so a test can deliver lifecycle
	// cancellation WHILE the collection is in flight.
	onCall func()
}

func (f *fakeServiceEndpointSource) RegistryServiceEndpoints(
	_ context.Context, expectedBindAddr string, probe runtime.ServiceProbePolicy,
) ([]runtime.RegistryServiceEndpoint, error) {
	if f.onCall != nil {
		f.onCall()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.binds = append(f.binds, expectedBindAddr)
	f.policy = probe
	return f.endpoints, f.err
}

func (f *fakeServiceEndpointSource) set(
	endpoints []runtime.RegistryServiceEndpoint, err error,
) {
	f.mu.Lock()
	f.endpoints = endpoints
	f.err = err
	f.mu.Unlock()
}

func (f *fakeServiceEndpointSource) boundTo() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.binds...)
}

// probePolicy returns the policy supplied to the most recent collection.
func (f *fakeServiceEndpointSource) probePolicy() runtime.ServiceProbePolicy {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.policy
}

// staticLeaseSource serves one fixed global lease.
type staticLeaseSource struct {
	lease *state.NodeLease
}

func (s *staticLeaseSource) Lease() *state.NodeLease { return s.lease }

// newServiceEndpointFixture builds a publisher whose loop is NOT started; the
// test drives syncOnce directly for deterministic call counts.
func newServiceEndpointFixture(
	t *testing.T, st serviceEndpointStore, src serviceEndpointSource,
	nodes nodeLeaseSource, apps *runner.Registry, logger *slog.Logger,
) *serviceEndpointPublisher {
	t.Helper()
	r := &serviceEndpointPublisher{
		st:       st,
		manager:  src,
		apps:     apps,
		nodes:    nodes,
		log:      logger,
		reporter: newRegistryFailureReporter(logger),
		bindAddr: "127.0.0.1",
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	t.Cleanup(r.cancel)
	return r
}

// registryRoutedApps returns a runner registry whose "alpha" app routes ONE
// service (web on api.example.com/v2) and declares one unrouted service.
func registryRoutedApps() *runner.Registry {
	apps := &runner.Registry{}
	apps.Set([]*runner.PreparedApp{
		runner.NewUnavailable(app.App{Name: "alpha", Template: &app.Template{
			Runtime: "nodejs24.x",
			Services: []app.Service{
				{Name: "web", Entrypoint: "service.js",
					Host: "api.example.com", Path: "/v2", Port: 8080, Replicas: 1},
				{Name: "internal", Entrypoint: "service.js", Port: 9090, Replicas: 1},
			},
		}}),
	})
	return apps
}

func registryCandidate(
	appName, service, container string, replica int, labels map[string]string,
) runtime.RegistryServiceEndpoint {
	return runtime.RegistryServiceEndpoint{
		App:          appName,
		Service:      service,
		Replica:      replica,
		InternalPort: 8080,
		ContainerID:  container,
		Generation:   "gen-" + container,
		HostIP:       "127.0.0.1",
		HostPort:     49155,
		Labels:       labels,
	}
}

func testNodeLease() state.NodeLease {
	return state.NodeLease{
		NodeID:        "node-1",
		Instance:      "inst-1",
		Incarnation:   7,
		BindAddr:      "127.0.0.1",
		AdvertiseAddr: "127.0.0.1:7001",
		ExpiresAt:     time.Now().Add(nodeRegistryLeaseTTL),
	}
}

// Given host-port publication disabled (no SERVICE_BIND_ADDR) or any missing
// dependency, when the worker starts the endpoint publisher, then it returns
// nil and registers NOTHING; with everything present it registers exactly one
// barrier step and syncs against the (initially absent) global lease without
// touching Docker.
func TestStartServiceEndpointPublisherRequiresBindConfigured(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testutil.DiscardLogger()
	shutdown := &shutdownRegistry{}
	inertNodes := &nodeRegistry{log: logger}

	if r := startServiceEndpointPublisher(
		ctx, logger, config.Config{}, nil, nil, nil, nil, shutdown); r != nil {
		t.Fatal("publisher started with no SERVICE_BIND_ADDR configured")
	}
	enabled := config.Config{ServiceBindAddr: "127.0.0.1"}
	// Publication enabled, but each missing dependency still refuses to start.
	if r := startServiceEndpointPublisher(
		ctx, logger, enabled, nil, &runtime.Manager{}, &runner.Registry{},
		inertNodes, shutdown); r != nil {
		t.Fatal("publisher started without a state handle")
	}
	if r := startServiceEndpointPublisher(
		ctx, logger, enabled, &state.State{}, nil, &runner.Registry{},
		inertNodes, shutdown); r != nil {
		t.Fatal("publisher started without a runtime manager")
	}
	if r := startServiceEndpointPublisher(
		ctx, logger, enabled, &state.State{}, &runtime.Manager{}, nil,
		inertNodes, shutdown); r != nil {
		t.Fatal("publisher started without the runner registry")
	}
	if r := startServiceEndpointPublisher(
		ctx, logger, enabled, &state.State{}, &runtime.Manager{},
		&runner.Registry{}, nil, shutdown); r != nil {
		t.Fatal("publisher started without the node registry")
	}
	if len(shutdown.steps) != 0 {
		t.Fatalf("registered steps = %d, want none while publication is disabled",
			len(shutdown.steps))
	}

	r := startServiceEndpointPublisher(ctx, logger, enabled,
		&state.State{}, &runtime.Manager{}, &runner.Registry{}, inertNodes,
		shutdown)
	if r == nil {
		t.Fatal("publisher did not start with publication enabled")
	}
	if r.bindAddr != "127.0.0.1" {
		t.Fatalf("bind address = %q, want the configured bind", r.bindAddr)
	}
	if len(shutdown.steps) != 1 {
		t.Fatalf("registered steps = %d, want exactly one", len(shutdown.steps))
	}
	step := shutdown.steps[0]
	if step.name != shutdownStepServiceEndpoints || !step.barrier ||
		step.timeout != shutdownServiceTimeout {
		t.Fatalf("step = %+v, want the %q barrier with the service timeout",
			step, shutdownStepServiceEndpoints)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := r.shutdown(shutdownCtx); err != nil {
		t.Fatalf("publisher shutdown with an absent global lease: %v", err)
	}
	// The inert node registry never claims a lease, so the started loop can
	// never have collected from Docker (a zero manager) or published.
	if binds := inertNodes.Lease(); binds != nil {
		t.Fatalf("inert registry lease = %+v, want none", binds)
	}
}

// Given a claimed global node lease, when the publisher syncs repeatedly, then
// it publishes using that lease and performs NO lease operations of its own —
// claims and heartbeats stay exactly where the node registry put them.
func TestServiceEndpointPublisherConsumesGlobalLeaseWithoutOwnHeartbeat(t *testing.T) {
	fakeNodes := newFakeNodeLeaseStore()
	logger := testutil.DiscardLogger()
	registry := newNodeRegistryFixture(t, fakeNodes, logger)
	registry.metadata = state.NodeLeaseMetadata{BindAddr: "127.0.0.1"}
	registry.heartbeatOnce() // the registry claims and heartbeats once

	store := newFakeServiceEndpointStore()
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	publisher := newServiceEndpointFixture(t, store, source, registry,
		registryRoutedApps(), logger)

	publisher.syncOnce()
	publisher.syncOnce()
	publisher.syncOnce()

	if store.publishCount() != 3 {
		t.Fatalf("publish calls = %d, want 3", store.publishCount())
	}
	claims, heartbeats, _, _ := fakeNodes.counts()
	if claims != 1 || heartbeats != 1 {
		t.Fatalf("lease operations = (%d claims, %d heartbeats), want the "+
			"registry's own (1, 1): the publisher must never claim or heartbeat",
			claims, heartbeats)
	}
	global := registry.Lease()
	if global == nil {
		t.Fatal("the registry lost its lease")
	}
	for i := 0; i < 3; i++ {
		if got := store.leaseAt(i); got.Incarnation != global.Incarnation {
			t.Fatalf("publish %d used lease incarnation %d, want the global %d",
				i, got.Incarnation, global.Incarnation)
		}
	}
}

// Given no global lease yet (the registry has not claimed), when the publisher
// syncs, then it collects nothing and publishes nothing: Docker and PostgreSQL
// stay untouched until the node registry admits a lease.
func TestServiceEndpointSyncWaitsForGlobalLease(t *testing.T) {
	store := newFakeServiceEndpointStore()
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: nil}, registryRoutedApps(),
		testutil.DiscardLogger())

	publisher.syncOnce()

	if binds := source.boundTo(); len(binds) != 0 {
		t.Fatalf("docker collection calls = %v, want none without a lease", binds)
	}
	if store.publishCount() != 0 {
		t.Fatalf("publish calls = %d, want none without a lease",
			store.publishCount())
	}
}

// Given a lease and Docker-verified candidates, when sync runs, then it
// collects with the configured bind address and publishes the exact batch
// derived from the CURRENT template's routes under the supplied global lease.
func TestServiceEndpointSyncPublishesCurrentTemplateRoutes(t *testing.T) {
	lease := testNodeLease()
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
		// No route for an unrouted service: never advertised.
		registryCandidate("alpha", "internal", "ctr-internal", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(),
		testutil.DiscardLogger())

	publisher.syncOnce()

	if store.publishCount() != 1 {
		t.Fatalf("publish calls = %d, want 1", store.publishCount())
	}
	if binds := source.boundTo(); len(binds) != 1 || binds[0] != "127.0.0.1" {
		t.Fatalf("collection bind addresses = %v, want [127.0.0.1]", binds)
	}
	publishedLease, batch := store.lastPublish()
	if publishedLease.Incarnation != lease.Incarnation {
		t.Fatalf("published with lease incarnation %d, want the global %d",
			publishedLease.Incarnation, lease.Incarnation)
	}
	if len(batch) != 1 {
		t.Fatalf("published batch = %d endpoints, want only the routed web "+
			"replica (%d)", len(batch), 1)
	}
	endpoint := batch[0]
	if endpoint.App != "alpha" || endpoint.Service != "web" ||
		endpoint.Replica != 0 || endpoint.Host != "api.example.com" ||
		endpoint.Path != "/v2" || endpoint.HostIP != "127.0.0.1" ||
		endpoint.HostPort != 49155 || endpoint.InternalPort != 8080 ||
		endpoint.ContainerID != "ctr-web" ||
		endpoint.Generation != "gen-ctr-web" {
		t.Fatalf("published endpoint = %+v, want the verified facts under the "+
			"template's route", endpoint)
	}
}

// Given candidates for a service with no current route, or repeated
// relational identities, when sync runs, then only the current routed
// candidate is advertised and the duplicate identity is published once.
func TestServiceEndpointSyncDropsUnroutedAndDuplicateCandidates(t *testing.T) {
	apps := &runner.Registry{}
	apps.Set([]*runner.PreparedApp{
		runner.NewUnavailable(app.App{Name: "alpha", Template: &app.Template{
			Runtime: "nodejs24.x",
			Services: []app.Service{{Name: "web", Entrypoint: "service.js",
				Host: "new.example.com", Port: 8080, Replicas: 1}},
		}}),
	})

	kept := registryCandidate("alpha", "web", "ctr-kept", 0, nil)
	duplicate := kept
	duplicate.HostPort = 49166 // same relational identity: must not double-publish
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		// No route in the current template.
		registryCandidate("alpha", "other", "ctr-foreign", 0, nil),
		kept,
		duplicate,
	}}
	store := newFakeServiceEndpointStore()
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, apps, testutil.DiscardLogger())

	publisher.syncOnce()

	_, batch := store.lastPublish()
	if len(batch) != 1 || batch[0].ContainerID != "ctr-kept" {
		ids := make([]string, len(batch))
		for i, endpoint := range batch {
			ids[i] = endpoint.ContainerID
		}
		t.Fatalf("published batch = %v, want only the current routed match "+
			"[ctr-kept] (unrouted/duplicate excluded)", ids)
	}
	if batch[0].Host != "new.example.com" {
		t.Fatalf("published host = %q, want the current template's route",
			batch[0].Host)
	}
	if batch[0].HostPort != 49155 {
		t.Fatalf("published port = %d, want the first occurrence's 49155 "+
			"(identity deduplicated)", batch[0].HostPort)
	}
}

// Given a configured ROUTING_HOST_OVERRIDE, when the publisher syncs, then the
// stored registry endpoint Host is the EFFECTIVE host — the exact override.
// Flipping the override later changes the published route against the SAME
// apps registry; nothing is rebuilt.
func TestServiceEndpointEffectiveHostUnderOverride(t *testing.T) {
	declared := "issuer.example.com"
	override := "portal.internal.test"
	effective := routing.EffectiveHost(declared, override)
	if effective != override {
		t.Fatalf("fixture: EffectiveHost(%q, %q) = %q, want the exact override",
			declared, override, effective)
	}

	apps := &runner.Registry{}
	apps.Set([]*runner.PreparedApp{
		runner.NewUnavailable(app.App{Name: "issuer", Template: &app.Template{
			Runtime: "nodejs24.x",
			Services: []app.Service{{Name: "web", Entrypoint: "service.js",
				Host: declared, Path: "/v2", Port: 8080, Replicas: 1}},
		}}),
	})

	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("issuer", "web", "ctr-effective", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, apps, testutil.DiscardLogger())
	publisher.hostOverride = override

	publisher.syncOnce()

	_, batch := store.lastPublish()
	if len(batch) != 1 || batch[0].ContainerID != "ctr-effective" {
		ids := make([]string, len(batch))
		for i, endpoint := range batch {
			ids[i] = endpoint.ContainerID
		}
		t.Fatalf("published batch = %v, want only [ctr-effective]", ids)
	}
	endpoint := batch[0]
	if endpoint.Host != override {
		t.Fatalf("published Host = %q, want the EFFECTIVE host %q "+
			"(declared was %q)", endpoint.Host, override, declared)
	}
	if endpoint.Path != "/v2" || endpoint.Service != "web" ||
		endpoint.App != "issuer" {
		t.Fatalf("published endpoint = %+v, want the template's route identity", endpoint)
	}

	publisher.hostOverride = ""
	source.set([]runtime.RegistryServiceEndpoint{
		registryCandidate("issuer", "web", "ctr-declared", 0, nil),
	}, nil)

	publisher.syncOnce()

	_, flipped := store.lastPublish()
	if store.publishCount() != 2 {
		t.Fatalf("publish calls = %d, want 2 (one per sync)", store.publishCount())
	}
	if len(flipped) != 1 || flipped[0].ContainerID != "ctr-declared" ||
		flipped[0].Host != declared {
		t.Fatalf("flipped batch = %+v, want the declared host %q",
			flipped, declared)
	}
}

// Given a previously published snapshot, when discovery fails outright, then
// sync publishes NOTHING (no destructive empty batch); the next successful
// sync publishes the fresh snapshot with the SAME global lease.
func TestServiceEndpointFailedCollectionKeepsPreviousSnapshot(t *testing.T) {
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(),
		testutil.DiscardLogger())

	publisher.syncOnce()
	if store.publishCount() != 1 {
		t.Fatalf("publishes after the first sync = %d, want 1",
			store.publishCount())
	}

	source.set(nil, errors.New("docker inspection wedged"))
	publisher.syncOnce()
	if store.publishCount() != 1 {
		t.Fatalf("publishes after failed discovery = %d, want 1: a failure must "+
			"keep the previous snapshot instead of publishing an empty batch",
			store.publishCount())
	}

	source.set([]runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}, nil)
	publisher.syncOnce()
	secondLease, batch := store.lastPublish()
	if store.publishCount() != 2 {
		t.Fatalf("publishes after recovery = %d, want 2", store.publishCount())
	}
	if secondLease.Incarnation != lease.Incarnation {
		t.Fatalf("recovery lease incarnation = %d, want the retained global %d",
			secondLease.Incarnation, lease.Incarnation)
	}
	if len(batch) != 1 || batch[0].ContainerID != "ctr-web" {
		t.Fatalf("recovery batch = %v, want the fresh snapshot", batch)
	}
}

// Given a collection that verified SOME endpoints before failing, when sync
// runs, then the partial batch is published instead of letting one failure
// suppress every healthy endpoint.
func TestServiceEndpointPartialCollectionPublishesVerifiedSubset(t *testing.T) {
	source := &fakeServiceEndpointSource{
		endpoints: []runtime.RegistryServiceEndpoint{
			registryCandidate("alpha", "web", "ctr-1", 0, nil),
			registryCandidate("alpha", "web", "ctr-2", 1, nil),
		},
		err: errors.New("inspect c3 failed"),
	}
	store := newFakeServiceEndpointStore()
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(),
		testutil.DiscardLogger())

	publisher.syncOnce()

	_, batch := store.lastPublish()
	if store.publishCount() != 1 || len(batch) != 2 {
		t.Fatalf("partial publish = (%d calls, %d endpoints), want (1, 2)",
			store.publishCount(), len(batch))
	}
}

// Given a healthy node with no eligible replicas, when sync runs, then it
// publishes an empty batch — the exact-set contract that withdraws departed
// endpoints while the global lease stays alive.
func TestServiceEndpointEmptyCollectionPublishesEmptyBatch(t *testing.T) {
	store := newFakeServiceEndpointStore()
	source := &fakeServiceEndpointSource{}
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(),
		testutil.DiscardLogger())

	publisher.syncOnce()

	_, batch := store.lastPublish()
	if store.publishCount() != 1 || len(batch) != 0 {
		t.Fatalf("publish = (%d calls, %d endpoints), want (1, 0)",
			store.publishCount(), len(batch))
	}
}

// Given a FENCED publication (the lease was superseded or expired), when the
// next sync runs, then the publisher keeps using the same global lease (lease
// recovery belongs to the node registry) and logs nothing at Warn: a fence is
// an expected rejection, not a failure transition.
func TestServiceEndpointFencedPublishKeepsLeaseAndStaysQuiet(t *testing.T) {
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	store.err = fmt.Errorf("publish superseded: %w", state.ErrNodeLeaseFence)
	logger, handler := newRecordingLogger(t)
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)

	publisher.syncOnce()
	publisher.syncOnce()

	if store.publishCount() != 2 {
		t.Fatalf("publish calls = %d, want 2 (each sync retries with the same "+
			"lease)", store.publishCount())
	}
	if got := store.leaseAt(1); got.Incarnation != lease.Incarnation {
		t.Fatalf("second publish lease incarnation = %d, want the unchanged %d "+
			"(the publisher never re-claims)", got.Incarnation, lease.Incarnation)
	}
	if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
		t.Fatalf("Warn records = %+v, want none for an expected fence", warns)
	}
}

// Given a transient database failure during publication, when sync runs, then
// the failure is reported once at Warn (rate-limited), and the next successful
// publish reports exactly one recovery at Info.
func TestServiceEndpointTransientPublishFailureRateLimited(t *testing.T) {
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	store.err = errors.New("connection refused")
	logger, handler := newRecordingLogger(t)
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)

	publisher.syncOnce()
	publisher.syncOnce()

	warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn)
	if len(warns) != 1 || warns[0].Attrs["operation"] != "publish" {
		t.Fatalf("Warn records = %+v, want exactly one publish failure", warns)
	}
	debugs := recordsAtLevel(handler.snapshot(), slog.LevelDebug)
	if len(debugs) != 1 || debugs[0].Attrs["operation"] != "publish" {
		t.Fatalf("Debug records = %+v, want exactly one still-failing record",
			debugs)
	}

	store.mu.Lock()
	store.err = nil
	store.mu.Unlock()
	publisher.syncOnce()

	infos := recordsAtLevel(handler.snapshot(), slog.LevelInfo)
	if len(infos) != 1 || infos[0].Message != "Registry: operation recovered" ||
		infos[0].Attrs["operation"] != "publish" {
		t.Fatalf("Info records = %+v, want exactly one publish recovery", infos)
	}
}

// Given running publisher and node-registry loops, when the publisher shuts
// down, then it ONLY joins its own loop — the release of endpoints and the
// node lease is the node registry's step, which runs afterwards and releases
// exactly once.
func TestServiceEndpointPublisherShutdownOnlyJoins(t *testing.T) {
	fakeNodes := newFakeNodeLeaseStore()
	logger := testutil.DiscardLogger()
	registry := newNodeRegistryFixture(t, fakeNodes, logger)
	registry.metadata = state.NodeLeaseMetadata{BindAddr: "127.0.0.1"}
	registry.Start(context.Background())
	select {
	case <-fakeNodes.claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("the node registry never claimed its lease")
	}

	store := newFakeServiceEndpointStore()
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	publisher := newServiceEndpointFixture(t, store, source, registry,
		registryRoutedApps(), logger)
	publisher.Start(context.Background())
	select {
	case <-store.firstPublish:
	case <-time.After(10 * time.Second):
		t.Fatal("the publisher never published its first batch")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := publisher.shutdown(shutdownCtx); err != nil {
		t.Fatalf("publisher shutdown: %v", err)
	}
	if _, _, releases, _ := fakeNodes.counts(); releases != 0 {
		t.Fatalf("release calls after publisher shutdown = %d, want 0 "+
			"(withdrawing rows is the node registry's step)", releases)
	}
	select {
	case <-publisher.done:
	default:
		t.Fatal("publisher done must be closed after its join")
	}

	if err := registry.shutdown(shutdownCtx); err != nil {
		t.Fatalf("node registry shutdown: %v", err)
	}
	if _, _, releases, _ := fakeNodes.counts(); releases != 1 {
		t.Fatalf("release calls after the node registry step = %d, want exactly 1",
			releases)
	}
}

// Given lifecycle cancellation arriving WHILE a collection or publish is in
// flight, when the operation then fails, then NO Warn or failure transition is
// logged — cancellation is normal termination. The contrasts pin the rest:
// a genuine active-context collection failure warns exactly once (then
// rate-limits to Debug), and a fence rejection stays a Debug record.
// TestServiceEndpointTransientPublishFailureRateLimited covers the active
// publish-failure Warn/Debug/recovery transitions.
func TestServiceEndpointCancellationIsNotAFailureTransition(t *testing.T) {
	t.Run("in-flight collection canceled mid-operation stays silent", func(t *testing.T) {
		store := newFakeServiceEndpointStore()
		source := &fakeServiceEndpointSource{err: context.Canceled}
		lease := testNodeLease()
		logger, handler := newRecordingLogger(t)
		publisher := newServiceEndpointFixture(t, store, source,
			&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)
		// Cancellation lands between collection ENTRY and its error return.
		source.onCall = func() { publisher.cancel() }

		publisher.syncOnce()

		if binds := source.boundTo(); len(binds) != 1 {
			t.Fatalf("collection calls = %v, want the in-flight call to have "+
				"been ENTERED (not skipped)", binds)
		}
		if store.publishCount() != 0 {
			t.Fatalf("publish calls = %d, want 0: a canceled discovery keeps "+
				"the previous snapshot", store.publishCount())
		}
		if records := handler.snapshot(); len(records) != 0 {
			t.Fatalf("log records = %+v, want NONE: a canceled in-flight "+
				"collection is normal termination", records)
		}
	})

	t.Run("in-flight publish canceled mid-operation stays silent", func(t *testing.T) {
		store := newFakeServiceEndpointStore()
		store.err = context.Canceled
		source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
			registryCandidate("alpha", "web", "ctr-web", 0, nil),
		}}
		lease := testNodeLease()
		logger, handler := newRecordingLogger(t)
		publisher := newServiceEndpointFixture(t, store, source,
			&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)
		store.publishHook = func() { publisher.cancel() } // cancellation mid-publish

		publisher.syncOnce()

		if store.publishCount() != 1 {
			t.Fatalf("publish calls = %d, want the in-flight publish ENTERED",
				store.publishCount())
		}
		if records := handler.snapshot(); len(records) != 0 {
			t.Fatalf("log records = %+v, want NONE (no Warn, no recovery): a "+
				"canceled in-flight publish is normal termination", records)
		}
		if got := store.leaseAt(0); got.Incarnation != lease.Incarnation {
			t.Fatalf("published lease incarnation = %d, want the unchanged %d",
				got.Incarnation, lease.Incarnation)
		}
	})

	t.Run("live-context collection failure warns once", func(t *testing.T) {
		store := newFakeServiceEndpointStore()
		source := &fakeServiceEndpointSource{}
		source.set(nil, errors.New("docker inspection wedged"))
		lease := testNodeLease()
		logger, handler := newRecordingLogger(t)
		publisher := newServiceEndpointFixture(t, store, source,
			&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)

		publisher.syncOnce()
		publisher.syncOnce()

		warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn)
		if len(warns) != 1 || warns[0].Attrs["operation"] != "publish" {
			t.Fatalf("Warn records = %+v, want exactly one live-context "+
				"collection failure", warns)
		}
		debugs := recordsAtLevel(handler.snapshot(), slog.LevelDebug)
		if len(debugs) != 1 || debugs[0].Attrs["operation"] != "publish" {
			t.Fatalf("Debug records = %+v, want exactly one rate-limited repeat",
				debugs)
		}
		if store.publishCount() != 0 {
			t.Fatalf("publish calls = %d, want 0 (failed discovery keeps the "+
				"previous snapshot)", store.publishCount())
		}
	})

	t.Run("fence publish remains a Debug record", func(t *testing.T) {
		store := newFakeServiceEndpointStore()
		store.err = fmt.Errorf("publish superseded: %w", state.ErrNodeLeaseFence)
		source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
			registryCandidate("alpha", "web", "ctr-web", 0, nil),
		}}
		lease := testNodeLease()
		logger, handler := newRecordingLogger(t)
		publisher := newServiceEndpointFixture(t, store, source,
			&staticLeaseSource{lease: &lease}, registryRoutedApps(), logger)

		publisher.syncOnce()

		if warns := recordsAtLevel(handler.snapshot(), slog.LevelWarn); len(warns) != 0 {
			t.Fatalf("Warn records = %+v, want none for a fence rejection", warns)
		}
		fenceDebugs := 0
		for _, debug := range recordsAtLevel(handler.snapshot(), slog.LevelDebug) {
			if debug.Message == "Service registry: publish fenced" {
				fenceDebugs++
			}
		}
		if fenceDebugs != 1 {
			t.Fatalf("fence Debug records = %d, want exactly 1", fenceDebugs)
		}
		if store.publishCount() != 1 {
			t.Fatalf("publish calls = %d, want 1 (attempted and rejected)",
				store.publishCount())
		}
	})
}

// Given one sync over the CURRENT registry snapshot, when the publisher
// collects, then the published route and the EFFECTIVE probe policy handed to
// the registry both derive from that same snapshot with the app's
// ServiceSourceMounted fact: a declared normal-mode probe passes its exact
// identity, a no-healthcheck service passes nil, and a source-mounted
// (HOT_RELOAD) app passes nil despite its declared probe. A template change
// between syncs flips the route and the policy together, and a failed
// collection keeps the previously published batch.
func TestServiceEndpointProbePolicyFollowsCurrentTemplateAndSourceMount(t *testing.T) {
	hcV1 := &app.ServiceHealthcheck{
		Path: "/health", Interval: 10 * time.Second, Timeout: 3 * time.Second,
		Retries: 3, StartPeriod: 10 * time.Second,
	}
	hcV2 := *hcV1
	hcV2.Interval = 30 * time.Second
	templateOf := func(host string, hc *app.ServiceHealthcheck) *app.Template {
		return &app.Template{
			Runtime: "nodejs24.x",
			Services: []app.Service{
				{Name: "web", Entrypoint: "service.js", Host: host,
					Path: "/v2", Port: 8080, Replicas: 1, Healthcheck: hc},
				// No declared probe: its effective policy must be nil.
				{Name: "internal", Entrypoint: "service.js",
					Port: 9090, Replicas: 1},
			},
		}
	}
	apps := &runner.Registry{}
	apps.Set([]*runner.PreparedApp{
		runner.NewUnavailable(app.App{Name: "alpha",
			Template: templateOf("api.example.com", hcV1)}),
		// A HOT_RELOAD app: its declared probe is bypassed while source-mounted.
		runner.NewUnavailable(app.App{Name: "hot",
			Template: templateOf("hot.example.com", hcV1)}),
	})
	source := &fakeServiceEndpointSource{endpoints: []runtime.RegistryServiceEndpoint{
		registryCandidate("alpha", "web", "ctr-web", 0, nil),
	}}
	store := newFakeServiceEndpointStore()
	lease := testNodeLease()
	publisher := newServiceEndpointFixture(t, store, source,
		&staticLeaseSource{lease: &lease}, apps, testutil.DiscardLogger())
	publisher.sourceMounted = func(name string) bool { return name == "hot" }

	publisher.syncOnce()

	policy := source.probePolicy()
	if policy == nil {
		t.Fatal("collection received no probe policy, want the template-derived " +
			"effective policy")
	}
	if got := policy("alpha", "web"); got == nil ||
		runtime.ServiceHealthcheckLabel(got) !=
			runtime.ServiceHealthcheckLabel(hcV1) {
		t.Fatalf("policy(alpha, web) = %+v, want the declared probe's exact "+
			"identity in normal mode", got)
	}
	if got := policy("alpha", "internal"); got != nil {
		t.Fatalf("policy(alpha, internal) = %+v, want nil (no probe declared)",
			got)
	}
	if got := policy("hot", "web"); got != nil {
		t.Fatalf("policy(hot, web) = %+v, want nil (HOT_RELOAD source mount "+
			"bypasses the declared probe)", got)
	}
	if got := policy("ghost", "web"); got != nil {
		t.Fatalf("policy(ghost, web) = %+v, want nil (service not in any "+
			"current template)", got)
	}
	_, batch := store.lastPublish()
	if len(batch) != 1 || batch[0].Host != "api.example.com" {
		t.Fatalf("published batch = %+v, want the snapshot's route "+
			"api.example.com alongside its policy", batch)
	}

	// A template change between syncs flips the route AND the policy together:
	// both views come from the same current snapshot, never from a cached one.
	apps.Set([]*runner.PreparedApp{
		runner.NewUnavailable(app.App{Name: "alpha",
			Template: templateOf("new.example.com", &hcV2)}),
		runner.NewUnavailable(app.App{Name: "hot",
			Template: templateOf("hot.example.com", hcV1)}),
	})
	publisher.syncOnce()

	_, flipped := store.lastPublish()
	if store.publishCount() != 2 {
		t.Fatalf("publish calls = %d, want 2 (one per sync)", store.publishCount())
	}
	if len(flipped) != 1 || flipped[0].Host != "new.example.com" {
		t.Fatalf("flipped batch = %+v, want the new snapshot's route", flipped)
	}
	flippedPolicy := source.probePolicy()
	if flippedPolicy == nil {
		t.Fatal("flipped collection received no probe policy")
	}
	if got := flippedPolicy("alpha", "web"); got == nil ||
		runtime.ServiceHealthcheckLabel(got) !=
			runtime.ServiceHealthcheckLabel(&hcV2) {
		t.Fatalf("policy(alpha, web) after the template change = %+v, want the "+
			"edited probe identity from the same snapshot as the route", got)
	}
	if runtime.ServiceHealthcheckLabel(flippedPolicy("hot", "web")) != "" {
		t.Fatal("the source-mounted app must keep a nil effective probe across " +
			"the unrelated template change")
	}

	// A failed collection keeps the previously published batch: no destructive
	// empty publish, no new snapshot at all.
	source.set(nil, errors.New("docker inspection wedged"))
	publisher.syncOnce()
	if store.publishCount() != 2 {
		t.Fatalf("publish calls after failed discovery = %d, want 2 (a failure "+
			"keeps the previous batch)", store.publishCount())
	}
	_, kept := store.lastPublish()
	if len(kept) != 1 || kept[0].Host != "new.example.com" {
		t.Fatalf("last batch after failed discovery = %+v, want the previous "+
			"snapshot untouched", kept)
	}
}

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/function"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// fakeContainer is one in-memory service container.
type fakeContainer struct {
	id         string
	function   string
	entrypoint string // the service identity (kept as the field name for existing assertions)
	image      string
	imageID    string
	port       int
	replica    int
	state      container.ContainerState
	entry      []string // the long-lived process command passed to StartService
	envHash    string   // relay.env_hash as stamped at create (spec.Env's hash); "" = legacy/unlabeled
	labels     map[string]string
	network    string   // spec.Network passed to StartService (the routing network)
	networks   []string // spec.Networks passed to StartService (the template list)
	hostname   string   // the worker identity (relay.hostname) that owns the container
}

// defaultFakeHostname is the worker identity containers get when started via
// StartService; ShutdownCleanup tests create containers for other workers by
// seeding f.ctrs directly.
const defaultFakeHostname = "w1"

// fakeDocker is an in-memory Docker for Reconcile unit tests. Containers are
// keyed by a deterministic id so tests can inspect and manipulate them.
type fakeDocker struct {
	mu              sync.Mutex
	nextID          int
	ctrs            map[string]*fakeContainer
	stops           []string        // container ids stopped (in order), for ordering assertions
	failStopFor     string          // when non-empty, StopServiceContainers errors for this id
	missingNetworks map[string]bool // NetworkExists reports false for these
	networkLookups  []string        // network names passed to NetworkExists, in order
	resolveErr      map[string]error
	resolveCalls    []string // identities passed to ResolveServiceImage, in order
	resolvedImages  map[string]string
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		ctrs:            map[string]*fakeContainer{},
		missingNetworks: map[string]bool{},
		resolveErr:      map[string]error{},
		resolvedImages:  map[string]string{},
	}
}

// ResolveServiceImage mirrors the production resolution shape without Docker:
// entrypoint sources resolve through runtime.ServiceEntry, build sources get a
// deterministic content-addressed reference, and image sources resolve to the
// identity (with a deterministic content ID). A resolveErr entry forces a
// resolution failure for the matching identity.
func (f *fakeDocker) ResolveServiceImage(_ context.Context, fnName, _ string, tmpl *function.Template, svc function.Service, functionImage string) (runtime.ServiceImage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	identity := svc.SourceRef()
	f.resolveCalls = append(f.resolveCalls, identity)
	if err := f.resolveErr[identity]; err != nil {
		return runtime.ServiceImage{}, err
	}
	switch svc.Source() {
	case function.ServiceSourceBuild:
		ref := f.resolvedImages[identity]
		if ref == "" {
			ref = "svc-build-" + fnName + ":" + identity
		}
		return runtime.ServiceImage{Ref: ref}, nil
	case function.ServiceSourceImage:
		id := f.resolvedImages[identity]
		if id == "" {
			id = identity + "-id"
		}
		return runtime.ServiceImage{Ref: identity, ID: id}, nil
	default:
		entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
		if err != nil {
			return runtime.ServiceImage{}, err
		}
		return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
	}
}

func (f *fakeDocker) StartService(_ context.Context, spec runtime.ServiceSpec, replica int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := fmt.Sprintf("id-%d", f.nextID)
	f.ctrs[id] = &fakeContainer{
		id:         id,
		function:   spec.Function,
		entrypoint: spec.Identity,
		image:      spec.Image,
		imageID:    spec.ImageID,
		port:       spec.Port,
		replica:    replica,
		state:      container.StateRunning,
		entry:      spec.Entry,
		// Mirror production's serviceLabels: a started container always carries
		// the effective env's content hash (relay.env_hash), even for an empty
		// env. Directly seeded fakeContainers omit it to model a legacy/unlabeled
		// container. It also carries the canonical relay.networks label (the
		// union of the routing network and the template networks), so a
		// converged pass sees the networks match.
		envHash:  runtime.EnvHash(spec.Env),
		labels:   spec.Labels,
		network:  spec.Network,
		networks: spec.Networks,
		hostname: defaultFakeHostname,
	}
	return id, nil
}

func (f *fakeDocker) NetworkExists(_ context.Context, network string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networkLookups = append(f.networkLookups, network)
	return !f.missingNetworks[network], nil
}

// VerifyNetworks mirrors the production pre-flight: it reports the first
// configured network that is missing. It records every name it was asked about
// so tests can assert the template's networks were verified.
func (f *fakeDocker) VerifyNetworks(_ context.Context, networks []string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range networks {
		if n == "" {
			continue
		}
		f.networkLookups = append(f.networkLookups, n)
		if f.missingNetworks[n] {
			return n, false, nil
		}
	}
	return "", true, nil
}

func (f *fakeDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.ServiceContainer
	for _, c := range f.ctrs {
		out = append(out, runtime.ServiceContainer{
			ID:       c.id,
			Function: c.function,
			Identity: c.entrypoint,
			Image:    c.image,
			ImageID:  c.imageID,
			State:    c.state,
			Replica:  c.replica,
			Port:     c.port,
			Hostname: c.hostname,
			EnvHash:  c.envHash,
			Networks: runtime.NetworksLabel(c.network, c.networks),
			Labels:   c.labels,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeDocker) StopServiceContainers(_ context.Context, containers []runtime.ServiceContainer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var firstErr error
	for _, sc := range containers {
		if sc.ID == f.failStopFor {
			if firstErr == nil {
				firstErr = fmt.Errorf("stop failed for %s", sc.ID)
			}
			continue
		}
		if c, ok := f.ctrs[sc.ID]; ok {
			f.stops = append(f.stops, c.id)
			delete(f.ctrs, sc.ID)
		}
	}
	return firstErr
}

func (f *fakeDocker) RemoveFunctionServiceContainers(_ context.Context, fnName string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var toRemove []string
	for id, c := range f.ctrs {
		if c.function == fnName {
			f.stops = append(f.stops, c.id)
			toRemove = append(toRemove, id)
		}
	}
	for _, id := range toRemove {
		delete(f.ctrs, id)
	}
	return len(toRemove), nil
}

// setState mutates a container's state (for crash-replacement tests).
func (f *fakeDocker) setState(id string, st container.ContainerState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.ctrs[id]; ok {
		c.state = st
	}
}

// runningCount returns how many of fn/service's containers are running.
func (f *fakeDocker) runningCount(fn, service string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.ctrs {
		if c.function == fn && c.entrypoint == service && c.state == container.StateRunning {
			n++
		}
	}
	return n
}

// replicas returns the sorted replica slots currently running for fn/service.
func (f *fakeDocker) replicas(fn, service string) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var slots []int
	for _, c := range f.ctrs {
		if c.function == fn && c.entrypoint == service && c.state == container.StateRunning {
			slots = append(slots, c.replica)
		}
	}
	sort.Ints(slots)
	return slots
}

// countForFunction returns how many containers belong to fn regardless of state.
func (f *fakeDocker) countForFunction(fn string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.ctrs {
		if c.function == fn {
			n++
		}
	}
	return n
}

// serviceTemplate builds a template with the given services. Runtimes default
// to node24 so ServiceEntry succeeds.
func serviceTemplate(runtimeName string, services ...function.Service) *function.Template {
	if runtimeName == "" {
		runtimeName = "node24"
	}
	return &function.Template{Runtime: runtimeName, Services: services}
}

// serviceEnvHash is the relay.env_hash label a started container carries for the
// given port with no prepared env and no secrets — the exact shape the
// package-level reconcile test helper produces (BuildEnv appends PORT last).
// Directly seeded fakeContainers in converged-state tests set this so they model
// a container Relay itself created; omitting it models a legacy/unlabeled one.
func serviceEnvHash(port int) string {
	return runtime.EnvHash([]string{fmt.Sprintf("PORT=%d", port)})
}

// reconcile runs Reconcile with the background context, no prepared env, no
// secrets, and a discarded logger — the common shape across the service tests.
func reconcile(t *testing.T, d Docker, fn string, tmpl *function.Template, image string, cfg routing.TraefikConfig) (bool, error) {
	t.Helper()
	return Reconcile(context.Background(), d, fn, t.TempDir(), tmpl, image, nil, nil, cfg, testutil.DiscardLogger())
}

func TestReconcileInitialCreation(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})

	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}
	slots := f.replicas("fn", "service.js")
	if len(slots) != 2 || slots[0] != 0 || slots[1] != 1 {
		t.Fatalf("slots = %v, want [0 1]", slots)
	}
	// Idempotent second pass is a no-op (replicas unchanged).
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running after idle reconcile = %d, want 2", got)
	}
}

func TestReconcileScaleUp(t *testing.T) {
	f := newFakeDocker()
	svc := function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1}
	if _, err := reconcile(t, f, "fn", serviceTemplate("node24", svc), "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}

	svc.Replicas = 3
	if _, err := reconcile(t, f, "fn", serviceTemplate("node24", svc), "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 3 {
		t.Fatalf("running = %d, want 3", got)
	}
	slots := f.replicas("fn", "service.js")
	want := []int{0, 1, 2}
	if len(slots) != len(want) {
		t.Fatalf("slots = %v, want %v", slots, want)
	}
	for i := range want {
		if slots[i] != want[i] {
			t.Fatalf("slots = %v, want %v", slots, want)
		}
	}
}

func TestReconcileScaleDownKeepsLowestReplicas(t *testing.T) {
	f := newFakeDocker()
	svc := function.Service{Entrypoint: "service.js", Port: 80, Replicas: 3}
	if _, err := reconcile(t, f, "fn", serviceTemplate("node24", svc), "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	slots3 := f.replicas("fn", "service.js")
	if len(slots3) != 3 {
		t.Fatalf("slots after scale to 3 = %v, want 3 entries", slots3)
	}

	svc.Replicas = 1
	if _, err := reconcile(t, f, "fn", serviceTemplate("node24", svc), "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	slots := f.replicas("fn", "service.js")
	if len(slots) != 1 || slots[0] != 0 {
		t.Fatalf("after scale-down kept slots = %v, want [0] (lowest replica)", slots)
	}
}

func TestReconcileServiceRemoved(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24",
		function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2},
		function.Service{Entrypoint: "old.js", Port: 80, Replicas: 1},
	)
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 1 {
		t.Fatalf("old.js running = %d, want 1", got)
	}

	// Remove old.js from the template; only service.js remains.
	reduced := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := reconcile(t, f, "fn", reduced, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile reduced: %v", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 0 {
		t.Fatalf("old.js running after removal = %d, want 0", got)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("service.js running = %d, want 2", got)
	}
}

func TestReconcilePortChange(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	old := f.replicas("fn", "service.js")
	if len(old) != 1 {
		t.Fatalf("slots = %v, want 1", old)
	}

	// Change the port: the now-stale-config container must be replaced.
	changed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1})
	if _, err := reconcile(t, f, "fn", changed, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile changed: %v", err)
	}
	// The single container should still run (replaced), on the new port.
	f.mu.Lock()
	var running *fakeContainer
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" && c.state == container.StateRunning {
			running = c
		}
	}
	f.mu.Unlock()
	if running == nil {
		t.Fatal("no running container after port change")
	}
	if running.port != 3000 {
		t.Fatalf("port = %d, want 3000 (replaced on port change)", running.port)
	}
	if len(f.stops) == 0 {
		t.Fatal("expected the old-config container to be stopped on port change")
	}
}

func TestReconcileImageChangeReplacesAll(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile img-1: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}

	// New image -> all stale -> replaced with the new image.
	if _, err := reconcile(t, f, "fn", tmpl, "img-2", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile img-2: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running after image change = %d, want 2", got)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" {
			if c.image != "img-2" {
				t.Errorf("container %s image = %q, want img-2", c.id, c.image)
			}
		}
	}
}

// A container running the OLD image (a rebuild left it on the superseded image)
// is never counted toward the desired replica count: it is stale and replaced,
// and exactly one new container on the desired image is started. This is the
// stale-by-image pin for the image-retirement ordering.
func TestReconcileServiceStaleByImage(t *testing.T) {
	f := newFakeDocker()
	// A running container for the old image on the desired function/entrypoint/
	// port/replica slot.
	f.ctrs["old-1"] = &fakeContainer{
		id: "old-1", function: "fn", entrypoint: "service.js",
		image: "img-old", port: 80, replica: 0, state: container.StateRunning,
	}

	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// The old-image container was stopped.
	if len(f.stops) != 1 || f.stops[0] != "old-1" {
		t.Fatalf("stops = %v, want [old-1]", f.stops)
	}
	// Exactly one running container remains, on the desired image, replica 0. The
	// old-image container must NEVER have counted toward the desired replica.
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want exactly 1", got)
	}
	var running *fakeContainer
	f.mu.Lock()
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" && c.state == container.StateRunning {
			running = c
		}
	}
	f.mu.Unlock()
	if running == nil {
		t.Fatal("no running container after reconcile")
	}
	if running.image != "img-new" {
		t.Fatalf("started spec image = %q, want img-new (the desired image)", running.image)
	}
	if running.replica != 0 {
		t.Fatalf("started replica = %d, want 0", running.replica)
	}
}

func TestReconcileCrashedReplicaRecreated(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	slots0 := f.replicas("fn", "service.js")
	if len(slots0) != 2 {
		t.Fatalf("slots = %v, want 2", slots0)
	}

	// Crash one replica: mark it exited.
	f.mu.Lock()
	var toCrash string
	for id, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" && c.state == container.StateRunning {
			toCrash = id
			break
		}
	}
	f.mu.Unlock()
	f.setState(toCrash, container.StateExited)

	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile after crash: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running after crash recreation = %d, want 2", got)
	}
	// Both desired slots must be populated again.
	slots := f.replicas("fn", "service.js")
	if len(slots) != 2 || slots[0] != 0 || slots[1] != 1 {
		t.Fatalf("slots after crash = %v, want [0 1]", slots)
	}
}

func TestReconcileUnlabeledReplicaTreatedStale(t *testing.T) {
	// A Replica == -1 container (legacy/unlabeled) is always stale and replaced.
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	slots0 := f.replicas("fn", "service.js")
	if len(slots0) != 1 {
		t.Fatalf("slots = %v, want 1", slots0)
	}

	// Force the container to appear unlabeled (replica -1).
	f.mu.Lock()
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" {
			c.replica = -1
		}
	}
	f.mu.Unlock()

	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile unlabeled: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (replaced on the labeled slot)", got)
	}
	f.mu.Lock()
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" {
			if c.replica != 0 {
				t.Fatalf("replica = %d, want 0 (relabeled on replacement)", c.replica)
			}
		}
	}
	f.mu.Unlock()
}

func TestReconcileLeavesOtherFunctionContainersUntouched(t *testing.T) {
	f := newFakeDocker()
	// Another function "other" pre-exists with a running container.
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "other", Identity: "svc.js", Port: 80, Image: "img-other",
	}, 0); err != nil {
		t.Fatalf("start other: %v", err)
	}

	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile fn: %v", err)
	}
	// "other"'s container is untouched.
	if got := f.runningCount("other", "svc.js"); got != 1 {
		t.Fatalf("other running = %d, want 1 (untouched)", got)
	}
	if got := f.countForFunction("other"); got != 1 {
		t.Fatalf("other count = %d, want 1", got)
	}
}

func TestReconcileStopFailureDoesNotAbort(t *testing.T) {
	// A Stop failure is surfaced but convergence of the rest still proceeds.
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	// Inject a running container for a service that will be REMOVED from the
	// template, and make stopping that exact id fail.
	f.mu.Lock()
	f.ctrs["id-1"] = &fakeContainer{id: "id-1", function: "fn", entrypoint: "old.js", image: "img-1", port: 80, replica: 0, state: container.StateRunning}
	f.mu.Unlock()
	f.failStopFor = "id-1"

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected an error surfaced from the failing stop")
	}
	if !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	// service.js still converges (started), the failed stop container remains
	// but convergence of the non-failing work proceeded.
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("service.js running = %d, want 1 (convergence proceeds despite stop failure)", got)
	}
}

func TestRemoveAll(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}

	RemoveAll(context.Background(), f, "fn", testutil.DiscardLogger())
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("count after RemoveAll = %d, want 0", got)
	}
}

func TestSweepOrphans(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "live", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile live: %v", err)
	}
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "ghost", Identity: "svc.js", Port: 80, Image: "img-ghost",
	}, 0); err != nil {
		t.Fatalf("start ghost: %v", err)
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger())
	c.SweepOrphans(context.Background(), map[string]bool{"live": true})

	if got := f.runningCount("live", "service.js"); got != 1 {
		t.Fatalf("live running = %d, want 1 (untouched)", got)
	}
	if got := f.countForFunction("ghost"); got != 0 {
		t.Fatalf("ghost count = %d, want 0 (orphaned)", got)
	}
}

func TestBuildEnvOrdering(t *testing.T) {
	tmpl := &function.Template{
		Runtime: "node24",
		Env:     map[string]string{"A": "template", "PORT": "9999"}, // PORT override attempt
		Secrets: map[string]function.SecretRef{"SECRET": "my-secret"},
	}
	provider := &fakeSecretResolver{values: map[string]string{"my-secret": "s3cr3t"}}

	env, err := BuildEnv(context.Background(), tmpl, 3000, []string{"PREPARED=1"}, provider)
	if err != nil {
		t.Fatalf("build env: %v", err)
	}
	want := []string{"PREPARED=1", "A=template", "PORT=9999", "SECRET=s3cr3t", "PORT=3000"}
	if len(env) != len(want) {
		t.Fatalf("env = %v, want %v", env, want)
	}
	for i := range want {
		if env[i] != want[i] {
			t.Fatalf("env[%d] = %q, want %q (full: %v)", i, env[i], want[i], env)
		}
	}
}

func TestBuildEnvMissingProviderErrors(t *testing.T) {
	tmpl := &function.Template{
		Runtime: "node24",
		Secrets: map[string]function.SecretRef{"SECRET": "my-secret"},
	}
	_, err := BuildEnv(context.Background(), tmpl, 3000, nil, nil)
	if err == nil {
		t.Fatal("expected an error when a secret is referenced but no provider is configured")
	}
}

// reconcileEnv runs Reconcile with a secret resolver and prepared env, for the
// env/secret staleness tests.
func reconcileEnv(t *testing.T, d Docker, fn string, tmpl *function.Template, image string, preparedEnv []string, secrets SecretResolver) (bool, error) {
	t.Helper()
	return Reconcile(context.Background(), d, fn, t.TempDir(), tmpl, image, preparedEnv, secrets, routing.TraefikConfig{}, testutil.DiscardLogger())
}

// TestReconcileEnvChangeReplacesContainer: changing a template env value on an
// `image` source (whose image reference is unchanged) leaves the running
// container's environment stale, so it must be replaced. This is the core
// regression the relay.env_hash label exists to catch.
func TestReconcileEnvChangeReplacesContainer(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	start.Env = map[string]string{"MODE": "a"}
	if _, err := reconcile(t, f, "fn", start, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=a: %v", err)
	}
	c := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if c == nil || c.envHash != runtime.EnvHash([]string{"MODE=a", "PORT=8080"}) {
		t.Fatalf("started container hash = %+v, want the hash of the env=a effective env", c)
	}

	// Same image reference, changed env value -> stale by env hash.
	changed := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	changed.Env = map[string]string{"MODE": "b"}
	if _, err := reconcile(t, f, "fn", changed, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=b: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the env-stale container replaced", f.stops)
	}
	c = f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if c == nil || c.envHash != runtime.EnvHash([]string{"MODE=b", "PORT=8080"}) {
		t.Fatalf("replacement hash = %+v, want the hash of the env=b effective env", c)
	}
}

// TestReconcileEnvRemovedReplacesContainer: removing a template env makes the
// previous container stale (the desired effective env shrank), so it is replaced.
func TestReconcileEnvRemovedReplacesContainer(t *testing.T) {
	f := newFakeDocker()
	withEnv := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	withEnv.Env = map[string]string{"FEATURE": "on"}
	if _, err := reconcile(t, f, "fn", withEnv, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile with env: %v", err)
	}

	withoutEnv := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", withoutEnv, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile without env: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the container replaced when an env was removed", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.envHash != runtime.EnvHash([]string{"PORT=80"}) {
		t.Fatalf("replacement hash = %+v, want the hash of the env-less effective env", c)
	}
}

// TestReconcileSecretRotationReplacesContainer: rotating a secret's VALUE (with
// an unchanged template and image, so the source fingerprint is unchanged) must
// replace the persistent container, because a long-lived container would
// otherwise serve the old secret forever. This is the behavior the per-invocation
// secret path gives event containers but the service path needs a container
// replacement for.
func TestReconcileSecretRotationReplacesContainer(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	tmpl.Secrets = map[string]function.SecretRef{"TOKEN": "api-token"}

	provider := &fakeSecretResolver{values: map[string]string{"api-token": "v1"}}
	if _, err := reconcileEnv(t, f, "fn", tmpl, "", nil, provider); err != nil {
		t.Fatalf("reconcile v1: %v", err)
	}
	first := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if first == nil {
		t.Fatal("no started container")
	}
	// The secret VALUE never appears in a label; only the digest does.
	for k, v := range first.labels {
		if strings.Contains(v, "v1") || strings.Contains(k, "TOKEN") {
			t.Fatalf("secret value/name leaked into label %q=%q", k, v)
		}
	}

	// Rotate the value: same template, same image, same fingerprint.
	provider.values["api-token"] = "v2"
	if _, err := reconcileEnv(t, f, "fn", tmpl, "", nil, provider); err != nil {
		t.Fatalf("reconcile v2: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the secret-rotated container replaced", f.stops)
	}
	second := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if second == nil {
		t.Fatal("no replacement container")
	}
	if second.envHash == first.envHash {
		t.Fatalf("env hash unchanged across secret rotation (%q); container would serve the old secret", second.envHash)
	}
	if second.envHash != runtime.EnvHash([]string{"TOKEN=v2", "PORT=8080"}) {
		t.Fatalf("replacement hash = %q, want the v2 effective env hash", second.envHash)
	}
}

// TestReconcileEnvUnchangedKeepsContainer: an unchanged effective env keeps the
// running container (no churn), pinning that the env-hash comparison is exact.
func TestReconcileEnvUnchangedKeepsContainer(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	tmpl.Env = map[string]string{"MODE": "stable"}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed || len(f.stops) != stopsSoFar {
		t.Fatalf("an unchanged env must keep the container: changed=%v stops %d->%d", changed, stopsSoFar, len(f.stops))
	}
}

// TestReconcileLegacyContainerWithoutEnvHashReplaced: a running container with
// no relay.env_hash (created before the label existed, or otherwise unwritable)
// never matches a desired hash, so it is replaced exactly once — the same
// missing-label semantics as relay.replica == -1.
func TestReconcileLegacyContainerWithoutEnvHashReplaced(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["legacy-1"] = &fakeContainer{
		id: "legacy-1", function: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		// No envHash: a pre-label container.
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.stops) != 1 || f.stops[0] != "legacy-1" {
		t.Fatalf("stops = %v, want [legacy-1] (an unlabeled env hash is always stale)", f.stops)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (replacement started)", got)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.envHash != serviceEnvHash(80) {
		t.Fatalf("replacement = %+v, want a stamped env hash", c)
	}
}

// TestReconcileBuildSourceEnvChangeReplaces: the env/secret comparison is
// source-agnostic — a `build` source (whose resolved image reference is
// content-addressed and unchanged by an env edit) is replaced on an env change
// just like an `image` source.
func TestReconcileBuildSourceEnvChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	f.resolvedImages["Dockerfile"] = "relay-fn-fn:buildtag"
	start := serviceTemplate("", function.Service{Build: "Dockerfile", Port: 3000, Replicas: 1})
	start.Env = map[string]string{"MODE": "a"}
	if _, err := reconcile(t, f, "fn", start, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=a: %v", err)
	}

	changed := serviceTemplate("", function.Service{Build: "Dockerfile", Port: 3000, Replicas: 1})
	changed.Env = map[string]string{"MODE": "b"}
	if _, err := reconcile(t, f, "fn", changed, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile env=b: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the build-source env-stale container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "Dockerfile")
	if c == nil || c.envHash != runtime.EnvHash([]string{"MODE=b", "PORT=3000"}) {
		t.Fatalf("replacement = %+v, want the env=b effective env hash", c)
	}
}

type fakeSecretResolver struct {
	mu     sync.Mutex
	values map[string]string
}

func (f *fakeSecretResolver) Resolve(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.values[name]
	if !ok {
		return "", fmt.Errorf("secret %q not found", name)
	}
	return v, nil
}

// A template that removed its services (desired = empty) converges any leftover
// containers away. This is the startup path for services removed while Relay
// was down: the reconciler takes the skip path (fingerprint re-seeded), so the
// startup Apply — not the reconciler — must do this cleanup.
func TestReconcileEmptyDesiredRemovesLeftovers(t *testing.T) {
	f := newFakeDocker()
	// Two leftovers from a previous boot: one for a removed service, one for a
	// function that no longer declares any services at all.
	f.ctrs["leftover-1"] = &fakeContainer{id: "leftover-1", function: "fn", entrypoint: "service.js", image: "img-old", port: 80, replica: 0, state: container.StateRunning}
	f.ctrs["leftover-2"] = &fakeContainer{id: "leftover-2", function: "fn", entrypoint: "other.js", image: "img-old", port: 3000, replica: 0, state: container.StateRunning}

	tmpl := serviceTemplate("node24") // no services
	if _, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("leftover containers = %d, want 0", got)
	}
}

// A reconcile whose service source cannot be resolved (here an entrypoint under
// an unsupported runtime) reports the failure and PRESERVES the existing
// containers: resolution happens before any container action, so a service that
// cannot produce a runnable image is never torn down for nothing.
func TestReconcileUnsupportedRuntimePreservesContainersAndFails(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["keep-1"] = &fakeContainer{id: "keep-1", function: "fn", entrypoint: "service.js", image: "img-old", port: 80, replica: 0, state: container.StateRunning}

	// An unsupported runtime makes ServiceEntry fail for any entrypoint.
	tmpl := serviceTemplate("rust", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected an error for the unresolvable entrypoint (unsupported runtime)")
	}
	if !strings.Contains(err.Error(), "unsupported runtime") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.countForFunction("fn"); got != 1 {
		t.Fatalf("existing containers must be preserved on a resolve failure, got %d", got)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none on a resolve failure", f.stops)
	}
}

// A nested entrypoint (a relative path inside the application directory)
// resolves via ServiceEntry and starts replicas with the correct launch path.
func TestReconcileNestedEntrypointResolvesAndStarts(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "app/service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "app/service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	// The started spec's Entry must be the /app-anchored nested launch path.
	f.mu.Lock()
	defer f.mu.Unlock()
	var started []string
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "app/service.js" {
			started = c.entry
		}
	}
	if len(started) != 2 || started[0] != "node" || started[1] != "/app/app/service.js" {
		t.Fatalf("entry = %v, want [node /app/app/service.js]", started)
	}
}

// A python3.14 service entrypoint resolves to module execution (python -m), and
// replicas start with that launch path.
func TestReconcilePythonServiceStartsWithModuleExecution(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("python3.14", function.Service{Entrypoint: "app/main.py", Port: 8000, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "app/main.py"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var started []string
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "app/main.py" {
			started = c.entry
		}
	}
	if len(started) != 3 || started[0] != "python" || started[1] != "-m" || started[2] != "app.main" {
		t.Fatalf("entry = %v, want [python -m app.main]", started)
	}
}

// A python service whose entrypoint is not a .py importable module cannot
// resolve its source (python.ServiceCommand errors): reconcile reports the
// failure, preserves existing containers, and never starts replicas.
func TestReconcilePythonNonPyEntrypointPreservesAndFails(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["keep-1"] = &fakeContainer{id: "keep-1", function: "fn", entrypoint: "app/main.js", image: "img-old", port: 8000, replica: 0, state: container.StateRunning}

	tmpl := serviceTemplate("python3.14", function.Service{Entrypoint: "app/main.js", Port: 8000, Replicas: 1})
	_, err := reconcile(t, f, "fn", tmpl, "img-new", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected an error for the unresolvable python entrypoint")
	}
	if !strings.Contains(err.Error(), "python services require a .py") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.countForFunction("fn"); got != 1 {
		t.Fatalf("existing containers must be preserved on a resolve failure, got %d", got)
	}
	// The preserved container is the pre-existing one (never a fresh start for
	// the unresolvable source) and is still running.
	if got := f.runningCount("fn", "app/main.js"); got != 1 {
		t.Fatalf("preserved container must still run, got %d running", got)
	}
	if len(f.resolveCalls) != 1 || f.resolveCalls[0] != "app/main.js" {
		t.Fatalf("resolve calls = %v, want a single resolution attempt", f.resolveCalls)
	}
}

// TestReconcileUnchangedIsNoOp: a fully-converged state (one running container
// with the correct image, port, and replica label) is a pass that performs NO
// convergence action — it returns (false, nil) and stops/starts nothing.
func TestReconcileUnchangedIsNoOp(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["id-1"] = &fakeContainer{
		id: "id-1", function: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80),
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if changed {
		t.Fatalf("changed = true, want false (already converged)")
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none for a no-op", f.stops)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (untouched)", got)
	}
}

// TestReconcileListErrorSurfaces verifies a ServiceContainerList failure is
// surfaced (and no convergence action is attempted): without a listing the
// running set is unknowable, so Reconcile must not guess.
func TestReconcileListErrorSurfaces(t *testing.T) {
	d := &listErrDocker{err: fmt.Errorf("daemon down")}
	changed, err := reconcile(t, d, "fn", serviceTemplate("node24"), "img-1", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected the list error to surface")
	}
	if !strings.Contains(err.Error(), "list containers") {
		t.Fatalf("unexpected error: %v", err)
	}
	if changed {
		t.Fatal("changed = true on a list failure, want false")
	}
}

// listErrDocker is a Docker whose ServiceContainerList always fails.
type listErrDocker struct{ err error }

func (d *listErrDocker) ResolveServiceImage(
	_ context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}
func (d *listErrDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "", nil
}
func (d *listErrDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, d.err
}
func (d *listErrDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *listErrDocker) RemoveFunctionServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *listErrDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }
func (d *listErrDocker) VerifyNetworks(context.Context, []string) (string, bool, error) {
	return "", true, nil
}

// TestReconcileStartServiceFailureContinues verifies a StartService failure for
// one replica is reported while the remaining desired replicas are still
// attempted (fail() records the first error without aborting the loop).
func TestReconcileStartServiceFailureContinues(t *testing.T) {
	f := &startFailDocker{failReplica: 0}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})

	_, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err == nil {
		t.Fatal("expected the StartService failure to surface")
	}
	if !strings.Contains(err.Error(), "replica 0") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.started; len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("started replicas = %v, want [0 1] (replica 0 failed, replica 1 still attempted)", got)
	}
}

// startFailDocker records StartService attempts and fails one replica slot.
type startFailDocker struct {
	failReplica int
	started     []int
}

func (d *startFailDocker) ResolveServiceImage(
	_ context.Context, _, _ string, tmpl *function.Template, svc function.Service, functionImage string,
) (runtime.ServiceImage, error) {
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: functionImage, Entry: entry}, nil
}
func (d *startFailDocker) StartService(_ context.Context, _ runtime.ServiceSpec, replica int) (string, error) {
	d.started = append(d.started, replica)
	if replica == d.failReplica {
		return "", fmt.Errorf("start failed for replica %d", replica)
	}
	return "id", nil
}
func (d *startFailDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	return nil, nil
}
func (d *startFailDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}
func (d *startFailDocker) RemoveFunctionServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}
func (d *startFailDocker) NetworkExists(context.Context, string) (bool, error) { return true, nil }
func (d *startFailDocker) VerifyNetworks(context.Context, []string) (string, bool, error) {
	return "", true, nil
}

// captureLogger is a minimal in-memory slog writer used to assert the level and
// text of ServiceReconciler.Apply's summary lines. Apply (and Reconcile) run
// synchronously in this test, so a plain strings.Builder is safe.
type captureLogger struct{ buf strings.Builder }

func (c *captureLogger) Write(p []byte) (int, error) { return c.buf.Write(p) }

func (c *captureLogger) String() string { return c.buf.String() }

// TestApplyNoOpLogsDebugNotInfo: applying a converged state logs "Service:
// unchanged" at Debug and never the Info "Service: reconciled" — neither the
// first nor a repeated (idempotent) pass.
func TestApplyNoOpLogsDebugNotInfo(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["id-1"] = &fakeContainer{
		id: "id-1", function: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80),
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	logger, capture := newCaptureLogger(slog.LevelDebug)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)

	c.Apply(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil)
	c.Apply(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil) // idempotent second pass

	out := capture.String()
	if !strings.Contains(out, "Service: unchanged") {
		t.Fatalf("expected no-op Apply to log \"Service: unchanged\", got:\n%s", out)
	}
	if !strings.Contains(out, "level=DEBUG") {
		t.Fatalf("expected the no-op pass at DEBUG level, got:\n%s", out)
	}
	if strings.Contains(out, "Service: reconciled") {
		t.Fatalf("no-op Apply must not log the Info \"Service: reconciled\" line, got:\n%s", out)
	}
}

// TestApplyChangedLogsInfo: a missing replica triggers a start, so Apply logs
// the Info "Service: reconciled" line (and performs the change).
func TestApplyChangedLogsInfo(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	logger, capture := newCaptureLogger(slog.LevelInfo)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)
	c.Apply(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil)

	out := capture.String()
	if !strings.Contains(out, "Service: reconciled") {
		t.Fatalf("expected the changed Apply to log Info \"Service: reconciled\", got:\n%s", out)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (the start happened)", got)
	}
}

// TestApplyChangedStaleReplacement: a stale container (wrong image) is replaced
// on Apply, which logs Info and stops the old / starts the new with the desired
// image.
func TestApplyChangedStaleReplacement(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["old-1"] = &fakeContainer{
		id: "old-1", function: "fn", entrypoint: "service.js",
		image: "img-old", port: 80, replica: 0, state: container.StateRunning,
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	logger, capture := newCaptureLogger(slog.LevelInfo)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)
	c.Apply(context.Background(), "fn", t.TempDir(), tmpl, "img-new", nil)

	out := capture.String()
	if !strings.Contains(out, "Service: reconciled") {
		t.Fatalf("expected the stale-replacement Apply to log Info, got:\n%s", out)
	}
	if len(f.stops) != 1 || f.stops[0] != "old-1" {
		t.Fatalf("stops = %v, want [old-1]", f.stops)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var running *fakeContainer
	for _, c := range f.ctrs {
		if c.function == "fn" && c.entrypoint == "service.js" && c.state == container.StateRunning {
			running = c
		}
	}
	if running == nil {
		t.Fatal("no running container after stale replacement")
	}
	if running.image != "img-new" {
		t.Fatalf("started image = %q, want img-new (the desired image)", running.image)
	}
}

// TestApplyErrorLogsWarn: a stop failure makes Apply log the Warn
// "Service: reconciled with errors" line.
func TestApplyErrorLogsWarn(t *testing.T) {
	f := newFakeDocker()
	f.mu.Lock()
	f.ctrs["id-1"] = &fakeContainer{
		id: "id-1", function: "fn", entrypoint: "old.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
	}
	f.mu.Unlock()
	f.failStopFor = "id-1"
	tmpl := serviceTemplate("node24")

	logger, capture := newCaptureLogger(slog.LevelWarn)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)
	c.Apply(context.Background(), "fn", t.TempDir(), tmpl, "img-1", nil)

	out := capture.String()
	if !strings.Contains(out, "Service: reconciled with errors") {
		t.Fatalf("expected the failing Apply to log Warn, got:\n%s", out)
	}
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected the failing pass at WARN level, got:\n%s", out)
	}
}

// newCaptureLogger builds a slog.Logger (at the given level) writing into a
// captureLogger, and returns both.
func newCaptureLogger(level slog.Level) (*slog.Logger, *captureLogger) {
	c := &captureLogger{}
	h := slog.NewTextHandler(c, &slog.HandlerOptions{Level: level})
	return slog.New(h), c
}

// A build-source service starts with the resolved build image and NO entrypoint
// override, so the image's own ENTRYPOINT/CMD is preserved.
func TestReconcileBuildServiceStartPreservesImageEntrypoint(t *testing.T) {
	f := newFakeDocker()
	f.resolvedImages["docker/Dockerfile.prod"] = "relay-fn-fn:buildtag"
	tmpl := serviceTemplate("", function.Service{Build: "docker/Dockerfile.prod", Port: 3000, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "docker/Dockerfile.prod")
	if c == nil {
		t.Fatal("no started container for the build service")
	}
	if c.image != "relay-fn-fn:buildtag" {
		t.Fatalf("image = %q, want the resolved build image", c.image)
	}
	if c.entry != nil {
		t.Fatalf("entry = %v, want nil (preserve image ENTRYPOINT/CMD)", c.entry)
	}
}

// An image-source service starts with the external reference and its content ID,
// and no entrypoint override.
func TestReconcileImageServiceStartPreservesImageEntrypoint(t *testing.T) {
	f := newFakeDocker()
	f.resolvedImages["ghcr.io/acme/api:1.2"] = "sha256:cafe"
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if c == nil {
		t.Fatal("no started container for the image service")
	}
	if c.image != "ghcr.io/acme/api:1.2" || c.imageID != "sha256:cafe" {
		t.Fatalf("image/imageID = %q/%q, want the external ref and its content id", c.image, c.imageID)
	}
	if c.entry != nil {
		t.Fatalf("entry = %v, want nil (preserve image ENTRYPOINT/CMD)", c.entry)
	}
}

// A changed external image CONTENT (a moved tag) replaces the running container,
// even though the image reference string is unchanged.
func TestReconcileImageContentChangeReplacesContainer(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:latest", Port: 8080, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	first := f.lastStartedFor("fn", "ghcr.io/acme/api:latest")
	if first == nil || first.imageID != "ghcr.io/acme/api:latest-id" {
		t.Fatalf("first container = %+v", first)
	}

	// The tag now points at different bytes.
	f.mu.Lock()
	f.resolvedImages["ghcr.io/acme/api:latest"] = "sha256:moved"
	f.mu.Unlock()
	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	f.mu.Lock()
	var running []*fakeContainer
	for _, c := range f.ctrs {
		if c.function == "fn" && c.state == container.StateRunning {
			running = append(running, c)
		}
	}
	f.mu.Unlock()
	if len(running) != 1 {
		t.Fatalf("running = %d, want 1", len(running))
	}
	if running[0].imageID != "sha256:moved" {
		t.Fatalf("running imageID = %q, want the moved content id", running[0].imageID)
	}
	if len(f.stops) == 0 {
		t.Fatal("expected the content-changed container to be stopped/replaced")
	}
}

// A build/pull resolution failure preserves the existing healthy container: it is
// neither stopped nor replaced, and the error surfaces.
func TestReconcileResolveFailurePreservesHealthyContainer(t *testing.T) {
	f := newFakeDocker()
	f.mu.Lock()
	f.resolvedImages["ghcr.io/acme/api:1.2"] = "sha256:cafe"
	f.mu.Unlock()
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	before := f.runningCount("fn", "ghcr.io/acme/api:1.2")
	if before != 1 {
		t.Fatalf("running after first reconcile = %d, want 1", before)
	}

	// The registry is now unreachable.
	f.mu.Lock()
	f.resolveErr["ghcr.io/acme/api:1.2"] = fmt.Errorf("pull failed: registry down")
	f.mu.Unlock()
	_, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{})
	if err == nil || !strings.Contains(err.Error(), "registry down") {
		t.Fatalf("err = %v, want the resolve failure surfaced", err)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1.2"); got != 1 {
		t.Fatalf("running after a resolve failure = %d, want 1 (preserved)", got)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none on a resolve failure", f.stops)
	}
}

// A removed service whose source identity is no longer in the template is
// stopped regardless of source kind.
func TestReconcileRemovedImageServiceStopped(t *testing.T) {
	f := newFakeDocker()
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "fn", Identity: "ghcr.io/acme/old:1", Port: 80, Image: "ghcr.io/acme/old:1",
	}, 0); err != nil {
		t.Fatalf("seed old service: %v", err)
	}
	tmpl := serviceTemplate("", function.Service{Image: "ghcr.io/acme/new:1", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if f.runningCount("fn", "ghcr.io/acme/old:1") != 0 {
		t.Fatal("removed image service container should be stopped")
	}
}

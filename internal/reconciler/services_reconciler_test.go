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
)

// fakeContainer is one in-memory service container.
type fakeContainer struct {
	id         string
	function   string
	entrypoint string
	image      string
	port       int
	replica    int
	state      container.ContainerState
	entry      []string // the long-lived process command passed to StartService
	labels     map[string]string
	network    string // spec.Network passed to StartService
}

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
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		ctrs:            map[string]*fakeContainer{},
		missingNetworks: map[string]bool{},
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
		entrypoint: spec.Entrypoint,
		image:      spec.Image,
		port:       spec.Port,
		replica:    replica,
		state:      container.StateRunning,
		entry:      spec.Entry,
		labels:     spec.Labels,
		network:    spec.Network,
	}
	return id, nil
}

func (f *fakeDocker) NetworkExists(_ context.Context, network string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networkLookups = append(f.networkLookups, network)
	return !f.missingNetworks[network], nil
}

func (f *fakeDocker) ServiceContainerList(context.Context) ([]runtime.ServiceContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.ServiceContainer
	for _, c := range f.ctrs {
		out = append(out, runtime.ServiceContainer{
			ID:         c.id,
			Function:   c.function,
			Entrypoint: c.entrypoint,
			Image:      c.image,
			State:      c.state,
			Replica:    c.replica,
			Port:       c.port,
			Labels:     c.labels,
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

// setImage mutates a container's image (for rebuild tests via the fake).
func (f *fakeDocker) setImage(id, image string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.ctrs[id]; ok {
		c.image = image
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

func noLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestReconcileInitialCreation(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})

	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running after idle reconcile = %d, want 2", got)
	}
}

func TestReconcileScaleUp(t *testing.T) {
	f := newFakeDocker()
	svc := function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1}
	if _, err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}

	svc.Replicas = 3
	if _, err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	slots3 := f.replicas("fn", "service.js")
	if len(slots3) != 3 {
		t.Fatalf("slots after scale to 3 = %v, want 3 entries", slots3)
	}

	svc.Replicas = 1
	if _, err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 1 {
		t.Fatalf("old.js running = %d, want 1", got)
	}

	// Remove old.js from the template; only service.js remains.
	reduced := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := Reconcile(context.Background(), f, "fn", reduced, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	old := f.replicas("fn", "service.js")
	if len(old) != 1 {
		t.Fatalf("slots = %v, want 1", old)
	}

	// Change the port: the now-stale-config container must be replaced.
	changed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "fn", changed, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile img-1: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}

	// New image -> all stale -> replaced with the new image.
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-2", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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

	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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

	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
		Function: "other", Entrypoint: "svc.js", Port: 80, Image: "img-other",
	}, 0); err != nil {
		t.Fatalf("start other: %v", err)
	}

	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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

	_, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog())
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

func containsErr(err error, substr string) bool {
	return err != nil && len(err.Error()) >= len(substr) && err.Error()[:len(substr)] == substr
}

func TestRemoveAll(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}

	RemoveAll(context.Background(), f, "fn", noLog())
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("count after RemoveAll = %d, want 0", got)
	}
}

func TestSweepOrphans(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "live", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile live: %v", err)
	}
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "ghost", Entrypoint: "svc.js", Port: 80, Image: "img-ghost",
	}, 0); err != nil {
		t.Fatalf("start ghost: %v", err)
	}

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, noLog())
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("leftover containers = %d, want 0", got)
	}
}

// A reconcile whose service runtime is unsupported cannot resolve the entrypoint
// (ServiceEntry errors): it still removes stale containers and reports the
// failure, but never starts replicas for that service.
func TestReconcileUnsupportedRuntimeStopsStaleAndFails(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["stale-1"] = &fakeContainer{id: "stale-1", function: "fn", entrypoint: "service.js", image: "img-old", port: 80, replica: 0, state: container.StateRunning}

	// An unsupported runtime makes ServiceEntry fail for any entrypoint.
	tmpl := serviceTemplate("rust", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	_, err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, routing.TraefikConfig{}, noLog())
	if err == nil {
		t.Fatal("expected an error for the unresolvable entrypoint (unsupported runtime)")
	}
	if !strings.Contains(err.Error(), "unsupported runtime") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("stale containers must still be removed, got %d", got)
	}
}

// A nested entrypoint (a relative path inside the application directory)
// resolves via ServiceEntry and starts replicas with the correct launch path.
func TestReconcileNestedEntrypointResolvesAndStarts(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "app/service.js", Port: 80, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
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
// resolve its entrypoint (python.ServiceCommand errors): reconcile still stops
// stale containers but never starts replicas for that service.
func TestReconcilePythonNonPyEntrypointStopsStaleAndFails(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["stale-1"] = &fakeContainer{id: "stale-1", function: "fn", entrypoint: "app/main.js", image: "img-old", port: 8000, replica: 0, state: container.StateRunning}

	tmpl := serviceTemplate("python3.14", function.Service{Entrypoint: "app/main.js", Port: 8000, Replicas: 1})
	_, err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, routing.TraefikConfig{}, noLog())
	if err == nil {
		t.Fatal("expected an error for the unresolvable python entrypoint")
	}
	if !strings.Contains(err.Error(), "python services require a .py") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := f.countForFunction("fn"); got != 0 {
		t.Fatalf("stale containers must still be removed, got %d", got)
	}
	if got := f.runningCount("fn", "app/main.js"); got != 0 {
		t.Fatalf("python non-importable service must never start, got %d running", got)
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
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	changed, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog())
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
	}
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})

	logger, capture := newCaptureLogger(slog.LevelDebug)
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, logger)

	c.Apply(context.Background(), "fn", tmpl, "img-1", nil)
	c.Apply(context.Background(), "fn", tmpl, "img-1", nil) // idempotent second pass

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
	c.Apply(context.Background(), "fn", tmpl, "img-1", nil)

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
	c.Apply(context.Background(), "fn", tmpl, "img-new", nil)

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
	c.Apply(context.Background(), "fn", tmpl, "img-1", nil)

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

// Traefik-label helpers used by the routing tests below.
const (
	routingEnableKey     = "traefik.enable"
	routingNetworkKey    = "traefik.docker.network"
	routingRouterPrefix  = "traefik.http.routers."
	routingServicePrefix = "traefik.http.services."
)

// lastStartedFor returns the most recently started fakeContainer for fn/entry.
func (f *fakeDocker) lastStartedFor(fn, entrypoint string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeContainer
	for _, c := range f.ctrs {
		if c.function == fn && c.entrypoint == entrypoint {
			found = c
		}
	}
	return found
}

// hasTraefikKey reports whether the label map carries any traefik.* key.
func hasTraefikKey(labels map[string]string) bool {
	for k := range labels {
		if strings.HasPrefix(k, "traefik.") {
			return true
		}
	}
	return false
}

// An unrouted service with a zero TraefikConfig reconciles normally: no
// NetworkExists lookup, no traefik.* labels on the started container.
func TestReconcileUnroutedNoRoutingActivity(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called %d times for an unrouted service, want 0", len(f.networkLookups))
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("unrouted container labels contain traefik keys: %v", c.labels)
	}
	if c.network != "" {
		t.Fatalf("unrouted container network = %q, want \"\"", c.network)
	}
}

// A routed service with an empty TraefikConfig fails validation and performs
// no container action: no stops, no starts, existing containers left alone.
func TestReconcileRoutedMissingTraefikConfig(t *testing.T) {
	f := newFakeDocker()
	f.mu.Lock()
	f.ctrs["keep-1"] = &fakeContainer{id: "keep-1", function: "fn", entrypoint: "service.js", image: "img-1", port: 80, replica: 0, state: container.StateRunning}
	f.mu.Unlock()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "service.test"})

	_, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{}, noLog())
	if err == nil {
		t.Fatal("expected a routing-validation error")
	}
	if !strings.Contains(err.Error(), `service "service.js": TRAEFIK_NETWORK is required when Traefik routing is configured`) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.networkLookups) != 0 {
		t.Fatalf("NetworkExists called, want 0 (validate fails first)")
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none (existing containers left alone)", f.stops)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (nothing replaced)", got)
	}
}

// A routed service whose configured network does not exist is refused; Relay
// does NOT create the network (zero StartService calls).
func TestReconcileRoutedMissingNetwork(t *testing.T) {
	f := newFakeDocker()
	f.missingNetworks["proxy"] = true
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1, Host: "service.test"})

	_, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog())
	if err == nil {
		t.Fatal("expected a missing-network error")
	}
	if !strings.Contains(err.Error(), `service "service.js": Traefik network "proxy" does not exist`) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
		t.Fatalf("networkLookups = %v, want a single [proxy] lookup", f.networkLookups)
	}
	if len(f.ctrs) != 0 {
		t.Fatalf("StartService was called for a missing routing network; want none, ctrs = %v", f.ctrs)
	}
	if len(f.stops) != 0 {
		t.Fatalf("stops = %v, want none", f.stops)
	}
}

// A routed service happy path: labels and network reach the started container.
func TestReconcileRoutedHappyPath(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "service.test"})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no started container")
	}
	if c.labels[routingEnableKey] != "true" {
		t.Fatalf("traefik.enable = %q, want true", c.labels[routingEnableKey])
	}
	if c.labels[routingNetworkKey] != "proxy" {
		t.Fatalf("traefik.docker.network = %q, want proxy", c.labels[routingNetworkKey])
	}
	id := "relay-fn-service-js" // relay-<fn>-<entrypoint>
	wantRule := "Host(`service.test`)"
	if c.labels[routingRouterPrefix+id+".rule"] != wantRule {
		t.Fatalf("router rule = %q, want %q", c.labels[routingRouterPrefix+id+".rule"], wantRule)
	}
	if _, ok := c.labels[routingServicePrefix+id+".loadbalancer.server.port"]; !ok {
		t.Fatalf("loadbalancer port label missing in %v", c.labels)
	}
	if c.network != "proxy" {
		t.Fatalf("started network = %q, want proxy", c.network)
	}
	if len(f.networkLookups) != 1 || f.networkLookups[0] != "proxy" {
		t.Fatalf("networkLookups = %v, want [proxy]", f.networkLookups)
	}
}

// A host change (a.test -> b.test) replaces the existing routed container: the
// old one is stopped and the replacement carries the new rule.
func TestReconcileHostChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := Reconcile(context.Background(), f, "fn", start, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile a.test: %v", err)
	}

	changed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "b.test"})
	if _, err := Reconcile(context.Background(), f, "fn", changed, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile b.test: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	id := "relay-fn-service-js"
	if c.labels[routingRouterPrefix+id+".rule"] != "Host(`b.test`)" {
		t.Fatalf("replacement rule = %q, want Host(`b.test`)", c.labels[routingRouterPrefix+id+".rule"])
	}
}

// A TRAEFIK_NETWORK change also replaces the routed container.
func TestReconcileNetworkChangeReplaces(t *testing.T) {
	f := newFakeDocker()
	start := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := Reconcile(context.Background(), f, "fn", start, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile proxy: %v", err)
	}
	if _, err := Reconcile(context.Background(), f, "fn", start, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy2"}, noLog()); err != nil {
		t.Fatalf("reconcile proxy2: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want one replaced container", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.network != "proxy2" || c.labels[routingNetworkKey] != "proxy2" {
		t.Fatalf("replacement container = %+v, want network/labels proxy2", c)
	}
}

// A HOST removed from a routed template makes the previously routed container
// stale: it is replaced with an unlabeled (no traefik.*) container and it no
// longer joins the routing network.
func TestReconcileHostRemovedReplacesUnrouted(t *testing.T) {
	f := newFakeDocker()
	routed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := Reconcile(context.Background(), f, "fn", routed, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile routed: %v", err)
	}

	unrouted := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1})
	if _, err := Reconcile(context.Background(), f, "fn", unrouted, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile unrouted: %v", err)
	}
	if len(f.stops) != 1 {
		t.Fatalf("stops = %v, want the routed container replaced", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil {
		t.Fatal("no replacement container")
	}
	if hasTraefikKey(c.labels) {
		t.Fatalf("replacement labels contain traefik keys: %v", c.labels)
	}
	if c.network != "" {
		t.Fatalf("replacement network = %q, want \"\"", c.network)
	}
}

// A converged ROUTED state (running container with matching labels) is a no-op
// pass: changed == false, no stops, no starts.
func TestReconcileRoutedConvergedNoOp(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1, Host: "a.test"})
	if _, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, routing.TraefikConfig{Network: "proxy"}, noLog())
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed {
		t.Fatal("converged routed state must be a no-op (changed = false)")
	}
	if len(f.stops) != stopsSoFar {
		t.Fatalf("stops grew from %d to %d on a converged pass", stopsSoFar, len(f.stops))
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1 (untouched)", got)
	}
}

// routingLabelsMatch unit tests pin the nil-vs-nil and extra-traefik-key rules.
func TestRoutingLabelsMatch(t *testing.T) {
	if !routingLabelsMatch(nil, nil) {
		t.Error("nil-vs-nil must match (unrouted, unlabeled)")
	}
	desired := map[string]string{"traefik.enable": "true", "traefik.docker.network": "proxy"}
	if !routingLabelsMatch(desired, map[string]string{
		"relay.type":             "service",
		"relay.function":         "fn",
		"traefik.enable":         "true",
		"traefik.docker.network": "proxy",
	}) {
		t.Error("actual with all desired keys plus non-traefik extras must match")
	}
	if routingLabelsMatch(desired, map[string]string{"traefik.enable": "false"}) {
		t.Error("wrong desired value must not match")
	}
	if routingLabelsMatch(nil, map[string]string{"traefik.enable": "true"}) {
		t.Error("a container with traefik labels must never match an unrouted service")
	}
	if routingLabelsMatch(desired, map[string]string{"traefik.enable": "true", "traefik.docker.network": "proxy", "traefik.http.routers.old.rule": "Host(`old.test`)"}) {
		t.Error("a stale extra traefik key must not match")
	}
	// The desired nil actual case: routed service, container without the labels.
	if routingLabelsMatch(map[string]string{"traefik.enable": "true"}, nil) {
		t.Error("routed desired vs unlabeled container must not match")
	}
}

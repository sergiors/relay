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
}

// fakeDocker is an in-memory Docker for Reconcile unit tests. Containers are
// keyed by a deterministic id so tests can inspect and manipulate them.
type fakeDocker struct {
	mu          sync.Mutex
	nextID      int
	ctrs        map[string]*fakeContainer
	stops       []string // container ids stopped (in order), for ordering assertions
	failStopFor string   // when non-empty, StopServiceContainers errors for this id
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{ctrs: map[string]*fakeContainer{}}
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
	}
	return id, nil
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

	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running after idle reconcile = %d, want 2", got)
	}
}

func TestReconcileScaleUp(t *testing.T) {
	f := newFakeDocker()
	svc := function.Service{Entrypoint: "service.js", Port: 80, Replicas: 1}
	if err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}

	svc.Replicas = 3
	if err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile 3: %v", err)
	}
	slots3 := f.replicas("fn", "service.js")
	if len(slots3) != 3 {
		t.Fatalf("slots after scale to 3 = %v, want 3 entries", slots3)
	}

	svc.Replicas = 1
	if err := Reconcile(context.Background(), f, "fn", serviceTemplate("node24", svc), "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := f.runningCount("fn", "old.js"); got != 1 {
		t.Fatalf("old.js running = %d, want 1", got)
	}

	// Remove old.js from the template; only service.js remains.
	reduced := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if err := Reconcile(context.Background(), f, "fn", reduced, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	old := f.replicas("fn", "service.js")
	if len(old) != 1 {
		t.Fatalf("slots = %v, want 1", old)
	}

	// Change the port: the now-stale-config container must be replaced.
	changed := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 3000, Replicas: 1})
	if err := Reconcile(context.Background(), f, "fn", changed, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile img-1: %v", err)
	}
	if got := f.runningCount("fn", "service.js"); got != 2 {
		t.Fatalf("running = %d, want 2", got)
	}

	// New image -> all stale -> replaced with the new image.
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-2", nil, nil, noLog()); err != nil {
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

func TestReconcileCrashedReplicaRecreated(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", function.Service{Entrypoint: "service.js", Port: 80, Replicas: 2})
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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

	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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

	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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

	err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog())
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "live", tmpl, "img-1", nil, nil, noLog()); err != nil {
		t.Fatalf("reconcile live: %v", err)
	}
	if _, err := f.StartService(context.Background(), runtime.ServiceSpec{
		Function: "ghost", Entrypoint: "svc.js", Port: 80, Image: "img-ghost",
	}, 0); err != nil {
		t.Fatalf("start ghost: %v", err)
	}

	c := NewServiceReconciler(f, nil, noLog())
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, noLog()); err != nil {
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
	err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, noLog())
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	if err := Reconcile(context.Background(), f, "fn", tmpl, "img-1", nil, nil, noLog()); err != nil {
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
	err := Reconcile(context.Background(), f, "fn", tmpl, "img-new", nil, nil, noLog())
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

package reconciler

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// restartFakeDocker is a fakeDocker that also implements the optional
// ServiceRestarter capability, so tests can exercise HOT_RELOAD's in-place
// source reload through the production ServiceReconciler entry point. Restarting
// keeps the SAME container ID and state (mirroring Docker ContainerRestart).
type restartFakeDocker struct {
	*fakeDocker
	restarts       []string
	failRestartFor string
}

func newRestartFakeDocker() *restartFakeDocker {
	return &restartFakeDocker{fakeDocker: newFakeDocker()}
}

func (f *restartFakeDocker) RestartServiceContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == f.failRestartFor {
		return fmt.Errorf("restart failed for %s", id)
	}
	c, ok := f.ctrs[id]
	if !ok {
		return fmt.Errorf("restart: container %s not found", id)
	}
	f.restarts = append(f.restarts, id)
	f.recordEvent("restart:" + id)
	c.state = container.StateRunning
	return nil
}

func (f *restartFakeDocker) restartCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.restarts)
}

// readinessFakeDocker overlays an explicit backend readiness observation on a
// restartFakeDocker. Test-only; the production Docker backend leaves the
// observation unknown (nil).
type readinessFakeDocker struct {
	*restartFakeDocker
	unready map[string]bool
}

func newReadinessFakeDocker() *readinessFakeDocker {
	return &readinessFakeDocker{restartFakeDocker: newRestartFakeDocker()}
}

func (f *readinessFakeDocker) ServiceContainerList(
	ctx context.Context,
) ([]runtime.ServiceContainer, error) {
	containers, err := f.restartFakeDocker.ServiceContainerList(ctx)
	if err != nil {
		return nil, err
	}
	for i := range containers {
		ready := !f.unready[containers[i].ID]
		containers[i].Ready = &ready
	}
	return containers, nil
}

// TestServiceReconcileUnreadySourceCandidateIsNotRestarted pins the readiness
// edge of HOT_RELOAD: an explicitly unready source-stale candidate must not be
// in-place restarted and then have the slot's actually-ready generation stopped.
// The ready generation is restarted instead; the unready one is replaced.
func TestServiceReconcileUnreadySourceCandidateIsNotRestarted(t *testing.T) {
	f := newReadinessFakeDocker()
	f.setMountIdentity("service.py", "newfp")
	f.mu.Lock()
	f.ctrs["a-unready"] = &fakeContainer{
		id: "a-unready", appName: "fn", entrypoint: "service.py",
		sourceRef: "service.py", image: "img-1", port: 80, replica: 0,
		state: container.StateRunning, sourceID: "oldfp",
		environmentFingerprint: serviceEnvironmentFingerprint(80),
		resources:              serviceResources(),
	}
	f.ctrs["b-ready"] = &fakeContainer{
		id: "b-ready", appName: "fn", entrypoint: "service.py",
		sourceRef: "service.py", image: "img-1", port: 80, replica: 0,
		state: container.StateRunning, sourceID: "oldfp",
		environmentFingerprint: serviceEnvironmentFingerprint(80),
		resources:              serviceResources(),
	}
	f.mu.Unlock()
	f.unready = map[string]bool{"a-unready": true}

	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.py", Entrypoint: "service.py", Port: 80, Replicas: 1,
	})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	result := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if result.Err != nil {
		t.Fatalf("apply: %v", result.Err)
	}
	if result.Pending() {
		t.Fatal("pass ended pending, want the ready generation restarted")
	}

	f.mu.Lock()
	restarts := append([]string(nil), f.restarts...)
	stops := append([]string(nil), f.stops...)
	f.mu.Unlock()
	if len(restarts) != 1 || restarts[0] != "b-ready" {
		t.Fatalf("restarts = %v, want only the actually-ready container b-ready", restarts)
	}
	if containsString(stops, "b-ready") {
		t.Fatalf("stops = %v, must not stop the actually-ready generation", stops)
	}
	if !containsString(stops, "a-unready") {
		t.Fatalf("stops = %v, want the unready candidate replaced after the ready restart", stops)
	}
}

// TestServiceReconcileRestartedUnreadyCandidateRemainsPending pins the other
// half: once a restarted candidate is explicitly observed as unready, the next
// pass must remain pending instead of treating it as converged or starting a
// duplicate replacement.
func TestServiceReconcileRestartedUnreadyCandidateRemainsPending(t *testing.T) {
	f := newReadinessFakeDocker()
	f.setMountIdentity("service.py", "newfp")
	f.mu.Lock()
	f.ctrs["svc"] = &fakeContainer{
		id: "svc", appName: "fn", entrypoint: "service.py",
		sourceRef: "service.py", image: "img-1", port: 80, replica: 0,
		state: container.StateRunning, sourceID: "oldfp",
		environmentFingerprint: serviceEnvironmentFingerprint(80),
		resources:              serviceResources(),
	}
	f.mu.Unlock()

	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.py", Entrypoint: "service.py", Port: 80, Replicas: 1,
	})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	first := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if first.Err != nil || first.Pending() {
		t.Fatalf("first pass = %+v, want a terminal in-place restart", first)
	}
	if got := f.restartCount(); got != 1 {
		t.Fatalf("restarts after first pass = %d, want 1", got)
	}

	f.unready = map[string]bool{"svc": true}
	second := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !second.Pending() {
		t.Fatalf("second pass outcome = %v, want pending for the explicitly unready candidate", second.Outcome)
	}

	f.mu.Lock()
	restarts := len(f.restarts)
	stops := append([]string(nil), f.stops...)
	containers := len(f.ctrs)
	f.mu.Unlock()
	if restarts != 1 {
		t.Fatalf("restarts after second pass = %d, want no second restart", restarts)
	}
	if len(stops) != 0 {
		t.Fatalf("stops after second pass = %v, want none", stops)
	}
	if containers != 1 {
		t.Fatalf("containers after second pass = %d, want no duplicate replacement", containers)
	}
}

// onlyContainerID returns the single container ID for fn/service, failing when
// there is not exactly one (any state).
func onlyContainerID(t *testing.T, f *fakeDocker, fn, service string) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id, c := range f.ctrs {
		if c.appName == fn && c.entrypoint == service {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) != 1 {
		t.Fatalf("found %d containers for %s/%s, want 1", len(ids), fn, service)
	}
	return ids[0]
}

// applyServices is the HOT_RELOAD service-reconciler entry point: it runs the
// reconciler (which supplies the source-apply tracker) rather than the
// package-level Reconcile, so in-place reload is active.
func applyServices(t *testing.T, c *ServiceReconciler, fn string, tmpl *app.Template, image string) error {
	t.Helper()
	return c.ApplyResult(context.Background(), fn, tmpl, image, nil, nil).Err
}

// TestServiceReconcileSourceOnlyChangeRestartsInPlace pins the HOT_RELOAD
// service reload contract: a source-only change under an unchanged image is
// applied by restarting the SAME container in place (same ID) rather than
// creating a replacement, exactly once per change, and the restarted container
// is recognized as converged on later passes despite its immutable relay.source
// label.
func TestServiceReconcileSourceOnlyChangeRestartsInPlace(t *testing.T) {
	f := newRestartFakeDocker()
	f.setMountIdentity("service.py", "fp1")
	tmpl := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	id := onlyContainerID(t, f.fakeDocker, "fn", "svc")
	if got := f.restartCount(); got != 0 {
		t.Fatalf("initial apply restarts = %d, want 0", got)
	}

	// Source-only edit: same image reference, new live fingerprint.
	f.setMountIdentity("service.py", "fp2")
	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("reload apply: %v", err)
	}
	if got := f.restartCount(); got != 1 {
		t.Fatalf("source-only apply restarts = %d, want 1", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got != id {
		t.Fatalf("source-only reload changed the container id: %s -> %s", id, got)
	}
	if got := f.runningCount("fn", "svc"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	// The relay.source label is immutable: the restarted container still reports
	// its creation fingerprint. Convergence must rely on the applied-source
	// record instead.
	if ctr, ok := f.containerByID(id); !ok || ctr.sourceID != "fp1" {
		t.Fatalf("container source label = %q, want the immutable fp1", ctr.sourceID)
	}

	// The same desired source must not restart again.
	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("idempotent apply: %v", err)
	}
	if got := f.restartCount(); got != 1 {
		t.Fatalf("idempotent apply restarts = %d, want 1 (no repeated restart)", got)
	}

	// A second edit restarts once more, still retaining the container ID.
	f.setMountIdentity("service.py", "fp3")
	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("second reload apply: %v", err)
	}
	if got := f.restartCount(); got != 2 {
		t.Fatalf("second source edit restarts = %d, want 2", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got != id {
		t.Fatalf("second source reload changed the container id: %s -> %s", id, got)
	}
}

// TestServiceReconcileImageChangeStillReplaces pins that a source reload never
// substitutes for a dependency/config replacement: a changed image reference
// (e.g. a dependency rebuild) still replaces the container through the normal
// start-before-stop path, leaving the restarter unused.
func TestServiceReconcileImageChangeStillReplaces(t *testing.T) {
	f := newRestartFakeDocker()
	f.setMountIdentity("service.py", "fp1")
	tmpl := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:v1"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	id := onlyContainerID(t, f.fakeDocker, "fn", "svc")

	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:v2"); err != nil {
		t.Fatalf("image-change apply: %v", err)
	}
	if got := f.restartCount(); got != 0 {
		t.Fatalf("image change restarts = %d, want 0 (a replacement, not a reload)", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got == id {
		t.Fatalf("image change kept the container id %s; want a replacement", id)
	}
	if got := f.runningCount("fn", "svc"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// TestServiceReconcileConfigChangeStillReplaces pins that a non-source
// configuration change (here the port) replaces rather than restarts, even
// though a live source mount is present: only a mismatch confined to the source
// fingerprint qualifies for an in-place reload.
func TestServiceReconcileConfigChangeStillReplaces(t *testing.T) {
	f := newRestartFakeDocker()
	f.setMountIdentity("service.py", "fp1")
	tmpl := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	id := onlyContainerID(t, f.fakeDocker, "fn", "svc")

	changed := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 9000, Replicas: 1})
	if err := applyServices(t, c, "fn", changed, "relay-app-fn:tag"); err != nil {
		t.Fatalf("port-change apply: %v", err)
	}
	if got := f.restartCount(); got != 0 {
		t.Fatalf("port change restarts = %d, want 0 (a replacement)", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got == id {
		t.Fatalf("port change kept the container id %s; want a replacement", id)
	}
}

// TestServiceReconcileRestartFailurePreservesAndRetries pins the failure path: a
// failed in-place restart does not remove the running generation, the pass
// reports the error, and a later pass retries the reload successfully.
func TestServiceReconcileRestartFailurePreservesAndRetries(t *testing.T) {
	f := newRestartFakeDocker()
	f.setMountIdentity("service.py", "fp1")
	tmpl := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)

	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	id := onlyContainerID(t, f.fakeDocker, "fn", "svc")

	f.setMountIdentity("service.py", "fp2")
	f.failRestartFor = id
	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err == nil {
		t.Fatal("reload apply with a failing restart returned nil error; want the failure surfaced")
	}
	if got := f.restartCount(); got != 0 {
		t.Fatalf("failed restart recorded %d restarts, want 0", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got != id {
		t.Fatalf("failed restart replaced the container: %s -> %s; want it preserved", id, got)
	}
	if got := f.runningCount("fn", "svc"); got != 1 {
		t.Fatalf("running after failed restart = %d, want 1 (generation preserved)", got)
	}

	// Recover: the next pass retries and succeeds.
	f.failRestartFor = ""
	if err := applyServices(t, c, "fn", tmpl, "relay-app-fn:tag"); err != nil {
		t.Fatalf("recovery apply: %v", err)
	}
	if got := f.restartCount(); got != 1 {
		t.Fatalf("recovery apply restarts = %d, want 1", got)
	}
	if got := onlyContainerID(t, f.fakeDocker, "fn", "svc"); got != id {
		t.Fatalf("recovery changed the container id: %s -> %s", id, got)
	}
}

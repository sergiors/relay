package reconciler

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// testServiceHealthcheck returns a valid probe at the shortest documented
// interval, so bounded-retry windows derived from it stay small in tests.
func testServiceHealthcheck(interval time.Duration) *app.ServiceHealthcheck {
	return &app.ServiceHealthcheck{
		Path: "/health", Interval: interval, Timeout: time.Second,
		Retries: 1, StartPeriod: time.Second,
	}
}

// setHealth flips a seeded container's Docker health summary.
func (f *fakeDocker) setHealth(id string, health container.HealthStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.ctrs[id]; ok {
		c.health = health
	}
}

// seedConvergedHealthService seeds a RUNNING container that matches the
// desired configuration of serviceTemplate's entrypoint service exactly —
// including the probe identity and Docker health metadata — except for the
// health state under test. desiredImage must be the image the pass resolves.
func seedConvergedHealthService(
	f *fakeDocker, id, fn, name, desiredImage string,
	port, replica int, hc *app.ServiceHealthcheck,
	health container.HealthStatus, attempt int,
) {
	now := time.Now()
	f.ctrs[id] = &fakeContainer{
		id: id, appName: fn, entrypoint: name, image: desiredImage, port: port,
		replica: replica, state: container.StateRunning,
		environmentFingerprint: serviceEnvironmentFingerprint(port),
		resources:              serviceResources(),
		healthcheck:            runtime.ServiceHealthcheckLabel(hc),
		healthcheckAttempt:     attempt,
		health:                 health,
		createdAt:              now,
		startedAt:              now,
	}
}

// markerStoreDocker adds the Docker-owned committed-marker store seam to
// fakeDocker, shared across "restarts" (new ServiceReconciler instances). Every
// committed write also lands in the fake's ordered event log as
// "commit:<id>", so tests can assert marker-write ordering against container
// transitions.
type markerStoreDocker struct {
	*fakeDocker
	mu       sync.Mutex
	states   map[string]runtime.ServiceConvergenceState
	writes   []runtime.ServiceConvergenceState
	failList bool
	// failWrite makes every WriteServiceConvergenceState fail without
	// recording, modeling a daemon that cannot persist a marker.
	failWrite bool
	// failWriteIf, when set, fails exactly the writes it selects (also without
	// recording), so a test can break one marker family while the other stays
	// durable. It is read under mu.
	failWriteIf func(state runtime.ServiceConvergenceState) bool
	// failDeleteIf, when set, fails exactly the kind-scoped deletes it selects,
	// modeling a transient cleanup failure that leaves the preparation admission
	// behind after a successful preparation. It is read under mu.
	failDeleteIf func(appName, serviceName, nodeID, kind string) bool
}

func newMarkerStoreDocker(f *fakeDocker) *markerStoreDocker {
	return &markerStoreDocker{
		fakeDocker: f,
		states:     map[string]runtime.ServiceConvergenceState{},
	}
}

// markerKey mirrors production's serviceStateKey: the kind separates the
// committed/candidate family (kind "") from the preparation-retry family, so
// the two never overwrite each other in the fake's map either.
func markerKey(state runtime.ServiceConvergenceState) string {
	return fmt.Sprintf("%s|%s|%s|%s|%d",
		state.Kind, state.NodeID, state.App, state.Service, state.Replica)
}

func (d *markerStoreDocker) ListServiceConvergenceStates(
	context.Context,
) ([]runtime.ServiceConvergenceState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failList {
		return nil, fmt.Errorf("list failed")
	}
	out := make([]runtime.ServiceConvergenceState, 0, len(d.states))
	for _, state := range d.states {
		out = append(out, state)
	}
	return out, nil
}

func (d *markerStoreDocker) WriteServiceConvergenceState(
	_ context.Context, state runtime.ServiceConvergenceState,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failWrite || (d.failWriteIf != nil && d.failWriteIf(state)) {
		return fmt.Errorf("write failed")
	}
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now()
	}
	d.states[markerKey(state)] = state
	d.writes = append(d.writes, state)
	if state.CommittedContainerID != "" {
		d.fakeDocker.mu.Lock()
		d.fakeDocker.recordEvent("commit:" + state.CommittedContainerID)
		d.fakeDocker.mu.Unlock()
	}
	return nil
}

func (d *markerStoreDocker) DeleteServiceConvergenceStates(
	_ context.Context, appName, serviceName, nodeID string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, state := range d.states {
		if state.App != appName || state.NodeID != nodeID {
			continue
		}
		if serviceName != "" && state.Service != serviceName {
			continue
		}
		delete(d.states, key)
	}
	return nil
}

// DeleteServiceConvergenceStatesForKind mirrors the production Manager's
// kind-scoped delete: only markers of ONE family for the app/service/node are
// removed, so preparation cleanup can never reach a replica's
// committed/candidate marker. It is what makes the fake satisfy the optional
// stateMarkerKindDeleter capability (see fallbackMarkerStore for a store
// without it).
func (d *markerStoreDocker) DeleteServiceConvergenceStatesForKind(
	_ context.Context, appName, serviceName, nodeID, kind string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failDeleteIf != nil && d.failDeleteIf(appName, serviceName, nodeID, kind) {
		return fmt.Errorf("delete failed")
	}
	for key, state := range d.states {
		if state.App != appName || state.NodeID != nodeID || state.Kind != kind {
			continue
		}
		if serviceName != "" && state.Service != serviceName {
			continue
		}
		delete(d.states, key)
	}
	return nil
}

// seedMarker plants a marker as if it were already present on the daemon
// (loaded by the one-time marker load): it bypasses the write history so a
// test's write assertions only see writes made by the run under test.
func (d *markerStoreDocker) seedMarker(state runtime.ServiceConvergenceState) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now()
	}
	d.states[markerKey(state)] = state
}

// markerState returns the stored committed/candidate marker for one replica key.
func (d *markerStoreDocker) markerState(
	appName, serviceName string, replica int,
) (runtime.ServiceConvergenceState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	state, ok := d.states[markerKey(runtime.ServiceConvergenceState{
		App: appName, Service: serviceName, Replica: replica,
	})]
	return state, ok
}

// preparationMarkerState returns the stored preparation-retry marker for one
// service (the preparation family always lives at replica 0).
func (d *markerStoreDocker) preparationMarkerState(
	appName, serviceName string,
) (runtime.ServiceConvergenceState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	state, ok := d.states[markerKey(runtime.ServiceConvergenceState{
		Kind: runtime.ServiceStateKindPreparation,
		App:  appName, Service: serviceName, Replica: 0,
	})]
	return state, ok
}

// markerWrites returns every write in order (failures first, commits last).
func (d *markerStoreDocker) markerWrites() []runtime.ServiceConvergenceState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]runtime.ServiceConvergenceState(nil), d.writes...)
}

// candidateWrites returns the recorded writes in the replica committed/candidate
// marker family (kind ""), in order. The preparation family is a separate
// durable carrier and never appears here.
func (d *markerStoreDocker) candidateWrites() []runtime.ServiceConvergenceState {
	return d.writesOfKind("")
}

// preparationWrites returns the recorded writes in the preparation marker
// family, in order.
func (d *markerStoreDocker) preparationWrites() []runtime.ServiceConvergenceState {
	return d.writesOfKind(runtime.ServiceStateKindPreparation)
}

func (d *markerStoreDocker) writesOfKind(
	kind string,
) []runtime.ServiceConvergenceState {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []runtime.ServiceConvergenceState
	for _, write := range d.writes {
		if write.Kind == kind {
			out = append(out, write)
		}
	}
	return out
}

// assertPreparationAdmittedThenPruned asserts the preparation family's
// semantic contract after a pass whose preparation SUCCEEDED: the attempt was
// admitted durably before any pre-resolution work (every recorded write is an
// inert admission — never a committed container, never retry eligibility), and
// no preparation marker remains afterwards — the kind-scoped cleanup pruned the
// family while the committed/candidate family stayed the only durable carrier
// of commit and retry state.
func assertPreparationAdmittedThenPruned(
	t *testing.T, d *markerStoreDocker, appName, serviceName string,
) {
	t.Helper()
	writes := d.preparationWrites()
	if len(writes) == 0 {
		t.Fatal("no preparation admission was recorded before pre-resolution work")
	}
	for _, write := range writes {
		if write.CommittedContainerID != "" || !write.RetryNotBefore.IsZero() ||
			write.CandidateAttempt < 1 || write.CandidateStartedAt.IsZero() {
			t.Fatalf("preparation write = %+v, want an inert admission record "+
				"(attempt >= 1, start instant, no commit, no retry window)", write)
		}
	}
	if marker, ok := d.preparationMarkerState(appName, serviceName); ok {
		t.Fatalf("preparation marker survived successful preparation = %+v, "+
			"want the family pruned", marker)
	}
}

// setRetryNotBefore rewrites a stored marker's retry eligibility, standing in
// for the passage of time between worker processes.
func (d *markerStoreDocker) setRetryNotBefore(
	appName, serviceName string, replica int, at time.Time,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := markerKey(runtime.ServiceConvergenceState{
		App: appName, Service: serviceName, Replica: replica,
	})
	state := d.states[key]
	state.RetryNotBefore = at
	d.states[key] = state
}

// setPreparationRetryNotBefore rewrites the preparation-family marker's retry
// eligibility, standing in for the passage of time between worker processes.
func (d *markerStoreDocker) setPreparationRetryNotBefore(
	appName, serviceName string, at time.Time,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := markerKey(runtime.ServiceConvergenceState{
		Kind: runtime.ServiceStateKindPreparation,
		App:  appName, Service: serviceName, Replica: 0,
	})
	state := d.states[key]
	state.RetryNotBefore = at
	d.states[key] = state
}

// rewriteMarker mutates one stored marker in place, standing in for durable
// state an earlier process left behind (for example a start instant from long
// ago, or a failure window that was never persisted). The lookup state supplies
// the marker key (Kind, App, Service, Replica); it bypasses the write history.
func (d *markerStoreDocker) rewriteMarker(
	lookup runtime.ServiceConvergenceState,
	mutate func(*runtime.ServiceConvergenceState),
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := markerKey(lookup)
	state := d.states[key]
	mutate(&state)
	d.states[key] = state
}

// fallbackMarkerStore satisfies StateMarkerStore through an inner
// markerStoreDocker while deliberately NOT implementing the optional
// stateMarkerKindDeleter capability — the production Manager has it, but a
// store that does not must take clearPreparationState's fallback: a cleared
// current-generation record carrying no attempt or window (which therefore
// gates nothing), instead of a kind-scoped delete.
type fallbackMarkerStore struct {
	*fakeDocker
	inner *markerStoreDocker
}

func newFallbackMarkerStore(f *fakeDocker) *fallbackMarkerStore {
	return &fallbackMarkerStore{fakeDocker: f, inner: newMarkerStoreDocker(f)}
}

func (s *fallbackMarkerStore) ListServiceConvergenceStates(
	ctx context.Context,
) ([]runtime.ServiceConvergenceState, error) {
	return s.inner.ListServiceConvergenceStates(ctx)
}

func (s *fallbackMarkerStore) WriteServiceConvergenceState(
	ctx context.Context, state runtime.ServiceConvergenceState,
) error {
	return s.inner.WriteServiceConvergenceState(ctx, state)
}

func (s *fallbackMarkerStore) DeleteServiceConvergenceStates(
	ctx context.Context, appName, serviceName, nodeID string,
) error {
	return s.inner.DeleteServiceConvergenceStates(ctx, appName, serviceName, nodeID)
}

// probeStep is one scripted revalidation answer: a definitive observation or a
// transient failure (no authoritative answer).
type probeStep struct {
	obs runtime.ServiceHealthObservation
	err error
}

// probeDocker adds the optional read-through probe seam on top of the marker
// store. Scripted steps are consumed per call; unscripted IDs report the live
// container's state/health (Missing when it no longer exists), which is what
// the production Manager derives from an inspect.
type probeDocker struct {
	*markerStoreDocker
	mu      sync.Mutex
	scripts map[string][]probeStep
	calls   []string
}

func newProbeDocker(f *fakeDocker) *probeDocker {
	return &probeDocker{
		markerStoreDocker: newMarkerStoreDocker(f),
		scripts:           map[string][]probeStep{},
	}
}

func (d *probeDocker) script(id string, steps ...probeStep) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.scripts[id] = append(d.scripts[id], steps...)
}

func (d *probeDocker) probeCalls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

// probeLog returns the per-call inspected id sequence, so a test can assert
// WHICH candidates were revalidated and how often (commit boundary vs final
// readiness check).
func (d *probeDocker) probeLog() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func (d *probeDocker) ObserveServiceHealth(
	_ context.Context, ids []string,
) (map[string]runtime.ServiceHealthObservation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, ids...)
	out := make(map[string]runtime.ServiceHealthObservation, len(ids))
	var firstErr error
	for _, id := range ids {
		if queue := d.scripts[id]; len(queue) > 0 {
			step := queue[0]
			d.scripts[id] = queue[1:]
			if step.err != nil {
				if firstErr == nil {
					firstErr = step.err
				}
				continue
			}
			out[id] = step.obs
			continue
		}
		d.fakeDocker.mu.Lock()
		ctr, ok := d.fakeDocker.ctrs[id]
		d.fakeDocker.mu.Unlock()
		if !ok {
			out[id] = runtime.ServiceHealthObservation{Missing: true}
			continue
		}
		out[id] = runtime.ServiceHealthObservation{State: ctr.state, Health: ctr.health}
	}
	return out, firstErr
}

// countEventPrefix counts ordered events carrying the prefix.
func countEventPrefix(events []string, prefix string) int {
	n := 0
	for _, ev := range events {
		if len(ev) >= len(prefix) && ev[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// healthApply runs one worker pass over the given template.
func healthApply(
	t *testing.T, c *ServiceReconciler, fn string, tmpl *app.Template, image string,
) ServicePassResult {
	t.Helper()
	result := c.ApplyResult(context.Background(), fn, tmpl, image, nil, nil)
	if result.Err != nil {
		t.Fatalf("apply: %v", result.Err)
	}
	return result
}

// A probe-aware replacement keeps the zero-downtime ordering: the replacement
// starts before the old generation stops, and the durable commit marker for the
// new container is written BEFORE that stop — never after.
func TestReconcileHealthcheckReplacementStartsThenCommitsThenStopsOld(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-new")

	if result.Pending() || result.Superseded {
		t.Fatalf("result = %+v, want a terminal successful pass", result)
	}
	if !result.Changed {
		t.Fatal("the replacement pass reported no change")
	}
	order := f.order()
	start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:old-1")
	if start < 0 || stop < 0 || start > stop {
		t.Fatalf("order = %v, want the replacement started before old-1 stopped", order)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil {
		t.Fatal("no replacement was started")
	}
	commit := f.indexOfEvent("commit:" + replacement.id)
	if commit < 0 || commit > stop {
		t.Fatalf("order = %v, want the commit marker written before old-1 stopped", order)
	}
	if replacement.healthcheck != runtime.ServiceHealthcheckLabel(hc) ||
		replacement.healthcheckAttempt != 1 || replacement.health != container.Healthy {
		t.Fatalf("replacement = %+v, want the probe identity, first attempt, healthy", replacement)
	}
	state, ok := md.markerState("fn", "service.js", 0)
	if !ok || state.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker = (%+v, %v), want the replacement", state, ok)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

// A probe-only template edit replaces the running container through the normal
// start-before-stop path: the relay.healthcheck identity is the only desired
// input that changed, and it is enough to make the old container stale.
func TestReconcileProbeOnlyEditReplacesConvergedContainer(t *testing.T) {
	f := newFakeDocker()
	oldHC := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "gen-1", "fn", "service.js", "img-1",
		80, 0, oldHC, container.Healthy, 1)

	changed := testServiceHealthcheck(30 * time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: changed,
	})
	c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Changed || result.Pending() {
		t.Fatalf("result = %+v, want a terminal changed pass", result)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil || replacement.id == "gen-1" {
		t.Fatalf("replacement = %+v, want a new container for the new probe", replacement)
	}
	if replacement.healthcheck != runtime.ServiceHealthcheckLabel(changed) {
		t.Fatalf("probe identity = %q, want the edited probe's identity",
			replacement.healthcheck)
	}
	if start, stop := f.indexOfEvent("start:0"), f.indexOfEvent("stop:gen-1"); start < 0 ||
		stop < 0 || start > stop {
		t.Fatalf("order = %v, want start before stop", f.order())
	}
	if _, ok := f.containerByID("gen-1"); ok {
		t.Fatal("the old-probe generation survived a probe-only edit")
	}
}

// An ACTIVE replica that later turns unhealthy is never replaced for health
// alone: the committed marker keeps it authoritative and the pass ends as a
// terminal no-op — no start, no stop, no new marker.
func TestReconcileActiveUnhealthyReplicaIsNotReplaced(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

	healthApply(t, c, "fn", tmpl, "img-1")
	active := f.lastStartedFor("fn", "service.js")
	if active == nil {
		t.Fatal("the initial pass started nothing")
	}
	if len(md.markerWrites()) == 0 {
		t.Fatal("the initial pass wrote no marker")
	}
	candidateWritesBefore := len(md.candidateWrites())
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")

	f.setHealth(active.id, container.Unhealthy)
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if result.Changed || result.Pending() {
		t.Fatalf("result = %+v, want a terminal unchanged pass (health alone never replaces)", result)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (an unhealthy active replica is untouched)", events)
	}
	if ctr, ok := f.containerByID(active.id); !ok || ctr.health != container.Unhealthy {
		t.Fatalf("active container = (%+v, %v), want it preserved as-is", ctr, ok)
	}
	if got := len(md.candidateWrites()); got != candidateWritesBefore {
		t.Fatalf("candidate-family writes = %d, want %d (health alone never re-commits "+
			"or writes replica-family state)", got, candidateWritesBefore)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")

	// Control: the same unhealthy container WITHOUT a committed marker is an
	// uncommitted candidate — it enters the bounded retry as a pending pass
	// instead, and it is still not replaced early.
	plain := newFakeDocker()
	seedConvergedHealthService(plain, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	plainC := NewServiceReconciler(plain, nil, testutil.DiscardLogger(), testReconcileTimeout)
	pendingResult := healthApply(t, plainC, "fn", tmpl, "img-1")
	if !pendingResult.Pending() {
		t.Fatalf("result = %+v, want pending for an uncommitted unhealthy candidate", pendingResult)
	}
	if pendingResult.RetryNotBefore.Before(time.Now()) ||
		pendingResult.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a fresh bounded retry window", pendingResult.RetryNotBefore)
	}
	if events := plain.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (the retry window has not elapsed)", events)
	}
}

// A starting candidate holds its slot: the old generation keeps serving and
// nothing is duplicated until the candidate reports healthy — at which point
// the candidate is committed and only then is the old generation retired. The
// reported startup deadline is the durable window anchored at the instant
// Docker STARTED the candidate; a delayed create->start (an old create time)
// never pulls the deadline earlier.
func TestReconcileStartingCandidatePreservesOldGenerationUntilHealthy(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Starting, 1)
	seeded, ok := f.containerByID("cand-1")
	if !ok {
		t.Fatal("candidate seed missing")
	}
	// Delayed create->start: the container was created a minute ago but
	// started only now. Create time must not anchor the startup grace.
	f.mu.Lock()
	f.ctrs["cand-1"].createdAt = time.Now().Add(-time.Minute)
	f.mu.Unlock()
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Pending() {
		t.Fatalf("result = %+v, want pending while the candidate is starting", result)
	}
	if !result.RetryNotBefore.IsZero() {
		t.Fatalf("RetryNotBefore = %v, want zero (a starting candidate has no hard retry)", result.RetryNotBefore)
	}
	wantDeadline := seeded.startedAt.Add(
		hc.StartPeriod + time.Duration(hc.Retries+1)*hc.Interval)
	if !result.StartupDeadline.Equal(wantDeadline) {
		t.Fatalf("StartupDeadline = %v, want the durable start-anchored %v "+
			"(create time is a minute old and must not anchor the grace)",
			result.StartupDeadline, wantDeadline)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-1" {
		t.Fatalf("candidates = %+v, want [cand-1]", result.Candidates)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none: no start, no stop while starting", events)
	}
	for _, id := range []string{"old-1", "cand-1"} {
		if ctr, ok := f.containerByID(id); !ok || ctr.state != container.StateRunning {
			t.Fatalf("container %s = (%+v, %v), want still running", id, ctr, ok)
		}
	}

	// When the candidate reports healthy the very next pass commits it and
	// retires the old generation — the candidate itself is never replaced.
	f.setHealth("cand-1", container.Healthy)
	result = healthApply(t, c, "fn", tmpl, "img-1")
	if result.Pending() || !result.Changed {
		t.Fatalf("result = %+v, want a terminal changed pass once healthy", result)
	}
	if events := f.order(); countEventPrefix(events, "start:") != 0 {
		t.Fatalf("events = %v, want no start ever (the candidate was adopted)", events)
	}
	commit, stop := f.indexOfEvent("commit:cand-1"), f.indexOfEvent("stop:old-1")
	if commit < 0 || stop < 0 || commit > stop {
		t.Fatalf("order = %v, want commit:cand-1 before stop:old-1", f.order())
	}
	if _, ok := f.containerByID("cand-1"); !ok {
		t.Fatal("the committed candidate was removed")
	}
	if _, ok := f.containerByID("old-1"); ok {
		t.Fatal("the old generation survived convergence")
	}
	if state, ok := md.markerState("fn", "service.js", 0); !ok ||
		state.CommittedContainerID != "cand-1" {
		t.Fatalf("committed marker = (%+v, %v), want cand-1", state, ok)
	}
}

// An ambiguous probe at the FINAL readiness check fails closed: the commit is
// refused, no candidate/committed marker is written (only the inert
// preparation admission, pruned after preparation succeeded), the fallback's
// stop is withdrawn, and the pass re-observes shortly on the soft window. Once
// the probe answers healthy again, the commit lands before the old generation's
// stop.
func TestReconcileCommitProbeAmbiguityFailsClosed(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	// The commit revalidation sees healthy (forming the commit intent); the
	// final readiness check moments later cannot produce an authoritative
	// answer at all.
	pd.script("cand-1",
		probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Healthy,
		}},
		probeStep{err: fmt.Errorf("daemon blip")},
	)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Pending() {
		t.Fatalf("result = %+v, want pending on an ambiguous final check", result)
	}
	if got := pd.probeCalls(); got != 2 {
		t.Fatalf("probe calls = %d, want 2 (revalidation then the ambiguous final check)", got)
	}
	if !result.RetryNotBefore.IsZero() ||
		result.StartupDeadline.Before(time.Now()) ||
		result.StartupDeadline.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("deadlines = (retry %v, startup %v), want a soft re-observe window",
			result.RetryNotBefore, result.StartupDeadline)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-1" {
		t.Fatalf("candidates = %+v, want [cand-1]", result.Candidates)
	}
	if writes := pd.candidateWrites(); len(writes) != 0 {
		t.Fatalf("candidate-family writes = %+v, want none (an ambiguous probe must not commit)",
			writes)
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (no start and no stop authorized by an ambiguous probe)", events)
	}
	for _, id := range []string{"old-1", "cand-1"} {
		if ctr, ok := f.containerByID(id); !ok || ctr.state != container.StateRunning {
			t.Fatalf("container %s = (%+v, %v), want still running", id, ctr, ok)
		}
	}

	// The probe recovers: the next pass commits before stopping the fallback.
	result = healthApply(t, c, "fn", tmpl, "img-1")
	if result.Pending() || !result.Changed {
		t.Fatalf("result = %+v, want a terminal changed pass after the probe recovered", result)
	}
	commit, stop := f.indexOfEvent("commit:cand-1"), f.indexOfEvent("stop:old-1")
	if commit < 0 || stop < 0 || commit > stop {
		t.Fatalf("order = %v, want commit:cand-1 before stop:old-1", f.order())
	}
	if state, ok := pd.markerState("fn", "service.js", 0); !ok ||
		state.CommittedContainerID != "cand-1" {
		t.Fatalf("committed marker = (%+v, %v), want cand-1", state, ok)
	}
}

// A transient probe failure at the FIRST pre-commit revalidation (before any
// commit intent exists) must fail closed exactly like the final readiness
// check: the healthy uncommitted candidate and its fallback both stay put, no
// replacement is started (no duplicate candidate appears), nothing is written
// from an answer that never existed beyond the pending candidate's own
// attempt-admission record (never a commit, never retry eligibility), and the
// pass schedules a SOFT re-observation window — pending, no hard retry
// eligibility, no corrective work. When the probe recovers, the very next pass
// commits the ORIGINAL candidate, proving nothing was duplicated or lost in
// between.
func TestReconcileFirstProbeFailurePreservesCandidateAndFallback(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	// The ONLY probe answer available in this pass is the transient failure:
	// no authoritative verdict exists for the commit candidate.
	pd.script("cand-1", probeStep{err: fmt.Errorf("daemon blip")})
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(), testReconcileTimeout)

	before := time.Now()
	result := healthApply(t, c, "fn", tmpl, "img-1")

	// The pass ends pending on a SOFT re-observation window: the candidate is
	// registered for the next wake, the wake lands within the candidate's
	// durable startup grace, and no hard retry eligibility is invented for a
	// failure that was never authoritatively observed.
	if !result.Pending() {
		t.Fatalf("result = %+v, want pending on the ambiguous pre-commit probe", result)
	}
	if !result.RetryNotBefore.IsZero() {
		t.Fatalf("RetryNotBefore = %v, want zero: a transient probe failure is a soft re-observation, not a hard retry",
			result.RetryNotBefore)
	}
	softLatest := before.Add(hc.StartPeriod +
		time.Duration(hc.Retries+1)*hc.Interval + 500*time.Millisecond)
	if !result.StartupDeadline.After(before) || result.StartupDeadline.After(softLatest) {
		t.Fatalf("StartupDeadline = %v, want a soft window after %v and within the durable startup grace ending %v",
			result.StartupDeadline, before, softLatest)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-1" {
		t.Fatalf("candidates = %+v, want the re-observed [cand-1]", result.Candidates)
	}
	if result.Changed {
		t.Fatal("the pass reported corrective work; an ambiguous probe must issue none")
	}

	// The failed probe short-circuits the commit path: it ran exactly once, and
	// no replica-family marker was written. The permissible durable writes are
	// the inert preparation admission (pruned once preparation succeeded) and
	// the candidate's OWN attempt admission — neither carries a committed
	// generation or retry eligibility, because the failure was never
	// authoritatively observed.
	if got := pd.probeCalls(); got != 1 {
		t.Fatalf("probe calls = %d, want 1 (the transient failure ends the commit attempt)", got)
	}
	writes := pd.candidateWrites()
	if len(writes) != 1 {
		t.Fatalf("candidate-family writes = %+v, want exactly the attempt-admission record", writes)
	}
	admission := writes[0]
	if admission.CommittedContainerID != "" ||
		!admission.RetryNotBefore.IsZero() ||
		admission.CandidateContainerID != "cand-1" ||
		admission.CandidateAttempt != 1 {
		t.Fatalf("admission record = %+v, want the uncommitted cand-1 attempt-1 "+
			"record with no retry eligibility", admission)
	}
	if marker, ok := pd.markerState("fn", "service.js", 0); !ok ||
		marker.CommittedContainerID != "" || !marker.RetryNotBefore.IsZero() {
		t.Fatalf("convergence marker = (%+v, %v), want an uncommitted record "+
			"despite the ambiguous probe", marker, ok)
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")

	// Neither generation was touched: no duplicate candidate was created and
	// the fallback still serves.
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none: no start (no duplicate candidate) and no stop (fallback preserved)",
			events)
	}
	if got := f.countForApp("fn"); got != 2 {
		t.Fatalf("containers = %d, want exactly the original 2 (cand-1 and old-1)", got)
	}
	for _, id := range []string{"old-1", "cand-1"} {
		if ctr, ok := f.containerByID(id); !ok || ctr.state != container.StateRunning {
			t.Fatalf("container %s = (%+v, %v), want still running", id, ctr, ok)
		}
	}

	// The probe recovers: the next pass converges the ORIGINAL candidate —
	// committed before the fallback's stop — and ends with exactly one
	// container, so no duplicate ever entered the slot.
	result = healthApply(t, c, "fn", tmpl, "img-1")
	if result.Pending() || !result.Changed {
		t.Fatalf("result after probe recovery = %+v, want terminal convergence", result)
	}
	commit, stop := f.indexOfEvent("commit:cand-1"), f.indexOfEvent("stop:old-1")
	if commit < 0 || stop < 0 || commit > stop {
		t.Fatalf("order = %v, want commit:cand-1 before stop:old-1", f.order())
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running after recovery = %d, want exactly 1 (no duplicate survived)", got)
	}
	if state, ok := pd.markerState("fn", "service.js", 0); !ok ||
		state.CommittedContainerID != "cand-1" {
		t.Fatalf("committed marker = (%+v, %v), want cand-1", state, ok)
	}
}

// A candidate that loses readiness between the commit revalidation and the
// final readiness check is refused: no committed marker, the fallback's stop is
// withdrawn, a durable FAILURE marker (candidate + attempt + eligibility) is
// written before the pass returns pending (alongside only the inert,
// already-pruned preparation admission), and nothing is stopped.
func TestReconcileCandidateLosingReadinessAtFinalCheckRecordsFailureOnly(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	// Revalidation sees healthy (forming the commit intent); the final check
	// moments later sees unhealthy (refusing it).
	pd.script("cand-1",
		probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Healthy,
		}},
		probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Unhealthy,
		}},
	)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Pending() {
		t.Fatalf("result = %+v, want pending after the final check refused the candidate", result)
	}
	if got := pd.probeCalls(); got != 2 {
		t.Fatalf("probe calls = %d, want 2 (revalidation then final check)", got)
	}
	if !result.RetryNotBefore.After(time.Now()) ||
		result.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a fresh bounded retry window", result.RetryNotBefore)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-1" {
		t.Fatalf("candidates = %+v, want [cand-1]", result.Candidates)
	}
	writes := pd.candidateWrites()
	if len(writes) != 1 {
		t.Fatalf("candidate-family writes = %+v, want exactly the failure record", writes)
	}
	failure := writes[0]
	if failure.CommittedContainerID != "" ||
		failure.CandidateContainerID != "cand-1" ||
		failure.CandidateAttempt != 1 ||
		!failure.RetryNotBefore.Equal(result.RetryNotBefore) {
		t.Fatalf("failure marker = %+v, want an uncommitted cand-1 attempt-1 record with the reported eligibility",
			failure)
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (the refused candidate's fallback stays)", events)
	}
	for _, id := range []string{"old-1", "cand-1"} {
		if ctr, ok := f.containerByID(id); !ok || ctr.state != container.StateRunning {
			t.Fatalf("container %s = (%+v, %v), want still running", id, ctr, ok)
		}
	}
}

// One pending slot must not stall independent progress: other slots and other
// services still converge, the starting candidate is never duplicated across
// passes, and a removed service's containers are preserved while any desired
// slot is pending — then cleaned up once everything converges.
func TestReconcilePendingSlotDoesNotBlockOtherProgressAndRemovedCleanup(t *testing.T) {
	f := newFakeDocker()
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "web-cand", "fn", "web.js", "img-new",
		80, 0, hc, container.Starting, 1)
	seedOldService(f, "legacy-1", "fn", "legacy.js", "img-old", 80, 0)
	tmpl := serviceTemplate("nodejs24.x",
		app.Service{Name: "web.js", Entrypoint: "web.js", Port: 80, Replicas: 2,
			Healthcheck: hc},
		app.Service{Name: "api.js", Entrypoint: "api.js", Port: 80, Replicas: 1},
	)
	c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-new")
	if !result.Pending() {
		t.Fatalf("result = %+v, want pending for the starting web slot", result)
	}
	if got := countEventPrefix(f.order(), "start:"); got != 2 {
		t.Fatalf("start events = %v, want exactly 2 (web slot 1 and api), got %d",
			f.order(), got)
	}
	if slots := f.replicas("fn", "web.js"); len(slots) != 2 || slots[0] != 0 || slots[1] != 1 {
		t.Fatalf("web slots = %v, want [0 1] (slot 0 held by the candidate)", slots)
	}
	if got := f.runningCount("fn", "api.js"); got != 1 {
		t.Fatalf("api running = %d, want 1 (independent service progressed)", got)
	}
	if _, ok := f.containerByID("legacy-1"); !ok {
		t.Fatal("the removed service's container was stopped while a slot is pending")
	}

	// A second pass re-evaluates and ends pending again without duplicating
	// anything: no new start, no stop, no second candidate.
	result = healthApply(t, c, "fn", tmpl, "img-new")
	if !result.Pending() {
		t.Fatalf("second result = %+v, want still pending", result)
	}
	if got := countEventPrefix(f.order(), "start:"); got != 2 {
		t.Fatalf("start events after the second pass = %d, want 2 (no duplicate starts)", got)
	}
	if _, ok := f.containerByID("legacy-1"); !ok {
		t.Fatal("the removed service's container was stopped during a pending pass")
	}

	// Once the candidate reports healthy the pass converges and the removed
	// service is cleaned up last.
	f.setHealth("web-cand", container.Healthy)
	result = healthApply(t, c, "fn", tmpl, "img-new")
	if result.Pending() {
		t.Fatalf("result = %+v, want terminal convergence", result)
	}
	if _, ok := f.containerByID("legacy-1"); ok {
		t.Fatal("the removed service's container survived convergence")
	}
	if slots := f.replicas("fn", "web.js"); len(slots) != 2 {
		t.Fatalf("web slots = %v, want 2 running replicas", slots)
	}
	if got := countEventPrefix(f.order(), "start:"); got != 2 {
		t.Fatalf("start events after convergence = %d, want 2", got)
	}
}

// A healthcheck pass superseded at its commit boundary abandons before the
// final readiness check: the old generation it was about to replace is never
// stopped, and no marker write commits a generation or installs retry
// eligibility for the abandoned attempt (the pre-start attempt-admission
// record is the only durable write a superseded pass may leave behind).
func TestReconcileSupersededHealthcheckPassPreservesOldGeneration(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

	superseded := false
	f.mu.Lock()
	f.startGate = func(string, runtime.ServiceSpec, int) {
		superseded = true
	}
	f.mu.Unlock()
	ctx := withReconcileAuthority(context.Background(), func() bool {
		return !superseded
	})

	result := c.ApplyResult(ctx, "fn", tmpl, "img-new", nil, nil)
	if result.Err != nil {
		t.Fatalf("apply: %v", result.Err)
	}
	if !result.Superseded {
		t.Fatalf("result = %+v, want Superseded", result)
	}
	if !result.Changed {
		t.Fatal("the pass must still report the corrective work it issued")
	}
	if events := f.order(); countEventPrefix(events, "stop:") != 0 {
		t.Fatalf("events = %v, want no stop: the old generation outlives the superseded pass", events)
	}
	if ctr, ok := f.containerByID("old-1"); !ok || ctr.state != container.StateRunning {
		t.Fatalf("old-1 = (%+v, %v), want still running", ctr, ok)
	}
	for _, write := range md.markerWrites() {
		if write.CommittedContainerID != "" {
			t.Fatalf("marker writes = %+v, want no commit after supersession", md.markerWrites())
		}
		if !write.RetryNotBefore.IsZero() {
			t.Fatalf("marker writes = %+v, want no retry eligibility for an abandoned attempt",
				md.markerWrites())
		}
	}
}

// HOT_RELOAD pins item 5: an old container that still carries a Relay
// healthcheck label is neither replaced nor gated by it. A source-only change
// restarts it IN PLACE (same container ID, no start, no stop), and with an
// unchanged source an unhealthy/unready health-labeled container stays fully
// converged — while the same explicit unready observation on a container
// WITHOUT the label still gates the pass as generic pending.
func TestReconcileHotReloadKeepsHealthLabeledContainerInPlace(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	seedLive := func(f *restartFakeDocker, sourceID string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.ctrs["live-1"] = &fakeContainer{
			id: "live-1", appName: "fn", entrypoint: "service.js", image: "img-1",
			port: 80, replica: 0, state: container.StateRunning,
			environmentFingerprint: serviceEnvironmentFingerprint(80),
			resources:              serviceResources(),
			sourceID:               sourceID,
			healthcheck:            runtime.ServiceHealthcheckLabel(hc),
			health:                 container.Unhealthy,
			ready:                  boolPtr(false),
			createdAt:              time.Now(),
		}
	}

	t.Run("source-only change restarts in place", func(t *testing.T) {
		f := newRestartFakeDocker()
		f.setMountIdentity("service.js", "fp2")
		seedLive(f, "fp1")
		c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if result.Pending() || !result.Changed {
			t.Fatalf("result = %+v, want a terminal changed pass (restart, not pending)", result)
		}
		if got := f.restartCount(); got != 1 {
			t.Fatalf("restarts = %d, want exactly 1 (in-place reload)", got)
		}
		events := f.order()
		if countEventPrefix(events, "start:") != 0 ||
			countEventPrefix(events, "stop:") != 0 {
			t.Fatalf("events = %v, want a restart only: no replacement, no stop", events)
		}
		ctr, ok := f.containerByID("live-1")
		if !ok || ctr.state != container.StateRunning {
			t.Fatalf("live-1 = (%+v, %v), want the same container still running", ctr, ok)
		}
		// Docker labels are immutable: relay.source still names the CREATED
		// fingerprint, and the applied one is tracked in memory — which is what
		// keeps the next pass from restarting the container again.
		if ctr.sourceID != "fp1" {
			t.Fatalf("relay.source = %q, want the immutable created value fp1", ctr.sourceID)
		}
		if applied, ok := c.sourceApply.appliedFor("live-1", "fn"); !ok || applied != "fp2" {
			t.Fatalf("applied source = (%q, %v), want the in-place fp2 recorded", applied, ok)
		}

		// A follow-up pass sees the restarted container as converged: no churn.
		again := healthApply(t, c, "fn", tmpl, "img-1")
		if again.Changed || again.Pending() {
			t.Fatalf("second result = %+v, want unchanged (the applied source is recognized)", again)
		}
		if got := f.restartCount(); got != 1 {
			t.Fatalf("restarts after the second pass = %d, want 1 (no repeated restart)", got)
		}
	})

	t.Run("unchanged source ignores the stale health label entirely", func(t *testing.T) {
		f := newRestartFakeDocker()
		f.setMountIdentity("service.js", "fp1")
		seedLive(f, "fp1")
		c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if result.Pending() || result.Changed {
			t.Fatalf("result = %+v, want an unchanged terminal pass (health never gates HOT_RELOAD)",
				result)
		}
		if events := f.order(); len(events) != 0 {
			t.Fatalf("events = %v, want none: the health-labeled container stays in place", events)
		}
	})

	t.Run("control: without the label an unready container still gates", func(t *testing.T) {
		f := newRestartFakeDocker()
		f.setMountIdentity("service.js", "fp1")
		seedLive(f, "fp1")
		f.mu.Lock()
		f.ctrs["live-1"].healthcheck = ""
		f.mu.Unlock()
		c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if !result.Pending() {
			t.Fatalf("result = %+v, want pending: generic readiness still gates", result)
		}
	})
}

func boolPtr(value bool) *bool { return &value }

// Repeated failures must not accumulate candidates in one slot: once the
// recorded retry window elapses, the pass stops every redundant failed
// generation — keeping only the highest-attempt carrier — BEFORE starting
// exactly one next attempt, never touches the old active fallback before that
// start succeeds, and converges to a single container.
func TestReconcileFailedCandidateCleanupStartsExactlyOneNextAttempt(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)

	// Slot 0 holds three failed candidates — two unhealthy and one still
	// "starting" past its durable deadline — plus the old active fallback.
	seedConvergedHealthService(f, "cand-low", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	seedConvergedHealthService(f, "cand-high", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 3)
	seedConvergedHealthService(f, "cand-expired", "fn", "service.js", "img-1",
		80, 0, hc, container.Starting, 1)
	// cand-expired was CREATED moments ago but STARTED a minute ago (a delayed
	// start is not the anchor — the start instant is): its startup grace
	// elapsed long ago, so it counts as a failed candidate.
	f.mu.Lock()
	f.ctrs["cand-expired"].startedAt = time.Now().Add(-time.Minute)
	f.mu.Unlock()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// First observation: still inside the bounded retry for the highest
	// attempt (3), so nothing is stopped or started and every failed
	// candidate is registered exactly once.
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !first.Pending() {
		t.Fatalf("first result = %+v, want pending inside the retry window", first)
	}
	if !first.RetryNotBefore.After(time.Now()) ||
		first.RetryNotBefore.After(time.Now().Add(5*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a fresh bounded window for attempt 3",
			first.RetryNotBefore)
	}
	seen := map[string]int{}
	for _, candidate := range first.Candidates {
		seen[candidate.ContainerID]++
	}
	if len(first.Candidates) != 3 ||
		seen["cand-expired"] != 1 || seen["cand-high"] != 1 || seen["cand-low"] != 1 {
		t.Fatalf("candidates = %+v, want each failed candidate tracked exactly once (no accumulation)",
			first.Candidates)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none before eligibility", events)
	}
	marker, ok := md.markerState("fn", "service.js", 0)
	if !ok || marker.CommittedContainerID != "" ||
		marker.CandidateContainerID != "cand-high" || marker.CandidateAttempt != 3 {
		t.Fatalf("failure marker = (%+v, %v), want the highest-attempt carrier cand-high at attempt 3",
			marker, ok)
	}
	if got := f.countForApp("fn"); got != 4 {
		t.Fatalf("containers after the first pass = %d, want all 4 untouched", got)
	}

	// The recorded window elapses; a fresh worker (reloaded marker) runs the
	// eligible retry.
	md.setRetryNotBefore("fn", "service.js", 0, time.Now().Add(-time.Second))
	second := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if second.Pending() || !second.Changed || second.Superseded {
		t.Fatalf("second result = %+v, want terminal convergence", second)
	}

	order := f.order()
	start := f.indexOfEvent("start:0")
	if start < 0 {
		t.Fatalf("order = %v, want the next attempt to start", order)
	}
	if got := countEventPrefix(order, "start:"); got != 1 {
		t.Fatalf("start events = %v, want exactly one next attempt", order)
	}
	// Redundant failed generations are removed BEFORE the single start.
	for _, id := range []string{"cand-expired", "cand-low"} {
		if i := f.indexOfEvent("stop:" + id); i < 0 || i > start {
			t.Fatalf("order = %v, want stop:%s removed before the next attempt starts",
				order, id)
		}
	}
	// The highest-attempt carrier and the old active fallback are preserved
	// until the replacement start succeeds.
	for _, id := range []string{"cand-high", "old-1"} {
		if i := f.indexOfEvent("stop:" + id); i < 0 || i < start {
			t.Fatalf("order = %v, want %s preserved until after the single start", order, id)
		}
	}
	if got := countEventPrefix(order, "stop:"); got != 4 {
		t.Fatalf("stop events = %v, want exactly 4 (2 redundant + carrier + fallback)", order)
	}

	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil {
		t.Fatal("no replacement container exists after convergence")
	}
	for _, id := range []string{"cand-expired", "cand-high", "cand-low", "old-1"} {
		if replacement.id == id {
			t.Fatalf("replacement = %q, want a FRESH container, not a reused failed candidate",
				replacement.id)
		}
	}
	if replacement.healthcheckAttempt != 4 {
		t.Fatalf("attempt = %d, want 4 (highest attempt 3 + 1)", replacement.healthcheckAttempt)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want exactly 1 (no candidate accumulation)", got)
	}
	if state, ok := md.markerState("fn", "service.js", 0); !ok ||
		state.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker = (%+v, %v), want %q", state, ok, replacement.id)
	}
}

// multiMatchFixture seeds slot 0 with two healthy config-matching candidates —
// cand-a first by deterministic container-ID order, cand-b the healthy sibling
// — plus optionally the old fallback generation, and returns a
// probe-scripted reconciler over the marker store.
func multiMatchFixture(
	t *testing.T, withFallback bool,
) (*fakeDocker, *probeDocker, *app.Template, *ServiceReconciler) {
	t.Helper()
	f := newFakeDocker()
	pd := newProbeDocker(f)
	if withFallback {
		seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	}
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-a", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	seedConvergedHealthService(f, "cand-b", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(), testReconcileTimeout)
	return f, pd, tmpl, c
}

// assertNoStartService pins "no StartService ran" through both witnesses: the
// ordered event log (StartService records before anything else) and the fake's
// create counter.
func assertNoStartService(t *testing.T, f *fakeDocker, what string) {
	t.Helper()
	if events := f.order(); countEventPrefix(events, "start:") != 0 {
		t.Fatalf("%s: start events = %v, want none", what, events)
	}
	if got := f.startedCount(); got != 0 {
		t.Fatalf("%s: containers created by StartService = %d, want 0", what, got)
	}
}

// When the deterministic first candidate fails its commit-boundary
// revalidation, the reconciler must fall through to the healthy sibling in the
// SAME pass: the sibling is revalidated at both boundaries and committed
// BEFORE any StartService could run, the rejected candidate (and the old
// fallback) are stopped exactly once after that commit, and no retry window
// is invented for a slot that converged.
func TestReconcileMultiMatchRejectedFirstCandidateCommitsHealthySiblingWithoutStart(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withFallback bool
		wantStops    []string
	}{
		{name: "without fallback", withFallback: false, wantStops: []string{"cand-a"}},
		{name: "with old fallback", withFallback: true,
			wantStops: []string{"old-1", "cand-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, pd, tmpl, c := multiMatchFixture(t, tc.withFallback)
			// The deterministic first candidate answers the commit-boundary
			// revalidation as authoritatively unhealthy; the sibling stays healthy.
			pd.script("cand-a", probeStep{obs: runtime.ServiceHealthObservation{
				State: container.StateRunning, Health: container.Unhealthy,
			}})

			result := healthApply(t, c, "fn", tmpl, "img-1")

			if !result.Changed || result.Pending() || result.Superseded {
				t.Fatalf("result = %+v, want a terminal changed pass adopting the "+
					"sibling", result)
			}
			if !result.RetryNotBefore.IsZero() ||
				!result.StartupDeadline.IsZero() ||
				len(result.Candidates) != 0 {
				t.Fatalf("deadlines = (retry %v, startup %v, candidates %v), want "+
					"none: a committed slot schedules no wake",
					result.RetryNotBefore, result.StartupDeadline, result.Candidates)
			}

			// Adoption happens entirely before any StartService.
			assertNoStartService(t, f, "sibling adoption")

			commitAt := f.indexOfEvent("commit:cand-b")
			if commitAt < 0 {
				t.Fatalf("events = %v, want the healthy sibling committed", f.order())
			}
			state, ok := pd.markerState("fn", "service.js", 0)
			if !ok || state.CommittedContainerID != "cand-b" {
				t.Fatalf("committed marker = (%+v, %v), want the healthy sibling cand-b",
					state, ok)
			}

			// Readiness revalidation remains: the rejected first candidate was
			// probed at the commit boundary, the sibling at BOTH boundaries
			// (commit revalidation + final readiness check).
			if log := pd.probeLog(); !slices.Equal(log,
				[]string{"cand-a", "cand-b", "cand-b"}) {
				t.Fatalf("probe log = %v, want [cand-a cand-b cand-b]", log)
			}

			// Exactly-once stop batch, strictly after the commit.
			stops := f.stopRequestLog()
			if !slices.Equal(stops, tc.wantStops) {
				t.Fatalf("stop requests = %v, want exactly %v (each container "+
					"stopped once)", stops, tc.wantStops)
			}
			for _, id := range tc.wantStops {
				if at := f.indexOfEvent("stop:" + id); at < 0 || at < commitAt {
					t.Fatalf("events = %v, want stop:%s strictly after the commit",
						f.order(), id)
				}
			}

			// The slot converged to exactly the committed replica.
			if ctr, ok := f.containerByID("cand-b"); !ok ||
				ctr.state != container.StateRunning {
				t.Fatalf("cand-b = (%+v, %v), want the committed replica still running",
					ctr, ok)
			}
			if got := f.runningCount("fn", "service.js"); got != 1 {
				t.Fatalf("running containers = %d, want 1", got)
			}

			// Marker semantics: exactly the started+commit pair for cand-b, and
			// no retry window anywhere — a rejected sibling converges, it does
			// not enter the bounded retry.
			writes := pd.candidateWrites()
			if len(writes) != 2 ||
				writes[0].CandidateContainerID != "cand-b" ||
				writes[0].CommittedContainerID != "" ||
				writes[1].CommittedContainerID != "cand-b" {
				t.Fatalf("candidate-family writes = %+v, want the started-then"+
					"commit pair for cand-b", writes)
			}
			for _, write := range writes {
				if !write.RetryNotBefore.IsZero() {
					t.Fatalf("candidate-family write = %+v, want no retry window "+
						"for a converged slot", write)
				}
			}
			assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker,
				"fn", "service.js")
		})
	}
}

// An AMBIGUOUS first revalidation must never authorize a commit, retire the old
// generation, or start a duplicate — but it must also not discard the healthy
// sibling: the sibling is adopted safely, and the unproven candidate is stopped
// exactly once after the commit with no failure window recorded for an answer
// that never existed.
func TestReconcileMultiMatchAmbiguousFirstProbeAdoptsHealthySiblingSafely(t *testing.T) {
	f, pd, tmpl, c := multiMatchFixture(t, false)
	pd.script("cand-a", probeStep{err: fmt.Errorf("daemon blip")})

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Changed || result.Pending() {
		t.Fatalf("result = %+v, want a terminal changed pass adopting the sibling",
			result)
	}
	if !result.RetryNotBefore.IsZero() || !result.StartupDeadline.IsZero() {
		t.Fatalf("deadlines = (retry %v, startup %v), want none: an ambiguous "+
			"probe alongside a healthy sibling converges without installing a window",
			result.RetryNotBefore, result.StartupDeadline)
	}
	assertNoStartService(t, f, "ambiguous-first adoption")

	commitAt := f.indexOfEvent("commit:cand-b")
	if commitAt < 0 {
		t.Fatalf("events = %v, want the healthy sibling committed", f.order())
	}
	state, ok := pd.markerState("fn", "service.js", 0)
	if !ok || state.CommittedContainerID != "cand-b" {
		t.Fatalf("committed marker = (%+v, %v), want the healthy sibling cand-b",
			state, ok)
	}
	if log := pd.probeLog(); !slices.Equal(log,
		[]string{"cand-a", "cand-b", "cand-b"}) {
		t.Fatalf("probe log = %v, want [cand-a cand-b cand-b] (ambiguous answer "+
			"consumed, sibling revalidated at both boundaries)", log)
	}
	stops := f.stopRequestLog()
	if !slices.Equal(stops, []string{"cand-a"}) {
		t.Fatalf("stop requests = %v, want exactly [cand-a]", stops)
	}
	if at := f.indexOfEvent("stop:cand-a"); at < 0 || at < commitAt {
		t.Fatalf("events = %v, want stop:cand-a strictly after the commit", f.order())
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want 1 (the committed sibling)", got)
	}

	// The ambiguous rejection recorded no failure eligibility — it was never an
	// authoritative answer — only the sibling's started+commit pair exists.
	writes := pd.candidateWrites()
	if len(writes) != 2 ||
		writes[0].CandidateContainerID != "cand-b" ||
		writes[1].CommittedContainerID != "cand-b" {
		t.Fatalf("candidate-family writes = %+v, want only the sibling's "+
			"started-then-commit pair", writes)
	}
	for _, write := range writes {
		if !write.RetryNotBefore.IsZero() {
			t.Fatalf("candidate-family write = %+v, want no retry window", write)
		}
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")
}

// A committed ACTIVE replica always wins over an uncommitted duplicate in the
// same slot even when the duplicate sorts FIRST by container ID: the duplicate
// is stale, stopped exactly once, while the active replica is never probed,
// never re-committed (the marker stays byte-for-byte), never started, and never
// stopped.
func TestReconcileCommittedActiveWinsOverLowerIDDuplicate(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-z", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(), testReconcileTimeout)

	// Pass 1 commits cand-z as the slot's active replica.
	first := healthApply(t, c, "fn", tmpl, "img-1")
	if first.Pending() {
		t.Fatalf("first result = %+v, want terminal convergence", first)
	}
	committedBefore, ok := pd.markerState("fn", "service.js", 0)
	if !ok || committedBefore.CommittedContainerID != "cand-z" {
		t.Fatalf("precondition: committed marker = (%+v, %v), want cand-z",
			committedBefore, ok)
	}
	if log := pd.probeLog(); !slices.Equal(log,
		[]string{"cand-z", "cand-z"}) {
		t.Fatalf("probe log after pass 1 = %v, want [cand-z cand-z]", log)
	}
	writesBefore := len(pd.candidateWrites())

	// A duplicate with a LOWER id appears in the same slot: deterministic list
	// order would pick it first, committed precedence must not.
	seedConvergedHealthService(f, "cand-a", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)

	second := healthApply(t, c, "fn", tmpl, "img-1")

	if !second.Changed || second.Pending() {
		t.Fatalf("second result = %+v, want a terminal changed pass stopping the "+
			"duplicate", second)
	}
	assertNoStartService(t, f, "active precedence")

	// The active replica was never probed (committed authority skips
	// revalidation), never stopped, and the marker was never rewritten.
	if log := pd.probeLog(); !slices.Equal(log, []string{"cand-z", "cand-z"}) {
		t.Fatalf("probe log = %v, want [cand-z cand-z]: committed precedence must "+
			"neither probe the active replica nor the duplicate", log)
	}
	stops := f.stopRequestLog()
	if !slices.Equal(stops, []string{"cand-a"}) {
		t.Fatalf("stop requests = %v, want exactly [cand-a] (the lower-ID "+
			"duplicate, once)", stops)
	}
	if ctr, ok := f.containerByID("cand-z"); !ok ||
		ctr.state != container.StateRunning {
		t.Fatalf("cand-z = (%+v, %v), want the committed active replica still running",
			ctr, ok)
	}
	committedAfter, ok := pd.markerState("fn", "service.js", 0)
	if !ok || committedAfter != committedBefore {
		t.Fatalf("committed marker = (%+v, %v), want it byte-for-byte unchanged: "+
			"%+v", committedAfter, ok, committedBefore)
	}
	if got := len(pd.candidateWrites()); got != writesBefore {
		t.Fatalf("candidate-family writes = %d, want %d (removing a duplicate "+
			"rewrites no marker state)", got, writesBefore)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want 1 (the active replica)", got)
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")
}

// When NO listed candidate survives revalidation, the pass fails closed: it
// stays pending on the soft observation window, registers exactly the
// unproven candidates, invents no hard retry window from an answer that never
// existed, and stops nothing — including the old fallback.
func TestReconcileMultiMatchAllRevalidationsFailClosedWithoutStartOrStop(t *testing.T) {
	t.Run("every probe ambiguous", func(t *testing.T) {
		f, pd, tmpl, c := multiMatchFixture(t, true)
		pd.script("cand-a", probeStep{err: fmt.Errorf("daemon blip")})
		pd.script("cand-b", probeStep{err: fmt.Errorf("daemon blip")})

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if !result.Pending() || result.Changed {
			t.Fatalf("result = %+v, want pending with no corrective work", result)
		}
		if !result.RetryNotBefore.IsZero() {
			t.Fatalf("RetryNotBefore = %v, want zero: an ambiguous probe is not a "+
				"failure and installs no hard window", result.RetryNotBefore)
		}
		if !result.StartupDeadline.After(time.Now()) ||
			result.StartupDeadline.After(time.Now().Add(5*time.Second)) {
			t.Fatalf("StartupDeadline = %v, want the soft re-observation window",
				result.StartupDeadline)
		}
		if len(result.Candidates) != 2 ||
			result.Candidates[0].ContainerID != "cand-a" ||
			result.Candidates[1].ContainerID != "cand-b" {
			t.Fatalf("candidates = %+v, want both unproven siblings registered",
				result.Candidates)
		}
		assertNoStartService(t, f, "all-ambiguous fail-closed")
		if stops := f.stopRequestLog(); len(stops) != 0 {
			t.Fatalf("stop requests = %v, want none (nothing is retired while "+
				"unproven)", stops)
		}
		for _, id := range []string{"old-1", "cand-a", "cand-b"} {
			if ctr, ok := f.containerByID(id); !ok ||
				ctr.state != container.StateRunning {
				t.Fatalf("container %s = (%+v, %v), want it still running", id, ctr, ok)
			}
		}
		state, ok := pd.markerState("fn", "service.js", 0)
		if !ok || state.CommittedContainerID != "" ||
			!state.RetryNotBefore.IsZero() {
			t.Fatalf("marker = (%+v, %v), want an uncommitted record with no "+
				"window", state, ok)
		}
		writes := pd.candidateWrites()
		if len(writes) != 2 ||
			writes[0].CandidateContainerID != "cand-a" ||
			writes[1].CandidateContainerID != "cand-b" {
			t.Fatalf("candidate-family writes = %+v, want the two pending start "+
				"records", writes)
		}
		for _, write := range writes {
			if write.CommittedContainerID != "" ||
				!write.RetryNotBefore.IsZero() {
				t.Fatalf("candidate-family write = %+v, want no commit and no "+
					"window", write)
			}
		}
		assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker,
			"fn", "service.js")
	})

	t.Run("first rejected unhealthy, second ambiguous", func(t *testing.T) {
		f, pd, tmpl, c := multiMatchFixture(t, true)
		pd.script("cand-a", probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Unhealthy,
		}})
		pd.script("cand-b", probeStep{err: fmt.Errorf("daemon blip")})

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if !result.Pending() || result.Changed {
			t.Fatalf("result = %+v, want pending with no corrective work", result)
		}
		if !result.RetryNotBefore.IsZero() {
			t.Fatalf("RetryNotBefore = %v, want zero: with no viable sibling the "+
				"pass observes softly and records no window this pass",
				result.RetryNotBefore)
		}
		if !result.StartupDeadline.After(time.Now()) {
			t.Fatalf("StartupDeadline = %v, want the soft re-observation window",
				result.StartupDeadline)
		}
		if len(result.Candidates) != 1 ||
			result.Candidates[0].ContainerID != "cand-b" {
			t.Fatalf("candidates = %+v, want exactly the unproven sibling cand-b",
				result.Candidates)
		}
		assertNoStartService(t, f, "mixed-rejection fail-closed")
		if stops := f.stopRequestLog(); len(stops) != 0 {
			t.Fatalf("stop requests = %v, want nothing retired while pending", stops)
		}
		for _, id := range []string{"old-1", "cand-a", "cand-b"} {
			if ctr, ok := f.containerByID(id); !ok ||
				ctr.state != container.StateRunning {
				t.Fatalf("container %s = (%+v, %v), want it still running", id, ctr, ok)
			}
		}
		state, ok := pd.markerState("fn", "service.js", 0)
		if !ok || state.CommittedContainerID != "" ||
			!state.RetryNotBefore.IsZero() {
			t.Fatalf("marker = (%+v, %v), want an uncommitted record with no "+
				"window", state, ok)
		}
		writes := pd.candidateWrites()
		if len(writes) != 1 || writes[0].CandidateContainerID != "cand-b" ||
			writes[0].CommittedContainerID != "" ||
			!writes[0].RetryNotBefore.IsZero() {
			t.Fatalf("candidate-family writes = %+v, want only cand-b's pending "+
				"start record with no commit or window", writes)
		}
		assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker,
			"fn", "service.js")
	})
}

// When EVERY candidate's commit-boundary revalidation is authoritatively
// unhealthy, no sibling survives to adopt: the pass records the bounded retry
// for the slot, registers each rejected candidate for observation, and stops
// nothing — the old fallback keeps serving until the window elapses.
func TestReconcileMultiMatchAllUnhealthyRevalidationsRecordBoundedRetryWithoutStart(t *testing.T) {
	f, pd, tmpl, c := multiMatchFixture(t, true)
	unhealthy := func() probeStep {
		return probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Unhealthy,
		}}
	}
	pd.script("cand-a", unhealthy())
	pd.script("cand-b", unhealthy())

	result := healthApply(t, c, "fn", tmpl, "img-1")

	if !result.Pending() || result.Changed {
		t.Fatalf("result = %+v, want pending with no corrective work inside the "+
			"window", result)
	}
	if !result.RetryNotBefore.After(time.Now()) ||
		result.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the fresh bounded retry window",
			result.RetryNotBefore)
	}
	if !result.StartupDeadline.IsZero() {
		t.Fatalf("StartupDeadline = %v, want zero: a hard bounded window, not a "+
			"soft wake", result.StartupDeadline)
	}
	if len(result.Candidates) != 2 ||
		result.Candidates[0].ContainerID != "cand-a" ||
		result.Candidates[1].ContainerID != "cand-b" {
		t.Fatalf("candidates = %+v, want both rejected candidates registered",
			result.Candidates)
	}
	assertNoStartService(t, f, "all-unhealthy revalidation")
	if stops := f.stopRequestLog(); len(stops) != 0 {
		t.Fatalf("stop requests = %v, want nothing retired before eligibility",
			stops)
	}
	for _, id := range []string{"old-1", "cand-a", "cand-b"} {
		if ctr, ok := f.containerByID(id); !ok ||
			ctr.state != container.StateRunning {
			t.Fatalf("container %s = (%+v, %v), want it still running", id, ctr, ok)
		}
	}
	// Both candidates were revalidated at the commit boundary; no final
	// readiness check ran because no commit intent could form.
	if log := pd.probeLog(); !slices.Equal(log,
		[]string{"cand-a", "cand-b"}) {
		t.Fatalf("probe log = %v, want [cand-a cand-b] (both revalidated once, "+
			"neither adopted)", log)
	}
	// The bounded window is durable and matches the reported wake exactly; the
	// highest-attempt carrier (deterministic first on a tie) names it.
	state, ok := pd.markerState("fn", "service.js", 0)
	if !ok || state.CommittedContainerID != "" ||
		state.CandidateContainerID != "cand-a" ||
		state.CandidateAttempt != 1 ||
		!state.RetryNotBefore.Equal(result.RetryNotBefore) {
		t.Fatalf("failure marker = (%+v, %v), want the uncommitted attempt-1 "+
			"record carrying the reported window", state, ok)
	}
	writes := pd.candidateWrites()
	if len(writes) != 1 {
		t.Fatalf("candidate-family writes = %+v, want exactly the slot failure "+
			"record (no started records, no commit)", writes)
	}
	assertPreparationAdmittedThenPruned(t, pd.markerStoreDocker, "fn", "service.js")
}

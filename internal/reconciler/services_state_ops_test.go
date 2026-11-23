package reconciler

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// hookMarkerStore wraps the shared marker store with per-operation hooks so a
// test can observe — and block on — the exact context each Docker-owned marker
// operation receives. A non-nil hook result short-circuits the operation, which
// is how a hung daemon or a cancelled lifecycle reaches the caller.
type hookMarkerStore struct {
	*markerStoreDocker
	mu         sync.Mutex
	listHook   func(context.Context) error
	writeHook  func(context.Context) error
	listCalls  int
	writeCalls int
}

func (s *hookMarkerStore) ListServiceConvergenceStates(
	ctx context.Context,
) ([]runtime.ServiceConvergenceState, error) {
	s.mu.Lock()
	s.listCalls++
	hook := s.listHook
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	return s.markerStoreDocker.ListServiceConvergenceStates(ctx)
}

func (s *hookMarkerStore) WriteServiceConvergenceState(
	ctx context.Context, state runtime.ServiceConvergenceState,
) error {
	s.mu.Lock()
	s.writeCalls++
	hook := s.writeHook
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return err
		}
	}
	return s.markerStoreDocker.WriteServiceConvergenceState(ctx, state)
}

func (s *hookMarkerStore) listCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls
}

// A convergence-state load is ONE bounded marker operation: it runs on the
// injected reconcile budget rooted in the caller's lifecycle, so a daemon that
// stops answering fails the pass at the budget instead of pinning a worker, and
// cancelling the lifecycle fails it promptly.
func TestServiceMarkerLoadIsBoundedAndCancellationAware(t *testing.T) {
	const budget = 60 * time.Millisecond
	f := newFakeDocker()
	store := &hookMarkerStore{markerStoreDocker: newMarkerStoreDocker(f)}
	var mu sync.Mutex
	var observedDeadline time.Time
	var observedOK bool
	store.listHook = func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		mu.Lock()
		observedDeadline, observedOK = deadline, ok
		mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	c := NewServiceReconciler(store, nil, testutil.DiscardLogger(), budget)
	tmpl := &app.Template{}

	t.Run("a hung daemon fails at the marker budget", func(t *testing.T) {
		start := time.Now()
		result := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
		elapsed := time.Since(start)
		if result.Outcome != ServicePassFailed || result.Err == nil ||
			!strings.Contains(result.Err.Error(), "convergence state") {
			t.Fatalf("result = %+v, want a failed pass naming the convergence state",
				result)
		}
		if elapsed < budget || elapsed > budget+2*time.Second {
			t.Fatalf("elapsed = %v, want the load to fail at the %v marker budget",
				elapsed, budget)
		}
		mu.Lock()
		deadline, ok := observedDeadline, observedOK
		mu.Unlock()
		if !ok || deadline.Before(start) || deadline.After(start.Add(budget+2*time.Second)) {
			t.Fatalf("marker list deadline = (%v, %v), want a deadline derived from the "+
				"%v budget rooted at %v", deadline, ok, budget, start)
		}
		if events := f.order(); len(events) != 0 {
			t.Fatalf("events = %v, want none (the pass never reached container work)",
				events)
		}
	})

	t.Run("lifecycle cancellation fails the load promptly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		result := c.ApplyResult(ctx, "fn", tmpl, "img-1", nil, nil)
		if result.Outcome != ServicePassFailed ||
			!errors.Is(result.Err, context.Canceled) {
			t.Fatalf("result = %+v, want a failed pass wrapping context.Canceled", result)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("elapsed = %v, want a prompt failure on a cancelled lifecycle", elapsed)
		}
	})
}

// An admitted attempt must be durable BEFORE any container work: when the
// replica-family marker write cannot persist, the pass stays pending with an
// independent error and starts NOTHING (the preparation admission itself
// succeeds, so pre-resolution work runs and only the start is refused). Once
// the store recovers, the retry runs with the next durable attempt and
// converges normally.
func TestCandidateAdmissionWriteFailureRefusesStart(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	// Fail exactly the replica-family ADMISSION records (they carry no window),
	// so pass 1 can still record its durable failure window while pass 2's
	// pre-start admission — and only that family of write — is refused.
	md.mu.Lock()
	md.failWriteIf = func(state runtime.ServiceConvergenceState) bool {
		return state.Kind == "" && state.RetryNotBefore.IsZero()
	}
	md.mu.Unlock()
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// The first pass records the durable failure window normally.
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !first.Pending() || !first.RetryNotBefore.After(time.Now()) {
		t.Fatalf("first result = %+v, want pending with a future durable window", first)
	}
	if _, ok := md.markerState("fn", "service.js", 0); !ok {
		t.Fatal("precondition: the failure window must be durable before the retry")
	}
	resolveCalls := f.resolveCount()

	// The window elapses, but the store cannot persist the next attempt's
	// admission: the pass must refuse the start entirely (while its own
	// preparation admission still succeeds, so resolution runs).
	md.setRetryNotBefore("fn", "service.js", 0, time.Now().Add(-time.Second))
	second := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !second.Pending() || second.Err == nil ||
		!strings.Contains(second.Err.Error(), "record candidate state") {
		t.Fatalf("second result = %+v, want pending with the admission write failure",
			second)
	}
	if got := f.resolveCount(); got != resolveCalls+1 {
		t.Fatalf("resolve calls = %d, want %d (the preparation admission succeeded, "+
			"so pre-resolution ran; only the start is refused)",
			got, resolveCalls+1)
	}
	if !second.RetryNotBefore.After(time.Now()) ||
		second.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the process-local hard window that keeps "+
			"notifications from retrying an unadmitted attempt", second.RetryNotBefore)
	}
	if events := f.order(); countEventPrefix(events, "start:") != 0 {
		t.Fatalf("events = %v, want no start on an attempt that could not be "+
			"durably admitted", events)
	}
	if got := f.countForApp("fn"); got != 1 {
		t.Fatalf("containers = %d, want 1 (cand-1 untouched by the refused start)", got)
	}

	// With the store healthy again the retry runs with the next durable attempt
	// and converges: commit before the failed generation's stop.
	md.mu.Lock()
	md.failWriteIf = nil
	md.mu.Unlock()
	third := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if third.Pending() || !third.Changed {
		t.Fatalf("third result = %+v, want terminal convergence once writes succeed",
			third)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil || replacement.id == "cand-1" {
		t.Fatalf("replacement = %+v, want a fresh candidate", replacement)
	}
	if replacement.healthcheckAttempt != 2 {
		t.Fatalf("attempt = %d, want 2 (the durable attempt advanced)", replacement.healthcheckAttempt)
	}
	commit, stop := f.indexOfEvent("commit:"+replacement.id), f.indexOfEvent("stop:cand-1")
	if commit < 0 || stop < 0 || commit > stop {
		t.Fatalf("order = %v, want the commit before cand-1's stop", f.order())
	}
}

// A candidate-failure record that cannot be persisted is never treated as
// durable authority: the pass stays PENDING with an independent error, keeps
// its observer candidate, and holds the bounded retry window PROCESS-LOCALLY —
// so a broken store cannot become a notification-driven retry loop in THIS
// process while a restart re-derives from the (absent) marker — all without
// writing any replica-family marker or starting anything. Once the store
// recovers, the same bounded retry becomes durable and matches the reported
// wake. Only the replica family's writes are broken: the preparation admission
// above it succeeds and is pruned, exactly as in a healthy pass.
func TestCandidateFailureWriteFailureKeepsPendingWithoutDurableEligibility(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.mu.Lock()
	md.failWriteIf = func(state runtime.ServiceConvergenceState) bool {
		return state.Kind == ""
	}
	md.mu.Unlock()
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}
	worker := newWorker()

	result := worker.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !result.Pending() || result.Err == nil ||
		!strings.Contains(result.Err.Error(), "record retry state") {
		t.Fatalf("result = %+v, want pending carrying the independent marker error",
			result)
	}
	if len(result.Candidates) != 1 || result.Candidates[0].ContainerID != "cand-1" {
		t.Fatalf("candidates = %+v, want the retained [cand-1]", result.Candidates)
	}
	if !result.RetryNotBefore.After(time.Now()) ||
		result.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the bounded process-local hard window: "+
			"a broken store must not become a notification-driven retry loop",
			result.RetryNotBefore)
	}
	if writes := md.candidateWrites(); len(writes) != 0 {
		t.Fatalf("candidate-family writes = %+v, want none (an unpersisted window is never "+
			"presented as durable)", writes)
	}
	if _, ok := md.markerState("fn", "service.js", 0); ok {
		t.Fatal("a replica-family marker exists despite the failed write")
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (no retry without durable eligibility)", events)
	}

	// Another pass in the SAME process, still with a broken store, stays
	// pending behind the local window and still starts nothing.
	again := worker.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !again.Pending() || again.Err == nil {
		t.Fatalf("repeat result = %+v, want pending with the independent error", again)
	}
	if !again.RetryNotBefore.After(time.Now()) {
		t.Fatalf("repeat RetryNotBefore = %v, want the process-local window still held",
			again.RetryNotBefore)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events after the repeat pass = %v, want none", events)
	}

	// A restarted worker over a healthy store re-derives the same bounded
	// retry — and only now does it become durable, matching the reported wake.
	md.mu.Lock()
	md.failWriteIf = nil
	md.mu.Unlock()
	second := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !second.Pending() || second.Err != nil {
		t.Fatalf("second result = %+v, want a clean pending pass", second)
	}
	if !second.RetryNotBefore.After(time.Now()) ||
		second.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a fresh bounded durable window",
			second.RetryNotBefore)
	}
	marker, ok := md.markerState("fn", "service.js", 0)
	if !ok || marker.CommittedContainerID != "" ||
		marker.CandidateContainerID != "cand-1" ||
		marker.CandidateAttempt != 1 ||
		!marker.RetryNotBefore.Equal(second.RetryNotBefore) {
		t.Fatalf("failure marker = (%+v, %v), want the durable cand-1 attempt-1 "+
			"eligibility matching the reported wake", marker, ok)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events after the durable record = %v, want none inside the window",
			events)
	}
}

// Concurrent passes share ONE marker load: initialization is serialized
// OUTSIDE the cache lock, so the Docker round trip never blocks cache readers,
// and a second pass waits for the in-flight load instead of duplicating it —
// exactly one list call initializes every pass of every app.
func TestServiceConcurrentPassesShareMarkerInitWithoutBlockingCache(t *testing.T) {
	f := newFakeDocker()
	store := &hookMarkerStore{markerStoreDocker: newMarkerStoreDocker(f)}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.listHook = func(ctx context.Context) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c := NewServiceReconciler(store, nil, testutil.DiscardLogger(), testReconcileTimeout)
	tmpl := &app.Template{}

	firstDone := make(chan ServicePassResult, 1)
	go func() {
		firstDone <- c.ApplyResult(context.Background(), "app-a", tmpl, "img", nil, nil)
	}()
	<-entered

	// The cache stays readable while the load is parked in Docker I/O: stateMu
	// guards only the map, never the marker list round trip.
	cacheDone := make(chan struct{})
	go func() {
		c.committedState("app-a", "service.js", 0)
		c.forgetServiceState("app-a", "")
		close(cacheDone)
	}()
	select {
	case <-cacheDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a cache read blocked behind the in-flight marker load")
	}

	// A second pass arrives for another app: it waits for the SAME
	// initialization instead of issuing a second list round trip.
	secondDone := make(chan ServicePassResult, 1)
	go func() {
		secondDone <- c.ApplyResult(context.Background(), "app-b", tmpl, "img", nil, nil)
	}()
	if lists := store.listCount(); lists != 1 {
		t.Fatalf("marker list calls while the load is parked = %d, want 1 "+
			"(stateInitMu serializes initialization)", lists)
	}
	close(release)
	for name, ch := range map[string]chan ServicePassResult{
		"first":  firstDone,
		"second": secondDone,
	} {
		select {
		case result := <-ch:
			if result.Failed() {
				t.Fatalf("%s pass failed after the load released: %v", name, result.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s pass never completed after the load was released", name)
		}
	}
	if lists := store.listCount(); lists != 1 {
		t.Fatalf("marker list calls = %d, want exactly 1 for the shared one-time init",
			lists)
	}
}

// A failed marker load leaves the reconciler UNINITIALIZED: the pass fails
// closed rather than trusting an empty cache, and the next caller retries the
// load instead of inheriting the failure.
func TestServiceConvergenceStateLoadRetriesAfterFailure(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.failList = true
	store := &hookMarkerStore{markerStoreDocker: md}
	c := NewServiceReconciler(store, nil, testutil.DiscardLogger(), testReconcileTimeout)
	tmpl := &app.Template{}

	first := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if first.Outcome != ServicePassFailed || first.Err == nil ||
		!strings.Contains(first.Err.Error(), "convergence state") {
		t.Fatalf("first result = %+v, want a failed pass naming the convergence state",
			first)
	}

	md.mu.Lock()
	md.failList = false
	md.mu.Unlock()
	second := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if second.Failed() {
		t.Fatalf("second result = %+v, want the load retried after the failure", second)
	}
	if lists := store.listCount(); lists != 2 {
		t.Fatalf("marker list calls = %d, want 2 (a failed load is retried, never "+
			"latched)", lists)
	}
}

// The preparation admission is written AFTER the deterministic routing-override
// validation but BEFORE any Docker call of the pre-resolution phase: when that
// write fails, the pass skips the source resolution entirely, starts nothing, classifies nothing, and keeps only a
// process-local hard window — a restart can never find an admitted attempt that
// was never durably recorded. Once the store recovers, the very next pass
// performs the skipped work and converges.
func TestPreparationAdmissionWriteFailureSkipsPreResolutionAndStart(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Host: "web.example.com", Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil,
			testutil.DiscardLogger(), testReconcileTimeout)
	}

	// Pass 1 (healthy store): the attempt is admitted, then the routed service
	// converges — one resolution ran and no provider-network check is involved.
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if first.Pending() || !first.Changed {
		t.Fatalf("first result = %+v, want terminal convergence", first)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1", got)
	}
	if got := f.networkLookupCount(); got != 0 {
		t.Fatalf("routing-network lookups = %d, want 0 (routing is provider-driven)", got)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
	eventsAfterFirst := f.order()
	candidateWrites := len(md.candidateWrites())

	// Pass 2: the admission write fails — no pre-resolution Docker work, no
	// start, no classification, nothing durable.
	md.mu.Lock()
	md.failWrite = true
	md.mu.Unlock()
	second := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !second.Pending() || second.Err == nil ||
		!strings.Contains(second.Err.Error(), "record preparation attempt") {
		t.Fatalf("second result = %+v, want pending with the admission write failure",
			second)
	}
	if !second.RetryNotBefore.After(time.Now()) ||
		second.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the process-local hard window for the "+
			"unrecorded attempt", second.RetryNotBefore)
	}
	if len(second.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none (the service was skipped before "+
			"classification)", second.Candidates)
	}
	if got := f.networkLookupCount(); got != 0 {
		t.Fatalf("routing-network lookups = %d, want 0 (no pre-resolution Docker "+
			"work after the failed admission)", got)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (no pre-resolution Docker work after "+
			"the failed admission)", got)
	}
	if got := f.order(); !slices.Equal(got, eventsAfterFirst) {
		t.Fatalf("events = %v, want %v (no start after the failed admission)",
			got, eventsAfterFirst)
	}
	if got := len(md.candidateWrites()); got != candidateWrites {
		t.Fatalf("candidate-family writes = %d, want %d (no replica-family write "+
			"either)", got, candidateWrites)
	}
	if _, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatal("a failed admission must never be cached or persisted as durable state")
	}

	// Pass 3 (fresh worker, store healthy): the skipped work runs and the
	// committed active replica verifies as an unchanged terminal pass.
	md.mu.Lock()
	md.failWrite = false
	md.mu.Unlock()
	third := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if third.Pending() || third.Changed {
		t.Fatalf("third result = %+v, want an unchanged terminal pass", third)
	}
	if got := f.resolveCount(); got != 2 {
		t.Fatalf("resolve calls = %d, want 2 (work resumes once the admission persists)", got)
	}
	if got := f.networkLookupCount(); got != 0 {
		t.Fatalf("routing-network lookups = %d, want 0 (routing is provider-driven)", got)
	}
	if got := f.order(); !slices.Equal(got, eventsAfterFirst) {
		t.Fatalf("events = %v, want %v (a converged replica is only verified)",
			got, eventsAfterFirst)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
}

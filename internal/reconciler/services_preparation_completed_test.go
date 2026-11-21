package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// preparationCompletedMarkerState returns the stored completion marker for one
// service (the completion family lives at replica 0, like the retry family).
func (d *markerStoreDocker) preparationCompletedMarkerState(
	appName, serviceName string,
) (runtime.ServiceConvergenceState, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	state, ok := d.states[markerKey(runtime.ServiceConvergenceState{
		Kind: runtime.ServiceStateKindPreparationCompleted,
		App:  appName, Service: serviceName, Replica: 0,
	})]
	return state, ok
}

// completedWrites returns the recorded writes in the completion marker family,
// in order.
func (d *markerStoreDocker) completedWrites() []runtime.ServiceConvergenceState {
	return d.writesOfKind(runtime.ServiceStateKindPreparationCompleted)
}

// completionMatchesAdmission reports whether a completion record identifies
// exactly the admitted attempt it is checked against: same generation, attempt,
// and start instant, carrying no container and no retry window of its own.
func completionMatchesAdmission(
	completion, admission runtime.ServiceConvergenceState,
) bool {
	return completion.CandidateGeneration == admission.CandidateGeneration &&
		completion.CandidateAttempt == admission.CandidateAttempt &&
		completion.CandidateStartedAt.Equal(admission.CandidateStartedAt) &&
		completion.CandidateContainerID == "" &&
		completion.RetryNotBefore.IsZero()
}

// newPreparationWorker builds a worker over the given store, standing in for a
// fresh process when called again over the same store.
func newPreparationWorker(md *markerStoreDocker) *ServiceReconciler {
	return NewServiceReconciler(md, nil, routing.TraefikConfig{},
		testutil.DiscardLogger(), testReconcileTimeout)
}

// seedUnfinishedPreparationAdmission leaves the durable state a process that
// stopped before recording an outcome leaves behind: pass 1 admits the attempt,
// preparation fails, and the FAILURE-window write fails too, so only the
// windowless admission survives — with no completion record. The write failure
// is lifted before returning, so a restarted worker over the same store
// observes only durable state.
func seedUnfinishedPreparationAdmission(t *testing.T) (
	*fakeDocker, *markerStoreDocker, *app.ServiceHealthcheck, *app.Template,
	runtime.ServiceConvergenceState,
) {
	t.Helper()
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.mu.Lock()
	md.failWriteIf = func(state runtime.ServiceConvergenceState) bool {
		return state.Kind == runtime.ServiceStateKindPreparation &&
			!state.RetryNotBefore.IsZero()
	}
	md.mu.Unlock()
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)
	f.setResolveError("service.js", errors.New("registry unavailable"))
	first := newPreparationWorker(md).
		ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !first.Pending() || first.Err == nil {
		t.Fatalf("setup result = %+v, want pending carrying the preparation "+
			"failure", first)
	}
	admission, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || admission.CandidateAttempt != 1 ||
		!admission.RetryNotBefore.IsZero() ||
		admission.CandidateContainerID != "" ||
		admission.CandidateStartedAt.IsZero() {
		t.Fatalf("setup admission = (%+v, %v), want a windowless attempt-1 "+
			"admission", admission, ok)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); ok {
		t.Fatal("setup: a failed preparation must not record a completion")
	}
	md.mu.Lock()
	md.failWriteIf = nil
	md.mu.Unlock()
	return f, md, hc, tmpl, admission
}

// assertConservativeRecoveryBackoff pins the conservative contract for an
// admission whose outcome is unknown: a restarted pass goes pending behind a
// FULL recovery-anchored window for the admitted attempt, persists exactly that
// window, and performs no preparation work while the window stands.
func assertConservativeRecoveryBackoff(
	t *testing.T,
	f *fakeDocker, md *markerStoreDocker,
	hc *app.ServiceHealthcheck, tmpl *app.Template, attempt int,
) {
	t.Helper()
	result := newPreparationWorker(md).
		ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !result.Pending() || result.Err != nil {
		t.Fatalf("result = %+v, want a clean pending pass held by the recovery "+
			"backoff", result)
	}
	fullBackoff := servicePreparationBackoff(hc.Interval, attempt)
	if !result.RetryNotBefore.After(time.Now()) ||
		result.RetryNotBefore.After(time.Now().Add(fullBackoff+time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a FULL %s (attempt-%d) backoff from "+
			"RECOVERY time (now, now+%s]", result.RetryNotBefore, fullBackoff,
			attempt, fullBackoff)
	}
	durable, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || durable.CandidateAttempt != attempt ||
		!durable.RetryNotBefore.Equal(result.RetryNotBefore) {
		t.Fatalf("durable record = (%+v, %v), want attempt %d carrying the "+
			"reported window %v exactly", durable, ok, attempt,
			result.RetryNotBefore)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (no preparation runs before the "+
			"window)", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none before the window", events)
	}
}

// A successful preparation records the admitted attempt's OUTCOME before the
// retry family is pruned: the completion write identifies the admission by
// generation, attempt, and start instant, is durable after the pass, and never
// carries a container or a retry window — while the windowless admission itself
// is gone. (That the completion lands BEFORE the prune, not merely earlier in
// the write history, is observable in the transient-cleanup test below: when
// the delete fails, the completion is already durable.)
func TestSuccessfulPreparationRecordsMatchingCompletionBeforePruningRetryState(
	t *testing.T,
) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)

	result := healthApply(t, newPreparationWorker(md), "fn", tmpl, "img-1")
	if result.Pending() || !result.Changed {
		t.Fatalf("result = %+v, want terminal convergence", result)
	}

	prepWrites := md.preparationWrites()
	if len(prepWrites) != 1 {
		t.Fatalf("preparation writes = %+v, want exactly the one pre-work "+
			"admission", prepWrites)
	}
	compWrites := md.completedWrites()
	if len(compWrites) != 1 {
		t.Fatalf("completion writes = %+v, want exactly the one outcome record",
			compWrites)
	}
	admission, completion := prepWrites[0], compWrites[0]
	if !completionMatchesAdmission(completion, admission) {
		t.Fatalf("completion = %+v, admission = %+v, want the completion to "+
			"identify the admission by generation, attempt, and start instant",
			completion, admission)
	}
	admissionIdx, completionIdx := -1, -1
	for i, write := range md.markerWrites() {
		if write.Kind == runtime.ServiceStateKindPreparation {
			admissionIdx = i
		}
		if write.Kind == runtime.ServiceStateKindPreparationCompleted {
			completionIdx = i
		}
	}
	if admissionIdx < 0 || completionIdx < 0 || admissionIdx > completionIdx {
		t.Fatalf("write order = %+v, want the admission written before its "+
			"completion", md.markerWrites())
	}
	if leftover, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatalf("preparation admission after success = %+v, want the retry "+
			"family pruned", leftover)
	}
	if durable, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok ||
		!completionMatchesAdmission(durable, admission) {
		t.Fatalf("durable completion = (%+v, %v), want the admission's outcome "+
			"to survive the prune", durable, ok)
	}
	if committed, ok := md.markerState("fn", "service.js", 0); !ok ||
		committed.CommittedContainerID == "" {
		t.Fatalf("committed marker = (%+v, %v), want the started replica "+
			"committed", committed, ok)
	}
}

// A leftover same-generation admission whose exact attempt is covered by a
// durable completion — a process that stopped before the retry-family cleanup —
// must not delay an already-converged, healthy service: the restarted pass is
// terminal, installs no RetryNotBefore, starts and stops nothing, and prunes
// the leftover admission while the completion stays durable.
func TestLeftoverCompletedAdmissionDoesNotDelayConvergedPassAfterRestart(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)

	first := healthApply(t, newPreparationWorker(md), "fn", tmpl, "img-1")
	if first.Pending() || !first.Changed {
		t.Fatalf("first result = %+v, want terminal convergence", first)
	}
	completion, ok := md.preparationCompletedMarkerState("fn", "service.js")
	if !ok {
		t.Fatal("the successful pass must record a completion")
	}
	// Crash before cleanup: the admission the successful pass wrote is still on
	// the daemon, identified exactly by that completion.
	md.seedMarker(runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparation,
		App:                 completion.App,
		Service:             completion.Service,
		NodeID:              completion.NodeID,
		Replica:             completion.Replica,
		CandidateGeneration: completion.CandidateGeneration,
		CandidateAttempt:    completion.CandidateAttempt,
		CandidateStartedAt:  completion.CandidateStartedAt,
	})
	admission, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || !admission.RetryNotBefore.IsZero() {
		t.Fatalf("precondition: leftover admission = (%+v, %v), want a windowless "+
			"record", admission, ok)
	}
	startsBefore := countEventPrefix(f.order(), "start:")
	stopsBefore := countEventPrefix(f.order(), "stop:")

	second := healthApply(t, newPreparationWorker(md), "fn", tmpl, "img-1")

	if second.Pending() {
		t.Fatalf("second result = %+v, want a terminal pass: a completed "+
			"leftover admission must not gate a converged service", second)
	}
	if !second.RetryNotBefore.IsZero() {
		t.Fatalf("RetryNotBefore = %v, want none (no recovery backoff is "+
			"installed)", second.RetryNotBefore)
	}
	if second.Changed {
		t.Fatalf("second result = %+v, want an unchanged verification pass", second)
	}
	if got := countEventPrefix(f.order(), "start:"); got != startsBefore {
		t.Fatalf("start events = %d, want %d (no container churn)", got, startsBefore)
	}
	if got := countEventPrefix(f.order(), "stop:"); got != stopsBefore {
		t.Fatalf("stop events = %d, want %d (no container churn)", got, stopsBefore)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want the converged replica untouched", got)
	}
	if leftover, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatalf("leftover admission after the pass = %+v, want it dropped", leftover)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok {
		t.Fatal("the completion must remain durable across the cleanup")
	}
}

// A transient cleanup failure after a successful preparation leaves the
// admission behind — but the completion is already durable (it is written
// before the delete), so the next pass of the SAME process is terminal with no
// RetryNotBefore, and the repaired cleanup prunes the admission without ever
// converting it into a recovery backoff.
func TestTransientCleanupFailureLeavesAdmissionCompletionPreventsNextPassDelay(
	t *testing.T,
) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.mu.Lock()
	md.failDeleteIf = func(_, _, _, kind string) bool {
		return kind == runtime.ServiceStateKindPreparation
	}
	md.mu.Unlock()
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)
	worker := newPreparationWorker(md)

	first := healthApply(t, worker, "fn", tmpl, "img-1")
	if first.Pending() || !first.Changed {
		t.Fatalf("first result = %+v, want terminal convergence despite the "+
			"failed cleanup", first)
	}
	admission, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || admission.CandidateAttempt < 1 ||
		!admission.RetryNotBefore.IsZero() {
		t.Fatalf("precondition: admission after the failed cleanup = (%+v, %v), "+
			"want the windowless admission left behind", admission, ok)
	}
	completion, ok := md.preparationCompletedMarkerState("fn", "service.js")
	if !ok || !completionMatchesAdmission(completion, admission) {
		t.Fatalf("completion = (%+v, %v), want it durable and matching the "+
			"leftover admission before the delete ran", completion, ok)
	}
	md.mu.Lock()
	md.failDeleteIf = nil
	md.mu.Unlock()
	startsBefore := countEventPrefix(f.order(), "start:")
	stopsBefore := countEventPrefix(f.order(), "stop:")

	second := healthApply(t, worker, "fn", tmpl, "img-1")

	if second.Pending() {
		t.Fatalf("second result = %+v, want a terminal pass: the completion must "+
			"prevent any recovery-backoff delay", second)
	}
	if !second.RetryNotBefore.IsZero() {
		t.Fatalf("RetryNotBefore = %v, want none", second.RetryNotBefore)
	}
	if second.Changed {
		t.Fatalf("second result = %+v, want an unchanged verification pass", second)
	}
	if got := countEventPrefix(f.order(), "start:"); got != startsBefore {
		t.Fatalf("start events = %d, want %d (no container churn)", got, startsBefore)
	}
	if got := countEventPrefix(f.order(), "stop:"); got != stopsBefore {
		t.Fatalf("stop events = %d, want %d (no container churn)", got, stopsBefore)
	}
	if leftover, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatalf("admission after the repaired cleanup = %+v, want the family "+
			"pruned", leftover)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok {
		t.Fatal("the completion must survive the retry-family prune")
	}
}

// An admitted attempt whose outcome is unknown — no completion record, or one
// identifying a DIFFERENT attempt or start instant — keeps the conservative
// contract: a restarted pass converts it into the same attempt's full
// recovery-anchored backoff and does no preparation work before the window.
// An exact completion match is required; anything less stays delayed.
func TestUnfinishedPreparationAdmissionKeepsFullRecoveryBackoffWithoutMatchingCompletion(
	t *testing.T,
) {
	mismatchedCompletion := func(
		t *testing.T, mutate func(*runtime.ServiceConvergenceState),
	) {
		t.Helper()
		f, md, hc, tmpl, admission := seedUnfinishedPreparationAdmission(t)
		completion := runtime.ServiceConvergenceState{
			Kind:                runtime.ServiceStateKindPreparationCompleted,
			App:                 admission.App,
			Service:             admission.Service,
			NodeID:              admission.NodeID,
			Replica:             admission.Replica,
			CandidateGeneration: admission.CandidateGeneration,
			CandidateAttempt:    admission.CandidateAttempt,
			CandidateStartedAt:  admission.CandidateStartedAt,
		}
		mutate(&completion)
		md.seedMarker(completion)
		if stored, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok ||
			completionMatchesAdmission(stored, admission) {
			t.Fatalf("precondition: seeded completion = (%+v, %v), want a "+
				"record that does NOT match the admission", stored, ok)
		}
		assertConservativeRecoveryBackoff(t, f, md, hc, tmpl, 1)
	}

	t.Run("no completion record", func(t *testing.T) {
		f, md, hc, tmpl, _ := seedUnfinishedPreparationAdmission(t)
		assertConservativeRecoveryBackoff(t, f, md, hc, tmpl, 1)
	})

	t.Run("completion with a different start instant", func(t *testing.T) {
		mismatchedCompletion(t, func(c *runtime.ServiceConvergenceState) {
			c.CandidateStartedAt = c.CandidateStartedAt.Add(time.Minute)
		})
	})

	t.Run("completion with a different attempt", func(t *testing.T) {
		mismatchedCompletion(t, func(c *runtime.ServiceConvergenceState) {
			c.CandidateAttempt++
		})
	})
}

// A completion record of an EARLIER successful preparation never suppresses a
// later recorded failure: the durable failure window is read verbatim and
// gates the restarted pass, regardless of the completion family's presence.
func TestEarlierCompletionDoesNotSuppressLaterRecordedPreparationFailure(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(30 * time.Second)
	tmpl := preparationTestTemplate(hc)
	f.setResolveError("service.js", errors.New("registry unavailable"))

	first := newPreparationWorker(md).
		ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !first.Pending() || first.Err == nil {
		t.Fatalf("first result = %+v, want pending carrying the preparation "+
			"failure", first)
	}
	recorded, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || recorded.RetryNotBefore.IsZero() {
		t.Fatalf("setup record = (%+v, %v), want a durable failure window", recorded, ok)
	}
	// An earlier admission's completion, still durable from a previous success.
	md.seedMarker(runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparationCompleted,
		App:                 recorded.App,
		Service:             recorded.Service,
		NodeID:              recorded.NodeID,
		Replica:             recorded.Replica,
		CandidateGeneration: recorded.CandidateGeneration,
		CandidateAttempt:    recorded.CandidateAttempt,
		CandidateStartedAt:  recorded.CandidateStartedAt.Add(-time.Hour),
	})
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok {
		t.Fatal("precondition: the earlier completion must be durable")
	}

	second := newPreparationWorker(md).
		ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)

	if !second.Pending() || second.Err != nil {
		t.Fatalf("second result = %+v, want a clean pending pass held by the "+
			"recorded failure window", second)
	}
	if !second.RetryNotBefore.Equal(recorded.RetryNotBefore) {
		t.Fatalf("RetryNotBefore = %v, want the recorded window %v verbatim: an "+
			"existing completion must never bypass a later failure",
			second.RetryNotBefore, recorded.RetryNotBefore)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (no preparation before the window)", got)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); !ok {
		t.Fatal("the earlier completion must remain untouched")
	}
}

// When BOTH durable writes of a successful preparation fail — the completion
// record and the retry-family cleanup — the current pass still converges
// (this process knows the outcome), but nothing durable says so: a restart
// observes only the leftover admission and must stay conservative, converting
// it into the same attempt's full recovery-anchored backoff before any work.
func TestDoublePreparationWriteFailureStaysConservativeAcrossRestart(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.mu.Lock()
	md.failWriteIf = func(state runtime.ServiceConvergenceState) bool {
		return state.Kind == runtime.ServiceStateKindPreparationCompleted
	}
	md.failDeleteIf = func(_, _, _, kind string) bool {
		return kind == runtime.ServiceStateKindPreparation
	}
	md.mu.Unlock()
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)

	first := healthApply(t, newPreparationWorker(md), "fn", tmpl, "img-1")
	if first.Pending() || !first.Changed {
		t.Fatalf("first result = %+v, want terminal convergence: neither write "+
			"failure may block the process that knows the outcome", first)
	}
	admission, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || !admission.RetryNotBefore.IsZero() ||
		admission.CandidateAttempt != 1 {
		t.Fatalf("precondition: admission = (%+v, %v), want the windowless "+
			"attempt-1 record left by the double failure", admission, ok)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); ok {
		t.Fatal("precondition: the completion write must have failed")
	}
	startsBefore := countEventPrefix(f.order(), "start:")

	md.mu.Lock()
	md.failWriteIf = nil
	md.failDeleteIf = nil
	md.mu.Unlock()

	// Restart: nothing durable records the success, so the conservative
	// recovery backoff applies even though the service is already serving.
	result := newPreparationWorker(md).
		ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !result.Pending() || result.Err != nil {
		t.Fatalf("result = %+v, want a clean pending pass: without a durable "+
			"completion a restart must stay conservative", result)
	}
	fullBackoff := servicePreparationBackoff(hc.Interval, 1)
	if !result.RetryNotBefore.After(time.Now()) ||
		result.RetryNotBefore.After(time.Now().Add(fullBackoff+time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a FULL %s backoff from RECOVERY time "+
			"(now, now+%s]", result.RetryNotBefore, fullBackoff, fullBackoff)
	}
	durable, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || durable.CandidateAttempt != 1 ||
		!durable.RetryNotBefore.Equal(result.RetryNotBefore) {
		t.Fatalf("durable record = (%+v, %v), want attempt 1 carrying the "+
			"reported window %v exactly", durable, ok, result.RetryNotBefore)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (the gated restart runs no "+
			"preparation)", got)
	}
	if got := countEventPrefix(f.order(), "start:"); got != startsBefore {
		t.Fatalf("start events = %d, want %d (the gated restart touches no "+
			"container)", got, startsBefore)
	}
	if _, ok := md.preparationCompletedMarkerState("fn", "service.js"); ok {
		t.Fatal("the completion must still be absent: nothing durable recorded " +
			"the success")
	}
}

// A service without an effective probe keeps the historical behavior: its pass
// writes neither a preparation admission nor a completion record, so no
// recovery-backoff state can ever exist for it.
func TestProbelessServiceRecordsNoPreparationAdmissionOrCompletion(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})

	result := healthApply(t, newPreparationWorker(md), "fn", tmpl, "img-1")
	if result.Pending() || !result.Changed {
		t.Fatalf("result = %+v, want terminal convergence", result)
	}
	if writes := md.preparationWrites(); len(writes) != 0 {
		t.Fatalf("preparation writes = %+v, want none (no probe, no admission)",
			writes)
	}
	if writes := md.completedWrites(); len(writes) != 0 {
		t.Fatalf("completion writes = %+v, want none (no admission, no outcome)",
			writes)
	}
	if marker, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatalf("preparation marker = %+v, want none", marker)
	}
	if marker, ok := md.preparationCompletedMarkerState("fn", "service.js"); ok {
		t.Fatalf("completion marker = %+v, want none", marker)
	}
}

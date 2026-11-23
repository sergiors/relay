package reconciler

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// A failed candidate's retry eligibility is Docker-owned durable state: a new
// worker process (a fresh reconciler over the same store) reconstructs the
// EXACT recorded instant instead of re-anchoring the backoff, the elapsed
// window then releases the retry with the next durable attempt, and the new
// candidate's start resets the recorded backoff before the commit lands.
func TestServiceReconcilerDurableRetrySurvivesRestartAndResetsBackoff(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	// A 30s interval keeps the first recorded eligibility comfortably in the
	// future for the whole test, proving later passes read it back rather than
	// deriving a fresh one.
	hc := testServiceHealthcheck(30 * time.Second)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// Pass 1 observes the failure and records eligibility before returning
	// pending; the reported deadline IS the recorded one. The only writes are
	// the inert preparation admission (pruned after preparation succeeded) and
	// the replica-family failure record.
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !first.Pending() {
		t.Fatalf("result = %+v, want pending inside the retry window", first)
	}
	if !first.RetryNotBefore.After(time.Now()) ||
		first.RetryNotBefore.After(time.Now().Add(61*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a bounded future window", first.RetryNotBefore)
	}
	writes := md.candidateWrites()
	if len(writes) != 1 {
		t.Fatalf("candidate-family writes = %+v, want exactly the failure record", writes)
	}
	recorded := writes[0]
	if recorded.CommittedContainerID != "" ||
		recorded.CandidateContainerID != "cand-1" ||
		recorded.CandidateAttempt != 1 ||
		recorded.CandidateGeneration == "" ||
		!recorded.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("failure record = %+v, want the uncommitted cand-1 attempt-1 eligibility",
			recorded)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none inside the retry window", events)
	}

	// A restarted worker (fresh reconciler, same Docker-owned store) reads the
	// same instant back: not re-anchored at "now", still future, no new write.
	second := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !second.Pending() {
		t.Fatalf("result after restart = %+v, want still pending", second)
	}
	if !second.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("RetryNotBefore after restart = %v, want the recorded %v "+
			"(the durable eligibility must not be re-anchored)",
			second.RetryNotBefore, first.RetryNotBefore)
	}
	cw := md.candidateWrites()
	if len(cw) != 1 || !cw[0].RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("candidate-family writes after restart = %+v, want the single "+
			"recorded failure window untouched (no rewrite while ineligible)", cw)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events after restart = %v, want none", events)
	}

	// Once the recorded window elapses the retry runs with the NEXT durable
	// attempt, and the new candidate's start resets the recorded backoff
	// before the commit marker replaces the record.
	md.setRetryNotBefore("fn", "service.js", 0, time.Now().Add(-time.Second))
	third := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if third.Pending() || !third.Changed {
		t.Fatalf("result after eligibility = %+v, want terminal convergence", third)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil || replacement.id == "cand-1" {
		t.Fatalf("replacement = %+v, want a fresh candidate", replacement)
	}
	if replacement.healthcheckAttempt != 2 {
		t.Fatalf("attempt = %d, want 2 (the durable attempt advanced, never reset)", replacement.healthcheckAttempt)
	}
	commit, stop := f.indexOfEvent("commit:"+replacement.id), f.indexOfEvent("stop:cand-1")
	if commit < 0 || stop < 0 || commit > stop {
		t.Fatalf("order = %v, want the commit before the failed candidate's stop", f.order())
	}

	writes = md.candidateWrites()
	if len(writes) != 4 {
		t.Fatalf("candidate-family writes = %d, want 4 "+
			"(failure, admitted attempt, started, commit)", len(writes))
	}
	admitted := writes[1]
	if admitted.CandidateContainerID != "" || admitted.CommittedContainerID != "" ||
		admitted.CandidateAttempt != 2 || !admitted.RetryNotBefore.IsZero() ||
		admitted.CandidateGeneration != writes[0].CandidateGeneration {
		t.Fatalf("admitted record = %+v, want the pre-start attempt-2 admission "+
			"(no container yet) with a RESET backoff", admitted)
	}
	started := writes[2]
	if started.CandidateContainerID != replacement.id ||
		started.CandidateAttempt != 2 || !started.RetryNotBefore.IsZero() {
		t.Fatalf("started record = %+v, want the new candidate with attempt 2 and a RESET backoff",
			started)
	}
	committed := writes[3]
	if committed.CommittedContainerID != replacement.id ||
		committed.CandidateAttempt != 0 || !committed.RetryNotBefore.IsZero() {
		t.Fatalf("commit record = %+v, want only the committed generation", committed)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")

	// Removing the app clears its markers so a later deployment starts clean.
	if _, ok := md.markerState("fn", "service.js", 0); !ok {
		t.Fatal("precondition: the marker must exist before removal")
	}
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	c.Remove(context.Background(), "fn")
	if _, ok := md.markerState("fn", "service.js", 0); ok {
		t.Fatal("marker survived app removal; it must be deleted with the app")
	}
}

// Startup reconstruction excludes the committed active replica (it may be
// unhealthy but is never a replace-on-health candidate), healthy containers,
// and containers without Relay's probe label; with no store every non-healthy
// probe-labeled container is returned as the historical best effort.
func TestRecoverServiceHealthCandidatesExcludesCommittedActiveAndHealthy(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	probe := runtime.ServiceHealthcheckLabel(hc)
	seed := func(f *fakeDocker) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.ctrs["active-1"] = &fakeContainer{
			id: "active-1", appName: "fn", entrypoint: "service.js",
			image: "img-1", port: 80, replica: 0, state: container.StateRunning,
			environmentFingerprint: serviceEnvironmentFingerprint(80),
			resources:              serviceResources(),
			healthcheck:            probe, health: container.Unhealthy,
		}
		f.ctrs["cand-1"] = &fakeContainer{
			id: "cand-1", appName: "fn", entrypoint: "service.js",
			image: "img-1", port: 80, replica: 1, state: container.StateRunning,
			environmentFingerprint: serviceEnvironmentFingerprint(80),
			resources:              serviceResources(),
			healthcheck:            probe, health: container.Starting,
		}
		f.ctrs["ok-1"] = &fakeContainer{
			id: "ok-1", appName: "fn", entrypoint: "service.js",
			image: "img-1", port: 80, replica: 2, state: container.StateRunning,
			environmentFingerprint: serviceEnvironmentFingerprint(80),
			resources:              serviceResources(),
			healthcheck:            probe, health: container.Healthy,
		}
		f.ctrs["plain-1"] = &fakeContainer{
			id: "plain-1", appName: "fn", entrypoint: "service.js",
			image: "img-1", port: 80, replica: 3, state: container.StateRunning,
			environmentFingerprint: serviceEnvironmentFingerprint(80),
			resources:              serviceResources(),
			health:                 container.Unhealthy,
		}
	}
	idsOf := func(candidates []runtime.ServiceHealthCandidate) []string {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			ids = append(ids, candidate.ContainerID)
		}
		return ids
	}

	t.Run("committed active excluded with a store", func(t *testing.T) {
		f := newFakeDocker()
		seed(f)
		md := newMarkerStoreDocker(f)
		if err := md.WriteServiceConvergenceState(context.Background(),
			runtime.ServiceConvergenceState{
				App: "fn", Service: "service.js", Replica: 0,
				CommittedContainerID: "active-1", Generation: "gen-1",
				UpdatedAt: time.Now(),
			}); err != nil {
			t.Fatalf("seed marker: %v", err)
		}
		c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
		got, err := c.RecoverServiceHealthCandidates(context.Background())
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		ids := idsOf(got)
		if len(ids) != 1 || ids[0] != "cand-1" {
			t.Fatalf("candidates = %v, want [cand-1] "+
				"(committed active, healthy, and unlabeled containers excluded)", ids)
		}
	})

	t.Run("historical best effort without a store", func(t *testing.T) {
		f := newFakeDocker()
		seed(f)
		c := NewServiceReconciler(f, nil, testutil.DiscardLogger(), testReconcileTimeout)
		got, err := c.RecoverServiceHealthCandidates(context.Background())
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		ids := idsOf(got)
		want := []string{"active-1", "cand-1"}
		if len(ids) != len(want) || ids[0] != want[0] || ids[1] != want[1] {
			t.Fatalf("candidates = %v, want %v (every non-healthy probe-labeled container)",
				ids, want)
		}
	})
}

// A convergence-state store that cannot be read fails the pass closed instead
// of mistaking an active replica for an uncommitted candidate.
func TestServiceReconcilerConvergenceStateLoadFailureFailsClosed(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	md.failList = true
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Healthy, 1)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

	result := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if result.Outcome != ServicePassFailed || result.Err == nil ||
		!strings.Contains(result.Err.Error(), "convergence state") {
		t.Fatalf("result = %+v, want a failed pass naming the convergence state", result)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (the pass refused to act on unreadable markers)", events)
	}

	if _, err := c.RecoverServiceHealthCandidates(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "convergence state") {
		t.Fatalf("recover error = %v, want the load failure surfaced", err)
	}
}

// Restart recovery when the failed candidate's container has disappeared: the
// Docker-owned marker still carries the exact generation, attempt, and
// eligibility. While the recorded window is open the slot starts nothing; once
// it elapses the next attempt is previous+1 and the request converges
// normally — the durable marker, not the container, carries the retry across
// the gap.
func TestServiceReconcilerDurableRetryRecoversWhenCandidateDisappeared(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "gone-cand", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 2)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// The failure is recorded while the candidate still exists.
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !first.Pending() {
		t.Fatalf("first result = %+v, want pending inside the retry window", first)
	}
	recorded, ok := md.markerState("fn", "service.js", 0)
	if !ok || recorded.CandidateContainerID != "gone-cand" ||
		recorded.CandidateAttempt != 2 || recorded.CandidateGeneration == "" ||
		!recorded.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("failure marker = (%+v, %v), want gone-cand attempt 2 with the reported window",
			recorded, ok)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none while ineligible", events)
	}

	// The candidate container disappears from the daemon; only the marker
	// volume remembers the generation, attempt, and eligibility.
	f.mu.Lock()
	delete(f.ctrs, "gone-cand")
	f.mu.Unlock()

	// A restarted worker reconstructs the exact window and starts nothing.
	second := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !second.Pending() {
		t.Fatalf("second result = %+v, want still pending before eligibility", second)
	}
	if !second.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("RetryNotBefore after restart = %v, want the recorded %v",
			second.RetryNotBefore, first.RetryNotBefore)
	}
	if len(second.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none (the candidate container no longer exists to observe)",
			second.Candidates)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events before eligibility = %v, want none: no start for a vanished candidate",
			events)
	}
	if got := f.countForApp("fn"); got != 0 {
		t.Fatalf("containers = %d, want none while ineligible", got)
	}
	cw := md.candidateWrites()
	if len(cw) != 1 || !cw[0].RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("candidate-family writes = %+v, want the single recorded window "+
			"reused, not rewritten", cw)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")

	// Once the recorded window elapses, the durable attempt drives the retry:
	// next attempt = previous (2) + 1, and the request converges.
	md.setRetryNotBefore("fn", "service.js", 0, time.Now().Add(-time.Second))
	third := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if third.Pending() || !third.Changed {
		t.Fatalf("third result = %+v, want terminal convergence after eligibility", third)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil {
		t.Fatal("no replacement started after eligibility")
	}
	if replacement.healthcheckAttempt != 3 {
		t.Fatalf("attempt = %d, want 3 (durable previous attempt 2 + 1)", replacement.healthcheckAttempt)
	}
	if got := countEventPrefix(f.order(), "start:"); got != 1 {
		t.Fatalf("start events = %v, want exactly one", f.order())
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want exactly the converged replacement", got)
	}
	writes := md.candidateWrites()
	if len(writes) != 4 {
		t.Fatalf("candidate-family writes = %d, want 4 "+
			"(failure, admitted attempt, started, commit)", len(writes))
	}
	if admitted := writes[1]; admitted.CandidateContainerID != "" ||
		admitted.CandidateAttempt != 3 || !admitted.RetryNotBefore.IsZero() {
		t.Fatalf("admitted record = %+v, want the pre-start attempt-3 admission "+
			"(no container yet) with a reset backoff", admitted)
	}
	if started := writes[2]; started.CandidateAttempt != 3 ||
		!started.RetryNotBefore.IsZero() || started.CandidateContainerID != replacement.id {
		t.Fatalf("started record = %+v, want attempt 3 with a reset backoff for %q",
			started, replacement.id)
	}
	if committed, ok := md.markerState("fn", "service.js", 0); !ok ||
		committed.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker = (%+v, %v), want %q", committed, ok, replacement.id)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
}

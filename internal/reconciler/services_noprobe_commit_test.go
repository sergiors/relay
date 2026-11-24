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

// seedConvergedNoProbeService seeds a RUNNING container that matches the
// desired configuration of a no-healthcheck service exactly: no
// relay.healthcheck label, no Docker health verdict, unknown backend readiness.
func seedConvergedNoProbeService(
	f *fakeDocker, id, fn, name, image string, port, replica int,
) {
	now := time.Now()
	f.ctrs[id] = &fakeContainer{
		id: id, appName: fn, entrypoint: name, image: image, port: port,
		replica: replica, state: container.StateRunning,
		environmentFingerprint: serviceEnvironmentFingerprint(port),
		resources:              serviceResources(),
		createdAt:              now,
		startedAt:              now,
	}
}

// noProbeTemplate declares an entrypoint service with NO healthcheck: its
// effective probe is nil from the first pass on.
func noProbeTemplate() *app.Template {
	return serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
}

// assertNoProbeCommit pins the no-probe commit contract for one pass: the
// serving replica is durably committed with its generation, the replica family
// carries exactly that one write (never a candidate/attempt record or a retry
// window), the pass schedules no health-driven wake, and no preparation state
// was written.
func assertNoProbeCommit(
	t *testing.T, f *fakeDocker, md *markerStoreDocker,
	result ServicePassResult, fn, service, wantID string,
) {
	t.Helper()
	if result.Err != nil || result.Pending() {
		t.Fatalf("result = %+v, want a terminal successful pass", result)
	}
	if len(result.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none: a no-probe commit must not "+
			"register with the healthcheck observer", result.Candidates)
	}
	if !result.RetryNotBefore.IsZero() || !result.StartupDeadline.IsZero() {
		t.Fatalf("deadlines = (retry %v, startup %v), want none: a no-probe "+
			"service schedules no health-driven retry or soft wake",
			result.RetryNotBefore, result.StartupDeadline)
	}
	state, ok := md.markerState(fn, service, 0)
	if !ok || state.CommittedContainerID != wantID || state.Generation == "" {
		t.Fatalf("committed marker = (%+v, %v), want %q committed with a "+
			"non-empty generation", state, ok, wantID)
	}
	if state.CandidateContainerID != "" || state.CandidateAttempt != 0 ||
		!state.RetryNotBefore.IsZero() {
		t.Fatalf("committed marker = %+v, want no candidate attempt or retry "+
			"state on a no-probe service", state)
	}
	writes := md.candidateWrites()
	if len(writes) != 1 || writes[0] != state {
		t.Fatalf("candidate-family writes = %+v, want exactly the single "+
			"commit %+v", writes, state)
	}
	if prep := md.preparationWrites(); len(prep) != 0 {
		t.Fatalf("preparation writes = %+v, want none (no effective probe "+
			"drives the preparation retry)", prep)
	}
	if at := f.indexOfEvent("commit:" + wantID); at < 0 {
		t.Fatalf("events = %v, want the durable commit of %s", f.order(), wantID)
	}
}

// Given a service with NO effective probe (no healthcheck declared), when its
// first pass runs, then the started replica is committed exactly once with its
// generation — for an entrypoint service and for an external `image` service
// alike — with no candidate/attempt record, no retry window, and no health
// candidate registration.
func TestNoProbeServiceFirstStartCommitsExactMarker(t *testing.T) {
	run := func(t *testing.T, tmpl *app.Template, service string) {
		t.Helper()
		f := newFakeDocker()
		md := newMarkerStoreDocker(f)
		c := NewServiceReconciler(md, nil, testutil.DiscardLogger(),
			testReconcileTimeout)

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if got := f.runningCount("fn", service); got != 1 {
			t.Fatalf("running containers = %d, want exactly 1", got)
		}
		started := f.lastStartedFor("fn", service)
		if started == nil {
			t.Fatal("no replica was started")
		}
		if countEventPrefix(f.order(), "start:") != 1 {
			t.Fatalf("events = %v, want exactly one start", f.order())
		}
		assertNoProbeCommit(t, f, md, result, "fn", service, started.id)
	}

	t.Run("entrypoint service without a healthcheck", func(t *testing.T) {
		run(t, noProbeTemplate(), "service.js")
	})

	t.Run("external image service", func(t *testing.T) {
		// The template parser rejects a healthcheck on `image` services, so
		// every image service is a no-probe service by construction.
		tmpl := serviceTemplate("", app.Service{
			Name: "ext", Image: "nginx:1.27", Port: 80, Replicas: 1,
		})
		run(t, tmpl, "ext")
	})
}

// Given an existing config-matching, markerless running no-probe replica, when
// a pass runs, then it is ADOPTED and committed without any new container, and
// a repeated pass is a byte-for-byte no-op: no new start, no new marker write,
// no health candidate, no deadlines.
func TestNoProbeServiceAdoptsMarkerlessRunningReplica(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	seedConvergedNoProbeService(f, "existing-1", "fn", "service.js",
		"img-1", 80, 0)
	tmpl := noProbeTemplate()
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(),
		testReconcileTimeout)

	result := healthApply(t, c, "fn", tmpl, "img-1")

	assertNoStartService(t, f, "markerless adoption")
	assertNoProbeCommit(t, f, md, result, "fn", "service.js", "existing-1")

	// A repeated pass converges on the committed replica without any work.
	eventsBefore := len(f.order())
	markerWritesBefore := len(md.markerWrites())
	again := healthApply(t, c, "fn", tmpl, "img-1")

	if again.Pending() || again.Changed {
		t.Fatalf("second result = %+v, want an unchanged terminal pass", again)
	}
	if len(f.order()) != eventsBefore {
		t.Fatalf("events = %v, want no new transitions after %d",
			f.order(), eventsBefore)
	}
	if got := len(md.markerWrites()); got != markerWritesBefore {
		t.Fatalf("marker writes = %d, want %d (an idempotent pass rewrites "+
			"nothing)", got, markerWritesBefore)
	}
	committed, ok := md.markerState("fn", "service.js", 0)
	if !ok || committed.CommittedContainerID != "existing-1" ||
		committed.Generation == "" {
		t.Fatalf("committed marker = (%+v, %v), want it untouched", committed, ok)
	}
	if len(again.Candidates) != 0 || !again.RetryNotBefore.IsZero() ||
		!again.StartupDeadline.IsZero() {
		t.Fatalf("second result = %+v, want no candidates and no deadlines",
			again)
	}
}

// Given an old generation and a no-probe template change, when the replacement
// converges, then the durable commit marker for the new replica is written
// BEFORE the old generation stops — never after.
func TestNoProbeReplacementCommitsBeforeStoppingOldGeneration(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	c := NewServiceReconciler(md, nil, testutil.DiscardLogger(),
		testReconcileTimeout)

	result := healthApply(t, c, "fn", noProbeTemplate(), "img-new")

	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil {
		t.Fatal("no replacement was started")
	}
	order := f.order()
	start := f.indexOfEvent("start:0")
	commit := f.indexOfEvent("commit:" + replacement.id)
	stop := f.indexOfEvent("stop:old-1")
	if start < 0 || commit < 0 || stop < 0 || start > commit || commit > stop {
		t.Fatalf("order = %v, want start < commit:%s < stop:old-1",
			order, replacement.id)
	}
	assertNoProbeCommit(t, f, md, result, "fn", "service.js", replacement.id)
	if _, ok := f.containerByID("old-1"); ok {
		t.Fatal("the old generation survived convergence")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want 1", got)
	}
}

// Given a started no-probe replica that fails the FINAL running revalidation
// (stopped, or vanished), when the boundary evaluates it, then no marker is
// written, the previous generation is preserved for the next pass, and the
// pass ends pending with NO healthcheck candidate and NO retry/startup
// deadline — a no-probe service has no health-driven retry policy at all.
func TestNoProbeFinalRecheckFailurePreservesFallbackWithoutHealthRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		step probeStep
	}{
		{"stopped before the boundary", probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateExited,
		}}},
		{"vanished before the boundary", probeStep{obs: runtime.ServiceHealthObservation{
			Missing: true,
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeDocker()
			pd := newProbeDocker(f)
			seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
			// The fake starts replicas as id-1, id-2, ... in order.
			pd.script("id-1", tc.step)
			c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
				testReconcileTimeout)

			result := healthApply(t, c, "fn", noProbeTemplate(), "img-1")

			if !result.Pending() {
				t.Fatalf("result = %+v, want pending (the revalidation refused "+
					"the commit)", result)
			}
			if !result.RetryNotBefore.IsZero() ||
				!result.StartupDeadline.IsZero() {
				t.Fatalf("deadlines = (retry %v, startup %v), want none: no "+
					"health-driven retry or backoff exists for a no-probe "+
					"service", result.RetryNotBefore, result.StartupDeadline)
			}
			if len(result.Candidates) != 0 {
				t.Fatalf("candidates = %+v, want none: a no-probe replica must "+
					"not register with the healthcheck observer",
					result.Candidates)
			}
			if writes := pd.candidateWrites(); len(writes) != 0 {
				t.Fatalf("candidate-family writes = %+v, want none (no marker "+
					"for a replica that failed the revalidation)", writes)
			}
			if _, ok := pd.markerState("fn", "service.js", 0); ok {
				t.Fatal("a committed marker must not exist for the refused pass")
			}
			if prep := pd.preparationWrites(); len(prep) != 0 {
				t.Fatalf("preparation writes = %+v, want none", prep)
			}
			if stops := f.stopRequestLog(); len(stops) != 0 {
				t.Fatalf("stop requests = %v, want none: the previous "+
					"generation must be preserved", stops)
			}
			old, ok := f.containerByID("old-1")
			if !ok || old.state != container.StateRunning {
				t.Fatalf("old-1 = (%+v, %v), want the previous generation still "+
					"running", old, ok)
			}
			if got := f.runningCount("fn", "service.js"); got != 2 {
				t.Fatalf("running containers = %d, want 2 (fallback plus the "+
					"re-evaluated replica)", got)
			}
		})
	}
}

// Given a started no-probe replica whose durable COMMIT marker write fails,
// when the boundary reports it, then the pass stays pending with the previous
// generation preserved and nothing recorded, and it produces NO healthcheck
// candidate and NO retry/startup deadline: a no-probe service never enters the
// health-driven retry policy, even on its commit path.
func TestNoProbeMarkerWriteFailurePreservesFallbackWithoutHealthRetry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tmpl  *app.Template
		mount bool
	}{
		{name: "no declared probe", tmpl: noProbeTemplate()},
		{name: "HOT_RELOAD source mount", mount: true,
			tmpl: serviceTemplate("nodejs24.x", app.Service{
				Name: "service.js", Entrypoint: "service.js", Port: 80,
				Replicas: 1, Healthcheck: testServiceHealthcheck(time.Second),
			})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeDocker()
			pd := newProbeDocker(f)
			pd.markerStoreDocker.mu.Lock()
			pd.failWrite = true
			pd.markerStoreDocker.mu.Unlock()
			seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
			if tc.mount {
				pd.setMountIdentity("service.js", "fp1")
				pd.setSourceMounted(true)
			}
			c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
				testReconcileTimeout)

			result := c.ApplyResult(context.Background(), "fn", tc.tmpl,
				"img-1", nil, nil)

			if !result.Pending() {
				t.Fatalf("result = %+v, want PENDING winning over the "+
					"independent marker-write failure", result)
			}
			if result.Err == nil ||
				!strings.Contains(result.Err.Error(), "write failed") {
				t.Fatalf("result.Err = %v, want the marker write failure "+
					"surfaced", result.Err)
			}
			if len(result.Candidates) != 0 {
				t.Fatalf("candidates = %+v, want none: a no-probe commit failure "+
					"must not register with the healthcheck observer",
					result.Candidates)
			}
			if !result.RetryNotBefore.IsZero() ||
				!result.StartupDeadline.IsZero() {
				t.Fatalf("deadlines = (retry %v, startup %v), want none: no "+
					"health-driven retry or backoff exists for a no-probe "+
					"service", result.RetryNotBefore, result.StartupDeadline)
			}
			if writes := pd.candidateWrites(); len(writes) != 0 {
				t.Fatalf("candidate-family writes = %+v, want none (a failed "+
					"write is never recorded and nothing else is written)",
					writes)
			}
			if _, ok := pd.markerState("fn", "service.js", 0); ok {
				t.Fatal("no committed marker may exist after a failed write")
			}
			if prep := pd.preparationWrites(); len(prep) != 0 {
				t.Fatalf("preparation writes = %+v, want none (no effective "+
					"probe drives the preparation retry)", prep)
			}
			if stops := f.stopRequestLog(); len(stops) != 0 {
				t.Fatalf("stop requests = %v, want none: the fallback must be "+
					"preserved across the failed commit", stops)
			}
			old, ok := f.containerByID("old-1")
			if !ok || old.state != container.StateRunning {
				t.Fatalf("old-1 = (%+v, %v), want the previous generation still "+
					"running", old, ok)
			}
		})
	}
}

// Given a no-probe pass that ended pending on a failed commit marker, when the
// healthcheck observer runs its rounds, then it polls Docker for NOTHING and
// hints Notify NEVER — even after the replica's native Docker health flips to
// unhealthy: with no registered candidate there is no health-observer work for
// a no-probe service.
func TestNoProbeCommitFailureLeavesHealthObserverIdle(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	pd.markerStoreDocker.mu.Lock()
	pd.failWrite = true
	pd.markerStoreDocker.mu.Unlock()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
		testReconcileTimeout)

	result := c.ApplyResult(context.Background(), "fn", noProbeTemplate(),
		"img-1", nil, nil)
	if !result.Pending() {
		t.Fatalf("result = %+v, want pending after the failed commit", result)
	}
	if len(result.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none from the failed no-probe commit",
			result.Candidates)
	}

	// The coordinator exposes exactly what the pass reported; the observer is
	// built the way the worker wires it.
	notify, count := notifyCounter()
	o := NewServiceHealthObserver(pd,
		func() []runtime.ServiceHealthCandidate { return result.Candidates },
		nil, notify, testutil.DiscardLogger())
	o.ctx, o.cancel = context.WithCancel(context.Background())
	defer o.cancel()

	probesAfterPass := pd.probeCalls()
	o.observeOnce()
	if got := pd.probeCalls(); got != probesAfterPass {
		t.Fatalf("probe calls = %d, want %d: the observer must poll nothing "+
			"for an unregistered no-probe replica", got, probesAfterPass)
	}

	// A native Docker health transition must not wake the observer either.
	f.setHealth("id-1", container.Unhealthy)
	o.observeOnce()
	if got := pd.probeCalls(); got != probesAfterPass {
		t.Fatalf("probe calls after a health transition = %d, want %d",
			got, probesAfterPass)
	}
	if got := count(); got != 0 {
		t.Fatalf("notify count = %d, want 0 (no health-driven hint for a "+
			"no-probe service)", got)
	}
}

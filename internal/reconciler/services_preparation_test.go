package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// preparationTestTemplate declares the probe service the preparation retry
// tests converge (or fail) through resolution.
func preparationTestTemplate(hc *app.ServiceHealthcheck) *app.Template {
	return serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
}

// stalePreparationMarker is a preparation-family marker recorded under a
// PREVIOUS desired identity, with a retry window that is still open: a current
// generation must never be gated by it, and a successful preparation must
// prune it.
func stalePreparationMarker() runtime.ServiceConvergenceState {
	return runtime.ServiceConvergenceState{
		Kind:                runtime.ServiceStateKindPreparation,
		App:                 "fn",
		Service:             "service.js",
		Replica:             0,
		CandidateGeneration: "previous-desired-generation",
		CandidateAttempt:    3,
		CandidateStartedAt:  time.Now().Add(-time.Minute),
		RetryNotBefore:      time.Now().Add(time.Hour),
	}
}

// An ADMITTED preparation attempt whose failure record could not be persisted
// is never lost: the pass holds the hard window PROCESS-LOCALLY (same attempt,
// so a notification cannot retry early), a restart converts the durable
// admission into the SAME attempt's bounded failure anchored at RECOVERY time
// — a full backoff from now, never at the (possibly long-past) recorded start
// instant — and that conversion happens BEFORE any pre-resolution work. Once
// persisted, the window is read verbatim by every later pass and never
// re-anchored; only after it elapses does the retry actually run.
func TestPreparationFailureWriteConvertsDurableAdmissionSameAttemptAfterRestart(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	// Fail exactly the durable FAILURE window; the admission write must succeed
	// so the attempt stays durable through every pass below.
	md.mu.Lock()
	md.failWriteIf = func(state runtime.ServiceConvergenceState) bool {
		return state.Kind == runtime.ServiceStateKindPreparation &&
			!state.RetryNotBefore.IsZero()
	}
	md.mu.Unlock()
	// A 30s interval keeps every window comfortably future for the whole test,
	// so the exact instants below are deterministic rather than timing-sensitive.
	hc := testServiceHealthcheck(30 * time.Second)
	tmpl := preparationTestTemplate(hc)
	f.setResolveError("service.js", errors.New("registry unavailable"))
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// Pass 1: the attempt is admitted durably, resolution fails, and the
	// FAILURE write fails too — pending with the independent resolution error
	// on the result and the process-local hard window; the only durable state
	// left behind is the admission itself.
	worker := newWorker()
	first := worker.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !first.Pending() || first.Err == nil ||
		!strings.Contains(first.Err.Error(), "registry unavailable") {
		t.Fatalf("first result = %+v, want pending carrying the independent "+
			"resolution failure", first)
	}
	if !first.RetryNotBefore.After(time.Now()) ||
		first.RetryNotBefore.After(time.Now().Add(31*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the bounded %s process-local window",
			first.RetryNotBefore, servicePreparationBackoff(hc.Interval, 1))
	}
	if len(first.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none (preparation never reached "+
			"classification)", first.Candidates)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (no container work)", events)
	}
	if writes := md.candidateWrites(); len(writes) != 0 {
		t.Fatalf("candidate-family writes = %+v, want none", writes)
	}
	if got := len(md.markerWrites()); got != 1 {
		t.Fatalf("marker writes = %d, want 1 (only the admission; the failed "+
			"failure-write is never recorded)", got)
	}
	// Recovery happens LONG after the admission's start: the recorded start
	// instant is two hours old — state an earlier process left behind.
	// Anchoring the conversion at CandidateStartedAt would already be elapsed
	// and admit an immediate retry; the conservative anchor must grant a FULL
	// backoff from recovery instead — while the recorded start marker itself
	// survives the failed conversion untouched.
	staleAnchor := time.Now().Add(-2 * time.Hour)
	md.rewriteMarker(runtime.ServiceConvergenceState{
		Kind: runtime.ServiceStateKindPreparation,
		App:  "fn", Service: "service.js", Replica: 0,
	}, func(state *runtime.ServiceConvergenceState) {
		state.CandidateStartedAt = staleAnchor
	})
	admission, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || admission.CandidateAttempt != 1 ||
		!admission.CandidateStartedAt.Equal(staleAnchor) ||
		!admission.RetryNotBefore.IsZero() ||
		admission.CandidateContainerID != "" {
		t.Fatalf("durable admission = (%+v, %v), want the unrecorded-outcome "+
			"attempt 1 carrying the stale start marker and no window",
			admission, ok)
	}

	// Pass 2, SAME process: the floor gate holds the identical window — not
	// re-anchored — and no work runs behind it.
	second := worker.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !second.Pending() || second.Err != nil {
		t.Fatalf("second result = %+v, want a clean pending pass held by the floor "+
			"(the gate adds no new error)", second)
	}
	if !second.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("RetryNotBefore = %v, want the identical process-local window %v "+
			"(the floor must not re-anchor)", second.RetryNotBefore,
			first.RetryNotBefore)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (the gate holds before any work)", got)
	}
	if got := len(md.markerWrites()); got != 1 {
		t.Fatalf("marker writes = %d, want 1 (the gated pass rewrites nothing)", got)
	}

	// Pass 3, RESTARTED worker: the durable admission is converted into the
	// SAME attempt's bounded failure anchored at RECOVERY time — a FULL
	// attempt-1 backoff from now, never at the two-hour-old start marker,
	// which would already be elapsed — and the conversion runs before any
	// pre-resolution work (resolution is never reached while the window
	// stands).
	third := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1",
		nil, nil)
	if !third.Pending() || third.Err == nil ||
		!strings.Contains(third.Err.Error(), "record preparation retry state") {
		t.Fatalf("third result = %+v, want pending with the converted failure",
			third)
	}
	fullBackoff := servicePreparationBackoff(hc.Interval, 1)
	if !third.RetryNotBefore.After(time.Now()) ||
		third.RetryNotBefore.After(time.Now().Add(fullBackoff+time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a FULL %s backoff from RECOVERY time "+
			"(after now, within now+%s) — never anchored at the stale start "+
			"marker %v", third.RetryNotBefore, fullBackoff, fullBackoff,
			staleAnchor)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (conversion happens before "+
			"pre-resolution work)", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none before eligibility", events)
	}
	if still, ok := md.preparationMarkerState("fn", "service.js"); !ok ||
		still.CandidateAttempt != admission.CandidateAttempt ||
		!still.CandidateStartedAt.Equal(staleAnchor) ||
		!still.RetryNotBefore.IsZero() ||
		still.CandidateGeneration != admission.CandidateGeneration {
		t.Fatalf("admission after the failed conversion = (%+v, %v), want the "+
			"recorded attempt %d and start marker %v preserved, with no window",
			still, ok, admission.CandidateAttempt, staleAnchor)
	}

	// Pass 4, restarted with writes repaired: the conversion becomes DURABLE —
	// again a FULL backoff from THIS recovery, same attempt — and the persisted
	// window is exact from here on. The open window still gates before any
	// pre-resolution work.
	md.mu.Lock()
	md.failWriteIf = nil
	md.mu.Unlock()
	fourth := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1",
		nil, nil)
	if !fourth.Pending() || fourth.Err != nil {
		t.Fatalf("fourth result = %+v, want a clean pending pass behind the now "+
			"durable window", fourth)
	}
	if !fourth.RetryNotBefore.After(time.Now()) ||
		fourth.RetryNotBefore.After(time.Now().Add(fullBackoff+time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a FULL %s backoff from THIS recovery "+
			"(now, now+%s] — never anchored at the stale start marker %v",
			fourth.RetryNotBefore, fullBackoff, fullBackoff, staleAnchor)
	}
	durable, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || durable.Kind != runtime.ServiceStateKindPreparation ||
		durable.CandidateAttempt != 1 ||
		durable.CandidateContainerID != "" ||
		!durable.RetryNotBefore.Equal(fourth.RetryNotBefore) ||
		durable.CandidateGeneration != admission.CandidateGeneration {
		t.Fatalf("durable failure = (%+v, %v), want the same generation and "+
			"attempt 1 carrying the persisted window EXACTLY as reported",
			durable, ok)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (the durable window still gates "+
			"before any retry)", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none before eligibility", events)
	}

	// Pass 5, another restart: the PERSISTED window is read verbatim — never
	// re-anchored at this pass's now — and a reading pass rewrites nothing.
	fifth := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1",
		nil, nil)
	if !fifth.Pending() || fifth.Err != nil {
		t.Fatalf("fifth result = %+v, want a clean pending pass behind the "+
			"recorded window", fifth)
	}
	if !fifth.RetryNotBefore.Equal(durable.RetryNotBefore) {
		t.Fatalf("RetryNotBefore = %v, want the persisted window %v verbatim "+
			"(a persisted RetryNotBefore is never re-anchored)",
			fifth.RetryNotBefore, durable.RetryNotBefore)
	}
	if persisted, ok := md.preparationMarkerState("fn", "service.js"); !ok ||
		persisted != durable {
		t.Fatalf("preparation marker = (%+v, %v), want it untouched by a "+
			"reading pass: %+v", persisted, ok, durable)
	}
	if got := f.resolveCount(); got != 1 {
		t.Fatalf("resolve calls = %d, want 1 (the persisted window still gates "+
			"before any retry)", got)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none before eligibility", events)
	}

	// Pass 6, once the recorded window elapses: the retry runs, preparation
	// succeeds, and the family is pruned again.
	md.setPreparationRetryNotBefore("fn", "service.js", time.Now().Add(-time.Second))
	f.setResolveError("service.js", nil)
	sixth := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if sixth.Pending() || !sixth.Changed {
		t.Fatalf("sixth result = %+v, want terminal convergence after the window",
			sixth)
	}
	if got := f.resolveCount(); got != 2 {
		t.Fatalf("resolve calls = %d, want 2 (the retry runs only after the window)",
			got)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want the converged replacement", got)
	}
	if _, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatal("preparation family must be pruned after the successful retry")
	}
}

// Successful preparation prunes EVERY preparation marker for the service —
// including one recorded under a previous desired identity whose window is
// still open — through the kind-scoped delete, while the replica's
// committed/candidate marker and its write history stay byte-for-byte intact.
// A store without the kind-scoped delete takes the fallback: a cleared record
// carrying no attempt or window, which can gate nothing.
func TestSuccessfulPreparationPrunesStalePreparationMarkersOnly(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)

	t.Run("probe service prunes a stale previous-generation marker", func(t *testing.T) {
		f := newFakeDocker()
		md := newMarkerStoreDocker(f)
		newWorker := func() *ServiceReconciler {
			return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
		}

		// Pass 1 converges and commits the replica.
		first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
		if first.Pending() || !first.Changed {
			t.Fatalf("first result = %+v, want terminal convergence", first)
		}
		committedBefore, ok := md.markerState("fn", "service.js", 0)
		if !ok || committedBefore.CommittedContainerID == "" {
			t.Fatalf("precondition: committed marker = (%+v, %v)", committedBefore, ok)
		}
		candidateWritesBefore := len(md.candidateWrites())

		// A marker from a previous desired identity, window still open.
		md.seedMarker(stalePreparationMarker())
		if _, ok := md.preparationMarkerState("fn", "service.js"); !ok {
			t.Fatal("precondition: the stale marker must be present before the pass")
		}

		// Pass 2 (fresh worker): the stale generation gates nothing, and the
		// successful preparation prunes it without touching the replica family.
		again := healthApply(t, newWorker(), "fn", tmpl, "img-1")
		if again.Pending() || again.Changed {
			t.Fatalf("second result = %+v, want an unchanged terminal pass: the "+
				"stale generation's open window must not gate the current one", again)
		}
		if stale, ok := md.preparationMarkerState("fn", "service.js"); ok {
			t.Fatalf("stale preparation marker survived = %+v, want the family pruned",
				stale)
		}
		committedAfter, ok := md.markerState("fn", "service.js", 0)
		if !ok || committedAfter != committedBefore {
			t.Fatalf("committed/candidate marker = (%+v, %v), want it untouched: "+
				"%+v", committedAfter, ok, committedBefore)
		}
		if got := len(md.candidateWrites()); got != candidateWritesBefore {
			t.Fatalf("candidate-family writes = %d, want %d (the kind-scoped prune "+
				"never writes the replica family)", got, candidateWritesBefore)
		}
	})

	t.Run("HOT_RELOAD prunes with zero preparation writes", func(t *testing.T) {
		f := newFakeDocker()
		md := newMarkerStoreDocker(f)
		// The declared probe is bypassed entirely: no admission is ever
		// written, so only the kind-scoped delete can remove the stale marker.
		f.setMountIdentity("service.js", "fp1")
		f.setSourceMounted(true)
		md.seedMarker(stalePreparationMarker())
		md.seedMarker(runtime.ServiceConvergenceState{
			App:                  "fn",
			Service:              "service.js",
			Replica:              0,
			CommittedContainerID: "live-1",
			Generation:           "unrelated-generation",
		})
		if _, ok := md.preparationMarkerState("fn", "service.js"); !ok {
			t.Fatal("precondition: the stale marker must be present before the pass")
		}
		committedBefore, ok := md.markerState("fn", "service.js", 0)
		if !ok {
			t.Fatal("precondition: the committed marker must be present before the pass")
		}
		c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := healthApply(t, c, "fn", tmpl, "img-1")

		if result.Pending() || !result.Changed {
			t.Fatalf("result = %+v, want terminal convergence under HOT_RELOAD",
				result)
		}
		if writes := md.preparationWrites(); len(writes) != 0 {
			t.Fatalf("preparation writes = %+v, want none (the bypassed probe "+
				"writes no preparation state)", writes)
		}
		if stale, ok := md.preparationMarkerState("fn", "service.js"); ok {
			t.Fatalf("stale preparation marker survived with zero preparation "+
				"writes = %+v, want the kind-scoped delete to have removed it", stale)
		}
		if writes := md.candidateWrites(); len(writes) != 0 {
			t.Fatalf("candidate-family writes = %+v, want none (a probe-less "+
				"service commits no marker state)", writes)
		}
		committedAfter, ok := md.markerState("fn", "service.js", 0)
		if !ok || committedAfter != committedBefore {
			t.Fatalf("committed/candidate marker = (%+v, %v), want it untouched: "+
				"%+v", committedAfter, ok, committedBefore)
		}
		if got := f.runningCount("fn", "service.js"); got != 1 {
			t.Fatalf("running containers = %d, want exactly the started replica", got)
		}
	})

	t.Run("a store without the kind-scoped delete falls back to a cleared record",
		func(t *testing.T) {
			f := newFakeDocker()
			store := newFallbackMarkerStore(f)
			store.inner.seedMarker(stalePreparationMarker())
			if _, ok := store.inner.preparationMarkerState("fn", "service.js"); !ok {
				t.Fatal("precondition: the stale marker must be present before the pass")
			}
			c := NewServiceReconciler(store, nil, testutil.DiscardLogger(), testReconcileTimeout)

			first := healthApply(t, c, "fn", tmpl, "img-1")
			if first.Pending() || !first.Changed {
				t.Fatalf("first result = %+v, want terminal convergence (the stale "+
					"generation must not gate)", first)
			}
			cleared, ok := store.inner.preparationMarkerState("fn", "service.js")
			if !ok {
				t.Fatal("the fallback must leave its cleared record in the store")
			}
			if !cleared.RetryNotBefore.IsZero() || cleared.CandidateAttempt != 0 ||
				!cleared.CandidateStartedAt.IsZero() {
				t.Fatalf("fallback record = %+v, want a cleared record carrying no "+
					"attempt, start instant, or window — it can gate nothing", cleared)
			}
			if cleared.CandidateGeneration == "previous-desired-generation" {
				t.Fatalf("fallback record = %+v, want the current generation, not "+
					"the stale one", cleared)
			}

			// A second pass proves the cleared record gates nothing.
			second := healthApply(t, c, "fn", tmpl, "img-1")
			if second.Pending() || second.Changed {
				t.Fatalf("second result = %+v, want an unchanged terminal pass "+
					"(the cleared record must never gate)", second)
			}
		})
}

// The preparation retry key covers every desired input knowable before
// resolution: the app image an entrypoint service runs (a Git source update
// under a byte-identical template) and the EFFECTIVE probe — Relay's declared
// probe only while it is actually applied (never under a HOT_RELOAD source
// mount). An image service's own reference already keys it. Routing host/path
// is deliberately excluded: routes are served from discovery and never replace
// a container.
func TestServicePreparationIdentityKeysImageAndEffectiveProbe(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	probeEdit := testServiceHealthcheck(30 * time.Second)
	tmpl := serviceTemplate("nodejs24.x")
	idOf := func(
		svc app.Service, appImage string, mounted bool,
	) string {
		return servicePreparationIdentity(tmpl, svc, appImage,
			[]string{"net"}, "", mounted)
	}
	routed := app.Service{
		Name: "web", Entrypoint: "service.js", Port: 8080, Replicas: 1,
		Host: "web.example.com", Path: "/api", Healthcheck: hc,
	}

	t.Run("app image keys an entrypoint service", func(t *testing.T) {
		if idOf(routed, "img-a", false) == idOf(routed, "img-b", false) {
			t.Fatal("a Git source update (a new app image under a byte-identical " +
				"template) must change the preparation key")
		}
	})

	t.Run("an image service is keyed by its own reference", func(t *testing.T) {
		imageSvc := app.Service{Name: "ext", Image: "nginx:1.27", Port: 80, Replicas: 1}
		if idOf(imageSvc, "img-a", false) != idOf(imageSvc, "img-b", false) {
			t.Fatal("the app image must not key an image service (its configured " +
				"reference already does)")
		}
		otherRef := imageSvc
		otherRef.Image = "nginx:1.28"
		if idOf(imageSvc, "img-a", false) == idOf(otherRef, "img-a", false) {
			t.Fatal("the image service's own reference must be part of its key")
		}
	})

	t.Run("routing is not part of the key", func(t *testing.T) {
		moved := routed
		moved.Host = "other.example.com"
		moved.Path = "/other"
		if idOf(routed, "img-a", false) != idOf(moved, "img-a", false) {
			t.Fatal("a routing host/path change must not re-key container preparation")
		}
	})

	t.Run("the probe only keys while it is effective", func(t *testing.T) {
		edited := routed
		edited.Healthcheck = probeEdit
		if idOf(routed, "img-a", false) == idOf(edited, "img-a", false) {
			t.Fatal("without a source mount the declared probe applies, so a " +
				"probe-only edit is a new preparation key")
		}
		if idOf(routed, "img-a", true) != idOf(edited, "img-a", true) {
			t.Fatal("under a HOT_RELOAD source mount the probe is bypassed, so a " +
				"probe-only edit must not change the key")
		}
		if idOf(routed, "img-a", false) == idOf(routed, "img-a", true) {
			t.Fatal("the mount state itself is part of the effective-probe key")
		}
	})
}

// The preparation retry follows the EFFECTIVE probe: a declared probe drives
// the durable admission plus the bounded failure window, while a HOT_RELOAD
// source-mounted app (probe bypassed) keeps the historical TERMINAL per-pass
// failure — no admission, no marker, no pending window, no retry state.
func TestPreparationRetryFollowsEffectiveProbeUnderHotReload(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	tmpl := preparationTestTemplate(hc)

	t.Run("declared probe drives admission and a bounded window", func(t *testing.T) {
		f := newFakeDocker()
		md := newMarkerStoreDocker(f)
		f.setResolveError("service.js", errors.New("registry unavailable"))
		c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)

		if !result.Pending() || result.Err == nil ||
			!strings.Contains(result.Err.Error(), "registry unavailable") {
			t.Fatalf("result = %+v, want pending carrying the resolution failure",
				result)
		}
		if !result.RetryNotBefore.After(time.Now()) ||
			result.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
			t.Fatalf("RetryNotBefore = %v, want the fresh bounded preparation window",
				result.RetryNotBefore)
		}
		prep, ok := md.preparationMarkerState("fn", "service.js")
		if !ok || prep.Kind != runtime.ServiceStateKindPreparation ||
			prep.CandidateAttempt != 1 ||
			!prep.RetryNotBefore.Equal(result.RetryNotBefore) ||
			prep.CandidateContainerID != "" {
			t.Fatalf("preparation marker = (%+v, %v), want the durable attempt-1 "+
				"window matching the reported wake", prep, ok)
		}
		if writes := md.candidateWrites(); len(writes) != 0 {
			t.Fatalf("candidate-family writes = %+v, want none (a preparation "+
				"failure never touches the replica family)", writes)
		}
		if events := f.order(); len(events) != 0 {
			t.Fatalf("events = %v, want none (no container work)", events)
		}
	})

	t.Run("HOT_RELOAD keeps the terminal per-pass failure", func(t *testing.T) {
		f := newFakeDocker()
		md := newMarkerStoreDocker(f)
		f.setMountIdentity("service.js", "fp1")
		f.setSourceMounted(true)
		f.setResolveError("service.js", errors.New("registry unavailable"))
		c := NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)

		result := c.ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)

		if result.Outcome != ServicePassFailed || result.Err == nil ||
			!strings.Contains(result.Err.Error(), "registry unavailable") {
			t.Fatalf("result = %+v, want a TERMINAL per-pass failure (the bypassed "+
				"probe must not drive the retry policy)", result)
		}
		if !result.RetryNotBefore.IsZero() || !result.StartupDeadline.IsZero() {
			t.Fatalf("deadlines = (retry %v, startup %v), want no window: a "+
				"probe-less service has no bounded retry to install",
				result.RetryNotBefore, result.StartupDeadline)
		}
		if got := len(md.markerWrites()); got != 0 {
			t.Fatalf("marker writes = %+v, want none (a HOT_RELOAD preparation "+
				"failure persists no attempt state)", md.markerWrites())
		}
		if events := f.order(); len(events) != 0 {
			t.Fatalf("events = %v, want none", events)
		}
	})
}

// The candidate family behaves the same way for a STARTED attempt whose
// container vanished before its failure instant was persisted: recovery
// converts the stale attempt/start record into a FULL backoff from now — never
// anchored at the hours-old start instant, which would already be elapsed and
// admit an immediate start — preserving the recorded attempt. Once persisted,
// later passes read the window verbatim and never re-anchor it.
func TestVanishedStartedCandidateRecoveryAnchorsFullBackoffAndPersistsExactWindow(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	seedConvergedHealthService(f, "cand-1", "fn", "service.js", "img-1",
		80, 0, hc, container.Unhealthy, 5)
	tmpl := preparationTestTemplate(hc)
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}

	// Pass 1 records the observed failure durably while the candidate exists
	// (attempt 5 comes from its immutable container label).
	first := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !first.Pending() || !first.RetryNotBefore.After(time.Now()) {
		t.Fatalf("first result = %+v, want pending with a future durable window",
			first)
	}

	// The container vanishes, and the marker is rewritten into what an
	// interrupted process leaves behind: a STARTED attempt record with an OLD
	// start instant and NO persisted failure window.
	f.mu.Lock()
	delete(f.ctrs, "cand-1")
	f.mu.Unlock()
	staleAnchor := time.Now().Add(-2 * time.Hour)
	md.rewriteMarker(runtime.ServiceConvergenceState{
		App: "fn", Service: "service.js", Replica: 0,
	}, func(state *runtime.ServiceConvergenceState) {
		state.RetryNotBefore = time.Time{}
		state.CandidateStartedAt = staleAnchor
	})

	// Recovery: a FULL attempt-5 backoff from now — anchoring at the
	// two-hour-old start would be elapsed and start attempt 6 immediately.
	second := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !second.Pending() || second.Changed {
		t.Fatalf("second result = %+v, want pending with no corrective work",
			second)
	}
	fullBackoff := serviceHealthBackoff(hc.Interval, 5)
	if !second.RetryNotBefore.After(time.Now()) ||
		second.RetryNotBefore.After(time.Now().Add(fullBackoff+time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want a FULL %s (attempt-5) backoff from "+
			"RECOVERY time (now, now+%s] — never anchored at %v",
			second.RetryNotBefore, fullBackoff, fullBackoff, staleAnchor)
	}
	assertNoStartService(t, f, "vanished-candidate recovery")
	if stops := f.stopRequestLog(); len(stops) != 0 {
		t.Fatalf("stop requests = %v, want nothing retired before eligibility",
			stops)
	}
	if got := f.countForApp("fn"); got != 0 {
		t.Fatalf("containers = %d, want none (the candidate vanished)", got)
	}
	if len(second.Candidates) != 0 {
		t.Fatalf("candidates = %+v, want none (no container exists to observe)",
			second.Candidates)
	}
	persisted, ok := md.markerState("fn", "service.js", 0)
	if !ok || persisted.CommittedContainerID != "" ||
		persisted.CandidateAttempt != 5 ||
		!persisted.CandidateStartedAt.Equal(staleAnchor) ||
		!persisted.RetryNotBefore.Equal(second.RetryNotBefore) ||
		persisted.CandidateContainerID != "cand-1" ||
		persisted.CandidateGeneration == "" {
		t.Fatalf("recovery marker = (%+v, %v), want attempt 5 and the start "+
			"marker preserved with the window persisted exactly as reported %v",
			persisted, ok, second.RetryNotBefore)
	}
	if got := len(md.candidateWrites()); got != 2 {
		t.Fatalf("candidate-family writes = %d, want 2 (the original failure + "+
			"exactly one recovery conversion)", got)
	}

	// A later restart reads the persisted window VERBATIM — never re-anchored
	// at this pass's now — and rewrites nothing.
	third := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if !third.Pending() || third.Changed {
		t.Fatalf("third result = %+v, want pending behind the recorded window",
			third)
	}
	if !third.RetryNotBefore.Equal(persisted.RetryNotBefore) {
		t.Fatalf("RetryNotBefore = %v, want the persisted window %v verbatim "+
			"(a persisted RetryNotBefore is never re-anchored)",
			third.RetryNotBefore, persisted.RetryNotBefore)
	}
	if after, ok := md.markerState("fn", "service.js", 0); !ok ||
		after != persisted {
		t.Fatalf("marker after a reading pass = (%+v, %v), want it byte-for-byte "+
			"unchanged: %+v", after, ok, persisted)
	}
	assertNoStartService(t, f, "persisted-window read")
	if got := len(md.candidateWrites()); got != 2 {
		t.Fatalf("candidate-family writes = %d, want 2 (a reading pass rewrites "+
			"nothing)", got)
	}
	assertPreparationAdmittedThenPruned(t, md, "fn", "service.js")
}

package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// The candidate retry delay doubles the probe interval once per failed attempt
// and caps only the DELAY — never the attempt count: a huge attempt still
// yields the capped polling delay instead of a give-up sentinel, and a probe
// slower than the cap is never retried faster than its own cadence.
func TestServiceHealthBackoffIsExponentialAndDelayCappedOnly(t *testing.T) {
	const interval = time.Second
	for attempt, want := range map[int]time.Duration{
		1:  time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		6:  32 * time.Second,
		7:  time.Minute,
		30: time.Minute,
	} {
		if got := serviceHealthBackoff(interval, attempt); got != want {
			t.Errorf("backoff(attempt %d) = %v, want %v", attempt, got, want)
		}
	}
	if got := serviceHealthBackoff(interval, 1<<20); got != time.Minute {
		t.Fatalf("backoff(attempt 1<<20) = %v, want the capped %v "+
			"(there is no attempt cutoff, only a delay cap)",
			got, time.Minute)
	}
	if got := serviceHealthBackoff(2*time.Minute, 5); got != 2*time.Minute {
		t.Fatalf("backoff(2m probe) = %v, want the probe's own %v cadence",
			got, 2*time.Minute)
	}
	if got := serviceHealthBackoff(0, 3); got != 0 {
		t.Fatalf("backoff(0 interval) = %v, want 0 (no probe, no window)", got)
	}
}

// The preparation retry delay is independent of the probe's readiness cadence:
// a fast probe keeps its own cadence for the early retries and doubles toward
// the one-minute polling cap, a missing interval floors at one minute (never a
// tight loop), and a slow probe starts at the floor and doubles toward its own
// interval — again with no attempt cutoff.
func TestServicePreparationBackoffFloorsAndCapsIndependently(t *testing.T) {
	for attempt, want := range map[int]time.Duration{
		1:    time.Second,
		2:    2 * time.Second,
		7:    time.Minute,
		1000: time.Minute,
	} {
		if got := servicePreparationBackoff(time.Second, attempt); got != want {
			t.Errorf("preparation backoff(1s probe, attempt %d) = %v, want %v",
				attempt, got, want)
		}
	}
	for attempt := 1; attempt <= 5; attempt++ {
		if got := servicePreparationBackoff(0, attempt); got != time.Minute {
			t.Errorf("preparation backoff(no probe, attempt %d) = %v, want the %v floor",
				attempt, got, time.Minute)
		}
	}
	for attempt, want := range map[int]time.Duration{
		1: time.Minute, 2: 2 * time.Minute, 9: 2 * time.Minute,
	} {
		if got := servicePreparationBackoff(2*time.Minute, attempt); got != want {
			t.Errorf("preparation backoff(2m probe, attempt %d) = %v, want %v",
				attempt, got, want)
		}
	}
	if got := servicePreparationBackoff(time.Second, 1<<20); got != time.Minute {
		t.Fatalf("preparation backoff(attempt 1<<20) = %v, want the capped %v "+
			"(no attempt cutoff)", got, time.Minute)
	}
}

// A preparation retry window is generation-fenced and resets on success: a
// recorded failure gates the pass with a durable window, successful
// preparation clears that durable record (and the process-local floor), and a
// later failure of the SAME desired generation starts a fresh attempt sequence
// — attempt 1 again — without ever touching the committed generation.
func TestPreparationRetryWindowResetsAfterSuccessfulPreparation(t *testing.T) {
	f := newFakeDocker()
	md := newMarkerStoreDocker(f)
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	newWorker := func() *ServiceReconciler {
		return NewServiceReconciler(md, nil, testutil.DiscardLogger(), testReconcileTimeout)
	}
	setResolveError := func(err error) {
		f.mu.Lock()
		if err == nil {
			delete(f.resolveErr, "service.js")
		} else {
			f.resolveErr["service.js"] = err
		}
		f.mu.Unlock()
	}

	// Pass 1: resolution fails before any candidate exists, so the durable
	// preparation family records attempt 1 with a bounded future window — and
	// no container work happens.
	setResolveError(errors.New("registry unavailable"))
	first := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !first.Pending() || first.Err == nil ||
		!strings.Contains(first.Err.Error(), "registry unavailable") {
		t.Fatalf("first result = %+v, want pending carrying the resolution failure",
			first)
	}
	if !first.RetryNotBefore.After(time.Now()) ||
		first.RetryNotBefore.After(time.Now().Add(2*time.Second)) {
		t.Fatalf("RetryNotBefore = %v, want the fresh bounded preparation window",
			first.RetryNotBefore)
	}
	prep, ok := md.preparationMarkerState("fn", "service.js")
	if !ok || prep.Kind != runtime.ServiceStateKindPreparation ||
		prep.CandidateContainerID != "" || prep.CandidateAttempt != 1 ||
		!prep.RetryNotBefore.Equal(first.RetryNotBefore) {
		t.Fatalf("preparation marker = (%+v, %v), want the uncommitted attempt-1 "+
			"window", prep, ok)
	}
	if events := f.order(); len(events) != 0 {
		t.Fatalf("events = %v, want none (preparation failure never touches containers)",
			events)
	}

	// The window elapses and resolution recovers: preparation succeeds, which
	// clears the durable window, and the service converges normally.
	md.setPreparationRetryNotBefore("fn", "service.js", time.Now().Add(-time.Second))
	setResolveError(nil)
	second := healthApply(t, newWorker(), "fn", tmpl, "img-1")
	if second.Pending() || !second.Changed {
		t.Fatalf("second result = %+v, want terminal convergence after recovery",
			second)
	}
	replacement := f.lastStartedFor("fn", "service.js")
	if replacement == nil {
		t.Fatal("no container started after the preparation recovered")
	}
	if cleared, ok := md.preparationMarkerState("fn", "service.js"); ok {
		t.Fatalf("preparation marker after success = (%+v, %v), want the family "+
			"pruned — no stale admission or window survives a successful preparation",
			cleared, ok)
	}
	if committed, ok := md.markerState("fn", "service.js", 0); !ok ||
		committed.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker = (%+v, %v), want %q",
			committed, ok, replacement.id)
	}

	// The SAME desired generation fails preparation again: the sequence restarts
	// at attempt 1 (success reset), and the write stays in the preparation
	// family — the committed generation is never overwritten.
	setResolveError(errors.New("registry unavailable again"))
	third := newWorker().ApplyResult(context.Background(), "fn", tmpl, "img-1", nil, nil)
	if !third.Pending() || third.Err == nil ||
		!strings.Contains(third.Err.Error(), "registry unavailable again") {
		t.Fatalf("third result = %+v, want pending carrying the new failure", third)
	}
	if !third.RetryNotBefore.After(time.Now()) {
		t.Fatalf("RetryNotBefore = %v, want a fresh future window", third.RetryNotBefore)
	}
	prep, ok = md.preparationMarkerState("fn", "service.js")
	if !ok || prep.CandidateAttempt != 1 ||
		!prep.RetryNotBefore.Equal(third.RetryNotBefore) {
		t.Fatalf("preparation marker = (%+v, %v), want a FRESH attempt-1 window "+
			"(the success reset the sequence)", prep, ok)
	}
	committed, ok := md.markerState("fn", "service.js", 0)
	if !ok || committed.CommittedContainerID != replacement.id {
		t.Fatalf("committed marker after the new failure = (%+v, %v), want the live "+
			"generation %q untouched by the preparation family", committed, ok,
			replacement.id)
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want the committed replica untouched", got)
	}
	if got := countEventPrefix(f.order(), "start:"); got != 1 {
		t.Fatalf("start events = %v, want exactly 1 (only the successful pass started)",
			f.order())
	}
}

package reconciler

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// hotReloadProbeDocker layers the in-place restart capability and the
// read-through probe seam over ONE shared fake daemon and marker store, so a
// HOT_RELOAD scenario exercises restart, revalidation, and the durable commit
// through the production entry point on a single deterministic daemon.
type hotReloadProbeDocker struct {
	*probeDocker
	restarter *restartFakeDocker
}

func newHotReloadProbeDocker() *hotReloadProbeDocker {
	restarter := newRestartFakeDocker()
	return &hotReloadProbeDocker{
		probeDocker: newProbeDocker(restarter.fakeDocker),
		restarter:   restarter,
	}
}

func (d *hotReloadProbeDocker) RestartServiceContainer(
	ctx context.Context, id string,
) error {
	return d.restarter.RestartServiceContainer(ctx, id)
}

// seedHotReloadReplica seeds the historical replica a HOT_RELOAD restart finds
// on the daemon: config-matching under the mount, carrying whatever probe-era
// state the scenario under test leaves behind (stale label, explicit
// Ready=false, Docker health verdict), bound to the given source identity.
func seedHotReloadReplica(
	d *hotReloadProbeDocker, id, sourceID string,
	healthcheck string, health container.HealthStatus, ready *bool,
) {
	now := time.Now()
	d.fakeDocker.mu.Lock()
	defer d.fakeDocker.mu.Unlock()
	d.fakeDocker.ctrs[id] = &fakeContainer{
		id: id, appName: "fn", entrypoint: "service.js", image: "img-1",
		port: 80, replica: 0, state: container.StateRunning,
		sourceID:               sourceID,
		environmentFingerprint: serviceEnvironmentFingerprint(80),
		resources:              serviceResources(),
		healthcheck:            healthcheck,
		health:                 health,
		ready:                  ready,
		createdAt:              now,
		startedAt:              now,
	}
}

// Given a HOT_RELOAD (source-mounted) app whose running replica still carries
// probe-era state — the stale relay.healthcheck label, an explicit Ready=false
// observation, and Docker reporting starting or unhealthy — when a pass runs,
// then none of it gates the effective no-probe policy: the replica is adopted
// without a new start and COMMITTED with its generation, with no health
// candidate, no retry/startup deadline, no preparation state, and exactly one
// probe call (the final running revalidation — the probe-gated pre-commit
// revalidation must not run for an effective-nil policy).
func TestHotReloadStaleProbeStateDoesNotBlockNoProbeCommit(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})
	for _, tc := range []struct {
		name   string
		health container.HealthStatus
	}{
		{"docker still starting", container.Starting},
		{"docker unhealthy", container.Unhealthy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newHotReloadProbeDocker()
			d.setMountIdentity("service.js", "fp1")
			d.setSourceMounted(true)
			seedHotReloadReplica(d, "live-1", "fp1",
				runtime.ServiceHealthcheckLabel(hc), tc.health, boolPtr(false))
			// The final running revalidation still observes the stale verdict.
			d.script("live-1", probeStep{obs: runtime.ServiceHealthObservation{
				State: container.StateRunning, Health: tc.health,
			}})
			c := NewServiceReconciler(d, nil, testutil.DiscardLogger(),
				testReconcileTimeout)

			result := healthApply(t, c, "fn", tmpl, "img-1")

			if result.Err != nil || result.Pending() {
				t.Fatalf("result = %+v, want a terminal pass: probe-era state "+
					"must not gate an effective no-probe replica", result)
			}
			assertNoStartService(t, d.fakeDocker, "HOT_RELOAD adoption")
			state, ok := d.markerState("fn", "service.js", 0)
			if !ok || state.CommittedContainerID != "live-1" ||
				state.Generation == "" {
				t.Fatalf("committed marker = (%+v, %v), want the adopted "+
					"replica committed with its generation", state, ok)
			}
			if state.CandidateContainerID != "" || state.CandidateAttempt != 0 ||
				!state.RetryNotBefore.IsZero() {
				t.Fatalf("committed marker = %+v, want no candidate attempt or "+
					"retry state under an effective no-probe policy", state)
			}
			writes := d.candidateWrites()
			if len(writes) != 1 || writes[0] != state {
				t.Fatalf("candidate-family writes = %+v, want exactly the single "+
					"commit", writes)
			}
			if prep := d.preparationWrites(); len(prep) != 0 {
				t.Fatalf("preparation writes = %+v, want none (the bypassed "+
					"probe drives no preparation retry)", prep)
			}
			if len(result.Candidates) != 0 ||
				!result.RetryNotBefore.IsZero() ||
				!result.StartupDeadline.IsZero() {
				t.Fatalf("result = %+v, want no health candidate and no deadlines",
					result)
			}
			if log := d.probeLog(); !slices.Equal(log, []string{"live-1"}) {
				t.Fatalf("probe log = %v, want [live-1] (only the final running "+
					"revalidation; the probe-gated pre-commit revalidation must not "+
					"run)", log)
			}
		})
	}
}

// Given a HOT_RELOAD replica converged and committed, when a source-only edit
// lands (same image and configuration, new live fingerprint), then the SAME
// container is restarted in place and the NEW source generation is committed —
// restart strictly before the durable commit — with no candidate attempt, no
// retry window, no health candidate, and a repeated pass that changes nothing.
func TestHotReloadInPlaceSourceRestartCommitsNewGeneration(t *testing.T) {
	d := newHotReloadProbeDocker()
	d.setMountIdentity("service.py", "fp1")
	d.setSourceMounted(true)
	tmpl := serviceTemplate("python3.14", app.Service{
		Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1,
	})
	c := NewServiceReconciler(d, nil, testutil.DiscardLogger(),
		testReconcileTimeout)

	first := healthApply(t, c, "fn", tmpl, "img-1")
	if first.Err != nil || first.Pending() {
		t.Fatalf("first result = %+v, want terminal convergence", first)
	}
	id := onlyContainerID(t, d.fakeDocker, "fn", "svc")
	firstMarker, ok := d.markerState("fn", "svc", 0)
	if !ok || firstMarker.CommittedContainerID != id ||
		firstMarker.Generation == "" {
		t.Fatalf("first marker = (%+v, %v), want %q committed with its "+
			"generation", firstMarker, ok, id)
	}
	writesBefore := len(d.candidateWrites())
	prepBefore := len(d.preparationWrites())

	// Source-only edit: same image reference, new live fingerprint.
	d.setMountIdentity("service.py", "fp2")
	eventsBefore := len(d.fakeDocker.order())
	stopsBefore := len(d.fakeDocker.stopRequestLog())
	second := healthApply(t, c, "fn", tmpl, "img-1")

	if second.Err != nil || second.Pending() {
		t.Fatalf("second result = %+v, want terminal convergence", second)
	}
	if got := d.restarter.restartCount(); got != 1 {
		t.Fatalf("restarts = %d, want exactly 1 (in-place source reload)", got)
	}
	if got := onlyContainerID(t, d.fakeDocker, "fn", "svc"); got != id {
		t.Fatalf("container id = %s, want the SAME container %s", got, id)
	}
	passEvents := d.fakeDocker.order()[eventsBefore:]
	restartAt, commitAt := -1, -1
	for i, event := range passEvents {
		switch event {
		case "restart:" + id:
			restartAt = i
		case "commit:" + id:
			commitAt = i
		}
	}
	if restartAt < 0 || commitAt < 0 || restartAt > commitAt {
		t.Fatalf("pass events = %v, want restart:%s before the new-generation "+
			"commit:%s", passEvents, id, id)
	}
	if countEventPrefix(passEvents, "start:") != 0 ||
		len(d.fakeDocker.stopRequestLog()) != stopsBefore {
		t.Fatalf("pass events = %v (stops %d -> %d), want a restart only "+
			"(no replacement, no stop)", passEvents, stopsBefore,
			len(d.fakeDocker.stopRequestLog()))
	}

	secondMarker, ok := d.markerState("fn", "svc", 0)
	if !ok || secondMarker.CommittedContainerID != id ||
		secondMarker.Generation == "" ||
		secondMarker.Generation == firstMarker.Generation {
		t.Fatalf("second marker = (%+v, %v), want the SAME container committed "+
			"under a NEW non-empty source generation (was %q)",
			secondMarker, ok, firstMarker.Generation)
	}
	if secondMarker.CandidateContainerID != "" ||
		secondMarker.CandidateAttempt != 0 ||
		!secondMarker.RetryNotBefore.IsZero() {
		t.Fatalf("second marker = %+v, want no candidate attempt or retry state "+
			"for a no-probe restart", secondMarker)
	}
	if len(second.Candidates) != 0 || !second.RetryNotBefore.IsZero() ||
		!second.StartupDeadline.IsZero() {
		t.Fatalf("second result = %+v, want no health candidate and no deadlines",
			second)
	}
	if writes := d.candidateWrites(); len(writes) != writesBefore+1 ||
		writes[len(writes)-1] != secondMarker {
		t.Fatalf("candidate-family writes = %+v, want exactly the new-generation "+
			"commit appended after %d", writes, writesBefore)
	}
	if prep := d.preparationWrites(); len(prep) != prepBefore {
		t.Fatalf("preparation writes = %d, want %d (an in-place restart under "+
			"the bypassed probe writes no preparation state)",
			len(prep), prepBefore)
	}

	// A repeated pass recognizes the applied source and changes nothing.
	third := healthApply(t, c, "fn", tmpl, "img-1")
	if third.Pending() || third.Changed {
		t.Fatalf("third result = %+v, want an unchanged terminal pass", third)
	}
	if got := d.restarter.restartCount(); got != 1 {
		t.Fatalf("restarts after the third pass = %d, want 1 (no repeated "+
			"restart)", got)
	}
	if got := len(d.candidateWrites()); got != writesBefore+1 {
		t.Fatalf("candidate-family writes after the third pass = %d, want %d",
			got, writesBefore+1)
	}
	if after, ok := d.markerState("fn", "svc", 0); !ok || after != secondMarker {
		t.Fatalf("marker after the third pass = (%+v, %v), want it byte-for-byte "+
			"unchanged", after, ok)
	}
}

// A mode transition flips which probe policy gates the commit in BOTH
// directions: turning HOT_RELOAD ON lets an in-place restart commit despite
// stale unhealthy/Ready=false probe state, while turning it OFF makes the
// declared probe effective again — the replacement must carry the expected
// probe identity AND be observed healthy before it may commit and retire the
// HOT_RELOAD-era generation.
func TestEffectiveProbeTransitionsGateCommits(t *testing.T) {
	hc := testServiceHealthcheck(time.Second)
	probeLabel := runtime.ServiceHealthcheckLabel(hc)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		Healthcheck: hc,
	})

	t.Run("normal to HOT_RELOAD commits despite stale probe state", func(t *testing.T) {
		d := newHotReloadProbeDocker()
		c := NewServiceReconciler(d, nil, testutil.DiscardLogger(),
			testReconcileTimeout)

		// Given: normal mode — the probe-labeled replica starts healthy and
		// commits its first generation.
		first := healthApply(t, c, "fn", tmpl, "img-1")
		if first.Err != nil || first.Pending() {
			t.Fatalf("first result = %+v, want terminal convergence", first)
		}
		id := onlyContainerID(t, d.fakeDocker, "fn", "service.js")
		firstMarker, ok := d.markerState("fn", "service.js", 0)
		if !ok || firstMarker.CommittedContainerID != id ||
			firstMarker.Generation == "" {
			t.Fatalf("first marker = (%+v, %v), want %q committed",
				firstMarker, ok, id)
		}
		ctr, _ := d.containerByID(id)
		if ctr.healthcheck != probeLabel || ctr.health != container.Healthy {
			t.Fatalf("container = %+v, want the expected probe identity and a "+
				"healthy verdict in normal mode", ctr)
		}
		writesBefore := len(d.candidateWrites())
		prepBefore := len(d.preparationWrites())

		// When: the app turns HOT_RELOAD while the replica reports stale
		// unhealthy state and an explicit Ready=false.
		d.setMountIdentity("service.js", "fp1")
		d.setSourceMounted(true)
		d.fakeDocker.mu.Lock()
		d.fakeDocker.ctrs[id].health = container.Unhealthy
		d.fakeDocker.ctrs[id].ready = boolPtr(false)
		d.fakeDocker.mu.Unlock()
		d.script(id, probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Unhealthy,
		}})

		second := healthApply(t, c, "fn", tmpl, "img-1")

		// Then: the source-only restart commits the new generation although
		// every probe-era signal still says unhealthy.
		if second.Err != nil || second.Pending() {
			t.Fatalf("second result = %+v, want terminal convergence under the "+
				"effective no-probe policy", second)
		}
		if got := d.restarter.restartCount(); got != 1 {
			t.Fatalf("restarts = %d, want 1 (in-place source reload)", got)
		}
		if got := onlyContainerID(t, d.fakeDocker, "fn", "service.js"); got != id {
			t.Fatalf("container id = %s, want the SAME container %s", got, id)
		}
		secondMarker, ok := d.markerState("fn", "service.js", 0)
		if !ok || secondMarker.CommittedContainerID != id ||
			secondMarker.Generation == "" ||
			secondMarker.Generation == firstMarker.Generation {
			t.Fatalf("second marker = (%+v, %v), want the same container under a "+
				"new generation", secondMarker, ok)
		}
		if len(second.Candidates) != 0 || !second.RetryNotBefore.IsZero() ||
			!second.StartupDeadline.IsZero() {
			t.Fatalf("second result = %+v, want no health candidate and no "+
				"deadlines", second)
		}
		if got := len(d.candidateWrites()); got != writesBefore+1 {
			t.Fatalf("candidate-family writes = %d, want %d (exactly the "+
				"new-generation commit)", got, writesBefore+1)
		}
		if got := len(d.preparationWrites()); got != prepBefore {
			t.Fatalf("preparation writes = %d, want %d (the bypassed probe "+
				"writes no preparation state)", got, prepBefore)
		}
		if log := d.probeLog(); !slices.Equal(log, []string{id, id, id}) {
			t.Fatalf("probe log = %v, want [%s %s %s] (the normal-mode pass's "+
				"post-start observation and final revalidation, plus the "+
				"no-probe pass's final running revalidation)", log, id, id, id)
		}
	})

	t.Run("HOT_RELOAD to normal requires probe identity and health", func(t *testing.T) {
		d := newHotReloadProbeDocker()
		d.setMountIdentity("service.js", "fp1")
		d.setSourceMounted(true)
		c := NewServiceReconciler(d, nil, testutil.DiscardLogger(),
			testReconcileTimeout)

		// Given: HOT_RELOAD convergence — the unlabeled replica commits under
		// the nil policy.
		first := healthApply(t, c, "fn", tmpl, "img-1")
		if first.Err != nil || first.Pending() {
			t.Fatalf("first result = %+v, want terminal convergence", first)
		}
		oldID := onlyContainerID(t, d.fakeDocker, "fn", "service.js")
		firstMarker, ok := d.markerState("fn", "service.js", 0)
		if !ok || firstMarker.CommittedContainerID != oldID {
			t.Fatalf("first marker = (%+v, %v), want %q committed",
				firstMarker, ok, oldID)
		}
		if ctr, _ := d.containerByID(oldID); ctr.healthcheck != "" {
			t.Fatalf("relay.healthcheck = %q, want empty under HOT_RELOAD",
				ctr.healthcheck)
		}

		// When: the mount disappears — the declared probe becomes effective
		// again. The replacement starts carrying the expected identity, but
		// Docker still reports it STARTING at the post-start observation.
		d.setMountIdentity("service.js", "")
		d.setSourceMounted(false)
		newID := "id-2"
		d.script(newID, probeStep{obs: runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Starting,
		}})

		second := healthApply(t, c, "fn", tmpl, "img-1")

		// Then: no commit yet — a starting candidate holds the slot, the
		// HOT_RELOAD-era generation keeps serving, and the pass reports the
		// probe service's soft re-observation wake (never a hard retry).
		if !second.Pending() {
			t.Fatalf("second result = %+v, want pending until the replacement "+
				"is observed healthy", second)
		}
		if !second.RetryNotBefore.IsZero() {
			t.Fatalf("RetryNotBefore = %v, want zero: starting installs no hard "+
				"retry window", second.RetryNotBefore)
		}
		if !second.StartupDeadline.After(time.Now()) ||
			second.StartupDeadline.After(time.Now().Add(hc.Interval+time.Second)) {
			t.Fatalf("StartupDeadline = %v, want the soft re-observation window "+
				"within %s", second.StartupDeadline, hc.Interval)
		}
		if len(second.Candidates) != 1 ||
			second.Candidates[0].ContainerID != newID {
			t.Fatalf("candidates = %+v, want exactly the starting replacement %s",
				second.Candidates, newID)
		}
		still, ok := d.markerState("fn", "service.js", 0)
		if !ok || still.CommittedContainerID != oldID ||
			still.Generation != firstMarker.Generation {
			t.Fatalf("marker after the refused commit = (%+v, %v), want the "+
				"HOT_RELOAD-era generation still committed", still, ok)
		}
		if stops := d.fakeDocker.stopRequestLog(); len(stops) != 0 {
			t.Fatalf("stop requests = %v, want none while the replacement is "+
				"starting", stops)
		}
		if got := d.fakeDocker.runningCount("fn", "service.js"); got != 2 {
			t.Fatalf("running containers = %d, want 2 (old generation preserved, "+
				"candidate starting)", got)
		}
		replacement, ok := d.containerByID(newID)
		if !ok || replacement.healthcheck != probeLabel {
			t.Fatalf("replacement = (%+v, %v), want the expected probe identity "+
				"%q stamped in normal mode", replacement, ok, probeLabel)
		}

		// And: once the replacement reports healthy, the pass commits it BEFORE
		// retiring the HOT_RELOAD-era generation.
		third := healthApply(t, c, "fn", tmpl, "img-1")
		if third.Err != nil || third.Pending() || !third.Changed {
			t.Fatalf("third result = %+v, want terminal convergence", third)
		}
		committed, ok := d.markerState("fn", "service.js", 0)
		if !ok || committed.CommittedContainerID != newID ||
			committed.Generation == "" ||
			committed.Generation == firstMarker.Generation {
			t.Fatalf("third marker = (%+v, %v), want %q committed under the "+
				"normal-mode generation", committed, ok, newID)
		}
		commitAt := d.indexOfEvent("commit:" + newID)
		stopAt := d.indexOfEvent("stop:" + oldID)
		if commitAt < 0 || stopAt < 0 || commitAt > stopAt {
			t.Fatalf("events = %v, want commit:%s before stop:%s",
				d.fakeDocker.order(), newID, oldID)
		}
		if got := d.fakeDocker.runningCount("fn", "service.js"); got != 1 {
			t.Fatalf("running containers = %d, want 1 (the committed "+
				"replacement)", got)
		}
		if len(third.Candidates) != 0 || !third.RetryNotBefore.IsZero() ||
			!third.StartupDeadline.IsZero() {
			t.Fatalf("third result = %+v, want no candidates and no deadlines "+
				"once committed", third)
		}
		if log := d.probeLog(); !slices.Equal(log,
			[]string{oldID, newID, newID, newID}) {
			t.Fatalf("probe log = %v, want [%s %s %s %s] (each pass's final "+
				"revalidation plus the healthy revalidation and final check)",
				log, oldID, newID, newID, newID)
		}
	})
}

// Startup reconstruction follows the EFFECTIVE probe policy too: a historical
// uncommitted probe-labeled container of a HOT_RELOAD app must not be
// re-registered as a health candidate. The worker starts its health observer
// only AFTER app preparation has installed the accepted generations'
// source-mount state, so recovery — and every observer round built on it —
// must evaluate the no-probe policy from its very first look: nothing is
// registered, nothing is polled, and no health-driven Notify fires even when
// the container's native Docker health later turns unhealthy.
func TestRecoverServiceHealthCandidatesFollowsEffectiveProbePolicy(t *testing.T) {
	seedHistorical := func() (*probeDocker, *ServiceReconciler) {
		hc := testServiceHealthcheck(time.Second)
		f := newFakeDocker()
		pd := newProbeDocker(f)
		// Uncommitted (no marker), probe-labeled, still starting: the exact
		// shape a restart discovers after a normal-mode era.
		seedConvergedHealthService(f, "hist-1", "fn", "service.js", "img-1",
			80, 0, hc, container.Starting, 1)
		c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
			testReconcileTimeout)
		return pd, c
	}
	idsOf := func(candidates []runtime.ServiceHealthCandidate) []string {
		ids := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			ids = append(ids, candidate.ContainerID)
		}
		return ids
	}

	t.Run("source-mounted app recovers no candidates", func(t *testing.T) {
		pd, c := seedHistorical()

		pd.setSourceMounted(true)
		got, err := c.RecoverServiceHealthCandidates(context.Background())
		if err != nil {
			t.Fatalf("recover: %v", err)
		}
		if ids := idsOf(got); len(ids) != 0 {
			t.Fatalf("recovered candidates = %v, want none: the effective policy "+
				"for a HOT_RELOAD app is no-probe", ids)
		}

		// Control: without the source mount the same container is a legitimate
		// recovery target, so the exclusion follows the mount fact alone.
		pd.setSourceMounted(false)
		got, err = c.RecoverServiceHealthCandidates(context.Background())
		if err != nil {
			t.Fatalf("recover (control): %v", err)
		}
		if ids := idsOf(got); !slices.Equal(ids, []string{"hist-1"}) {
			t.Fatalf("recovered candidates = %v, want [hist-1] in normal mode",
				ids)
		}
	})

	t.Run("observer never polls the source-mounted app's historical container",
		func(t *testing.T) {
			pd, c := seedHistorical()
			// The worker starts the health observer only AFTER app preparation
			// has installed the accepted generations' source-mount state, so the
			// FIRST recovery round already evaluates the no-probe policy: the
			// historical probe-labeled container is never re-registered.
			pd.setSourceMounted(true)
			notify, count := notifyCounter()
			o := NewServiceHealthObserver(pd,
				func() []runtime.ServiceHealthCandidate { return nil },
				c.RecoverServiceHealthCandidates, notify,
				testutil.DiscardLogger())
			o.ctx, o.cancel = context.WithCancel(context.Background())
			defer o.cancel()

			o.observeOnce()
			o.observeOnce()
			// A native Docker health transition must not produce observer work
			// or a health-driven Notify either.
			pd.setHealth("hist-1", container.Unhealthy)
			o.observeOnce()

			if got := pd.probeCalls(); got != 0 {
				t.Fatalf("probe calls = %d, want 0: recovery under the no-probe "+
					"policy registers nothing to poll", got)
			}
			if got := count(); got != 0 {
				t.Fatalf("notify count = %d, want 0: a health-driven hint must "+
					"never fire for a no-probe service", got)
			}
		})
}

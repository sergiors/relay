package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// routedNoProbeTemplate declares a ROUTED service with no healthcheck: its
// effective probe is nil, and with WithServiceBindAddr wired its replica must
// actually publish on the configured host address before it may be committed.
func routedNoProbeTemplate() *app.Template {
	return serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80,
		Host: "web.test", Replicas: 1,
	})
}

// seedServingRoutedGeneration seeds one RUNNING routed replica that is
// genuinely serving: its bind label AND the daemon's actual host binding both
// name bindAddr, so it is the previous generation worth preserving.
func seedServingRoutedGeneration(f *fakeDocker, id, image, bindAddr string) {
	seedOldService(f, id, "fn", "service.js", image, 80, 0)
	f.ctrs[id].bindAddr = bindAddr
	f.ctrs[id].boundAddr = bindAddr
}

// seedUnboundRoutedCandidate seeds the regression's markerless,
// config-matching no-healthcheck ROUTED candidate: its relay.bind_addr label
// matches the configured publication address, while actualBinding is what the
// daemon REALLY reports ("" = no mapping materialized, another address = a
// binding elsewhere). Only the probe observation's published-binding facts can
// prove the difference.
func seedUnboundRoutedCandidate(
	f *fakeDocker, actualBinding string,
) {
	seedConvergedNoProbeService(f, "existing-1", "fn", "service.js",
		"img-1", 80, 0)
	f.ctrs["existing-1"].bindAddr = "127.0.0.1"
	f.ctrs["existing-1"].boundAddr = actualBinding
}

// Given a config-matching, markerless running no-healthcheck ROUTED container
// whose relay.bind_addr label matches the configured publication address but
// whose ACTUAL Docker host binding is missing or on another address, when a
// pass runs (through the probe seam, whose observation carries the published
// binding facts), then the replica is NOT committed: no marker is written, the
// previous committed generation survives byte-for-byte and still running, the
// unbound replica is neither replaced nor stopped, and — having no effective
// probe — the refused pass schedules no healthcheck candidate or
// retry/startup deadline. The label is a claim; only the daemon's real binding
// may authorize the commit.
func TestNoProbeRoutedReplicaWithoutActualBindingIsNotCommitted(t *testing.T) {
	for _, tc := range []struct {
		name          string
		actualBinding string
	}{
		{name: "missing host binding", actualBinding: ""},
		{name: "binding on another address", actualBinding: "10.0.0.9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeDocker()
			pd := newProbeDocker(f)
			// Given: the previous generation is committed and actually serving
			// on the configured address.
			seedServingRoutedGeneration(f, "old-1", "img-old", "127.0.0.1")
			pd.seedMarker(runtime.ServiceConvergenceState{
				App: "fn", Service: "service.js", Replica: 0,
				CommittedContainerID: "old-1", Generation: "gen-old",
				UpdatedAt: time.Now(),
			})
			markerBefore, ok := pd.markerState("fn", "service.js", 0)
			if !ok {
				t.Fatal("precondition: the previous generation's marker must exist")
			}
			// And: the label-claimed but unbound candidate.
			seedUnboundRoutedCandidate(f, tc.actualBinding)
			c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
				testReconcileTimeout, WithServiceBindAddr("127.0.0.1"))

			result := c.ApplyResult(context.Background(), "fn",
				routedNoProbeTemplate(), "img-1", nil, nil)

			if !result.Pending() {
				t.Fatalf("result = %+v, want PENDING: a bind label without the "+
					"actual host binding must never be committed", result)
			}
			if len(result.Candidates) != 0 || !result.RetryNotBefore.IsZero() ||
				!result.StartupDeadline.IsZero() {
				t.Fatalf("result = %+v, want no healthcheck candidate and no "+
					"retry/startup deadline for a no-probe service", result)
			}
			marker, ok := pd.markerState("fn", "service.js", 0)
			if !ok || marker != markerBefore {
				t.Fatalf("committed marker = (%+v, %v), want the previous "+
					"generation preserved byte-for-byte: %+v",
					marker, ok, markerBefore)
			}
			if writes := pd.candidateWrites(); len(writes) != 0 {
				t.Fatalf("candidate-family writes = %+v, want none (the refused "+
					"replica commits nothing)", writes)
			}
			events := f.order()
			if countEventPrefix(events, "start:") != 0 {
				t.Fatalf("events = %v, want no start: refusing the unbound "+
					"candidate must not trigger a replacement", events)
			}
			if stops := f.stopRequestLog(); len(stops) != 0 {
				t.Fatalf("stop requests = %v, want none: the previous generation "+
					"keeps serving", stops)
			}
			for _, id := range []string{"old-1", "existing-1"} {
				ctr, ok := f.containerByID(id)
				if !ok || ctr.state != container.StateRunning {
					t.Fatalf("container %s = (%+v, %v), want it still running",
						id, ctr, ok)
				}
			}
			if got := f.runningCount("fn", "service.js"); got != 2 {
				t.Fatalf("running containers = %d, want 2 (both generations "+
					"preserved for the next pass)", got)
			}
		})
	}

	t.Run("no previous generation", func(t *testing.T) {
		f := newFakeDocker()
		pd := newProbeDocker(f)
		seedUnboundRoutedCandidate(f, "")
		c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
			testReconcileTimeout, WithServiceBindAddr("127.0.0.1"))

		result := c.ApplyResult(context.Background(), "fn",
			routedNoProbeTemplate(), "img-1", nil, nil)

		if !result.Pending() {
			t.Fatalf("result = %+v, want pending while nothing may be committed",
				result)
		}
		if _, ok := pd.markerState("fn", "service.js", 0); ok {
			t.Fatal("no committed marker may exist for an unbound replica")
		}
		if writes := pd.candidateWrites(); len(writes) != 0 {
			t.Fatalf("candidate-family writes = %+v, want none", writes)
		}
		if events := f.order(); len(events) != 0 {
			t.Fatalf("events = %v, want none (no start, no stop)", events)
		}
		if ctr, ok := f.containerByID("existing-1"); !ok ||
			ctr.state != container.StateRunning {
			t.Fatalf("existing-1 = (%+v, %v), want it preserved for "+
				"re-evaluation", ctr, ok)
		}
	})
}

// Given the same markerless config-matching routed replica whose ACTUAL Docker
// host binding verifies on the configured address, when a pass runs, then it is
// ADOPTED and committed — the previous generation retires only after the
// durable commit — and no health candidate or deadline is produced.
func TestNoProbeRoutedReplicaWithVerifiedBindingIsAdoptedAndCommitted(t *testing.T) {
	f := newFakeDocker()
	pd := newProbeDocker(f)
	seedServingRoutedGeneration(f, "old-1", "img-old", "127.0.0.1")
	seedUnboundRoutedCandidate(f, "127.0.0.1")
	c := NewServiceReconciler(pd, nil, testutil.DiscardLogger(),
		testReconcileTimeout, WithServiceBindAddr("127.0.0.1"))

	result := c.ApplyResult(context.Background(), "fn",
		routedNoProbeTemplate(), "img-1", nil, nil)

	assertNoStartService(t, f, "verified-binding adoption")
	assertNoProbeCommit(t, f, pd.markerStoreDocker, result, "fn",
		"service.js", "existing-1")
	commitAt := f.indexOfEvent("commit:existing-1")
	stopAt := f.indexOfEvent("stop:old-1")
	if commitAt < 0 || stopAt < 0 || commitAt > stopAt {
		t.Fatalf("events = %v, want commit:existing-1 before stop:old-1",
			f.order())
	}
	if _, ok := f.containerByID("old-1"); ok {
		t.Fatal("the previous generation survived despite the verified commit")
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running containers = %d, want 1 (the committed replica)", got)
	}
}

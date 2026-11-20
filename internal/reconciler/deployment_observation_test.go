package reconciler

import (
	"errors"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/state"
)

// TestUnchangedObservationFailureReportsDegradedThenActive pins the unchanged
// app's deferred activation contract: with a status-capable service observer,
// an unchanged app that declares services publishes NO deployment phase until
// its periodic observation concludes. A terminal observation failure marks the
// deployment degraded with the failure detail — the available generation still
// serves, so it is never failed and never active — and a later successful
// observation restores active. A pending observation must not publish active.
func TestUnchangedObservationFailureReportsDegradedThenActive(t *testing.T) {
	root := t.TempDir()
	dir := writeServiceAppDir(t, root, "svc-obs", "v1")
	fn := servicePreparedApp(t, "svc-obs", dir, "img-svc-obs")
	b := &fakeBuilder{}

	var progress, drifts *[]deploymentEvent
	var captured []ServiceRequestHooks
	r, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.UpdateServicesObservationWithLifecycle = func(
			_ string, _ *app.Template, _ string, hooks ServiceRequestHooks,
		) {
			captured = append(captured, hooks)
		}
	})
	authorizeServiceApp(t, r, root, "svc-obs")

	// given an unchanged, available app whose observation has not concluded
	r.reconcileApp("svc-obs")

	if len(captured) != 1 {
		t.Fatalf("captured %d observation hook sets, want 1", len(captured))
	}
	if len(*progress) != 0 {
		t.Fatalf("progress = %+v, want none before the observation concludes (a pending "+
			"observation must not publish active)", *progress)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (an unchanged app is never rebuilt)", b.prepares())
	}

	// when the observation fails terminally
	captured[0].OnComplete(errors.New("service backend down"))

	// then the deployment is degraded with the failure detail — not failed,
	// not active, not drifted
	if !containsEvent(*progress, deploymentEvent{
		name: "svc-obs", phase: state.DeploymentAppDegraded, detail: "service backend down",
	}) {
		t.Fatalf("progress = %+v, want the degraded phase carrying the failure detail", *progress)
	}
	if hasProgressPhase(*progress, "svc-obs", state.DeploymentAppActive) {
		t.Fatalf("progress = %+v, want no active phase for the failed observation", *progress)
	}
	if hasProgressPhase(*progress, "svc-obs", state.DeploymentAppFailed) {
		t.Fatalf("progress = %+v, want degraded (the available generation serves), not failed", *progress)
	}
	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none", *drifts)
	}

	// when a later periodic pass observes the unchanged app again
	r.reconcileApp("svc-obs")

	if len(captured) != 2 {
		t.Fatalf("captured %d observation hook sets, want 2", len(captured))
	}
	if hasProgressPhase(*progress, "svc-obs", state.DeploymentAppActive) {
		t.Fatalf("progress = %+v, the new observation must conclude before active is restored", *progress)
	}

	// and that observation succeeds
	captured[1].OnComplete(nil)

	last, ok := lastProgressFor(*progress, "svc-obs")
	if !ok || last.phase != state.DeploymentAppActive || last.detail != "" {
		t.Fatalf("last progress = (%+v, %v), want active restored by the successful observation",
			last, ok)
	}
}

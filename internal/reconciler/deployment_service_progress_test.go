package reconciler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

// deploymentServiceTemplate is a valid app with one entrypoint service, so a
// reconcile reaches the post-publication service-convergence callbacks.
const deploymentServiceTemplate = `runtime: python3.14
services:
  - name: svc
    entrypoint: service.py
    port: 8000
    replicas: 1
`

// writeServiceAppDir writes a service-bearing app at a known content version
// and returns its directory.
func writeServiceAppDir(t *testing.T, root, name, version string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(deploymentServiceTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "version.txt"),
		[]byte(version), 0o644); err != nil {
		t.Fatalf("write version: %v", err)
	}
	return dir
}

// servicePreparedApp builds an active registry entry for the service template.
func servicePreparedApp(t *testing.T, name, dir, image string) *runner.PreparedApp {
	t.Helper()
	return runner.NewPrepared(
		app.App{Name: name, Dir: dir, Template: mustParse(deploymentServiceTemplate)},
		&runtime.Prepared{Name: name, Image: image},
		&fakeBuilder{},
	)
}

// lastProgressFor returns the last recorded progress event for name.
func lastProgressFor(events []deploymentEvent, name string) (deploymentEvent, bool) {
	var last deploymentEvent
	found := false
	for _, e := range events {
		if e.name == name {
			last = e
			found = true
		}
	}
	return last, found
}

// hasProgressPhase reports whether any progress event for name carries phase.
func hasProgressPhase(events []deploymentEvent, name, phase string) bool {
	for _, e := range events {
		if e.name == name && e.phase == phase {
			return true
		}
	}
	return false
}

// authorizeServiceApp installs generation-1 cluster authorization matching the
// app's current on-disk content.
func authorizeServiceApp(t *testing.T, r *Reconciler, root, name string) {
	t.Helper()
	fp, tid := authorizationFor(t, root, name)
	r.SetDeploymentAuthorization(1,
		map[string]string{name: fp},
		map[string]string{name: tid}, nil)
}

// failingServiceHook returns an UpdateServicesWithStatus callback whose
// convergence always fails with cause after real corrective work started.
func failingServiceHook(cause error) func(
	string, *app.Template, string, func(), func(error),
) {
	return func(
		_ string, _ *app.Template, _ string,
		onReconcileStart func(), onComplete func(error),
	) {
		onReconcileStart()
		onComplete(cause)
	}
}

// TestServiceConvergenceFailureReportsTerminalPhase pins that a failed
// service-convergence callback is TERMINAL for deployment progress — the
// cluster status never remains stuck at activating. With a usable previous
// generation the report is degraded (the prior version still serves); with
// nothing usable it is failed. Readiness/active is never reported.
func TestServiceConvergenceFailureReportsTerminalPhase(t *testing.T) {
	t.Run("usable prior generation reports degraded", func(t *testing.T) {
		root := t.TempDir()
		dir := writeServiceAppDir(t, root, "svcapp", "v1")

		fn := servicePreparedApp(t, "svcapp", dir, "img-svc-g1")
		b := &fakeBuilder{}
		var progress, drifts *[]deploymentEvent
		r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
			cfg.UpdateServicesWithStatus = failingServiceHook(errors.New("service boom"))
		})

		// New materialized content makes this pass prepare and publish a new
		// generation before service convergence runs and fails.
		if err := os.WriteFile(filepath.Join(dir, "version.txt"),
			[]byte("v2"), 0o644); err != nil {
			t.Fatalf("write v2: %v", err)
		}
		authorizeServiceApp(t, r, root, "svcapp")

		r.reconcileApp("svcapp")

		if !hasProgressPhase(*progress, "svcapp", state.DeploymentAppActivating) {
			t.Fatalf("progress = %+v, want the activating report before services",
				*progress)
		}
		last, ok := lastProgressFor(*progress, "svcapp")
		if !ok || last.phase != state.DeploymentAppDegraded ||
			last.detail != "service boom" {
			t.Fatalf("last progress = (%+v, %v), want terminal degraded with the "+
				"service error — never left at activating", last, ok)
		}
		if hasProgressPhase(*progress, "svcapp", state.DeploymentAppActive) {
			t.Fatalf("progress = %+v, want no active report for a failed convergence",
				*progress)
		}
		if len(*drifts) != 0 {
			t.Fatalf("drift reports = %+v, want none", *drifts)
		}
		if got := reg.GetByName("svcapp"); got == nil || got.Prepared() == nil {
			t.Fatalf("registry = %+v, want the published generation serving", got)
		}
	})

	t.Run("no usable generation reports failed", func(t *testing.T) {
		root := t.TempDir()
		writeServiceAppDir(t, root, "svcapp", "v1")

		b := &fakeBuilder{}
		var progress, drifts *[]deploymentEvent
		r, _ := newTestReconciler(t, root, b, nil, func(cfg *Config) {
			progress, drifts = deploymentRecorder(cfg)
			cfg.UpdateServicesWithStatus = failingServiceHook(errors.New("service boom"))
		})
		authorizeServiceApp(t, r, root, "svcapp")

		r.reconcileApp("svcapp")

		last, ok := lastProgressFor(*progress, "svcapp")
		if !ok || last.phase != state.DeploymentAppFailed ||
			last.detail != "service boom" {
			t.Fatalf("last progress = (%+v, %v), want terminal failed with the "+
				"service error — never left at activating", last, ok)
		}
		if hasProgressPhase(*progress, "svcapp", state.DeploymentAppActive) {
			t.Fatalf("progress = %+v, want no active report", *progress)
		}
		if len(*drifts) != 0 {
			t.Fatalf("drift reports = %+v, want none", *drifts)
		}
	})
}

// TestServiceConvergenceCancellationStaysActivating pins that a cancelled
// service convergence (lifecycle shutdown) is retryable, not a terminal
// outcome: no active/ready, degraded, or failed report is emitted — the last
// progress for the app remains activating.
func TestServiceConvergenceCancellationStaysActivating(t *testing.T) {
	root := t.TempDir()
	dir := writeServiceAppDir(t, root, "svcapp", "v1")

	fn := servicePreparedApp(t, "svcapp", dir, "img-svc-g1")
	b := &fakeBuilder{}
	var progress, drifts *[]deploymentEvent

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	t.Cleanup(cancelLifecycle)

	r, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		progress, drifts = deploymentRecorder(cfg)
		cfg.UpdateServicesWithStatus = func(
			_ string, _ *app.Template, _ string,
			onReconcileStart func(), onComplete func(error),
		) {
			onReconcileStart()
			// Shutdown lands while services converge: the reconciler lifecycle
			// is cancelled and the callback observes context.Canceled.
			cancelLifecycle()
			onComplete(context.Canceled)
		}
	})
	// The cancellation branch requires the lifecycle context to be cancelled
	// when the completion fires (rctx), mirroring PrepareWatch/Start wiring.
	r.ctx = lifecycle

	if err := os.WriteFile(filepath.Join(dir, "version.txt"),
		[]byte("v2"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	authorizeServiceApp(t, r, root, "svcapp")

	r.reconcileApp("svcapp")

	if !hasProgressPhase(*progress, "svcapp", state.DeploymentAppActivating) {
		t.Fatalf("progress = %+v, want the activating report before services",
			*progress)
	}
	last, ok := lastProgressFor(*progress, "svcapp")
	if !ok || last.phase != state.DeploymentAppActivating {
		t.Fatalf("last progress = (%+v, %v), want the app left activating "+
			"(retryable) after a cancelled convergence", last, ok)
	}
	for _, forbidden := range []string{
		state.DeploymentAppActive,
		state.DeploymentAppDegraded,
		state.DeploymentAppFailed,
	} {
		if hasProgressPhase(*progress, "svcapp", forbidden) {
			t.Fatalf("progress = %+v, want no %q report for a cancellation",
				*progress, forbidden)
		}
	}
	if len(*drifts) != 0 {
		t.Fatalf("drift reports = %+v, want none", *drifts)
	}
}

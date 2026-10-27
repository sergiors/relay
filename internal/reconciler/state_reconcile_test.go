package reconciler

import (
	"os"
	"strings"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/runner"
)

// TestReconcileRemovalDeletesMetricsSeries wires a metrics.Registry through the
// RemoveApp hook the worker builds (RemoveApp: m.RemoveApp(...) +
// image retirement), so a removed app's Prometheus series are dropped at
// reconciliation time while an unrelated app's series and the globals stay
// untouched. This is the reconciler-package view of the production hook wiring;
// internal/worker owns the actual closure.
func TestReconcileRemovalDeletesMetricsSeries(t *testing.T) {
	root := t.TempDir()
	victimDir := writeFnDir(t, root, "victim")
	bystanderDir := writeFnDir(t, root, "bystander")

	victim := initialFn("victim", victimDir)
	bystander := initialFn("bystander", bystanderDir)
	b := &fakeBuilder{}

	m := metrics.New()
	// Seed per-app series for both apps, mirroring prior activity.
	seedMetricsApp(m, "victim")
	seedMetricsApp(m, "bystander")
	// A global counter the removal must never touch.
	m.Inc(metrics.MetricEventsReceived)
	before := m.Snapshot()
	if !strings.Contains(before, "app_events_matched_total{app=victim}") {
		t.Fatalf("expected victim series before removal:\n%s", before)
	}
	if !strings.Contains(before, "app_events_matched_total{app=bystander}") {
		t.Fatalf("expected bystander series before removal:\n%s", before)
	}

	// Build the reconciler with the production-style RemoveApp hook: delete
	// the app's metrics series, then retire its images (a no-op with the
	// fake builder). The runner registry and state wiring mirror production.
	r, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{victim, bystander}, func(cfg *Config) {
		cfg.RemoveApp = func(name string) {
			m.RemoveApp(name)
		}
	})

	if err := os.RemoveAll(victimDir); err != nil {
		t.Fatalf("remove victim: %v", err)
	}
	r.reconcileApp("victim")

	got := m.Snapshot()
	// Victim's series are gone from every app-scoped vec.
	if strings.Contains(got, "app_events_matched_total{app=victim}") {
		t.Fatalf("victim series must be deleted on removal:\n%s", got)
	}
	if strings.Contains(got, "handler_invocations_total{app=victim,") {
		t.Fatalf("victim handler_invocations_total must be deleted:\n%s", got)
	}
	if strings.Contains(got, "app_build_seconds{app=victim}") {
		t.Fatalf("victim app_build_seconds must be deleted:\n%s", got)
	}
	// Bystander's series and the global counter survive.
	if !strings.Contains(got, "app_events_matched_total{app=bystander}") {
		t.Fatalf("bystander series must survive removal:\n%s", got)
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Fatalf("events_received_total = %d, want 1", got)
	}
}

// seedMetricsApp increments every app-carrying vec for name on the
// given registry, so an app has a full set of series for the removal test.
func seedMetricsApp(m *metrics.Registry, name string) {
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "app", Value: name}, {Name: "handler", Value: "x"}})
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "failure"}, {Name: "app", Value: name}, {Name: "handler", Value: "x"}})
	m.IncLabels(metrics.MetricAppBuildFailures, []metrics.Label{{Name: "app", Value: name}})
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: name}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "app", Value: name}})
	m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: name}})
	m.IncLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "app", Value: name}})
	m.IncLabels(metrics.MetricFunctionDLQ, []metrics.Label{{Name: "app", Value: name}})
	m.ObserveDurationLabels(metrics.MetricHandlerDuration, []metrics.Label{{Name: "app", Value: name}, {Name: "handler", Value: "x"}}, time.Millisecond)
	m.ObserveDurationLabels(metrics.MetricAppBuild, []metrics.Label{{Name: "app", Value: name}}, time.Millisecond)
}

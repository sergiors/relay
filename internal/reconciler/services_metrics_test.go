package reconciler

import (
	"context"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/routing"
	"relay/internal/testutil"
)

// TestServiceReconcilerMetricsChanged verifies a corrective pass records a
// changed outcome and one duration observation for the app, exactly once.
func TestServiceReconcilerMetricsChanged(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	m := metrics.New()

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout, WithMetrics(m))
	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err != nil {
		t.Fatalf("apply: %v", err)
	}

	if got := m.CounterLabels(metrics.MetricServiceReconciles, []metrics.Label{{Name: "app", Value: "fn"}, {Name: "outcome", Value: metrics.ServiceOutcomeChanged}}); got != 1 {
		t.Fatalf("changed = %d, want 1", got)
	}
	count, sum := m.HistogramLabels(metrics.MetricServiceReconcileDuration, []metrics.Label{{Name: "app", Value: "fn"}})
	if count != 1 {
		t.Fatalf("duration observations = %d, want 1", count)
	}
	if sum < 0 {
		t.Fatalf("duration sum = %v, want >= 0", sum)
	}
}

// TestServiceReconcilerMetricsUnchanged verifies a fully-converged verification
// pass records an unchanged outcome (no double counting of changed).
func TestServiceReconcilerMetricsUnchanged(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	m := metrics.New()
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout, WithMetrics(m))

	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err != nil {
		t.Fatalf("apply 2: %v", err)
	}

	if got := m.CounterLabels(metrics.MetricServiceReconciles, []metrics.Label{{Name: "app", Value: "fn"}, {Name: "outcome", Value: metrics.ServiceOutcomeChanged}}); got != 1 {
		t.Fatalf("changed = %d, want 1 (second pass is a no-op)", got)
	}
	if got := m.CounterLabels(metrics.MetricServiceReconciles, []metrics.Label{{Name: "app", Value: "fn"}, {Name: "outcome", Value: metrics.ServiceOutcomeUnchanged}}); got != 1 {
		t.Fatalf("unchanged = %d, want 1", got)
	}
	if count, _ := m.HistogramLabels(metrics.MetricServiceReconcileDuration, []metrics.Label{{Name: "app", Value: "fn"}}); count != 2 {
		t.Fatalf("duration observations = %d, want 2 (one per pass)", count)
	}
}

// TestServiceReconcilerMetricsError verifies a pass that returns an error
// records an error outcome, and that the failure and success series are
// distinct — no raw error label.
func TestServiceReconcilerMetricsError(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	f.resolveErr["service.js"] = errServiceConverge
	m := metrics.New()

	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout, WithMetrics(m))
	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err == nil {
		t.Fatal("apply: err = nil, want the resolve failure surfaced")
	}

	if got := m.CounterLabels(metrics.MetricServiceReconciles, []metrics.Label{{Name: "app", Value: "fn"}, {Name: "outcome", Value: metrics.ServiceOutcomeError}}); got != 1 {
		t.Fatalf("error = %d, want 1", got)
	}
	if count, _ := m.HistogramLabels(metrics.MetricServiceReconcileDuration, []metrics.Label{{Name: "app", Value: "fn"}}); count != 1 {
		t.Fatalf("duration observations = %d, want 1", count)
	}
	if s := m.Snapshot(); strings.Contains(s, "error=") {
		t.Fatalf("service metrics must not carry an error label:\n%s", s)
	}
}

// TestServiceReconcilerMetricsNilRegistryNoPanic pins that a reconciler with no
// metrics wiring (the default, existing callers) works exactly as before.
func TestServiceReconcilerMetricsNilRegistryNoPanic(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	c := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := c.Apply(context.Background(), "fn", tmpl, "img", nil); err != nil {
		t.Fatalf("apply 2: %v", err)
	}
}

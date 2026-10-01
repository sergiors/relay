package metrics

import (
	"strings"
	"testing"
)

// TestAppIdentityMetricNamesAndLabels pins the telemetry naming boundary:
// app-lifecycle/engagement families keep the relay_app_ namespace and the "app"
// identity label, while the handler-execution counters use the relay_function_
// namespace (they count handler work) yet STILL carry the "app" label as their
// identity dimension. A regression here would break every dashboard and
// silently re-blur execution telemetry with app identity.
func TestAppIdentityMetricNamesAndLabels(t *testing.T) {
	r := New()
	r.IncLabels(MetricAppEventsMatched, []Label{{Name: "app", Value: "demo"}})
	r.IncLabels(MetricFunctionHandlerSuccess, []Label{{Name: "app", Value: "demo"}})
	r.IncLabels(MetricFunctionHandlerFailure, []Label{{Name: "app", Value: "demo"}})
	r.IncLabels(MetricFunctionRetries, []Label{{Name: "app", Value: "demo"}})
	r.IncLabels(MetricFunctionDLQ, []Label{{Name: "app", Value: "demo"}})
	r.IncLabels(MetricHandlerInvocations,
		[]Label{{Name: "outcome", Value: "success"}, {Name: "app", Value: "demo"}, {Name: "handler", Value: "index.run"}})
	r.ObserveDurationLabels(MetricAppBuild, []Label{{Name: "app", Value: "demo"}}, 1e9)
	r.IncLabels(MetricAppBuildFailures, []Label{{Name: "app", Value: "demo"}})
	r.SetAppStatus("demo", "ready")

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	byName := map[string]bool{}
	appLabeled := map[string]bool{}
	funcLabeled := map[string]bool{}
	for _, f := range families {
		name := f.GetName()
		byName[name] = true
		for _, m := range f.GetMetric() {
			if labelValue(m, "app") != "" {
				appLabeled[name] = true
			}
			if labelValue(m, "function") != "" {
				funcLabeled[name] = true
			}
		}
	}

	for _, want := range []string{
		"relay_app_events_matched_total",
		"relay_function_handler_success_total",
		"relay_function_handler_failure_total",
		"relay_function_retries_total",
		"relay_function_dlq_total",
		"relay_handler_invocations_total",
		"relay_app_build_seconds",
		"relay_app_build_failures_total",
		"relay_app_status",
	} {
		if !byName[want] {
			t.Errorf("missing canonical metric %q", want)
		}
	}

	// The handler-execution counters are function_-prefixed but app-labeled:
	// their identity dimension is the app, never a "function" label.
	for _, name := range []string{
		"relay_app_events_matched_total",
		"relay_function_handler_success_total",
		"relay_function_handler_failure_total",
		"relay_function_retries_total",
		"relay_function_dlq_total",
		"relay_handler_invocations_total",
		"relay_app_build_seconds",
		"relay_app_build_failures_total",
	} {
		if !appLabeled[name] {
			t.Errorf("metric %q must carry the app label", name)
		}
		if funcLabeled[name] {
			t.Errorf("metric %q must not carry the function label", name)
		}
	}
}

// TestStatusGaugeIsAppPrefixed pins that the one-hot lifecycle gauge is
// relay_app_status and uses the app/status label pair.
func TestStatusGaugeIsAppPrefixed(t *testing.T) {
	r := New()
	r.SetAppStatus("demo", "ready")
	s := r.Snapshot()
	if !strings.Contains(s, "app_status{app=demo,status=ready} value=1") {
		t.Fatalf("snapshot missing app_status one-hot series:\n%s", s)
	}
	if strings.Contains(s, "function_status") {
		t.Fatalf("snapshot must not expose the old function_status series:\n%s", s)
	}
}

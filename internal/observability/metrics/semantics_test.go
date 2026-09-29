package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSemanticsHelpExposedExactly pins that the HELP text a scrape actually
// receives for the four semantics-sensitive metrics is exactly metricHelp's
// text: the wording operators read on /metrics is the wording documented here,
// with no drift. The registry's metadata test enforces non-emptiness and type;
// this enforces the exact content of the four confusable families.
func TestSemanticsHelpExposedExactly(t *testing.T) {
	r := New()
	seedAllMetrics(r)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, name := range []string{MetricRetries, MetricFunctionRetries, MetricDLQEntries, MetricFunctionDLQ} {
		want := "# HELP " + name + " " + metricHelp[name] + "\n"
		if !strings.Contains(body, want) {
			t.Errorf("exposition HELP for %q does not match metricHelp exactly; want line:\n%s\ngot body:\n%s", name, want, body)
		}
	}
}

// TestSemanticsHelpDistinguishesRetriesAndDLQ pins the exact wording of the four
// metrics whose human-facing meaning is easiest to confuse, so a future edit
// cannot silently blur them:
//
//   - relay_retries_total is stream MESSAGE reclaims, not handler retries.
//   - relay_function_retries_total is handler retries.
//   - relay_dlq_entries_total is successful Redis DLQ entry writes.
//   - relay_function_dlq_total is the exhaustion commit, not a successful write.
//
// The wording (not just non-emptiness) is asserted because these distinctions
// are the contract operators read on a dashboard.
func TestSemanticsHelpDistinguishesRetriesAndDLQ(t *testing.T) {
	tests := []struct {
		name     string
		mustHave []string
	}{
		{
			name:     MetricRetries,
			mustHave: []string{"Message reclaims", "not handler retries", "function_retries_total"},
		},
		{
			name:     MetricFunctionRetries,
			mustHave: []string{"Handler retries", "not stream message reclaims"},
		},
		{
			name:     MetricDLQEntries,
			mustHave: []string{"Successful dead-letter writes", "XADD", "function_dlq_total"},
		},
		{
			name:     MetricFunctionDLQ,
			mustHave: []string{"counts", "exhaustion", "not a successful DLQ write", "dlq_entries_total"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			help, ok := metricHelp[tc.name]
			if !ok {
				t.Fatalf("metric %q has no HELP entry", tc.name)
			}
			for _, want := range tc.mustHave {
				if !strings.Contains(help, want) {
					t.Errorf("HELP for %q missing %q:\n%s", tc.name, want, help)
				}
			}
		})
	}

	// The two retry families must not describe the same thing.
	if metricHelp[MetricRetries] == metricHelp[MetricFunctionRetries] {
		t.Fatal("relay_retries_total and relay_function_retries_total must have distinct HELP")
	}
	// The two DLQ families must not describe the same thing.
	if metricHelp[MetricDLQEntries] == metricHelp[MetricFunctionDLQ] {
		t.Fatal("relay_dlq_entries_total and relay_function_dlq_total must have distinct HELP")
	}
}

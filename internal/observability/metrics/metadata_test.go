package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// seedAllMetrics writes at least one series to every registered collector so a
// Gather surfaces every family. Lazy collectors (CounterVecs, HistogramVecs, and
// GaugeVecs) emit no family until a series exists, so without this seeding a
// metadata audit would silently skip them.
func seedAllMetrics(r *Registry) {
	for _, name := range []string{
		MetricEventsReceived,
		MetricEventsMatched,
		MetricEventsUnmatched,
		MetricRetries,
		MetricDLQEntries,
		MetricHandlerSuccess,
		MetricHandlerFailure,
		MetricConcurrencyWaits,
		MetricScheduleOccurrencesPublished,
		MetricScheduleOccurrencesDuplicate,
		MetricSchedulePublishFailures,
		MetricSchedulePublishRetries,
		MetricSchedulePublishExhausted,
		MetricScheduleCatchUp,
		MetricSchedulePendingPersisted,
		MetricSchedulePendingRetries,
		MetricSchedulePendingExpired,
		MetricSchedulerDegraded,
		MetricSchedulerRecoveries,
		MetricMissingPayload,
		MetricEventsOversized,
	} {
		r.Inc(name)
	}
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "a"}, {"handler", "x"}})
	r.IncLabels(MetricAppBuildFailures, []Label{{"app", "a"}})
	for _, name := range []string{
		MetricAppEventsMatched,
		MetricFunctionHandlerSuccess,
		MetricFunctionHandlerFailure,
		MetricFunctionRetries,
		MetricFunctionDLQ,
		MetricRuntimeContainerWaits,
	} {
		r.IncLabels(name, []Label{{"app", "a"}})
	}
	// Duration histograms are observed with a known 2s value so the test can
	// assert the exposed sample sums are in seconds, not milliseconds.
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "a"}, {"handler", "x"}}, 2*time.Second)
	r.ObserveDurationLabels(MetricAppBuild, []Label{{"app", "a"}}, 2*time.Second)
	r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"app", "a"}}, 2*time.Second)
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"app", "a"}, {"outcome", RuntimeOutcomeWarm}})
	r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"app", "a"}, {"reason", "shutdown"}})
	r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"app", "a"}, {"state", RuntimeStateIdle}}, 1)
	r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"app", "a"}}, 1)
	r.SetGaugeLabels(MetricAppStatus, []Label{{"app", "a"}, {"status", "ready"}}, 1)
	r.SetGaugeLabels(MetricSchedulerState, []Label{{"state", SchedulerStateRunning}}, 1)
	r.IncLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpReadGroup}})
	r.IncLabels(MetricServiceReconciles, []Label{{"app", "a"}, {"outcome", ServiceOutcomeChanged}})
	r.ObserveDurationLabels(MetricServiceReconcileDuration, []Label{{"app", "a"}}, 2*time.Second)
	for _, name := range []string{
		MetricPendingEntries,
		MetricPendingOldestAge,
		MetricBufferedEvents,
		MetricInFlightInvocations,
	} {
		r.SetGauge(name, 1)
	}
}

// expectedMetricTypes derives the complete name→type map from the registry's own
// registration tables, so adding a collector automatically extends the
// completeness expectation rather than requiring a hard-coded list.
func expectedMetricTypes(r *Registry) map[string]dto.MetricType {
	want := make(map[string]dto.MetricType)
	for name := range r.counters {
		want[name] = dto.MetricType_COUNTER
	}
	for name := range r.gauges {
		want[name] = dto.MetricType_GAUGE
	}
	for name := range r.counterVecs {
		want[name] = dto.MetricType_COUNTER
	}
	for name := range r.histogramVecs {
		want[name] = dto.MetricType_HISTOGRAM
	}
	for name := range r.gaugeVecs {
		want[name] = dto.MetricType_GAUGE
	}
	return want
}

// TestMetricMetadataCompletenessAndTypes gathers every family after seeding and
// asserts the registry exposes exactly the Relay-owned registered metrics, each
// with a non-empty, sentence-case HELP and the type its registration implies. It
// pins the _total-is-counter rule for every counter family and catches any
// collector added without HELP. The standard Go runtime/process collectors share
// the registry and are excluded here (their metadata is the client library's,
// not Relay's).
func TestMetricMetadataCompletenessAndTypes(t *testing.T) {
	r := New()
	seedAllMetrics(r)

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	want := expectedMetricTypes(r)
	seen := make(map[string]bool, len(families))

	for _, f := range families {
		name := f.GetName()
		if !r.ownsMetric(name) {
			// Standard Go runtime/process family, not a Relay registration.
			continue
		}
		seen[name] = true

		wantType, ok := want[name]
		if !ok {
			t.Errorf("unexpected gathered family %q (not registered)", name)
			continue
		}
		if got := f.GetType(); got != wantType {
			t.Errorf("family %q type = %v, want %v", name, got, wantType)
		}

		// Every registered metric must carry non-empty, sentence-case HELP.
		help := f.GetHelp()
		if strings.TrimSpace(help) == "" {
			t.Errorf("family %q has empty HELP", name)
			continue
		}
		if first := []rune(help)[0]; first < 'A' || first > 'Z' {
			t.Errorf("family %q HELP does not start sentence-case: %q", name, help)
		}
		if !strings.HasSuffix(help, ".") {
			t.Errorf("family %q HELP is not a complete sentence: %q", name, help)
		}

		// Every counter family is named *_total (Prometheus convention).
		if wantType == dto.MetricType_COUNTER && !strings.HasSuffix(name, "_total") {
			t.Errorf("counter family %q lacks the _total suffix", name)
		}
	}

	// Completeness: every registered metric surfaced. Seeding every vec above
	// guarantees a family per registration.
	for name := range want {
		if !seen[name] {
			t.Errorf("registered metric %q was not gathered", name)
		}
	}
}

// TestMetricMetadataTotalFamiliesAreCounters re-asserts the _total→Counter rule
// directly on the gathered families, independent of the registration tables: no
// family ending in _total may be a gauge or histogram.
func TestMetricMetadataTotalFamiliesAreCounters(t *testing.T) {
	r := New()
	seedAllMetrics(r)

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		name := f.GetName()
		if !r.ownsMetric(name) {
			continue
		}
		if !strings.HasSuffix(name, "_total") {
			continue
		}
		if got := f.GetType(); got != dto.MetricType_COUNTER {
			t.Errorf("family %q ends in _total but has type %v, want COUNTER", name, got)
		}
	}
}

// TestMetricMetadataDurationHistogramsAreSeconds re-asserts the
// _seconds→histogram rule and that the exposed sample sum is in seconds rather
// than milliseconds: each seeded duration histogram observed exactly 2s, so its
// gathered sum must be 2. MetricPendingOldestAge is the one deliberate
// exception — an AGE gauge, not an observed duration — and is documented as
// such below.
func TestMetricMetadataDurationHistogramsAreSeconds(t *testing.T) {
	r := New()
	seedAllMetrics(r)

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		name := f.GetName()
		if !r.ownsMetric(name) {
			continue
		}
		if !strings.HasSuffix(name, "_seconds") {
			continue
		}
		// pending_oldest_age_seconds is a point-in-time AGE gauge (how old the
		// oldest pending entry is), not an observed duration histogram. It is
		// the sole sanctioned _seconds gauge; every other _seconds family must
		// be a duration histogram.
		if name == MetricPendingOldestAge {
			if got := f.GetType(); got != dto.MetricType_GAUGE {
				t.Errorf("family %q must remain a GAUGE (point-in-time age), got %v", name, got)
			}
			continue
		}
		if got := f.GetType(); got != dto.MetricType_HISTOGRAM {
			t.Errorf("family %q ends in _seconds but has type %v, want HISTOGRAM", name, got)
			continue
		}
		for _, m := range f.GetMetric() {
			h := m.GetHistogram()
			if h.GetSampleCount() == 0 {
				t.Errorf("family %q has an unobserved series after seeding", name)
				continue
			}
			// Two seconds observed, so the sum must be 2 (seconds), not 2000.
			if sum := h.GetSampleSum(); sum < 1.99 || sum > 2.01 {
				t.Errorf("family %q sample sum = %v, want ~2 (seconds)", name, sum)
			}
		}
	}
}

// TestMetricMetadataExpositionHasHelpAndType renders the registry through the
// same promhttp handler the worker serves and asserts every registered family's
// canonical name is preceded by a HELP and a TYPE line. This is the end-to-end
// metadata check: a client scraping /metrics can always pair each family with
// its documented semantics.
func TestMetricMetadataExpositionHasHelpAndType(t *testing.T) {
	r := New()
	seedAllMetrics(r)

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, f := range families {
		name := f.GetName()
		if !strings.Contains(body, "# HELP "+name+" ") {
			t.Errorf("exposition missing HELP line for %q:\n%s", name, body)
		}
		if !strings.Contains(body, "# TYPE "+name+" ") {
			t.Errorf("exposition missing TYPE line for %q:\n%s", name, body)
		}
	}
}

// TestMetricMetadataStandardRuntimeAndProcessFamilies pins that the dedicated
// registry also exposes the standard client_golang Go runtime and process
// collectors, so a scrape carries the conventional go_* and process_* families
// in addition to Relay's. Only PRESENCE is asserted (never exact runtime
// values, which change between scrapes). These families are registered on the
// same dedicated registry the /metrics handler serves — not the global default
// registry — and are deliberately not part of Relay's own metadata audit or the
// human-readable Snapshot.
func TestMetricMetadataStandardRuntimeAndProcessFamilies(t *testing.T) {
	r := New()

	// Gather first (the collectors are eager: they emit on every gather).
	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	gathered := make(map[string]bool, len(families))
	for _, f := range families {
		gathered[f.GetName()] = true
	}

	want := []string{
		"go_goroutines",
		"go_memstats_heap_alloc_bytes",
		"go_memstats_heap_objects",
		"go_memstats_heap_sys_bytes",
		"process_resident_memory_bytes",
	}
	for _, name := range want {
		if !gathered[name] {
			t.Errorf("registry missing standard family %q", name)
		}
	}

	// The same families must be visible end to end on the served exposition.
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, name := range want {
		if !strings.Contains(body, "# TYPE "+name+" ") {
			t.Errorf("/metrics missing TYPE line for standard family %q:\n%s", name, body)
		}
	}
}

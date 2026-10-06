package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestNamespacePrefixOnAllMetrics verifies that every metric registered by
// New() carries the relay_ namespace prefix and that no unprefixed duplicate or
// synonym family lingers in the registry (a name registered under two spellings
// would double-expose). It walks the gathered families, which is exactly what
// the /metrics exposition serves.
func TestNamespacePrefixOnAllMetrics(t *testing.T) {
	r := New()
	// Lazy collectors (the vecs and histograms) emit no family until at least
	// one series exists, so seed one series in each kind before gathering —
	// otherwise the registry only surfaces the 10 unlabeled counters and 4
	// gauges.
	r.Inc(MetricEventsReceived)
	r.Inc(MetricEventsMatched)
	r.Inc(MetricEventsUnmatched)
	r.Inc(MetricRetries)
	r.Inc(MetricDLQEntries)
	r.Inc(MetricHandlerSuccess)
	r.Inc(MetricHandlerFailure)
	r.Inc(MetricConcurrencyWaits)
	r.Inc(MetricScheduleOccurrencesPublished)
	r.Inc(MetricScheduleOccurrencesDuplicate)
	r.Inc(MetricSchedulePublishFailures)
	r.Inc(MetricSchedulePublishRetries)
	r.Inc(MetricSchedulePublishExhausted)
	r.Inc(MetricScheduleCatchUp)
	r.Inc(MetricSchedulePendingPersisted)
	r.Inc(MetricSchedulePendingRetries)
	r.Inc(MetricSchedulePendingExpired)
	r.Inc(MetricSchedulerDegraded)
	r.Inc(MetricSchedulerRecoveries)
	r.Inc(MetricMissingPayload)
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "a"}, {"handler", "x"}})
	r.IncLabels(MetricAppBuildFailures, []Label{{"app", "a"}})
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "a"}})
	r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"app", "a"}})
	r.IncLabels(MetricFunctionHandlerFailure, []Label{{"app", "a"}})
	r.IncLabels(MetricFunctionRetries, []Label{{"app", "a"}})
	r.IncLabels(MetricFunctionDLQ, []Label{{"app", "a"}})
	r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "a"}, {"handler", "x"}}, time.Millisecond)
	r.ObserveDurationLabels(MetricAppBuild, []Label{{"app", "a"}}, time.Millisecond)
	// Warm-container pool vecs.
	r.SetGaugeLabels(MetricRuntimeContainers, []Label{{"app", "a"}, {"state", RuntimeStateIdle}}, 1)
	r.SetGaugeLabels(MetricRuntimePoolCapacity, []Label{{"app", "a"}}, 1)
	r.IncLabels(MetricRuntimeContainerAcquires, []Label{{"app", "a"}, {"outcome", RuntimeOutcomeCold}})
	r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"app", "a"}, {"reason", "shutdown"}})
	r.ObserveDurationLabels(MetricRuntimeContainerAcquireDuration, []Label{{"app", "a"}}, time.Millisecond)
	r.IncLabels(MetricRuntimeContainerWaits, []Label{{"app", "a"}})
	r.SetGaugeLabels(MetricAppStatus, []Label{{"app", "a"}, {"status", "ready"}}, 1)
	r.SetGaugeLabels(MetricSchedulerState, []Label{{"state", SchedulerStateRunning}}, 1)
	r.IncLabels(MetricRedisReadErrors, []Label{{"operation", RedisOpReadGroup}})
	r.IncLabels(MetricServiceReconciles, []Label{{"app", "a"}, {"outcome", ServiceOutcomeChanged}})
	r.ObserveDurationLabels(MetricServiceReconcileDuration, []Label{{"app", "a"}}, time.Millisecond)
	r.SetGauge(MetricPendingEntries, 1)
	r.SetGauge(MetricPendingOldestAge, 1)
	r.SetGauge(MetricBufferedEvents, 1)
	r.SetGauge(MetricInFlightInvocations, 1)

	families, err := r.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	seen := map[string]bool{}
	for _, f := range families {
		name := f.GetName()
		if !r.ownsMetric(name) {
			// The standard Go runtime/process collectors also gather here; they
			// are not Relay-namespaced and are deliberately excluded.
			continue
		}
		if seen[name] {
			t.Fatalf("duplicate family %q gathered", name)
		}
		seen[name] = true
		if !strings.HasPrefix(name, metricNamespacePrefix) {
			t.Errorf("metric %q lacks the %q namespace prefix", name, metricNamespacePrefix)
		}
		display := metricDisplayName(name)
		if metricNamespacePrefix+display != name {
			t.Errorf("display/display-name roundtrip broken for %q", name)
		}
	}
	// The expected family count is derived from the registry's own registration
	// tables rather than a hard-coded constant, so adding a collector updates the
	// expectation automatically: one family per registered collector once its
	// series are seeded (every counter/vec/histogram vec is seeded above).
	wantFamilies := len(r.counters) + len(r.gauges) + len(r.counterVecs) + len(r.histogramVecs) + len(r.gaugeVecs)
	if len(seen) != wantFamilies {
		t.Errorf("gathered %d families, want %d (%d static counters + %d static gauges + %d counter vecs + %d histogram vecs + %d gauge vecs)",
			len(seen), wantFamilies, len(r.counters), len(r.gauges), len(r.counterVecs), len(r.histogramVecs), len(r.gaugeVecs))
	}
}

// TestCounterAddsAccumulateAcrossIncAndAdd verifies Inc and Add accumulate on the
// same counter while a distinct counter stays independent, and that the
// pre-registered zero gauges are still present in the rendered snapshot.
func TestCounterAddsAccumulateAcrossIncAndAdd(t *testing.T) {
	r := New()
	r.Inc(MetricEventsMatched)
	r.Inc(MetricEventsMatched)
	r.Add(MetricEventsMatched, 3)
	r.Inc(MetricRetries)
	got := r.Snapshot()
	// Pre-registered counters and gauges render their current (zero) value too;
	// assert the accumulated counters and that the required zero gauges are
	// present rather than an exact full-string match (the gauge set grows).
	for _, want := range []string{
		"events_matched_total count=5",
		"retries_total count=1",
		"pending_entries value=0",
		"pending_oldest_age_seconds value=0",
		"buffered_events value=0",
		"in_flight_invocations value=0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("snapshot missing %q; got %q", want, got)
		}
	}
}

func TestLabeledCounterIsolation(t *testing.T) {
	r := New()
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "a"}, {"handler", "x"}})
	// Reversed label order must resolve to the same metric.
	r.IncLabels(MetricHandlerInvocations, []Label{{"handler", "x"}, {"outcome", "success"}, {"app", "a"}})
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "b"}, {"handler", "x"}})
	got := r.Snapshot()
	if !strings.Contains(got, "handler_invocations_total{app=a,handler=x,outcome=success} count=2") {
		t.Fatalf("snapshot missing first series:\n%s", got)
	}
	if !strings.Contains(got, "handler_invocations_total{app=b,handler=x,outcome=success} count=1") {
		t.Fatalf("snapshot missing second series:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Fatalf("snapshot unexpectedly contains failure series:\n%s", got)
	}
}

func TestDurationCountSum(t *testing.T) {
	r := New()
	obs := func(d time.Duration) {
		r.ObserveDurationLabels(MetricHandlerDuration, []Label{{"app", "a"}, {"handler", "x"}}, d)
	}
	obs(1 * time.Second)
	obs(3 * time.Second)
	obs(2 * time.Second)
	got := r.Snapshot()
	if !strings.Contains(got, "handler_duration_seconds{app=a,handler=x} count=3 sum=6.000") {
		t.Fatalf("snapshot = %q, want it to contain %q", got,
			"handler_duration_seconds{app=a,handler=x} count=3 sum=6.000")
	}
}

func TestCounterGetter(t *testing.T) {
	r := New()
	r.Inc(MetricEventsMatched)
	r.Add(MetricEventsMatched, 3)
	r.Inc(MetricRetries)
	if got := r.Counter(MetricEventsMatched); got != 4 {
		t.Fatalf("Counter(events_matched_total) = %d, want 4", got)
	}
	if got := r.Counter(MetricRetries); got != 1 {
		t.Fatalf("Counter(retries_total) = %d, want 1", got)
	}
	// Absent and labeled-only names read as 0.
	if got := r.Counter("missing"); got != 0 {
		t.Fatalf("Counter(missing) = %d, want 0", got)
	}
	r.IncLabels(MetricHandlerInvocations, []Label{{"outcome", "success"}, {"app", "a"}, {"handler", "x"}})
	if got := r.Counter(MetricHandlerInvocations); got != 0 {
		t.Fatalf("Counter(handler_invocations_total) = %d, want 0 (labeled only)", got)
	}
}

func TestGaugeGetter(t *testing.T) {
	r := New()
	r.SetGauge(MetricPendingEntries, 1.5)
	r.SetGauge(MetricPendingEntries, 2.25)
	if got := r.Gauge(MetricPendingEntries); got != 2.25 {
		t.Fatalf("Gauge(pending_entries) = %v, want 2.25", got)
	}
	if got := r.Gauge("missing"); got != 0 {
		t.Fatalf("Gauge(missing) = %v, want 0", got)
	}
}

func TestGettersNilReceiver(t *testing.T) {
	var r *Registry
	if got := r.Counter("a"); got != 0 {
		t.Fatalf("nil Counter = %d, want 0", got)
	}
	if got := r.Gauge("g"); got != 0 {
		t.Fatalf("nil Gauge = %v, want 0", got)
	}
}

func TestGaugeSet(t *testing.T) {
	r := New()
	r.SetGauge(MetricPendingEntries, 1.5)
	r.SetGauge(MetricPendingEntries, 2.25)
	// There are no labeled gauges: a labeled set is a no-op.
	r.SetGaugeLabels(MetricPendingEntries, []Label{{"l", "x"}}, 9)
	got := r.Snapshot()
	if !strings.Contains(got, "pending_entries value=2.25") {
		t.Fatalf("snapshot missing pending_entries value:\n%s", got)
	}
	if strings.Contains(got, `pending_entries{l=x}`) {
		t.Fatalf("snapshot unexpectedly contains labeled gauge:\n%s", got)
	}
}

func TestSnapshotDeterminism(t *testing.T) {
	r := New()
	r.Inc(MetricDLQEntries)
	r.Inc(MetricEventsMatched)
	r.ObserveDurationLabels(MetricAppBuild, []Label{{"app", "a"}}, time.Second)
	r.SetGauge(MetricPendingEntries, 1)
	r.IncLabels(MetricAppBuildFailures, []Label{{"app", "a"}})
	first := r.Snapshot()
	for i := 0; i < 100; i++ {
		if got := r.Snapshot(); got != first {
			t.Fatalf("snapshot not deterministic:\nfirst=%q\ngot=%q", first, got)
		}
	}
}

func TestNilReceiverNoop(t *testing.T) {
	var r *Registry
	// None of these may panic.
	r.Inc("a")
	r.Add("a", 1)
	r.IncLabels("h", []Label{{"a", "1"}})
	r.ObserveDuration("d", time.Second)
	r.ObserveDurationLabels("d", []Label{{"a", "1"}}, time.Second)
	r.SetGauge("g", 1)
	r.SetGaugeLabels("g", []Label{{"a", "1"}}, 1)
	if got := r.Snapshot(); got != "" {
		t.Fatalf("nil snapshot = %q, want empty", got)
	}
}

func TestAppStatsSnapshot(t *testing.T) {
	r := New()
	// Two apps with distinct per-app counters.
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "a"}})
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "a"}})
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "b"}})
	r.IncLabels(MetricFunctionHandlerSuccess, []Label{{"app", "a"}})
	r.IncLabels(MetricFunctionHandlerFailure, []Label{{"app", "b"}})
	r.IncLabels(MetricFunctionRetries, []Label{{"app", "b"}})
	r.IncLabels(MetricFunctionDLQ, []Label{{"app", "b"}})

	got := r.AppStatsSnapshot()
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	// Sorted by app name.
	if got[0].App != "a" || got[1].App != "b" {
		t.Fatalf("unexpected order: %+v", got)
	}
	if got[0].EventsMatchedTotal != 2 || got[0].HandlerSuccessTotal != 1 || got[0].HandlerFailureTotal != 0 {
		t.Fatalf("function a stats = %+v", got[0])
	}
	if got[1].EventsMatchedTotal != 1 || got[1].HandlerFailureTotal != 1 || got[1].RetriesTotal != 1 || got[1].DLQTotal != 1 {
		t.Fatalf("function b stats = %+v", got[1])
	}
}

// TestAppStatsSnapshotIncludesPoolCounters verifies the per-app
// snapshot reads the cumulative warm-container pool counters: warm/cold acquires
// by outcome and discards summed across every reason. It also pins that a
// app with ONLY pool activity still surfaces (the pool series is a
// app-scoped series the snapshot groups).
func TestAppStatsSnapshotIncludesPoolCounters(t *testing.T) {
	r := New()
	r.AddLabels(MetricRuntimeContainerAcquires, []Label{{"app", "a"}, {"outcome", RuntimeOutcomeWarm}}, 7)
	r.AddLabels(MetricRuntimeContainerAcquires, []Label{{"app", "a"}, {"outcome", RuntimeOutcomeCold}}, 3)
	r.AddLabels(MetricRuntimeContainerDiscards, []Label{{"app", "a"}, {"reason", "timeout"}}, 2)
	r.AddLabels(MetricRuntimeContainerDiscards, []Label{{"app", "a"}, {"reason", "shutdown"}}, 1)

	got := byFn(r.AppStatsSnapshot(), "a")
	if got.WarmAcquiresTotal != 7 || got.ColdStartsTotal != 3 {
		t.Fatalf("acquires = %+v, want warm 7 cold 3", got)
	}
	if got.DiscardedTotal != 3 {
		t.Fatalf("discarded = %d, want 3 (all reasons summed)", got.DiscardedTotal)
	}

	// A pool-only app (no operational counter series) still surfaces.
	r.AddLabels(MetricRuntimeContainerAcquires, []Label{{"app", "poolonly"}, {"outcome", RuntimeOutcomeCold}}, 1)
	got = byFn(r.AppStatsSnapshot(), "poolonly")
	if got.App != "poolonly" || got.ColdStartsTotal != 1 || got.EventsMatchedTotal != 0 {
		t.Fatalf("pool-only function = %+v, want cold 1", got)
	}
}

// TestSeedAppStatPoolCounters verifies SeedAppStat restores the
// cumulative pool counters: acquires into their fixed outcome series and the
// aggregate discard total into the INTERNAL restored baseline rather than a
// synthetic Prometheus reason series — so the aggregate the snapshot later reads
// stays monotonic while the exposed per-reason series remain strictly causal.
// Zero values seed nothing.
func TestSeedAppStatPoolCounters(t *testing.T) {
	r := New()
	r.SeedAppStat(AppStat{
		App:               "alpha",
		WarmAcquiresTotal: 5,
		ColdStartsTotal:   2,
		DiscardedTotal:    4,
	})
	r.SeedAppStat(AppStat{App: "beta"}) // zero pool counters

	got := byFn(r.AppStatsSnapshot(), "alpha")
	if got.WarmAcquiresTotal != 5 || got.ColdStartsTotal != 2 || got.DiscardedTotal != 4 {
		t.Fatalf("seeded pool counters = %+v, want warm 5 cold 2 discarded 4", got)
	}
	// The restored discard aggregate must NOT surface as a Prometheus series:
	// the discard counter carries only real reasons. alpha has ONLY restored
	// discards, so the discard vec has no series at all.
	if seriesPresent(t, r.Snapshot(), MetricRuntimeContainerDiscards, "alpha") {
		t.Fatalf("restored discards must not create a reason series:\n%s", r.Snapshot())
	}
	// RuntimePoolCounters (the live inspect read) folds the baseline in too.
	if _, _, d := r.RuntimePoolCounters("alpha"); d != 4 {
		t.Fatalf("RuntimePoolCounters(alpha) discarded = %d, want 4", d)
	}
	// A zero-valued pool seed writes no pool counters (the pre-existing
	// operational counter series are still created by SeedAppStat).
	if b := byFn(r.AppStatsSnapshot(), "beta"); b.WarmAcquiresTotal != 0 ||
		b.ColdStartsTotal != 0 || b.DiscardedTotal != 0 {
		t.Fatalf("zero-valued pool seed must not create pool counters: %+v", b)
	}

	// An app whose ONLY restored pool counter is a discard total still
	// surfaces in the snapshot (it had a synthetic series before the baseline
	// change), and no Prometheus discard series is created for it.
	r.SeedAppStat(AppStat{App: "gamma", DiscardedTotal: 3})
	if g := byFn(r.AppStatsSnapshot(), "gamma"); g.App != "gamma" || g.DiscardedTotal != 3 {
		t.Fatalf("discard-only restored function must surface: %+v", g)
	}
	if seriesPresent(t, r.Snapshot(), MetricRuntimeContainerDiscards, "gamma") {
		t.Fatalf("restored discard-only function must not expose a reason series:\n%s", r.Snapshot())
	}

	// A live discard after the seed keeps accumulating on top of the restored
	// total rather than replacing it, and the live series is causal (its real
	// reason), not the restored aggregate.
	r.IncLabels(MetricRuntimeContainerDiscards, []Label{{"app", "alpha"}, {"reason", "idle_timeout"}})
	if d := byFn(r.AppStatsSnapshot(), "alpha").DiscardedTotal; d != 5 {
		t.Fatalf("discarded after live increment = %d, want 5", d)
	}
	s := r.Snapshot()
	if !strings.Contains(s, "runtime_container_discards_total{app=alpha,reason=idle_timeout} count=1") {
		t.Fatalf("live discard must be causal:\n%s", s)
	}
	if strings.Contains(s, "reason=restored") {
		t.Fatalf("no synthetic restored reason series may exist:\n%s", s)
	}
	// RuntimePoolCounters also reports the causal series plus the baseline.
	if _, _, d := r.RuntimePoolCounters("alpha"); d != 5 {
		t.Fatalf("RuntimePoolCounters(alpha) discarded = %d, want 5", d)
	}
}

// TestRestoredDiscardsClearedOnRemoval pins that the internal restored discard
// baseline is cleared by every app retirement path — RemoveApp,
// SweepAppMetrics, and DeleteRuntimePool — so a removed app's restored
// aggregate never lingers in a future snapshot or inspect read.
func TestRestoredDiscardsClearedOnRemoval(t *testing.T) {
	t.Run("RemoveApp", func(t *testing.T) {
		r := New()
		r.SeedAppStat(AppStat{App: "alpha", DiscardedTotal: 4})
		if _, _, d := r.RuntimePoolCounters("alpha"); d != 4 {
			t.Fatalf("baseline not seeded: %d", d)
		}
		r.RemoveApp("alpha")
		if _, _, d := r.RuntimePoolCounters("alpha"); d != 0 {
			t.Fatalf("RemoveApp must clear the restored baseline: %d", d)
		}
		if len(r.AppStatsSnapshot()) != 0 {
			t.Fatalf("snapshot must be empty after removal: %+v", r.AppStatsSnapshot())
		}
	})

	t.Run("SweepAppMetrics", func(t *testing.T) {
		r := New()
		r.SeedAppStat(AppStat{App: "alpha", DiscardedTotal: 4})
		r.SweepAppMetrics(map[string]bool{"other": true})
		if _, _, d := r.RuntimePoolCounters("alpha"); d != 0 {
			t.Fatalf("sweep must clear the restored baseline: %d", d)
		}
	})

	t.Run("DeleteRuntimePool", func(t *testing.T) {
		r := New()
		r.SeedAppStat(AppStat{App: "alpha", DiscardedTotal: 4})
		r.DeleteRuntimePool("alpha")
		if _, _, d := r.RuntimePoolCounters("alpha"); d != 0 {
			t.Fatalf("DeleteRuntimePool must clear the restored baseline: %d", d)
		}
	})

	t.Run("NilSafe", func(t *testing.T) {
		var nilR *Registry
		if _, _, d := nilR.RuntimePoolCounters("alpha"); d != 0 {
			t.Fatalf("nil registry discarded = %d, want 0", d)
		}
		nilR.DeleteRuntimePool("alpha")
		nilR.RemoveApp("alpha")
	})
}

func TestAppStatsSnapshotEmptyAndNil(t *testing.T) {
	r := New()
	if got := r.AppStatsSnapshot(); got != nil {
		t.Fatalf("empty snapshot = %+v, want nil", got)
	}
	var nilR *Registry
	if got := nilR.AppStatsSnapshot(); got != nil {
		t.Fatalf("nil snapshot = %+v, want nil", got)
	}
}

// TestHandlerNilRegistryServesEmpty verifies a nil-receiver Handler returns a
// valid handler serving an empty 200 body (callers never get a nil http.Handler).
func TestHandlerNilRegistryServesEmpty(t *testing.T) {
	var r *Registry
	h := r.Handler()
	if h == nil {
		t.Fatal("nil registry Handler() returned nil http.Handler")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("nil registry body = %q, want empty", rec.Body.String())
	}
}

// TestSetAppTimestampSnapshot verifies SetAppTimestamp stores the
// latest per-kind values and AppStatsSnapshot surfaces them, including the
// counter-without-timestamp (all-zero timestamps) and timestamp-without-counter
// shapes.
func TestSetAppTimestampSnapshot(t *testing.T) {
	r := New()
	// "a": counters + all four timestamps.
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "a"}})
	ts := int64(1700000000)
	r.SetAppTimestamp("a", AppTimestampExecution, ts)
	r.SetAppTimestamp("a", AppTimestampSuccess, ts+1)
	r.SetAppTimestamp("a", AppTimestampFailure, ts+2)
	r.SetAppTimestamp("a", AppTimestampDLQ, ts+3)

	// SetTimestamp entries survive repeated overwrites (latest wins).
	r.SetAppTimestamp("a", AppTimestampExecution, ts+10)

	got := r.AppStatsSnapshot()
	byName := map[string]AppStat{}
	for _, f := range got {
		byName[f.App] = f
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing from snapshot: %+v", got)
	}
	if a.LastExecution != ts+10 || a.LastSuccess != ts+1 || a.LastFailure != ts+2 || a.LastDLQ != ts+3 {
		t.Fatalf("a timestamps = %+v", a)
	}

	// An app with counters but NO timestamp entry reads zeros (counters-only shape).
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "c"}})
	got = r.AppStatsSnapshot()
	c := byFn(got, "c")
	if c.EventsMatchedTotal != 1 || c.LastExecution != 0 || c.LastSuccess != 0 || c.LastFailure != 0 || c.LastDLQ != 0 {
		t.Fatalf("c (counters-only) = %+v", c)
	}

	// Nil-safe: a nil receiver is a no-op.
	var nilR *Registry
	nilR.SetAppTimestamp("x", AppTimestampExecution, ts)
	// An unknown kind is ignored.
	r.SetAppTimestamp("a", AppTimestampKind(99), ts+20)
	if a := byFn(r.AppStatsSnapshot(), "a"); a.LastExecution != ts+10 {
		t.Fatalf("unknown kind must not write: %+v", a)
	}
}

// byFn finds the AppStat for name in a snapshot, or a zero value.
func byFn(fs []AppStat, name string) AppStat {
	for _, f := range fs {
		if f.App == name {
			return f
		}
	}
	return AppStat{}
}

// TestSeedAppStatTimestamps verifies SeedAppStat restores persisted
// timestamps into the snapshot and SKIPS zeros (a persisted zero never
// materializes as an entry that could later look newer than nothing).
func TestSeedAppStatTimestamps(t *testing.T) {
	r := New()
	ts := int64(1700000000)
	r.SeedAppStat(AppStat{App: "alpha", EventsMatchedTotal: 1, LastExecution: ts, LastSuccess: ts + 1})
	// beta has counters but only zero timestamps: nothing is stored.
	r.SeedAppStat(AppStat{App: "beta", EventsMatchedTotal: 1})

	got := r.AppStatsSnapshot()
	a := byFn(got, "alpha")
	if a.LastExecution != ts || a.LastSuccess != ts+1 || a.LastFailure != 0 || a.LastDLQ != 0 {
		t.Fatalf("alpha = %+v", a)
	}
	if b := byFn(got, "beta"); b.LastExecution != 0 || b.LastSuccess != 0 {
		t.Fatalf("beta must have no timestamp entries: %+v", b)
	}

	// A partial overwrite preserves the kinds that were not seeded.
	r.SeedAppStat(AppStat{App: "alpha", EventsMatchedTotal: 2, LastFailure: ts + 5})
	a = byFn(r.AppStatsSnapshot(), "alpha")
	if a.EventsMatchedTotal != 3 || a.LastExecution != ts || a.LastSuccess != ts+1 || a.LastFailure != ts+5 {
		t.Fatalf("alpha after partial seed = %+v", a)
	}
}

// TestRemoveAppClearsTimestamps verifies RemoveApp deletes the
// Relay-side timestamp entry alongside the Prometheus series.
func TestRemoveAppClearsTimestamps(t *testing.T) {
	r := New()
	ts := int64(1700000000)
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "a"}})
	r.SetAppTimestamp("a", AppTimestampExecution, ts)
	r.IncLabels(MetricAppEventsMatched, []Label{{"app", "b"}})
	r.SetAppTimestamp("b", AppTimestampExecution, ts)

	r.RemoveApp("a")
	got := r.AppStatsSnapshot()
	if byFn(got, "a").App != "" {
		t.Fatalf("a (series+timestamps) must be fully removed: %+v", got)
	}
	if byFn(got, "b").LastExecution != ts {
		t.Fatalf("b must survive: %+v", got)
	}

	// Sweep does the same with the live set: with live={"a"} (no series at all
	// after the removal, so a is absent from the snapshot entirely) b's
	// timestamps and series are swept; with live={"b"} b survives fully.
	r.SweepAppMetrics(map[string]bool{"b": true})
	if a := byFn(r.AppStatsSnapshot(), "b"); a.LastExecution != ts {
		t.Fatalf("sweep must keep live b's timestamps:\n%+v", r.AppStatsSnapshot())
	}
	r.SweepAppMetrics(map[string]bool{"a": true})
	if fs := r.AppStatsSnapshot(); len(fs) != 0 {
		t.Fatalf("sweep must clear non-live b's timestamps too:\n%+v", fs)
	}

	// Nil-safety: none of these cleanup paths panic on a nil receiver.
	var nilR *Registry
	nilR.RemoveApp("a")
	nilR.SweepAppMetrics(map[string]bool{})
}

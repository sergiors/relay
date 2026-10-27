package worker

import (
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// TestSnapshotStatsMapping pins the registry-to-state row mapping: counter names
// feed the columns directly (retries_total → RetryTotal) and the float gauges
// are truncated to int64.
func TestSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsReceived, 150)
	m.Add(metrics.MetricEventsMatched, 10)
	m.Add(metrics.MetricEventsUnmatched, 5)
	m.Add(metrics.MetricHandlerSuccess, 7)
	m.Add(metrics.MetricHandlerFailure, 3)
	m.Add(metrics.MetricRetries, 2)
	m.Add(metrics.MetricDLQEntries, 1)
	m.SetGauge(metrics.MetricPendingEntries, 4.9)
	m.SetGauge(metrics.MetricPendingOldestAge, 12.7)

	got := snapshotStats(m, nil)
	want := state.Stats{
		EventsReceivedTotal:     150,
		EventsMatchedTotal:      10,
		EventsUnmatchedTotal:    5,
		HandlerSuccessTotal:     7,
		HandlerFailureTotal:     3,
		RetryTotal:              2, // registry retries_total → stats RetryTotal
		DLQTotal:                1,
		PendingEntries:          4, // float gauge truncated to int64
		OldestPendingAgeSeconds: 12,
	}
	if got != want {
		t.Fatalf("snapshotStats = %+v, want %+v", got, want)
	}
}

// TestSnapshotStatsNilRegistry pins the nil-safety contract of the global mapper.
func TestSnapshotStatsNilRegistry(t *testing.T) {
	got := snapshotStats(nil, nil)
	if got != (state.Stats{}) {
		t.Fatalf("snapshotStats(nil, nil) = %+v, want zero Stats", got)
	}
}

// TestFuncSnapshotStatsMapping pins the per-app registry-to-state row
// mapping, indexed by app name.
func TestFuncSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "a"}})
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "app", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "app", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "app", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionDLQ, []metrics.Label{{Name: "app", Value: "b"}})

	got := snapshotAppStats(m, nil)
	byName := map[string]state.AppStats{}
	for _, fs := range got {
		byName[fs.App] = fs
	}
	if len(byName) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(byName), got)
	}
	if a := byName["a"]; a.EventsMatchedTotal != 2 || a.HandlerSuccessTotal != 1 {
		t.Fatalf("function a = %+v", a)
	}
	if b := byName["b"]; b.HandlerFailureTotal != 1 || b.RetryTotal != 1 || b.DLQTotal != 1 {
		t.Fatalf("function b = %+v", b)
	}
}

// TestFuncSnapshotStatsNilRegistry pins the nil-safety contract of the
// per-app mapper.
func TestFuncSnapshotStatsNilRegistry(t *testing.T) {
	if got := snapshotAppStats(nil, nil); got != nil {
		t.Fatalf("snapshotAppStats(nil, nil) = %+v, want nil", got)
	}
}

// TestSnapshotStatsIgnoresLabeledCounters guards against the unlabeled getters
// accidentally reading labeled-only metrics (which would double-count).
func TestSnapshotStatsIgnoresLabeledCounters(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "app", Value: "a"}, {Name: "handler", Value: "x"}})
	got := snapshotStats(m, nil)
	if got.HandlerSuccessTotal != 0 {
		t.Fatalf("HandlerSuccessTotal = %d, want 0 (labeled only)", got.HandlerSuccessTotal)
	}
	if !strings.Contains(m.Snapshot(), "handler_invocations_total{app=a,handler=x,outcome=success} count=1") {
		t.Fatalf("labeled counter missing from snapshot:\n%s", m.Snapshot())
	}
}

// TestFuncSnapshotStatsPoolCountersMapping verifies the flush mapper carries the
// pool counters from the registry snapshot into the state rows.
func TestFuncSnapshotStatsPoolCountersMapping(t *testing.T) {
	m := metrics.New()
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "a"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}}, 4)
	m.AddLabels(metrics.MetricRuntimeContainerAcquires, []metrics.Label{{Name: "app", Value: "a"}, {Name: "outcome", Value: metrics.RuntimeOutcomeCold}}, 1)
	m.AddLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{{Name: "app", Value: "a"}, {Name: "reason", Value: "timeout"}}, 2)

	got := snapshotAppStats(m, nil)
	byName := map[string]state.AppStats{}
	for _, fs := range got {
		byName[fs.App] = fs
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing from flush mapping: %+v", got)
	}
	if a.WarmAcquiresTotal != 4 || a.ColdStartsTotal != 1 || a.DiscardedTotal != 2 {
		t.Fatalf("a pool counters = %+v, want warm 4 cold 1 discarded 2", a)
	}
}

// byApp finds the AppStat for name in a snapshot, or a zero value.
func byApp(fs []metrics.AppStat, name string) metrics.AppStat {
	for _, f := range fs {
		if f.App == name {
			return f
		}
	}
	return metrics.AppStat{}
}

// stateApp builds a minimal app.App for seeding state rows in
// worker tests without importing the reconciler's helpers.
func stateApp(name, dir string) app.App {
	tmpl, err := app.ParseTemplate([]byte("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"))
	if err != nil {
		panic(err)
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

// TestFuncSnapshotStatsTimestampsMapping verifies the flush mapper: unix
// seconds map to RFC3339 strings and zeros map to "" (never observed).
func TestFuncSnapshotStatsTimestampsMapping(t *testing.T) {
	m := metrics.New()
	ts := int64(1700000000)
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "a"}})
	m.SetAppTimestamp("a", metrics.AppTimestampExecution, ts)
	m.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "b"}})
	m.SetAppTimestamp("b", metrics.AppTimestampDLQ, ts+60)

	got := snapshotAppStats(m, nil)
	byName := map[string]state.AppStats{}
	for _, fs := range got {
		byName[fs.App] = fs
	}
	a, ok := byName["a"]
	if !ok {
		t.Fatalf("a missing: %+v", got)
	}
	if a.LastExecutionAt != time.Unix(ts, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("a = %+v; want execution %s", a, time.Unix(ts, 0).UTC().Format(time.RFC3339))
	}
	if a.LastSuccessAt != "" || a.LastFailureAt != "" || a.LastDLQAt != "" {
		t.Fatalf("unobserved timestamps must map to empty strings: %+v", a)
	}
	b := byName["b"]
	if b.LastDLQAt != time.Unix(ts+60, 0).UTC().Format(time.RFC3339) || b.LastExecutionAt != "" {
		t.Fatalf("b = %+v", b)
	}
}

// TestRfc3339ToUnixRoundTrip pins the two conversion helpers as strict
// inverses for valid values and "never" sentinels at the boundaries.
func TestRfc3339ToUnixRoundTrip(t *testing.T) {
	ts := int64(1700000000)
	if got := unixSecToRFC3339(ts); got != "2023-11-14T22:13:20Z" {
		t.Fatalf("unixSecToRFC3339(%d) = %q", ts, got)
	}
	if got := rfc3339ToUnix(unixSecToRFC3339(ts)); got != ts {
		t.Fatalf("round trip = %d, want %d", got, ts)
	}
	// "never" conventions: zero in → empty out; empty/invalid in → 0 out.
	if unixSecToRFC3339(0) != "" || unixSecToRFC3339(-5) != "" {
		t.Fatal("non-positive timestamps must map to empty strings")
	}
	if rfc3339ToUnix("") != 0 || rfc3339ToUnix("garbage") != 0 {
		t.Fatal("empty/invalid values must map to 0")
	}
}

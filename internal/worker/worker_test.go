package worker

import (
	"context"
	"io"
	"log/slog"

	"strings"
	"testing"
	"time"

	"relay/internal/metrics"
	"relay/internal/state"
)

func TestSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.Add(metrics.MetricEventsProcessed, 10)
	m.Add(metrics.MetricHandlerSuccess, 7)
	m.Add(metrics.MetricHandlerFailure, 3)
	m.Add(metrics.MetricRetries, 2)
	m.Add(metrics.MetricDLQEntries, 1)
	m.SetGauge(metrics.MetricPendingEntries, 4.9)
	m.SetGauge(metrics.MetricPendingOldestAge, 12.7)

	got := snapshotStats(m)
	want := state.Stats{
		EventsProcessedTotal:    10,
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

func TestSnapshotStatsNilRegistry(t *testing.T) {
	got := snapshotStats(nil)
	if got != (state.Stats{}) {
		t.Fatalf("snapshotStats(nil) = %+v, want zero Stats", got)
	}
}

func TestFuncSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricFunctionEvents, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionEvents, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels(metrics.MetricFunctionDLQ, []metrics.Label{{Name: "function", Value: "b"}})

	got := snapshotFunctionStats(m)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}
	if got[0].Function != "a" || got[0].EventsProcessedTotal != 2 || got[0].HandlerSuccessTotal != 1 {
		t.Fatalf("function a = %+v", got[0])
	}
	if got[1].Function != "b" || got[1].HandlerFailureTotal != 1 || got[1].RetryTotal != 1 || got[1].DLQTotal != 1 {
		t.Fatalf("function b = %+v", got[1])
	}
}

func TestFuncSnapshotStatsNilRegistry(t *testing.T) {
	if got := snapshotFunctionStats(nil); got != nil {
		t.Fatalf("snapshotFunctionStats(nil) = %+v, want nil", got)
	}
}

// TestServerStartBadAddrFailsFast proves a metrics bind failure surfaces
// immediately: Server.Start binds synchronously and returns the error rather
// than retrying, so the worker's startup gate (which logger.Fatalfs on a non-nil
// return) treats a taken/unparseable metrics addr as a fatal config error. The
// old retry-bind behavior (which healed a temporarily occupied port) is
// deliberately removed.
func TestServerStartBadAddrFailsFast(t *testing.T) {
	m := metrics.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := metrics.NewServer("crap", m.Handler(), logger)
	err := srv.Start()
	if err == nil {
		t.Fatal("Start on bad addr returned nil, want immediate error")
	}
}

// TestStatsLoopNilStateExitsOnCancel ensures the snapshot loop is nil-safe on
// the state handle and stops promptly on cancel.
func TestStatsLoopNilStateExitsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, metrics.New(), nil, time.Hour)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}
}

// TestSnapshotStatsIgnoresLabeledCounters guards against the unlabeled getters
// accidentally reading labeled-only metrics (which would double-count).
func TestSnapshotStatsIgnoresLabeledCounters(t *testing.T) {
	m := metrics.New()
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: "a"}, {Name: "handler", Value: "x"}})
	got := snapshotStats(m)
	if got.HandlerSuccessTotal != 0 {
		t.Fatalf("HandlerSuccessTotal = %d, want 0 (labeled only)", got.HandlerSuccessTotal)
	}
	if !strings.Contains(m.Snapshot(), "handler_invocations_total{function=a,handler=x,outcome=success} count=1") {
		t.Fatalf("labeled counter missing from snapshot:\n%s", m.Snapshot())
	}
}

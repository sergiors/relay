package main

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"relay/internal/config"
	"relay/internal/metrics"
	"relay/internal/state"
)

func TestSnapshotStatsMapping(t *testing.T) {
	m := metrics.New()
	m.Add("events_processed_total", 10)
	m.Add("handler_success_total", 7)
	m.Add("handler_failure_total", 3)
	m.Add("retries_total", 2)
	m.Add("dlq_entries_total", 1)
	m.SetGauge("pending_entries", 4.9)
	m.SetGauge("pending_oldest_age_seconds", 12.7)

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
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels("function_events_total", []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels("function_handler_success_total", []metrics.Label{{Name: "function", Value: "a"}})
	m.IncLabels("function_handler_failure_total", []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels("function_retries_total", []metrics.Label{{Name: "function", Value: "b"}})
	m.IncLabels("function_dlq_total", []metrics.Label{{Name: "function", Value: "b"}})

	got := funcSnapshotStats(m)
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
	if got := funcSnapshotStats(nil); got != nil {
		t.Fatalf("funcSnapshotStats(nil) = %+v, want nil", got)
	}
}

func TestEnvFallback(t *testing.T) {
	t.Setenv("RELAY_TEST_ENV", "")
	if got := config.Env("RELAY_TEST_ENV", "fallback"); got != "fallback" {
		t.Fatalf("Env(empty) = %q, want fallback", got)
	}

	t.Setenv("RELAY_TEST_ENV", "set")
	if got := config.Env("RELAY_TEST_ENV", "fallback"); got != "set" {
		t.Fatalf("Env(set) = %q, want set", got)
	}

	t.Setenv("RELAY_TEST_ENV", "")
	if got := config.Env("RELAY_TEST_ENV", ""); got != "" {
		t.Fatalf("Env(empty, empty fallback) = %q, want empty", got)
	}
}

// TestMetricsServerSurvivesBadAddr proves a metrics bind failure never takes
// down the worker: ServeHTTP logs the failure, retries, and exits promptly when
// ctx is cancelled. A deliberately unparseable address always fails to bind, so
// the retry-bind path returns context.Canceled on shutdown rather than hanging.
func TestMetricsServerSurvivesBadAddr(t *testing.T) {
	m := metrics.New()
	logger := log.New(io.Discard, "", 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.ServeHTTP(ctx, "crap", logger.Printf) }()

	// Give the retry loop a moment to attempt (and fail) the bind.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("ServeHTTP returned %v, want context.Canceled on cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeHTTP did not return after cancel")
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
	m.IncLabels("handler_invocations_total", []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: "a"}, {Name: "handler", Value: "x"}})
	got := snapshotStats(m)
	if got.HandlerSuccessTotal != 0 {
		t.Fatalf("HandlerSuccessTotal = %d, want 0 (labeled only)", got.HandlerSuccessTotal)
	}
	if !strings.Contains(m.Snapshot(), "handler_invocations_total{function=a,handler=x,outcome=success} count=1") {
		t.Fatalf("labeled counter missing from snapshot:\n%s", m.Snapshot())
	}
}

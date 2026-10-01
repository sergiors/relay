package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/state"
)

// lifecycleLogger is the discard logger for the stats-lifecycle tests.
func lifecycleLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSetupMetricsAlwaysBuildsRegistry pins the core lifecycle change: the
// accounting registry is ALWAYS constructed, independent of METRICS_ADDR, while
// the /metrics HTTP server (the exposition gate) is nil when disabled and
// non-nil when an address is configured. This is what makes persistent stats
// independent of the HTTP listener without creating a second accounting path.
func TestSetupMetricsAlwaysBuildsRegistry(t *testing.T) {
	reg, srv := setupMetrics(config.Config{}, lifecycleLogger())
	if reg == nil {
		t.Fatal("metrics disabled: registry must still be constructed")
	}
	if srv != nil {
		t.Fatalf("metrics disabled: server = %v, want nil (exposition is opt-in)", srv)
	}

	regAddr, srvAddr := setupMetrics(config.Config{MetricsAddr: "127.0.0.1:0"}, lifecycleLogger())
	if regAddr == nil {
		t.Fatal("metrics enabled: registry must be constructed")
	}
	if srvAddr == nil {
		t.Fatal("metrics enabled: server must be constructed")
	}
}

// TestStatsPersistWithMetricsDisabled is the regression for the reported bug:
// with METRICS_ADDR unset the stats loop must still flush the registry into
// SQLite. It wires the registry exactly as Run does (setupMetrics with an empty
// address), runs the unconditional statsLoop, and asserts the persisted totals.
func TestStatsPersistWithMetricsDisabled(t *testing.T) {
	reg, srv := setupMetrics(config.Config{}, lifecycleLogger())
	if srv != nil {
		t.Fatal("precondition: metrics server must be nil when disabled")
	}
	st := openTempState(t)
	st.RecordDiscovered(stateApp("alpha", t.TempDir()))
	reg.Add(metrics.MetricEventsMatched, 3)
	reg.Add(metrics.MetricHandlerSuccess, 2)
	reg.IncLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, newStatsFlusher(st, reg), time.Hour)
	}()

	waitForStatsRow(t, "statsLoop to persist with metrics disabled", func() bool {
		gs, ok := st.Stats()
		return ok && gs.EventsMatchedTotal == 3
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after flush with metrics disabled")
	}
	if gs.EventsMatchedTotal != 3 || gs.HandlerSuccessTotal != 2 {
		t.Fatalf("persisted globals = %+v, want events 3 success 2", gs)
	}
	a, ok := st.AppStats("alpha")
	if !ok || a.EventsMatchedTotal != 1 {
		t.Fatalf("persisted alpha = %+v, ok=%v; want events 1", a, ok)
	}
}

// TestMetricsEnabledExposesSingleAccountingRegistry proves metrics enabled does
// NOT introduce a second observer: the HTTP exposition serves the SAME registry
// the stats flush reads, a single accounted event appears once on /metrics and
// once in the persisted snapshot (never double-counted).
func TestMetricsEnabledExposesSingleAccountingRegistry(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	reg, srv := setupMetrics(config.Config{MetricsAddr: fmt.Sprintf("127.0.0.1:%d", port)}, lifecycleLogger())
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = srv.Stop(stopCtx)
	})

	st := openTempState(t)
	st.RecordDiscovered(stateApp("alpha", t.TempDir()))

	// ONE accounted event on the single registry.
	reg.Inc(metrics.MetricEventsMatched)

	// The exposition reflects exactly that one event.
	body := scrapeMetrics(t, port)
	if !strings.Contains(body, "relay_events_matched_total 1") {
		t.Fatalf("/metrics must expose the single accounted event exactly once:\n%s", body)
	}

	// The persisted snapshot reads the same registry value: one event, not two.
	recordSnapshots(context.Background(), st, reg, nil)
	gs, ok := st.Stats()
	if !ok || gs.EventsMatchedTotal != 1 {
		t.Fatalf("persisted events = %+v, ok=%v; want exactly 1 (single accounting path)", gs, ok)
	}
}

// TestStatsResetWithMetricsDisabled verifies `reset stats` works with metrics
// disabled: the always-present registry captures the worker-owned baseline and
// the persisted rows zero, while the Prometheus counters stay monotonic (the
// same semantics as when exposition is enabled, since it is the same registry).
func TestStatsResetWithMetricsDisabled(t *testing.T) {
	reg, srv := setupMetrics(config.Config{}, lifecycleLogger())
	if srv != nil {
		t.Fatal("precondition: metrics server must be nil when disabled")
	}
	st := openTempState(t)
	st.RecordDiscovered(stateApp("alpha", t.TempDir()))
	f := newStatsFlusher(st, reg)

	reg.Add(metrics.MetricEventsMatched, 100)
	reg.AddLabels(metrics.MetricAppEventsMatched, []metrics.Label{{Name: "app", Value: "alpha"}}, 4)
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 100 {
		t.Fatalf("pre-reset persisted events = %d, want 100", gs.EventsMatchedTotal)
	}

	if err := f.ResetStats(); err != nil {
		t.Fatalf("ResetStats with metrics disabled: %v", err)
	}

	// Persisted rows are zero immediately; Prometheus is unchanged.
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("persisted events after reset = %d, want 0", gs.EventsMatchedTotal)
	}
	if a, _ := st.AppStats("alpha"); a.EventsMatchedTotal != 0 {
		t.Fatalf("persisted function events after reset = %d, want 0", a.EventsMatchedTotal)
	}
	if got := reg.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("Prometheus events after reset = %d, want 100 (monotonic)", got)
	}

	// A post-reset flush must not resurrect the pre-reset value; post-reset
	// activity accumulates from zero.
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 0 {
		t.Fatalf("flush after reset resurrected events = %d, want 0", gs.EventsMatchedTotal)
	}
	reg.Add(metrics.MetricEventsMatched, 5)
	f.flush(context.Background())
	if gs, _ := st.Stats(); gs.EventsMatchedTotal != 5 {
		t.Fatalf("persisted events after post-reset activity = %d, want 5", gs.EventsMatchedTotal)
	}
}

// TestFinalStatsFlushWithMetricsDisabled verifies the shutdown final flush runs
// and persists with METRICS_ADDR unset (the registry always exists), so the
// last interval of telemetry is not lost when HTTP exposition is disabled.
func TestFinalStatsFlushWithMetricsDisabled(t *testing.T) {
	reg, srv := setupMetrics(config.Config{}, lifecycleLogger())
	if srv != nil {
		t.Fatal("precondition: metrics server must be nil when disabled")
	}
	st := openTempState(t)
	st.RecordDiscovered(stateApp("alpha", t.TempDir()))
	reg.Add(metrics.MetricEventsMatched, 42)
	reg.Add(metrics.MetricHandlerSuccess, 30)

	finalStatsFlush(context.Background(), newStatsFlusher(st, reg))

	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected stats row after finalStatsFlush with metrics disabled")
	}
	if gs.EventsMatchedTotal != 42 || gs.HandlerSuccessTotal != 30 {
		t.Fatalf("final globals = %+v, want events 42 success 30", gs)
	}
}

// TestStatsRestoreWithMetricsDisabled verifies restart restoration is unchanged
// when metrics are disabled: a fresh always-present registry is seeded from the
// persisted totals and the first flush writes them back rather than zeroing.
func TestStatsRestoreWithMetricsDisabled(t *testing.T) {
	st := openTempState(t)
	st.RecordDiscovered(stateApp("alpha", t.TempDir()))
	st.RecordStats(state.Stats{EventsMatchedTotal: 100, HandlerSuccessTotal: 70})
	st.RecordAppStats(state.AppStats{App: "alpha", EventsMatchedTotal: 9})

	reg, srv := setupMetrics(config.Config{}, lifecycleLogger())
	if srv != nil {
		t.Fatal("precondition: metrics server must be nil when disabled")
	}
	restorePersistedStats(reg, st)

	if got := reg.Counter(metrics.MetricEventsMatched); got != 100 {
		t.Fatalf("restored global events = %d, want 100", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, newStatsFlusher(st, reg), time.Hour)
	}()
	waitForStatsRow(t, "restored totals to be written back", func() bool {
		gs, ok := st.Stats()
		return ok && gs.EventsMatchedTotal == 100
	})
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}

	gs, ok := st.Stats()
	if !ok || gs.EventsMatchedTotal != 100 || gs.HandlerSuccessTotal != 70 {
		t.Fatalf("post-restart flush = %+v, ok=%v; want restored totals preserved", gs, ok)
	}
	if a, _ := st.AppStats("alpha"); a.EventsMatchedTotal != 9 {
		t.Fatalf("post-restart function events = %d, want 9", a.EventsMatchedTotal)
	}
}

// scrapeMetrics fetches /metrics from the loopback port until it responds,
// returning the exposition body. It bounds the retry so a server that never
// comes up fails rather than hangs.
func scrapeMetrics(t *testing.T, port int) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
		if err == nil && resp.StatusCode == 200 {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return string(b)
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics server never became scrapeable: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

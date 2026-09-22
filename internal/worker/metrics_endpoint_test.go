package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"relay/internal/metrics"
)

// TestMetricsEndpointScrapeExposesFunctionSeries boots a metrics server on a
// free port fed from an in-memory registry, then scrapes /metrics and asserts
// the Prometheus text exposition carries the function-scoped and warm-pool
// series end to end. It needs no Redis or Docker: the registry is in-memory.
func TestMetricsEndpointScrapeExposesFunctionSeries(t *testing.T) {
	// Free port for the metrics server.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	metricsInstance := metrics.New()
	metricsInstance.Inc(metrics.MetricEventsMatched)
	metricsInstance.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: "demo"}, {Name: "handler", Value: "index.hi"}})
	metricsInstance.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "handler", Value: "index.hi"}}, 250*time.Millisecond)
	metricsInstance.SetGauge(metrics.MetricPendingEntries, 3)
	// Warm-container pool observability series.
	metricsInstance.SetGaugeLabels(metrics.MetricRuntimePoolCapacity, []metrics.Label{{Name: "function", Value: "demo"}}, 2)
	metricsInstance.SetGaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "state", Value: metrics.RuntimeStateIdle}}, 1)
	metricsInstance.SetGaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "state", Value: metrics.RuntimeStateBusy}}, 1)
	metricsInstance.IncLabels(metrics.MetricRuntimeContainerAcquires,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "outcome", Value: metrics.RuntimeOutcomeCold}})
	metricsInstance.IncLabels(metrics.MetricRuntimeContainerAcquires,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "outcome", Value: metrics.RuntimeOutcomeWarm}})
	metricsInstance.IncLabels(metrics.MetricRuntimeContainerDiscards,
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "reason", Value: "timeout"}})
	metricsInstance.ObserveDurationLabels(metrics.MetricRuntimeContainerAcquireDuration,
		[]metrics.Label{{Name: "function", Value: "demo"}}, 100*time.Millisecond)

	metricsServer := metrics.NewServer(fmt.Sprintf("127.0.0.1:%d", port), metricsInstance.Handler(), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := metricsServer.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}

	// Bounded scrape-retry: the server starts asynchronously, so poll until it
	// responds rather than sleeping a fixed amount.
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
		if err == nil && resp.StatusCode == 200 {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = string(b)
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Metrics server never became scrapeable: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, want := range []string{
		"relay_events_matched_total 1",
		`handler_invocations_total{function="demo",handler="index.hi",outcome="success"} 1`,
		`handler_duration_seconds_count{function="demo",handler="index.hi"} 1`,
		"relay_pending_entries 3",
		"# TYPE",
		// Warm-container pool observability.
		`relay_runtime_pool_capacity{function="demo"} 2`,
		`relay_runtime_containers{function="demo",state="idle"} 1`,
		`relay_runtime_containers{function="demo",state="busy"} 1`,
		`relay_runtime_container_acquires_total{function="demo",outcome="cold"} 1`,
		`relay_runtime_container_acquires_total{function="demo",outcome="warm"} 1`,
		`relay_runtime_container_discards_total{function="demo",reason="timeout"} 1`,
		`relay_runtime_container_acquire_duration_seconds_count{function="demo"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, body)
		}
	}

	// Start bound synchronously, so the server was already serving during the
	// scrape loop above. Stop performs the bounded graceful shutdown.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := metricsServer.Stop(stopCtx); err != nil {
		t.Fatalf("server stop: %v", err)
	}
}

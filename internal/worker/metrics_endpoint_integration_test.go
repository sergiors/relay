//go:build integration

// This file exercises the metrics HTTP endpoint against a real Redis: it boots
// the worker-style metrics wiring (a metrics server on a free port fed from a
// real Redis) and scrapes /metrics to assert the Prometheus text exposition
// works end to end.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES Redis at REDIS_TEST_ADDR
// (default localhost:6379, matching compose.dev.yaml); a missing dependency fails
// the affected tests rather than skipping them. Start the documented dev
// dependencies with `docker compose -f compose.dev.yaml up -d`.
package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"relay/internal/metrics"
)

// TestIntegrationMetricsEndpoint boots the worker wiring (excluding the real
// Consume loop, which needs /functions + Docker) with a free metrics port and a
// real Redis, then scrapes /metrics and asserts the Prometheus exposition works
// end-to-end.
func TestIntegrationMetricsEndpoint(t *testing.T) {
	requireRedis(t)

	// Free port for the metrics server.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	m := metrics.New()
	m.Inc("events_processed_total")
	m.IncLabels("handler_invocations_total", []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: "demo"}, {Name: "handler", Value: "index.hi"}})
	m.ObserveDurationLabels("handler_duration_seconds",
		[]metrics.Label{{Name: "function", Value: "demo"}, {Name: "handler", Value: "index.hi"}}, 250*time.Millisecond)
	m.SetGauge("pending_entries", 3)

	sctx, scancel := context.WithCancel(context.Background())
	t.Cleanup(scancel)
	done := make(chan error, 1)
	go func() { done <- m.ServeHTTP(sctx, fmt.Sprintf("127.0.0.1:%d", port), log.New(os.Stderr, "", 0).Printf) }()

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
			t.Fatalf("metrics server never became scrapeable: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, want := range []string{
		"events_processed_total 1",
		`handler_invocations_total{function="demo",handler="index.hi",outcome="success"} 1`,
		`handler_duration_seconds_count{function="demo",handler="index.hi"} 1`,
		"pending_entries 3",
		"# TYPE",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, body)
		}
	}

	scancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("server error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("server did not stop on cancel")
	}
}

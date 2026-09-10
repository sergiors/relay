//go:build integration

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

	"github.com/redis/go-redis/v9"

	"relay/internal/metrics"
)

// TestIntegrationMetricsEndpoint boots the worker wiring (excluding the real
// Consume loop, which needs /functions + Docker) with a free metrics port and a
// real Redis, then scrapes /metrics and asserts the Prometheus exposition works
// end-to-end.
func TestIntegrationMetricsEndpoint(t *testing.T) {
	probe := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer probe.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}

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
	done := make(chan error, 1)
	go func() { done <- m.ServeHTTP(sctx, fmt.Sprintf("127.0.0.1:%d", port), log.New(os.Stderr, "", 0).Printf) }()
	time.Sleep(300 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	for _, want := range []string{
		"events_processed_total 1",
		`handler_invocations_total{function="demo",handler="index.hi",outcome="success"} 1`,
		`handler_duration_seconds_count{function="demo",handler="index.hi"} 1`,
		"pending_entries 3",
		"# TYPE",
	} {
		if !strings.Contains(string(body), want) {
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

package worker

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"relay/internal/config"
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

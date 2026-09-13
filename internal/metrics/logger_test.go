package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsLoggerTicksAndStopsOnCancel(t *testing.T) {
	r := New()
	r.Inc("events_received_total")

	var mu sync.Mutex
	var lines []string
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		NewMetricsLogger(r, 10*time.Millisecond, logf).Start(ctx)
	}()

	// Wait for at least one tick to emit.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(lines)
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("MetricsLogger never logged")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("MetricsLogger did not stop on cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(lines[0], "Metrics events_received_total count=1") {
		t.Fatalf("unexpected first log line: %q", lines[0])
	}
}

func TestMetricsLoggerNilRegistryExitsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		NewMetricsLogger(nil, time.Hour, func(string, ...any) { t.Error("must not log on nil registry") }).Start(ctx)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nil MetricsLogger did not stop on cancel")
	}
}

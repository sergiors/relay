package worker

import (
	"context"
	"testing"
	"time"

	"relay/internal/metrics"
)

// TestStatsLoopNilStateExitsOnCancel ensures the snapshot loop is nil-safe on
// the state handle and stops promptly on cancel.
func TestStatsLoopNilStateExitsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		statsLoop(ctx, newStatsFlusher(nil, metrics.New()), time.Hour)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not stop on cancel")
	}
}

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// stubTrimmer is a test double for streamTrimmer. It records every
// XTrimMinIDApprox call (stream key and cutoff ID) and returns a canned result
// (value or error) so unit tests never need a real Redis.
type stubTrimmer struct {
	mu       sync.Mutex
	streams  []string
	cutoffID []string
	val      int64
	err      error
}

func (s *stubTrimmer) XTrimMinIDApprox(ctx context.Context, key string, minID string, limit int64) *redis.IntCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams = append(s.streams, key)
	s.cutoffID = append(s.cutoffID, minID)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(s.val)
	if s.err != nil {
		cmd.SetErr(s.err)
	}
	return cmd
}

func (s *stubTrimmer) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *stubTrimmer) lastStream() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.streams) == 0 {
		return ""
	}
	return s.streams[len(s.streams)-1]
}

func (s *stubTrimmer) lastCutoffID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.cutoffID) == 0 {
		return ""
	}
	return s.cutoffID[len(s.cutoffID)-1]
}

func discardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

// TestRetentionTickInterval pins the pure interval derivation: 6h → 15m, small
// values clamp to the 1m floor, huge values clamp to the 1h ceiling.
func TestRetentionTickInterval(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
		want      time.Duration
	}{
		{"6h -> 15m", 6 * time.Hour, 15 * time.Minute},
		{"1h -> 2m30s", time.Hour, 2*time.Minute + 30*time.Second},
		{"small clamps to 1m floor", time.Minute, time.Minute},
		{"tiny clamps to 1m floor", time.Second, time.Minute},
		{"huge clamps to 1h ceiling", 48 * time.Hour, time.Hour},
		{"very huge clamps to 1h ceiling", 30 * 24 * time.Hour, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retentionTickInterval(tt.retention); got != tt.want {
				t.Fatalf("retentionTickInterval(%v) = %v, want %v", tt.retention, got, tt.want)
			}
		})
	}
}

// TestRetentionCutoffID pins the Stream ID rendering: a fixed time becomes the
// exact "<unix-milliseconds>-0" string.
func TestRetentionCutoffID(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	want := "1789041600000-0"
	if got := retentionCutoffID(now); got != want {
		t.Fatalf("retentionCutoffID(%v) = %q, want %q", now, got, want)
	}
}

// TestRetentionTickTrimsConfiguredStream verifies retentionTick issues a trim
// against the configured stream with the correct approximate-MINID cutoff —
// (now - retention) rendered as the "<unix-milliseconds>-0" form — using an
// injected now for determinism.
func TestRetentionTickTrimsConfiguredStream(t *testing.T) {
	stub := &stubTrimmer{val: 3}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	retention := 6 * time.Hour // cutoff = now - 6h
	retentionTick(context.Background(), stub, "relay:events", retention, func() time.Time { return now }, discardLogger())

	if got := stub.calls(); got != 1 {
		t.Fatalf("trim calls = %d, want 1", got)
	}
	if got := stub.lastStream(); got != "relay:events" {
		t.Fatalf("trimmed stream = %q, want %q", got, "relay:events")
	}
	// now(2026-09-10 12:00 UTC) - 6h = 06:00 UTC = 1789019... millis.
	wantCutoff := retentionCutoffID(now.Add(-retention))
	if got := stub.lastCutoffID(); got != wantCutoff {
		t.Fatalf("cutoff ID = %q, want %q", got, wantCutoff)
	}
	// Pin the exact "<unix-milliseconds>-0" rendering too (catches a future
	// regression that forgets to subtract the retention window — the cutoff
	// must be 6h earlier than now, not now itself).
	wantMillis := now.Add(-retention).UnixMilli()
	if want := fmt.Sprintf("%d-0", wantMillis); wantCutoff != want {
		t.Fatalf("cutoff ID %q is not the expected %q", wantCutoff, want)
	}
}

// TestRetentionTickErrorLoggedAndRetried verifies a Redis trim failure is
// logged and swallowed (never panics, never stops the worker), and that a
// subsequent tick retries the trim.
func TestRetentionTickErrorLoggedAndRetried(t *testing.T) {
	stub := &stubTrimmer{err: errors.New("redis down")}
	now := func() time.Time { return time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC) }

	// Two ticks, both failing: the loop must survive and retry.
	retentionTick(context.Background(), stub, "relay:events", time.Hour, now, discardLogger())
	retentionTick(context.Background(), stub, "relay:events", time.Hour, now, discardLogger())

	if got := stub.calls(); got != 2 {
		t.Fatalf("trim calls = %d, want 2 (retried on next tick)", got)
	}
}

// TestRetentionLoopStopsOnCancel verifies the loop exits promptly when ctx is
// cancelled. The initial trim runs first (bounded), then the select observes
// ctx.Done before the first tick, so cancellation returns fast.
func TestRetentionLoopStopsOnCancel(t *testing.T) {
	stub := &stubTrimmer{val: 0}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		retentionLoop(ctx, stub, "relay:events", time.Hour, discardLogger())
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retentionLoop did not stop on cancel")
	}
}

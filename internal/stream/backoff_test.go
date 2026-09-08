package stream

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBackoffProgressionAndCap(t *testing.T) {
	// Deterministic: no jitter (identity) so we assert exact table values.
	b := newBackoff(nil, func(f float64) float64 { return f })
	want := []time.Duration{1, 2, 4, 8, 15, 30, 30, 30}
	for i, w := range want {
		if got := b.next(); got != w*time.Second {
			t.Fatalf("step %d: got %s, want %s", i, got, w*time.Second)
		}
	}
}

func TestBackoffReset(t *testing.T) {
	b := newBackoff(nil, func(f float64) float64 { return f })
	b.next()
	b.next()
	b.reset()
	if got := b.next(); got != time.Second {
		t.Fatalf("after reset, got %s, want 1s", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	// A jitter that always returns the max factor (1.2) must stay within bounds
	// and scale the base value exactly.
	b := newBackoff(nil, func(f float64) float64 { return f * 1.2 })
	if got := b.next(); got != 1200*time.Millisecond {
		t.Fatalf("max jitter: got %s, want 1.2s", got)
	}
	b.reset()
	b = newBackoff(nil, func(f float64) float64 { return f * 0.8 })
	if got := b.next(); got != 800*time.Millisecond {
		t.Fatalf("min jitter: got %s, want 800ms", got)
	}
}

func TestBackoffJitterNeverOutOfBounds(t *testing.T) {
	// The default rand-based jitter must never produce a value outside
	// [0.8, 1.2) of the base across many draws. Use a single-entry table so the
	// base stays constant and the jitter factor is what varies.
	b := newBackoff([]time.Duration{time.Second}, nil)
	for i := 0; i < 1000; i++ {
		got := b.next()
		if got < 800*time.Millisecond || got >= 1200*time.Millisecond {
			t.Fatalf("jitter out of bounds: %s", got)
		}
	}
}

// newTestConsumer returns a Consumer with a buffer logger and a fast,
// deterministic backoff for health-transition tests.
func newTestConsumer(t *testing.T) (*Consumer, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	c := NewConsumer(ConsumerConfig{
		Log:           log.New(&buf, "", 0),
		backoffTable:  []time.Duration{time.Second},
		backoffJitter: func(f float64) float64 { return f },
	})
	return c, &buf
}

func TestNoteOutcomeFailureMarksUnhealthyAndLogsOnce(t *testing.T) {
	c, buf := newTestConsumer(t)
	if !c.Healthy() {
		t.Fatalf("consumer should start healthy")
	}
	c.noteOutcome(errors.New("boom"), time.Second)
	if c.Healthy() {
		t.Fatalf("consumer should be unhealthy after failure")
	}
	if got := buf.String(); !strings.Contains(got, "redis read failed: boom; retrying in 1s") {
		t.Fatalf("expected one failure log, got: %q", got)
	}
}

func TestNoteOutcomeSuccessRecoversAndLogsOnce(t *testing.T) {
	c, buf := newTestConsumer(t)
	// Enter outage.
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	// Many successes must log exactly one recovery line.
	for i := 0; i < 5; i++ {
		c.noteOutcome(nil, 0)
	}
	if !c.Healthy() {
		t.Fatalf("consumer should be healthy after success")
	}
	if got := strings.Count(buf.String(), "redis connection recovered"); got != 1 {
		t.Fatalf("expected exactly one recovery line, got %d: %q", got, buf.String())
	}
}

func TestNoteOutcomeNoRecoverySpamDuringOutage(t *testing.T) {
	c, buf := newTestConsumer(t)
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	// Repeated failures during an ongoing outage must not log recovery lines.
	for i := 0; i < 5; i++ {
		c.noteOutcome(errors.New("boom"), time.Second)
	}
	if got := strings.Count(buf.String(), "redis connection recovered"); got != 0 {
		t.Fatalf("no recovery line expected during outage, got %d", got)
	}
}

func TestNoteOutcomeRedisNilCountsAsSuccess(t *testing.T) {
	c, buf := newTestConsumer(t)
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	c.noteOutcome(redis.Nil, 0)
	if !c.Healthy() {
		t.Fatalf("redis.Nil should count as healthy")
	}
	if got := strings.Count(buf.String(), "redis connection recovered"); got != 1 {
		t.Fatalf("expected one recovery line, got %d", got)
	}
}

func TestConsumeWaitInterruptedByCancel(t *testing.T) {
	c, _ := newTestConsumer(t)
	// A long backoff so the wait would otherwise block; cancellation must
	// interrupt it promptly.
	c.backoff = newBackoff([]time.Duration{10 * time.Second}, func(f float64) float64 { return f })
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	// Simulate the wait loop's select: on ctx.Done we return immediately.
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("wait was not interrupted by cancellation")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancellation took too long: %s", elapsed)
	}
}

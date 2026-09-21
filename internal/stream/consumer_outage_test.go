package stream

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// newDeadRedisConsumer returns a Consumer pointed at a TCP address with no
// listener. Every Redis operation fails (connect refused), which exercises the
// backoff/health path without needing a real server.
func newDeadRedisConsumer(t *testing.T, buf *testutil.SyncBuffer, backoff time.Duration) *Consumer {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", testutil.FreePort(t))
	// Fail fast: no command retries and a short dial timeout, so the consumer
	// observes the outage immediately instead of paying go-redis's default
	// retry/backoff budget on every read.
	cli := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = cli.Close() })
	return NewConsumer(ConsumerConfig{
		Client:        cli,
		Stream:        "outage-stream",
		Group:         "outage-group",
		Consumer:      "outage-consumer",
		Log:           slog.New(slog.NewTextHandler(buf, nil)),
		backoffTable:  []time.Duration{backoff},
		backoffJitter: func(f float64) float64 { return f },
	})
}

// TestConsumeSurvivesOutage verifies that a consumer pointed at a Redis address
// with no listener keeps running (backing off) rather than exiting, marks itself
// unhealthy, logs the failure once, and returns nil on cancellation.
func TestConsumeSurvivesOutage(t *testing.T) {
	buf := &testutil.SyncBuffer{}
	c := newDeadRedisConsumer(t, buf, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = c.Consume(ctx, func(ctx context.Context, msgID string, ev map[string]any) error { return nil })
	}()

	// The loop must fail and back off repeatedly while staying up. The bounded
	// poll replaces a fixed grace-period sleep: it returns as soon as the
	// failure is observed, and the sustained hold proves the consumer did not
	// exit or flip back to healthy between polls.
	testutil.WaitFor(t, 5*time.Second, "consumer unhealthy during outage", func() bool { return !c.Healthy() })
	waitSustained(t, "consumer stays unhealthy and running during outage", 300*time.Millisecond, func() bool {
		select {
		case <-done:
			return false
		default:
			return !c.Healthy()
		}
	})
	if !strings.Contains(buf.String(), "Redis: read failed; retrying") {
		t.Fatalf("expected backoff failure log, got: %q", buf.String())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop on cancellation")
	}
	if err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
}

// TestRecoveryLoopStopsOnCancel verifies Consume joins its recovery goroutine on
// cancellation: with a reclaim interval configured, Consume must return promptly
// once ctx is cancelled (run under -race to catch a leaked goroutine). It needs
// no live Redis because the dead-address client only ever errors out.
func TestRecoveryLoopStopsOnCancel(t *testing.T) {
	buf := &testutil.SyncBuffer{}
	c := newDeadRedisConsumer(t, buf, 20*time.Millisecond)
	// A fast reclaim tick exercises the recovery goroutine before shutdown.
	c.reclaimInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, func(ctx context.Context, msgID string, ev map[string]any) error { return nil })
	}()

	// Let the loop and recovery goroutine run, then cancel.
	waitSustained(t, "consumer unhealthy before shutdown", 100*time.Millisecond, func() bool { return !c.Healthy() })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop on cancellation (recovery goroutine leaked?)")
	}
}

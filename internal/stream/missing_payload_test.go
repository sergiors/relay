package stream

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// newMissingPayloadConsumer builds a Consumer whose Redis client points at a
// dead address (no listener). Every Redis command fails fast, which is enough
// for the missing-value path: the guard must never reach the handler, and the
// failing XAck is intentionally logged-and-swallowed. It carries a real metrics
// registry so the anomaly counter can be asserted.
func newMissingPayloadConsumer(t *testing.T, m *metrics.Registry) *Consumer {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", testutil.FreePort(t))
	cli := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = cli.Close() })
	return NewConsumer(ConsumerConfig{
		Client:   cli,
		Stream:   "missing-stream",
		Group:    "missing-group",
		Consumer: "missing-consumer",
		Log:      slog.New(slog.DiscardHandler),
		Metrics:  m,
	})
}

// TestProcessMessageMissingValuesNeverRunsHandler pins the defensive guard: a
// message with nil values (the shape Redis 6.2 XAUTOCLAIM returns for a PEL
// entry whose body was trimmed/XDEL'd) must NOT be classified, NOT be handed to
// a handler, and NOT be dead-lettered as a malformed message. It increments the
// missing_payload counter and records the "missing" outcome, clearing the
// dangling PEL reference instead.
func TestProcessMessageMissingValuesNeverRunsHandler(t *testing.T) {
	m := metrics.New()
	c := newMissingPayloadConsumer(t, m)

	var handlerCalls atomic.Int64
	handler := func(ctx context.Context, msgID string, ev map[string]any) error {
		handlerCalls.Add(1)
		return nil
	}

	// A nil-values message is exactly what XAUTOCLAIM returns when the body is
	// gone; the dead Redis client makes the conservative XAck fail harmlessly.
	c.processMessage(context.Background(), redis.XMessage{ID: "1700000000000-1"}, 3, handler)

	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler called %d times for a body-less message, want 0", got)
	}
	if got := m.Counter(metrics.MetricMissingPayload); got != 1 {
		t.Fatalf("missing_payload_total = %d, want 1", got)
	}
	// The guard must not count the entry as a retry (that counter counts real
	// message redeliveries) nor as a DLQ entry.
	if got := m.Counter(metrics.MetricRetries); got != 0 {
		t.Fatalf("retries_total = %d, want 0 (a body-less entry is not a redelivery)", got)
	}
	if got := m.Counter(metrics.MetricDLQEntries); got != 0 {
		t.Fatalf("dlq_entries_total = %d, want 0 (a body-less entry is not dead-lettered)", got)
	}
}

// TestClearMissingValueEntryNilRegistryIsSafe pins that the missing-value path
// is nil-metric-safe: a Consumer with no metrics registry still never panics or
// reaches a handler.
func TestClearMissingValueEntryNilRegistryIsSafe(t *testing.T) {
	c := newMissingPayloadConsumer(t, nil)
	var handlerCalls atomic.Int64
	c.processMessage(context.Background(), redis.XMessage{ID: "1700000000000-2"}, 1,
		func(ctx context.Context, msgID string, ev map[string]any) error {
			handlerCalls.Add(1)
			return nil
		})
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler called %d times with a nil registry, want 0", got)
	}
}

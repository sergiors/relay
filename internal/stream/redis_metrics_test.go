package stream

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
)

// TestRecordRedisReadErrorOperationLabels verifies the helper increments the
// redis read-error counter exactly once per genuine failure, labeled by the
// given operation, and that distinct operations are distinct series.
func TestRecordRedisReadErrorOperationLabels(t *testing.T) {
	m := metrics.New()

	recordRedisReadError(m, metrics.RedisOpReadGroup, errors.New("connection refused"))
	recordRedisReadError(m, metrics.RedisOpPending, errors.New("boom"))
	recordRedisReadError(m, metrics.RedisOpPending, errors.New("boom again"))
	recordRedisReadError(m, metrics.RedisOpAutoclaim, errors.New("boom"))
	recordRedisReadError(m, metrics.RedisOpPendingGauge, errors.New("boom"))

	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpReadGroup}}); got != 1 {
		t.Fatalf("read_group = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpPending}}); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpAutoclaim}}); got != 1 {
		t.Fatalf("autoclaim = %d, want 1", got)
	}
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpPendingGauge}}); got != 1 {
		t.Fatalf("pending_gauge = %d, want 1", got)
	}
}

// TestRecordRedisReadErrorExcludesBenignOutcomes pins that redis.Nil (a blocking
// read timeout), context cancellation, and context deadline are NOT counted as
// Redis read failures, and that a nil registry is a no-op.
func TestRecordRedisReadErrorExcludesBenignOutcomes(t *testing.T) {
	m := metrics.New()
	recordRedisReadError(m, metrics.RedisOpReadGroup, redis.Nil)
	recordRedisReadError(m, metrics.RedisOpReadGroup, context.Canceled)
	recordRedisReadError(m, metrics.RedisOpReadGroup, context.DeadlineExceeded)
	recordRedisReadError(m, metrics.RedisOpReadGroup, nil)

	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpReadGroup}}); got != 0 {
		t.Fatalf("benign outcomes must not be counted, got %d", got)
	}

	var nilReg *metrics.Registry
	recordRedisReadError(nilReg, metrics.RedisOpReadGroup, errors.New("boom"))
}

// TestRecordRedisReadErrorNoErrorLabel pins that the counter only ever carries
// the operation label — never the raw error text, which would be unbounded.
func TestRecordRedisReadErrorNoErrorLabel(t *testing.T) {
	m := metrics.New()
	recordRedisReadError(m, metrics.RedisOpPending, errors.New("unique-error-text-12345"))
	s := m.Snapshot()
	if strings.Contains(s, "unique-error-text-12345") {
		t.Fatalf("raw error text must never be a label:\n%s", s)
	}
	if !strings.Contains(s, "redis_read_errors_total{operation=pending}") {
		t.Fatalf("expected the pending operation series:\n%s", s)
	}
}

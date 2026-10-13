package stream

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// fakePendingSummarizer is the pendingSummarizer seam: it records the exact
// stream/group the sampler queries and returns a canned XPENDING summary (or
// error), so the depth/age gauge semantics are unit-testable without Redis.
type fakePendingSummarizer struct {
	val        *redis.XPending
	err        error
	calls      int
	lastStream string
	lastGroup  string
}

func (f *fakePendingSummarizer) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	f.calls++
	f.lastStream, f.lastGroup = stream, group
	cmd := redis.NewXPendingCmd(ctx)
	cmd.SetVal(f.val)
	cmd.SetErr(f.err)
	return cmd
}

// newPendingGaugeTestSource wires a PendingGaugeSource to the fake seam with a
// discard logger and the given registry. Refresh is called synchronously, so
// the tests need no sleeps or polling.
func newPendingGaugeTestSource(f *fakePendingSummarizer, m *metrics.Registry) *PendingGaugeSource {
	return &PendingGaugeSource{
		client:  f,
		stream:  "events",
		group:   "workers",
		metrics: m,
		log:     slog.New(slog.DiscardHandler),
	}
}

// TestPendingGaugeSourceRefreshRecordsDepthAndAge pins the happy path: one
// XPENDING query per Refresh, issued for the source's stream/group, recording
// the pending depth and the age of the oldest pending ID.
func TestPendingGaugeSourceRefreshRecordsDepthAndAge(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{val: &redis.XPending{Count: 1, Lower: "1234567-0", Higher: "1234567-0"}}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())

	if f.calls != 1 {
		t.Fatalf("XPENDING calls = %d, want 1", f.calls)
	}
	if f.lastStream != "events" || f.lastGroup != "workers" {
		t.Fatalf("queried (%q, %q), want (events, workers)", f.lastStream, f.lastGroup)
	}
	if got := m.Gauge(metrics.MetricPendingEntries); got != 1 {
		t.Fatalf("pending_entries = %v, want 1", got)
	}
	got := m.Gauge(metrics.MetricPendingOldestAge)
	want := time.Since(time.UnixMilli(1234567)).Seconds()
	if got <= 0 {
		t.Fatalf("pending_oldest_age_seconds = %v, want > 0", got)
	}
	if got < want-2 || got > want+2 {
		t.Fatalf("pending_oldest_age_seconds = %v, want ~%v", got, want)
	}
}

// TestPendingGaugeSourceRefreshEmptyPELZeroesDepthAndAge is the regression for
// the stale age gauge: once the PEL empties (every message acked) XPENDING
// returns Count 0 with an empty oldest ID, and the sampler must set BOTH the
// depth and the oldest age to zero rather than keep the last non-zero age.
func TestPendingGaugeSourceRefreshEmptyPELZeroesDepthAndAge(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{val: &redis.XPending{Count: 2, Lower: "1234567-0"}}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricPendingEntries); got != 2 {
		t.Fatalf("setup: pending_entries = %v, want 2", got)
	}
	if got := m.Gauge(metrics.MetricPendingOldestAge); got <= 0 {
		t.Fatalf("setup: pending_oldest_age_seconds = %v, want > 0", got)
	}

	// Both pending messages are acked before the next sample: the summary has no
	// oldest ID (empty Lower).
	f.val = &redis.XPending{Count: 0, Lower: ""}
	src.Refresh(context.Background())

	if f.calls != 2 {
		t.Fatalf("XPENDING calls = %d, want 2 (exactly one per Refresh)", f.calls)
	}
	if got := m.Gauge(metrics.MetricPendingEntries); got != 0 {
		t.Fatalf("pending_entries after empty PEL = %v, want 0", got)
	}
	if got := m.Gauge(metrics.MetricPendingOldestAge); got != 0 {
		t.Fatalf("pending_oldest_age_seconds after empty PEL = %v, want 0 (stale age must be cleared)", got)
	}

	// The zeroed gauges must be visible on the wire, not only in the Registry's
	// typed getters: scrape the same exposition handler the worker serves and
	// assert the rendered sample is exactly 0.
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, metrics.MetricPendingOldestAge+" 0") {
		t.Fatalf("exposition missing %q:\n%s", metrics.MetricPendingOldestAge+" 0", body)
	}
	if !strings.Contains(body, metrics.MetricPendingEntries+" 0") {
		t.Fatalf("exposition missing %q:\n%s", metrics.MetricPendingEntries+" 0", body)
	}
}

// TestPendingGaugeSourceRefreshErrorDoesNotZeroGauges pins that a XPENDING
// failure preserves the previously sampled gauges (fail open, no false zero):
// the read error is counted and the depth/age are left untouched.
func TestPendingGaugeSourceRefreshErrorDoesNotZeroGauges(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{val: &redis.XPending{Count: 3, Lower: "1234567-0"}}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())
	depth := m.Gauge(metrics.MetricPendingEntries)
	age := m.Gauge(metrics.MetricPendingOldestAge)
	if depth != 3 || age <= 0 {
		t.Fatalf("setup: depth = %v, age = %v, want (3, >0)", depth, age)
	}

	f.err = errors.New("connection refused")
	f.val = nil
	src.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricPendingEntries); got != depth {
		t.Fatalf("pending_entries after read error = %v, want %v (preserved)", got, depth)
	}
	if got := m.Gauge(metrics.MetricPendingOldestAge); got != age {
		t.Fatalf("pending_oldest_age_seconds after read error = %v, want %v (preserved)", got, age)
	}
	labels := []metrics.Label{{Name: "operation", Value: metrics.RedisOpPendingGauge}}
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, labels); got != 1 {
		t.Fatalf("pending_gauge read errors = %d, want 1", got)
	}
}

// TestPendingGaugeSourceRefreshUnparsableOldestIDKeepsPriorAge pins the
// non-empty-PEL semantics exactly as they were: the depth is always recorded,
// but an unparsable oldest ID leaves the prior age rather than zeroing it (only
// an empty PEL zeroes the age).
func TestPendingGaugeSourceRefreshUnparsableOldestIDKeepsPriorAge(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{val: &redis.XPending{Count: 1, Lower: "1234567-0"}}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())
	age := m.Gauge(metrics.MetricPendingOldestAge)
	if age <= 0 {
		t.Fatalf("setup: pending_oldest_age_seconds = %v, want > 0", age)
	}

	f.val = &redis.XPending{Count: 2, Lower: "not-a-stream-id"}
	src.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricPendingEntries); got != 2 {
		t.Fatalf("pending_entries = %v, want 2", got)
	}
	if got := m.Gauge(metrics.MetricPendingOldestAge); got != age {
		t.Fatalf("pending_oldest_age_seconds = %v, want %v (unchanged on unparsable ID)", got, age)
	}
}

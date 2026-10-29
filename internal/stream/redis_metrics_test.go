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
	recordRedisReadError(m, metrics.RedisOpGroupLag, errors.New("boom"))

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
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: metrics.RedisOpGroupLag}}); got != 1 {
		t.Fatalf("group_lag = %d, want 1", got)
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

func TestNewConsumerInitializesBufferedEventsGauge(t *testing.T) {
	m := metrics.New()
	NewConsumer(ConsumerConfig{Metrics: m})
	if got := m.Gauge(metrics.MetricBufferedEvents); got != 0 {
		t.Fatalf("buffered_events = %v after consumer construction, want 0", got)
	}
}

// fakePendingSummarizer is the streamMetricsReader seam: it records the exact
// stream/group the sampler queries and returns canned XPENDING / XINFO GROUPS /
// XLEN replies (or errors), so the depth/age/lag/DLQ gauge semantics are
// unit-testable without Redis. The three commands have independent result and
// error fields so one failing command can be exercised without disturbing the
// others.
type fakePendingSummarizer struct {
	val        *redis.XPending
	err        error
	calls      int
	lastStream string
	lastGroup  string

	groups    []redis.XInfoGroup
	groupErr  error
	infoCalls int
	lastInfo  string

	lenVal   int64
	lenErr   error
	lenCalls int
	lastLen  string
}

func (f *fakePendingSummarizer) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	f.calls++
	f.lastStream, f.lastGroup = stream, group
	cmd := redis.NewXPendingCmd(ctx)
	cmd.SetVal(f.val)
	cmd.SetErr(f.err)
	return cmd
}

func (f *fakePendingSummarizer) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	f.infoCalls++
	f.lastInfo = stream
	cmd := redis.NewXInfoGroupsCmd(ctx, stream)
	cmd.SetVal(f.groups)
	cmd.SetErr(f.groupErr)
	return cmd
}

func (f *fakePendingSummarizer) XLen(ctx context.Context, stream string) *redis.IntCmd {
	f.lenCalls++
	f.lastLen = stream
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(f.lenVal)
	cmd.SetErr(f.lenErr)
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

// TestPendingGaugeSourceRefreshRecordsGroupLag pins the lag happy path: exactly
// one XINFO GROUPS per Refresh, issued for the source's configured stream, and
// the CONFIGURED group's lag is recorded (a second group's lag is ignored).
func TestPendingGaugeSourceRefreshRecordsGroupLag(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{
		val: &redis.XPending{},
		groups: []redis.XInfoGroup{
			{Name: "other", Lag: 99},
			{Name: "workers", Lag: 7},
		},
	}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())

	if f.infoCalls != 1 {
		t.Fatalf("XINFO GROUPS calls = %d, want 1", f.infoCalls)
	}
	if f.lastInfo != "events" {
		t.Fatalf("queried stream %q, want events", f.lastInfo)
	}
	if got := m.Gauge(metrics.MetricStreamGroupLag); got != 7 {
		t.Fatalf("stream_group_lag_entries = %v, want 7 (configured group only)", got)
	}
}

// TestPendingGaugeSourceRefreshGroupLagZeroIsRecorded pins that a Redis-reported
// lag of 0 (the group is fully caught up) IS recorded, distinct from an
// undeterminable lag.
func TestPendingGaugeSourceRefreshGroupLagZeroIsRecorded(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{
		val:    &redis.XPending{},
		groups: []redis.XInfoGroup{{Name: "workers", Lag: 5}},
	}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricStreamGroupLag); got != 5 {
		t.Fatalf("setup: stream_group_lag_entries = %v, want 5", got)
	}

	f.groups = []redis.XInfoGroup{{Name: "workers", Lag: 0}}
	src.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricStreamGroupLag); got != 0 {
		t.Fatalf("stream_group_lag_entries after catch-up = %v, want 0", got)
	}
}

// TestPendingGaugeSourceRefreshGroupLagUndeterminableKeepsPrior pins the
// no-false-zero rule for lag: a NULL/undeterminable lag (go-redis surfaces -1)
// and a missing configured group both leave the prior gauge value untouched.
func TestPendingGaugeSourceRefreshGroupLagUndeterminableKeepsPrior(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{
		val:    &redis.XPending{},
		groups: []redis.XInfoGroup{{Name: "workers", Lag: 4}},
	}
	src := newPendingGaugeTestSource(f, m)
	src.Refresh(context.Background())
	prior := m.Gauge(metrics.MetricStreamGroupLag)
	if prior != 4 {
		t.Fatalf("setup: stream_group_lag_entries = %v, want 4", prior)
	}

	// Undeterminable lag.
	f.groups = []redis.XInfoGroup{{Name: "workers", Lag: -1}}
	src.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricStreamGroupLag); got != prior {
		t.Fatalf("stream_group_lag_entries on undeterminable lag = %v, want %v (preserved)", got, prior)
	}

	// Missing configured group (only another group present).
	f.groups = []redis.XInfoGroup{{Name: "someone-else", Lag: 123}}
	src.Refresh(context.Background())
	if got := m.Gauge(metrics.MetricStreamGroupLag); got != prior {
		t.Fatalf("stream_group_lag_entries on missing group = %v, want %v (preserved)", got, prior)
	}
}

// TestPendingGaugeSourceRefreshGroupLagErrorCountsAndPreserves pins that an
// XINFO GROUPS failure is counted with its own operation label and preserves the
// prior lag gauge (fail open, no false zero) without affecting the pending read.
func TestPendingGaugeSourceRefreshGroupLagErrorCountsAndPreserves(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{
		val:    &redis.XPending{Count: 2, Lower: "1234567-0"},
		groups: []redis.XInfoGroup{{Name: "workers", Lag: 9}},
	}
	src := newPendingGaugeTestSource(f, m)
	src.Refresh(context.Background())
	prior := m.Gauge(metrics.MetricStreamGroupLag)
	if prior != 9 {
		t.Fatalf("setup: stream_group_lag_entries = %v, want 9", prior)
	}

	f.groupErr = errors.New("connection refused")
	src.Refresh(context.Background())

	if got := m.Gauge(metrics.MetricStreamGroupLag); got != prior {
		t.Fatalf("stream_group_lag_entries after error = %v, want %v (preserved)", got, prior)
	}
	labels := []metrics.Label{{Name: "operation", Value: metrics.RedisOpGroupLag}}
	if got := m.CounterLabels(metrics.MetricRedisReadErrors, labels); got != 1 {
		t.Fatalf("group_lag read errors = %d, want 1", got)
	}
	// The independent XPENDING observation still ran and recorded its depth.
	if got := m.Gauge(metrics.MetricPendingEntries); got != 2 {
		t.Fatalf("pending_entries = %v, want 2 (independent of the lag failure)", got)
	}
}

// TestPendingGaugeSourceRefreshIssuesOneCommandEach pins the per-tick command
// budget: one XPENDING and one XINFO GROUPS per Refresh. DLQ depth is sampled
// separately from PostgreSQL by the worker.
func TestPendingGaugeSourceRefreshIssuesOneCommandEach(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{val: &redis.XPending{}, groups: []redis.XInfoGroup{{Name: "workers", Lag: 1}}}
	src := newPendingGaugeTestSource(f, m)

	src.Refresh(context.Background())

	if f.calls != 1 || f.infoCalls != 1 {
		t.Fatalf("commands per refresh = xpending %d, xinfo %d; want 1/1",
			f.calls, f.infoCalls)
	}
}

// TestPendingGaugeSourceRefreshExposesStreamGauges pins that the lag gauge is
// visible on the wire through the same exposition handler the worker serves,
// not only in the Registry's typed getters.
func TestPendingGaugeSourceRefreshExposesStreamGauges(t *testing.T) {
	m := metrics.New()
	f := &fakePendingSummarizer{
		val:    &redis.XPending{},
		groups: []redis.XInfoGroup{{Name: "workers", Lag: 6}},
	}
	src := newPendingGaugeTestSource(f, m)
	src.Refresh(context.Background())

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, metrics.MetricStreamGroupLag+" 6") {
		t.Fatalf("exposition missing %q:\n%s", metrics.MetricStreamGroupLag+" 6", body)
	}
}

// fakeExtraGaugeSource is a metrics.GaugeSource standing in for a worker-global
// source registered via Consumer.AddGaugeSource (the scheduler's durable-outbox
// gauges are the production case). It records its registration order and writes
// one gauge so the composition test can prove it ran through the composed source
// path, with no loop and no sleep.
type fakeExtraGaugeSource struct {
	name    string
	order   *[]string
	metrics *metrics.Registry
	gauge   string
	value   float64
	calls   int
}

func (f *fakeExtraGaugeSource) Refresh(context.Context) {
	f.calls++
	*f.order = append(*f.order, f.name)
	f.metrics.SetGauge(f.gauge, f.value)
}

// TestConsumerGaugeSourcesComposition pins Consumer.AddGaugeSource and the
// gaugeSources composition: the consumer's own stream sampler is first and every
// registered extra follows in registration order, and driving the composed slice
// (exactly what Consume hands the Refresher) refreshes each source — the extras'
// gauges are written — while the stream sampler still records its gauges. It is
// fully deterministic: Refresh is called synchronously with the Redis seam faked,
// so there is no new loop and no sleep.
func TestConsumerGaugeSourcesComposition(t *testing.T) {
	m := metrics.New()
	c := NewConsumer(ConsumerConfig{
		Stream:  "events",
		Group:   "workers",
		Log:     slog.New(slog.DiscardHandler),
		Metrics: m,
	})

	var order []string
	first := &fakeExtraGaugeSource{name: "first", order: &order, metrics: m, gauge: metrics.MetricBufferedEvents, value: 3}
	second := &fakeExtraGaugeSource{name: "second", order: &order, metrics: m, gauge: metrics.MetricInFlightInvocations, value: 5}
	// A nil extra is ignored by AddGaugeSource and must not appear in the slice.
	c.AddGaugeSource(nil)
	c.AddGaugeSource(first)
	c.AddGaugeSource(second)

	sources := c.gaugeSources()
	if len(sources) != 3 {
		t.Fatalf("composed sources = %d, want 3 (stream sampler + two extras)", len(sources))
	}
	stream, ok := sources[0].(*PendingGaugeSource)
	if !ok {
		t.Fatalf("sources[0] = %T, want *PendingGaugeSource (stream sampler first)", sources[0])
	}
	if sources[1] != metrics.GaugeSource(first) || sources[2] != metrics.GaugeSource(second) {
		t.Fatalf("composed extras = [%T, %T], want the registered extras in order", sources[1], sources[2])
	}

	// Point the stream sampler at the fake Redis seam so the WHOLE composed slice
	// can be driven without a Redis server.
	f := &fakePendingSummarizer{val: &redis.XPending{Count: 4}}
	stream.client = f

	// Drive each composed source once, in order, exactly as the Refresher does.
	for _, src := range sources {
		src.Refresh(context.Background())
	}

	if first.calls != 1 || second.calls != 1 {
		t.Fatalf("extra refreshes = (%d, %d), want (1, 1) through the composed path", first.calls, second.calls)
	}
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("refresh order = %v, want [first second]", order)
	}
	if got := m.Gauge(metrics.MetricBufferedEvents); got != 3 {
		t.Fatalf("first extra gauge = %v, want 3", got)
	}
	if got := m.Gauge(metrics.MetricInFlightInvocations); got != 5 {
		t.Fatalf("second extra gauge = %v, want 5", got)
	}
	if got := m.Gauge(metrics.MetricPendingEntries); got != 4 {
		t.Fatalf("stream sampler pending_entries = %v, want 4 (stream sampler still runs)", got)
	}

	// No-label behavior preserved: the stream sampler's gauge stays unlabeled on
	// the wire alongside the registered extra's gauge.
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, metrics.MetricPendingEntries+" 4") {
		t.Fatalf("exposition missing unlabeled %q:\n%s", metrics.MetricPendingEntries+" 4", body)
	}
	if strings.Contains(body, metrics.MetricPendingEntries+"{") {
		t.Fatalf("stream gauge must remain unlabeled:\n%s", body)
	}
}

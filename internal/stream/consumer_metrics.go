package stream

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
)

// AddGaugeSource registers src as an additional periodic gauge source that the
// consumer's metrics refresher drives on the same interval as its own stream
// sampler (see Consume). It exists so a worker-global source that does not
// belong to the stream layer — e.g. the scheduler's durable-outbox gauges — can
// piggyback on the ONE existing periodic observation goroutine instead of
// starting a second ticker. It must be called before Consume; it is not safe to
// call concurrently with Consume. A nil src or nil receiver is ignored. It has
// no effect when no metrics registry is configured: the refresher only runs
// when Metrics is non-nil.
func (c *Consumer) AddGaugeSource(src metrics.GaugeSource) {
	if c == nil || src == nil {
		return
	}
	c.extraGaugeSources = append(c.extraGaugeSources, src)
}

// gaugeSources returns the ordered GaugeSources the metrics refresher drives:
// the consumer's own stream sampler first, then any extra sources registered via
// AddGaugeSource. It is called once from Consume.
func (c *Consumer) gaugeSources() []metrics.GaugeSource {
	sources := make([]metrics.GaugeSource, 0, 1+len(c.extraGaugeSources))
	sources = append(sources, c.streamGaugeSource())
	sources = append(sources, c.extraGaugeSources...)
	return sources
}

// streamGaugeSource builds a metrics.GaugeSource that samples the Redis
// pending-backlog and consumer-group-lag gauges. It needs the consumer's client,
// stream, and group, and reuses the consumer's metrics registry and logger, so it
// is constructed from the consumer rather than at the package level.
func (c *Consumer) streamGaugeSource() metrics.GaugeSource {
	return &PendingGaugeSource{
		client:  c.client,
		stream:  c.stream,
		group:   c.group,
		metrics: c.metrics,
		log:     c.log,
	}
}

// streamMetricsReader is the narrow Redis view the periodic stream-metrics
// sampler needs: the XPENDING summary (pending depth/oldest) and XINFO GROUPS
// (the configured consumer group's lag). *redis.Client satisfies it; a test fake
// can implement it too, so both gauge semantics are unit-testable without a
// Redis server (the same seam pattern as groupCreator).
type streamMetricsReader interface {
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd
}

// The production Redis client must satisfy the stream-metrics seam.
var _ streamMetricsReader = (*redis.Client)(nil)

// PendingGaugeSource is a metrics.GaugeSource that samples the Redis stream
// gauges on each Refresh: the XPENDING summary (pending depth and oldest-pending
// age) and the configured consumer group's XINFO GROUPS lag. It is decoupled
// from health/backoff/processing: a failing Redis command only skips its own
// gauge for that tick (recorded on the read-error counter) and is logged
// quietly, never zeroed and never propagated. The overall registry snapshot is
// exposed by the worker's MetricsLogger. DLQ depth is no longer a Redis stream
// observation: the durable DLQ lives in PostgreSQL and is sampled by the
// worker's own gauge source.
type PendingGaugeSource struct {
	client  streamMetricsReader
	stream  string
	group   string
	metrics *metrics.Registry
	log     *slog.Logger
}

// Refresh samples the stream gauges. It satisfies metrics.GaugeSource. Each of
// the three observations runs INDEPENDENTLY: one failing Redis command never
// suppresses the others'. It never blocks processing: it runs in its own
// goroutine (via the Refresher), holds no locks across the Redis calls, and any
// error is logged and skipped, never propagated.
func (source *PendingGaugeSource) Refresh(ctx context.Context) {
	// The group-lag observation runs AFTER the pending sample, whatever its
	// outcome: the pending body below returns early on a read error or an empty
	// PEL, and a failure of one stream command must never suppress the other.
	defer func() {
		source.refreshGroupLag(ctx)
	}()

	// The summary form (no Start/End/Count) is O(1)-ish and returns the total
	// Count plus the oldest pending message ID in Lower.
	pending, err := source.client.XPending(ctx, source.stream, source.group).Result()
	if err != nil {
		recordRedisReadError(source.metrics, metrics.RedisOpPendingGauge, err)
		source.log.Debug("Metrics: xpending failed", "stream", source.stream, "group", source.group, "error", err)
		return
	}
	source.metrics.SetGauge(metrics.MetricPendingEntries, float64(pending.Count))
	// An empty PEL has no oldest pending ID to parse (Lower is ""), so the age
	// must be cleared explicitly; otherwise it would keep the last non-empty
	// value and report a pending age for a group with nothing pending. A
	// non-empty PEL keeps the existing behavior: the age is updated only when
	// the oldest ID parses.
	if pending.Count == 0 {
		source.metrics.SetGauge(metrics.MetricPendingOldestAge, 0)
		return
	}
	if age, ok := pendingAge(pending.Lower); ok {
		source.metrics.SetGauge(metrics.MetricPendingOldestAge, age.Seconds())
	}
}

// refreshGroupLag reads XINFO GROUPS for the configured stream and records the
// configured group's lag. A lag Redis reports as undeterminable (go-redis
// surfaces the NULL lag as -1) is left UNOBSERVED rather than recorded as zero:
// zero would falsely claim the group is fully caught up. A group absent from the
// reply is likewise left unobserved — an empty/missing scope must never invent a
// zero. A genuine command error follows the existing convention: counted by the
// finite operation label, logged, and the prior gauge value preserved.
func (source *PendingGaugeSource) refreshGroupLag(ctx context.Context) {
	groups, err := source.client.XInfoGroups(ctx, source.stream).Result()
	if err != nil {
		recordRedisReadError(source.metrics, metrics.RedisOpGroupLag, err)
		source.log.Debug("Metrics: xinfo groups failed", "stream", source.stream, "error", err)
		return
	}
	for _, g := range groups {
		if g.Name != source.group {
			continue
		}
		if g.Lag < 0 {
			// Redis could not determine the lag (e.g. it was reset): leave the
			// gauge at its prior value rather than reporting a false zero.
			source.log.Debug("Metrics: group lag undeterminable; leaving gauge unchanged",
				"stream", source.stream, "group", source.group)
			return
		}
		source.metrics.SetGauge(metrics.MetricStreamGroupLag, float64(g.Lag))
		return
	}
}

// pendingAge computes the age of a Redis stream ID (the "<ms>-<seq>" form) as
// the elapsed duration since its millisecond timestamp. It returns ok=false when
// the ID cannot be parsed, in which case the caller skips the age gauge rather
// than logging or failing.
func pendingAge(id string) (time.Duration, bool) {
	msStr, _, ok := strings.Cut(id, "-")
	if !ok {
		return 0, false
	}
	ms, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil {
		return 0, false
	}
	age := time.Since(time.UnixMilli(ms))
	if age < 0 {
		age = 0
	}
	return age, true
}

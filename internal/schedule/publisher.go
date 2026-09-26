package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
)

// occurrenceTTL is how long a schedule-occurrence dedup key survives after
// publication. It is far longer than any realistic scheduling/recovery window
// (reclaim cadence is ~1m; invocation state TTL is 7 days), so a dedup key
// outlives the window during which a redelivery could re-evaluate the same tick.
// It is an internal constant, not env-configurable, matching invocationStateTTL.
const occurrenceTTL = 7 * 24 * time.Hour

// dedupKey returns the Redis key holding one occurrence's publish-once marker:
// the "relay:" namespace followed by the occurrence ID, e.g.
//
//	relay:schedule:courses:jobs.cleanup.handler:2026-09-16T03:00:00Z
//
// The ID already carries the "schedule:" family prefix, so the namespace is
// just "relay:" and the family word is not repeated. Like invocation keys, no
// percent-encoding is needed: the ID is composed of validated identifiers
// (function names are validated at load, handlers are validated, scheduled_at
// is RFC3339) and is deterministic, so the same logical occurrence maps to the
// same key on every worker.
func dedupKey(o Occurrence) string {
	return "relay:" + o.ID()
}

// publishScript is the atomic publish-if-new Lua script. The dedup EXISTS/SET and
// the XADD run in one atomic (single-threaded) step so a concurrent EVAL cannot
// observe a half-state, and no window exists where a dedup key is present without
// its stream entry. Because Redis Lua scripts do NOT roll back earlier writes when
// a later redis.call raises a runtime error (e.g. a wrong-type stream name),
// pcall+DEL explicitly roll back the key so a failed XADD leaves neither the key
// nor a divergent entry — exactly the invariant a "no key-without-entry window"
// guarantees.
//
// KEYS[1] = dedup key, KEYS[2] = relay stream;
// ARGV[1] = occurrence ID (key value), ARGV[2] = TTL ms, ARGV[3] = envelope JSON.
// Returns 1 = published, 0 = duplicate, or an error if the XADD failed.
var publishScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
local ok, err = pcall(redis.call, 'XADD', KEYS[2], '*', 'event', ARGV[3])
if not ok then
  redis.call('DEL', KEYS[1])
  return redis.error_reply(err)
end
return 1
`)

// SchedulePublisher publishes schedule occurrences to the Relay event stream,
// deduplicating each logical occurrence cluster-wide with an atomic
// publish-if-new Lua script (one stream entry per occurrence, ever).
type SchedulePublisher struct {
	client  *redis.Client
	stream  string
	log     *slog.Logger
	metrics *metrics.Registry
	// envelopeFn and runScript are unexported test seams mirrored on
	// ConsumerConfig's backoff hooks: production leaves them at the real
	// implementations (Occurrence.Envelope and publishScript.Run); unit tests
	// substitute fakes to drive the branch/metric matrix without Redis. They are
	// never configurable from outside the package.
	envelopeFn func(Occurrence) ([]byte, error)
	runScript  func(ctx context.Context, c redis.Scripter, keys []string, args ...any) (int, error)
}

// NewPublisher constructs a SchedulePublisher over the given client and stream.
// A nil metrics registry is nil-safe (every metric call is a no-op). It does not
// own the client's lifecycle: the caller (the worker) closes it.
func NewPublisher(
	client *redis.Client,
	stream string,
	logger *slog.Logger,
	metrics *metrics.Registry,
) *SchedulePublisher {
	return &SchedulePublisher{
		client:     client,
		stream:     stream,
		log:        logger,
		metrics:    metrics,
		envelopeFn: Occurrence.Envelope,
		runScript: func(ctx context.Context, c redis.Scripter, keys []string, args ...any) (int, error) {
			return publishScript.Run(ctx, c, keys, args...).Int()
		},
	}
}

// PublishOccurrence atomically publishes one schedule occurrence to the Relay
// event stream if it has not been published before. The dedup check and the
// XADD run in a single Lua script, so there is no window where a dedup key
// exists without its stream entry (and vice versa: a failed script leaves
// neither). A duplicate (the key already exists) is a clean no-op returning
// (false, nil) — another worker published this occurrence first. The dedup key
// is history and expires by TTL only; it is never deleted on completion.
func (p *SchedulePublisher) PublishOccurrence(ctx context.Context, o Occurrence) (published bool, err error) {
	id := o.ID()
	envelope, err := p.envelopeFn(o)
	if err != nil {
		p.metrics.Inc(metrics.MetricSchedulePublishFailures)
		p.log.Warn("Schedule: publish failed",
			"function", o.Function,
			"handler", o.Handler,
			"occurrence_id", id,
			"reason", err,
		)
		return false, fmt.Errorf("schedule publish: marshal envelope: %w", err)
	}
	key := dedupKey(o)
	res, err := p.runScript(ctx, p.client, []string{key, p.stream}, id, occurrenceTTL.Milliseconds(), string(envelope))
	if err != nil {
		p.metrics.Inc(metrics.MetricSchedulePublishFailures)
		p.log.Warn("Schedule: publish failed",
			"function", o.Function,
			"handler", o.Handler,
			"occurrence_id", id,
			"reason", err,
		)
		return false, fmt.Errorf("schedule publish: %w", err)
	}
	switch res {
	case 1:
		p.metrics.Inc(metrics.MetricScheduleOccurrencesPublished)
		p.log.Debug("Schedule: occurrence published",
			"function", o.Function,
			"handler", o.Handler,
			"scheduled_at", o.ScheduledAt.UTC().Format(time.RFC3339),
			"occurrence_id", id,
		)
		return true, nil
	default:
		p.metrics.Inc(metrics.MetricScheduleOccurrencesDuplicate)
		p.log.Debug("Schedule: occurrence already published",
			"function", o.Function,
			"handler", o.Handler,
			"occurrence_id", id,
		)
		return false, nil
	}
}

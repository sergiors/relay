package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
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
// The stream entry always carries the untouched "event" envelope beside the
// optional flat trace-context fields (traceparent/tracestate/baggage). A field
// is written only when its argument is non-empty, so a publish with tracing
// disabled (or no recording span) writes exactly the same entry as before.
//
// KEYS[1] = dedup key, KEYS[2] = relay stream;
// ARGV[1] = occurrence ID (key value), ARGV[2] = TTL ms, ARGV[3] = envelope JSON,
// ARGV[4..6] = traceparent, tracestate, baggage (empty string = absent).
// Returns 1 = published, 0 = duplicate, or an error if the XADD failed.
var publishScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
local fields = {'event', ARGV[3]}
local traceKeys = {'traceparent', 'tracestate', 'baggage'}
for i = 1, 3 do
  if ARGV[3 + i] ~= nil and ARGV[3 + i] ~= '' then
    fields[#fields + 1] = traceKeys[i]
    fields[#fields + 1] = ARGV[3 + i]
  end
end
local ok, err = pcall(redis.call, 'XADD', KEYS[2], '*', unpack(fields))
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
//
// A `schedule.publish.attempt` span wraps ONE attempt, and the current W3C trace
// context is injected as flat message metadata (traceparent/tracestate/baggage)
// beside the untouched event payload, so the consumer's stream.message span
// continues the same trace. Callers that retry a single logical occurrence (the
// cron scheduler's bounded publication recovery) start a `schedule.publish`
// logical span around the whole retry loop; these attempt spans are its
// children, so every attempt of one occurrence shares one trace. With tracing
// disabled the carrier is nil and the entry is identical to before.
func (p *SchedulePublisher) PublishOccurrence(ctx context.Context, o Occurrence) (published bool, err error) {
	ctx, span := tracing.Start(ctx, "schedule.publish.attempt",
		trace.WithAttributes(
			attribute.String("relay.function", o.Function),
			attribute.String("relay.handler", o.Handler),
		),
	)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

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
	carrier := tracing.CarrierFromContext(ctx)
	key := dedupKey(o)
	res, err := p.runScript(ctx, p.client, []string{key, p.stream},
		id, occurrenceTTL.Milliseconds(), string(envelope),
		carrier[tracing.TraceparentKey], carrier[tracing.TracestateKey], carrier[tracing.BaggageKey],
	)
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

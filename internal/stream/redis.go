package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/schedule"
)

// Handler is the contract between the stream layer and the runner. It receives
// the message ID and the decoded event, and returns nil only when the event
// should be acknowledged.
type Handler func(ctx context.Context, msgID string, event map[string]any) error

// Defaults for the retry/recovery settings. They are fixed application
// constants, not env-configurable; zero-valued ConsumerConfig fields fall back
// to these in NewConsumer.
const (
	DefaultBlock = 5 * time.Second
	DefaultCount = int64(10)
	// DefaultReclaimInterval is how often the recovery loop scans for idle
	// pending messages.
	DefaultReclaimInterval = time.Minute
	// DefaultMetricsInterval is how often the pending-gauge GaugeSource samples
	// XPENDING depth. It is overridable via ConsumerConfig.MetricsInterval (used
	// by tests).
	DefaultMetricsInterval = 15 * time.Second
	// DefaultMaxBufferedEvents is the default number of messages read from Redis
	// and held locally before completion/ACK. It bounds the consumer's local
	// buffer so the backlog stays in the Redis stream when the buffer is full
	// (backpressure).
	DefaultMaxBufferedEvents = 16
	// DefaultMaxEventBytes is the byte cap applied to a message's raw `event`
	// value when ConsumerConfig.MaxEventBytes is not set (a direct test
	// construction). It mirrors config.DefaultMaxEventBytes; a leaf package
	// cannot import config, so the default lives here too. A value above
	// config.MaxEventBytesLimit is never accepted from the environment.
	DefaultMaxEventBytes = 256 << 10
)

// MaxRuleTimeout is the upper bound on any rule's handler timeout. It is the
// same value as app.MaxTimeout (kept in sync; app is a leaf package
// and stream may import it, not the reverse). It feeds the runner's runtime cap
// (see runner.SetMaxHandlerTimeout, wired in internal/worker), which becomes
// the maximum persisted running deadline an invocation can carry.
const MaxRuleTimeout = 5 * time.Minute

// ConsumerConfig configures the Consumer. Field-zero defaults are applied in
// NewConsumer.
type ConsumerConfig struct {
	Client   *redis.Client
	Stream   string
	Group    string
	Consumer string
	// Block is the XREADGROUP BLOCK duration. Defaults to 5s if zero.
	Block time.Duration
	// Count is the XREADGROUP/XPENDING/CLAIM batch size. Defaults to 10 if zero.
	Count int64
	// ReclaimInterval is how often the recovery loop scans for idle pending
	// messages. Defaults to 1m if zero.
	ReclaimInterval time.Duration
	// MinPendingIdle is the minimum time a message must have sat pending before
	// it is eligible for reclamation. Defaults to DefaultReclaimInterval (1m) if
	// zero. It is a message-level recovery-pacing backstop: it only delays
	// MESSAGE re-delivery, never defines retry timing. Per-invocation execution
	// eligibility (including retry backoff) is decided separately at run time
	// from the invocation's persisted running/next-attempt deadline (see
	// InvocationState.TryStart), so MinPendingIdle no longer needs to be the
	// oversized 3*MaxRuleTimeout guard against in-flight reclamation.
	MinPendingIdle time.Duration
	Log            *slog.Logger
	// Metrics is an optional metrics registry. A nil registry disables all
	// observability: every metric call is a no-op.
	Metrics *metrics.Registry
	// MetricsInterval is how often the pending-gauge GaugeSource runs.
	// Defaults to DefaultMetricsInterval if zero.
	MetricsInterval time.Duration
	// MaxBufferedEvents bounds the number of messages read from Redis (via
	// XREADGROUP or reclaimed) that have been handed to processing but not yet
	// finished (ACKed / DLQ'd / left-pending). When the buffer is at capacity,
	// Consume stops reading (backpressure) so the backlog stays in Redis.
	// Defaults to DefaultMaxBufferedEvents (16) if zero or negative.
	MaxBufferedEvents int
	// MaxEventBytes is the maximum byte length of a message's raw `event`
	// field value. A delivered message whose raw event value exceeds it is
	// rejected BEFORE JSON decode, schedule classification, event matching,
	// invocation-state migration, and handler execution, then routed to the DLQ
	// with a bounded diagnostic summary (never the oversized payload). It
	// bounds the raw event value only; the Redis entry and go-redis response
	// have already been materialized before the consumer sees them. Defaults to
	// DefaultMaxEventBytes (262144) if zero or negative — zero must not mean
	// "unlimited".
	MaxEventBytes int
	// InvocationRetention is the TERMINAL invocation-state retention window
	// (config REDIS_INVOCATION_RETENTION) the consumer applies after a message
	// leaves the PEL. It is a POINTER so an explicit disable (0) is
	// distinguishable from "unconfigured": nil means the caller did not
	// configure it and the package default (DefaultInvocationRetention, 48h) is
	// used, while a non-nil value is honored verbatim — including 0, which
	// disables terminal expiry (the marker is still written; the hash is left
	// PERSISTENT). This is what lets the worker deliberately pass an operator's
	// empty/0/negative value without NewConsumer defaulting it back to 48h. The
	// worker always sets it from config; direct callers may leave it nil.
	InvocationRetention *time.Duration
	// ScheduleRunner, when set, executes messages identified as schedule
	// occurrences directly against the named app/schedule/handler,
	// bypassing event matching. msgID is the message's real Redis stream ID, so
	// the runner can stamp it on the execution container's relay.message_id
	// label (the same identity the invocation-state machinery uses). The whole
	// validated Occurrence (including the scheduled instant and derived identity)
	// is passed so the runner can semantically validate it against the current
	// template before admission — the envelope handler is never the target; the
	// runner resolves the schedule by its stable name, so a handler change under
	// the same schedule is picked up on delivery. The stream layer stays the same
	// consumer-group/PEL/recovery machinery for both message kinds. It is wired
	// by the worker to runner.InvokeOccurrence. A nil value means a WELL-FORMED
	// schedule message is treated as a normal event (the safe fallback for tests
	// that do not wire it); a structurally INVALID schedule claim is dead-lettered
	// regardless, never executed as an ordinary event.
	ScheduleRunner func(ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte) error
	// backoffTable and backoffJitter override the retry backoff for tests. They
	// are unexported so production always uses the fixed defaults.
	backoffTable  []time.Duration
	backoffJitter func(float64) float64
}

// Consumer reads events from a Redis stream and hands each decoded event to a
// handler. All Redis concerns live in this package.
type Consumer struct {
	client          *redis.Client
	stream          string
	group           string
	consumer        string
	block           time.Duration
	count           int64
	reclaimInterval time.Duration
	minPendingIdle  time.Duration
	dlqStream       string
	log             *slog.Logger
	metrics         *metrics.Registry
	metricsInterval time.Duration
	backoff         *backoff
	invStateStore   invocationStateStore
	healthy         atomic.Bool
	// buffer is the bounded local-event semaphore: it caps the number of
	// messages read from Redis and held locally before completion, so the
	// consumption loop applies backpressure instead of unboundedly buffering.
	// capacity is the configured MaxBufferedEvents limit.
	buffer   *bufferSemaphore
	capacity int
	// scheduleRunner is the ScheduleRunner seam (see ConsumerConfig). When nil,
	// well-formed schedule-occurrence messages are treated as normal events.
	scheduleRunner func(ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte) error
	// maxEventBytes caps the raw `event` value length (see
	// ConsumerConfig.MaxEventBytes). It is always positive.
	maxEventBytes int
	// invocationRetention is the resolved terminal invocation-state window
	// applied after a message leaves the PEL (see
	// ConsumerConfig.InvocationRetention). A non-positive value disables
	// terminal expiry; the consumer still writes the terminal marker.
	invocationRetention time.Duration
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	return newConsumer(cfg, &invocationStore{client: cfg.Client})
}

func newConsumer(cfg ConsumerConfig, store invocationStateStore) *Consumer {
	if cfg.Block == 0 {
		cfg.Block = DefaultBlock
	}

	if cfg.Count == 0 {
		cfg.Count = DefaultCount
	}

	if cfg.ReclaimInterval == 0 {
		cfg.ReclaimInterval = DefaultReclaimInterval
	}

	// MinPendingIdle defaults to DefaultReclaimInterval (1m): it is a
	// message-level recovery-pacing backstop that only delays MESSAGE
	// re-delivery. Per-invocation execution eligibility (including retry
	// backoff) is decided separately at run time from the invocation's
	// persisted deadline, so MinPendingIdle no longer needs to be the oversized
	// 3*MaxRuleTimeout guard against in-flight reclamation.
	if cfg.MinPendingIdle == 0 {
		cfg.MinPendingIdle = DefaultReclaimInterval
	}

	if cfg.MetricsInterval == 0 {
		cfg.MetricsInterval = DefaultMetricsInterval
	}

	// The bounded local-event buffer defaults to DefaultMaxBufferedEvents (16)
	// and falls back to it on a zero or negative value (a value of 0 must not
	// mean "unbounded").
	capacity := DefaultMaxBufferedEvents
	if cfg.MaxBufferedEvents >= 1 {
		capacity = cfg.MaxBufferedEvents
	}

	// The raw-event byte cap defaults to DefaultMaxEventBytes (256 KiB) and
	// falls back to it on a zero or negative value (a value of 0 must not mean
	// "unlimited").
	maxEventBytes := DefaultMaxEventBytes
	if cfg.MaxEventBytes >= 1 {
		maxEventBytes = cfg.MaxEventBytes
	}

	// Terminal invocation-state retention: a nil pointer means the caller did
	// not configure it (direct/internal construction) and the package default
	// applies; a non-nil pointer is honored VERBATIM, so an explicit 0 from the
	// worker's REDIS_INVOCATION_RETENTION is preserved as "disabled" rather than
	// being defaulted back to 48h.
	invocationRetention := DefaultInvocationRetention
	if cfg.InvocationRetention != nil {
		invocationRetention = *cfg.InvocationRetention
	}
	c := &Consumer{
		client:              cfg.Client,
		stream:              cfg.Stream,
		group:               cfg.Group,
		consumer:            cfg.Consumer,
		block:               cfg.Block,
		count:               cfg.Count,
		reclaimInterval:     cfg.ReclaimInterval,
		minPendingIdle:      cfg.MinPendingIdle,
		dlqStream:           DLQStreamFor(cfg.Stream),
		log:                 cfg.Log,
		metrics:             cfg.Metrics,
		metricsInterval:     cfg.MetricsInterval,
		backoff:             newBackoff(cfg.backoffTable, cfg.backoffJitter),
		capacity:            capacity,
		buffer:              newBufferSemaphore(capacity),
		scheduleRunner:      cfg.ScheduleRunner,
		maxEventBytes:       maxEventBytes,
		invocationRetention: invocationRetention,
	}
	// The consumer is always constructed with a functional invocation-state
	// store. processMessage/processScheduleMessage/routeToDLQ rely on it being
	// present (no nil guard), so the seam must be injected here.
	c.invStateStore = store
	c.healthy.Store(true)
	return c
}

// DLQStreamFor returns the Relay-owned DLQ stream derived from source.
func DLQStreamFor(stream string) string {
	return "relay:" + stream + ":dlq"
}

// groupCreator is the narrow Redis view the consumer-group bootstrap needs:
// XGROUP CREATE with MKSTREAM. *redis.Client satisfies it; a test fake can
// implement it too, so the BUSYGROUP/MKSTREAM-at-0 semantics are unit-testable
// without a Redis server.
type groupCreator interface {
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
}

// EnsureGroup creates the consumer group if it does not exist, tolerating a
// group that already exists (BUSYGROUP). MKSTREAM creates the stream if needed;
// the group starts at "0", so its consumers see every entry already in the
// stream as well as new ones (not just new messages); a fresh group over an
// existing stream therefore replays the backlog.
//
// It is the single stream-level bootstrap both Consumer.EnsureGroup and the
// worker's external-dependency preflight delegate to, so these semantics have
// exactly one implementation. The worker invokes it as a startup prerequisite
// before any app is loaded or fingerprinted and before the state DB and
// runtime workload initialization (the Redis client and tracing already exist;
// the Docker manager is opened afterwards, see internal/worker); an embedder
// invokes it before Consume.
func EnsureGroup(ctx context.Context, client groupCreator, stream, group string) error {
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err == nil {
		return nil
	}
	// BUSYGROUP means the group already exists; that is not a failure.
	if isBusyGroup(err) {
		return nil
	}
	return fmt.Errorf("create consumer group %q on stream %q: %w", group, stream, err)
}

// EnsureGroup creates the consumer group required for consumption. It delegates
// to the package-level EnsureGroup, binding this consumer's client, stream, and
// group, so the BUSYGROUP/MKSTREAM-at-0 semantics are not duplicated.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	return EnsureGroup(ctx, c.client, c.stream, c.group)
}

// The production Redis client must satisfy the group-bootstrap seam.
var _ groupCreator = (*redis.Client)(nil)

// Healthy reports whether the consumer's last observed Redis operation
// succeeded: true while Redis is reachable, false during an outage. It is an
// in-process readiness accessor for embedders and orchestrators that run the
// consumer themselves; the worker's own `relay health` probe consults it in
// steady state. `relay health` itself never touches Redis: it asks the running
// worker over its control socket.
func (c *Consumer) Healthy() bool {
	return c.healthy.Load()
}

// noteOutcome feeds a single Redis operation result into the health state and
// logs only on state transitions. On failure it marks the consumer unhealthy
// (logging once on the healthy→unhealthy transition); on success it marks it
// healthy (logging once on the unhealthy→healthy transition) and resets the
// backoff. redis.Nil counts as success because connectivity is fine. delay is
// the retry delay the caller is about to wait, used only in the failure log.
func (c *Consumer) noteOutcome(err error, delay time.Duration) {
	if err != nil && !errors.Is(err, redis.Nil) {
		if c.healthy.Swap(false) {
			c.log.Warn("Redis: read failed; retrying", "error", err, "delay", delay)
		}
		return
	}
	// Success (or redis.Nil): mark healthy and reset backoff on the transition.
	if !c.healthy.Swap(true) {
		c.log.Info("Redis connection recovered")
	}
	c.backoff.reset()
}

// recordRedisReadError increments the Redis read-error counter for a genuine
// read-command failure, labeled by the finite operation set. It deliberately
// excludes redis.Nil (a blocking XREADGROUP timeout, not a failure) and context
// cancellation/deadline (an intentional shutdown, not a Redis fault), mirroring
// the health path's classification. It never attaches error text: the counter is
// a count of failures by operation, and the caller keeps logging the error. A
// nil metrics registry (or nil receiver) is a no-op. One failed command yields
// exactly one increment.
func recordRedisReadError(reg *metrics.Registry, operation string, err error) {
	if err == nil || errors.Is(err, redis.Nil) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	reg.IncLabels(metrics.MetricRedisReadErrors, []metrics.Label{{Name: "operation", Value: operation}})
}

// bufferSemaphore is a channel-based counting semaphore that bounds the number
// of locally buffered events (messages read from Redis but not yet finished).
// It also tracks the current occupancy (a mutex-protected counter) so the
// consumer can set the buffered_events gauge on acquire/release and compute the
// current in-flight count without draining the channel.
type bufferSemaphore struct {
	slots chan struct{}
	mu    sync.Mutex
	count int // current in-flight occupancy
}

// newBufferSemaphore builds a semaphore of the given capacity. capacity must be
// >= 1 (callers pass a validated MaxBufferedEvents).
func newBufferSemaphore(capacity int) *bufferSemaphore {
	return &bufferSemaphore{slots: make(chan struct{}, capacity)}
}

// inflight returns the current occupancy (number of slots held).
func (s *bufferSemaphore) inflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// acquire acquires one slot, blocking until either a slot is free or ctx is
// cancelled. It returns false when ctx is done first (no slot acquired).
func (s *bufferSemaphore) acquire(ctx context.Context) bool {
	select {
	case s.slots <- struct{}{}:
		s.mu.Lock()
		s.count++
		s.mu.Unlock()
		return true
	case <-ctx.Done():
		return false
	}
}

// tryAcquire acquires a slot without blocking. It returns false when no slot is
// immediately free. Used by the reclaim path, which must never block (it skips
// a message for capacity and retries it next tick instead).
func (s *bufferSemaphore) tryAcquire() bool {
	select {
	case s.slots <- struct{}{}:
		s.mu.Lock()
		s.count++
		s.mu.Unlock()
		return true
	default:
		return false
	}
}

// release frees a slot, waking an acquire that is waiting. It must be called
// exactly once for every successful acquire/tryAcquire.
func (s *bufferSemaphore) release() {
	<-s.slots
	s.mu.Lock()
	s.count--
	s.mu.Unlock()
}

// setGauge publishes the current occupancy to the buffered_events gauge (nil-safe
// in the registry).
func (c *Consumer) setBufferGauge() {
	c.metrics.SetGauge(metrics.MetricBufferedEvents, float64(c.buffer.inflight()))
}

// freeSlots returns the number of slots currently available (capacity - inFlight).
func (c *Consumer) freeSlots() int {
	free := c.capacity - c.buffer.inflight()
	if free < 0 {
		free = 0
	}
	return free
}

// Consume reads messages from the stream and calls handler for each decoded
// event. A message is acknowledged (XACK) only after the handler returns nil or
// it is routed to the DLQ. Consume blocks until ctx is cancelled, and stops the
// recovery goroutine before returning so nothing leaks.
//
// Backpressure: the number of messages read from Redis but not yet finished is
// bounded by the buffer capacity. Before each XREADGROUP the loop computes how
// many slots are free and reads at most that many; when the buffer is full it
// waits (context-aware) for a slot to release instead of reading more, so the
// backlog stays in Redis rather than in local memory. Each read message is
// handed to the handler in its own goroutine (bounded by the buffer capacity),
// so handler latency never blocks further reads, only the release of slots.
// In-flight messages are drained via a WaitGroup before Consume returns, so
// shutdown joins them exactly as it joins reclaim and metrics goroutines.
func (c *Consumer) Consume(
	ctx context.Context,
	handler Handler,
) error {
	// The recovery loop runs in its own goroutine and uses the same handler. It
	// is stopped and joined before Consume returns on shutdown.
	if c.reclaimInterval > 0 {
		rctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.reclaimLoop(rctx, handler)
		}()
		defer func() {
			stop()
			<-done
		}()
	}

	// The metrics refresher drives the pending-gauge GaugeSource in its own
	// goroutine and never affects health, backoff, or processing. It is stopped
	// and joined before Consume returns.
	if c.metrics != nil {
		sctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			metrics.NewRefresher(c.metricsInterval, c.pendingGaugeSource()).Start(sctx)
		}()
		defer func() {
			stop()
			<-done
		}()
	}

	// Track in-flight message goroutines so Consume can join them on shutdown.
	// The count is bounded by the buffer capacity (each goroutine holds a slot),
	// so the WaitGroup can never grow unbounded.
	var inflight sync.WaitGroup

	// bufferFull is a transition flag so the "read loop paused" log fires only
	// once on the healthy→blocked transition (and once on the recovery), not on
	// every iteration, mirroring noteOutcome's transition-only logging.
	bufferFull := false

Drain:
	for {
		// Apply backpressure BEFORE issuing a read: compute how many slots are
		// free and wait (context-aware, not a busy spin) until capacity exists if
		// the buffer is full. Since XREADGROUP BLOCK is not interrupted by ctx
		// cancellation in go-redis, the loop re-checks capacity at least every
		// block interval; the wait below is a blocking channel receive, so it
		// does not spin.
		free := c.freeSlots()
		if free == 0 {
			if !bufferFull {
				c.log.Debug("Read loop paused: local event buffer full")
				bufferFull = true
			}
			// Wait for a slot release or shutdown. This is context-aware: on
			// cancellation it returns promptly rather than spinning until a
			// handler frees a slot.
			if !c.buffer.acquire(ctx) {
				// ctx done; nothing was handed out. Drain in-flight goroutines
				// before returning (see below).
				break
			}
			// We acquired a slot, so the buffer has room now; release it back so
			// the read below is not double-counted — we only re-check free slots
			// below and issue a read sized to them. (Keeping a slot held here
			// and immediately releasing avoids needing a read to know how many
			// it will produce.)
			c.buffer.release()
		} else if bufferFull {
			// Buffer has room again: leave the paused state.
			bufferFull = false
		}

		// Read at most as many messages as there are free slots, so every
		// message we get can be handed out to a slot without blocking the loop
		// on a full buffer mid-batch.
		batch := c.count
		if n := c.freeSlots(); n < int(batch) {
			batch = int64(n)
		}
		if batch < 1 {
			// A race released a slot between the check above and here; loop and
			// recompute rather than issuing a zero-count read. This cannot
			// busy-spin: the acquire/release path above paces it.
			continue
		}

		streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    batch,
			Block:    c.block,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			recordRedisReadError(c.metrics, metrics.RedisOpReadGroup, err)
			if errors.Is(err, redis.Nil) {
				// Block timed out with no messages; connectivity is fine.
				c.noteOutcome(nil, 0)
				continue
			}
			// A transient Redis failure (restart, flaky network) should not kill the
			// process; go-redis re-establishes connections. Back off with jitter so
			// replicas do not retry in lockstep, and wait context-aware so shutdown
			// interrupts a pending retry promptly.
			delay := c.backoff.next()
			c.noteOutcome(err, delay)
			select {
			case <-ctx.Done():
				break
			case <-time.After(delay):
			}
			continue
		}

		c.noteOutcome(nil, 0)
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				// Acquire a slot synchronously. The batch was sized to the free
				// slots just now (see above), so acquisition must not block in
				// practice; on ctx cancellation it returns false and the message
				// is skipped (it stays in the PEL, redelivered on a later read).
				// Release the slot when the message finishes.
				if !c.buffer.acquire(ctx) {
					// Shutting down: stop handing out fresh work. The message is
					// left pending in the PEL for a live consumer (this matches
					// processMessage's ctx-cancelled behavior for in-flight
					// messages). Break out of the whole loop and drain.
					break Drain
				}
				inflight.Add(1)
				go func(m redis.XMessage) {
					defer inflight.Done()
					// A defensive recover in the goroutine wrapper is the last
					// line of defense so a panic escaping processMessage (which
					// recovers internally) can never crash the worker. It is
					// effectively unreachable in normal operation.
					defer func() {
						if pv := recover(); pv != nil {
							c.log.Error("Message: panic outside processMessage", "message_id", m.ID, "error", pv, "stack", string(debug.Stack()))
						}
					}()
					defer c.buffer.release()
					c.setBufferGauge()
					// A message read fresh from XREADGROUP is on its first delivery.
					c.processMessage(ctx, m, 1, handler)
					c.setBufferGauge()
				}(msg)
			}
		}
	}
	// Drain all in-flight message goroutines before returning. On ctx
	// cancellation the handler path already leaves messages pending
	// (processMessage checks ctx.Err()); joining here bounds shutdown to at most
	// the longest in-flight handler. The deferred stops for the reclaim and
	// metrics goroutines run when Consume returns (after this join), preserving
	// the existing stop-and-join-before-return shutdown contract.
	inflight.Wait()
	return nil
}

// pendingGaugeSource builds a metrics.GaugeSource that samples the XPENDING
// pending-depth gauges. It needs the consumer's client, stream, and group, and
// reuses the consumer's metrics registry and logger, so it is constructed from
// the consumer rather than at the package level.
func (c *Consumer) pendingGaugeSource() metrics.GaugeSource {
	return &PendingGaugeSource{
		client:  c.client,
		stream:  c.stream,
		group:   c.group,
		metrics: c.metrics,
		log:     c.log,
	}
}

// PendingGaugeSource is a metrics.GaugeSource that samples the Redis XPENDING
// summary (Count and oldest pending ID) and records the pending_depth gauges on
// each Refresh. It is decoupled from health/backoff/processing: a Redis failure
// just skips the pending gauges for that tick and is logged quietly. The overall
// registry snapshot is exposed by the worker's MetricsLogger; this source only
// produces the pending gauges.
type PendingGaugeSource struct {
	client  *redis.Client
	stream  string
	group   string
	metrics *metrics.Registry
	log     *slog.Logger
}

// Refresh reads the XPENDING summary and records the pending-depth gauges. It
// satisfies metrics.GaugeSource. It never blocks processing: it runs in its own
// goroutine (via the Refresher), holds no locks across the Redis call, and any
// error is logged and skipped, never propagated.
func (source *PendingGaugeSource) Refresh(ctx context.Context) {
	// The summary form (no Start/End/Count) is O(1)-ish and returns the total
	// Count plus the oldest pending message ID in Lower.
	pending, err := source.client.XPending(ctx, source.stream, source.group).Result()
	if err != nil {
		recordRedisReadError(source.metrics, metrics.RedisOpPendingGauge, err)
		source.log.Debug("Metrics: xpending failed", "stream", source.stream, "group", source.group, "error", err)
		return
	}
	source.metrics.SetGauge(metrics.MetricPendingEntries, float64(pending.Count))
	if age, ok := pendingAge(pending.Lower); ok {
		source.metrics.SetGauge(metrics.MetricPendingOldestAge, age.Seconds())
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

// reclaimLoop paces the recovery loop with a ticker so idle pending messages are
// re-delivered without busy-spinning.
func (c *Consumer) reclaimLoop(ctx context.Context, handler Handler) {
	ticker := time.NewTicker(c.reclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reclaimTick(ctx, handler)
		}
	}
}

// reclaimTick finds messages pending idle beyond MinPendingIdle, takes ownership
// of them for this consumer, and runs them through the shared processing path.
//
// XPendingExt (the full form) is used for the per-message retry count because
// XAUTOCLAIM (RESP2) does not return delivery counts; the retry counter must
// come from Redis (survives restarts), not from in-process state.
//
// Reclaim is message-ownership recovery only: it transfers idle pending messages
// to this consumer. Whether a transferred message's invocation is actually
// executed is decided later, at run time, from the invocation's persisted
// running/next-attempt deadline (see InvocationState.TryStart): a message whose
// invocation is still protected by an active attempt deadline or a retry backoff
// is skipped, so an in-flight handler on another replica is never run
// concurrently and a backoff is honored. MinPendingIdle remains a message-level
// recovery-pacing backstop; it never defines retry timing.
func (c *Consumer) reclaimTick(ctx context.Context, handler Handler) {
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.stream,
		Group:  c.group,
		Idle:   c.minPendingIdle,
		Start:  "-",
		End:    "+",
		Count:  c.count,
	}).Result()
	if err != nil {
		// The recovery loop is paced by its own ticker, so it only feeds the
		// health state (transition-log + mark unhealthy) and does not run a
		// second backoff mechanism.
		recordRedisReadError(c.metrics, metrics.RedisOpPending, err)
		c.noteOutcome(err, c.backoff.peek())
		return
	}
	c.noteOutcome(nil, 0)
	byID := make(map[string]redis.XPendingExt, len(pending))
	for _, pe := range pending {
		byID[pe.ID] = pe
	}

	// XAUTOCLAIM atomically moves ownership of idle messages to this consumer
	// and returns the next cursor, so we walk the cursor space until done.
	//
	// XAutoClaimWithDeleted runs the SAME XAUTOCLAIM command; go-redis only adds
	// reply parsing for the third (deleted-ids) array, which the plain variant
	// silently discards. Redis 7+ purges a PEL entry whose stream body was
	// already trimmed/XDEL'd and reports it there instead of claiming it, so on
	// the supported Redis that array is the ONLY place the missing-payload
	// anomaly is observable. A pre-7 server returns a 2-element reply; the
	// parsed deleted list is then empty, and the nil-values branch below stays
	// the defensive fallback for any shape that claims a body-less message.
	start := "0-0"
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, next, deletedIDs, err := c.client.XAutoClaimWithDeleted(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumer,
			MinIdle:  c.minPendingIdle,
			Start:    start,
			Count:    c.count,
		}).Result()
		if err != nil {
			recordRedisReadError(c.metrics, metrics.RedisOpAutoclaim, err)
			c.noteOutcome(err, c.backoff.peek())
			return
		}
		// The server already purged these dangling PEL references as part of the
		// scan; there is nothing to ack. Surface each as missing-payload data
		// loss (no handler, no retry, no DLQ) exactly like the nil-values path.
		for _, id := range deletedIDs {
			c.clearMissingValueEntry(ctx, redis.XMessage{ID: id}, missingPayloadUnknownDeliveries, false)
		}
		for _, msg := range msgs {
			// A pending entry whose stream body is already gone (trimmed or
			// XDEL'd) is claimed with nil values by servers that do not purge
			// dangling PEL entries during XAUTOCLAIM. This is NOT successful
			// processing: there is no payload, so no handler can ever run and it
			// must not be counted as an ACK/DLQ outcome. Surface it loudly and
			// clear the dangling PEL reference (the server did NOT purge it, so
			// an XACK is needed to stop the recovery loop spinning on it).
			if msg.Values == nil {
				deliveries := missingPayloadUnknownDeliveries
				if pe, ok := byID[msg.ID]; ok {
					deliveries = pe.RetryCount
				}
				c.clearMissingValueEntry(ctx, msg, deliveries, true)
				continue
			}
			pe, ok := byID[msg.ID]
			if !ok {
				// XAUTOCLAIM walks the whole PEL with a cursor, but byID comes
				// from a Count-truncated XPendingExt. Under backlog (more pending
				// than Count) messages beyond the window miss byID; recover the
				// TRUE pending entry with a targeted single-ID query so DLQ
				// accounting is never fabricated. On failure, skip this tick
				// (leave pending; retried next tick) rather than fabricating
				// attempt 1.
				entries, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
					Stream: c.stream, Group: c.group, Start: msg.ID, End: msg.ID, Count: 1,
				}).Result()
				if err != nil || len(entries) == 0 {
					recordRedisReadError(c.metrics, metrics.RedisOpPending, err)
					c.log.Debug("Message: beyond reclaim window and pending lookup failed; skipping this tick",
						"message_id", msg.ID, "error", err)
					continue
				}
				pe = entries[0]
			}
			c.log.Debug("Reclaimed message",
				"message_id", msg.ID,
				"consumer", c.consumer,
				"idle", pe.Idle,
				"delivery_attempt", pe.RetryCount,
			)
			c.deliverClaimed(ctx, msg, pe.RetryCount, handler)
		}
		if len(msgs) == 0 || next == "0-0" {
			break
		}
		start = next
	}
}

// missingPayloadUnknownDeliveries is the sentinel passed to clearMissingValueEntry
// when the PEL delivery count is not known (a defensive guard that did not read
// XPENDING); the log then omits the count rather than reporting a fabricated
// zero.
const missingPayloadUnknownDeliveries = int64(-1)

// clearMissingValueEntry handles a message whose stream body no longer exists:
// XAUTOCLAIM claimed it with nil values, XAUTOCLAIM reported it in its purged
// deleted-id array, or a defensive caller observed it downstream. This is data
// loss that surfaced at recovery time, NOT successful processing: there is no
// payload to hand to a handler, so the entry is neither retried nor
// dead-lettered, and no handler attempt, retry, or DLQ entry is fabricated. It
// is surfaced explicitly with a high-signal Warn log and the missing_payload_total
// counter, then (only when ack is true) the dangling PEL reference is cleared
// with a single conservative XAck so the recovery loop cannot spin on it forever.
//
// ack is false for ids the server already purged from the PEL during the scan:
// acking them is a no-op at best and misleading at worst, so it is skipped.
//
// deliveries is the Redis PEL delivery count when known
// (missingPayloadUnknownDeliveries otherwise), reported for diagnosis only; it
// is never converted into a handler attempt.
//
// Invocation state: once the dangling PEL reference is gone (XACKed here, or
// already purged by the server during the scan) the message is no longer
// recoverable, so its state hash is switched to terminal retention. Without
// that, a persistent hash written for a body-less invocation before this change
// would leak forever. When the conservative XAck fails the entry may still be in
// the PEL, so the state is deliberately left recoverable (persistent, no TTL):
// a leak is safer than dropping state a redelivery might still need.
func (c *Consumer) clearMissingValueEntry(ctx context.Context, msg redis.XMessage, deliveries int64, ack bool) {
	c.metrics.Inc(metrics.MetricMissingPayload)
	attrs := []any{
		"message_id", msg.ID,
		"consumer", c.consumer,
	}
	if deliveries != missingPayloadUnknownDeliveries {
		attrs = append(attrs, "deliveries", deliveries)
	}
	c.log.Warn("Message: pending entry has no stream body (trimmed or deleted); not processed", attrs...)
	if ack {
		if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
			// The reference may still be pending: keep state recoverable.
			c.log.Warn("Message: ack missing-value entry failed", "message_id", msg.ID, "error", err)
			return
		}
	}
	// The dangling PEL reference is gone: retain the (never-completing)
	// invocation state under the retention TTL instead of leaking it.
	if err := c.invStateStore.retainTerminal(ctx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
		c.log.Warn("Message: retain invocation state for missing-value entry failed",
			"message_id", msg.ID, "error", err)
	}
}

// deliverClaimed runs a reclaimed message through the shared path. Reclaim is
// message-ownership recovery only: it transfers an idle pending message to this
// consumer and replays it. Whether the message's invocations actually execute
// (and whether they are exhausted) is decided at run time from per-invocation
// state, so there is no message-level attempt pre-check here. The stream-level
// retries_total counter counts message reclaims (actual re-delivery events, not
// executions): a reclaim is counted even when the invocation is skipped as
// protected, because a redelivery DID occur. This is distinct from the
// per-app function_retries_total (runner), which counts failed handler
// executions only.
//
// Backpressure: the reclaimed message also counts against the bounded local
// buffer. Slot acquisition is non-blocking (tryAcquire): if the buffer is full
// the message is skipped THIS tick and left pending in the PEL for the next
// reclaim tick. With the default MinPendingIdle the skip simply defers the
// retry by one tick — no starvation beyond pacing — and the reclaim goroutine
// never blocks on the semaphore.
func (c *Consumer) deliverClaimed(
	ctx context.Context,
	msg redis.XMessage,
	retryCount int64,
	handler Handler,
) {
	c.metrics.Inc(metrics.MetricRetries)
	if !c.buffer.tryAcquire() {
		// Buffer full: leave the message pending; a later reclaim tick retries
		// it. Never block the reclaim goroutine on the semaphore.
		c.log.Debug("Message: buffer full; deferring reclaimed delivery", "message_id", msg.ID)
		return
	}
	// Release the slot when the message finishes (ACK/DLQ/pending).
	// processMessage runs synchronously here on the reclaim goroutine (bounded
	// by the reclaim cadence), exactly as before; the slot is held for its
	// duration so the buffer occupancy reflects a reclaimed message too.
	c.setBufferGauge()
	c.processMessage(ctx, msg, retryCount+1, handler)
	c.setBufferGauge()
	c.buffer.release()
}

// processMessage is the single shared path used by both the XREADGROUP loop and
// the recovery loop. It decodes, classifies, and either ACKs on success, routes
// to the DLQ on a non-retryable failure or when every non-complete invocation is
// exhausted, or leaves the message pending for a later retry.
//
// Panic boundary: a recover is registered at the top of the app so a panic
// anywhere in the handler handoff (including the runner's matching/pre-pass code
// that sits OUTSIDE its per-invocation recover) is converted into the standard
// failure path: the message is left pending (no ACK) and a later reclaim retries
// it (at-least-once). This is defense in depth behind the runner's per-invocation
// boundary, which already converts executor panics into normal failed attempts
// so retry/exhaustion state machinery runs. A panic here is a programming bug and
// stays visible (logged every cycle) rather than killing the worker. Panics in
// Consume/reclaimLoop are outside message processing (startup or
// programming) and are deliberately NOT recovered: they remain fatal.
func (c *Consumer) processMessage(
	ctx context.Context,
	msg redis.XMessage,
	deliveryNum int64,
	handler Handler,
) {
	// A processing span for this delivery attempt. It is a child of the caller's
	// context (the Consume/reclaim loop), and its outcome is recorded on every
	// terminal path below by the single deferred finalizer. It never carries the
	// event payload. outcome defaults to "pending" (left in the PEL for retry)
	// and is set to a more specific terminal value where one applies.
	//
	// W3C trace context carried as flat message metadata (traceparent/
	// tracestate/baggage, written beside the untouched event payload by the
	// schedule publisher or any trace-aware producer) is extracted FIRST, so the
	// message span becomes a child of the publishing span (remote parent) and
	// the whole stream → dispatch → function.invoke chain shares one trace. An
	// ordinary external event (no metadata fields) is unaffected: extraction is
	// a no-op and the span is a root.
	if carrier := traceCarrierFromMessage(msg.Values); len(carrier) != 0 {
		ctx = tracing.ExtractStrings(ctx, carrier)
	}
	spanCtx, span := c.messageSpan(ctx, "stream.message", deliveryNum)
	outcome := "pending"
	var spanErr error
	defer func() { finishMessageSpan(span, outcome, spanErr) }()
	ctx = spanCtx

	// Register the panic boundary before any handler work so a panic in the
	// handler handoff (or in classifyMessage, which is pure JSON parsing and
	// practically cannot panic) is caught. The delivery is treated as failed:
	// leave pending, no ACK, no counters; a later reclaim retries it. A
	// panicking non-handler code path is a bug to fix, and it stays visible
	// (logged every cycle). One edge to know: a panic AFTER a successful DLQ
	// write but before the ACK leaves the message pending, so the DLQ write
	// repeats on the retry cycle — a duplicate DLQ entry. That is the normal
	// at-least-once window (the DLQ entry carries the original message ID), not
	// a new failure class.
	defer func() {
		if pv := recover(); pv != nil {
			outcome = "panic"
			spanErr = fmt.Errorf("panic: %v", pv)
			c.log.Error("Message: panic in handler", "message_id", msg.ID, "error", pv, "stack", string(debug.Stack()))
		}
	}()
	// Defensive guard: a message with no body can never be classified. It is
	// data loss (the entry was trimmed or XDEL'd), not a malformed payload, so
	// it must NOT fall through to classifyMessage and be dead-lettered as a
	// malformed message. The reclaim path clears these before this point; this
	// guard keeps any other caller (or a future one) from silently treating it
	// as successful processing.
	if msg.Values == nil {
		outcome = "missing"
		spanErr = fmt.Errorf("message body no longer exists")
		// The message reached the processing path, so it is still in the PEL:
		// clear it (the reclaim path normally handles these before here).
		c.clearMissingValueEntry(ctx, msg, missingPayloadUnknownDeliveries, true)
		return
	}
	event, err := extractAndDecodeEvent(msg, c.maxEventBytes)
	if err != nil {
		var oversize *EventOversizedError
		if errors.As(err, &oversize) {
			// An oversized message is rejected BEFORE decode/classification/
			// matching and routed non-retryably to the DLQ. The DLQ entry carries
			// a bounded diagnostic summary instead of the oversized payload (see
			// oversizedEventSummary); it has no handler invocation to attribute,
			// so it uses the placeholder app/handler and is intentionally
			// non-replayable. The counter is incremented exactly once here, in the
			// sole over-limit branch.
			c.metrics.Inc(metrics.MetricEventsOversized)
			c.log.Error("Message: event exceeds MAX_EVENT_BYTES; routing to DLQ",
				"message_id", msg.ID,
				"delivery_attempt", deliveryNum,
				"event_bytes", oversize.Bytes,
				"max_event_bytes", oversize.MaxBytes,
				"error", err,
			)
			outcome, spanErr = "dlq", err
			c.routeToDLQ(ctx, msg, err, deliveryNum)
			return
		}
		// A malformed message can never succeed, so it goes straight to the DLQ on
		// first encounter rather than consuming retry cycles.
		c.log.Error("Message: non-retryable failure; routing to DLQ",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
			"error", err,
		)
		outcome, spanErr = "dlq", err
		c.routeToDLQ(ctx, msg, err, deliveryNum)
		return
	}

	// Classify the decoded event against the schedule envelope. Three outcomes:
	//
	//   - NotScheduleClaim: an ordinary external event, handled below exactly as
	//     before.
	//   - ValidScheduleClaim: a well-formed schedule occurrence; when a
	//     ScheduleRunner is wired it is routed to it, bypassing event matching
	//     entirely. A nil ScheduleRunner falls back to treating it as a normal
	//     event (the safe fallback for tests/unwired consumers).
	//   - InvalidScheduleClaim: the event deliberately claims schedule identity
	//     (source==relay.schedule) but is structurally incomplete, malformed, or
	//     carries a mismatched occurrence_id. It must NEVER fall through to event
	//     matching and be executed/ACKed as unmatched: it can never succeed, so
	//     it is dead-lettered as a non-retryable failure, exactly like a
	//     malformed event body. This holds whether or not a ScheduleRunner is
	//     wired: a malformed claim is malformed either way.
	//
	// Classification happens BEFORE the invocation-state migration below, like
	// the malformed-message path: an invalid claim is non-retryable and needs no
	// recoverable state, so it is dead-lettered directly.
	occ, claimKind, claimErr := schedule.ClassifyClaim(event)
	if claimKind == schedule.InvalidScheduleClaim {
		c.log.Error("Message: invalid schedule claim; routing to DLQ",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
			"error", claimErr,
		)
		outcome, spanErr = "dlq", claimErr
		c.routeToDLQ(ctx, msg, claimErr, deliveryNum)
		return
	}

	// The message decoded successfully and is about to be handed to the handler.
	// Event classification (received/matched/unmatched) is NOT counted here: it
	// is a property of the logical event, owned by the runner, which claims it
	// exactly once per message via the invocation-state hash (see
	// runner.Handle and stream.InvocationState.ClaimClassification). Counting a
	// delivery attempt here would break the once-per-logical-event invariant on
	// redeliveries.

	// Migrate a legacy/pre-persistence state hash to persistent BEFORE the
	// invocation-state handle is used: a hash written with an old fixed TTL must
	// not expire under a redelivery. This is a no-op for an absent hash and for
	// an already terminal-retained hash.
	//
	// A failure is a Redis-state failure, not a best-effort migration: the hash
	// may still carry its old TTL and expire mid-delivery, so the state cannot be
	// relied on. Do NOT dispatch the handler and do NOT ACK: the message is left
	// pending (a later reclaim retries it), exactly like a transport failure on
	// TryStart. This keeps the at-least-once contract intact; a transient Redis
	// fault self-heals on the next delivery.
	if err := c.invStateStore.makeRecoverable(ctx, c.stream, c.group, msg.ID); err != nil {
		c.log.Warn("Message: make invocation state recoverable failed; leaving pending",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
			"error", err,
		)
		return
	}

	// Inject a per-message invocation-state handle so the runner can skip
	// invocations that already completed on a previous delivery or are protected
	// by an active attempt deadline. The handle is bound to this (stream, group,
	// msgID) and reads/writes the invocation-state hash in Redis.
	handlerCtx := WithDeliveryAttempt(ctx, deliveryNum)
	handlerCtx = WithInvocationState(handlerCtx,
		NewInvocationState(ctx, c.invStateStore, c.stream, c.group, msg.ID, c.log))

	if claimKind == schedule.ValidScheduleClaim && c.scheduleRunner != nil {
		outcome, spanErr = c.processScheduleMessage(ctx, msg.ID, deliveryNum, occ)
		return
	}

	// A child span around the handler dispatch (the runner's matching and
	// execution) keeps the Redis bookkeeping of processMessage visibly separate
	// from invocation execution, which the runner further instruments.
	dispatchCtx, dispatchSpan := tracing.Start(handlerCtx, "stream.dispatch")
	err = handler(dispatchCtx, msg.ID, event)
	if err != nil {
		dispatchSpan.RecordError(err)
		dispatchSpan.SetStatus(codes.Error, err.Error())
	}
	dispatchSpan.End()
	if err != nil {
		// If we are shutting down (ctx cancelled), this is not a real attempt: do
		// not count it nor DLQ the message — leave it pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Debug("Message: handler canceled during shutdown; leaving pending", "message_id", msg.ID)
			return
		}
		// A protected invocation (running on another replica, or waiting out its
		// retry backoff) means the message must stay pending but is NOT a failed
		// attempt: no retry accounting, no DLQ. The protected invocation may
		// still complete or fail on its own, so the message must not be
		// acknowledged (this is the cross-replica ACK-hazard fix).
		if errors.Is(err, ErrInvocationNotEligible) {
			c.log.Debug("Message: invocation(s) not eligible (running or waiting for retry); leaving pending",
				"message_id", msg.ID,
				"delivery_attempt", deliveryNum,
			)
			return
		}
		// A terminal message: every non-complete matched invocation is exhausted,
		// so the whole message is routed to the DLQ. This is the per-invocation
		// exhaustion path that replaces the old message-level max-attempts check.
		if errors.Is(err, ErrInvocationExhausted) {
			c.log.Error("Message: invocation(s) exhausted; routing to DLQ",
				"message_id", msg.ID,
				"delivery_attempt", deliveryNum,
				"reason", err,
			)
			outcome, spanErr = "dlq", err
			c.routeToDLQ(ctx, msg, err, deliveryNum)
			return
		}
		c.log.Warn("Message: retryable failure; leaving pending for a later reclaim",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
			"reason", err,
		)
		// A retryable failure: this delivery will be retried, so it counts as a
		// retry event. The message stays pending for a later reclaim.
		return
	}

	ackCtx, ackSpan := redisSpan(ctx, "redis.ack")
	if err := c.client.XAck(ackCtx, c.stream, c.group, msg.ID).Err(); err != nil {
		ackSpan.RecordError(err)
		ackSpan.SetStatus(codes.Error, err.Error())
		ackSpan.End()
		c.log.Warn("Message: ack failed", "message_id", msg.ID, "error", err)
		c.noteOutcome(err, 0)
		return
	}
	ackSpan.End()
	outcome = "acked"
	// The message is fully processed and acknowledged: switch its invocation
	// state to terminal retention. Ordering matters — retain only AFTER a
	// successful ACK. If the ACK failed (handled above) the message stays in the
	// PEL and may be redelivered, so its state must remain recoverable
	// (persistent, no TTL) for the redelivery to skip completed handlers. A
	// retention failure is logged only: the state is simply left persistent (a
	// leak, never a premature loss).
	if err := c.invStateStore.retainTerminal(ctx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
		c.log.Warn("Message: retain invocation state failed", "message_id", msg.ID, "error", err)
	}
}

// processScheduleMessage routes a schedule-occurrence message directly to the
// ScheduleRunner (which validates the occurrence against the current template,
// resolves the app's current timeout from the registry, and executes it),
// bypassing event matching entirely. It shares the exact delivery contract of
// processMessage: invocation state protects redeliveries
// (complete/running/backoff/exhausted), and the message is ACKed on success,
// left pending on a retryable failure or protected skip, and routed to the DLQ
// on exhaustion OR on a semantic validation failure (ErrScheduleInvalid).
//
// It returns the terminal outcome label and an error for the caller's message
// span, so the schedule path is traced identically to the event path.
//
// Schedule occurrences are deliberately NOT counted in the event-classification
// counters (received/matched/unmatched): they bypass event matching, so they
// have no meaningful matched/unmatched class and would otherwise break the
// partition invariant. Schedule activity is accounted by the schedule
// publication counters and the per-app handler counters.
func (c *Consumer) processScheduleMessage(ctx context.Context, msgID string, deliveryNum int64, occ schedule.Occurrence) (string, error) {
	c.log.Debug("Schedule: executing occurrence",
		"app", occ.App,
		"schedule", occ.Schedule,
		"handler", occ.Handler,
		"occurrence_id", occ.ID(),
		"scheduled_at", occ.ScheduledAt.UTC().Format(time.RFC3339),
		"message_id", msgID,
		"delivery_attempt", deliveryNum,
	)

	// Migrate a legacy/pre-persistence state hash to persistent BEFORE the
	// handle is used, exactly like processMessage (see makeRecoverable).
	//
	// A failure is a Redis-state failure, not a best-effort migration: the hash
	// may still carry its old TTL and expire mid-delivery, so the state cannot be
	// relied on. Return a pending outcome so the caller does NOT ACK or DLQ the
	// message: it stays pending in the PEL for a later reclaim, preserving the
	// at-least-once contract while a transient Redis fault self-heals.
	if err := c.invStateStore.makeRecoverable(ctx, c.stream, c.group, msgID); err != nil {
		c.log.Warn("Schedule: make invocation state recoverable failed; leaving pending",
			"message_id", msgID,
			"delivery_attempt", deliveryNum,
			"error", err,
		)
		return "pending", err
	}

	// Inject the same per-message context as processMessage: the delivery-attempt
	// number and the invocation-state handle bound to this (stream, group, msgID).
	// The ScheduleRunner (runner.InvokeHandler) participates in the SAME
	// TryStart / complete / failure / exhaustion lifecycle as event handlers via
	// this handle: a completed schedule invocation is skipped and ACKed, a
	// protected (running or backoff) one stays pending, and an exhausted one
	// routes to the DLQ.
	handlerCtx := WithDeliveryAttempt(ctx, deliveryNum)
	handlerCtx = WithInvocationState(handlerCtx,
		NewInvocationState(ctx, c.invStateStore, c.stream, c.group, msgID, c.log))

	err := c.scheduleRunner(handlerCtx, msgID, occ, occ.Payload())
	if err != nil {
		// Shutting down: not a real attempt; leave pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Debug("Schedule: message canceled during shutdown; leaving pending", "message_id", msgID)
			return "pending", nil
		}
		// A protected invocation (running on another replica, or waiting out its
		// retry backoff) means the message stays pending, not a failed attempt:
		// no retry accounting, no DLQ.
		if errors.Is(err, ErrInvocationNotEligible) {
			c.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
				"message_id", msgID,
				"delivery_attempt", deliveryNum,
			)
			return "pending", nil
		}
		// An obsolete invocation: the app or its schedule entry/handler was
		// removed from the current configuration while the message was pending.
		// That removal is an intentional configuration change, so the message is
		// terminal but MUST NOT be retried or routed to the DLQ — acknowledge it
		// (and then switch its invocation state to terminal retention), exactly
		// like the success tail. Note the ordering: this must run BEFORE the
		// exhaustion check, because an obsolete occurrence is never exhausted
		// (exhaustion implies retries were attempted, which an obsolete
		// occurrence never is).
		if errors.Is(err, ErrInvocationObsolete) {
			c.log.Debug("Schedule: occurrence obsolete (app or schedule removed); acknowledging",
				"message_id", msgID,
				"delivery_attempt", deliveryNum,
				"reason", err,
			)
			ackErr := c.client.XAck(ctx, c.stream, c.group, msgID).Err()
			if ackErr != nil {
				c.log.Warn("Schedule: message ack failed", "message_id", msgID, "error", ackErr)
				c.noteOutcome(ackErr, 0)
				return "pending", ackErr
			}
			// Retain the invocation-state hash after a successful ACK, exactly
			// like the success tail. A retention failure is logged only; the
			// state is left persistent (a leak, never a premature loss).
			if cerr := c.invStateStore.retainTerminal(ctx, c.stream, c.group, msgID, c.invocationRetention); cerr != nil {
				c.log.Warn("Schedule: message retain invocation state failed", "message_id", msgID, "error", cerr)
			}
			return "acked", nil
		}
		// A semantically invalid occurrence: the app and schedule exist and are
		// available, but scheduled_at is not a real firing of the schedule's
		// CURRENT cron definition (a forged/non-firing instant), or the schedule's
		// own cron could not be parsed. The claim can never succeed, so it is
		// terminal and MUST be dead-lettered rather than retried or ACKed as
		// success. This is a distinct disposition from obsolete (removed
		// configuration, ACKed) and from a temporary failure (retried).
		if errors.Is(err, ErrScheduleInvalid) {
			c.log.Error("Schedule: invalid occurrence claim; routing to DLQ",
				"message_id", msgID,
				"delivery_attempt", deliveryNum,
				"reason", err,
			)
			if envelope, eerr := occ.Envelope(); eerr == nil {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID, Values: map[string]any{"event": string(envelope)}}, err, deliveryNum)
			} else {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID}, err, deliveryNum)
			}
			return "dlq", err
		}
		// A terminal message: the schedule invocation is exhausted, so the message
		// routes to the DLQ.
		if errors.Is(err, ErrInvocationExhausted) {
			c.log.Error("Schedule: invocation exhausted; routing to DLQ",
				"message_id", msgID,
				"delivery_attempt", deliveryNum,
				"reason", err,
			)
			// Rebuild the message with its envelope so the DLQ entry carries the
			// schedule body (mirroring a normal event's `event` field).
			if envelope, eerr := occ.Envelope(); eerr == nil {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID, Values: map[string]any{"event": string(envelope)}}, err, deliveryNum)
			} else {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID}, err, deliveryNum)
			}
			return "dlq", err
		}
		// A retryable failure: leave pending for a later reclaim. The
		// invocation error is recorded by the runner's function.invoke span, so
		// the message span only records the pending disposition.
		c.log.Warn("Schedule: retryable failure; leaving pending for a later reclaim",
			"message_id", msgID,
			"delivery_attempt", deliveryNum,
			"reason", err,
		)
		return "pending", nil
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msgID).Err(); err != nil {
		c.log.Warn("Schedule: message ack failed", "message_id", msgID, "error", err)
		c.noteOutcome(err, 0)
		return "pending", err
	}
	// Switch the invocation-state hash to terminal retention after a successful
	// ACK, exactly like processMessage. A retention failure is logged only; the
	// state is left persistent (a leak, never a premature loss).
	if err := c.invStateStore.retainTerminal(ctx, c.stream, c.group, msgID, c.invocationRetention); err != nil {
		c.log.Warn("Schedule: message retain invocation state failed", "message_id", msgID, "error", err)
	}
	return "acked", nil
}

// unpersistedDLQSpecs filters the DLQ entry specs down to those whose entry has
// not already been persisted, consulting each invocation's per-invocation
// persistence marker ("exhausted:<attempt>:<token>:dlq") rather than scanning
// the DLQ stream. Skipping already-persisted entries is what makes retrying a
// partially-written multi-entry DLQ (after a failed XACK, a crash, or a partial
// write) idempotent: the retry writes only the missing entries and never
// duplicates the ones that succeeded.
//
// It also returns each retained spec's exact exhausted claim identity (from the
// marker) so the subsequent DLQ-persistence upgrade CASes that identity, never a
// guessed one. A spec with no well-formed exhausted marker carries a zero claim;
// the upgrade then refuses (a marker-less invocation cannot be upgraded, and its
// entry is still written because it was not marked persisted).
//
// A store read error fails safe: the spec is kept so the entry is rewritten (a
// duplicate is allowed under at-least-once, while skipping a required write
// would lose the entry). A spec with no invocation ID (the malformed-message
// placeholder) is always kept; it has no persistence marker to consult.
func (c *Consumer) unpersistedDLQSpecs(ctx context.Context, msgID string, specs []dlqEntrySpec) []dlqEntrySpec {
	out := make([]dlqEntrySpec, 0, len(specs))
	for _, spec := range specs {
		if spec.invocation == "" {
			out = append(out, spec)
			continue
		}
		claim, persisted, ok, err := c.invStateStore.exhaustedState(ctx, c.stream, c.group, msgID, spec.invocation)
		if err != nil {
			c.log.Warn("Message: DLQ persistence check failed; rewriting entry",
				"message_id", msgID, "invocation", spec.invocation, "error", err)
			out = append(out, spec)
			continue
		}
		if ok {
			spec.claim = claim
		}
		if persisted {
			c.log.Debug("Message: DLQ entry already persisted; skipping write",
				"message_id", msgID, "invocation", spec.invocation)
			continue
		}
		out = append(out, spec)
	}
	return out
}

// dlqTraceFor reads the compact trace lineage persisted for one exhausted
// invocation, to be written into its DLQ entry. It consults the invocation-state
// hash (the "last failed attempt" reference), which the runner wrote on the
// final attempt and which survives until the state hash is cleared after the DLQ
// write + ACK. It is best-effort: a placeholder invocation (empty ID) or a read
// failure yields "" (an omitted field), never a failed DLQ write. Only the
// compact traceparent[|tracestate] form is stored, so baggage can never reach
// the DLQ.
func (c *Consumer) dlqTraceFor(ctx context.Context, msgID, invocation string) string {
	if invocation == "" {
		return ""
	}
	lineage, err := c.invStateStore.traceReference(ctx, c.stream, c.group, msgID, invocation)
	if err != nil {
		c.log.Debug("Message: DLQ trace reference read failed; writing entry without trace",
			"message_id", msgID, "invocation", invocation, "error", err)
		return ""
	}
	return lineage
}

// routeToDLQ writes one DLQ entry per exhausted invocation and only then acks
// the original. The XADD-before-XACK ordering matters: if any DLQ write fails
// the original stays pending so the next recovery cycle retries the remaining
// writes rather than losing the message.
//
// Per-invocation entries: reason is the runner's terminal *HandlerExhaustedError
// carrying the exact app/handler and exhausted attempt for every terminal
// invocation, so a message matching several apps or handlers produces one
// precisely-attributed entry each (see dlqEntrySpecs). A reason without that
// typed metadata (a malformed message routed pre-handler) produces a single
// placeholder entry with an explicit handler_attempts of 0, never one invented
// from the delivery count.
//
// Idempotent retry without scanning the DLQ: each invocation's exhausted marker
// records whether its entry has already been persisted
// ("exhausted:<attempt>:<token>:dlq") and retains the exhausted claim identity.
// On a redelivery after an XACK failure or a crash — or after a partial
// multi-entry write — invocations whose entry already exists are skipped, so the
// retry writes only the missing entries and can never duplicate (or lose) the
// ones that succeeded. The upgrade CASes the retained exhausted identity, so a
// stale XADD outcome can never downgrade a newer exhausted marker or a success.
//
// deliveryAttempts is the authoritative Redis Stream/PEL delivery count passed
// through the consumer/reclaim flow (the DLQ `deliveries` field, diagnostic
// only).
func (c *Consumer) routeToDLQ(
	ctx context.Context,
	msg redis.XMessage,
	reason error,
	deliveries int64,
) {
	event := eventString(msg)
	specs := c.unpersistedDLQSpecs(ctx, msg.ID, dlqEntrySpecs(reason))
	if len(specs) == 0 {
		// Every invocation's entry was already persisted (a redelivery after an
		// XACK failure, or a fully-written DLQ whose ACK failed). There is
		// nothing left to write; fall through to the ACK so the message leaves
		// the PEL with exactly the entries already written, never duplicated.
		c.log.Debug("Message: all DLQ entries already persisted; acking without rewrite",
			"message_id", msg.ID)
	}
	// An oversized-event rejection overrides the DLQ `event` field with a
	// bounded diagnostic summary, so the oversized raw payload is never copied
	// into the DLQ. Every other path keeps the original raw event verbatim.
	if errors.Is(reason, ErrEventOversized) {
		var oversize *EventOversizedError
		if errors.As(reason, &oversize) {
			event = oversizedEventSummary(oversize.Bytes, oversize.MaxBytes)
		}
	}
	// One child span around the whole DLQ write+ack operation. It is a child of
	// the current message span; the payload and reason text are never attached
	// (the reason may quote handler output).
	dlqCtx, dlqSpan := redisSpan(ctx, "redis.dlq")
	defer dlqSpan.End()
	ctx = dlqCtx
	for _, spec := range specs {
		// The compact lineage of the final failed invocation, read from the
		// invocation-state hash just before the entry is written (best-effort:
		// no lineage yields an omitted field). It is a sibling field, not part
		// of the lifecycle value, so it survives until the state hash is
		// cleared after the DLQ write + ACK.
		spec.trace = c.dlqTraceFor(ctx, msg.ID, spec.invocation)
		entry := dlqPayload(
			c.stream, msg.ID, c.group, c.consumer,
			event, spec.reason, spec.app, spec.handler, deliveries, spec.attempts, spec.trace,
		)
		if _, err := c.client.XAdd(ctx, &redis.XAddArgs{
			Stream: c.dlqStream,
			Values: entry,
		}).Result(); err != nil {
			dlqSpan.RecordError(err)
			dlqSpan.SetStatus(codes.Error, err.Error())
			c.log.Error("Message: DLQ write failed (leaving pending)",
				"message_id", msg.ID,
				"delivery_attempt", deliveries,
				"invocation", spec.invocation,
				"error", err,
			)
			c.noteOutcome(err, 0)
			return
		}
		c.metrics.Inc(metrics.MetricDLQEntries)
		// Record per-invocation DLQ persistence ONLY after the XADD succeeded,
		// so a failed write is retried on redelivery while a successful one is
		// skipped. The marker is upgraded CASed on the exact exhausted claim
		// identity read from the marker (spec.claim), so a stale XADD outcome can
		// never downgrade a newer exhausted marker or a success. A mark failure is
		// logged only: the entry is already written and the worst case is a
		// duplicate on the next redelivery.
		if spec.invocation != "" {
			if _, err := c.invStateStore.markExhaustedDLQ(ctx, c.stream, c.group, msg.ID, spec.invocation, spec.claim); err != nil {
				c.log.Warn("Message: mark DLQ persisted failed",
					"message_id", msg.ID, "invocation", spec.invocation, "error", err)
			}
		}
		c.log.Error("Message: invocation routed to DLQ",
			"message_id", msg.ID,
			"dlq_stream", c.dlqStream,
			"app", spec.app,
			"handler", spec.handler,
			"handler_attempts", spec.attempts,
			"delivery_attempt", deliveries,
			"reason", spec.reason,
		)
	}
	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		dlqSpan.RecordError(err)
		dlqSpan.SetStatus(codes.Error, err.Error())
		c.log.Warn("Message: ack after DLQ failed", "message_id", msg.ID, "error", err)
		c.noteOutcome(err, 0)
		return
	}
	// The message is dead-lettered and the original acked: switch its
	// invocation-state hash to terminal retention. Ordering matters — retain only
	// after BOTH every DLQ write and the ACK succeed. If the ACK failed (handled
	// above) the message stays in the PEL and may be redelivered, so its state
	// (including the per-invocation DLQ-persisted markers) must remain
	// recoverable (persistent, no TTL). A retention failure is logged only; the
	// state is left persistent (a leak, never a premature loss).
	if err := c.invStateStore.retainTerminal(ctx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
		c.log.Warn("Message: retain invocation state after DLQ failed", "message_id", msg.ID, "error", err)
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

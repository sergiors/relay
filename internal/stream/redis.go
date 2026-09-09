package stream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/logging"
	"relay/internal/metrics"
)

// Handler is the contract between the stream layer and the runner. It receives
// the message ID and the decoded event, and returns nil only when the event
// should be acknowledged.
type Handler func(ctx context.Context, msgID string, event map[string]any) error

// Defaults for the retry/recovery settings. They are fixed application
// constants, not env-configurable; zero-valued ConsumerConfig fields fall back
// to these in NewConsumer.
const (
	DefaultBlock           = 5 * time.Second
	DefaultCount           = int64(10)
	DefaultMaxAttempts     = int64(5)
	DefaultReclaimInterval = time.Minute
	DefaultMinPendingIdle  = time.Minute
	// DefaultMetricsInterval is how often the pending-gauge sampler samples
	// XPENDING depth and logs the metrics snapshot. It is overridable via
	// ConsumerConfig.MetricsInterval (used by tests).
	DefaultMetricsInterval = 15 * time.Second
)

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
	// MaxAttempts is the maximum number of delivery attempts before a message
	// that keeps failing is routed to the DLQ. Defaults to 5 if zero.
	MaxAttempts int64
	// ReclaimInterval is how often the recovery loop scans for idle pending
	// messages. Defaults to 1m if zero.
	ReclaimInterval time.Duration
	// MinPendingIdle is the minimum time a message must have sat pending before
	// it is eligible for reclamation. Defaults to 1m if zero. It must comfortably
	// exceed normal processing time so in-flight messages are not reclaimed.
	MinPendingIdle time.Duration
	// DLQStream is the stream that exhausted/poison messages are written to.
	// Defaults to "<Stream>:dlq" if empty.
	DLQStream string
	Log       *log.Logger
	// Metrics is an optional metrics registry. A nil registry disables all
	// observability: every metric call is a no-op.
	Metrics *metrics.Registry
	// MetricsInterval is how often the pending-gauge sampler runs and the
	// metrics snapshot is logged. Defaults to DefaultMetricsInterval if zero.
	MetricsInterval time.Duration
	// DisableProgress disables per-handler invocation-state tracking. When
	// true, redelivered messages re-run every matching handler exactly as before
	// (at-least-once, no skip). Test/ops hook: invocation-state tracking is
	// enabled by default (zero value); set true only in tests.
	DisableProgress bool
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
	maxAttempts     int64
	reclaimInterval time.Duration
	minPendingIdle  time.Duration
	dlqStream       string
	log             *log.Logger
	metrics         *metrics.Registry
	metricsInterval time.Duration
	backoff         *backoff
	invocationStore *invocationStore
	healthy         atomic.Bool
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	if cfg.Block == 0 {
		cfg.Block = DefaultBlock
	}
	if cfg.Count == 0 {
		cfg.Count = DefaultCount
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.ReclaimInterval == 0 {
		cfg.ReclaimInterval = DefaultReclaimInterval
	}
	if cfg.MinPendingIdle == 0 {
		cfg.MinPendingIdle = DefaultMinPendingIdle
	}
	if cfg.DLQStream == "" {
		cfg.DLQStream = cfg.Stream + ":dlq"
	}
	if cfg.Log == nil {
		cfg.Log = log.Default()
	}
	if cfg.MetricsInterval == 0 {
		cfg.MetricsInterval = DefaultMetricsInterval
	}
	c := &Consumer{
		client:          cfg.Client,
		stream:          cfg.Stream,
		group:           cfg.Group,
		consumer:        cfg.Consumer,
		block:           cfg.Block,
		count:           cfg.Count,
		maxAttempts:     cfg.MaxAttempts,
		reclaimInterval: cfg.ReclaimInterval,
		minPendingIdle:  cfg.MinPendingIdle,
		dlqStream:       cfg.DLQStream,
		log:             cfg.Log,
		metrics:         cfg.Metrics,
		metricsInterval: cfg.MetricsInterval,
		backoff:         newBackoff(cfg.backoffTable, cfg.backoffJitter),
	}
	// Invocation-state tracking is always constructed when a client is present
	// (the consumer always has one). DisableProgress turns it off for tests.
	if !cfg.DisableProgress {
		c.invocationStore = &invocationStore{client: cfg.Client}
	}
	c.healthy.Store(true)
	return c
}

// EnsureGroup creates the consumer group if it does not exist, tolerating a
// group that already exists (BUSYGROUP). MKSTREAM creates the stream if needed;
// the group reads from "0" so only new messages are consumed.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err == nil {
		return nil
	}
	// BUSYGROUP means the group already exists; that is not a failure.
	if isBusyGroup(err) {
		return nil
	}
	return fmt.Errorf("create consumer group %q on stream %q: %w", c.group, c.stream, err)
}

// Healthy reports whether the consumer's last observed Redis operation
// succeeded. It is the readiness signal for the `relay health` command: true
// while Redis is reachable, false during an outage.
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
			c.log.Printf("redis read failed: %v; retrying in %s", err, delay)
		}
		return
	}
	// Success (or redis.Nil): mark healthy and reset backoff on the transition.
	if !c.healthy.Swap(true) {
		c.log.Printf("redis connection recovered")
	}
	c.backoff.reset()
}

// Consume reads messages from the stream and calls handler for each decoded
// event. A message is acknowledged (XACK) only after the handler returns nil or
// it is routed to the DLQ. Consume blocks until ctx is cancelled, and stops the
// recovery goroutine before returning so nothing leaks.
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

	// The metrics sampler runs in its own goroutine and never affects health,
	// backoff, or processing. It is stopped and joined before Consume returns.
	if c.metrics != nil {
		sctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.metricsLoop(sctx)
		}()
		defer func() {
			stop()
			<-done
		}()
	}

	for {
		streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    c.count,
			Block:    c.block,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
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
				return nil
			case <-time.After(delay):
			}
			continue
		}

		c.noteOutcome(nil, 0)
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				// A message read fresh from XREADGROUP is on its first delivery.
				c.process(ctx, msg, 1, handler)
			}
		}
	}
}

// process routes a freshly-read message to the shared processMessage path. Since
// XPendingExt is the source of truth for retry counts (see reclaimTick), fresh
// XREADGROUP reads are always treated as delivery attempt 1.
func (c *Consumer) process(
	ctx context.Context,
	msg redis.XMessage,
	deliveryNum int64,
	handler Handler,
) {
	c.processMessage(ctx, msg, deliveryNum, handler)
}

// metricsLoop samples the XPENDING pending-depth gauges once per interval until
// ctx is cancelled. It is decoupled from health/backoff/processing: a Redis
// failure just skips the pending gauges for that tick and is logged quietly. The
// overall registry snapshot is exposed by the worker's LogLoop; this goroutine
// only produces the pending gauges.
func (c *Consumer) metricsLoop(ctx context.Context) {
	t := time.NewTicker(c.metricsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.samplePending(ctx)
		}
	}
}

// samplePending reads the XPENDING summary (Count and the oldest pending ID) and
// records the pending-depth gauges. It never blocks processing: it runs in its
// own goroutine, holds no locks across the Redis call, and any error is logged
// and skipped, never propagated.
func (c *Consumer) samplePending(ctx context.Context) {
	// The summary form (no Start/End/Count) is O(1)-ish and returns the total
	// Count plus the oldest pending message ID in Lower.
	p, err := c.client.XPending(ctx, c.stream, c.group).Result()
	if err != nil {
		c.log.Printf("metrics: xpending %q/%q: %v", c.stream, c.group, err)
		return
	}
	c.metrics.SetGauge("pending_entries", float64(p.Count))
	if age, ok := pendingAge(p.Lower); ok {
		c.metrics.SetGauge("pending_oldest_age_seconds", age.Seconds())
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

// reclaimTick finds pending messages idle beyond MinPendingIdle, takes ownership
// of them for this consumer, and runs them through the shared processing path.
//
// XPendingExt (the full form) is used for the per-message retry count because
// XAUTOCLAIM (RESP2) does not return delivery counts; the retry counter must
// come from Redis (survives restarts), not from in-process state.
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
	start := "0-0"
	for {
		if ctx.Err() != nil {
			return
		}
		msgs, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumer,
			MinIdle:  c.minPendingIdle,
			Start:    start,
			Count:    c.count,
		}).Result()
		if err != nil {
			c.noteOutcome(err, c.backoff.peek())
			return
		}
		for _, msg := range msgs {
			// A pending entry whose message was deleted returns nil values; ack it
			// only to clear the PEL — there is nothing to process.
			if msg.Values == nil {
				c.log.Printf("message %q: deleted from stream; acking to clear PEL", msg.ID)
				if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
					c.log.Printf("message %q: ack deleted entry: %v", msg.ID, err)
				}
				continue
			}
			pe := byID[msg.ID]
			c.log.Printf("reclaimed message %q for consumer %q (idle %s, attempts %d)",
				msg.ID, c.consumer, pe.Idle, pe.RetryCount)
			c.deliverClaimed(ctx, msg, pe.RetryCount, handler)
		}
		if len(msgs) == 0 || next == "0-0" {
			break
		}
		start = next
	}
}

// deliverClaimed runs a reclaimed message through the shared path. A message
// whose retry count already equals or exceeds the limit is routed to the DLQ
// without re-running the handler (its attempts are exhausted). Otherwise this
// is delivery attempt retryCount+1.
func (c *Consumer) deliverClaimed(
	ctx context.Context,
	msg redis.XMessage,
	retryCount int64,
	handler Handler,
) {
	if retryCount >= c.maxAttempts {
		c.log.Printf("message %q: retry %d/%d failed: max attempts reached%s",
			msg.ID, retryCount+1, c.maxAttempts,
			logging.Fields("message_id", msg.ID, "attempt", retryCount+1, "attempts_total", c.maxAttempts))
		c.routeToDLQ(ctx, msg, fmt.Errorf("max attempts reached after %d deliveries", retryCount), retryCount+1)
		return
	}
	// A reclaimed (redelivered) message is an additional delivery attempt: it is a
	// retry/redelivery event, counted here at the stream layer. This is the
	// message-level retry counter, distinct from the per-function
	// function_retries_total (runner), which counts every failing rule execution.
	c.metrics.Inc("retries_total")
	c.processMessage(ctx, msg, retryCount+1, handler)
}

// processMessage is the single shared path used by both the XREADGROUP loop and
// the recovery loop. It decodes, classifies, and either ACKs on success, routes
// to the DLQ on a non-retryable failure or when retries are exhausted, or leaves
// the message pending for a later retry.
func (c *Consumer) processMessage(
	ctx context.Context,
	msg redis.XMessage,
	deliveryNum int64,
	handler Handler,
) {
	event, err := classifyMessage(msg)
	if err != nil {
		// A malformed message can never succeed, so it goes straight to the DLQ on
		// first encounter rather than consuming retry cycles.
		c.log.Printf("message %q: non-retryable failure (%v); routing to DLQ%s",
			msg.ID, err, logging.Fields("message_id", msg.ID, "attempt", deliveryNum, "reason", err))
		c.routeToDLQ(ctx, msg, err, deliveryNum)
		return
	}

	// The message decoded successfully and is about to be handed to the handler.
	// This is the single message-level "processed" counter in the stream layer:
	// an event matching N functions counts once globally here (per-function
	// attribution lives in function_events_total). It is incremented on EVERY
	// delivery attempt that reaches the handler handoff, including redeliveries,
	// so retries increment it too — it is a delivery-attempt counter, not a
	// unique-event counter.
	c.metrics.Inc("events_processed_total")

	// Inject a per-message invocation-state handle so the runner can skip
	// invocations that already completed on a previous delivery. The handle is
	// bound to this (stream, group, msgID) and reads/writes the invocation-state
	// hash in Redis. When invocation-state tracking is disabled (tests) the
	// context carries none and the runner behaves exactly as before.
	handlerCtx := WithDeliveryAttempt(ctx, deliveryNum)
	if c.invocationStore != nil {
		handlerCtx = WithInvocationState(handlerCtx, &invocationState{
			ctx:    ctx,
			store:  c.invocationStore,
			stream: c.stream,
			group:  c.group,
			msgID:  msg.ID,
			log:    c.log,
		})
	}

	if err := handler(handlerCtx, msg.ID, event); err != nil {
		// If we are shutting down (ctx cancelled), this is not a real attempt: do
		// not count it nor DLQ the message — leave it pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Printf("message %q: handler canceled during shutdown; leaving pending", msg.ID)
			return
		}
		c.log.Printf("message %q: retry %d/%d failed: %v%s",
			msg.ID, deliveryNum, c.maxAttempts, err,
			logging.Fields("message_id", msg.ID, "attempt", deliveryNum, "attempts_total", c.maxAttempts))
		if deliveryNum >= c.maxAttempts {
			c.routeToDLQ(ctx, msg, err, deliveryNum)
			return
		}
		// A retryable failure: this delivery will be retried, so it counts as a
		// retry event.
		c.metrics.Inc("retries_total")
		return
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Printf("message %q: ack: %v", msg.ID, err)
		c.noteOutcome(err, 0)
		return
	}
	// The message is fully processed and acknowledged: eagerly clear its
	// invocation-state hash. Ordering matters — clear only AFTER a successful
	// ACK. If the ACK failed (handled above) the message stays in the PEL and
	// may be redelivered, so its state must remain for the redelivery to skip
	// completed handlers. A clear failure is logged only; the TTL is the
	// fallback cleanup.
	if c.invocationStore != nil {
		if err := c.invocationStore.clear(ctx, c.stream, c.group, msg.ID); err != nil {
			c.log.Printf("message %q: clear invocation state: %v", msg.ID, err)
		}
	}
}

// routeToDLQ writes the message to the DLQ and only then acks the original. The
// XADD-before-XACK ordering matters: if the DLQ write fails the original stays
// pending so the next recovery cycle retries the DLQ write rather than losing
// the message.
func (c *Consumer) routeToDLQ(
	ctx context.Context,
	msg redis.XMessage,
	reason error,
	attempts int64,
) {
	entry := dlqPayload(
		c.stream, msg.ID, c.group, c.consumer,
		eventString(msg), reason.Error(), attempts,
	)
	if _, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: c.dlqStream,
		Values: entry,
	}).Result(); err != nil {
		c.log.Printf("message %q: DLQ write failed (leaving pending): %v%s",
			msg.ID, err, logging.Fields("message_id", msg.ID, "attempt", attempts))
		c.noteOutcome(err, 0)
		return
	}
	c.metrics.Inc("dlq_entries_total")
	c.log.Printf("message %q: routed to DLQ stream %q after %d attempts: %v%s",
		msg.ID, c.dlqStream, attempts, reason,
		logging.Fields("message_id", msg.ID, "attempt", attempts, "reason", reason))
	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Printf("message %q: ack after DLQ: %v", msg.ID, err)
		c.noteOutcome(err, 0)
		return
	}
	// The message is dead-lettered and the original acked: eagerly clear its
	// invocation-state hash. Ordering matters — clear only after BOTH the DLQ
	// write and the ACK succeed. If the ACK failed (handled above) the message
	// stays in the PEL and may be redelivered, so its state must remain. A clear
	// failure is logged only; the TTL is the fallback cleanup.
	if c.invocationStore != nil {
		if err := c.invocationStore.clear(ctx, c.stream, c.group, msg.ID); err != nil {
			c.log.Printf("message %q: clear invocation state after DLQ: %v", msg.ID, err)
		}
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

package stream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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
	return &Consumer{
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
	}
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
				// Block timed out with no messages; keep looping.
				continue
			}
			// A transient Redis failure (restart, flaky network) should not kill the
			// process; go-redis re-establishes connections. Sleep a bit so we do not
			// hammer Redis while it is down, then keep looping.
			c.log.Printf("read group from stream %q: %v; retrying", c.stream, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(c.block):
			}
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				// A message read fresh from XREADGROUP is on its first delivery.
				c.process(ctx, msg, 1, handler)
			}
		}
	}
}

// process hands a freshly-read message to the shared processMessage path. Since
// XPendingExt is the source of truth for retry counts (see reclaimTick), fresh
// XREADGROUP reads are always treated as delivery attempt 1.
func (c *Consumer) process(ctx context.Context, msg redis.XMessage, deliveryNum int64, handler Handler) {
	c.processMessage(ctx, msg, deliveryNum, handler)
}

// reclaimLoop paces the recovery loop with a ticker so idle pending messages are
// re-delivered without busy-spinning. It returns when ctx is cancelled.
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
		c.log.Printf("recovery tick failed: %v", err)
		return
	}
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
			c.log.Printf("recovery tick failed: %v", err)
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
func (c *Consumer) deliverClaimed(ctx context.Context, msg redis.XMessage, retryCount int64, handler Handler) {
	if retryCount >= c.maxAttempts {
		c.log.Printf("message %q: retry %d/%d failed: max attempts reached",
			msg.ID, retryCount+1, c.maxAttempts)
		c.routeToDLQ(ctx, msg, fmt.Errorf("max attempts reached after %d deliveries", retryCount), retryCount+1)
		return
	}
	c.processMessage(ctx, msg, retryCount+1, handler)
}

// processMessage is the single shared path used by both the XREADGROUP loop and
// the recovery loop. It decodes, classifies, and either ACKs on success, routes
// to the DLQ on a non-retryable failure or when retries are exhausted, or leaves
// the message pending for a later retry.
func (c *Consumer) processMessage(ctx context.Context, msg redis.XMessage, deliveryNum int64, handler Handler) {
	event, err := classifyMessage(msg)
	if err != nil {
		// A malformed message can never succeed, so it goes straight to the DLQ on
		// first encounter rather than consuming retry cycles.
		c.log.Printf("message %q: non-retryable failure (%v); routing to DLQ", msg.ID, err)
		c.routeToDLQ(ctx, msg, err, deliveryNum)
		return
	}

	if err := handler(ctx, msg.ID, event); err != nil {
		// If we are shutting down (ctx cancelled), this is not a real attempt: do
		// not count it nor DLQ the message — leave it pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Printf("message %q: handler canceled during shutdown; leaving pending", msg.ID)
			return
		}
		c.log.Printf("message %q: retry %d/%d failed: %v", msg.ID, deliveryNum, c.maxAttempts, err)
		if deliveryNum >= c.maxAttempts {
			c.routeToDLQ(ctx, msg, err, deliveryNum)
		}
		return
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Printf("message %q: ack: %v", msg.ID, err)
	}
}

// routeToDLQ writes the message to the DLQ and only then acks the original. The
// XADD-before-XACK ordering matters: if the DLQ write fails the original stays
// pending so the next recovery cycle retries the DLQ write rather than losing
// the message.
func (c *Consumer) routeToDLQ(ctx context.Context, msg redis.XMessage, reason error, attempts int64) {
	entry := dlqPayload(
		c.stream, msg.ID, c.group, c.consumer,
		eventString(msg), reason.Error(), attempts,
	)
	if _, err := c.client.XAdd(ctx, &redis.XAddArgs{Stream: c.dlqStream, Values: entry}).Result(); err != nil {
		c.log.Printf("message %q: DLQ write failed (leaving pending): %v", msg.ID, err)
		return
	}
	c.log.Printf("message %q: routed to DLQ stream %q after %d attempts: %v",
		msg.ID, c.dlqStream, attempts, reason)
	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Printf("message %q: ack after DLQ: %v", msg.ID, err)
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

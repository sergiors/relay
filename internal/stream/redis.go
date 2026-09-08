package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Consumer reads events from a Redis stream and hands each decoded event to a
// handler. All Redis concerns live in this package.
type Consumer struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
	block    time.Duration
	count    int64
	log      *log.Logger
}

type ConsumerConfig struct {
	Client   *redis.Client
	Stream   string
	Group    string
	Consumer string
	// Block is the XREADGROUP BLOCK duration. Defaults to 5s if zero.
	Block time.Duration
	// Count is the XREADGROUP COUNT. Defaults to 10 if zero.
	Count int64
	Log   *log.Logger
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	if cfg.Block == 0 {
		cfg.Block = 5 * time.Second
	}
	if cfg.Count == 0 {
		cfg.Count = 10
	}
	if cfg.Log == nil {
		cfg.Log = log.Default()
	}
	return &Consumer{
		client:   cfg.Client,
		stream:   cfg.Stream,
		group:    cfg.Group,
		consumer: cfg.Consumer,
		block:    cfg.Block,
		count:    cfg.Count,
		log:      cfg.Log,
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
// event. A message is acknowledged (XACK) only after the handler returns nil;
// if the event cannot be decoded or the handler returns an error, the message
// stays pending. Consume blocks until ctx is cancelled.
func (c *Consumer) Consume(
	ctx context.Context,
	handler func(
		ctx context.Context,
		msgID string,
		event map[string]any,
	) error,
) error {
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
			return fmt.Errorf("read group from stream %q: %w", c.stream, err)
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				c.process(ctx, msg, handler)
			}
		}
	}
}

// process decodes and handles a single message, then acks it only on success.
func (c *Consumer) process(
	ctx context.Context,
	msg redis.XMessage,
	handler func(
		ctx context.Context,
		msgID string,
		event map[string]any,
	) error,
) {
	raw, ok := msg.Values["event"]
	if !ok {
		c.log.Printf("message %q: missing 'event' field; not acknowledging", msg.ID)
		return
	}

	rawStr, ok := raw.(string)
	if !ok {
		c.log.Printf("message %q: 'event' field is not a string; not acknowledging", msg.ID)
		return
	}

	var event map[string]any
	if err := json.Unmarshal([]byte(rawStr), &event); err != nil {
		c.log.Printf("message %q: decode event: %v; not acknowledging", msg.ID, err)
		return
	}

	if err := handler(ctx, msg.ID, event); err != nil {
		c.log.Printf("message %q: handler error: %v; not acknowledging", msg.ID, err)
		return
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Printf("message %q: ack: %v", msg.ID, err)
	}
}

// isBusyGroup reports whether the error indicates the group already exists.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

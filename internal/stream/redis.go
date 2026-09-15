package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/metrics"
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
)

// MaxRuleTimeout is the upper bound on any rule's handler timeout. It is the
// same value as function.MaxTimeout (kept in sync; function is a leaf package
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
	// ScheduleRunner, when set, executes messages identified as schedule
	// occurrences directly against the named function/handler, bypassing event
	// matching. The stream layer stays the same consumer-group/PEL/recovery
	// machinery for both message kinds. It is wired by the worker to
	// runner.InvokeHandler. A nil value means schedule messages are treated as
	// normal events (the safe fallback for tests that do not wire it).
	ScheduleRunner func(ctx context.Context, fnName, handler string, payload []byte) error
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
	// schedule-occurrence messages are treated as normal events.
	scheduleRunner func(ctx context.Context, fnName, handler string, payload []byte) error
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
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(os.Stderr, nil))
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
	c := &Consumer{
		client:          cfg.Client,
		stream:          cfg.Stream,
		group:           cfg.Group,
		consumer:        cfg.Consumer,
		block:           cfg.Block,
		count:           cfg.Count,
		reclaimInterval: cfg.ReclaimInterval,
		minPendingIdle:  cfg.MinPendingIdle,
		dlqStream:       DLQStreamFor(cfg.Stream),
		log:             cfg.Log,
		metrics:         cfg.Metrics,
		metricsInterval: cfg.MetricsInterval,
		backoff:         newBackoff(cfg.backoffTable, cfg.backoffJitter),
		capacity:        capacity,
		buffer:          newBufferSemaphore(capacity),
		scheduleRunner:  cfg.ScheduleRunner,
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
			c.log.Warn(fmt.Sprintf("Redis read failed: %v; retrying in %s", err, delay))
		}
		return
	}
	// Success (or redis.Nil): mark healthy and reset backoff on the transition.
	if !c.healthy.Swap(true) {
		c.log.Info("Redis connection recovered")
	}
	c.backoff.reset()
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
	c.metrics.SetGauge("buffered_events", float64(c.buffer.inflight()))
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
							c.log.Error(fmt.Sprintf("Message %q: PANIC outside processMessage: %v\n%s", m.ID, pv, debug.Stack()))
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
func (p *PendingGaugeSource) Refresh(ctx context.Context) {
	// The summary form (no Start/End/Count) is O(1)-ish and returns the total
	// Count plus the oldest pending message ID in Lower.
	pending, err := p.client.XPending(ctx, p.stream, p.group).Result()
	if err != nil {
		p.log.Debug(fmt.Sprintf("Metrics: xpending %q/%q: %v", p.stream, p.group, err))
		return
	}
	p.metrics.SetGauge("pending_entries", float64(pending.Count))
	if age, ok := pendingAge(pending.Lower); ok {
		p.metrics.SetGauge("pending_oldest_age_seconds", age.Seconds())
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
				c.log.Debug(fmt.Sprintf("Message %q: deleted from stream; acking to clear PEL", msg.ID))
				if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
					c.log.Warn(fmt.Sprintf("Message %q: ack deleted entry: %v", msg.ID, err))
				}
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
					c.log.Debug(fmt.Sprintf("Message %q: beyond reclaim window and pending lookup failed (%v); skipping this tick", msg.ID, err))
					continue
				}
				pe = entries[0]
			}
			c.log.Debug(fmt.Sprintf("Reclaimed message %q for consumer %q (idle %s, attempts %d)",
				msg.ID, c.consumer, pe.Idle, pe.RetryCount))
			c.deliverClaimed(ctx, msg, pe.RetryCount, handler)
		}
		if len(msgs) == 0 || next == "0-0" {
			break
		}
		start = next
	}
}

// deliverClaimed runs a reclaimed message through the shared path. Reclaim is
// message-ownership recovery only: it transfers an idle pending message to this
// consumer and replays it. Whether the message's invocations actually execute
// (and whether they are exhausted) is decided at run time from per-invocation
// state, so there is no message-level attempt pre-check here. The stream-level
// retries_total counter counts message redeliveries (actual re-delivery events,
// not executions): a reclaim is a retry event even when the invocation is
// skipped as protected, because a redelivery DID occur. This is distinct from
// the per-function function_retries_total (runner), which counts failed
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
	c.metrics.Inc("retries_total")
	if !c.buffer.tryAcquire() {
		// Buffer full: leave the message pending; a later reclaim tick retries
		// it. Never block the reclaim goroutine on the semaphore.
		c.log.Debug(fmt.Sprintf("Message %q: buffer full; deferring reclaimed delivery", msg.ID))
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
// Panic boundary: a recover is registered at the top of the function so a panic
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
			c.log.Error(fmt.Sprintf("Message %q: PANIC in handler: %v\n%s", msg.ID, pv, debug.Stack()))
		}
	}()
	event, err := classifyMessage(msg)
	if err != nil {
		// A malformed message can never succeed, so it goes straight to the DLQ on
		// first encounter rather than consuming retry cycles.
		c.log.Error("Message %q: non-retryable failure (%v); routing to DLQ",
			msg.ID, err,
			"message_id", msg.ID,
			"attempt", deliveryNum,
			"reason", err,
		)
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
	// invocations that already completed on a previous delivery or are protected
	// by an active attempt deadline. The handle is bound to this (stream, group,
	// msgID) and reads/writes the invocation-state hash in Redis.
	handlerCtx := WithDeliveryAttempt(ctx, deliveryNum)
	handlerCtx = WithInvocationState(handlerCtx,
		NewInvocationState(ctx, c.invStateStore, c.stream, c.group, msg.ID, c.log))

	// A schedule-occurrence message is recognized by its relay.schedule envelope
	// and, when a ScheduleRunner is wired, routed directly to it, bypassing event
	// matching entirely. A nil ScheduleRunner falls back to treating schedule
	// messages as normal events (the safe fallback for tests/unwired consumers).
	if occ, ok := schedule.IsScheduleEvent(event); ok && c.scheduleRunner != nil {
		c.processScheduleMessage(ctx, msg.ID, deliveryNum, occ)
		return
	}

	if err := handler(handlerCtx, msg.ID, event); err != nil {
		// If we are shutting down (ctx cancelled), this is not a real attempt: do
		// not count it nor DLQ the message — leave it pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Debug(fmt.Sprintf("Message %q: handler canceled during shutdown; leaving pending", msg.ID))
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
				"attempt", deliveryNum,
			)
			return
		}
		// A terminal message: every non-complete matched invocation is exhausted,
		// so the whole message is routed to the DLQ. This is the per-invocation
		// exhaustion path that replaces the old message-level max-attempts check.
		if errors.Is(err, ErrInvocationExhausted) {
			c.log.Error("Message: invocation(s) exhausted; routing to DLQ",
				"message_id", msg.ID,
				"attempt", deliveryNum,
				"reason", err,
			)
			c.routeToDLQ(ctx, msg, err, deliveryNum)
			return
		}
		c.log.Warn("Message: retryable failure; leaving pending for a later reclaim",
			"message_id", msg.ID,
			"attempt", deliveryNum,
			"reason", err,
		)
		// A retryable failure: this delivery will be retried, so it counts as a
		// retry event. The message stays pending for a later reclaim.
		return
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Warn(fmt.Sprintf("Message %q: ack: %v", msg.ID, err))
		c.noteOutcome(err, 0)
		return
	}
	// The message is fully processed and acknowledged: eagerly clear its
	// invocation-state hash. Ordering matters — clear only AFTER a successful
	// ACK. If the ACK failed (handled above) the message stays in the PEL and
	// may be redelivered, so its state must remain for the redelivery to skip
	// completed handlers. A clear failure is logged only; the TTL is the
	// fallback cleanup.
	if err := c.invStateStore.clear(ctx, c.stream, c.group, msg.ID); err != nil {
		c.log.Warn(fmt.Sprintf("Message %q: clear invocation state: %v", msg.ID, err))
	}
}

// processScheduleMessage routes a schedule-occurrence message directly to the
// ScheduleRunner (which resolves the function's current timeout from the
// registry), bypassing event matching entirely. It shares the exact delivery
// contract of processMessage: events_processed_total counts the delivery
// attempt, invocation state protects redeliveries (complete/running/backoff/
// exhausted), and the message is ACKed on success, left pending on a retryable
// failure or protected skip, and routed to the DLQ on exhaustion.
func (c *Consumer) processScheduleMessage(ctx context.Context, msgID string, deliveryNum int64, occ schedule.Occurrence) {
	// Schedules ARE stream deliveries now, so this is the same delivery-attempt
	// counter as processMessage: it keeps the global events_processed_total
	// semantics uniform across message kinds (a redelivered schedule increments
	// it again, like any other delivery attempt).
	c.metrics.Inc("events_processed_total")

	c.log.Debug("Schedule: executing occurrence",
		"function", occ.Function,
		"handler", occ.Handler,
		"occurrence_id", occ.ID(),
		"scheduled_at", occ.ScheduledAt.UTC().Format(time.RFC3339),
		"message_id", msgID,
		"attempt", deliveryNum,
	)

	// Inject the same per-message context as processMessage: the delivery-attempt
	// number and the invocation-state handle bound to this (stream, group, msgID),
	// so a redelivery skips a completed schedule invocation. The ScheduleRunner
	// (runner.InvokeHandler) itself carries no invocation state (it has no
	// matching pre-pass), but the invocation store still protects this message's
	// redelivery via the same hash keying.
	handlerCtx := WithDeliveryAttempt(ctx, deliveryNum)
	handlerCtx = WithInvocationState(handlerCtx,
		NewInvocationState(ctx, c.invStateStore, c.stream, c.group, msgID, c.log))

	err := c.scheduleRunner(handlerCtx, occ.Function, occ.Handler, occ.Payload())
	if err != nil {
		// Shutting down: not a real attempt; leave pending for a live consumer.
		if ctx.Err() != nil {
			c.log.Debug(fmt.Sprintf("Schedule: message %q canceled during shutdown; leaving pending", msgID))
			return
		}
		// A protected invocation (running on another replica, or waiting out its
		// retry backoff) means the message stays pending, not a failed attempt:
		// no retry accounting, no DLQ.
		if errors.Is(err, ErrInvocationNotEligible) {
			c.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
				"message_id", msgID,
				"attempt", deliveryNum,
			)
			return
		}
		// A terminal message: the schedule invocation is exhausted, so the message
		// routes to the DLQ.
		if errors.Is(err, ErrInvocationExhausted) {
			c.log.Error("Schedule: invocation exhausted; routing to DLQ",
				"message_id", msgID,
				"attempt", deliveryNum,
				"reason", err,
			)
			// Rebuild the message with its envelope so the DLQ entry carries the
			// schedule body (mirroring a normal event's `event` field).
			if envelope, eerr := occ.Envelope(); eerr == nil {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID, Values: map[string]any{"event": string(envelope)}}, err, deliveryNum)
			} else {
				c.routeToDLQ(ctx, redis.XMessage{ID: msgID}, err, deliveryNum)
			}
			return
		}
		// A retryable failure: leave pending for a later reclaim.
		c.log.Warn("Schedule: retryable failure; leaving pending for a later reclaim",
			"message_id", msgID,
			"attempt", deliveryNum,
			"reason", err,
		)
		return
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msgID).Err(); err != nil {
		c.log.Warn(fmt.Sprintf("Schedule: message %q: ack: %v", msgID, err))
		c.noteOutcome(err, 0)
		return
	}
	// Eagerly clear the invocation-state hash after a successful ACK, exactly
	// like processMessage. A clear failure is logged only; the TTL is the fallback.
	if err := c.invStateStore.clear(ctx, c.stream, c.group, msgID); err != nil {
		c.log.Warn(fmt.Sprintf("Schedule: message %q: clear invocation state: %v", msgID, err))
	}
}

// routeToDLQ writes the message to the DLQ and only then acks the original. The// XADD-before-XACK ordering matters: if the DLQ write fails the original stays
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
		c.log.Error("Message: DLQ write failed (leaving pending)",
			"message_id", msg.ID,
			"attempt", attempts,
			"reason", err,
		)
		c.noteOutcome(err, 0)
		return
	}
	c.metrics.Inc("dlq_entries_total")
	c.log.Error("Message: routed to DLQ",
		"message_id", msg.ID,
		"dlq_stream", c.dlqStream,
		"attempt", attempts,
		"reason", reason,
	)
	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.log.Warn(fmt.Sprintf("Message %q: ack after DLQ: %v", msg.ID, err))
		c.noteOutcome(err, 0)
		return
	}
	// The message is dead-lettered and the original acked: eagerly clear its
	// invocation-state hash. Ordering matters — clear only after BOTH the DLQ
	// write and the ACK succeed. If the ACK failed (handled above) the message
	// stays in the PEL and may be redelivered, so its state must remain. A clear
	// failure is logged only; the TTL is the fallback cleanup.
	if err := c.invStateStore.clear(ctx, c.stream, c.group, msg.ID); err != nil {
		c.log.Warn(fmt.Sprintf("Message %q: clear invocation state after DLQ: %v", msg.ID, err))
	}
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

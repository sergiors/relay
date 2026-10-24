package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
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
	// extraGaugeSources are additional GaugeSources the metrics refresher drives
	// alongside the consumer's own stream sampler (see AddGaugeSource). They are
	// appended once at startup and read by Consume on its single Refresher
	// goroutine, so a worker-global source (e.g. the scheduler's outbox gauges)
	// needs no second ticker. Never mutated after Consume starts.
	extraGaugeSources []metrics.GaugeSource
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
		buffer: newBufferSemaphore(capacity, func(occupancy int) {
			cfg.Metrics.SetGauge(metrics.MetricBufferedEvents, float64(occupancy))
		}),
		scheduleRunner:      cfg.ScheduleRunner,
		maxEventBytes:       maxEventBytes,
		invocationRetention: invocationRetention,
	}
	// The consumer is always constructed with a functional invocation-state
	// store. processMessage/processScheduleMessage/routeToDLQ rely on it being
	// present (no nil guard), so the seam must be injected here.
	c.invStateStore = store
	c.healthy.Store(true)
	c.metrics.SetGauge(metrics.MetricBufferedEvents, 0)
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

// bufferSemaphore is a counting semaphore that bounds the number
// of locally buffered events (messages read from Redis but not yet finished).
// It also tracks occupancy and publishes each transition under the occupancy
// lock, so concurrent acquire/release callbacks cannot publish stale snapshots.
type bufferSemaphore struct {
	capacity int
	mu       sync.Mutex
	count    int // current in-flight occupancy
	changed  chan struct{}
	onChange func(int)
}

// newBufferSemaphore builds a semaphore of the given capacity. capacity must be
// >= 1 (callers pass a validated MaxBufferedEvents).
func newBufferSemaphore(capacity int, onChange func(int)) *bufferSemaphore {
	return &bufferSemaphore{
		capacity: capacity,
		changed:  make(chan struct{}),
		onChange: onChange,
	}
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
	for {
		if ctx.Err() != nil {
			return false
		}
		s.mu.Lock()
		if s.count < s.capacity {
			s.count++
			if s.onChange != nil {
				s.onChange(s.count)
			}
			s.notifyLocked()
			s.mu.Unlock()
			return true
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// tryAcquire acquires a slot without blocking. It returns false when no slot is
// immediately free. Used by the reclaim path, which must never block (it skips
// a message for capacity and retries it next tick instead).
func (s *bufferSemaphore) tryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count >= s.capacity {
		return false
	}
	s.count++
	if s.onChange != nil {
		s.onChange(s.count)
	}
	s.notifyLocked()
	return true
}

// release frees a slot, waking an acquire that is waiting. It must be called
// exactly once for every successful acquire/tryAcquire.
func (s *bufferSemaphore) release() {
	s.mu.Lock()
	s.count--
	if s.onChange != nil {
		s.onChange(s.count)
	}
	s.notifyLocked()
	s.mu.Unlock()
}

// waitForFree waits until at least one slot is available without reserving it.
// The consumer rechecks capacity before reading, while avoiding a synthetic
// semaphore acquisition that would temporarily count as a buffered message.
func (s *bufferSemaphore) waitForFree(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		s.mu.Lock()
		if s.count < s.capacity {
			s.mu.Unlock()
			return true
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// notifyLocked wakes waiters after occupancy changes. s.mu must be held.
func (s *bufferSemaphore) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
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

	// The metrics refresher drives the stream-metrics GaugeSource (pending
	// backlog, group lag, DLQ depth) plus any extra sources registered via
	// AddGaugeSource (e.g. the scheduler's outbox gauges) on the single
	// configured interval, in its own goroutine. It never affects health,
	// backoff, or processing, and is stopped and joined before Consume returns.
	if c.metrics != nil {
		sctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			metrics.NewRefresher(c.metricsInterval, c.gaugeSources()...).Start(sctx)
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
			if !c.buffer.waitForFree(ctx) {
				// ctx done; nothing was handed out. Drain in-flight goroutines
				// before returning (see below).
				break
			}
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
					// A message read fresh from XREADGROUP is on its first delivery.
					c.processMessage(ctx, m, 1, handler)
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

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

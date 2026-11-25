package stream

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
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
	// DefaultMaxEventBytes is the byte cap applied to a message's raw `value`
	// when ConsumerConfig.MaxEventBytes is not set (a direct test
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
	// MaxEventBytes is the maximum byte length of a message's raw `value`
	// field. A delivered message whose raw value exceeds it is
	// rejected BEFORE JSON decode, envelope classification, event matching,
	// invocation-state migration, and handler execution, then routed to the DLQ
	// with a bounded diagnostic summary (never the oversized payload). It
	// bounds the raw value only; the Redis entry and go-redis response
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
	// DLQStore is the durable dead-letter store. It is REQUIRED for exhaustion
	// or malformed-message routing: the stream writes every required entry before
	// acknowledging the source, so a nil store leaves the source pending rather
	// than ACKing a delivery whose dead letters were never persisted. The worker
	// wires the PostgreSQL state handle; direct callers and tests may supply a
	// fake. The store is broker-neutral and never touches invocation state.
	DLQStore dlq.Store
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
	// by the worker to runner.InvokeOccurrence. A nil value leaves a schedule
	// message pending (never ACKed and never matched as an ordinary event); only
	// the worker wires the runner. A structurally INVALID schedule claim is
	// dead-lettered regardless.
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
	dlqStore        dlq.Store
	log             *slog.Logger
	metrics         *metrics.Registry
	metricsInterval time.Duration
	backoff         *backoff
	invStateStore   invocationStateStore
	healthy         atomic.Bool
	// groupGen advances on each local consumer-group recreation after NOGROUP.
	// A delivery captures it before running so a completion that crosses a
	// recreation cannot acknowledge a replayed source entry.
	groupGen atomic.Uint64
	// recovering is true from a NOGROUP observation until normal consumer
	// consumption (a successful XREADGROUP) resumes. Healthy consults it so
	// readiness stays unhealthy through the repair: a successful reclaim or any
	// other Redis command must not mask a still-missing group.
	recovering atomic.Bool
	// recoveryEpoch advances when a NOGROUP recovery begins. A read captures it
	// before the command, so a success from a read issued before the loss cannot
	// clear the recovering state.
	recoveryEpoch atomic.Uint64
	// recoveryMu serializes group recreation and guards recoveryBackoff and
	// recoveryStreak. Concurrent NOGROUP observers coalesce on one attempt via
	// groupGen.
	recoveryMu      sync.Mutex
	recoveryBackoff *backoff
	recoveryStreak  int
	// buffer is the bounded local-event semaphore: it caps the number of
	// messages read from Redis and held locally before completion, so the
	// consumption loop applies backpressure instead of unboundedly buffering.
	// capacity is the configured MaxBufferedEvents limit.
	buffer   *bufferSemaphore
	capacity int
	// scheduleRunner is the ScheduleRunner seam (see ConsumerConfig). When nil,
	// a schedule message is left pending rather than falling through to event
	// matching; only the worker wires the runner.
	scheduleRunner func(ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte) error
	// maxEventBytes caps the raw `value` length (see
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
	return newConsumer(cfg, &invocationStore{client: cfg.Client, consumer: cfg.Consumer})
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
		dlqStore:        cfg.DLQStore,
		log:             cfg.Log,
		metrics:         cfg.Metrics,
		metricsInterval: cfg.MetricsInterval,
		backoff:         newBackoff(cfg.backoffTable, cfg.backoffJitter),
		recoveryBackoff: newBackoff(cfg.backoffTable, cfg.backoffJitter),
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

// GroupCreation reports what an ensure-group call observed. It lets the
// consumer-group recovery metric distinguish a group this process actually
// created from one another actor had already created (BUSYGROUP).
type GroupCreation int

const (
	// GroupCreated means XGROUP CREATE succeeded: the group did not exist.
	GroupCreated GroupCreation = iota
	// GroupAlreadyExists means Redis answered BUSYGROUP: the group was already
	// present, so nothing was created.
	GroupAlreadyExists
)

// EnsureGroup creates the consumer group if it does not exist, tolerating a
// group that already exists (BUSYGROUP). MKSTREAM creates the stream if
// needed; the group starts at "0", so its consumers see every entry already in
// the stream as well as new ones (not just new messages); a fresh group over
// an existing stream therefore replays the backlog. Group creation and its
// opaque fencing-token update are atomic; BUSYGROUP preserves the current token.
//
// It delegates to ensureGroup, the single authoritative implementation, and
// preserves the error-only bootstrap contract used by the worker's
// external-dependency preflight and by embedders. The worker runs it as a
// startup prerequisite before any app is loaded or fingerprinted and before
// the state DB and runtime workload initialization (the Redis client and
// tracing already exist; the Docker manager is opened afterwards, see
// internal/worker); an embedder invokes it before Consume.
func EnsureGroup(ctx context.Context, client groupCreator, stream, group string) error {
	_, _, err := ensureGroup(ctx, client, stream, group)
	return err
}

// EnsureGroup creates the consumer group required for consumption. It delegates
// to the package-level EnsureGroup, binding this consumer's client, stream, and
// group, so the BUSYGROUP/MKSTREAM-at-0 semantics are not duplicated.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	return EnsureGroup(ctx, c.client, c.stream, c.group)
}

// ensureGroup recreates this consumer's group during NOGROUP recovery and
// reports whether Relay created it.
func (c *Consumer) recoverGroup(ctx context.Context) (GroupCreation, error) {
	outcome, _, err := ensureGroup(ctx, c.client, c.stream, c.group)
	return outcome, err
}

// The production Redis client must satisfy the group-bootstrap seam.
var _ groupCreator = (*redis.Client)(nil)

// Healthy reports whether the consumer is ready to consume: its last observed
// Redis operation succeeded and no consumer-group recovery is outstanding. It
// is an in-process readiness accessor for embedders and orchestrators that run
// the consumer themselves; the worker's own `relay health` probe consults it in
// steady state. `relay health` itself never touches Redis: it asks the running
// worker over its control socket. Prefer ReadinessReason when a fixed,
// low-cardinality reason is needed.
func (c *Consumer) Healthy() bool {
	return c.healthy.Load() && !c.recovering.Load()
}

// ReadinessReason reports consumer readiness together with a fixed, bounded
// reason suitable for a readiness response. A group recovery in progress is
// reported distinctly from a generic Redis outage so an operator can tell that
// the stream group is being repaired rather than that Redis is unreachable.
// Like Healthy, it never blocks on Redis.
func (c *Consumer) ReadinessReason() (bool, string) {
	if c.recovering.Load() {
		return false, "redis consumer group recovery pending"
	}
	if !c.healthy.Load() {
		return false, "redis consumer unhealthy"
	}
	return true, ""
}

// noteOutcome feeds a single Redis operation result into the health state and
// logs only on state transitions. On failure it marks the consumer unhealthy
// (logging once on the healthy→unhealthy transition); on success it marks it
// healthy (logging once on the unhealthy→healthy transition) and resets the
// backoff. redis.Nil counts as success because connectivity is fine. delay is
// the retry delay the caller is about to wait, used only in the failure log.
//
// It deliberately does NOT clear a pending group recovery: any Redis operation
// can succeed while the consumer group is still missing (a reclaim tick, a
// metrics sample, or a read issued before the loss was observed). Only a
// successful normal XREADGROUP issued after the loss clears that state (see
// noteConsumeSuccess), so a concurrent stale success cannot mask a NOGROUP.
func (c *Consumer) noteOutcome(err error, delay time.Duration) {
	if err != nil && !errors.Is(err, redis.Nil) {
		if c.healthy.Swap(false) {
			c.log.Warn("Redis: read failed; retrying", "error", err, "delay", delay)
		}
		return
	}
	if !c.healthy.Swap(true) {
		c.log.Info("Redis connection recovered")
	}
	c.backoff.reset()
}

// noteConsumeSuccess records one successful normal XREADGROUP (a read or its
// blocking timeout). gen and epoch are the local group generation and recovery
// epoch captured immediately before the command was issued.
//
// A success clears the recovering state only when the command was issued under
// the CURRENT generation and epoch: a read that started before a NOGROUP was
// observed (or before a recreation completed) is stale and cannot prove the
// missing group is back. The check is repeated under recoveryMu, which also
// serializes it against recoverConsumerGroup, so a recovery that advances the
// generation while this call is waiting cannot be masked. Only this confirmed
// normal-consumption success resets the recovery pacing: reclaim and metrics
// successes never do.
func (c *Consumer) noteConsumeSuccess(gen, epoch uint64) {
	if !c.healthy.Swap(true) {
		c.log.Info("Redis connection recovered")
	}
	c.backoff.reset()

	if !c.recovering.Load() {
		return
	}
	if gen != c.groupGeneration() || epoch != c.recoveryEpoch.Load() {
		return
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	if !c.recovering.Load() ||
		gen != c.groupGeneration() ||
		epoch != c.recoveryEpoch.Load() {
		return
	}
	c.recovering.Store(false)
	c.recoveryStreak = 0
	c.recoveryBackoff.reset()
	c.log.Info("Redis: consumer group recovery confirmed; consumption resumed",
		"stream", c.stream,
		"group", c.group)
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

		// Capture the generation and recovery epoch BEFORE issuing the read: the
		// result belongs to the group state that existed when the command was
		// issued, so it can neither confirm a recovery nor ACK a delivery after a
		// concurrent recreation.
		gen := c.groupGeneration()
		epoch := c.recoveryEpoch.Load()
		groupToken, tokenErr := readGroupGeneration(ctx, c.client, c.stream, c.group)
		if tokenErr != nil {
			if errors.Is(tokenErr, context.Canceled) || errors.Is(tokenErr, context.DeadlineExceeded) {
				break
			}
			if handled, wait := c.handleGroupLoss(ctx, metrics.RedisOpReadGroup, gen, tokenErr); handled {
				if !waitContext(ctx, wait) {
					break
				}
				continue
			}
			recordRedisReadError(c.metrics, metrics.RedisOpReadGroup, tokenErr)
			delay := c.backoff.next()
			c.noteOutcome(tokenErr, delay)
			if !waitContext(ctx, delay) {
				break
			}
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
			// Only NOGROUP triggers recreation; every other failure keeps the
			// existing health/backoff behavior below.
			if handled, wait := c.handleGroupLoss(ctx, metrics.RedisOpReadGroup, gen, err); handled {
				if !waitContext(ctx, wait) {
					break
				}
				continue
			}
			recordRedisReadError(c.metrics, metrics.RedisOpReadGroup, err)
			if errors.Is(err, redis.Nil) {
				// Block timed out with no messages; the command reached the server
				// and the group exists, so normal consumption has resumed.
				c.noteConsumeSuccess(gen, epoch)
				continue
			}
			// A transient Redis failure (restart, flaky network) should not kill the
			// process; go-redis re-establishes connections. Back off with jitter so
			// replicas do not retry in lockstep, and wait context-aware so shutdown
			// interrupts a pending retry promptly.
			delay := c.backoff.next()
			c.noteOutcome(err, delay)
			if !waitContext(ctx, delay) {
				break
			}
			continue
		}

		c.noteConsumeSuccess(gen, epoch)
		// Bind the generation captured before this read to every message in the
		// batch, so a completion that crosses a later group recreation cannot
		// ACK an entry the recreated group may not have delivered yet.
		deliveryCtx := withDeliveryGroupGeneration(withDeliveryGeneration(ctx, gen), groupToken)
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
					c.processMessage(deliveryCtx, m, 1, handler)
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

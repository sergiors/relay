package stream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
)

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
// with a single conservative atomic ack-and-retain so the recovery loop cannot
// spin on it forever.
//
// ack is false for ids the server already purged from the PEL during the scan:
// acking them is a no-op at best and misleading at worst, so it is skipped.
//
// deliveries is the Redis PEL delivery count when known
// (missingPayloadUnknownDeliveries otherwise), reported for diagnosis only; it
// is never converted into a handler attempt.
//
// Invocation state: once the dangling PEL reference is gone (ACKed here, or
// already purged by the server during the scan) the message is no longer
// recoverable, so its state hash is switched to terminal retention. Without
// that, a persistent hash written for a body-less invocation would leak forever.
// When ack is true the ACK and the retention happen in ONE atomic script
// (ackAndRetainTerminal): a failed ACK leaves the marker PERSISTENT (no TTL), so
// the entry stays pending/recoverable and nothing expires — a leak is safer than
// dropping state a redelivery might still need. When ack is false there is no ACK
// to run, so the server-purged reference is retained via the non-XACK
// retainTerminal path.
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
		// Acknowledge the dangling PEL reference AND switch the invocation state
		// to terminal retention in ONE atomic script: the ACK and the
		// retention can no longer be split by a crash or error, and a failed ACK
		// can never leave an expiring TTL on still-pending state (the script
		// stages only a persistent marker before the XACK). On failure nothing is
		// guaranteed, so the entry is left pending/recoverable for a later pass.
		if _, err := c.invStateStore.ackAndRetainTerminal(ctx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
			c.log.Warn("Message: ack missing-value entry failed", "message_id", msg.ID, "error", err)
			return
		}
		return
	}
	// The server already purged the dangling PEL reference during the scan, so
	// there is no XACK to run. Switch the (never-completing) invocation state to
	// terminal retention under the retention TTL instead of leaking it: this is
	// the non-XACK retention path.
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

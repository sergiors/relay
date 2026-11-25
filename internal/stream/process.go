package stream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

	"relay/internal/dlq"
	"relay/internal/envelope"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/schedule"
)

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
	// tracestate/baggage, written beside the `value` by the schedule publisher
	// or any trace-aware producer) is extracted FIRST, so the
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

	// The group generation this delivery belongs to is captured by the read or
	// reclaim that produced the message, BEFORE concurrent group recreation. A
	// direct caller (a test or an embedder invoking processMessage itself) has
	// no generation in its context, so it uses the current one: there is no
	// earlier read to fence against.
	deliveryGen, captured := deliveryGeneration(ctx)
	if !captured {
		deliveryGen = c.groupGeneration()
	}

	// Register the panic boundary before any handler work so a panic in the
	// handler handoff (or in extractAndDecodeEnvelope, which is pure JSON
	// parsing and practically cannot panic) is caught. The delivery is treated
	// as failed:
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
	// it must NOT fall through to extractAndDecodeEnvelope and be dead-lettered
	// as a malformed message. The reclaim path clears these before this point;
	// this guard keeps any other caller (or a future one) from silently treating
	// it as successful processing.
	if msg.Values == nil {
		outcome = "missing"
		spanErr = fmt.Errorf("message body no longer exists")
		// The message reached the processing path, so it is still in the PEL:
		// clear it (the reclaim path normally handles these before here).
		c.clearMissingValueEntry(ctx, msg, missingPayloadUnknownDeliveries, true)
		return
	}
	decoded, err := extractAndDecodeEnvelope(msg, c.maxEventBytes)
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
			c.routeToDLQ(ctx, msg, err, deliveryNum, deliveryGen, nil)
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
		c.routeToDLQ(ctx, msg, err, deliveryNum, deliveryGen, nil)
		return
	}

	// Envelope decoding has already separated the three message kinds. A
	// confirmed tombstone is acknowledged without execution or event matching.
	// A schedule occurrence is dispatched directly and can never fall through to
	// ordinary matching, even when no ScheduleRunner is wired.
	switch decoded.Kind {
	case envelope.KindTombstone:
		c.log.Debug("Message: confirmed tombstone; acknowledging",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
		)
		result, err := c.acknowledgeMessage(ctx, msg.ID, deliveryGen)
		if err != nil {
			c.handleAckFailure(ctx, deliveryGen, msg.ID, err)
			if !errors.Is(err, errDeliverySuperseded) {
				outcome, spanErr = "pending", err
			}
			return
		}
		if !result.Settled {
			c.log.Debug("Message: tombstone delivery not settled; leaving pending",
				"message_id", msg.ID,
				"disposition", result.Disposition)
			return
		}
		outcome = "acked"
		return
	case envelope.KindSchedule:
		if c.scheduleRunner == nil {
			err := fmt.Errorf(
				"schedule message received but ScheduleRunner is not configured")
			c.log.Error(
				"Message: schedule dispatch is not configured; leaving pending",
				"message_id", msg.ID,
				"delivery_attempt", deliveryNum,
			)
			outcome, spanErr = "pending", err
			return
		}
		outcome, spanErr = c.processScheduleMessage(
			ctx, msg.ID, deliveryNum, deliveryGen, decoded.Occurrence, valueString(msg))
		return
	}
	event := decoded.Event

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
			c.routeToDLQ(ctx, msg, err, deliveryNum, deliveryGen, decoded.Payload)
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
	result, err := c.ackDelivery(ackCtx, msg.ID, deliveryGen, false)
	if err != nil {
		if !errors.Is(err, errDeliverySuperseded) {
			ackSpan.RecordError(err)
			ackSpan.SetStatus(codes.Error, err.Error())
		}
		ackSpan.End()
		c.handleAckFailure(ctx, deliveryGen, msg.ID, err)
		return
	}
	ackSpan.End()
	if !result.Settled {
		// The delivery is not ours to retire: the group was recreated while the
		// handler ran, a foreign consumer owns it, or the entry is unknown. Leave
		// the state persistent; a later delivery resolves it.
		c.log.Debug("Message: source delivery not settled; leaving pending",
			"message_id", msg.ID,
			"disposition", result.Disposition)
		return
	}
	outcome = "acked"
	// The message is fully processed and acknowledged, and its invocation state
	// was switched to terminal retention in the SAME atomic script, so
	// there is no post-ACK orphan window. If the ACK failed (handled above) the
	// message stays in the PEL; the script's marker staging is persistent (no
	// TTL), so a redelivery still observes completed handlers and the state never
	// expires. A retention/ack failure is a single error: logged here, never
	// fatal.
}

// acknowledgeMessage performs the single atomic source ACK and terminal
// invocation-state retention for a message that needs no handler dispatch, such
// as a confirmed tombstone. It applies the same group-generation fence as every
// other ACK path.
func (c *Consumer) acknowledgeMessage(ctx context.Context, msgID string, deliveryGen uint64) (terminalAckResult, error) {
	ackCtx, ackSpan := redisSpan(ctx, "redis.ack")
	result, err := c.ackDelivery(ackCtx, msgID, deliveryGen, false)
	if err != nil && !errors.Is(err, errDeliverySuperseded) {
		ackSpan.RecordError(err)
		ackSpan.SetStatus(codes.Error, err.Error())
	}
	ackSpan.End()
	return result, err
}

// deliveryGenerationKey carries the consumer-group generation that produced a
// delivery from the read/reclaim site into processMessage. It is bound per
// batch (one shared value) rather than per message, and is never mutated.
type deliveryGenerationKey struct{}
type deliveryGroupGenerationKey struct{}

// withDeliveryGeneration binds gen to ctx for the messages this read produced.
func withDeliveryGeneration(ctx context.Context, gen uint64) context.Context {
	return context.WithValue(ctx, deliveryGenerationKey{}, gen)
}

// deliveryGeneration returns the generation bound by the read or reclaim that
// produced the delivery.
func deliveryGeneration(ctx context.Context) (uint64, bool) {
	gen, ok := ctx.Value(deliveryGenerationKey{}).(uint64)
	return gen, ok
}

func withDeliveryGroupGeneration(ctx context.Context, generation string) context.Context {
	return context.WithValue(ctx, deliveryGroupGenerationKey{}, generation)
}

func deliveryGroupGeneration(ctx context.Context) (string, bool) {
	generation, ok := ctx.Value(deliveryGroupGenerationKey{}).(string)
	return generation, ok && generation != ""
}

// errDeliverySuperseded reports that a delivery began under an older local
// consumer-group generation. It is not a Redis failure: the completion is stale
// because this process recreated the group while the handler ran, so the source
// entry may be unread in the recreated group and must not be acknowledged.
var errDeliverySuperseded = errors.New("stream: delivery superseded by consumer-group recreation")

// ackDelivery acknowledges a source delivery only when it still belongs to the
// local and shared group generations captured before the delivery started.
//
// The local fence handles recreations this process observes. The shared token
// handles cross-process recreation: both an old and replayed delivery may have
// the same PEL owner, so only the token checked atomically with terminal-state
// writes and XACK can distinguish their group generations.
// Consume and reclaim bind that token before reading. A direct contextless
// caller is passed through to the store, which resolves the current token.
func (c *Consumer) ackDelivery(
	ctx context.Context,
	msgID string,
	deliveryGen uint64,
	trackReceipt bool,
) (terminalAckResult, error) {
	if deliveryGen != c.groupGeneration() {
		return terminalAckResult{}, errDeliverySuperseded
	}
	return c.invStateStore.ackAndRetainTerminal(
		ctx, c.stream, c.group, msgID, c.invocationRetention, trackReceipt)
}

// handleAckFailure feeds an ACK-script failure into health and, for a NOGROUP
// error, runs the shared group-recovery path so the group is repaired. The
// message is left pending either way: an ambiguous or missing group is never
// proof that the source delivery resolved. A local generation supersession is
// not a Redis failure and is logged only at debug.
func (c *Consumer) handleAckFailure(
	ctx context.Context,
	deliveryGen uint64,
	msgID string,
	err error,
) {
	if errors.Is(err, errDeliverySuperseded) {
		c.log.Debug("Message: delivery superseded by consumer-group recreation; leaving pending",
			"message_id", msgID)
		return
	}
	c.log.Warn("Message: ack failed", "message_id", msgID, "error", err)
	if isNoGroup(err) {
		wait, _ := c.recoverConsumerGroup(ctx, deliveryGen)
		c.noteOutcome(err, wait)
		return
	}
	c.noteOutcome(err, 0)
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
// span, so the schedule path is traced identically to the event path. It owns
// its own invocation-state migration and handler context, so a direct caller
// stays self-contained; processMessage dispatches it before its generic event
// setup. rawValue is the original transport `value`, preserved into a
// dead-lettered entry's diagnostic body; a direct caller may pass "" to have the
// occurrence envelope reconstructed.
//
// Schedule occurrences are deliberately NOT counted in the event-classification
// counters (received/matched/unmatched): they bypass event matching, so they
// have no meaningful matched/unmatched class and would otherwise break the
// partition invariant. Schedule activity is accounted by the schedule
// publication counters and the per-app handler counters.
func (c *Consumer) processScheduleMessage(ctx context.Context, msgID string, deliveryNum int64, deliveryGen uint64, occ schedule.Occurrence, rawValue string) (string, error) {
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
	// The schedule runner participates in the SAME TryStart / complete / failure /
	// exhaustion lifecycle as event handlers via this handle. A completed schedule
	// invocation is skipped and ACKed, a protected (running or backoff) one stays
	// pending, and an exhausted one routes to the DLQ.
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
			// Acknowledge AND switch the invocation state to terminal retention in
			// one atomic script, exactly like the success tail. An ACK
			// failure means the message stays pending (the script's staged marker is
			// persistent, so a redelivery still resolves it and nothing expires).
			result, ackErr := c.ackDelivery(ctx, msgID, deliveryGen, false)
			if ackErr != nil {
				c.handleAckFailure(ctx, deliveryGen, msgID, ackErr)
				if errors.Is(ackErr, errDeliverySuperseded) {
					return "pending", nil
				}
				return "pending", ackErr
			}
			if !result.Settled {
				c.log.Debug("Schedule: delivery not settled; leaving pending",
					"message_id", msgID,
					"disposition", result.Disposition)
				return "pending", nil
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
			// A semantically invalid claim has no handler payload that ran, so
			// the entry is non-replayable (nil replay payload).
			c.routeToDLQ(ctx,
				scheduleDLQMessage(msgID, occ, rawValue), err, deliveryNum, deliveryGen, nil)
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
			// The handler payload the schedule path actually handed the runner
			// is Occurrence.Payload(), never the coordination envelope.
			c.routeToDLQ(ctx,
				scheduleDLQMessage(msgID, occ, rawValue), err, deliveryNum, deliveryGen, occ.Payload())
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

	result, err := c.ackDelivery(ctx, msgID, deliveryGen, false)
	if err != nil {
		c.handleAckFailure(ctx, deliveryGen, msgID, err)
		if errors.Is(err, errDeliverySuperseded) {
			return "pending", nil
		}
		return "pending", err
	}
	if !result.Settled {
		c.log.Debug("Schedule: delivery not settled; leaving pending",
			"message_id", msgID,
			"disposition", result.Disposition)
		return "pending", nil
	}
	// The ACK and the terminal-retention switch happened in the SAME atomic
	// script, exactly like processMessage, so no post-ACK orphan window
	// exists.
	return "acked", nil
}

// scheduleDLQMessage rebuilds the synthetic stream message routeToDLQ records
// for a failed schedule occurrence. It mirrors the transport shape the publisher
// writes: `value` preserves the original raw envelope, so the DLQ `event` stays
// the dead-lettered body. When the caller has no original value (a direct
// internal call), the occurrence envelope is reconstructed; if even that fails,
// only the source message ID is retained and the DLQ `event` falls back to the
// malformed placeholder.
func scheduleDLQMessage(
	msgID string,
	occ schedule.Occurrence,
	rawValue string,
) redis.XMessage {
	value := rawValue
	if value == "" {
		if envelope, err := occ.Envelope(); err == nil {
			value = string(envelope)
		}
	}
	msg := redis.XMessage{ID: msgID}
	if value != "" {
		msg.Values = map[string]any{"value": value}
	}
	return msg
}

// dlqTraceFor reads the compact trace lineage persisted for one exhausted
// invocation, to be written into its DLQ entry. It consults the invocation-state
// hash (the "last failed attempt" reference), which the runner wrote on the
// final attempt and which survives until the state hash is retained/expired
// after the source ACK. It is best-effort: a placeholder invocation (empty ID)
// or a read failure yields "" (an omitted field), never a failed DLQ write. Only
// the compact traceparent[|tracestate] form is stored, so baggage can never
// reach the DLQ.
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

// routeToDLQ persists one dead-letter entry per exhausted invocation, then
// acknowledges the source delivery. PostgreSQL is the sole DLQ persistence: each
// row is written idempotently by its stable ID BEFORE the source ACK, so a crash
// or an ambiguous ACK can never lose a dead letter.
//
// Claim validation precedes every real-invocation write: the exhausted marker is
// read from Redis and its retained attempt must match the one the runner
// attributed, with a well-formed retained claim token. Exhausted markers are
// terminal and immutable, and each attempt has exactly one claim token, so a
// validated identity cannot be superseded; a missing or mismatched marker
// refuses the write and leaves the source pending rather than creating a row on
// a stale claim's behalf. The placeholder path has no invocation and therefore
// no claim to validate.
//
// Per-invocation entries: reason is the runner's terminal *HandlerExhaustedError
// carrying the exact app/handler and exhausted attempt for every terminal
// invocation, so a message matching several apps or handlers produces one
// precisely-attributed entry each (see dlqEntrySpecs). A reason without that
// typed metadata (a malformed message routed pre-handler) produces a single
// placeholder entry with an explicit handler_attempts of 0, never one invented
// from the delivery count.
//
// Idempotent retry: the stable ID combines the logical source delivery identity
// with the invocation identity, so a redelivery after a failed ACK, a crash, or
// a partially-written multi-entry DLQ inserts only the missing rows. PostgreSQL's
// ON CONFLICT DO NOTHING is the correctness boundary, not a Redis persistence
// marker.
//
// deliveryAttempts is the authoritative source delivery count passed through the
// consumer/reclaim flow (the DLQ `deliveries` field, diagnostic only).
//
// replayPayload is the exact logical handler payload produced by the envelope
// decoder (raw JSON, normalized Debezium event, or Occurrence.Payload for a
// schedule). It is stored separately from the diagnostic `event` body so replay
// never has to re-derive it, and is nil for an entry that can never be replayed.
func (c *Consumer) routeToDLQ(
	ctx context.Context,
	msg redis.XMessage,
	reason error,
	deliveries int64,
	deliveryGen uint64,
	replayPayload []byte,
) {
	event := valueString(msg)
	// An oversized-event rejection overrides the DLQ `event` field with a
	// bounded diagnostic summary, so the oversized raw payload is never copied
	// into PostgreSQL. Every other path keeps the original raw value verbatim.
	if errors.Is(reason, ErrEventOversized) {
		var oversize *EventOversizedError
		if errors.As(reason, &oversize) {
			event = oversizedEventSummary(oversize.Bytes, oversize.MaxBytes)
		}
	}
	// One child span around the whole DLQ write+ack operation. It is a child of
	// the current message span; the payload and reason text are never attached
	// (the reason may quote handler output).
	dlqCtx, dlqSpan := redisSpan(ctx, "dlq.persist")
	defer dlqSpan.End()
	ctx = dlqCtx

	if c.dlqStore == nil {
		// Without a DLQ store the required row cannot be written, so the source
		// must stay pending. This is a wiring error, not a reason to ACK and
		// lose the dead letter.
		err := fmt.Errorf("DLQ store is not configured")
		dlqSpan.RecordError(err)
		dlqSpan.SetStatus(codes.Error, err.Error())
		c.log.Error("Message: DLQ store is not configured; leaving pending",
			"message_id", msg.ID,
			"delivery_attempt", deliveries,
		)
		c.noteOutcome(err, 0)
		return
	}

	specs := dlqEntrySpecs(reason)
	deliveryID := dlq.NewDeliveryID(c.stream, c.group, msg.ID)
	for _, spec := range specs {
		if spec.invocation != "" {
			claim, exhausted, err := c.invStateStore.exhaustedClaim(
				ctx, c.stream, c.group, msg.ID, spec.invocation,
			)
			if err != nil {
				dlqSpan.RecordError(err)
				dlqSpan.SetStatus(codes.Error, err.Error())
				c.log.Error("Message: exhausted claim validation failed (leaving pending)",
					"message_id", msg.ID,
					"delivery_attempt", deliveries,
					"invocation", spec.invocation,
					"error", err,
				)
				c.noteOutcome(err, 0)
				return
			}
			// A real invocation must still hold a well-formed exhausted marker
			// whose retained attempt matches the runner's attribution. The token
			// is validated as part of parsing but never persisted or logged.
			if !exhausted || claim.Attempt != spec.attempts {
				err := fmt.Errorf("exhausted claim not current for invocation %q", spec.invocation)
				dlqSpan.RecordError(err)
				dlqSpan.SetStatus(codes.Error, err.Error())
				c.log.Error("Message: exhausted claim is stale or missing; leaving pending",
					"message_id", msg.ID,
					"delivery_attempt", deliveries,
					"invocation", spec.invocation,
					"attributed_attempt", spec.attempts,
					"state_attempt", claim.Attempt,
					"exhausted", exhausted,
				)
				c.noteOutcome(err, 0)
				return
			}
		}
		// The compact lineage of the final failed invocation, read from the
		// invocation-state hash just before the row is written (best-effort: no
		// lineage yields an omitted field). It is a sibling field, not part of
		// the lifecycle value.
		spec.trace = c.dlqTraceFor(ctx, msg.ID, spec.invocation)
		entry := dlqEntry(
			deliveryID, c.stream, msg.ID, c.group, c.consumer,
			event, replayPayload, spec, deliveries, time.Now(),
		)
		inserted, err := c.dlqStore.SaveDLQEntry(ctx, entry)
		if err != nil {
			dlqSpan.RecordError(err)
			dlqSpan.SetStatus(codes.Error, err.Error())
			c.log.Error("Message: DLQ persistence failed (leaving pending)",
				"message_id", msg.ID,
				"delivery_attempt", deliveries,
				"invocation", spec.invocation,
				"dlq_id", entry.ID,
				"error", err,
			)
			c.noteOutcome(err, 0)
			return
		}
		if !inserted {
			// The row was already committed (a redelivery after a failed ACK, or
			// a concurrent replica). No new entry was written, so the DLQ counter
			// is deliberately not incremented.
			c.log.Debug("Message: DLQ entry already persisted; skipping write",
				"message_id", msg.ID, "invocation", spec.invocation, "dlq_id", entry.ID)
			continue
		}
		c.metrics.Inc(metrics.MetricDLQEntries)
		c.log.Error("Message: invocation routed to DLQ",
			"message_id", msg.ID,
			"dlq_id", entry.ID,
			"app", spec.app,
			"handler", spec.handler,
			"handler_attempts", spec.attempts,
			"delivery_attempt", deliveries,
			"reason", spec.reason,
		)
	}
	result, err := c.ackDelivery(ctx, msg.ID, deliveryGen, true)
	if err != nil {
		if !errors.Is(err, errDeliverySuperseded) {
			dlqSpan.RecordError(err)
			dlqSpan.SetStatus(codes.Error, err.Error())
		}
		c.handleAckFailure(ctx, deliveryGen, msg.ID, err)
		return
	}
	if !result.Settled {
		// The committed DLQ rows stay unsettled; reconciliation preserves them
		// until the source delivery is genuinely resolved (the recreated group
		// will replay it, and a duplicate DLQ insert is idempotent).
		c.log.Debug("Message: DLQ source delivery not settled; leaving source pending",
			"message_id", msg.ID,
			"disposition", result.Disposition)
		return
	}
	// Every required row was committed before this call, so a failed or lost ACK
	// can never lose a dead letter: the source stays pending (or the reply is
	// lost after a successful XACK) and a redelivery either re-inserts the same
	// stable rows or observes the already-acked state. The ACK script stages its
	// persistent terminal marker before XACK and applies the retention TTL only
	// after XACK succeeds, so terminal Redis state never expires while the
	// source could still be recovered.
	//
	// Settlement is recorded only AFTER the ACK resolves successfully. If this
	// update is lost (crash between Redis and PostgreSQL), the bounded worker
	// reconciliation asks Redis for the exact disposition and marks the delivery
	// settled only when it proves the group already passed it.
	if err := c.dlqStore.MarkDLQDeliverySettled(ctx, deliveryID, time.Now()); err != nil {
		dlqSpan.RecordError(err)
		dlqSpan.SetStatus(codes.Error, err.Error())
		c.log.Warn("Message: DLQ settlement mark failed; reconciliation will retry",
			"message_id", msg.ID,
			"delivery_id", deliveryID,
			"error", err,
		)
	}
}

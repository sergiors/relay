package stream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"

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
	//   - NotScheduleEnvelope: an ordinary external event, handled below exactly
	//     as before.
	//   - ValidScheduleEnvelope: a well-formed schedule occurrence; when a
	//     ScheduleRunner is wired it is routed to it, bypassing event matching
	//     entirely. A nil ScheduleRunner falls back to treating it as a normal
	//     event (the safe fallback for tests/unwired consumers).
	//   - InvalidScheduleEnvelope: the event deliberately claims schedule
	//     identity (source==relay.schedule) but is structurally incomplete,
	//     malformed, or carries a mismatched occurrence_id. It must NEVER fall
	//     through to event matching and be executed/ACKed as unmatched: it can
	//     never succeed, so it is dead-lettered as a non-retryable failure,
	//     exactly like a malformed event body. This holds whether or not a
	//     ScheduleRunner is wired: a malformed claim is malformed either way.
	//
	// Classification happens BEFORE any invocation-state migration, like the
	// malformed-message path: an invalid claim is non-retryable and needs no
	// recoverable state, so it is dead-lettered directly. A valid occurrence
	// with a wired ScheduleRunner is dispatched before the generic migration
	// because the schedule path owns that setup itself.
	occ, envelopeKind, envelopeErr := schedule.ClassifyScheduleEnvelope(event)
	if envelopeKind == schedule.InvalidScheduleEnvelope {
		c.log.Error("Message: invalid schedule claim; routing to DLQ",
			"message_id", msg.ID,
			"delivery_attempt", deliveryNum,
			"error", envelopeErr,
		)
		outcome, spanErr = "dlq", envelopeErr
		c.routeToDLQ(ctx, msg, envelopeErr, deliveryNum)
		return
	}

	// A valid schedule occurrence with a wired ScheduleRunner bypasses event
	// matching entirely. Dispatch it before the generic invocation-state setup
	// below: processScheduleMessage owns its own migration and handler context,
	// so doing both here would migrate twice and build an unused event handler
	// context. A valid envelope without a runner falls through to ordinary
	// event handling (the safe fallback for unwired consumers).
	if envelopeKind == schedule.ValidScheduleEnvelope && c.scheduleRunner != nil {
		outcome, spanErr = c.processScheduleMessage(ctx, msg.ID, deliveryNum, occ)
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
	if _, err := c.invStateStore.ackAndRetainTerminal(ackCtx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
		ackSpan.RecordError(err)
		ackSpan.SetStatus(codes.Error, err.Error())
		ackSpan.End()
		c.log.Warn("Message: ack failed", "message_id", msg.ID, "error", err)
		c.noteOutcome(err, 0)
		return
	}
	ackSpan.End()
	outcome = "acked"
	// The message is fully processed and acknowledged, and its invocation state
	// was switched to terminal retention in the SAME atomic script, so
	// there is no post-ACK orphan window. If the ACK failed (handled above) the
	// message stays in the PEL; the script's marker staging is persistent (no
	// TTL), so a redelivery still observes completed handlers and the state never
	// expires. A retention/ack failure is a single error: logged here, never
	// fatal.
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
// setup.
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
			if _, ackErr := c.invStateStore.ackAndRetainTerminal(ctx, c.stream, c.group, msgID, c.invocationRetention); ackErr != nil {
				c.log.Warn("Schedule: message ack failed", "message_id", msgID, "error", ackErr)
				c.noteOutcome(ackErr, 0)
				return "pending", ackErr
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

	if _, err := c.invStateStore.ackAndRetainTerminal(ctx, c.stream, c.group, msgID, c.invocationRetention); err != nil {
		c.log.Warn("Schedule: message ack failed", "message_id", msgID, "error", err)
		c.noteOutcome(err, 0)
		return "pending", err
	}
	// The ACK and the terminal-retention switch happened in the SAME atomic
	// script, exactly like processMessage, so no post-ACK orphan window
	// exists.
	return "acked", nil
}

// unpersistedDLQSpecs filters the DLQ entry specs down to those whose entry has
// not already been persisted, consulting each invocation's per-invocation
// persistence marker ("exhausted:<attempt>:<token>:dlq") rather than scanning
// the DLQ stream. Skipping already-persisted entries is what makes retrying a
// partially-written multi-entry DLQ (after a failed XACK, a crash, or a partial
// write) idempotent: the retry writes only the missing entries and never
// duplicates the ones that succeeded. (The atomic persist script also refuses to
// rewrite an already-marked entry, so this pre-filter is a round-trip
// optimization and a metrics guard, not the correctness boundary.)
//
// It also returns each retained spec's exact exhausted claim identity (from the
// marker) so the subsequent atomic DLQ-persistence CASes that identity, never a
// guessed one. A spec with no well-formed exhausted marker carries a zero claim;
// the atomic script then refuses it without writing, and the message stays
// pending rather than appending an unattributable entry (a marker-less exhausted
// invocation is an anomaly: the runner writes the exhausted marker before it
// reports exhaustion).
//
// A store read error fails safe: the spec is kept so the entry is retried (a
// duplicate is allowed under at-least-once, while skipping a required write
// would lose the entry). A spec with no invocation ID (the malformed-message
// placeholder) is always kept; the atomic script skips it via its reserved
// message-scoped marker.
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
// the original. Each entry's XADD and its persistence marker are performed by
// ONE atomic script (invocationStateStore.persistDLQ), so a crash or error
// between the append and the marker can never leave an unmarked entry that a
// redelivery would append again (F-008); if the script fails, nothing was
// written and the original stays pending so the next recovery cycle retries the
// remaining writes rather than losing the message.
//
// Per-invocation entries: reason is the runner's terminal *HandlerExhaustedError
// carrying the exact app/handler and exhausted attempt for every terminal
// invocation, so a message matching several apps or handlers produces one
// precisely-attributed entry each (see dlqEntrySpecs). A reason without that
// typed metadata (a malformed message routed pre-handler) produces a single
// placeholder entry with an explicit handler_attempts of 0, never one invented
// from the delivery count; its persistence uses a reserved message-scoped field
// instead of a handler identity.
//
// Idempotent retry without scanning the DLQ: each invocation's exhausted marker
// records whether its entry has already been persisted
// ("exhausted:<attempt>:<token>:dlq"), and the malformed placeholder records its
// own reserved marker. On a redelivery after an XACK failure or a crash — or
// after a partial multi-entry write — invocations whose entry already exists are
// skipped (by the pre-filter and, defensively, by the atomic script itself), so
// the retry writes only the missing entries and can never duplicate (or lose)
// the ones that succeeded. The atomic script CASes the retained exhausted
// identity, so a stale claim can never append an unattributable entry or
// downgrade a newer marker.
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
		// One atomic script appends the entry AND records its persistence
		// (per-invocation ":dlq" marker, or the message-scoped placeholder field
		// for the malformed-message path), so a crash or error between the two
		// can never leave a duplicate or unmarked entry. On any failure nothing
		// was written: leave the message pending so the next recovery cycle
		// retries the remaining writes rather than losing the message.
		wrote, err := c.invStateStore.persistDLQ(
			ctx, c.dlqStream, c.stream, c.group, msg.ID, spec.invocation, spec.claim, entry,
		)
		if err != nil {
			dlqSpan.RecordError(err)
			dlqSpan.SetStatus(codes.Error, err.Error())
			c.log.Error("Message: DLQ persistence failed (leaving pending)",
				"message_id", msg.ID,
				"delivery_attempt", deliveries,
				"invocation", spec.invocation,
				"error", err,
			)
			c.noteOutcome(err, 0)
			return
		}
		if !wrote {
			// The entry/placeholder was already persisted (a concurrent or
			// replayed delivery won the race). No new entry was appended, so the
			// DLQ counter is deliberately not incremented.
			c.log.Debug("Message: DLQ entry already persisted; skipping write",
				"message_id", msg.ID, "invocation", spec.invocation)
			continue
		}
		c.metrics.Inc(metrics.MetricDLQEntries)
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
	if _, err := c.invStateStore.ackAndRetainTerminal(ctx, c.stream, c.group, msg.ID, c.invocationRetention); err != nil {
		dlqSpan.RecordError(err)
		dlqSpan.SetStatus(codes.Error, err.Error())
		c.log.Warn("Message: ack after DLQ failed", "message_id", msg.ID, "error", err)
		c.noteOutcome(err, 0)
		return
	}
	// The message is dead-lettered and the original acknowledged, and its
	// invocation-state hash was switched to terminal retention in the SAME atomic
	// script. Every DLQ entry was persisted before this call, so a failed
	// ACK can never lose an entry: the message stays pending (the staged marker
	// is persistent, no TTL) and a redelivery re-acks idempotently.
}

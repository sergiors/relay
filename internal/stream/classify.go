package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/envelope"
)

// dlqEntry builds ONE broker-neutral dead-letter entry for one exhausted
// invocation (or, for a message that never reached a handler, the placeholder
// with NoHandler app/handler and an explicit 0 handler attempt count).
//
// Two distinct counts are carried, deliberately:
//
//   - Deliveries is the source delivery count (the retry counter read from
//     XPENDING and passed through the consumer/reclaim flow). It counts every
//     message redelivery, including redeliveries that skipped a protected
//     invocation, so it is >= HandlerAttempts.
//   - HandlerAttempts is the handler execution attempt that exhausted this
//     invocation's per-invocation retry state, i.e. the count that actually
//     drove the exhaustion decision. It comes from the runner's typed
//     exhaustion error; a message routed before a handler ran is explicitly 0,
//     never fabricated from the delivery count.
//
// The entry ID is derived from the source delivery identity plus the invocation
// identity, so writing the same exhaustion again is idempotent by primary key.
// The Event payload is supplied by the caller: an oversized message passes only
// its bounded diagnostic summary. ReplayPayload is the decoded logical handler
// payload, and is nil for a message that can never be replayed.
func dlqEntry(
	deliveryID, source, messageID, group, consumer, event string,
	replayPayload []byte,
	spec dlqEntrySpec,
	deliveries int64,
	timestamp time.Time,
) dlq.Entry {
	return dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, spec.invocation),
		Source:          source,
		SourceGroup:     group,
		SourceMessageID: messageID,
		DeliveryID:      deliveryID,
		Consumer:        consumer,
		Event:           event,
		ReplayPayload:   replayPayload,
		Reason:          spec.reason,
		App:             spec.app,
		Handler:         spec.handler,
		Deliveries:      deliveries,
		HandlerAttempts: spec.attempts,
		Trace:           spec.trace,
		Timestamp:       timestamp.UTC().Format(time.RFC3339),
	}
}

// exhaustedInvocationsFromError extracts the exhausted invocation metadata from
// the runner's terminal exhaustion signal. It returns nil when the error is not
// a *HandlerExhaustedError (e.g. a malformed-message DLQ routing that never
// reached the handler), which routeToDLQ maps onto one placeholder entry with
// an explicit handler_attempts of 0 — a delivery count is never mistaken for a
// handler attempt count. Entries with a non-positive attempt count are dropped
// defensively so a zero cannot be persisted for a real invocation.
func exhaustedInvocationsFromError(err error) []ExhaustedInvocation {
	var exhausted *HandlerExhaustedError
	if !errors.As(err, &exhausted) {
		return nil
	}
	out := make([]ExhaustedInvocation, 0, len(exhausted.Invocations))
	for _, iv := range exhausted.Invocations {
		if iv.Attempts > 0 && iv.App != "" && iv.Handler != "" {
			out = append(out, iv)
		}
	}
	return out
}

// dlqEntrySpec is one DLQ entry to write: exactly one exhausted invocation (or,
// for a message with no handler invocation, the placeholder). The invocation ID
// is empty when the entry has no per-invocation exhaustion marker to consult
// (the malformed-message path), so routeToDLQ neither checks nor marks
// persistence for it.
type dlqEntrySpec struct {
	app      string
	handler  string
	attempts int
	reason   string
	// invocation is the "<app>/<handler>" ID used to record this entry's
	// persistence in the invocation-state hash. It is empty when there is no
	// invocation (malformed message routed pre-handler).
	invocation string
	// trace is the OPTIONAL compact lineage of the final failed invocation,
	// read from the invocation-state hash by routeToDLQ just before the write
	// (see dlqTraceFor). Empty for the placeholder or when no lineage was
	// recorded.
	trace string
}

// dlqEntrySpecs expands the runner's terminal exhaustion error into one DLQ
// entry per exhausted invocation. A message matching several apps or
// handlers is therefore dead-lettered as one precisely-attributed entry each,
// with its own attempt count and reason. When the error carries no typed
// invocation metadata (a malformed message routed pre-handler), a single
// placeholder entry is produced with "-" app/handler and an explicit 0
// handler_attempts, never a fabricated count.
func dlqEntrySpecs(reason error) []dlqEntrySpec {
	invocations := exhaustedInvocationsFromError(reason)
	if len(invocations) == 0 {
		return []dlqEntrySpec{{
			app:     dlq.NoHandler,
			handler: dlq.NoHandler,
			reason:  reason.Error(),
		}}
	}
	specs := make([]dlqEntrySpec, 0, len(invocations))
	seen := make(map[string]bool, len(invocations))
	for _, iv := range invocations {
		// Duplicate "<app>/<handler>" metadata in the aggregate collapses
		// to one entry: a parsed template rejects duplicate event handlers, but
		// this defensive dedup keeps the contract ("one entry per exhausted
		// invocation") explicit. The persistence-marker skip would also cover
		// it.
		if seen[iv.Invocation()] {
			continue
		}
		seen[iv.Invocation()] = true
		specs = append(specs, dlqEntrySpec{
			app:        iv.App,
			handler:    iv.Handler,
			attempts:   iv.Attempts,
			reason:     ErrInvocationExhausted.Error() + ": " + iv.Reason(),
			invocation: iv.Invocation(),
		})
	}
	return specs
}

// valueString extracts the raw "value" field value from a message as a string,
// falling back to "-" when it cannot be represented. Used for the DLQ "event"
// field (the original payload). Since a non-retryable message may be missing
// the field or contain a non-string, this must not fail.
func valueString(msg redis.XMessage) string {
	if raw, err := extractValueString(msg); err == nil {
		return raw
	}
	return "-"
}

// extractValueString extracts the raw "value" field value as a string. It is
// the single field-presence/type validation seam: processMessage uses it once
// and then applies the byte cap or hands the string to the envelope decoder, so
// the field extraction is never duplicated or reparsed.
func extractValueString(msg redis.XMessage) (string, error) {
	raw, ok := msg.Values["value"]
	if !ok {
		return "", fmt.Errorf("missing 'value' field")
	}
	rawStr, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("'value' field is not a string")
	}
	return rawStr, nil
}

// extractOptionalString extracts an optional transport field. A missing field
// is the empty string; a present non-string field is malformed input.
func extractOptionalString(msg redis.XMessage, field string) (string, error) {
	raw, ok := msg.Values[field]
	if !ok {
		return "", nil
	}
	rawStr, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%q field is not a string", field)
	}
	return rawStr, nil
}

// extractAndDecodeEnvelope is the single extraction+decode seam the processing
// path uses: it extracts the raw value string once, applies the raw-byte cap
// (len(raw) is the Go string byte length), and only then decodes the envelope.
// Every other entry field is ignored; only `value` is the body and only
// `format` (when present) changes decoding. A raw value over maxBytes returns
// an *EventOversizedError without decoding or
// matching, so the oversized payload is neither parsed nor fanned out to
// handlers. maxBytes is always positive (the consumer resolves a default; zero
// never means unlimited). Missing/non-string/malformed values keep the ordinary
// non-retryable (malformed) error behavior.
func extractAndDecodeEnvelope(
	msg redis.XMessage,
	maxBytes int,
) (envelope.Result, error) {
	rawStr, err := extractValueString(msg)
	if err != nil {
		return envelope.Result{}, err
	}
	if n := len(rawStr); n > maxBytes {
		return envelope.Result{}, &EventOversizedError{
			Bytes: n, MaxBytes: maxBytes,
		}
	}
	format, err := extractOptionalString(msg, "format")
	if err != nil {
		return envelope.Result{}, err
	}
	return envelope.Decode(format, []byte(rawStr))
}

// ErrEventOversized is the stable, non-retryable rejection reason for a message
// whose raw `value` exceeds the configured byte cap. It is the sentinel the DLQ
// `reason` is built around (see EventOversizedError) and is used by tests to
// assert the oversize path distinct from an ordinary malformed decode failure.
var ErrEventOversized = errors.New("event oversized")

// EventOversizedError is the rejection returned when a message's raw `value` is
// longer than the configured cap. It wraps ErrEventOversized, so
// errors.Is(err, ErrEventOversized) is the predicate. Its message reports the
// measured byte length and the configured maximum, never the payload itself.
type EventOversizedError struct {
	Bytes    int
	MaxBytes int
}

func (e *EventOversizedError) Error() string {
	return fmt.Sprintf("%s: raw value is %d bytes, exceeds MAX_EVENT_BYTES %d",
		ErrEventOversized, e.Bytes, e.MaxBytes)
}

func (e *EventOversizedError) Unwrap() error { return ErrEventOversized }

// oversizedEventSummary builds the bounded DLQ `event` value for an oversized
// message. It is deliberately a small diagnostic JSON object carrying the
// measured byte length and the configured cap (plus a fixed marker), NOT the
// oversized raw payload: storing the payload would defeat the cap by writing it
// to the DLQ. No event_id is extracted because that would require parsing the
// huge payload the cap exists to avoid. Only the fixed string/int fields are
// marshaled, so Marshal cannot fail; the fallback is deterministic.
func oversizedEventSummary(n, maxBytes int) string {
	const marker = "event_oversized"
	if b, err := json.Marshal(map[string]any{
		"relay_summary":   marker,
		"event_bytes":     n,
		"max_event_bytes": maxBytes,
	}); err == nil {
		return string(b)
	}
	return fmt.Sprintf(
		`{"relay_summary":%q,"event_bytes":%d,"max_event_bytes":%d}`,
		marker, n, maxBytes)
}

package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// dlqPayload builds the flat field map for ONE DLQ entry, corresponding to one
// exhausted invocation (or, for a malformed message that never reached a
// handler, a single entry with placeholder app/handler and an explicit 0
// handler_attempts). Keeping it flat (no nested JSON) keeps the entry easy to
// inspect with redis-cli and re-drive by hand.
//
// Two distinct counts are emitted, deliberately:
//
//   - "deliveries" is the authoritative Redis Stream/PEL delivery count (the
//     retry counter read from XPENDING and passed through the consumer/reclaim
//     flow). It counts every message redelivery, including redeliveries that
//     skipped a protected invocation, so it is >= handler_attempts.
//   - "handler_attempts" is the handler execution attempt that exhausted this
//     invocation's per-invocation retry state (TryStart/recordFailure/
//     MarkExhausted), i.e. the count that actually drove the exhaustion
//     decision. It comes from the runner's typed exhaustion error; for DLQ
//     paths with no handler retry state (e.g. a malformed message routed
//     pre-handler) it is explicitly 0, never fabricated from the delivery
//     count.
//
// "app" and "handler" identify the exact exhausted invocation so a message
// matching several apps/handlers produces one precisely-attributed entry
// each. The malformed-message path has no invocation, so it carries the "-"
// placeholder for both.
//
// trace is the OPTIONAL compact lineage (traceparent[|tracestate]) of the final
// failed invocation, read from the invocation-state hash at DLQ-write time. It is
// omitted entirely when absent (a pre-tracing entry, the malformed-message
// path, or tracing disabled), so every existing entry's field shape is
// unchanged; baggage is never recorded.
func dlqPayload(
	stream, id, group, consumer, event, reason, app, handler string,
	deliveries int64, handlerAttempts int, trace string,
) map[string]any {
	fields := map[string]any{
		"original_stream":  stream,
		"original_id":      id,
		"group":            group,
		"consumer":         consumer,
		"event":            event,
		"reason":           reason,
		"app":              app,
		"handler":          handler,
		"deliveries":       deliveries,
		"handler_attempts": handlerAttempts,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	}
	if trace != "" {
		fields["trace"] = trace
	}
	return fields
}

// dlqNoHandler is the app/handler placeholder for a DLQ entry that has no
// handler invocation to attribute (a malformed message routed pre-handler). It
// mirrors eventString's "-" fallback so every DLQ entry keeps the same flat,
// always-present field shape.
const dlqNoHandler = "-"

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
	// claim is the exhausted claim identity (attempt + token) retained by the
	// invocation's exhausted marker at DLQ-write time. routeToDLQ CASes the
	// markExhaustedDLQ upgrade on it, so a stale XADD outcome can never upgrade a
	// newer/foreign marker. It is the zero claim when there is no well-formed
	// exhausted marker (the placeholder, or a marker-less invocation).
	claim InvocationClaim
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
			app:     dlqNoHandler,
			handler: dlqNoHandler,
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

// eventString extracts the raw "event" field value from a message as a string,
// falling back to "-" when it cannot be represented. Used for the DLQ "event"
// field (the original payload). Since a non-retryable message may be missing
// the field or contain a non-string, this must not fail.
func eventString(msg redis.XMessage) string {
	if raw, err := extractEventString(msg); err == nil {
		return raw
	}
	return "-"
}

// extractEventString extracts the raw "event" field value as a string. It is
// the single field-presence/type validation seam: processMessage uses it once
// and then either applies the byte cap or hands the string to decodeEvent, so
// the field extraction is never duplicated or reparsed.
func extractEventString(msg redis.XMessage) (string, error) {
	raw, ok := msg.Values["event"]
	if !ok {
		return "", fmt.Errorf("missing 'event' field")
	}
	rawStr, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("'event' field is not a string")
	}
	return rawStr, nil
}

// decodeEvent decodes an already-extracted raw event string into a JSON object.
// It is the single decode seam, so processMessage applies the byte cap and
// decodes without a second extraction path. Unmarshal into a map rejects JSON
// that is not an object (arrays, scalars), which is the desired non-retryable
// classification.
func decodeEvent(rawStr string) (map[string]any, error) {
	var event map[string]any
	if err := json.Unmarshal([]byte(rawStr), &event); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	if event == nil {
		// "null" decodes into a nil map without error; it is not a usable object.
		return nil, fmt.Errorf("'event' decodes to null")
	}
	return event, nil
}

// classifyMessage extracts and decodes the "event" field. Any error indicates the
// message is malformed and can never be processed successfully (non-retryable).
// It is the single-message convenience form of extractEventString + decodeEvent,
// kept for the existing tests and any caller that does not need the raw string.
func classifyMessage(msg redis.XMessage) (map[string]any, error) {
	rawStr, err := extractEventString(msg)
	if err != nil {
		return nil, err
	}
	return decodeEvent(rawStr)
}

// extractAndDecodeEvent is the single extraction+decode seam the processing path
// uses: it extracts the raw event string once, applies the raw-byte cap
// (len(raw) is the Go string byte length), and only then decodes. A raw value
// over maxBytes returns an *EventOversizedError without decoding or matching, so
// the oversized payload is neither parsed nor fanned out to handlers. maxBytes
// is always positive (the consumer resolves a default; zero never means
// unlimited). Missing/non-string/malformed values keep the ordinary
// non-retryable (malformed) error behavior.
func extractAndDecodeEvent(msg redis.XMessage, maxBytes int) (map[string]any, error) {
	rawStr, err := extractEventString(msg)
	if err != nil {
		return nil, err
	}
	if n := len(rawStr); n > maxBytes {
		return nil, &EventOversizedError{Bytes: n, MaxBytes: maxBytes}
	}
	return decodeEvent(rawStr)
}

// ErrEventOversized is the stable, non-retryable rejection reason for a message
// whose raw `event` value exceeds the configured byte cap. It is the sentinel
// the DLQ `reason` is built around (see EventOversizedError) and is used by
// tests to assert the oversize path distinct from an ordinary malformed decode
// failure.
var ErrEventOversized = errors.New("event oversized")

// EventOversizedError is the rejection returned when a message's raw `event`
// value is longer than the configured cap. It wraps ErrEventOversized, so
// errors.Is(err, ErrEventOversized) is the predicate. Its message reports the
// measured byte length and the configured maximum, never the payload itself.
type EventOversizedError struct {
	Bytes    int
	MaxBytes int
}

func (e *EventOversizedError) Error() string {
	return fmt.Sprintf("%s: raw event value is %d bytes, exceeds MAX_EVENT_BYTES %d",
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
	return fmt.Sprintf(`{"relay_summary":%q,"event_bytes":%d,"max_event_bytes":%d}`, marker, n, maxBytes)
}

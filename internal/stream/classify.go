package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// dlqPayload builds the flat field map written to the DLQ stream. Keeping it
// flat (no nested JSON) keeps the entry easy to inspect with redis-cli and
// re-drive by hand.
//
// Two distinct counts are emitted, deliberately:
//
//   - "deliveries" is the authoritative Redis Stream/PEL delivery count (the
//     retry counter read from XPENDING and passed through the consumer/reclaim
//     flow). It counts every message redelivery, including redeliveries that
//     skipped a protected invocation, so it is >= handler_attempts.
//   - "handler_attempts" is the handler execution attempt that exhausted the
//     per-invocation retry state (TryStart/recordFailure/MarkExhausted), i.e.
//     the count that actually drove the exhaustion decision. It comes from the
//     runner's typed exhaustion error; for DLQ paths with no handler retry
//     state (e.g. a malformed message routed pre-handler) it is explicitly 0,
//     never fabricated from the delivery count.
func dlqPayload(stream, id, group, consumer, event, reason string, deliveries int64, handlerAttempts int) map[string]any {
	return map[string]any{
		"original_stream":  stream,
		"original_id":      id,
		"group":            group,
		"consumer":         consumer,
		"event":            event,
		"reason":           reason,
		"deliveries":       deliveries,
		"handler_attempts": handlerAttempts,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	}
}

// handlerAttemptsFromError extracts the exhausted handler attempt count from the
// runner's terminal exhaustion signal. It returns 0 when the error is not a
// *HandlerExhaustedError (e.g. a malformed-message DLQ routing that never
// reached the handler), so a delivery count is never mistaken for a handler
// attempt count.
func handlerAttemptsFromError(err error) int {
	var exhausted *HandlerExhaustedError
	if errors.As(err, &exhausted) && exhausted.HandlerAttempts > 0 {
		return exhausted.HandlerAttempts
	}
	return 0
}

// eventString extracts the raw "event" field value from a message as a string,
// falling back to "-" when it cannot be represented. Used for the DLQ "event"
// field (the original payload). Since a non-retryable message may be missing
// the field or contain a non-string, this must not fail.
func eventString(msg redis.XMessage) string {
	if raw, ok := msg.Values["event"].(string); ok {
		return raw
	}
	return "-"
}

// classifyMessage extracts and decodes the "event" field. Any error indicates the
// message is malformed and can never be processed successfully (non-retryable).
func classifyMessage(msg redis.XMessage) (map[string]any, error) {
	raw, ok := msg.Values["event"]
	if !ok {
		return nil, fmt.Errorf("missing 'event' field")
	}
	rawStr, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("'event' field is not a string")
	}
	// Unmarshal into a map rejects JSON that is not an object (arrays, scalars),
	// which is the desired non-retryable classification.
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

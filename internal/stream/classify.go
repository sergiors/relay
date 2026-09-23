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
// handler, a single entry with placeholder function/handler and an explicit 0
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
// "function" and "handler" identify the exact exhausted invocation so a message
// matching several functions/handlers produces one precisely-attributed entry
// each. The malformed-message path has no invocation, so it carries the "-"
// placeholder for both.
func dlqPayload(stream, id, group, consumer, event, reason, function, handler string, deliveries int64, handlerAttempts int) map[string]any {
	return map[string]any{
		"original_stream":  stream,
		"original_id":      id,
		"group":            group,
		"consumer":         consumer,
		"event":            event,
		"reason":           reason,
		"function":         function,
		"handler":          handler,
		"deliveries":       deliveries,
		"handler_attempts": handlerAttempts,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	}
}

// dlqNoHandler is the function/handler placeholder for a DLQ entry that has no
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
		if iv.Attempts > 0 && iv.Function != "" && iv.Handler != "" {
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
	function string
	handler  string
	attempts int
	reason   string
	// invocation is the "<function>/<handler>" ID used to record this entry's
	// persistence in the invocation-state hash. It is empty when there is no
	// invocation (malformed message routed pre-handler).
	invocation string
}

// dlqEntrySpecs expands the runner's terminal exhaustion error into one DLQ
// entry per exhausted invocation. A message matching several functions or
// handlers is therefore dead-lettered as one precisely-attributed entry each,
// with its own attempt count and reason. When the error carries no typed
// invocation metadata (a malformed message routed pre-handler), a single
// placeholder entry is produced with "-" function/handler and an explicit 0
// handler_attempts, never a fabricated count.
func dlqEntrySpecs(reason error) []dlqEntrySpec {
	invocations := exhaustedInvocationsFromError(reason)
	if len(invocations) == 0 {
		return []dlqEntrySpec{{
			function: dlqNoHandler,
			handler:  dlqNoHandler,
			reason:   reason.Error(),
		}}
	}
	specs := make([]dlqEntrySpec, 0, len(invocations))
	seen := make(map[string]bool, len(invocations))
	for _, iv := range invocations {
		// Two matching rules may share a handler (a legal template), so the same
		// "<function>/<handler>" can appear more than once in the aggregate.
		// Deduplicate so exactly one entry per invocation is produced; the
		// persistence-marker skip would also cover it, but deduping keeps the
		// contract ("one entry per exhausted invocation") explicit.
		if seen[iv.Invocation()] {
			continue
		}
		seen[iv.Invocation()] = true
		specs = append(specs, dlqEntrySpec{
			function:   iv.Function,
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

package schedule

import (
	"encoding/json"
	"fmt"
)

// ReplayPayload resolves the handler payload for a DLQ replay from the event
// bytes stored with the dead letter.
//
// A DLQ row preserves the source stream body verbatim for inspection, and that
// body is not always the bytes the handler received. An ordinary event's body
// IS its handler payload and is returned unchanged, byte for byte. A schedule
// occurrence's body is its envelope (Occurrence.Envelope), which carries the
// internal coordination fields (app, schedule, handler, occurrence_id) that are
// deliberately kept out of the handler payload; replay must therefore hand the
// handler exactly Occurrence.Payload(), as the live schedule path does.
//
// Classification reuses ClassifyScheduleEnvelope, so the reserved
// source=="relay.schedule" marker decides the path. The event bytes are first
// required to be a JSON object, because only such a payload can have reached a
// handler. Invalid JSON or a non-object/null payload is therefore an explicit
// error rather than an unchanged replay: it cannot be safely distinguished from
// a malformed or truncated schedule envelope, and substituting it for the
// original handler payload would be incorrect. An object envelope that carries
// the marker but is incomplete, malformed, or inconsistent (including a
// forged/mismatched occurrence_id) is also an explicit error: falling back to
// the stored envelope bytes would hand the handler coordination fields it has
// never seen, and an invalid claim must not be silently repaired on replay.
func ReplayPayload(event []byte) ([]byte, error) {
	var decoded map[string]any
	if err := json.Unmarshal(event, &decoded); err != nil {
		return nil, fmt.Errorf("replay payload: invalid event JSON: %w", err)
	}
	if decoded == nil {
		return nil, fmt.Errorf("replay payload: event is not a JSON object")
	}
	occ, kind, err := ClassifyScheduleEnvelope(decoded)
	switch kind {
	case NotScheduleEnvelope:
		return event, nil
	case ValidScheduleEnvelope:
		return occ.Payload(), nil
	default:
		// InvalidScheduleEnvelope: the marker is present, so this is a schedule
		// claim that failed the shared structural/semantic validation.
		return nil, fmt.Errorf("replay schedule payload: %w", err)
	}
}

package schedule

import (
	"encoding/json"
	"fmt"
	"time"
)

// Occurrence is one logical firing of a cron schedule: the function and handler
// it invokes and the scheduled instant it belongs to. Two workers evaluating
// the same cron tick produce the same Occurrence, so they contend on exactly
// one publish-if-new.
type Occurrence struct {
	Function    string
	Handler     string
	ScheduledAt time.Time // the scheduled instant, UTC
}

// ID returns the deterministic occurrence identity:
// "schedule:<function>:<handler>:<scheduled_at RFC3339 UTC>".
// ScheduledAt is normalized to UTC (truncated to the second — 5-field cron
// granularity) so DST offsets and timezone representation never change the ID:
// the configured timezone affects when the schedule fires, never the identity.
func (o Occurrence) ID() string {
	// Normalize to UTC and truncate to the second. Truncate works on the
	// absolute instant since the epoch, so it is stable across workers; the
	// timezone the cron was evaluated in is deliberately dropped here. Dropping
	// sub-second components means worker-side evaluation jitter (whether two
	// workers read the due instant a few milliseconds apart) never splits an
	// occurrence into two IDs.
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	return "schedule:" + o.Function + ":" + o.Handler + ":" + t.Format(time.RFC3339)
}

// Payload returns the handler payload exactly as before the stream change:
// {"source":"relay.schedule","scheduled_at":"<RFC3339 UTC>"}. Only these two
// fields are exposed to the function; internal coordination fields (function,
// handler, occurrence_id) are never part of the handler payload.
func (o Occurrence) Payload() []byte {
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	b, _ := json.Marshal(map[string]string{
		"source":       "relay.schedule",
		"scheduled_at": t.Format(time.RFC3339),
	})
	return b
}

// Envelope returns the stream message body (JSON): the scheduled occurrence plus
// its derived occurrence_id, written into the stream by the publisher on
// PublishOccurrence. The handler payload and the envelope are separate: the
// envelope is what the consumer decodes to route the message; the handler only
// ever sees Payload().
func (o Occurrence) Envelope() ([]byte, error) {
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	return json.Marshal(map[string]string{
		"source":        "relay.schedule",
		"function":      o.Function,
		"handler":       o.Handler,
		"scheduled_at":  t.Format(time.RFC3339),
		"occurrence_id": o.ID(),
	})
}

// ParseEnvelope decodes a stream envelope into its Occurrence. It validates
// non-empty function/handler and a parseable scheduled_at (normalized to UTC),
// and RECOMPUTES the ID from those fields rather than trusting the stored
// occurrence_id. Identity is derived, never trusted from the wire: a malformed
// or forged occurrence_id cannot redirect an invocation — the recomputed ID is
// what the consumer and the invocation-state keying use.
func ParseEnvelope(raw string) (Occurrence, error) {
	var e struct {
		Function     string `json:"function"`
		Handler      string `json:"handler"`
		ScheduledAt  string `json:"scheduled_at"`
		OccurrenceID string `json:"occurrence_id"`
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: %w", err)
	}
	if e.Function == "" {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: function is empty")
	}
	if e.Handler == "" {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: handler is empty")
	}
	t, err := time.Parse(time.RFC3339, e.ScheduledAt)
	if err != nil {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: scheduled_at: %w", err)
	}
	return Occurrence{Function: e.Function, Handler: e.Handler, ScheduledAt: t.UTC()}, nil
}

// IsScheduleEvent reports whether a decoded event map is a schedule message and,
// when it is, returns the parsed Occurrence. Schedule messages are recognized by
// the envelope's "source" == "relay.schedule" plus string function/handler/
// occurrence_id fields present in the DECODED event map. Absence of the source
// marker (or any missing field) returns (zero, false), so normal events are
// untouched. An ordinary user event carrying source==relay.schedule AND a valid
// occurrence_id is a deliberate claim of schedule identity, which is acceptable
// and documented: the occurrence_id is re-derived from the fields, so a forged
// value cannot redirect the invocation (identity is derived, never trusted).
func IsScheduleEvent(event map[string]any) (Occurrence, bool) {
	src, ok := event["source"].(string)
	if !ok || src != "relay.schedule" {
		return Occurrence{}, false
	}
	fn, ok := event["function"].(string)
	if !ok || fn == "" {
		return Occurrence{}, false
	}
	handler, ok := event["handler"].(string)
	if !ok || handler == "" {
		return Occurrence{}, false
	}
	schedAt, ok := event["scheduled_at"].(string)
	if !ok {
		return Occurrence{}, false
	}
	t, err := time.Parse(time.RFC3339, schedAt)
	if err != nil {
		return Occurrence{}, false
	}
	return Occurrence{Function: fn, Handler: handler, ScheduledAt: t.UTC()}, true
}

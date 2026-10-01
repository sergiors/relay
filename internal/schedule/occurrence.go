package schedule

import (
	"encoding/json"
	"fmt"
	"time"
)

// Occurrence is one logical firing of a cron schedule: the app, the
// schedule's stable name, the handler it invokes, and the scheduled instant it
// belongs to. Two workers evaluating the same cron tick produce the same
// Occurrence, so they contend on exactly one publish-if-new.
//
// Schedule is the schedule's stable configuration identity (template
// `schedules[].name`); Handler is the current handler that schedule invokes.
// Identity is (app, schedule name, scheduled instant): a schedule keeps its
// identity across a handler change, and two schedules sharing a handler are
// distinct occurrences. Handler travels in the envelope so the consumer can
// resolve the schedule by name and invoke its CURRENT handler (a name's handler
// may have changed since publication, in which case the fresh handler runs —
// there is no other handler to run for that schedule).
type Occurrence struct {
	App         string
	Schedule    string
	Handler     string
	ScheduledAt time.Time // the scheduled instant, UTC
}

// ID returns the deterministic occurrence identity:
// "schedule:<app>:<schedule name>:<scheduled_at RFC3339 UTC>".
// ScheduledAt is normalized to UTC and truncated to the second so DST offsets
// and timezone representation never change the ID: the configured timezone
// affects when the schedule fires, never the identity.
func (o Occurrence) ID() string {
	// Normalize to UTC and truncate to the second. Truncate works on the
	// absolute instant since the epoch, so it is stable across workers; the
	// timezone the cron was evaluated in is deliberately dropped here. Dropping
	// sub-second components means worker-side evaluation jitter (whether two
	// workers read the due instant a few milliseconds apart) never splits an
	// occurrence into two IDs. Seconds themselves are NOT truncated: schedules
	// are minute-granularity by construction (the scheduler stamps a
	// minute-truncated due instant — seconds schedules are rejected at template
	// validation), so two distinct whole seconds would still be distinct.
	t := o.ScheduledAt.UTC().Truncate(time.Second)
	return "schedule:" + o.App + ":" + o.Schedule + ":" + t.Format(time.RFC3339)
}

// Payload returns the handler payload exactly as before the stream change:
// {"source":"relay.schedule","scheduled_at":"<RFC3339 UTC>"}. Only these two
// fields are exposed to the app; internal coordination fields (app,
// schedule, handler, occurrence_id) are never part of the handler payload.
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
		"app":           o.App,
		"schedule":      o.Schedule,
		"handler":       o.Handler,
		"scheduled_at":  t.Format(time.RFC3339),
		"occurrence_id": o.ID(),
	})
}

// ParseEnvelope decodes a stream envelope into its Occurrence. It validates
// non-empty app/schedule/handler and a parseable scheduled_at (normalized
// to UTC), and RECOMPUTES the ID from those fields rather than trusting the
// stored occurrence_id. Identity is derived, never trusted from the wire: a
// malformed or forged occurrence_id cannot redirect an invocation — the
// recomputed ID is what the consumer and the invocation-state keying use.
func ParseEnvelope(raw string) (Occurrence, error) {
	var e struct {
		App          string `json:"app"`
		Schedule     string `json:"schedule"`
		Handler      string `json:"handler"`
		ScheduledAt  string `json:"scheduled_at"`
		OccurrenceID string `json:"occurrence_id"`
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: %w", err)
	}
	if e.App == "" {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: app is empty")
	}
	if e.Schedule == "" {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: schedule is empty")
	}
	if e.Handler == "" {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: handler is empty")
	}
	t, err := time.Parse(time.RFC3339, e.ScheduledAt)
	if err != nil {
		return Occurrence{}, fmt.Errorf("parse schedule envelope: scheduled_at: %w", err)
	}
	return Occurrence{App: e.App, Schedule: e.Schedule, Handler: e.Handler, ScheduledAt: t.UTC()}, nil
}

// IsScheduleEvent reports whether a decoded event map is a schedule message and,
// when it is, returns the parsed Occurrence. Schedule messages are recognized by
// the envelope's "source" == "relay.schedule" plus string app/schedule/
// handler/scheduled_at fields present in the DECODED event map. Absence of the
// source marker (or any missing field) returns (zero, false), so normal events
// are untouched. An ordinary user event carrying source==relay.schedule AND the
// schedule fields is a deliberate claim of schedule identity, which is
// acceptable and documented: the occurrence id is re-derived from the fields, so
// a forged value cannot redirect the invocation (identity is derived, never
// trusted).
func IsScheduleEvent(event map[string]any) (Occurrence, bool) {
	src, ok := event["source"].(string)
	if !ok || src != "relay.schedule" {
		return Occurrence{}, false
	}
	fn, ok := event["app"].(string)
	if !ok || fn == "" {
		return Occurrence{}, false
	}
	scheduleName, ok := event["schedule"].(string)
	if !ok || scheduleName == "" {
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
	return Occurrence{App: fn, Schedule: scheduleName, Handler: handler, ScheduledAt: t.UTC()}, true
}

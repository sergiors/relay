package schedule

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// claimMap decodes a valid envelope into a mutable map, so tests can perturb one
// field at a time.
func claimMap(t *testing.T) map[string]any {
	t.Helper()
	o := Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	raw, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return event
}

// clone returns a shallow copy of event.
func clone(event map[string]any) map[string]any {
	out := make(map[string]any, len(event))
	for k, v := range event {
		out[k] = v
	}
	return out
}

// TestEnvelopeClassifyNotSchedule pins that only the reserved source marker
// identifies a schedule envelope. An absent, empty, or foreign source is an
// ordinary event and must be left to normal matching.
func TestEnvelopeClassifyNotSchedule(t *testing.T) {
	for _, event := range []map[string]any{
		{},
		{"event_name": "INSERT"},
		{"source": "postgres.cdc", "app": "f", "schedule": "s", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": "", "app": "f", "schedule": "s", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": 42, "app": "f", "schedule": "s", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": "relay.scheduleX", "app": "f", "schedule": "s", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
	} {
		occ, kind, err := ClassifyScheduleEnvelope(event)
		if kind != NotScheduleEnvelope || err != nil {
			t.Fatalf(
				"classification(%+v) = (%+v, %v, %v), want NotScheduleEnvelope",
				event, occ, kind, err,
			)
		}
	}
}

// TestEnvelopeClassifyValid pins that a complete, self-consistent envelope is
// valid.
func TestEnvelopeClassifyValid(t *testing.T) {
	occ, kind, err := ClassifyScheduleEnvelope(claimMap(t))
	if err != nil || kind != ValidScheduleEnvelope {
		t.Fatalf("classification(valid) = (%+v, %v, %v), want valid", occ, kind, err)
	}
	if occ.App != "courses" || occ.Schedule != "cleanup" || occ.Handler != "jobs.cleanup.handler" {
		t.Fatalf("occurrence = %+v", occ)
	}
	if !occ.ScheduledAt.Equal(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("scheduled_at = %v", occ.ScheduledAt)
	}
}

// TestEnvelopeClassifyInvalid pins that a marker-present event with any
// structural failure is invalid, so the stream routes it to the DLQ.
func TestEnvelopeClassifyInvalid(t *testing.T) {
	valid := claimMap(t)
	tests := []struct {
		name   string
		event  map[string]any
		reason string
	}{
		{"missing app", func() map[string]any { e := clone(valid); delete(e, "app"); return e }(), "app"},
		{"empty app", func() map[string]any { e := clone(valid); e["app"] = ""; return e }(), "app"},
		{"non-string app", func() map[string]any { e := clone(valid); e["app"] = 7; return e }(), "app"},
		{"missing schedule", func() map[string]any { e := clone(valid); delete(e, "schedule"); return e }(), "schedule"},
		{"empty schedule", func() map[string]any { e := clone(valid); e["schedule"] = ""; return e }(), "schedule"},
		{"missing handler", func() map[string]any { e := clone(valid); delete(e, "handler"); return e }(), "handler"},
		{"empty handler", func() map[string]any { e := clone(valid); e["handler"] = ""; return e }(), "handler"},
		{"missing scheduled_at", func() map[string]any { e := clone(valid); delete(e, "scheduled_at"); return e }(), "scheduled_at"},
		{"unparseable scheduled_at", func() map[string]any { e := clone(valid); e["scheduled_at"] = "not-a-date"; return e }(), "scheduled_at"},
		{"non-string scheduled_at", func() map[string]any { e := clone(valid); e["scheduled_at"] = 7; return e }(), "scheduled_at"},
		{"missing occurrence_id", func() map[string]any { e := clone(valid); delete(e, "occurrence_id"); return e }(), "occurrence_id"},
		{"empty occurrence_id", func() map[string]any { e := clone(valid); e["occurrence_id"] = ""; return e }(), "occurrence_id"},
		{"mismatched occurrence_id", func() map[string]any {
			e := clone(valid)
			e["occurrence_id"] = "schedule:other:x:2030-01-01T00:00:00Z"
			return e
		}(), "occurrence_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			occ, kind, err := ClassifyScheduleEnvelope(tt.event)
			if kind != InvalidScheduleEnvelope {
				t.Fatalf("kind = %v, want InvalidScheduleEnvelope", kind)
			}
			if err == nil {
				t.Fatal("expected a non-nil structural error")
			}
			if !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("error = %q, want it to name %q", err.Error(), tt.reason)
			}
			if occ != (Occurrence{}) {
				t.Fatalf("invalid claim returned a non-zero occurrence: %+v", occ)
			}
		})
	}
}

// TestParseEnvelopeRequiresOccurrenceID pins that the raw-JSON decoder enforces
// the same occurrence_id requirement and identity match as the event
// classifier.
func TestParseEnvelopeRequiresOccurrenceID(t *testing.T) {
	o := Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	raw, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if _, err := ParseEnvelope(string(raw)); err != nil {
		t.Fatalf("ParseEnvelope(valid): %v", err)
	}

	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	delete(event, "occurrence_id")
	noID, _ := json.Marshal(event)
	if _, err := ParseEnvelope(string(noID)); err == nil {
		t.Fatal("ParseEnvelope must reject a missing occurrence_id")
	}

	event["occurrence_id"] = "schedule:evil:evil:2030-01-01T00:00:00Z"
	bad, _ := json.Marshal(event)
	if _, err := ParseEnvelope(string(bad)); err == nil {
		t.Fatal("ParseEnvelope must reject a mismatched occurrence_id")
	}
}

// TestEnvelopeClassifyRejectsFractionalTime pins that structural validation
// rejects only a sub-second scheduled_at: cron evaluation is whole-second
// granularity, so a fractional second (e.g. "08:00:00.500Z") can never be a real
// occurrence even when its occurrence_id matches the second-truncated derived
// identity. A nonzero WHOLE second is deliberately accepted structurally:
// historical IANA offsets can carry a seconds component, so a legitimate
// local-minute firing may have a nonzero UTC second; whether it actually fires is
// decided semantically (schedule.IsFiring) against the schedule's timezone.
func TestEnvelopeClassifyRejectsFractionalTime(t *testing.T) {
	valid := claimMap(t)

	fractional := clone(valid)
	fractional["scheduled_at"] = "2026-07-01T08:00:00.500Z"
	// occurrence_id matches what ID derives from the truncated second, so only
	// the whole-second rule can reject this.
	fractional["occurrence_id"] = "schedule:courses:cleanup:2026-07-01T08:00:00Z"
	occ, kind, err := ClassifyScheduleEnvelope(fractional)
	if kind != InvalidScheduleEnvelope {
		t.Fatalf("kind = %v, want InvalidScheduleEnvelope", kind)
	}
	if err == nil {
		t.Fatal("expected a non-nil structural error")
	}
	if !strings.Contains(err.Error(), "scheduled_at") {
		t.Fatalf("error = %q, want it to name scheduled_at", err.Error())
	}
	if occ != (Occurrence{}) {
		t.Fatalf("invalid claim returned a non-zero occurrence: %+v", occ)
	}

	// A nonzero whole second is structurally valid and parses to its exact
	// whole-second instant; semantic timezone validation is not this layer's job.
	nonzero := clone(valid)
	nonzero["scheduled_at"] = "2026-07-01T08:00:30Z"
	nonzero["occurrence_id"] = "schedule:courses:cleanup:2026-07-01T08:00:30Z"
	got, kind, err := ClassifyScheduleEnvelope(nonzero)
	if err != nil || kind != ValidScheduleEnvelope {
		t.Fatalf(
			"classification(nonzero whole second) = (%+v, %v, %v), want valid",
			got, kind, err,
		)
	}
	if !got.ScheduledAt.Equal(time.Date(2026, 7, 1, 8, 0, 30, 0, time.UTC)) {
		t.Fatalf("parsed scheduled_at = %v, want 08:00:30Z preserved", got.ScheduledAt)
	}

	// The exact minute is still a valid claim, so the canonical identity path is
	// unchanged for real occurrences.
	if _, kind, err := ClassifyScheduleEnvelope(valid); err != nil ||
		kind != ValidScheduleEnvelope {
		t.Fatalf("classification(exact minute) = (%v, %v), want valid", kind, err)
	}
}

// TestParseEnvelopeRejectsFractionalScheduledAt pins that the raw-JSON decoder
// applies the same whole-second structural rule as the event classifier: a
// fractional second is rejected even when the occurrence_id matches the
// truncated identity,
// while an exact minute and a nonzero whole second (a possible historical
// timezone firing) both parse.
func TestParseEnvelopeRejectsFractionalScheduledAt(t *testing.T) {
	if _, err := ParseEnvelope(`{"source":"relay.schedule","app":"courses","schedule":"cleanup","handler":"h","scheduled_at":"2026-07-01T08:00:00.500Z","occurrence_id":"schedule:courses:cleanup:2026-07-01T08:00:00Z"}`); err == nil {
		t.Fatal("ParseEnvelope must reject a sub-second scheduled_at")
	}
	for _, raw := range []string{
		`{"source":"relay.schedule","app":"courses","schedule":"cleanup","handler":"h","scheduled_at":"2026-07-01T08:00:00Z","occurrence_id":"schedule:courses:cleanup:2026-07-01T08:00:00Z"}`,
		`{"source":"relay.schedule","app":"courses","schedule":"cleanup","handler":"h","scheduled_at":"2026-07-01T08:00:30Z","occurrence_id":"schedule:courses:cleanup:2026-07-01T08:00:30Z"}`,
	} {
		if _, err := ParseEnvelope(raw); err != nil {
			t.Fatalf("ParseEnvelope(%s): %v", raw, err)
		}
	}
}

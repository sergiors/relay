package schedule

import (
	"encoding/json"
	"testing"
	"time"
)

// Deterministic ID: the exact string is "schedule:<function>:<handler>:<UTC RFC3339>".
func TestOccurrenceIDDeterministic(t *testing.T) {
	inst := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	o := Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: inst}
	want := "schedule:courses:jobs.cleanup.handler:" + inst.Format(time.RFC3339)
	if got := o.ID(); got != want {
		t.Fatalf("ID = %q, want %q", got, want)
	}
}

func jsonUnmarshal(data []byte, into any) error {
	return json.Unmarshal(data, into)
}

// A ScheduledAt in a fixed +02:00 offset (08:00 local) equals the UTC instant
// 06:00:00Z -> the same ID.
func TestOccurrenceIDOffsetIndependent(t *testing.T) {
	rome := time.FixedZone("Rome", 2*3600)
	local := time.Date(2026, 7, 1, 8, 0, 0, 0, rome)
	utc := time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC)
	a := Occurrence{Function: "fn", Handler: "h", ScheduledAt: local}
	b := Occurrence{Function: "fn", Handler: "h", ScheduledAt: utc}
	if a.ID() != b.ID() {
		t.Fatalf("IDs differ for equal instants: %q vs %q", a.ID(), b.ID())
	}
}

// Sub-second truncation: a ScheduledAt with .999ms jitter equals the truncated
// second, so worker-side evaluation jitter never splits an occurrence.
func TestOccurrenceIDTruncatesSubSecond(t *testing.T) {
	whole := time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC)
	jitter := time.Date(2026, 7, 1, 6, 0, 0, 999_000_000, time.UTC)
	a := Occurrence{Function: "fn", Handler: "h", ScheduledAt: whole}
	b := Occurrence{Function: "fn", Handler: "h", ScheduledAt: jitter}
	if a.ID() != b.ID() {
		t.Fatalf("IDs differ across sub-second jitter: %q vs %q", a.ID(), b.ID())
	}
}

// DST stability: the ID depends only on the absolute instant, never on the
// zone representation (an IANA zone vs UTC for the same instant -> same ID).
func TestOccurrenceIDDSTStable(t *testing.T) {
	rome, err := time.LoadLocation("Europe/Rome")
	if err != nil {
		t.Fatalf("load Europe/Rome: %v", err)
	}
	local := time.Date(2026, 7, 1, 8, 0, 0, 0, rome)   // CEST (+02:00)
	utc := time.Date(2026, 7, 1, 6, 0, 0, 0, time.UTC) // the same instant
	a := Occurrence{Function: "fn", Handler: "h", ScheduledAt: local}
	b := Occurrence{Function: "fn", Handler: "h", ScheduledAt: utc}
	if a.ID() != b.ID() {
		t.Fatalf("IDs differ for the same DST instant: %q vs %q", a.ID(), b.ID())
	}
}

// Identity distinguishes occurrences: different instants or different handlers
// never collide.
func TestOccurrenceIDDistinct(t *testing.T) {
	t1 := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	if a, b := (Occurrence{Function: "fn", Handler: "h", ScheduledAt: t1}.ID()),
		(Occurrence{Function: "fn", Handler: "h", ScheduledAt: t2}.ID()); a == b {
		t.Fatalf("distinct instants collided: %q", a)
	}
	if a, b := (Occurrence{Function: "fn", Handler: "h", ScheduledAt: t1}.ID()),
		(Occurrence{Function: "fn", Handler: "h2", ScheduledAt: t1}.ID()); a == b {
		t.Fatalf("distinct handlers collided: %q", a)
	}
	if a, b := (Occurrence{Function: "fn1", Handler: "h", ScheduledAt: t1}.ID()),
		(Occurrence{Function: "fn2", Handler: "h", ScheduledAt: t1}.ID()); a == b {
		t.Fatalf("distinct functions collided: %q", a)
	}
}

// Envelope round-trip: ParseEnvelope(o.Envelope()) reconstructs the same
// Occurrence, with the ID recomputed from the fields.
func TestEnvelopeRoundTrip(t *testing.T) {
	o := Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 500000000, time.UTC)}
	raw, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	got, err := ParseEnvelope(string(raw))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if got.Function != o.Function || got.Handler != o.Handler {
		t.Fatalf("ParseEnvelope = %+v, want function/handler %q/%q", got, o.Function, o.Handler)
	}
	// The parsed scheduled_at is truncated to the second (identity normalization);
	// the recomputed ID must equal the original's.
	if got.ID() != o.ID() {
		t.Fatalf("recomputed ID %q != original %q", got.ID(), o.ID())
	}
	if !got.ScheduledAt.Equal(o.ScheduledAt.UTC().Truncate(time.Second)) {
		t.Fatalf("ScheduledAt = %v, want truncated %v", got.ScheduledAt, o.ScheduledAt.UTC().Truncate(time.Second))
	}
}

// IsScheduleEvent recognizes schedule messages and ignores normal events,
// source-with-missing-fields events, tampered occurrence_id, and invalid dates.
func TestIsScheduleEvent(t *testing.T) {
	o := Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)}
	raw, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	var event map[string]any
	if err := jsonUnmarshal(raw, &event); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	got, ok := IsScheduleEvent(event)
	if !ok {
		t.Fatal("schedule envelope not recognized")
	}
	if got.Function != o.Function || got.Handler != o.Handler {
		t.Fatalf("IsScheduleEvent = %+v, want %q/%q", got, o.Function, o.Handler)
	}

	// A normal event is untouched.
	if _, ok := IsScheduleEvent(map[string]any{"event_name": "INSERT"}); ok {
		t.Fatal("normal event recognized as schedule")
	}

	// source==relay.schedule but missing required fields -> not a schedule.
	for _, missing := range []map[string]any{
		{"source": "relay.schedule", "function": "f", "handler": "h"},
		{"source": "relay.schedule", "function": "f", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": "relay.schedule", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": "relay.schedule", "function": "", "handler": "h", "scheduled_at": "2026-07-01T08:00:00Z"},
		{"source": "relay.schedule", "function": "f", "handler": "h", "scheduled_at": "not-a-date"},
	} {
		if _, ok := IsScheduleEvent(missing); ok {
			t.Fatalf("event with missing/invalid fields recognized as schedule: %+v", missing)
		}
	}

	// A tampered occurrence_id in the envelope does not change the recomputed ID
	// (identity is derived from the fields, never trusted from the wire).
	tampered := make(map[string]any, len(event))
	for k, v := range event {
		tampered[k] = v
	}
	tampered["occurrence_id"] = "schedule:evil:evil:2030-01-01T00:00:00Z"
	got2, ok := IsScheduleEvent(tampered)
	if !ok {
		t.Fatal("tampered envelope not recognized as schedule")
	}
	if got2.ID() != o.ID() {
		t.Fatalf("tampered occurrence_id changed derived identity: got %q, want %q", got2.ID(), o.ID())
	}
}

// Payload exposes exactly source and scheduled_at — never the internal
// coordination fields (function/handler/occurrence_id).
func TestOccurrencePayloadFields(t *testing.T) {
	o := Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)}
	var m map[string]any
	if err := jsonUnmarshal(o.Payload(), &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("payload has %d fields, want exactly 2 (source, scheduled_at): %v", len(m), m)
	}
	if m["source"] != "relay.schedule" {
		t.Fatalf("source = %v, want relay.schedule", m["source"])
	}
	if m["scheduled_at"] != "2026-07-01T08:00:00Z" {
		t.Fatalf("scheduled_at = %v, want 2026-07-01T08:00:00Z", m["scheduled_at"])
	}
	if _, ok := m["function"]; ok {
		t.Fatalf("payload leaks function; internal fields must not be exposed: %v", m)
	}
	if _, ok := m["handler"]; ok {
		t.Fatalf("payload leaks handler; internal fields must not be exposed: %v", m)
	}
	if _, ok := m["occurrence_id"]; ok {
		t.Fatalf("payload leaks occurrence_id; internal fields must not be exposed: %v", m)
	}
}

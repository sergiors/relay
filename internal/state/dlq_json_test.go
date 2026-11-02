package state

import (
	"strings"
	"testing"

	"relay/internal/dlq"
)

// TestDLQDataRoundTripsArbitraryIdentityAndPayloadBytes pins the private JSONB
// representation independently of PostgreSQL: identity, event, replay payload,
// and reason bytes must survive even when they are not valid UTF-8 or contain
// NUL. encoding/json base64-encodes the []byte fields, so JSONB never sees the
// forbidden bytes.
func TestDLQDataRoundTripsArbitraryIdentityAndPayloadBytes(t *testing.T) {
	const lossy = "\x00nul \xff\xfe invalid \n"
	entry := dlq.Entry{
		ID:              "entry",
		Source:          lossy,
		SourceGroup:     "\xffgroup",
		SourceMessageID: "\x00message",
		Consumer:        "\xffworker",
		App:             "fn",
		Handler:         "h",
		Event:           `{"a":1}` + lossy,
		ReplayPayload:   []byte(`{"collectionName":"customers"}` + lossy),
		Reason:          "exhausted" + lossy,
		Deliveries:      7,
		HandlerAttempts: 3,
		Trace:           "trace",
	}

	text, err := marshalDLQData(entry)
	if err != nil {
		t.Fatalf("marshalDLQData: %v", err)
	}
	if strings.ContainsAny(text, "\x00") {
		t.Fatalf("private JSON must not contain raw NUL: %q", text)
	}
	got, err := unmarshalDLQData(entry.ID, text)
	if err != nil {
		t.Fatalf("unmarshalDLQData: %v", err)
	}
	if got.Source != entry.Source || got.SourceGroup != entry.SourceGroup ||
		got.SourceMessageID != entry.SourceMessageID || got.Consumer != entry.Consumer {
		t.Fatalf("identity round-trip mismatch: %+v", got)
	}
	if got.Event != entry.Event || got.Reason != entry.Reason {
		t.Fatalf("payload round-trip mismatch: event=%q reason=%q", got.Event, got.Reason)
	}
	if string(got.ReplayPayload) != string(entry.ReplayPayload) {
		t.Fatalf("ReplayPayload round-trip mismatch: got %q want %q",
			got.ReplayPayload, entry.ReplayPayload)
	}
}

// TestDLQDataRoundTripsAbsentReplayPayloadAsNil pins the no-payload encoding: a
// replayable-attributed entry whose dead-lettering could not derive a payload
// (a semantically invalid schedule claim) stores the replay-payload field
// omitted, and decodes back to an empty (nil) payload — not a fake empty JSON
// document nor the Event body substituted in. The attribution round-trips, so
// the replay refusal for such an entry is the payload check, not a lost
// app/handler.
func TestDLQDataRoundTripsAbsentReplayPayloadAsNil(t *testing.T) {
	entry := dlq.Entry{
		ID:              "invalid-schedule",
		Source:          "events",
		SourceGroup:     "relay",
		SourceMessageID: "1-0",
		App:             "courses",
		Handler:         "jobs.cleanup.handler",
		Event:           `{"source":"relay.schedule","scheduled_at":"2030-02-30T00:00:00Z"}`,
		Reason:          "schedule claim: scheduled_at not a real firing",
		Deliveries:      1,
		HandlerAttempts: 0,
	}

	text, err := marshalDLQData(entry)
	if err != nil {
		t.Fatalf("marshalDLQData: %v", err)
	}
	if strings.Contains(text, "replay_payload") {
		t.Fatalf("an absent replay payload must be omitted, not empty: %q", text)
	}
	got, err := unmarshalDLQData(entry.ID, text)
	if err != nil {
		t.Fatalf("unmarshalDLQData: %v", err)
	}
	if got.ReplayPayload != nil {
		t.Fatalf("ReplayPayload = %q, want nil", got.ReplayPayload)
	}
	if got.Event != entry.Event {
		t.Fatalf("Event = %q, want the diagnostic body preserved", got.Event)
	}
	if !got.Replayable() {
		t.Fatalf("attribution was lost: %+v (a nil payload must not erase app/handler)", got)
	}
}

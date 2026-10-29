package schedule

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// replayOccurrence returns the occurrence the replay tests work with: the same
// app/schedule/handler/instant as the claimMap fixture.
func replayOccurrence() Occurrence {
	return Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// marshalEvent re-encodes a decoded event map into stream-body bytes, so a test
// can perturb a valid envelope and feed ReplayPayload the same way the DLQ
// command would.
func marshalEvent(t *testing.T, event map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}

// TestReplayPayloadValidScheduleEnvelopeIsHandlerPayload pins the replay
// contract for an exhausted schedule invocation: the stored envelope bytes
// resolve to exactly Occurrence.Payload(), the same bytes the live schedule
// path hands the handler, and never the envelope's coordination fields
// (app, schedule, handler, occurrence_id).
func TestReplayPayloadValidScheduleEnvelopeIsHandlerPayload(t *testing.T) {
	occ := replayOccurrence()
	envelope, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}

	payload, err := ReplayPayload(envelope)
	if err != nil {
		t.Fatalf("ReplayPayload(schedule envelope): %v", err)
	}
	if !bytes.Equal(payload, occ.Payload()) {
		t.Fatalf(
			"replay payload = %q, want the live handler payload %q",
			payload, occ.Payload(),
		)
	}

	// The handler payload must carry ONLY the exposed occurrence fields,
	// independent of how Payload renders them.
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("replay payload is not JSON: %v", err)
	}
	if len(fields) != 2 || fields["source"] != "relay.schedule" {
		t.Fatalf("replay payload fields = %+v, want source + scheduled_at only", fields)
	}
	for _, hidden := range []string{"app", "schedule", "handler", "occurrence_id"} {
		if _, ok := fields[hidden]; ok {
			t.Fatalf("replay payload leaked the coordination field %q: %s", hidden, payload)
		}
	}
}

// TestReplayPayloadOrdinaryEventUnchanged pins that an ordinary event (no
// schedule marker) is returned byte for byte: replay must not re-encode,
// normalize field order, or trim whitespace, so the handler receives exactly
// the stored body.
func TestReplayPayloadOrdinaryEventUnchanged(t *testing.T) {
	events := []string{
		`{"event_name":"INSERT","id":7}`,
		// Whitespace and non-lexicographic key order survive verbatim: a
		// re-marshal would collapse this to `{"aaa":2,"zzz":1}` without
		// whitespace.
		"{\n  \"zzz\": 1,\n  \"aaa\": 2\n}\n",
		`{}`,
	}
	for _, event := range events {
		payload, err := ReplayPayload([]byte(event))
		if err != nil {
			t.Fatalf("ReplayPayload(%q): %v", event, err)
		}
		if string(payload) != event {
			t.Fatalf("replay payload = %q, want the event verbatim %q", payload, event)
		}
	}
}

// TestReplayPayloadInvalidJSONFailsExplicitly pins that stored event bytes that
// are not JSON at all are an explicit error, not an unchanged replay: they
// cannot be safely distinguished from a malformed or truncated schedule
// envelope.
func TestReplayPayloadInvalidJSONFailsExplicitly(t *testing.T) {
	for _, event := range []string{
		"",
		"   \n  ",
		"not-json",
		`{"event_name":`,
		`42`,
		`"just a string"`,
	} {
		payload, err := ReplayPayload([]byte(event))
		if err == nil {
			t.Fatalf("ReplayPayload(%q) succeeded with %q, want an explicit error", event, payload)
		}
		if !strings.Contains(err.Error(), "invalid event JSON") {
			t.Fatalf("ReplayPayload(%q) error = %v, want an invalid-JSON error", event, err)
		}
		if payload != nil {
			t.Fatalf("ReplayPayload(%q) returned a payload with its error: %q", event, payload)
		}
	}
}

// TestReplayPayloadNonObjectJSONFailsExplicitly pins that a JSON `null` or an
// array body is rejected rather than replayed: only a JSON object could have
// reached a handler, so anything else is treated as a malformed body.
func TestReplayPayloadNonObjectJSONFailsExplicitly(t *testing.T) {
	// `null` decodes cleanly but to no object; an array fails to decode as a map.
	for _, event := range []string{"null", `[1,2,3]`} {
		payload, err := ReplayPayload([]byte(event))
		if err == nil {
			t.Fatalf("ReplayPayload(%q) succeeded with %q, want an explicit error", event, payload)
		}
		if !strings.Contains(err.Error(), "replay payload") {
			t.Fatalf("ReplayPayload(%q) error = %v, want the replay-payload error", event, err)
		}
		if payload != nil {
			t.Fatalf("ReplayPayload(%q) returned a payload with its error: %q", event, payload)
		}
	}
}

// TestReplayPayloadMalformedScheduleEnvelopeFailsExplicitly pins that a
// marker-present envelope that fails the shared structural validation is an
// explicit error on replay: neither its envelope bytes (the handler has never
// seen those coordination fields) nor a silent repair may be substituted, so the
// entry stays for inspection instead of executing something else.
func TestReplayPayloadMalformedScheduleEnvelopeFailsExplicitly(t *testing.T) {
	valid := claimMap(t)
	tests := []struct {
		name   string
		event  map[string]any
		reason string
	}{
		{"missing app", func() map[string]any { e := clone(valid); delete(e, "app"); return e }(), "app"},
		{"missing handler", func() map[string]any { e := clone(valid); delete(e, "handler"); return e }(), "handler"},
		{"empty schedule", func() map[string]any { e := clone(valid); e["schedule"] = ""; return e }(), "schedule"},
		{"unparseable scheduled_at", func() map[string]any { e := clone(valid); e["scheduled_at"] = "not-a-date"; return e }(), "scheduled_at"},
		{"missing occurrence_id", func() map[string]any { e := clone(valid); delete(e, "occurrence_id"); return e }(), "occurrence_id"},
		{"mismatched occurrence_id", func() map[string]any {
			e := clone(valid)
			e["occurrence_id"] = "schedule:other:evil:2030-01-01T00:00:00Z"
			return e
		}(), "occurrence_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := ReplayPayload(marshalEvent(t, tt.event))
			if err == nil {
				t.Fatalf("ReplayPayload succeeded with %q, want an explicit error", payload)
			}
			if !strings.Contains(err.Error(), "replay schedule payload") {
				t.Fatalf("error = %v, want the replay schedule payload error", err)
			}
			if !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("error = %q, want it to name %q", err.Error(), tt.reason)
			}
			if payload != nil {
				t.Fatalf("ReplayPayload returned a payload with its error: %q", payload)
			}
		})
	}
}

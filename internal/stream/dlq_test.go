package stream

import "testing"

// TestParseDLQEntryCurrentFormat pins the current flat field shape: every field
// is read back exactly, including the two distinct counts.
func TestParseDLQEntryCurrentFormat(t *testing.T) {
	values := map[string]any{
		"original_stream":  "events",
		"original_id":      "1700000000000-0",
		"group":            "relay",
		"consumer":         "worker-1",
		"event":            `{"event_name":"INSERT","id":7}`,
		"reason":           `invocation exhausted: function "fn" handler "index.run" exhausted after 5 handler attempts`,
		"function":         "fn",
		"handler":          "index.run",
		"deliveries":       "7",
		"handler_attempts": "5",
		"timestamp":        "2026-09-23T10:00:00Z",
	}
	e, err := ParseDLQEntry("1700000000000-1", values)
	if err != nil {
		t.Fatalf("ParseDLQEntry: %v", err)
	}
	if e.ID != "1700000000000-1" {
		t.Errorf("ID = %q", e.ID)
	}
	if e.OriginalStream != "events" || e.OriginalID != "1700000000000-0" {
		t.Errorf("original = %q/%q", e.OriginalStream, e.OriginalID)
	}
	if e.Group != "relay" || e.Consumer != "worker-1" {
		t.Errorf("group/consumer = %q/%q", e.Group, e.Consumer)
	}
	if e.Event != `{"event_name":"INSERT","id":7}` {
		t.Errorf("Event = %q", e.Event)
	}
	if e.Function != "fn" || e.Handler != "index.run" {
		t.Errorf("function/handler = %q/%q", e.Function, e.Handler)
	}
	if e.Deliveries != 7 || e.HandlerAttempts != 5 {
		t.Errorf("deliveries/handler_attempts = %d/%d", e.Deliveries, e.HandlerAttempts)
	}
	if e.Reason == "" || e.Timestamp != "2026-09-23T10:00:00Z" {
		t.Errorf("reason/timestamp = %q/%q", e.Reason, e.Timestamp)
	}
}

// TestParseDLQEntryMalformedPlaceholder pins the malformed-message entry: the "-"
// function/handler placeholder and explicit 0 handler_attempts parse normally
// (there is no compatibility reinterpretation).
func TestParseDLQEntryMalformedPlaceholder(t *testing.T) {
	values := map[string]any{
		"original_stream":  "events",
		"original_id":      "1-0",
		"group":            "relay",
		"consumer":         "worker-1",
		"event":            "-",
		"reason":           "decode event: missing 'event' field",
		"function":         dlqNoHandler,
		"handler":          dlqNoHandler,
		"deliveries":       "1",
		"handler_attempts": "0",
		"timestamp":        "2026-09-23T10:00:00Z",
	}
	e, err := ParseDLQEntry("1-1", values)
	if err != nil {
		t.Fatalf("ParseDLQEntry: %v", err)
	}
	if e.Function != "-" || e.Handler != "-" || e.HandlerAttempts != 0 {
		t.Fatalf("placeholder entry = %+v", e)
	}
}

// TestDLQEntryReplayable pins the replayability predicate on the current
// format: an entry that attributes a real function/handler invocation is
// replayable, while the malformed-message placeholder ("-" function/handler)
// is not. There is no reinterpretation of the placeholder.
func TestDLQEntryReplayable(t *testing.T) {
	real := DLQEntry{Function: "fn", Handler: "index.run"}
	if !real.Replayable() {
		t.Fatalf("a real invocation entry must be replayable: %+v", real)
	}
	placeholder := DLQEntry{Function: dlqNoHandler, Handler: dlqNoHandler}
	if placeholder.Replayable() {
		t.Fatalf("the malformed-message placeholder must not be replayable: %+v", placeholder)
	}
	// Either side being the placeholder is enough: a partial invocation has no
	// exact handler to re-execute.
	for _, e := range []DLQEntry{
		{Function: dlqNoHandler, Handler: "index.run"},
		{Function: "fn", Handler: dlqNoHandler},
		{Function: "", Handler: "index.run"},
		{Function: "fn", Handler: ""},
	} {
		if e.Replayable() {
			t.Fatalf("entry %+v must not be replayable", e)
		}
	}
}

// TestParseDLQEntryRejectsMissingField pins that a truncated entry is an error
// naming the field rather than an entry with zero values.
func TestParseDLQEntryRejectsMissingField(t *testing.T) {
	values := map[string]any{
		"original_stream": "events",
		"original_id":     "1-0",
		// group, consumer, event, reason, function, handler, counts, timestamp absent
	}
	if _, err := ParseDLQEntry("1-1", values); err == nil {
		t.Fatal("missing fields must be rejected")
	}
}

// TestParseDLQEntryRejectsBadInteger pins that an unparseable count is an error,
// never silently coerced to 0.
func TestParseDLQEntryRejectsBadInteger(t *testing.T) {
	values := map[string]any{
		"original_stream":  "events",
		"original_id":      "1-0",
		"group":            "relay",
		"consumer":         "worker-1",
		"event":            `{}`,
		"reason":           "boom",
		"function":         "fn",
		"handler":          "index.run",
		"deliveries":       "not-a-number",
		"handler_attempts": "1",
		"timestamp":        "2026-09-23T10:00:00Z",
	}
	if _, err := ParseDLQEntry("1-1", values); err == nil {
		t.Fatal("non-integer deliveries must be rejected")
	}
}

// TestParseDLQEntryRejectsNonStringField pins that a non-string field (a
// defensive guard; real Redis returns strings) is an error.
func TestParseDLQEntryRejectsNonStringField(t *testing.T) {
	values := map[string]any{
		"original_stream":  "events",
		"original_id":      "1-0",
		"group":            "relay",
		"consumer":         "worker-1",
		"event":            `{}`,
		"reason":           42,
		"function":         "fn",
		"handler":          "index.run",
		"deliveries":       "1",
		"handler_attempts": "1",
		"timestamp":        "2026-09-23T10:00:00Z",
	}
	if _, err := ParseDLQEntry("1-1", values); err == nil {
		t.Fatal("non-string reason must be rejected")
	}
}

// TestDLQStreamFor pins the DLQ stream naming convention the store and consumer
// share.
func TestDLQStreamFor(t *testing.T) {
	if got := DLQStreamFor("events"); got != "relay:events:dlq" {
		t.Fatalf("DLQStreamFor(events) = %q", got)
	}
}

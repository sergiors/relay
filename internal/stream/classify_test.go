package stream

import (
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestClassifyMessage(t *testing.T) {
	validEvent := `{"event_id":"1","status":"COMPLETED"}`
	tests := []struct {
		name       string
		values     map[string]any
		wantErr    bool
		wantReason string // required when wantErr; a stable fragment of the error text
	}{
		{"missing event field", map[string]any{}, true, "missing 'event' field"},
		{"event not a string", map[string]any{"event": 42}, true, "'event' field is not a string"},
		{"event invalid JSON", map[string]any{"event": `{oops`}, true, "decode event:"},
		{"event array JSON", map[string]any{"event": `[1,2,3]`}, true, "decode event:"},
		{"event scalar JSON", map[string]any{"event": `"hello"`}, true, "decode event:"},
		{"event null JSON", map[string]any{"event": `null`}, true, "'event' decodes to null"},
		{"event valid object", map[string]any{"event": validEvent}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := classifyMessage(redis.XMessage{ID: "1-0", Values: tt.values})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantReason) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.wantReason)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if event["event_id"] != "1" {
				t.Fatalf("unexpected decoded event: %v", event)
			}
		})
	}
}

func TestDLQPayload(t *testing.T) {
	p := dlqPayload("events", "1-0", "relay", "worker-1", `{"a":1}`, "boom", 3)
	if p["original_stream"] != "events" || p["original_id"] != "1-0" ||
		p["group"] != "relay" || p["consumer"] != "worker-1" ||
		p["event"] != `{"a":1}` || p["reason"] != "boom" || p["attempts"] != int64(3) {
		t.Fatalf("unexpected payload: %v", p)
	}
	// The timestamp is generated inside dlqPayload, so parse it and compare the
	// instant within a small tolerance rather than re-deriving it (a second
	// boundary between the two time.Now calls would fail spuriously).
	ts, ok := p["timestamp"].(string)
	if !ok {
		t.Fatalf("timestamp field is not a string: %v", p["timestamp"])
	}
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatalf("timestamp %q is not RFC3339: %v", ts, err)
	}
	if delta := time.Since(parsed); delta < -5*time.Second || delta > 5*time.Second {
		t.Fatalf("timestamp %q is not near now (delta %s)", ts, delta)
	}
}

// eventString never fails even when the event field is missing or non-string
// (used when building DLQ entries for malformed messages).
func TestEventStringFallback(t *testing.T) {
	if got := eventString(redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"a":1}`}}); got != `{"a":1}` {
		t.Fatalf("expected raw event string, got %q", got)
	}
	if got := eventString(redis.XMessage{ID: "1-0", Values: map[string]any{}}); got != "-" {
		t.Fatalf("expected fallback '-', got %q", got)
	}
	if got := eventString(redis.XMessage{ID: "1-0", Values: map[string]any{"event": 7}}); got != "-" {
		t.Fatalf("expected fallback '-' for non-string, got %q", got)
	}
}

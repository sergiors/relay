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
		name    string
		values  map[string]any
		wantErr bool
	}{
		{"missing event field", map[string]any{}, true},
		{"event not a string", map[string]any{"event": 42}, true},
		{"event invalid JSON", map[string]any{"event": `{oops`}, true},
		{"event array JSON", map[string]any{"event": `[1,2,3]`}, true},
		{"event scalar JSON", map[string]any{"event": `"hello"`}, true},
		{"event null JSON", map[string]any{"event": `null`}, true},
		{"event valid object", map[string]any{"event": validEvent}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := classifyMessage(redis.XMessage{ID: "1-0", Values: tt.values})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
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
	if ts, ok := p["timestamp"].(string); !ok || ts != time.Now().UTC().Format(time.RFC3339) {
		t.Fatalf("timestamp field invalid: %v", p["timestamp"])
	}
}

// TestEventStringFallback verifies eventString never fails even when the event
// field is missing or non-string (used when building DLQ entries for malformed
// messages).
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

func TestNewConsumerDefaults(t *testing.T) {
	c := NewConsumer(ConsumerConfig{Client: redis.NewClient(&redis.Options{}), Stream: "events"})
	if c.maxAttempts != 5 {
		t.Errorf("maxAttempts default = %d, want 5", c.maxAttempts)
	}
	if c.dlqStream != "events:dlq" {
		t.Errorf("dlqStream default = %q, want events:dlq", c.dlqStream)
	}
	if c.minPendingIdle != time.Minute {
		t.Errorf("minPendingIdle default = %s, want 1m", c.minPendingIdle)
	}
	if c.reclaimInterval != 0 {
		t.Errorf("reclaimInterval default = %s, want 0 (disabled)", c.reclaimInterval)
	}
}

func TestConsumerNameInDLQPayload(t *testing.T) {
	// Sanity: the consumer name flows into the DLQ entry, which matters for
	// attribution during a restart scenario.
	p := dlqPayload("events", "1-0", "relay", "consumer-b", "", "err", 1)
	if got := p["consumer"].(string); strings.Contains(got, "consumer-b") == false {
		t.Fatalf("expected consumer name in payload, got %q", got)
	}
}

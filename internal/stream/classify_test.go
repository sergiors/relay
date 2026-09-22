package stream

import (
	"errors"
	"fmt"
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
	// deliveries (Redis/PEL) and handler_attempts (invocation retry state) are
	// distinct, explicit fields. handler_attempts may exceed or trail deliveries
	// depending on reclaim/redelivery, so the payload must never alias them.
	p := dlqPayload("events", "1-0", "relay", "worker-1", `{"a":1}`, "boom", 7, 3)
	if p["original_stream"] != "events" || p["original_id"] != "1-0" ||
		p["group"] != "relay" || p["consumer"] != "worker-1" ||
		p["event"] != `{"a":1}` || p["reason"] != "boom" ||
		p["deliveries"] != int64(7) || p["handler_attempts"] != 3 {
		t.Fatalf("unexpected payload: %v", p)
	}
	// The old `attempts` alias must be gone: it conflated delivery and handler
	// attempt semantics.
	if _, ok := p["attempts"]; ok {
		t.Fatalf("payload must not carry the legacy attempts alias: %v", p)
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

// TestHandlerAttemptsFromError pins the attribution rule: only the runner's
// typed exhaustion error yields a handler attempt count; any other error (a
// malformed-message routing, a plain retryable failure) yields 0 so a delivery
// count is never mistaken for a handler attempt count.
func TestHandlerAttemptsFromError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"typed exhaustion", &HandlerExhaustedError{HandlerAttempts: 3, Err: errors.New("exhausted")}, 3},
		{"wrapped typed exhaustion", fmt.Errorf("outer: %w", &HandlerExhaustedError{HandlerAttempts: 5}), 5},
		{"zero attempt typed exhaustion", &HandlerExhaustedError{}, 0},
		{"plain errors.New", errors.New("decode event: boom"), 0},
		{"nil", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handlerAttemptsFromError(tt.err); got != tt.want {
				t.Fatalf("handlerAttemptsFromError = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestHandlerExhaustedErrorUnwrap pins that the typed exhaustion error keeps
// matching stream.ErrInvocationExhausted through errors.Is (the stream's DLQ
// routing predicate) while also exposing the wrapped cause.
func TestHandlerExhaustedErrorUnwrap(t *testing.T) {
	cause := errors.New("boom")
	err := &HandlerExhaustedError{HandlerAttempts: 2, Err: fmt.Errorf("function %q exhausted: %w", "fn", cause)}
	if !errors.Is(err, ErrInvocationExhausted) {
		t.Fatalf("errors.Is(err, ErrInvocationExhausted) = false, want true")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(err, cause) = false, want true")
	}
	// Multi-error Unwrap ([]error) is consumed by errors.Is/As; errors.Unwrap
	// only follows the single-error form, so assert the exported chain directly.
	unwrapper := any(err).(interface{ Unwrap() []error })
	if got := unwrapper.Unwrap(); len(got) != 2 || !errors.Is(got[0], ErrInvocationExhausted) {
		t.Fatalf("Unwrap() = %v, want [ErrInvocationExhausted, cause]", got)
	}
}

// TestHandlerExhaustedErrorReasonConsistent pins the DLQ `reason` contract for
// both runner construction paths: the reason is the human-readable
// `invocation exhausted: function ... exhausted after N handler attempts: ...`
// form and always names the same attempt count carried by handler_attempts,
// never a bare/machine-only cause, and never a duplicated prefix. The schedule
// path wraps ErrInvocationExhausted inside Err, so it must pass through
// verbatim instead of gaining a second `invocation exhausted:` prefix.
func TestHandlerExhaustedErrorReasonConsistent(t *testing.T) {
	tests := []struct {
		name string
		err  *HandlerExhaustedError
		want int
	}{
		{
			name: "handle path (plain cause, sentinel added by Error)",
			err: &HandlerExhaustedError{
				HandlerAttempts: 5,
				Err: fmt.Errorf("function %q handler %q exhausted after %d handler attempts: %w",
					"fn", "h", 5, errors.New("boom")),
			},
			want: 5,
		},
		{
			name: "schedule path (Err already wraps the sentinel)",
			err: &HandlerExhaustedError{
				HandlerAttempts: 3,
				Err: fmt.Errorf("%w: schedule function %q handler %q exhausted after %d handler attempts",
					ErrInvocationExhausted, "fn", "h", 3),
			},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the DLQ entry exactly as routeToDLQ does, so the assertion
			// covers the reason/handler_attempts pairing that reaches Redis.
			entry := dlqPayload("events", "1-0", "relay", "worker-1", `{"a":1}`,
				tt.err.Error(), 9, handlerAttemptsFromError(tt.err))
			reason, _ := entry["reason"].(string)

			wantPrefix := "invocation exhausted: "
			if !strings.HasPrefix(reason, wantPrefix) {
				t.Fatalf("reason = %q, want prefix %q", reason, wantPrefix)
			}
			if n := strings.Count(reason, "invocation exhausted"); n != 1 {
				t.Fatalf("reason = %q, want exactly one %q prefix, got %d", reason, "invocation exhausted", n)
			}
			wantCount := fmt.Sprintf("exhausted after %d handler attempts", tt.want)
			if !strings.Contains(reason, wantCount) {
				t.Fatalf("reason = %q, want it to report %q", reason, wantCount)
			}
			if entry["handler_attempts"] != tt.want {
				t.Fatalf("handler_attempts = %v, want %d (reason and handler_attempts must agree)",
					entry["handler_attempts"], tt.want)
			}
			if !errors.Is(tt.err, ErrInvocationExhausted) {
				t.Fatalf("errors.Is(err, ErrInvocationExhausted) = false, want true")
			}
		})
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

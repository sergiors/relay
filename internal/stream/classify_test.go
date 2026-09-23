package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// depending on reclaim/redelivery, so the payload must never alias them. The
	// exact function/handler identify the exhausted invocation this entry
	// attributes.
	p := dlqPayload("events", "1-0", "relay", "worker-1", `{"a":1}`, "boom", "fn", "index.run", 7, 3)
	if p["original_stream"] != "events" || p["original_id"] != "1-0" ||
		p["group"] != "relay" || p["consumer"] != "worker-1" ||
		p["event"] != `{"a":1}` || p["reason"] != "boom" ||
		p["function"] != "fn" || p["handler"] != "index.run" ||
		p["deliveries"] != int64(7) || p["handler_attempts"] != 3 {
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

// TestExhaustedInvocationsFromError pins the attribution rule: only the runner's
// typed exhaustion error yields invocation metadata; any other error (a
// malformed-message routing, a plain retryable failure) yields nothing so no
// handler attempt is fabricated. Entries missing a positive attempt count or the
// function/handler identity are dropped defensively.
func TestExhaustedInvocationsFromError(t *testing.T) {
	iv := ExhaustedInvocation{Function: "fn", Handler: "h", Attempts: 3, Err: errors.New("boom")}
	tests := []struct {
		name string
		err  error
		want []ExhaustedInvocation
	}{
		{"typed single", &HandlerExhaustedError{Invocations: []ExhaustedInvocation{iv}}, []ExhaustedInvocation{iv}},
		{"typed multiple", &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{Function: "a", Handler: "h", Attempts: 2},
			{Function: "b", Handler: "h", Attempts: 5},
		}}, []ExhaustedInvocation{
			{Function: "a", Handler: "h", Attempts: 2},
			{Function: "b", Handler: "h", Attempts: 5},
		}},
		{"wrapped typed", fmt.Errorf("outer: %w", &HandlerExhaustedError{Invocations: []ExhaustedInvocation{iv}}), []ExhaustedInvocation{iv}},
		{"empty typed", &HandlerExhaustedError{}, nil},
		{"non-positive attempt dropped", &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{Function: "a", Handler: "h", Attempts: 0},
			{Function: "b", Handler: "h", Attempts: 2},
		}}, []ExhaustedInvocation{{Function: "b", Handler: "h", Attempts: 2}}},
		{"missing identity dropped", &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{Handler: "h", Attempts: 2},
		}}, nil},
		{"plain errors.New", errors.New("decode event: boom"), nil},
		{"nil", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exhaustedInvocationsFromError(tt.err)
			if len(got) != len(tt.want) {
				t.Fatalf("exhaustedInvocationsFromError = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i].Function != tt.want[i].Function || got[i].Handler != tt.want[i].Handler || got[i].Attempts != tt.want[i].Attempts {
					t.Fatalf("exhaustedInvocationsFromError[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestDLQEntrySpecs pins the expansion of the runner's exhaustion signal into
// DLQ entries: one entry per exhausted invocation with exact function/handler/
// attempt/reason, and a single placeholder entry (function/handler "-", 0
// attempts) when the error carries no typed invocation metadata (the malformed
// message path).
func TestDLQEntrySpecs(t *testing.T) {
	t.Run("multiple invocations", func(t *testing.T) {
		err := &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{Function: "fnA", Handler: "index.run", Attempts: 2, Err: errors.New("first")},
			{Function: "fnB", Handler: "jobs.clean", Attempts: 5, Err: errors.New("second")},
		}}
		specs := dlqEntrySpecs(err)
		if len(specs) != 2 {
			t.Fatalf("specs = %d, want 2", len(specs))
		}
		if specs[0].function != "fnA" || specs[0].handler != "index.run" || specs[0].attempts != 2 ||
			specs[0].invocation != "fnA/index.run" || !strings.Contains(specs[0].reason, "first") {
			t.Fatalf("specs[0] = %+v", specs[0])
		}
		if specs[1].function != "fnB" || specs[1].handler != "jobs.clean" || specs[1].attempts != 5 ||
			specs[1].invocation != "fnB/jobs.clean" || !strings.Contains(specs[1].reason, "second") {
			t.Fatalf("specs[1] = %+v", specs[1])
		}
		// Each reason carries exactly one sentinel prefix.
		for _, s := range specs {
			if n := strings.Count(s.reason, "invocation exhausted"); n != 1 {
				t.Fatalf("reason %q has %d sentinels, want 1", s.reason, n)
			}
		}
	})

	t.Run("malformed placeholder", func(t *testing.T) {
		specs := dlqEntrySpecs(errors.New("decode event: boom"))
		if len(specs) != 1 {
			t.Fatalf("specs = %d, want 1 placeholder", len(specs))
		}
		if specs[0].function != dlqNoHandler || specs[0].handler != dlqNoHandler ||
			specs[0].attempts != 0 || specs[0].invocation != "" ||
			specs[0].reason != "decode event: boom" {
			t.Fatalf("placeholder spec = %+v", specs[0])
		}
	})
}

// TestHandlerExhaustedErrorUnwrap pins that the typed exhaustion error keeps
// matching stream.ErrInvocationExhausted through errors.Is (the stream's DLQ
// routing predicate) while also exposing each invocation's wrapped cause.
func TestHandlerExhaustedErrorUnwrap(t *testing.T) {
	causeA := errors.New("boom-a")
	causeB := errors.New("boom-b")
	err := &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
		{Function: "fnA", Handler: "h", Attempts: 2, Err: causeA},
		{Function: "fnB", Handler: "h", Attempts: 4, Err: causeB},
	}}
	if !errors.Is(err, ErrInvocationExhausted) {
		t.Fatalf("errors.Is(err, ErrInvocationExhausted) = false, want true")
	}
	if !errors.Is(err, causeA) || !errors.Is(err, causeB) {
		t.Fatalf("errors.Is(err, cause) = false, want both wrapped causes exposed")
	}
	// Multi-error Unwrap ([]error) is consumed by errors.Is/As; errors.Unwrap
	// only follows the single-error form, so assert the exported chain directly.
	unwrapper := any(err).(interface{ Unwrap() []error })
	got := unwrapper.Unwrap()
	if len(got) != 3 || !errors.Is(got[0], ErrInvocationExhausted) {
		t.Fatalf("Unwrap() = %v, want [ErrInvocationExhausted, causeA, causeB]", got)
	}
}

// TestHandlerExhaustedErrorReasonConsistent pins the DLQ `reason` contract: the
// reason names the exact function/handler and the same attempt count the entry's
// handler_attempts carries, with exactly one `invocation exhausted:` sentinel
// prefix, never a bare/machine-only cause.
func TestHandlerExhaustedErrorReasonConsistent(t *testing.T) {
	tests := []struct {
		name string
		err  *HandlerExhaustedError
		want []int
	}{
		{
			name: "single invocation with cause",
			err: &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
				{Function: "fn", Handler: "h", Attempts: 5, Err: errors.New("boom")},
			}},
			want: []int{5},
		},
		{
			name: "multiple invocations",
			err: &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
				{Function: "fnA", Handler: "h", Attempts: 3},
				{Function: "fnB", Handler: "h", Attempts: 7, Err: errors.New("boom")},
			}},
			want: []int{3, 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the DLQ entries exactly as routeToDLQ does, so the assertion
			// covers the reason/handler_attempts pairing that reaches Redis.
			specs := dlqEntrySpecs(tt.err)
			if len(specs) != len(tt.want) {
				t.Fatalf("specs = %d, want %d", len(specs), len(tt.want))
			}
			for i, spec := range specs {
				if n := strings.Count(spec.reason, "invocation exhausted"); n != 1 {
					t.Fatalf("reason = %q, want exactly one %q prefix, got %d", spec.reason, "invocation exhausted", n)
				}
				if !strings.HasPrefix(spec.reason, "invocation exhausted: ") {
					t.Fatalf("reason = %q, want prefix %q", spec.reason, "invocation exhausted: ")
				}
				wantCount := fmt.Sprintf("exhausted after %d handler attempts", tt.want[i])
				if !strings.Contains(spec.reason, wantCount) {
					t.Fatalf("reason = %q, want it to report %q", spec.reason, wantCount)
				}
				if spec.attempts != tt.want[i] {
					t.Fatalf("spec attempts = %v, want %d (reason and handler_attempts must agree)",
						spec.attempts, tt.want[i])
				}
			}
			if !errors.Is(tt.err, ErrInvocationExhausted) {
				t.Fatalf("errors.Is(err, ErrInvocationExhausted) = false, want true")
			}
		})
	}
}

// TestUnpersistedDLQSpecsIdempotentRetry pins the no-DLQ-scan idempotency: a
// spec whose invocation marker already records a persisted DLQ entry is skipped,
// a spec with no marker (or a read error) is kept, and the malformed placeholder
// is always kept. This is what makes a retry after a partial multi-entry write
// complete only the missing entries.
func TestUnpersistedDLQSpecsIdempotentRetry(t *testing.T) {
	store := newFakeInvocationStore(map[string]string{
		"fnA/index.run": exhaustedValue(2, true),  // already persisted → skip
		"fnC/index.run": exhaustedValue(2, false), // not persisted → keep
	})
	c := newConsumer(ConsumerConfig{
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)

	specs := []dlqEntrySpec{
		{function: "fnA", handler: "index.run", attempts: 2, invocation: "fnA/index.run"},
		{function: "fnB", handler: "index.run", attempts: 5, invocation: "fnB/index.run"}, // absent → keep
		{function: "fnC", handler: "index.run", attempts: 2, invocation: "fnC/index.run"},
	}
	got := c.unpersistedDLQSpecs(context.Background(), "m-0", specs)
	if len(got) != 2 {
		t.Fatalf("unpersisted specs = %+v, want fnB and fnC only", got)
	}
	if got[0].invocation != "fnB/index.run" || got[1].invocation != "fnC/index.run" {
		t.Fatalf("unpersisted specs = %+v, want [fnB, fnC]", got)
	}

	// Once fnC is marked persisted too, only the absent fnB remains.
	if err := store.markExhaustedDLQ(context.Background(), "s", "g", "m-0", "fnC/index.run", 2); err != nil {
		t.Fatalf("markExhaustedDLQ: %v", err)
	}
	got = c.unpersistedDLQSpecs(context.Background(), "m-0", specs)
	if len(got) != 1 || got[0].invocation != "fnB/index.run" {
		t.Fatalf("after marking fnC persisted, specs = %+v, want [fnB]", got)
	}

	// A read error fails safe: every spec is kept so no entry is lost.
	store.readErr = errors.New("redis down")
	got = c.unpersistedDLQSpecs(context.Background(), "m-0", specs)
	if len(got) != len(specs) {
		t.Fatalf("on read error specs = %d, want all %d kept (fail safe)", len(got), len(specs))
	}

	// A spec with no invocation (malformed placeholder) is always kept.
	store.readErr = nil
	ph := []dlqEntrySpec{{function: dlqNoHandler, handler: dlqNoHandler, reason: "boom"}}
	if got := c.unpersistedDLQSpecs(context.Background(), "m-0", ph); len(got) != 1 {
		t.Fatalf("placeholder specs = %+v, want the placeholder kept", got)
	}
}

// TestExhaustedValueGrammar pins the two exhausted marker forms and that only
// the ":dlq" form reports persisted.
func TestExhaustedValueGrammar(t *testing.T) {
	if v := exhaustedValue(3, false); v != "exhausted:3" {
		t.Fatalf("exhaustedValue(3,false) = %q", v)
	}
	if v := exhaustedValue(3, true); v != "exhausted:3:dlq" {
		t.Fatalf("exhaustedValue(3,true) = %q", v)
	}
	// Both parse to kindExhausted with the attempt count; only the suffixed form
	// reports persisted.
	for _, tc := range []struct {
		v         string
		attempts  int
		persisted bool
	}{
		{"exhausted:3", 3, false},
		{"exhausted:3:dlq", 3, true},
	} {
		kind, _, n, ok := parseInvocationState(tc.v)
		if !ok || kind != kindExhausted || n != tc.attempts {
			t.Fatalf("parseInvocationState(%q) = kind %v n %d ok %v", tc.v, kind, n, ok)
		}
		if got := isExhaustedDLQValue(tc.v); got != tc.persisted {
			t.Fatalf("isExhaustedDLQValue(%q) = %v, want %v", tc.v, got, tc.persisted)
		}
	}
	// A malformed suffixed marker does not parse.
	for _, v := range []string{"exhausted::dlq", "exhausted:0:dlq", "exhausted:abc:dlq"} {
		if _, _, _, ok := parseInvocationState(v); ok {
			t.Errorf("parseInvocationState(%q) ok = true, want false", v)
		}
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

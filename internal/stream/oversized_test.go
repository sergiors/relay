package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
)

// TestExtractAndDecodeEventByteCap pins the single extraction+decode seam's byte
// cap: a raw event value exactly at the limit decodes, one byte over is rejected
// as an *EventOversizedError BEFORE any JSON decode (so the decoder never sees
// the oversized payload), and the error carries the measured/configured byte
// counts without quoting the payload.
func TestExtractAndDecodeEventByteCap(t *testing.T) {
	const maxBytes = 64
	exact := `{"x":"` + strings.Repeat("a", maxBytes-8) + `"}`
	if len(exact) != maxBytes {
		t.Fatalf("fixture length = %d, want %d", len(exact), maxBytes)
	}
	if _, err := extractAndDecodeEvent(redis.XMessage{ID: "1-0", Values: map[string]any{"event": exact}}, maxBytes); err != nil {
		t.Fatalf("exact-limit event rejected: %v", err)
	}

	over := `{"x":"` + strings.Repeat("a", maxBytes-7) + `"}`
	if len(over) != maxBytes+1 {
		t.Fatalf("oversized fixture length = %d, want %d", len(over), maxBytes+1)
	}
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": over}}
	_, err := extractAndDecodeEvent(msg, maxBytes)
	var oversize *EventOversizedError
	if !errors.As(err, &oversize) {
		t.Fatalf("err = %v, want *EventOversizedError", err)
	}
	if oversize.Bytes != maxBytes+1 || oversize.MaxBytes != maxBytes {
		t.Fatalf("oversize error = %+v, want Bytes %d MaxBytes %d", oversize, maxBytes+1, maxBytes)
	}
	if !errors.Is(err, ErrEventOversized) {
		t.Fatalf("oversize error must wrap ErrEventOversized")
	}
	if !strings.Contains(err.Error(), "event oversized") {
		t.Fatalf("error %q must carry the stable oversize reason", err)
	}
	if strings.Contains(err.Error(), strings.Repeat("a", 16)) {
		t.Fatalf("error must not quote the payload: %q", err)
	}
}

// TestExtractAndDecodeEventOversizeSkipsDecoder pins that the byte cap is
// applied BEFORE decode even when the oversized value is malformed JSON: the
// returned error is the oversize rejection, never a decode error, proving the
// decoder is not invoked for an over-limit payload.
func TestExtractAndDecodeEventOversizeSkipsDecoder(t *testing.T) {
	const maxBytes = 16
	over := "{" + strings.Repeat("x", maxBytes+8) // not valid JSON, and over the cap
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": over}}

	_, err := extractAndDecodeEvent(msg, maxBytes)
	var oversize *EventOversizedError
	if !errors.As(err, &oversize) {
		t.Fatalf("err = %v, want the oversize rejection (decoder must be skipped)", err)
	}
	if strings.Contains(err.Error(), "decode event") {
		t.Fatalf("error %q must not be a decode error", err)
	}
}

// TestExtractAndDecodeEventPreservesMalformedBehavior pins that a
// missing/non-string/malformed event keeps the ordinary malformed error (never
// the oversize rejection), so the cap changes nothing for under-limit messages.
func TestExtractAndDecodeEventPreservesMalformedBehavior(t *testing.T) {
	const maxBytes = 256
	for _, tc := range []struct {
		name   string
		values map[string]any
		want   string
	}{
		{"missing", map[string]any{}, "missing 'event' field"},
		{"non-string", map[string]any{"event": 42}, "'event' field is not a string"},
		{"invalid JSON", map[string]any{"event": `{oops`}, "decode event:"},
		{"null", map[string]any{"event": `null`}, "'event' decodes to null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extractAndDecodeEvent(redis.XMessage{ID: "1-0", Values: tc.values}, maxBytes)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			var oversize *EventOversizedError
			if errors.As(err, &oversize) {
				t.Fatalf("malformed error %v must not be an oversize error", err)
			}
			// classifyMessage (the convenience wrapper) keeps identical behavior.
			if _, cerr := classifyMessage(redis.XMessage{ID: "1-0", Values: tc.values}); cerr == nil || !strings.Contains(cerr.Error(), tc.want) {
				t.Fatalf("classifyMessage err = %v, want it to contain %q", cerr, tc.want)
			}
		})
	}
}

// TestOversizedEventSummaryBounded pins that the DLQ event override is a small
// diagnostic JSON object carrying the measured bytes and configured max, with
// no part of the oversized payload (and no parsed event_id).
func TestOversizedEventSummaryBounded(t *testing.T) {
	const maxBytes = 262144
	summary := oversizedEventSummary(300000, maxBytes)

	if len(summary) > 256 {
		t.Fatalf("summary length = %d, want a bounded summary", len(summary))
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(summary), &decoded); err != nil {
		t.Fatalf("summary is not JSON: %v", err)
	}
	if decoded["relay_summary"] != "event_oversized" {
		t.Fatalf("summary marker = %v, want event_oversized", decoded["relay_summary"])
	}
	if decoded["event_bytes"] != float64(300000) || decoded["max_event_bytes"] != float64(maxBytes) {
		t.Fatalf("summary counts = %v, want event_bytes=300000 max_event_bytes=%d", decoded, maxBytes)
	}
	if _, ok := decoded["event_id"]; ok {
		t.Fatalf("summary must not parse/extract an event_id: %v", decoded)
	}
}

// TestProcessMessageOversizedRejectsBeforeHandler pins the processing-path
// contract: an oversized raw event routes non-retryably to the DLQ without
// invoking the handler, and increments the unlabeled oversize counter exactly
// once per rejected delivery. The client cannot dial, so routeToDLQ's XADD
// fails and the message is simply left pending; the disposition and counter are
// still observed.
func TestProcessMessageOversizedRejectsBeforeHandler(t *testing.T) {
	m := metrics.New()
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(), Stream: "s", Group: "g", Consumer: "c",
		Log:           slog.New(slog.DiscardHandler),
		Metrics:       m,
		MaxEventBytes: 32,
	}, store)

	overPayloads := map[string]string{
		"ordinary": `{"x":"` + strings.Repeat("a", 64) + `"}`,
		// A schedule-like payload is still rejected before schedule
		// classification: the cap precedes decode entirely.
		"schedule-like": `{"source":"relay.schedule","app":"a","schedule":"s","handler":"h","scheduled_at":"2026-01-01T00:00:00Z","occurrence_id":"` + strings.Repeat("0", 40) + `"}`,
	}
	for name, payload := range overPayloads {
		t.Run(name, func(t *testing.T) {
			handlerRan := false
			c.processMessage(context.Background(),
				redis.XMessage{ID: "m-0", Values: map[string]any{"event": payload}}, 1,
				func(context.Context, string, map[string]any) error {
					handlerRan = true
					return nil
				})
			if handlerRan {
				t.Fatal("handler must not run for an oversized event")
			}
		})
	}

	if got := m.Counter(metrics.MetricEventsOversized); got != int64(len(overPayloads)) {
		t.Fatalf("events_oversized_total = %d, want %d (once per rejected delivery)", got, len(overPayloads))
	}
	// The counter is unlabeled: no payload, message ID, or byte count leaks
	// into the series name.
	snap := m.Snapshot()
	if !strings.Contains(snap, "events_oversized_total count=") {
		t.Fatalf("snapshot missing the oversize counter:\n%s", snap)
	}
	if strings.Contains(snap, "m-0") || strings.Contains(snap, "aaaa") {
		t.Fatalf("oversize counter must be unlabeled; snapshot:\n%s", snap)
	}
}

// TestNewConsumerMaxEventBytesDefault pins the constructor's byte-cap
// normalization: unset, zero, and negative all fall back to the stream default
// (zero must not mean unlimited), and an explicit positive value is honored.
func TestNewConsumerMaxEventBytesDefault(t *testing.T) {
	cfg := func() ConsumerConfig {
		return ConsumerConfig{
			Client: noDialClient(), Stream: "events",
			Log: slog.New(slog.DiscardHandler),
		}
	}
	if got := NewConsumer(cfg()).maxEventBytes; got != DefaultMaxEventBytes {
		t.Fatalf("maxEventBytes = %d, want default %d", got, DefaultMaxEventBytes)
	}
	for _, bad := range []int{0, -1} {
		c := cfg()
		c.MaxEventBytes = bad
		if got := NewConsumer(c).maxEventBytes; got != DefaultMaxEventBytes {
			t.Fatalf("MaxEventBytes=%d maxEventBytes = %d, want default %d", bad, got, DefaultMaxEventBytes)
		}
	}
	c := cfg()
	c.MaxEventBytes = 128
	if got := NewConsumer(c).maxEventBytes; got != 128 {
		t.Fatalf("explicit MaxEventBytes=128 maxEventBytes = %d, want 128", got)
	}
}

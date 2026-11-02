package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/schedule"
)

// noDialClient builds a Redis client that fails any command immediately without
// retrying, so DLQ-routing unit tests exercise the classification/disposition
// without a Redis server and without retry delays.
func noDialClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
}

// errRetryableClaimTest is a plain retryable runner error used to keep a schedule
// message pending (no ACK round trip) while a routing test asserts the dispatch.
var errRetryableClaimTest = errors.New("retryable claim test")

// validScheduleOccurrence is the fixed occurrence the claim/dispatch tests use,
// matching validEnvelope's fixture fields.
func validScheduleOccurrence() schedule.Occurrence {
	return schedule.Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// validEnvelope builds a structurally valid schedule envelope body for the
// fixed occurrence, so tests can perturb individual fields.
func validEnvelope(t *testing.T) map[string]any {
	t.Helper()
	raw, err := validScheduleOccurrence().Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// envelopeBody renders an event map as the raw "value" field payload.
func envelopeBody(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// scheduleMessageValues renders the transport fields a schedule occurrence
// rides on, exactly as the publisher writes them: the envelope JSON as the
// only non-trace field, `value`.
func scheduleMessageValues(t *testing.T, occ schedule.Occurrence) map[string]any {
	t.Helper()
	value, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	return map[string]any{"value": string(value)}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// TestProcessMessageInvalidScheduleEnvelopeNeverDispatched pins the core C3
// integrity rule: an event that claims schedule identity
// (source==relay.schedule) but is structurally incomplete, malformed, or
// carries a malformed/mismatched occurrence_id inside the envelope is NEVER
// handed to ordinary event matching (so it cannot execute an event rule and be
// ACKed as unmatched) and never reaches the schedule runner. It is classified
// as a non-retryable failure and routed to the DLQ on first encounter.
func TestProcessMessageInvalidScheduleEnvelopeNeverDispatched(t *testing.T) {
	valid := validEnvelope(t)
	tests := []struct {
		name string
		body string
	}{
		{"missing app", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			delete(e, "app")
			return e
		}())},
		{"missing schedule", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			delete(e, "schedule")
			return e
		}())},
		{"missing handler", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			delete(e, "handler")
			return e
		}())},
		{"missing scheduled_at", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			delete(e, "scheduled_at")
			return e
		}())},
		{"missing occurrence_id", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			delete(e, "occurrence_id")
			return e
		}())},
		{"unparseable scheduled_at", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			e["scheduled_at"] = "not-a-date"
			return e
		}())},
		{"fractional-second scheduled_at", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			e["scheduled_at"] = "2026-07-01T08:00:00.500Z"
			// Match the second-truncated derived identity so only the
			// whole-second structural rule can reject this claim.
			e["occurrence_id"] = "schedule:courses:cleanup:2026-07-01T08:00:00Z"
			return e
		}())},
		{"mismatched occurrence_id", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			e["occurrence_id"] = "schedule:other:x:2030-01-01T00:00:00Z"
			return e
		}())},
		{"non-string app", envelopeBody(t, func() map[string]any {
			e := cloneMap(valid)
			e["app"] = 7
			return e
		}())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := withSpanRecorder(t)
			// The fake Redis client is never dialed on this path: the invalid
			// claim is DLQ'd before any invocation-state store access.
			store := newFakeInvocationStore(nil)
			c := newConsumer(ConsumerConfig{
				Client: noDialClient(),
				Stream: "s", Group: "g", Consumer: "c",
				Log: slog.New(slog.DiscardHandler),
			}, store)
			scheduleCalls := 0
			c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error {
				scheduleCalls++
				return nil
			}
			handlerCalls := 0
			handler := func(context.Context, string, map[string]any) error {
				handlerCalls++
				return nil
			}

			msg := redis.XMessage{ID: "1-0", Values: map[string]any{"value": tt.body}}
			c.processMessage(context.Background(), msg, 1, handler)

			if handlerCalls != 0 {
				t.Fatalf("ordinary event handler invoked %d times; an invalid schedule claim must never fall through to matching", handlerCalls)
			}
			if scheduleCalls != 0 {
				t.Fatalf("schedule runner invoked %d times; an invalid claim must not be executed", scheduleCalls)
			}
			if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "dlq" {
				t.Fatalf("relay.outcome = %q, want dlq", got)
			}
		})
	}
}

// TestProcessMessageValidScheduleEnvelopeRoutesToRunner pins that a well-formed
// occurrence is still routed to the schedule runner (bypass) and never to the
// ordinary handler, and that the runner now receives the full Occurrence.
func TestProcessMessageValidScheduleEnvelopeRoutesToRunner(t *testing.T) {
	rec := withSpanRecorder(t)
	store := newFakeInvocationStore(nil)

	var gotOcc schedule.Occurrence
	var gotPayload []byte
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	// A retryable error keeps the message pending, so no Redis ACK round trip is
	// needed; the routing itself is what this test asserts.
	c.scheduleRunner = func(_ context.Context, _ string, occ schedule.Occurrence, payload []byte) error {
		gotOcc = occ
		gotPayload = append([]byte(nil), payload...)
		return errRetryableClaimTest
	}

	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: scheduleMessageValues(t, validScheduleOccurrence())}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})

	if handlerCalls != 0 {
		t.Fatalf("ordinary handler invoked for a valid schedule claim; want bypass")
	}
	if gotOcc.App != "courses" || gotOcc.Schedule != "cleanup" || gotOcc.Handler != "jobs.cleanup.handler" {
		t.Fatalf("runner occurrence = %+v", gotOcc)
	}
	if !gotOcc.ScheduledAt.Equal(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("runner scheduled_at = %v", gotOcc.ScheduledAt)
	}
	// The runner receives the handler payload (source + scheduled_at only), never
	// the envelope.
	var p map[string]any
	if err := json.Unmarshal(gotPayload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(p) != 2 || p["source"] != "relay.schedule" {
		t.Fatalf("payload = %v, want the handler payload", p)
	}
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "pending" {
		t.Fatalf("relay.outcome = %q, want pending (retryable runner error)", got)
	}
}

// TestProcessMessageScheduleClaimIgnoresStrayTransportFields pins that the
// schedule claim's transport is value-only: additional entry fields a producer
// may attach — the Debezium sink's `key`, an UPPERCASE header, an unknown name
// — are ignored by extraction and neither invalidate a valid claim nor leak
// anywhere. A structurally valid occurrence with any such fields still routes
// to the schedule runner.
func TestProcessMessageScheduleClaimIgnoresStrayTransportFields(t *testing.T) {
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	scheduleCalls := 0
	var gotOcc schedule.Occurrence
	c.scheduleRunner = func(_ context.Context, _ string, occ schedule.Occurrence, _ []byte) error {
		scheduleCalls++
		gotOcc = occ
		return errRetryableClaimTest
	}

	values := scheduleMessageValues(t, validScheduleOccurrence())
	values["key"] = "schedule:some:other:occurrence"
	values["HEADERKEY"] = "headervalue"
	values["unknown_field"] = "x"
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: values}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})

	if handlerCalls != 0 {
		t.Fatalf("ordinary handler invoked for a schedule claim with stray fields; want bypass")
	}
	if scheduleCalls != 1 {
		t.Fatalf("schedule runner calls = %d, want 1 (stray fields must not invalidate the claim)", scheduleCalls)
	}
	if gotOcc.ID() != validScheduleOccurrence().ID() {
		t.Fatalf("runner occurrence = %+v, want the envelope's own occurrence", gotOcc)
	}
}

// TestProcessMessageNonzeroSecondClaimRoutesToRunner pins the structural/semantic
// split: a schedule claim with a nonzero whole UTC second is structurally valid
// (only a sub-second component is malformed), so it is routed to the schedule
// runner, which is where timezone-aware firing validation decides. It must not be
// dead-lettered at classification.
func TestProcessMessageNonzeroSecondClaimRoutesToRunner(t *testing.T) {
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)

	var gotOcc schedule.Occurrence
	c.scheduleRunner = func(_ context.Context, _ string, occ schedule.Occurrence, _ []byte) error {
		gotOcc = occ
		return errRetryableClaimTest
	}

	// 1866-06-01T11:10:04Z is 12:00 Rome local mean time (UTC+00:49:56): a real
	// historical firing whose UTC second is nonzero.
	occ := validScheduleOccurrence()
	occ.ScheduledAt = time.Date(1866, 6, 1, 11, 10, 4, 0, time.UTC)
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: scheduleMessageValues(t, occ)}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})

	if handlerCalls != 0 {
		t.Fatal("ordinary handler invoked for a nonzero-second schedule claim; want schedule-runner bypass")
	}
	if !gotOcc.ScheduledAt.Equal(time.Date(1866, 6, 1, 11, 10, 4, 0, time.UTC)) {
		t.Fatalf("runner scheduled_at = %v, want 1866-06-01T11:10:04Z", gotOcc.ScheduledAt)
	}
}

// TestProcessMessageNormalEventUnchanged pins that an ordinary external event is
// still handled by the matcher path, even when a ScheduleRunner is wired.
func TestProcessMessageNormalEventUnchanged(t *testing.T) {
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	scheduleCalls := 0
	c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error {
		scheduleCalls++
		return nil
	}
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"value": `{"event_name":"INSERT"}`}}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})
	if handlerCalls != 1 {
		t.Fatalf("ordinary handler calls = %d, want 1", handlerCalls)
	}
	if scheduleCalls != 0 {
		t.Fatalf("schedule runner calls = %d, want 0", scheduleCalls)
	}
}

// TestProcessMessageTombstoneAckedWithoutMatching pins the confirmed-tombstone
// disposition: a tombstone entry carries only the null-value sentinel
// (`default`) in `value` — no transport key involved — and is acknowledged and
// reaches neither the ordinary handler (never matched, never a synthetic
// DELETE) nor the schedule runner.
func TestProcessMessageTombstoneAckedWithoutMatching(t *testing.T) {
	rec := withSpanRecorder(t)
	store := newFakeInvocationStore(nil)
	c := processMessageConsumer(store)
	scheduleCalls := 0
	c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error {
		scheduleCalls++
		return nil
	}
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{
		"value": "default",
	}}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})

	if handlerCalls != 0 || scheduleCalls != 0 {
		t.Fatalf("handler/schedule calls = %d/%d, want 0/0 (a tombstone is ignored)", handlerCalls, scheduleCalls)
	}
	assertAtomicAckOnly(t, store)
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "acked" {
		t.Fatalf("relay.outcome = %q, want acked", got)
	}
}

// TestProcessMessageRawSentinelNotTombstoneIsDeadLettered pins the format=raw
// precedence at the dispatch level: a message whose transport is tombstone-like
// (the sentinel in `value`) but whose `format=raw` marker forces the raw JSON
// decode is a non-retryable decode failure — never acknowledged as a tombstone.
// It is routed to the DLQ as a non-replayable placeholder (an unknown format
// marker behaves the same way).
func TestProcessMessageRawSentinelNotTombstoneIsDeadLettered(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{"format raw", "raw"},
		{"unknown format", "xml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := withSpanRecorder(t)
			store := newFakeInvocationStore(nil)
			c := processMessageConsumer(store)
			handlerCalls := 0
			msg := redis.XMessage{ID: "1-0", Values: map[string]any{
				"format": tt.format,
				"value":  "default",
			}}
			c.processMessage(context.Background(), msg, 1,
				func(context.Context, string, map[string]any) error {
					handlerCalls++
					return nil
				})

			if handlerCalls != 0 {
				t.Fatalf("handler invoked %d times for a format-marked sentinel", handlerCalls)
			}
			assertAtomicAckOnly(t, store)
			entries, ok := c.dlqStore.(*fakeDLQStore)
			if !ok || len(entries.entries) != 1 {
				t.Fatalf("a format-marked sentinel must be dead-lettered once, got %+v", entries)
			}
			entry := entries.entries[0]
			if entry.App != dlq.NoHandler || entry.Handler != dlq.NoHandler {
				t.Fatalf("placeholder attribution = %q/%q", entry.App, entry.Handler)
			}
			if entry.ReplayPayload != nil {
				t.Fatalf("a format-marked sentinel must never carry a replay payload: %q", entry.ReplayPayload)
			}
			if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "dlq" {
				t.Fatalf("relay.outcome = %q, want dlq", got)
			}
		})
	}
}

// TestProcessMessageValidClaimWithoutScheduleRunnerStaysPending pins the
// wiring fallback: a structurally valid schedule message arrives while no
// ScheduleRunner is wired. The envelope decode has already claimed schedule
// identity, so it is neither matched as an ordinary event nor dead-lettered
// (the claim itself is well-formed): it stays pending — no dispatch, no
// migration, no ACK — so a correctly wired consumer processes it.
func TestProcessMessageValidClaimWithoutScheduleRunnerStaysPending(t *testing.T) {
	rec := withSpanRecorder(t)
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	// No scheduleRunner wired.

	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: scheduleMessageValues(t, validScheduleOccurrence())}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})
	if handlerCalls != 0 {
		t.Fatalf("ordinary handler invoked for a valid claim without a schedule runner; want pending")
	}
	if store.recoverableCalls != 0 || store.ackRetainCalls != 0 {
		t.Fatalf("a pending schedule message must not migrate or ACK: %d/%d",
			store.recoverableCalls, store.ackRetainCalls)
	}
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "pending" {
		t.Fatalf("relay.outcome = %q, want pending", got)
	}
}

// TestProcessMessageInvalidClaimWithoutScheduleRunnerStillDLQed pins that a
// malformed schedule claim is dead-lettered even when no ScheduleRunner is
// wired: a structurally invalid claim can never be admitted, not even as an
// ordinary event.
func TestProcessMessageInvalidClaimWithoutScheduleRunnerStillDLQed(t *testing.T) {
	rec := withSpanRecorder(t)
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	// No scheduleRunner wired.

	valid := validEnvelope(t)
	delete(valid, "occurrence_id")
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"value": envelopeBody(t, valid)}}
	c.processMessage(context.Background(), msg, 1, func(context.Context, string, map[string]any) error {
		handlerCalls++
		return nil
	})
	if handlerCalls != 0 {
		t.Fatalf("ordinary handler invoked for an invalid claim without a schedule runner; want DLQ")
	}
	if got := attrString(spanByName(t, rec, "stream.message"), "relay.outcome"); got != "dlq" {
		t.Fatalf("relay.outcome = %q, want dlq", got)
	}
}

// TestProcessScheduleMessageInvalidClaimRoutesToDLQ pins the schedule-path
// disposition for ErrScheduleInvalid: it is terminal and dead-lettered, never
// retried and never ACKed as success, and it is distinct from obsolete (ACKed)
// and not-eligible (pending).rawValue is the transport value a live delivery
// passes; a direct caller may pass "".
func TestProcessScheduleMessageInvalidClaimRoutesToDLQ(t *testing.T) {
	store := newFakeInvocationStore(nil)
	c := newConsumer(ConsumerConfig{
		Client: noDialClient(),
		Stream: "s", Group: "g", Consumer: "c",
		Log: slog.New(slog.DiscardHandler),
	}, store)
	c.scheduleRunner = func(context.Context, string, schedule.Occurrence, []byte) error {
		return ErrScheduleInvalid
	}

	outcome, err := c.processScheduleMessage(
		context.Background(), "9-0", 1, validScheduleOccurrence(), "")
	if outcome != "dlq" {
		t.Fatalf("outcome = %q, want dlq", outcome)
	}
	if err == nil || !strings.Contains(err.Error(), ErrScheduleInvalid.Error()) {
		t.Fatalf("err = %v, want ErrScheduleInvalid", err)
	}
}

// TestProcessScheduleMessageDLQMessagePreservesRawValueOnly pins that the
// schedule DLQ route records the EXACT transport shape the publisher writes:
// the only field is `value` — the original raw envelope (no transport key) —
// while an injected replay payload is separate (see dlqEntry). Without an
// original value, the envelope is reconstructed into the same value-only shape.
func TestProcessScheduleMessageDLQMessagePreservesRawValueOnly(t *testing.T) {
	occ := validScheduleOccurrence()
	raw, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}

	msg := scheduleDLQMessage("9-0", occ, string(raw))
	if len(msg.Values) != 1 {
		t.Fatalf("DLQ message fields = %v, want exactly one `value` field", msg.Values)
	}
	if msg.Values["value"] != string(raw) {
		t.Fatalf("value = %v, want the original raw envelope", msg.Values["value"])
	}

	reconstructed := scheduleDLQMessage("9-0", occ, "")
	if len(reconstructed.Values) != 1 {
		t.Fatalf("reconstructed DLQ fields = %v, want exactly one `value` field",
			reconstructed.Values)
	}
	if reconstructed.Values["value"] != string(raw) {
		t.Fatalf("reconstructed value = %v, want the reconstructed envelope %q",
			reconstructed.Values["value"], string(raw))
	}
}

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

// validEnvelope builds a structurally valid schedule envelope body for a fixed
// occurrence, so tests can perturb individual fields.
func validEnvelope(t *testing.T) map[string]any {
	t.Helper()
	occ := schedule.Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	raw, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// envelopeBody renders an event map as the raw "event" field value.
func envelopeBody(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// TestProcessMessageInvalidScheduleClaimNeverDispatched pins the core C3
// integrity rule: an event that claims schedule identity
// (source==relay.schedule) but is structurally incomplete, malformed, or carries
// a mismatched occurrence_id is NEVER handed to ordinary event matching (so it
// cannot execute an event rule and be ACKed as unmatched) and never reaches the
// schedule runner. It is classified as a non-retryable failure and routed to the
// DLQ on first encounter.
func TestProcessMessageInvalidScheduleClaimNeverDispatched(t *testing.T) {
	valid := validEnvelope(t)
	tests := []struct {
		name  string
		event map[string]any
	}{
		{"missing app", func() map[string]any { e := cloneMap(valid); delete(e, "app"); return e }()},
		{"missing schedule", func() map[string]any { e := cloneMap(valid); delete(e, "schedule"); return e }()},
		{"missing handler", func() map[string]any { e := cloneMap(valid); delete(e, "handler"); return e }()},
		{"missing scheduled_at", func() map[string]any { e := cloneMap(valid); delete(e, "scheduled_at"); return e }()},
		{"missing occurrence_id", func() map[string]any { e := cloneMap(valid); delete(e, "occurrence_id"); return e }()},
		{"unparseable scheduled_at", func() map[string]any { e := cloneMap(valid); e["scheduled_at"] = "not-a-date"; return e }()},
		{"fractional-second scheduled_at", func() map[string]any {
			e := cloneMap(valid)
			e["scheduled_at"] = "2026-07-01T08:00:00.500Z"
			// Match the second-truncated derived identity so only the
			// whole-second structural rule can reject this claim.
			e["occurrence_id"] = "schedule:courses:cleanup:2026-07-01T08:00:00Z"
			return e
		}()},
		{"mismatched occurrence_id", func() map[string]any {
			e := cloneMap(valid)
			e["occurrence_id"] = "schedule:other:x:2030-01-01T00:00:00Z"
			return e
		}()},
		{"non-string app", func() map[string]any { e := cloneMap(valid); e["app"] = 7; return e }()},
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

			msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": envelopeBody(t, tt.event)}}
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

// TestProcessMessageValidScheduleClaimRoutesToRunner pins that a well-formed
// occurrence is still routed to the schedule runner (bypass) and never to the
// ordinary handler, and that the runner now receives the full Occurrence.
func TestProcessMessageValidScheduleClaimRoutesToRunner(t *testing.T) {
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

	valid := validEnvelope(t)
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": envelopeBody(t, valid)}}
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
	nonzero := cloneMap(validEnvelope(t))
	nonzero["scheduled_at"] = "1866-06-01T11:10:04Z"
	nonzero["occurrence_id"] = "schedule:courses:cleanup:1866-06-01T11:10:04Z"
	handlerCalls := 0
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": envelopeBody(t, nonzero)}}
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
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": `{"event_name":"INSERT"}`}}
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

// TestProcessMessageInvalidClaimWithoutScheduleRunnerStillDLQed pins that an
// invalid schedule claim is dead-lettered even when no ScheduleRunner is wired:
// the fallback of treating schedule messages as ordinary events applies only to
// WELL-FORMED claims, never to malformed ones.
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
	msg := redis.XMessage{ID: "1-0", Values: map[string]any{"event": envelopeBody(t, valid)}}
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
// and not-eligible (pending).
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

	occ := schedule.Occurrence{
		App:         "courses",
		Schedule:    "cleanup",
		Handler:     "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
	outcome, err := c.processScheduleMessage(context.Background(), "9-0", 1, occ)
	if outcome != "dlq" {
		t.Fatalf("outcome = %q, want dlq", outcome)
	}
	if err == nil || !strings.Contains(err.Error(), ErrScheduleInvalid.Error()) {
		t.Fatalf("err = %v, want ErrScheduleInvalid", err)
	}
}

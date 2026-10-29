//go:build integration

// F-030 integration coverage against real Redis: the schedule pre-admission
// rejection boundary. A delivery that observed "no admission" (its schedule is
// gone or the occurrence is invalid) must make its terminal rejection atomic
// against a concurrent admission, using the invocation-state hash only:
//
//   - admission-first: a rejection paused BEFORE it is reserved loses to a
//     concurrent admission — it adopts the pinned descriptor and leaves the
//     message pending, never writing a reservation or ACKing/DLQing the live
//     invocation;
//   - rejection-first: the reserved rejection is durable and every later
//     admission is refused, with no descriptor and no active claim ever created;
//   - a reservation written before a crash is completed on the retry (the DLQ
//     row committed to PostgreSQL, then the terminal ACK), and a duplicate
//     disposition never commits a second DLQ row.
//
// The interleavings are deterministic: A is paused by a store wrapper at the
// exact reservation boundary (channels, no sleeps), so B's admission is
// guaranteed to land between A's observation and its disposition.
package stream

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// blockingReserveStore wraps the real Redis-backed store and blocks inside
// reserveScheduleRejection until the test releases it, so the admission-first
// interleaving is deterministic.
type blockingReserveStore struct {
	invocationStateStore
	entered chan struct{}
	proceed chan struct{}
}

func (b *blockingReserveStore) reserveScheduleRejection(
	ctx context.Context,
	stream, group, msgID string,
	kind ScheduleRejection,
) (scheduleRejectionResult, error) {
	close(b.entered)
	<-b.proceed
	return b.invocationStateStore.reserveScheduleRejection(ctx, stream, group, msgID, kind)
}

// rejectionEnv creates a uniquely-named stream/group with one message already in
// the group PEL (so an ACK is observable), and cleans up the stream, its DLQ, and
// any invocation-state keys. It returns the message body too.
func rejectionEnv(t *testing.T, cli *redis.Client) (stream, group, msgID, event string) {
	t.Helper()
	ctx := context.Background()
	prefix := fmt.Sprintf("rej-%d", time.Now().UnixNano())
	stream = prefix + "-stream"
	group = prefix + "-group"
	event = `{"source":"relay.schedule","app":"fn","schedule":"cleanup","handler":"jobs.old","scheduled_at":"2026-07-01T08:00:00Z"}`
	id, err := cli.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": event}}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if err := cli.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		t.Fatalf("xgroup create: %v", err)
	}
	// Read it into the PEL so the message is pending (an ACK removes it).
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "probe", Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_ = cli.Del(c, stream).Err()
		cleanupInvocationKeys(c, cli, prefix)
	})
	return stream, group, id, event
}

// TestIntegrationScheduleRejectionRaceAdmissionFirstStaysPending pins the
// admission-first branch of F-030 on real Redis: A observes no admission and is
// paused before its rejection is reserved; B admits (pins + claims) in the
// meantime; A then adopts the pinned descriptor and stays PENDING — no
// reservation, no DLQ, no ACK, no terminal marker — and B's claim still
// completes.
func TestIntegrationScheduleRejectionRaceAdmissionFirstStaysPending(t *testing.T) {
	cli := testutil.RequireRedis(t)
	stream, group, msgID, _ := rejectionEnv(t, cli)
	ctx := context.Background()
	log := testutil.DiscardLogger()

	realStore := &invocationStore{client: cli}
	bs := &blockingReserveStore{
		invocationStateStore: realStore,
		entered:              make(chan struct{}),
		proceed:              make(chan struct{}),
	}

	type admResult struct {
		adm ScheduleAdmission
		err error
	}
	aCh := make(chan admResult, 1)
	go func() {
		invA := NewInvocationState(ctx, bs, stream, group, msgID, log)
		adm, err := invA.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation)
		aCh <- admResult{adm, err}
	}()

	// A is now paused at the reservation boundary, before it is written.
	<-bs.entered

	// B admits first (a valid proposal) while A is paused.
	invB := NewInvocationState(ctx, realStore, stream, group, msgID, log)
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	admB, err := invB.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("B TryStartScheduled: %v", err)
	}
	if !admB.Started {
		t.Fatalf("B admission = %+v, want started", admB)
	}

	close(bs.proceed)
	a := <-aCh
	if a.err != nil {
		t.Fatalf("A TryStartScheduled: %v", a.err)
	}
	if a.adm.Rejected {
		t.Fatalf("A admission = %+v, an admission-first rejection must be refused", a.adm)
	}
	if a.adm.Started {
		t.Fatal("A must not start a parallel invocation")
	}
	if a.adm.Wait <= 0 {
		t.Fatalf("A admission = %+v, want the adopted (protected) claim, wait>0", a.adm)
	}
	if a.adm.Descriptor != desc {
		t.Fatalf("A adopted descriptor = %+v, want B's %+v", a.adm.Descriptor, desc)
	}

	key := invocationStateKey(stream, group, msgID)
	// No rejection was reserved.
	if _, err := cli.HGet(ctx, key, scheduleRejectionField).Result(); err != redis.Nil {
		t.Fatalf("rejection field read err = %v, want redis.Nil (absent)", err)
	}
	// B's descriptor is pinned and is the only claimed invocation field.
	if v, err := cli.HGet(ctx, key, scheduleField).Result(); err != nil || v == "" {
		t.Fatalf("descriptor field = (%q,%v), want B's pinned descriptor", v, err)
	}
	fields, err := cli.HKeys(ctx, key).Result()
	if err != nil {
		t.Fatalf("hkeys: %v", err)
	}
	for _, f := range fields {
		if f != scheduleField && f != "fn/jobs.old" {
			t.Fatalf("unexpected invocation field %q (a parallel handler was claimed)", f)
		}
	}
	// B's claim still completes (A did not clobber it).
	if !invB.MarkComplete("fn/jobs.old", admB.Claim) {
		t.Fatal("B MarkComplete = false, want true (A must not terminalize B's claim)")
	}

	// No terminal marker, the message is still pending (never ACKed), and nothing
	// was dead-lettered.
	if n, err := cli.HExists(ctx, key, terminalField).Result(); err != nil || n {
		t.Fatalf("terminal marker = (%v,%v), want absent", n, err)
	}
	pending, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: "-", End: "+", Count: 10}).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != msgID {
		t.Fatalf("pending = %+v, want the message still pending (never ACKed)", pending)
	}
	// No DLQ row exists: the race produced no dead letter. There is no store to
	// assert (this branch never wires one), and with a nil DLQ store the route
	// refuses to ACK at all, so pending is the observable invariant.
}

// TestIntegrationScheduleRejectionFirstRefusesAdmission pins the rejection-first
// branch: the reservation is written first, then a valid proposal is refused
// without pinning a descriptor or claiming an invocation, and the reserved kind
// is the durable authority.
func TestIntegrationScheduleRejectionFirstRefusesAdmission(t *testing.T) {
	cli := testutil.RequireRedis(t)
	stream, group, msgID, _ := rejectionEnv(t, cli)
	ctx := context.Background()
	store := &invocationStore{client: cli}
	inv := NewInvocationState(ctx, store, stream, group, msgID, testutil.DiscardLogger())

	// A rejects (invalid) with no proposal: the reservation is written.
	adm, err := inv.TryStartScheduled(ScheduleDescriptor{}, ScheduleRejectionInvalid, seamInvocation)
	if err != nil {
		t.Fatalf("A TryStartScheduled: %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("A admission = %+v, want rejected(invalid)", adm)
	}

	key := invocationStateKey(stream, group, msgID)
	if v, err := cli.HGet(ctx, key, scheduleRejectionField).Result(); err != nil || v != scheduleRejectionInvalidValue {
		t.Fatalf("rejection field = (%q,%v), want %q", v, err, scheduleRejectionInvalidValue)
	}
	// Invariant: no descriptor pinned and no active claim.
	if n, err := cli.HExists(ctx, key, scheduleField).Result(); err != nil || n {
		t.Fatalf("descriptor field present = (%v,%v), want absent", n, err)
	}
	if n, err := cli.HExists(ctx, key, "fn/jobs.old").Result(); err != nil || n {
		t.Fatalf("invocation field present = (%v,%v), want absent (no active claim)", n, err)
	}

	// B proposes a valid descriptor: refused, never claims, never pins.
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	admB, err := inv.TryStartScheduled(desc, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("B TryStartScheduled: %v", err)
	}
	if !admB.Rejected || admB.Rejection != ScheduleRejectionInvalid {
		t.Fatalf("B admission = %+v, want rejected(invalid) — the reservation must win", admB)
	}
	if admB.Started {
		t.Fatal("an admission after a reservation must never start")
	}
	if n, err := cli.HExists(ctx, key, scheduleField).Result(); err != nil || n {
		t.Fatalf("descriptor field present = (%v,%v), want absent", n, err)
	}
	if n, err := cli.HExists(ctx, key, "fn/jobs.old").Result(); err != nil || n {
		t.Fatalf("invocation field present = (%v,%v), want absent", n, err)
	}
}

// TestIntegrationScheduleReservedInvalidDispositionIsIdempotent pins the
// reservation-interrupted/retry contract: a reservation written by a delivery
// that crashed BEFORE its DLQ write is completed by a later delivery — the DLQ
// placeholder is persisted and only THEN is the message terminal-ACKed — and a
// duplicate disposition appends no second DLQ entry.
func TestIntegrationScheduleReservedInvalidDispositionIsIdempotent(t *testing.T) {
	cli := testutil.RequireRedis(t)
	stream, group, msgID, event := rejectionEnv(t, cli)
	ctx := context.Background()
	store := &invocationStore{client: cli}
	// The durable DLQ store the disposition commits to (production wiring).
	pg := testutil.NewState(t)

	// Simulate the crashed delivery: the INVALID rejection is reserved, but no DLQ
	// row was written and the message is still pending.
	if _, err := store.reserveScheduleRejection(ctx, stream, group, msgID, ScheduleRejectionInvalid); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	c := NewConsumer(ConsumerConfig{
		Client: cli, Stream: stream, Group: group, Consumer: "c",
		Log:      testutil.DiscardLogger(),
		DLQStore: pg,
	})
	msg := redis.XMessage{ID: msgID, Values: map[string]any{"event": event}}

	// The "retry" completes the disposition: the DLQ row is committed to
	// PostgreSQL and only then is the message terminal-ACKed.
	c.routeToDLQ(ctx, msg, fmt.Errorf("%w: reserved", ErrScheduleInvalid), 1)

	key := invocationStateKey(stream, group, msgID)
	entries := dlqEntriesFor(t, pg, msgID)
	if len(entries) != 1 {
		t.Fatalf("DLQ rows = %d, want exactly 1", len(entries))
	}
	if entries[0].App != dlq.NoHandler || entries[0].Handler != dlq.NoHandler {
		t.Fatalf("placeholder row attribution = %q/%q", entries[0].App, entries[0].Handler)
	}
	if n, err := cli.HExists(ctx, key, terminalField).Result(); err != nil || !n {
		t.Fatalf("terminal marker = (%v,%v), want present (terminal ACK)", n, err)
	}
	// A reserved-rejection disposition has no active claim to preserve.
	if n, err := cli.HExists(ctx, key, "fn/jobs.old").Result(); err != nil || n {
		t.Fatalf("invocation field present = (%v,%v), want absent (no active claim)", n, err)
	}
	pending, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: "-", End: "+", Count: 10}).Result()
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending = (%+v,%v), want none (ACKed)", pending, err)
	}
	// The confirmed ACK settles the placeholder delivery.
	if entries[0].SettledAt == "" {
		t.Fatalf("the placeholder row must be settled after the confirmed ACK")
	}

	// A duplicate disposition must not commit a second row (the stable identity
	// is idempotent in PostgreSQL, with no Redis persistence marker at all).
	c.routeToDLQ(ctx, msg, fmt.Errorf("%w: reserved", ErrScheduleInvalid), 2)
	if entries := dlqEntriesFor(t, pg, msgID); len(entries) != 1 {
		t.Fatalf("DLQ rows after duplicate disposition = %d, want still 1", len(entries))
	}
}

// TestIntegrationScheduleReservedObsoleteAckedOnRetry pins the obsolete half of
// the retry contract: a reserved OBSOLETE rejection is ACKed (and terminal-
// retained) on the retry, never dead-lettered.
func TestIntegrationScheduleReservedObsoleteAckedOnRetry(t *testing.T) {
	cli := testutil.RequireRedis(t)
	stream, group, msgID, _ := rejectionEnv(t, cli)
	ctx := context.Background()
	store := &invocationStore{client: cli}
	pg := testutil.NewState(t)
	inv := NewInvocationState(ctx, store, stream, group, msgID, testutil.DiscardLogger())

	// A reserved obsolete; a later delivery reports the same kind, so the stream
	// ACKs (here modelled by the same atomic ack+retain the obsolete path uses).
	if _, err := store.reserveScheduleRejection(ctx, stream, group, msgID, ScheduleRejectionObsolete); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	adm, err := inv.TryStartScheduled(ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour}, ScheduleRejectionNone, seamInvocation)
	if err != nil {
		t.Fatalf("TryStartScheduled: %v", err)
	}
	if !adm.Rejected || adm.Rejection != ScheduleRejectionObsolete {
		t.Fatalf("admission = %+v, want rejected(obsolete)", adm)
	}
	if _, err := store.ackAndRetainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err != nil {
		t.Fatalf("ackAndRetainTerminal: %v", err)
	}
	key := invocationStateKey(stream, group, msgID)
	if n, err := cli.HExists(ctx, key, terminalField).Result(); err != nil || !n {
		t.Fatalf("terminal marker = (%v,%v), want present", n, err)
	}
	if n, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: group, Start: "-", End: "+", Count: 10}).Result(); err != nil || len(n) != 0 {
		t.Fatalf("pending = (%+v,%v), want none (ACKed)", n, err)
	}
	if entries := dlqEntriesFor(t, pg, msgID); len(entries) != 0 {
		t.Fatalf("DLQ rows = %d, want 0 (obsolete is never dead-lettered)", len(entries))
	}
}

// TestIntegrationReserveScheduleRejectionAdmittedMarkerWithoutDescriptor pins
// the defensive legacy branch against real Redis: an invocation marker with no
// pinned descriptor means the occurrence was admitted, so the rejection
// reservation scan writes NOTHING and never overwrites the marker. The reported
// disposition follows the marker: ACTIVE (running/backoff) → in-flight (pending),
// TERMINAL "ok" → complete (ACK), TERMINAL exhausted → exhausted with the
// still-owed invocation, and an active marker wins over a terminal one.
func TestIntegrationReserveScheduleRejectionAdmittedMarkerWithoutDescriptor(t *testing.T) {
	type want struct {
		outcome scheduleRejectionOutcome
		markers int
		inv     string
		attempt int
	}
	active := runningValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "ab"})
	cases := map[string]struct {
		fields map[string]string
		want   want
	}{
		"running": {
			fields: map[string]string{"fn/jobs.old": active},
			want:   want{outcome: scheduleRejectionInFlight},
		},
		"next_attempt": {
			fields: map[string]string{"fn/jobs.old": nextAttemptValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "ab"})},
			want:   want{outcome: scheduleRejectionInFlight},
		},
		"complete": {
			fields: map[string]string{"fn/jobs.old": "ok"},
			want:   want{outcome: scheduleRejectionComplete},
		},
		"exhausted": {
			fields: map[string]string{"fn/jobs.old": exhaustedValue(InvocationClaim{Attempt: 2, Token: "ab"}, false)},
			want:   want{outcome: scheduleRejectionExhausted, markers: 1, inv: "fn/jobs.old", attempt: 2},
		},
		"exhausted dlq": {
			fields: map[string]string{"fn/jobs.old": exhaustedValue(InvocationClaim{Attempt: 3, Token: "ab"}, true)},
			want:   want{outcome: scheduleRejectionExhausted, markers: 1, inv: "fn/jobs.old", attempt: 3},
		},
		"active wins over terminal": {
			fields: map[string]string{"fn/jobs.old": active, "fn/jobs.new": "ok"},
			want:   want{outcome: scheduleRejectionInFlight},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cli := testutil.RequireRedis(t)
			stream, group, msgID, _ := rejectionEnv(t, cli)
			ctx := context.Background()
			store := &invocationStore{client: cli}
			key := invocationStateKey(stream, group, msgID)

			// Seed legacy invocation marker(s) with NO __schedule descriptor.
			for field, marker := range tc.fields {
				if err := cli.HSet(ctx, key, field, marker).Err(); err != nil {
					t.Fatalf("seed marker: %v", err)
				}
			}

			res, err := store.reserveScheduleRejection(ctx, stream, group, msgID, ScheduleRejectionObsolete)
			if err != nil {
				t.Fatalf("reserve: %v", err)
			}
			if res.outcome != tc.want.outcome {
				t.Fatalf("reserve = %+v, want outcome %v", res, tc.want.outcome)
			}
			if len(res.markers) != tc.want.markers {
				t.Fatalf("reserve markers = %+v, want %d", res.markers, tc.want.markers)
			}
			if tc.want.markers == 1 && (res.markers[0].Invocation != tc.want.inv || res.markers[0].Attempt != tc.want.attempt) {
				t.Fatalf("reserve marker = %+v, want %s attempt %d", res.markers[0], tc.want.inv, tc.want.attempt)
			}
			if n, err := cli.HExists(ctx, key, scheduleRejectionField).Result(); err != nil || n {
				t.Fatalf("rejection field present = (%v,%v), want absent (an admitted marker must not be overwritten)", n, err)
			}
			// Every marker is untouched.
			for field, marker := range tc.fields {
				if v, err := cli.HGet(ctx, key, field).Result(); err != nil || v != marker {
					t.Fatalf("marker %q = (%q,%v), want the seeded %q", field, v, err, marker)
				}
			}
		})
	}
}

// TestIntegrationReserveScheduleRejectionPinnedMarker pins the descriptor-pinned
// branch against real Redis: a pinned descriptor (a current-version delivery that
// pinned + claimed, then completed/exhausted but crashed before its ACK/DLQ) is
// returned for adoption together with its invocation marker's classification, so
// an app-absent caller can complete the terminal disposition. The marker is never
// overwritten and no reservation is written.
func TestIntegrationReserveScheduleRejectionPinnedMarker(t *testing.T) {
	desc := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	encoded := encodeScheduleDescriptor(desc)

	cases := map[string]struct {
		marker     string
		wantKind   scheduleMarkerKind
		wantMarker int
	}{
		"active":    {runningValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "ab"}), scheduleMarkerActive, 0},
		"complete":  {"ok", scheduleMarkerComplete, 0},
		"exhausted": {exhaustedValue(InvocationClaim{Attempt: 2, Token: "ab"}, false), scheduleMarkerExhausted, 1},
		"no marker": {"", scheduleMarkerNone, 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cli := testutil.RequireRedis(t)
			stream, group, msgID, _ := rejectionEnv(t, cli)
			ctx := context.Background()
			store := &invocationStore{client: cli}
			key := invocationStateKey(stream, group, msgID)

			if err := cli.HSet(ctx, key, scheduleField, encoded).Err(); err != nil {
				t.Fatalf("seed descriptor: %v", err)
			}
			if tc.marker != "" {
				if err := cli.HSet(ctx, key, "fn/jobs.old", tc.marker).Err(); err != nil {
					t.Fatalf("seed marker: %v", err)
				}
			}

			res, err := store.reserveScheduleRejection(ctx, stream, group, msgID, ScheduleRejectionObsolete)
			if err != nil {
				t.Fatalf("reserve: %v", err)
			}
			if res.outcome != scheduleRejectionAdmitted || !res.hasPinned {
				t.Fatalf("reserve = %+v, want admitted with a pinned descriptor", res)
			}
			if res.pinned != desc {
				t.Fatalf("pinned = %+v, want %+v", res.pinned, desc)
			}
			if res.markerKind != tc.wantKind {
				t.Fatalf("markerKind = %v, want %v", res.markerKind, tc.wantKind)
			}
			if len(res.markers) != tc.wantMarker {
				t.Fatalf("markers = %+v, want %d", res.markers, tc.wantMarker)
			}
			if tc.wantMarker == 1 && (res.markers[0].Invocation != "fn/jobs.old" || res.markers[0].Attempt != 2) {
				t.Fatalf("marker = %+v, want fn/jobs.old attempt 2", res.markers[0])
			}
			if n, err := cli.HExists(ctx, key, scheduleRejectionField).Result(); err != nil || n {
				t.Fatalf("rejection field present = (%v,%v), want absent", n, err)
			}
			if tc.marker != "" {
				if v, err := cli.HGet(ctx, key, "fn/jobs.old").Result(); err != nil || v != tc.marker {
					t.Fatalf("marker = (%q,%v), want untouched %q", v, err, tc.marker)
				}
			}
		})
	}
}

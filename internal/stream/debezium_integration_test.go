//go:build integration

// This file exercises the Debezium boundary end to end against a real Redis
// server and PostgreSQL, with stream entries whose transport fields mirror the
// official Debezium Server 3.7 Redis sink "extended" format: the record value
// in `value`, and (optionally) the record `key` and UPPERCASE header fields
// that external producers may still attach. Relay's extraction is value-only:
// every other field is ignored and cannot change decoding. It pins routing of
// schema-enabled and schemaless MongoDB/PostgreSQL changes, snapshot reads,
// tombstones (acknowledged from the value sentinel alone, never matched),
// format=raw overrides, and the durable DLQ replay payload each dead-lettered
// entry stores.
//
// This file is excluded from the default suite by the integration build tag;
// running it REQUIRES Redis at REDIS_TEST_ADDR (default localhost:6379) and
// PostgreSQL at TEST_DATABASE_URL, failing rather than skipping when either is
// absent.
package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/app"
	"relay/internal/event"
	"relay/internal/testutil"
)

// dbzSchemaValue renders the schema-enabled record value shape
// (debezium.format.*.schemas.enable=true): the payload nested under a schema.
func dbzSchemaValue(t *testing.T, payload map[string]any) string {
	t.Helper()
	return marshalDBZ(t, map[string]any{
		"schema": map[string]any{
			"type": "struct", "name": "dbz.inventory.Envelope", "optional": false,
			"fields": []any{
				map[string]any{"type": "struct", "field": "before", "optional": true},
				map[string]any{"type": "struct", "field": "after", "optional": true},
				map[string]any{"type": "string", "field": "op", "optional": false},
			},
		},
		"payload": payload,
	})
}

// dbzSchemaKey renders the schema-enabled record key shape.
func dbzSchemaKey(t *testing.T, payload map[string]any) string {
	t.Helper()
	return marshalDBZ(t, map[string]any{
		"schema": map[string]any{
			"type": "struct", "name": "dbz.inventory.customers.Key", "optional": false,
			"fields": []any{map[string]any{"field": "id", "optional": false, "type": "int32"}},
		},
		"payload": payload,
	})
}

// dbzPostgresSource renders a realistic PostgreSQL source block.
func dbzPostgresSource(table string) map[string]any {
	return map[string]any{
		"version": "3.7.0.Final", "connector": "postgresql", "name": "dbz",
		"ts_ms": 1767200000000, "snapshot": "false", "db": "inventory",
		"lsn": 35451236, "txid": 762, "schema": "public", "table": table,
	}
}

// dbzChange renders a Debezium Envelope payload.
func dbzChange(op string, before, after any, source map[string]any) map[string]any {
	return map[string]any{
		"before": before, "after": after, "source": source,
		"op": op, "ts_ms": 1767200000100, "transaction": nil,
	}
}

// dbzCustomersRow is one inventory.customers row image.
func dbzCustomersRow(id int, email string) map[string]any {
	return map[string]any{"id": id, "email": email}
}

// dbzMongoSource renders a realistic MongoDB source block.
func dbzMongoSource(collection string) map[string]any {
	return map[string]any{
		"version": "3.7.0.Final", "connector": "mongodb", "name": "dbz",
		"ts_ms": 1767200000000, "snapshot": "last", "db": "inventory",
		"rs": "rs0", "collection": collection, "ord": 42, "stage": "RS0_8012",
	}
}

// dbzMongoChange renders a MongoDB change payload whose images are the JSON
// strings the connector serializes.
func dbzMongoChange(op string, before, after any, collection string) map[string]any {
	return map[string]any{
		"before": before, "after": after, "source": dbzMongoSource(collection),
		"op": op, "ts_ms": 1767200000100, "transaction": nil,
	}
}

// dbzOrderDoc is one orders document with Extended JSON wrappers.
func dbzOrderDoc(oid string) map[string]any {
	return map[string]any{
		"_id":       map[string]any{"$oid": oid},
		"orderdate": map[string]any{"$date": "2026-01-17T10:33:00Z"},
		"total":     map[string]any{"$numberDecimal": "112.50"},
		"tags":      []any{"rush", map[string]any{"$oid": "663c9c0f00000000000007c5"}},
	}
}

// dbzOrderDocExpected is the NORMALIZED expected form of dbzOrderDoc: the
// Extended JSON wrappers are normalized — ObjectIds unwrap to strings, dates
// become RFC3339 UTC strings, and $numberDecimal survives untouched.
func dbzOrderDocExpected(oid string) map[string]any {
	return map[string]any{
		"_id":       oid,
		"orderdate": "2026-01-17T10:33:00Z",
		"total":     map[string]any{"$numberDecimal": "112.50"},
		"tags":      []any{"rush", "663c9c0f00000000000007c5"},
	}
}

// dbzImage renders a JSON-string document image as the connector does.
func dbzImage(t *testing.T, doc map[string]any) string {
	t.Helper()
	return marshalDBZ(t, doc)
}

// marshalDBZ renders a fixture as compact canonical JSON.
func marshalDBZ(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return string(b)
}

// expectEventJSON marshals an expectation for deep JSON comparison.
func expectEventJSON(t *testing.T, want any) string {
	t.Helper()
	return marshalDBZ(t, want)
}

// xaddDebezium appends a Debezium-extended stream entry with the given
// transport fields: the record `value` plus arbitrary additional fields the
// external sink may write (`key`, headers) — ignored by Relay's extraction.
func (e *testEnv) xaddDebezium(
	t *testing.T, key, value string, headers map[string]any,
) string {
	t.Helper()
	values := map[string]any{"value": value}
	if key != "" {
		values["key"] = key
	}
	for k, v := range headers {
		values[k] = v
	}
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream, Values: values,
	}).Result()
	if err != nil {
		t.Fatalf("xaddDebezium: %v", err)
	}
	return id
}

// TestIntegrationDebeziumPostgresCreateRoutesNormalizedEvent pins the full
// schema-enabled PostgreSQL path: the entry (the record `key` riding beside
// `value`/headers, exactly as the sink writes, ignored by Relay) is normalized
// to the four exposed fields with the table as the collection name, handed to
// the matcher, and acknowledged.
func TestIntegrationDebeziumPostgresCreateRoutesNormalizedEvent(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	value := dbzSchemaValue(t, dbzChange("c", nil,
		dbzCustomersRow(1, "sally.thomas@acme.com"), dbzPostgresSource("customers")))
	want := expectEventJSON(t, map[string]any{
		"collectionName": "customers",
		"recordType":     "INSERT",
		"newImage":       dbzCustomersRow(1, "sally.thomas@acme.com"),
		"oldImage":       nil,
	})

	id := e.xaddDebezium(t, dbzSchemaKey(t, map[string]any{"id": 1}), value,
		map[string]any{"HEADERKEY": "headervalue"})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if got := marshalDBZ(t, ev); got != want {
			t.Errorf("handler event = %s, want the normalized %s", got, want)
		}
		return nil
	})
	testutil.WaitFor(t, 8*time.Second, "CDC create event handled", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

// TestIntegrationDebeziumUpdateAndSnapshotRead pin the update (u) and snapshot
// read (r) normalizations against real transports: both images travel to the
// handler on update, and a snapshot read is delivered as an INSERT.
func TestIntegrationDebeziumUpdateAndSnapshotRead(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	schemaless := func(payload map[string]any) string {
		return marshalDBZ(t, payload)
	}
	change := func(op string, before, after any) map[string]any {
		return dbzChange(op, before, after, dbzPostgresSource("orders"))
	}
	events := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "update",
			value: schemaless(change("u", dbzCustomersRow(11, "old@acme.com"), dbzCustomersRow(11, "new@acme.com"))),
			want: expectEventJSON(t, map[string]any{
				"collectionName": "orders", "recordType": "UPDATE",
				"newImage": dbzCustomersRow(11, "new@acme.com"),
				"oldImage": dbzCustomersRow(11, "old@acme.com"),
			}),
		},
		{
			name:  "snapshot-read",
			value: schemaless(change("r", nil, dbzCustomersRow(12, "fred@acme.com"))),
			want: expectEventJSON(t, map[string]any{
				"collectionName": "orders", "recordType": "INSERT",
				"newImage": dbzCustomersRow(12, "fred@acme.com"), "oldImage": nil,
			}),
		},
		{
			name:  "delete",
			value: schemaless(change("d", dbzCustomersRow(13, "kate@acme.com"), nil)),
			want: expectEventJSON(t, map[string]any{
				"collectionName": "orders", "recordType": "DELETE",
				"newImage": nil,
				"oldImage": dbzCustomersRow(13, "kate@acme.com"),
			}),
		},
	}

	ids := make(map[string]string, len(events)) // entry id -> want
	for _, ev := range events {
		id := e.xaddDebezium(t, `{"payload":{"id":1}}`, ev.value, nil)
		ids[id] = ev.want
	}
	// Handler workers run on separate goroutines and the waitFor predicate polls
	// from the test goroutine, so the completion record is mutex-guarded.
	var handledMu sync.Mutex
	handled := make(map[string]bool, len(events))
	handledCount := func() int {
		handledMu.Lock()
		defer handledMu.Unlock()
		return len(handled)
	}
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		want, ok := ids[msgID]
		if !ok {
			return nil
		}
		if got := marshalDBZ(t, ev); got != want {
			t.Errorf("handler event = %s, want %s", got, want)
		}
		handledMu.Lock()
		handled[msgID] = true
		handledMu.Unlock()
		return nil
	})
	testutil.WaitFor(t, 8*time.Second, "all CDC operations handled", func() bool {
		return handledCount() == len(events)
	})
	e.stop(t)
	if got := handledCount(); got != len(events) {
		t.Fatalf("handled %d of %d CDC events", got, len(events))
	}
}

// TestIntegrationDebeziumMongoImagesNormalized pins the MongoDB path against
// real transports: the connector's JSON-string images are unwrapped from
// Extended JSON ObjectIds recursively while every other Extended JSON wrapper
// is preserved losslessly.
func TestIntegrationDebeziumMongoImagesNormalized(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	const oid = "663c9c0f0000000000000111"
	doc := dbzOrderDoc(oid)
	value := marshalDBZ(t, dbzMongoChange("c", nil, dbzImage(t, doc), "orders"))
	want := expectEventJSON(t, map[string]any{
		"collectionName": "orders",
		"recordType":     "INSERT",
		"newImage":       dbzOrderDocExpected(oid),
		"oldImage":       nil,
	})

	id := e.xaddDebezium(t, `{"_id":{"$oid":"`+oid+`"}}`, value, nil)
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if got := marshalDBZ(t, ev); got != want {
			t.Errorf("handler event = %s, want the normalized %s", got, want)
		}
		return nil
	})
	testutil.WaitFor(t, 8*time.Second, "mongo CDC event handled", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

// dbzCompletedAtDoc is one realistic orders document whose only changed field
// is completed_at, carried as the canonical Extended JSON $date wrapper the
// MongoDB connector emits ($numberLong Unix milliseconds).
func dbzCompletedAtDoc(oid string, ms int64) map[string]any {
	return map[string]any{
		"_id":          map[string]any{"$oid": oid},
		"completed_at": map[string]any{"$date": map[string]any{"$numberLong": strconv.FormatInt(ms, 10)}},
	}
}

// dbzNormalizedCompletedAt renders the exact normalized handler value for a
// $date carried as Unix milliseconds: the RFC3339Nano UTC string.
func dbzNormalizedCompletedAt(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

// TestIntegrationMongoUpdateCompletedAtTemporalMatching pins the date
// normalization + temporal matching boundary end to end on real transports: a
// MongoDB UPDATE whose after image carries a recent $date normalizes
// completed_at to an RFC3339 UTC string, and the runner-shaped pattern
// `newImage.completed_at > now()-5m` matches that update while a stale one does
// not. The cutoff is pinned with the template clock seam
// (app.ParseTemplateWithClock) and both instants sit four minutes inside and
// outside the cutoff, so the result cannot depend on test timing.
func TestIntegrationMongoUpdateCompletedAtTemporalMatching(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	// One pinned clock for the whole test: the temporal cutoff is recomputed
	// from this closure at match time, so it is deterministic.
	now := time.Now().UTC().Truncate(time.Millisecond)
	tmpl, err := app.ParseTemplateWithClock([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      newImage:
        completed_at: [{gt: "now()-5m"}]
`), func() time.Time { return now })
	if err != nil {
		t.Fatalf("parse template with clock: %v", err)
	}

	recentMS := now.Add(-time.Minute).UnixMilli()     // inside the 5-minute cutoff
	staleMS := now.Add(-10 * time.Minute).UnixMilli() // outside it
	recentDoc := dbzCompletedAtDoc("663c9c0f0000000000000aaa", recentMS)
	staleDoc := dbzCompletedAtDoc("663c9c0f0000000000000bbb", staleMS)

	// Realistic updates: the old image carries the (older) previous value and
	// the new image the fresh one.
	idRecent := e.xaddDebezium(t, "", marshalDBZ(t, dbzMongoChange("u",
		dbzImage(t, dbzCompletedAtDoc("663c9c0f0000000000000aaa", staleMS)),
		dbzImage(t, recentDoc), "orders")), nil)
	idStale := e.xaddDebezium(t, "", marshalDBZ(t, dbzMongoChange("u",
		dbzImage(t, dbzCompletedAtDoc("663c9c0f0000000000000bbb", staleMS)),
		dbzImage(t, staleDoc), "orders")), nil)

	// Handler workers run on separate goroutines while the test polls, so the
	// observed results are mutex-guarded.
	var mu sync.Mutex
	matched := map[string]bool{}
	normalized := map[string]string{}
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != idRecent && msgID != idStale {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		// The handler-visible image carries completed_at as a plain RFC3339
		// UTC string: the Extended JSON wrapper is gone in BOTH images.
		newImage, ok := ev["newImage"].(map[string]any)
		if !ok {
			t.Errorf("newImage = %v (%T), want a normalized document map", ev["newImage"], ev["newImage"])
			return nil
		}
		got, ok := newImage["completed_at"].(string)
		if !ok {
			t.Errorf("newImage.completed_at = %v (%T), want a normalized RFC3339 string",
				newImage["completed_at"], newImage["completed_at"])
			return nil
		}
		normalized[msgID] = got
		if oldImage, ok := ev["oldImage"].(map[string]any); ok {
			if _, ok := oldImage["completed_at"].(string); !ok {
				t.Errorf("oldImage.completed_at = %v (%T), want a normalized RFC3339 string too",
					oldImage["completed_at"], oldImage["completed_at"])
			}
		}
		// The runner-shaped matching decision over the normalized event.
		matched[msgID] = len(event.MatchingEventRules(tmpl.Events, ev)) > 0
		return nil
	})
	testutil.WaitFor(t, 8*time.Second, "both mongo updates handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(matched) == 2
	})
	e.stop(t)

	if got := normalized[idRecent]; got != dbzNormalizedCompletedAt(recentMS) {
		t.Fatalf("recent update completed_at = %q, want the normalized UTC instant %q",
			got, dbzNormalizedCompletedAt(recentMS))
	}
	if got := normalized[idStale]; got != dbzNormalizedCompletedAt(staleMS) {
		t.Fatalf("stale update completed_at = %q, want the normalized UTC instant %q",
			got, dbzNormalizedCompletedAt(staleMS))
	}
	if !matched[idRecent] {
		t.Fatalf("the recent update did not match completed_at > now()-5m (normalized %q, cutoff %q)",
			normalized[idRecent], now.Add(-5*time.Minute).Format(time.RFC3339Nano))
	}
	if matched[idStale] {
		t.Fatalf("the stale update matched completed_at > now()-5m, but %q is outside the window",
			normalized[idStale])
	}
}

// TestIntegrationMongoUpdateExhaustedCarriesWireValueAndNormalizedReplay pins
// the durable DLQ replay contract for a MongoDB UPDATE on real transports: the
// exhausted entry stores the ORIGINAL Debezium wire value verbatim (its $date
// wrappers intact), while ReplayPayload is the exact normalized four-field
// handler payload the live handler received — completed_at already the
// RFC3339 UTC string.
func TestIntegrationMongoUpdateExhaustedCarriesWireValueAndNormalizedReplay(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	const oid = "663c9c0f0000000000000111"
	completedMS := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute).UnixMilli()
	previousMS := time.Now().UTC().Truncate(time.Millisecond).Add(-10 * time.Minute).UnixMilli()
	value := marshalDBZ(t, dbzMongoChange("u",
		dbzImage(t, dbzCompletedAtDoc(oid, previousMS)),
		dbzImage(t, dbzCompletedAtDoc(oid, completedMS)), "orders"))
	want := expectEventJSON(t, map[string]any{
		"collectionName": "orders",
		"recordType":     "UPDATE",
		"newImage":       map[string]any{"_id": oid, "completed_at": dbzNormalizedCompletedAt(completedMS)},
		"oldImage":       map[string]any{"_id": oid, "completed_at": dbzNormalizedCompletedAt(previousMS)},
	})
	id := e.xaddDebezium(t, `{"_id":{"$oid":"`+oid+`"}}`, value, nil)

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		// The live handler payload must already be the normalized form: plain
		// RFC3339 UTC strings, wrappers gone.
		newImage, _ := ev["newImage"].(map[string]any)
		if got, ok := newImage["completed_at"].(string); !ok || got != dbzNormalizedCompletedAt(completedMS) {
			t.Errorf("live handler newImage.completed_at = %v (%T), want normalized %q",
				newImage["completed_at"], newImage["completed_at"], dbzNormalizedCompletedAt(completedMS))
		}
		// Then exhaust the invocation so the message routes to the DLQ.
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			return ErrInvocationNotEligible
		}
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	})
	testutil.WaitFor(t, 8*time.Second, "mongo update routed to DLQ", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "mongo update acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected a DLQ row for %s", id)
	}
	if m.Event != value {
		t.Fatalf("DLQ Event = %q, want the original Debezium wire value verbatim ($date wrappers intact)", m.Event)
	}
	if string(m.ReplayPayload) != want {
		t.Fatalf("DLQ ReplayPayload = %q, want the normalized handler payload %q", m.ReplayPayload, want)
	}
	if !m.Replayable() {
		t.Fatalf("an exhausted CDC entry must be replayable: %+v", m)
	}
	if m.SettledAt == "" {
		t.Fatalf("the row must be settled after the confirmed ACK")
	}
}

// TestIntegrationDebeziumTombstoneAckedNeverMatched pins the value-sentinel
// tombstone contract end to end: a null record value is published as the
// null-value sentinel (`default`) in `value` — with or without the sink's
// record `key` riding along ignored — and such entries are acknowledged without
// ever being matched (never a synthetic DELETE). A format=raw-marked sentinel
// is NOT a tombstone: the raw decode of the scalar fails and the message is
// dead-lettered as a non-replayable malformed message instead.
func TestIntegrationDebeziumTombstoneAckedNeverMatched(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	schemaKey := dbzSchemaKey(t, map[string]any{"id": 7})
	tombstones := []struct {
		name string
		key  string
	}{
		{"value-only", ""},
		{"record key present (ignored)", schemaKey},
	}
	ids := map[string]string{}
	for _, tb := range tombstones {
		ids[e.xaddDebezium(t, tb.key, "default", nil)] = tb.name + " tombstone"
	}
	// A format=raw-marked sentinel is decoded as a raw scalar and fails: it is
	// never silently acknowledged as a tombstone.
	rawSentinelID := e.xaddDebezium(t, "", "default", map[string]any{"format": "raw"})
	// A live ordinary event after them, so the matcher provably still runs.
	normalID := e.xadd(t, `{"event_name":"INSERT"}`)

	// The matcher-visit counter is an atomic: handler workers run on separate
	// goroutines while the test polls and eventually reads it after stop.
	var deleteruns atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == rawSentinelID || msgID == normalID {
			return nil
		}
		if _, isCDC := ev["recordType"]; isCDC {
			deleteruns.Add(1)
			if rv, ok := ev["recordType"]; ok && rv == "DELETE" {
				t.Error("a tombstone must never reach the handler as a synthetic DELETE")
			}
		}
		return nil
	})
	// Every tombstone is acknowledged...
	testutil.WaitFor(t, 8*time.Second, "tombstones acked", func() bool {
		for id := range ids {
			if _, ok := e.pending()[id]; ok {
				return false
			}
		}
		return true
	})
	// ...the raw-marked sentinel is dead-lettered for inspection...
	testutil.WaitFor(t, 8*time.Second, "raw-marked sentinel routed to DLQ", func() bool {
		_, ok := e.dlqEntry(rawSentinelID)
		return ok
	})
	// ...and the ordinary event still flowed.
	testutil.WaitFor(t, 8*time.Second, "ordinary event acked", func() bool {
		_, ok := e.pending()[normalID]
		return !ok
	})
	e.stop(t)

	if got := deleteruns.Load(); got != 0 {
		t.Fatalf("the matcher ran %d times for tombstones; want 0", got)
	}
	for _, tb := range tombstones {
		if _, dlqed := e.dlqEntry(ids[tb.name]); dlqed {
			t.Errorf("a confirmed %s was dead-lettered", tb.name)
		}
	}
	m, ok := e.dlqEntry(rawSentinelID)
	if !ok {
		t.Fatalf("expected a DLQ row for the raw-marked sentinel")
	}
	if m.Replayable() || m.ReplayPayload != nil {
		t.Fatalf("raw-marked sentinel row = %+v, want non-replayable with no payload", m)
	}
	if m.SettledAt == "" {
		t.Fatalf("the raw-marked sentinel row must be settled after the ACK")
	}
}

// TestIntegrationDebeziumExhaustedEntryCarriesReplayPayload pins the durable
// replay-payload contract on real transports: an exhausted CDC delivery stores
// BOTH the original raw transport value (diagnostic) and the exact normalized
// handler payload (ReplayPayload) in the PostgreSQL row, so replay hands the
// handler the same bytes live delivery did.
func TestIntegrationDebeziumExhaustedEntryCarriesReplayPayload(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	value := dbzSchemaValue(t, dbzChange("c", nil,
		dbzCustomersRow(9, "replay@acme.com"), dbzPostgresSource("customers")))
	want := expectEventJSON(t, map[string]any{
		"collectionName": "customers",
		"recordType":     "INSERT",
		"newImage":       dbzCustomersRow(9, "replay@acme.com"),
		"oldImage":       nil,
	})
	id := e.xaddDebezium(t, dbzSchemaKey(t, map[string]any{"id": 9}), value, nil)

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if marshalDBZ(t, ev) != want {
			t.Errorf("live handler event = %s, want %s", marshalDBZ(t, ev), want)
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			return ErrInvocationNotEligible
		}
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	})
	testutil.WaitFor(t, 8*time.Second, "CDC message routed to DLQ", func() bool {
		_, ok := e.dlqEntry(id)
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "CDC message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	m, ok := e.dlqEntry(id)
	if !ok {
		t.Fatalf("expected the DLQ row for %s", id)
	}
	if m.Event != value {
		t.Fatalf("DLQ Event = %q, want the raw transport value verbatim", m.Event)
	}
	if string(m.ReplayPayload) != want {
		t.Fatalf("DLQ ReplayPayload = %q, want the normalized handler payload %q",
			m.ReplayPayload, want)
	}
	if !m.Replayable() {
		t.Fatalf("an exhausted CDC entry must be replayable: %+v", m)
	}
	if m.SettledAt == "" {
		t.Fatalf("the row must be settled after the confirmed ACK")
	}
}

// TestIntegrationFormatRawOverridesDebezium pins the explicit escape hatch end
// to end: with `format=raw` a value that would otherwise be a Debezium change
// is delivered to the handler as the whole raw record (schema wrapper intact,
// no normalization), and the tombstone sentinel is never skipped.
func TestIntegrationFormatRawOverridesDebezium(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})

	record := map[string]any{
		"schema":  map[string]any{"type": "struct", "name": "dbz.inventory.Envelope"},
		"payload": dbzChange("c", nil, dbzCustomersRow(5, ""), dbzPostgresSource("customers")),
	}
	rawValue := marshalDBZ(t, record)
	id := e.xaddDebezium(t, dbzSchemaKey(t, map[string]any{"id": 5}), rawValue,
		map[string]any{"format": "raw"})

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if marshalDBZ(t, ev) != rawValue {
			t.Errorf("handler event = %s, want the whole raw record %s",
				marshalDBZ(t, ev), rawValue)
		}
		if _, isCDC := ev["recordType"]; isCDC {
			t.Error("a format=raw record must not be CDC-normalized")
		}
		return nil
	})
	testutil.WaitFor(t, 8*time.Second, "raw-overridden record handled", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

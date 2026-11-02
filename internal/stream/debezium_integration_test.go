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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

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
// ObjectIds unwrapped and the other Extended JSON wrappers untouched.
func dbzOrderDocExpected(oid string) map[string]any {
	return map[string]any{
		"_id":       oid,
		"orderdate": map[string]any{"$date": "2026-01-17T10:33:00Z"},
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

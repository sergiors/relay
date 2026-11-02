package envelope

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"relay/internal/schedule"
)

// The unit tests below pin the transport-envelope decode contract with
// fixtures modeled on the official Debezium Server 3.7 Redis sink "extended"
// message structure: the record travels in `value` (the record value JSON, or
// the sink's null-value sentinel), and the optional `format` marker is the only
// other field this layer knows about. Every other stream-entry field — the
// sink's `key`, UPPERCASE headers, unknown names — is not part of the Relay
// envelope and cannot influence decoding. `format=raw` is the explicit escape
// hatch.
//
// Schema-enabled (`debezium.format.*.schemas.enable=true`) fixtures wrap the
// record in a `schema`/`payload` envelope; schemaless fixtures carry the raw
// payload object.

// compactJSON renders any value as canonical JSON bytes (sorted object keys),
// so payload expectations can be built without hand-written encodings.
func compactJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return b
}

// decodeObject unmarshals a JSON object fixture into a map, failing the test on
// any mismatch so a malformed fixture is visible at the source.
func decodeObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("fixture is not a JSON object: %v", err)
	}
	return m
}

// schemaWrap renders the schema-enabled connector record shape: the record
// payload nested under a Debezium Envelope schema.
func schemaWrap(t *testing.T, payload map[string]any) string {
	t.Helper()
	return string(compactJSON(t, map[string]any{
		"schema": map[string]any{
			"type":     "struct",
			"name":     "dbz.inventory.Envelope",
			"optional": false,
			"fields": []any{
				map[string]any{"type": "struct", "field": "before", "optional": true},
				map[string]any{"type": "struct", "field": "after", "optional": true},
				map[string]any{"type": "string", "field": "op", "optional": false},
			},
		},
		"payload": payload,
	}))
}

// pgSource renders a realistic Debezium 3.7 PostgreSQL source block.
func pgSource(table string) map[string]any {
	return map[string]any{
		"version":   "3.7.0.Final",
		"connector": "postgresql",
		"name":      "dbz",
		"ts_ms":     1767200000000,
		"snapshot":  "false",
		"db":        "inventory",
		"lsn":       35451236,
		"txid":      762,
		"schema":    "public",
		"table":     table,
	}
}

// pgChange renders a schemaless PostgreSQL change payload with the Debezium
// Envelope fields (before/after/source/op).
func pgChange(op string, before, after any, table string) map[string]any {
	return map[string]any{
		"before":      before,
		"after":       after,
		"source":      pgSource(table),
		"op":          op,
		"ts_ms":       1767200000100,
		"transaction": nil,
	}
}

// pgChangeForSource renders a change payload with an explicitly crafted source
// block, so malformed-source fixtures keep the rest of the envelope intact.
func pgChangeForSource(source map[string]any, op string, before, after any) map[string]any {
	return map[string]any{
		"before": before,
		"after":  after,
		"source": source,
		"op":     op,
		"ts_ms":  1767200000100,
	}
}

// pgCustomersRow is one inventory.customers row image.
func pgCustomersRow(id int, email string) map[string]any {
	return map[string]any{"id": id, "email": email}
}

// mongoSource renders a realistic Debezium 3.7 MongoDB source block.
func mongoSource(collection string) map[string]any {
	return map[string]any{
		"version":    "3.7.0.Final",
		"connector":  "mongodb",
		"name":       "dbz",
		"ts_ms":      1767200000000,
		"snapshot":   "last",
		"db":         "inventory",
		"rs":         "rs0",
		"collection": collection,
		"ord":        42,
		"stage":      "RS0_8012",
	}
}

// mongoChange renders a MongoDB change payload whose images are the JSON
// strings the connector serializes (exactly as the Redis sink carries them).
func mongoChange(op string, before, after any, collection string) map[string]any {
	return map[string]any{
		"before":      before,
		"after":       after,
		"source":      mongoSource(collection),
		"op":          op,
		"ts_ms":       1767200000100,
		"transaction": nil,
	}
}

const mongoOID = "663c9c0f0000000000000111"

// mongoDoc is one orders document with Extended JSON wrappers: an ObjectId,
// a date, a decimal, binary data, a timestamp, and an ObjectId nested inside
// an array and a nested document.
func mongoDoc(oid string) map[string]any {
	return map[string]any{
		"_id":       map[string]any{"$oid": oid},
		"orderdate": map[string]any{"$date": "2026-01-17T10:33:00Z"},
		"total":     map[string]any{"$numberDecimal": "112.50"},
		"checksum":  map[string]any{"$binary": map[string]any{"base64": "SGVsbG8=", "subType": "00"}},
		"clock":     map[string]any{"$timestamp": map[string]any{"t": 1767200000, "i": 1}},
		"tags":      []any{"rush", map[string]any{"$oid": "663c9c0f00000000000007c5"}},
		"buyer":     map[string]any{"tier": "gold", "_id": map[string]any{"$oid": "663c9c0f00000000000009a1"}},
	}
}

// mongoDocUnwrapped is the EXPECTED normalized form of mongoDoc after the
// ObjectId wrappers are unwrapped recursively and every other Extended JSON
// wrapper is left untouched.
func mongoDocUnwrapped(oid string) map[string]any {
	return map[string]any{
		"_id":       oid,
		"orderdate": map[string]any{"$date": "2026-01-17T10:33:00Z"},
		"total":     map[string]any{"$numberDecimal": "112.50"},
		"checksum":  map[string]any{"$binary": map[string]any{"base64": "SGVsbG8=", "subType": "00"}},
		"clock":     map[string]any{"$timestamp": map[string]any{"t": 1767200000, "i": 1}},
		"tags":      []any{"rush", "663c9c0f00000000000007c5"},
		"buyer":     map[string]any{"tier": "gold", "_id": "663c9c0f00000000000009a1"},
	}
}

// mongoImage renders one JSON-string document image as the connector does.
func mongoImage(t *testing.T, doc map[string]any) string {
	t.Helper()
	return string(compactJSON(t, doc))
}

// assertFourFieldCDCEvent pins the normalized output shape: exactly
// collectionName/recordType/newImage/oldImage, with JSON-comparable images.
func assertFourFieldCDCEvent(t *testing.T, event map[string]any, collection, recordType string, newImage, oldImage any) {
	t.Helper()
	if len(event) != 4 {
		t.Fatalf("event fields = %d (%v), want exactly the normalized four", len(event), event)
	}
	for _, key := range []string{"collectionName", "recordType", "newImage", "oldImage"} {
		if _, ok := event[key]; !ok {
			t.Fatalf("event is missing the normalized field %q: %v", key, event)
		}
	}
	if event["collectionName"] != collection {
		t.Fatalf("collectionName = %v, want %q", event["collectionName"], collection)
	}
	if event["recordType"] != recordType {
		t.Fatalf("recordType = %v, want %q", event["recordType"], recordType)
	}
	assertImage(t, "newImage", event["newImage"], newImage)
	assertImage(t, "oldImage", event["oldImage"], oldImage)
}

// assertImage compares one normalized image against the expectation: nil, a
// JSON object (deep-compared through canonical bytes), or a plain string (an
// unwrapped ObjectId).
func assertImage(t *testing.T, field string, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case nil:
		if got != nil {
			t.Fatalf("%s = %v, want null", field, got)
		}
	case string:
		if got != w {
			t.Fatalf("%s = %v, want the string %q", field, got, w)
		}
	default:
		gotJSON, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("marshal %s: %v", field, err)
		}
		wantJSON := compactJSON(t, w)
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("%s = %s, want %s", field, gotJSON, wantJSON)
		}
	}
}

// scheduleOccurrence is the fixed occurrence every schedule-envelope test
// shares, with its value fixture.
func scheduleOccurrence() schedule.Occurrence {
	return schedule.Occurrence{
		App: "courses", Schedule: "cleanup", Handler: "jobs.cleanup.handler",
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}
}

// TestDecodeRawObjectValueOnly pins the minimal external producer contract: a
// stream entry carrying only a JSON object in `value` (no `format` marker, and
// no other Relay field at all) is an ordinary event whose Event is the decoded
// map, exactly as the runner receives it, and whose payload is the canonical
// JSON the live handler gets.
func TestDecodeRawObjectValueOnly(t *testing.T) {
	res, err := Decode("", []byte(`{"event_name":"INSERT","id":7}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if res.Kind != KindEvent {
		t.Fatalf("Kind = %v, want KindEvent", res.Kind)
	}
	if res.Event["event_name"] != "INSERT" || res.Event["id"] != float64(7) {
		t.Fatalf("Event = %v, want the decoded map", res.Event)
	}
	if res.Occurrence != (schedule.Occurrence{}) {
		t.Fatalf("Occurrence = %+v, want empty", res.Occurrence)
	}
	if string(res.Payload) != string(compactJSON(t, res.Event)) {
		t.Fatalf("Payload = %s, want the canonical handler payload %s",
			res.Payload, compactJSON(t, res.Event))
	}
}

// TestDecodeRawObjectCanonicalizesPayloadOrder pins that a raw event's replay
// payload is the canonical re-encoding of the decoded map (sorted keys, no
// whitespace) — the same bytes the live runner hands the handler.
func TestDecodeRawObjectCanonicalizesPayloadOrder(t *testing.T) {
	res, err := Decode("", []byte("{\n  \"zzz\": 1,\n  \"aaa\": 2\n}\n"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if string(res.Payload) != `{"aaa":2,"zzz":1}` {
		t.Fatalf("Payload = %s, want the canonical re-encoding", res.Payload)
	}
}

// TestDecodeFormatRawOverridesStructure pins that an explicit `format=raw`
// marker wins over every structural recognition path: a value that would
// otherwise decode as a schedule claim or a Debezium change is decoded as an
// ordinary raw event with its wrapper/coordination fields intact, and the
// null-value sentinel is not silently skipped but decoded (and fails, being a
// scalar).
func TestDecodeFormatRawOverridesStructure(t *testing.T) {
	t.Run("debezium-shaped value stays unnormalized", func(t *testing.T) {
		value := schemaWrap(t, pgChange("c", nil, pgCustomersRow(1, "sally@acme.com"), "customers"))
		res, err := Decode(FormatRaw, []byte(value))
		if err != nil {
			t.Fatalf("Decode(raw): %v", err)
		}
		if res.Kind != KindEvent {
			t.Fatalf("Kind = %v, want KindEvent", res.Kind)
		}
		if _, ok := res.Event["schema"]; !ok {
			t.Fatalf("Event = %v, want the whole record including its schema", res.Event)
		}
		if _, ok := res.Event["collectionName"]; ok {
			t.Fatalf("Event = %v, want NO CDC normalization under format=raw", res.Event)
		}
	})

	t.Run("schedule-shaped value stays raw", func(t *testing.T) {
		occ := scheduleOccurrence()
		env, err := occ.Envelope()
		if err != nil {
			t.Fatalf("Envelope: %v", err)
		}
		res, err := Decode(FormatRaw, env)
		if err != nil {
			t.Fatalf("Decode(raw): %v", err)
		}
		if res.Kind != KindEvent {
			t.Fatalf("Kind = %v, want KindEvent (format=raw overrides schedule)", res.Kind)
		}
		if res.Event["source"] != "relay.schedule" || res.Event["app"] != "courses" {
			t.Fatalf("Event = %v, want the envelope decoded as an ordinary map", res.Event)
		}
	})

	t.Run("null-value sentinel is decoded, not skipped", func(t *testing.T) {
		_, err := Decode(FormatRaw, []byte("default"))
		if err == nil {
			t.Fatal("format=raw with the null-value sentinel must not be a tombstone skip")
		}
		if !strings.Contains(err.Error(), "decode envelope value") {
			t.Fatalf("err = %v, want the raw-decode scalar error", err)
		}
	})

	t.Run("claimed metadata record with missing location stays raw", func(t *testing.T) {
		// The record is one the structured path CLAIMS (source metadata names
		// Debezium; the CDC marker is present) and would fail on: its Postgres
		// `table` location is missing. format=raw wins before the claim, so no
		// structure recognition error is possible — the whole record, truncated
		// location included, is the ordinary event.
		truncated := map[string]any{
			"source": func() map[string]any {
				s := pgSource("customers")
				delete(s, "table")
				return s
			}(),
			"op":    "c",
			"after": pgCustomersRow(1, "sally@acme.com"),
		}
		res, err := Decode(FormatRaw, compactJSON(t, truncated))
		if err != nil {
			t.Fatalf("Decode(raw): %v (format=raw must always win)", err)
		}
		if res.Kind != KindEvent {
			t.Fatalf("Kind = %v, want KindEvent", res.Kind)
		}
		if _, ok := res.Event["source"]; !ok {
			t.Fatalf("Event = %v, want the truncated record verbatim", res.Event)
		}
		if _, isCDC := res.Event["collectionName"]; isCDC {
			t.Fatalf("Event = %v, want NO CDC claim path under format=raw", res.Event)
		}
	})
}

// TestDecodeUnknownFormatFails pins that an unknown format marker is a
// non-retryable error, never silently decoded as raw.
func TestDecodeUnknownFormatFails(t *testing.T) {
	res, err := Decode("xml", []byte(`{"a":1}`))
	if err == nil {
		t.Fatalf("Decode(xml) succeeded with %v, want an error", res)
	}
	if !strings.Contains(err.Error(), `unknown envelope format "xml"`) {
		t.Fatalf("err = %v, want the unknown-format error", err)
	}
}

// TestDecodeMalformedValueFails pins that malformed `value` payloads are
// explicit non-retryable errors on both the structured and forced-raw paths:
// they never fall through to an empty "matched nothing" event.
func TestDecodeMalformedValueFails(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"invalid JSON", `{oops`},
		{"array", `[1,2,3]`},
		{"scalar string", `"hello"`},
		{"number", `42`},
		{"null", `null`},
	}
	for _, tt := range tests {
		for _, format := range []string{"", FormatRaw} {
			if _, err := Decode(format, []byte(tt.value)); err == nil {
				t.Fatalf("Decode(format=%q, %q) succeeded, want an error", format, tt.value)
			}
		}
	}
}

// TestDecodePartialCDCShapeStaysRaw pins the claim precision in the opposite
// direction from the malformed-claim test: an object that merely carries
// `source`, `op`, `before`/`after`, a single image, a source with no Debezium
// evidence beyond the connector, or a connector/location WITHOUT a CDC marker
// is an ordinary event, never normalized. Only a record with the claimed CDC
// evidence (a schema wrapper, or a source whose connector comes with a location
// or other Debezium source metadata) plus an `op` or both images is a Debezium
// claim — and malformed variants of those fail instead of falling back.
func TestDecodePartialCDCShapeStaysRaw(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"string source only", map[string]any{"source": "inventory.customers"}},
		{"source object without connector", map[string]any{
			"source": map[string]any{"db": "inventory", "table": "customers"}}},
		{"connector/location but no CDC marker", map[string]any{
			"source": pgSource("customers")}},
		{"op only", map[string]any{"op": "c"}},
		{"op without source", map[string]any{
			"op": "c", "after": pgCustomersRow(1, "")}},
		{"only before", map[string]any{
			"source": pgSource("customers"), "before": pgCustomersRow(1, "")}},
		{"only after", map[string]any{
			"source": pgSource("customers"), "after": pgCustomersRow(1, "")}},
		// A raw partial with source+op but no connector (and no Debezium source
		// metadata) stays raw: the source block alone is ordinary field data.
		{"source without connector plus op", map[string]any{
			"source": map[string]any{"db": "inventory", "schema": "public"},
			"op":     "c", "after": pgCustomersRow(1, "")}},
		// A raw object with partial Debezium-like fields stays raw: the source
		// names a connector but carries NO location and NO other Debezium source
		// metadata, so there is no claim evidence beyond coincidence.
		{"connector-only source with op", map[string]any{
			"source": map[string]any{"connector": "postgresql"}, "op": "c"}},
		{"connector-only source with both images", map[string]any{
			"source": map[string]any{"connector": "postgresql"},
			"before": pgCustomersRow(1, ""), "after": pgCustomersRow(1, "")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Decode("", compactJSON(t, tt.value))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if res.Kind != KindEvent {
				t.Fatalf("Kind = %v, want KindEvent", res.Kind)
			}
			if string(res.Payload) != string(compactJSON(t, tt.value)) {
				t.Fatalf("Payload = %s, want the raw object verbatim-canonical", res.Payload)
			}
		})
	}

	t.Run("full claim without op is still a claim", func(t *testing.T) {
		value := map[string]any{
			"source": pgSource("customers"),
			"before": pgCustomersRow(1, "old@acme.com"),
			"after":  pgCustomersRow(1, "new@acme.com"),
		}
		res, err := Decode("", compactJSON(t, value))
		if err == nil {
			t.Fatalf("Decode(%v) = %v, want the missing-op failure", value, res)
		}
		if !strings.Contains(err.Error(), "op is missing") {
			t.Fatalf("err = %v, want the missing-op error", err)
		}
	})
}

// TestDecodeClaimedDebeziumMissingLocationFails pins the truncated-record
// guarantee: once a record has claimed Debezium structure — through source
// metadata beyond the connector (schemaless) or through its schema wrapper — a
// missing or empty `table`/`collection` source location is a non-retryable
// decode error naming the missing location. Such a clearly Debezium Postgres/
// MongoDB record must NEVER fall through to raw matching, where a truncated
// payload could silently execute an ordinary event rule.
func TestDecodeClaimedDebeziumMissingLocationFails(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{
			// Schemaless claim: the source carries other Debezium metadata
			// (version/name/ts_ms/db), which is the claim evidence; the
			// Postgres location is missing.
			name: "schemaless postgres without table",
			value: pgChangeForSource(func() map[string]any {
				s := pgSource("customers")
				delete(s, "table")
				return s
			}(), "c", nil, pgCustomersRow(1, "")),
			want: "postgresql source has no table name",
		},
		{
			// Same rule for MongoDB: rs/db metadata claims the record, the
			// missing collection is the failure.
			name: "schemaless mongodb without collection",
			value: pgChangeForSource(func() map[string]any {
				s := mongoSource("orders")
				delete(s, "collection")
				return s
			}(), "c", nil, mongoImage(t, mongoDoc(mongoOID))),
			want: "mongodb source has no collection name",
		},
		{
			// An EMPTY location is the same failure as a missing one.
			name: "schemaless postgres with empty table",
			value: pgChangeForSource(func() map[string]any {
				s := pgSource("customers")
				s["table"] = ""
				return s
			}(), "c", nil, pgCustomersRow(1, "")),
			want: "postgresql source has no table name",
		},
		{
			name: "schemaless mongodb with empty collection",
			value: pgChangeForSource(func() map[string]any {
				s := mongoSource("orders")
				s["collection"] = ""
				return s
			}(), "c", nil, mongoImage(t, mongoDoc(mongoOID))),
			want: "mongodb source has no collection name",
		},
		{
			// Schema-enabled claim: the wrapper's schema is the additional
			// evidence, so even a source with ONLY the connector (no location,
			// no metadata) claims Debezium structure and fails on the missing
			// location.
			name: "schema-enabled postgres wrapper without table",
			value: map[string]any{
				"schema":  map[string]any{"type": "struct", "name": "dbz.inventory.Envelope", "optional": false},
				"payload": pgChangeForSource(map[string]any{"connector": "postgresql"}, "c", nil, pgCustomersRow(1, "")),
			},
			want: "postgresql source has no table name",
		},
		{
			name: "schema-enabled mongodb wrapper without collection",
			value: map[string]any{
				"schema":  map[string]any{"type": "struct", "name": "dbz.inventory.Envelope", "optional": false},
				"payload": pgChangeForSource(map[string]any{"connector": "mongodb"}, "c", nil, mongoImage(t, mongoDoc(mongoOID))),
			},
			want: "mongodb source has no collection name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Decode("", compactJSON(t, tt.value))
			if err == nil {
				t.Fatalf("Decode = %v, want the missing-location failure", res)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want an error naming %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), "debezium") {
				t.Fatalf("err = %v, want the debezium-classified error", err)
			}
		})
	}
}

// TestDecodeValidScheduleEnvelope pins the schedule occurrence contract: an
// envelope value with the reserved source marker decodes to KindSchedule with
// the exact Occurrence and the handler payload. The occurrence identity is
// validated from the envelope's own derived occurrence_id — there is no
// external transport key involved.
func TestDecodeValidScheduleEnvelope(t *testing.T) {
	occ := scheduleOccurrence()
	value, err := occ.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}

	res, err := Decode("", value)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if res.Kind != KindSchedule {
		t.Fatalf("Kind = %v, want KindSchedule", res.Kind)
	}
	if res.Occurrence.ID() != occ.ID() {
		t.Fatalf("Occurrence = %+v, want %q", res.Occurrence, occ.ID())
	}
	if string(res.Payload) != string(occ.Payload()) {
		t.Fatalf("Payload = %s, want Occurrence.Payload() %s", res.Payload, occ.Payload())
	}
	if res.Event != nil {
		t.Fatalf("Event = %v, want nil for a schedule occurrence", res.Event)
	}
}

// TestDecodeMalformedScheduleEnvelopeNeverFallsThrough pins that an envelope
// bearing the reserved schedule marker but failing structural validation is a
// non-retryable error: it can neither run as a schedule occurrence nor fall
// through to ordinary event matching. The occurrence identity is validated
// from the envelope's own fields against the deterministically derived ID —
// including a malformed (missing) or forged (mismatched) occurrence_id —
// so an entry can never be redirected to another occurrence.
func TestDecodeMalformedScheduleEnvelopeNeverFallsThrough(t *testing.T) {
	valid, _ := scheduleOccurrence().Envelope()
	validMap := decodeObject(t, string(valid))
	tests := []struct {
		name   string
		delete string
		set    map[string]any
		want   string
	}{
		{"missing app", "app", nil, "app"},
		{"missing handler", "handler", nil, "handler"},
		{"empty schedule", "", map[string]any{"schedule": ""}, "schedule is empty"},
		{"missing scheduled_at", "scheduled_at", nil, "scheduled_at"},
		{"unparseable scheduled_at", "", map[string]any{"scheduled_at": "not-a-date"}, "scheduled_at"},
		{"sub-second scheduled_at", "", map[string]any{"scheduled_at": "2026-07-01T08:00:00.500Z"}, "sub-second"},
		{"missing occurrence_id", "occurrence_id", nil, "occurrence_id is missing"},
		{"mismatched occurrence_id", "", map[string]any{"occurrence_id": "schedule:evil:x:2030-01-01T00:00:00Z"}, "does not match derived identity"},
		{"non-string app", "", map[string]any{"app": 7}, "app"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := cloneMapForEnvelope(validMap)
			if tt.delete != "" {
				delete(value, tt.delete)
			}
			for k, v := range tt.set {
				value[k] = v
			}
			res, err := Decode("", compactJSON(t, value))
			if err == nil {
				t.Fatalf("Decode = %v, want an error", res)
			}
			if !strings.Contains(err.Error(), "schedule envelope") {
				t.Fatalf("err = %v, want the schedule-envelope error", err)
			}
			if tt.want != "" && !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want an error naming %q", err, tt.want)
			}
		})
	}
}

func cloneMapForEnvelope(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// TestDecodeDebeziumTombstone pins the value-sentinel tombstone contract: the
// Debezium Redis sink's null-value sentinel (`default`) is the exact `value` a
// tombstone entry carries — with no transport key involved at all. Such an
// entry is reliably identified as a tombstone to acknowledge without execution
// — never a synthetic DELETE.
func TestDecodeDebeziumTombstone(t *testing.T) {
	res, err := Decode("", []byte("default"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if res.Kind != KindTombstone {
		t.Fatalf("Kind = %v, want KindTombstone", res.Kind)
	}
	if res.Event != nil {
		t.Fatalf("Event = %v, want nil (a tombstone never becomes a synthetic DELETE)", res.Event)
	}
	if res.Payload != nil {
		t.Fatalf("Payload = %v, want nil", res.Payload)
	}
	if res.Occurrence != (schedule.Occurrence{}) {
		t.Fatalf("Occurrence = %+v, want empty", res.Occurrence)
	}
}

// TestDecodeTombstoneRequiresExactSentinel pins that only the exact official
// null-value sentinel is recognized: any other scalar `value` is a malformed
// message that fails non-retryably (and is dead-lettered), never an
// acknowledged tombstone. The transport key no longer participates, so nothing
// can confirm or deny a tombstone except the value itself.
func TestDecodeTombstoneRequiresExactSentinel(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"custom default", "Default"},
		{"trailing whitespace", "default "},
		{"leading whitespace", " default"},
		{"custom null.value", "tombstone"},
		{"quoted JSON string", `"default"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Decode("", []byte(tt.value))
			// A tombstone is the only successful Kind for the bare sentinel; any
			// other value must fail outright, never decode as an event.
			if err == nil {
				t.Fatalf("Decode(%q) = %+v with no error; a lookalike must never be ACKed", tt.value, res)
			}
			if !strings.Contains(err.Error(), "decode envelope value") {
				t.Fatalf("err = %v, want the raw-decode error", err)
			}
		})
	}

	// A JSON object is an ordinary event, never a tombstone candidate: only the
	// exact sentinel switches the kind.
	res, err := Decode("", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if res.Kind != KindEvent {
		t.Fatalf("Kind = %v, want KindEvent for a JSON object value", res.Kind)
	}
}

// TestDecodePostgresSchemaEnabledAndSchemaless pins the full normalized CDC
// contract for Debezium 3.7 PostgreSQL fixtures: schema-enabled (the sink's
// converter with `debezium.format.*.schemas.enable=true`) and schemaless
// records of every operation normalize to exactly the four exposed fields,
// with the table as the collection name and snapshot reads as INSERTs.
func TestDecodePostgresSchemaEnabledAndSchemaless(t *testing.T) {
	type change struct {
		name    string
		op      string
		before  map[string]any
		after   map[string]any
		recType string
		newExp  any
		oldExp  any
	}
	changes := []change{
		{
			name: "update", op: "u",
			before:  pgCustomersRow(1, "old@acme.com"),
			after:   pgCustomersRow(1, "new@acme.com"),
			recType: "UPDATE", newExp: pgCustomersRow(1, "new@acme.com"),
			oldExp: pgCustomersRow(1, "old@acme.com"),
		},
		{
			name: "create", op: "c",
			after:   pgCustomersRow(2, "george@acme.com"),
			recType: "INSERT", newExp: pgCustomersRow(2, "george@acme.com"),
		},
		{
			name: "delete", op: "d",
			before:  pgCustomersRow(3, "kate@acme.com"),
			recType: "DELETE", oldExp: pgCustomersRow(3, "kate@acme.com"),
		},
		{
			name: "snapshot-read", op: "r",
			after:   pgCustomersRow(4, "fred@acme.com"),
			recType: "INSERT", newExp: pgCustomersRow(4, "fred@acme.com"),
		},
	}

	for _, shape := range []struct {
		name    string
		wrapVal func(payload map[string]any) string
	}{
		{"schema-enabled", func(p map[string]any) string { return schemaWrap(t, p) }},
		{"schemaless", func(p map[string]any) string { return string(compactJSON(t, p)) }},
	} {
		t.Run(shape.name, func(t *testing.T) {
			for _, ch := range changes {
				t.Run(ch.name, func(t *testing.T) {
					value := shape.wrapVal(pgChange(ch.op, ch.before, ch.after, "customers"))
					res, err := Decode("", []byte(value))
					if err != nil {
						t.Fatalf("Decode(%s): %v", ch.name, err)
					}
					if res.Kind != KindEvent {
						t.Fatalf("Kind = %v, want KindEvent", res.Kind)
					}
					assertFourFieldCDCEvent(t, res.Event,
						"customers", ch.recType, ch.newExp, ch.oldExp)
					// The replay payload is byte-identical to what the handler
					// receives live: the canonical four-field JSON.
					if string(res.Payload) != string(compactJSON(t, res.Event)) {
						t.Fatalf("Payload = %s, want the canonical event", res.Payload)
					}
				})
			}
		})
	}
}

// TestDecodeMongoSchemaEnabledAndSchemaless pins the MongoDB contract: the
// connector's JSON-string images are decoded and their Extended JSON
// ObjectIds unwrapped recursively, while every other Extended JSON wrapper
// survives losslessly and the top-level $oid-only image is rejected (it is not
// a document image).
func TestDecodeMongoSchemaEnabledAndSchemaless(t *testing.T) {
	afterDoc := mongoDoc(mongoOID)
	beforeDoc := mongoDoc("663c9c0f0000000000000ddd")
	afterExp := mongoDocUnwrapped(mongoOID)
	beforeExp := mongoDocUnwrapped("663c9c0f0000000000000ddd")

	for _, shape := range []struct {
		name    string
		wrapVal func(payload map[string]any) string
	}{
		{"schema-enabled", func(p map[string]any) string { return schemaWrap(t, p) }},
		{"schemaless", func(p map[string]any) string { return string(compactJSON(t, p)) }},
	} {
		t.Run(shape.name, func(t *testing.T) {
			cases := []struct {
				name string
				ch   map[string]any
				rec  string
				newP any
				oldP any
			}{
				{"create", mongoChange("c", nil, mongoImage(t, afterDoc), "orders"),
					"INSERT", afterExp, nil},
				{"update", mongoChange("u", mongoImage(t, beforeDoc), mongoImage(t, afterDoc), "orders"),
					"UPDATE", afterExp, beforeExp},
				{"delete", mongoChange("d", mongoImage(t, beforeDoc), nil, "orders"),
					"DELETE", nil, beforeExp},
				{"snapshot-read", mongoChange("r", nil, mongoImage(t, afterDoc), "orders"),
					"INSERT", afterExp, nil},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					value := shape.wrapVal(tc.ch)
					res, err := Decode("", []byte(value))
					if err != nil {
						t.Fatalf("Decode: %v", err)
					}
					if res.Kind != KindEvent {
						t.Fatalf("Kind = %v", res.Kind)
					}
					assertFourFieldCDCEvent(t, res.Event, "orders", tc.rec, tc.newP, tc.oldP)
					if string(res.Payload) != string(compactJSON(t, res.Event)) {
						t.Fatalf("Payload = %s, want the canonical event", res.Payload)
					}
				})
			}
		})
	}

	t.Run("oid-only image is not a document image", func(t *testing.T) {
		// An image whose WHOLE content is an ObjectId wrapper cannot be a
		// document image; it must fail, never silently become a scalar.
		value := compactJSON(t, mongoChange("c", nil, `{"$oid":"`+mongoOID+`"}`, "orders"))
		res, err := Decode("", value)
		if err == nil {
			t.Fatalf("Decode = %v, want the oid-only image error", res)
		}
		if !strings.Contains(err.Error(), "newImage must be a JSON object or null") {
			t.Fatalf("err = %v, want the image-shape error", err)
		}
	})
}

// TestDecodeSourceLocationSelectsCollectionOrTable pins the collection-name
// rule per connector: MongoDB reads source.collection, PostgreSQL reads
// source.table.
func TestDecodeSourceLocationSelectsCollectionOrTable(t *testing.T) {
	t.Run("postgresql table becomes collectionName", func(t *testing.T) {
		res, err := Decode("",
			compactJSON(t, pgChange("c", nil, pgCustomersRow(1, ""), "shipments")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if res.Event["collectionName"] != "shipments" {
			t.Fatalf("collectionName = %v, want the table", res.Event["collectionName"])
		}
	})

	t.Run("mongodb collection becomes collectionName", func(t *testing.T) {
		res, err := Decode("",
			compactJSON(t, mongoChange("c", nil, mongoImage(t, mongoDoc(mongoOID)), "shipments")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if res.Event["collectionName"] != "shipments" {
			t.Fatalf("collectionName = %v, want the collection", res.Event["collectionName"])
		}
	})
}

// TestDecodeDebeziumMalformedClaimsFail pins that a recognized Debezium record
// with a malformed operation, source, or image is a non-retryable error naming
// the CDC field: the record already claimed CDC structure, so it can never
// fall through to raw matching.
func TestDecodeDebeziumMalformedClaimsFail(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"unknown op", pgChange("x", nil, pgCustomersRow(1, ""), "customers"),
			`unsupported op "x"`},
		{"empty op", pgChange("", nil, pgCustomersRow(1, ""), "customers"),
			"op is missing"},
		{"non-string op", func() map[string]any {
			ch := pgChange("c", nil, pgCustomersRow(1, ""), "customers")
			ch["op"] = 3
			return ch
		}(), "op is missing"},
		{"unsupported connector", func() map[string]any {
			s := pgSource("orders")
			s["connector"] = "mysql"
			return pgChangeForSource(s, "c", nil, pgCustomersRow(1, ""))
		}(), `unsupported source connector "mysql"`},
		{"empty connector", func() map[string]any {
			s := pgSource("customers")
			s["connector"] = ""
			return pgChangeForSource(s, "c", nil, pgCustomersRow(1, ""))
		}(), "source.connector is missing"},
		{"postgres with ill-typed table", func() map[string]any {
			s := pgSource("customers")
			s["table"] = 7
			return pgChangeForSource(s, "c", nil, pgCustomersRow(1, ""))
		}(), "postgresql source has no table name"},
		{"postgres scalar image", pgChange("c", nil, 3, "customers"),
			"newImage must be a JSON object or null"},
		{"postgres scalar oldImage", pgChange("d", 3, nil, "customers"),
			"oldImage must be a JSON object or null"},
		{"mongo scalar image", mongoChange("c", nil, 3, "orders"),
			"newImage must be a JSON object or null"},
		{"mongo image invalid JSON", mongoChange("c", nil, `{oops`, "orders"),
			"decode JSON image"},
		{"mongo image scalar", mongoChange("c", nil, `"1"`, "orders"),
			"decoded image is not a JSON object or null"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := Decode("", compactJSON(t, tt.value))
			if err == nil {
				t.Fatalf("Decode = %v, want an error", res)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want an error naming %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), "debezium") {
				t.Fatalf("err = %v, want the debezium-classified error", err)
			}
		})
	}

	t.Run("string source is not a claim", func(t *testing.T) {
		res, err := Decode("", []byte(`{"source":"inventory.customers","op":"c"}`))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if res.Kind != KindEvent {
			t.Fatalf("Kind = %v, want a raw event for a non-object source", res.Kind)
		}
	})

	t.Run("null images stay null", func(t *testing.T) {
		res, err := Decode("", compactJSON(t, pgChange("c", nil, pgCustomersRow(1, ""), "customers")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if res.Event["oldImage"] != nil || res.Event["newImage"] == nil {
			t.Fatalf("images = %v/%v, want null oldImage and an object newImage",
				res.Event["oldImage"], res.Event["newImage"])
		}
	})
}

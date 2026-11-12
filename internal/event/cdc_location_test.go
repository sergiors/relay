package event

import (
	"testing"

	"relay/internal/envelope"
)

// The tests below pin the decode-to-match contract for normalized CDC events:
// envelope.Decode exposes the connector's source location under exactly one
// field — tableName for PostgreSQL, collectionName for MongoDB — and a parsed
// pattern must address that same field. A pattern naming the other connector's
// field never matches the decoded event.

// postgresDebeziumChange is one schemaless Debezium PostgreSQL change on
// inventory.customers, as the Redis sink carries it in `value`.
const postgresDebeziumChange = `{
	"before": null,
	"after": {"id": 1, "email": "new@acme.test"},
	"source": {
		"version": "3.7.0.Final",
		"connector": "postgresql",
		"name": "dbz",
		"ts_ms": 1767200000000,
		"snapshot": "false",
		"db": "inventory",
		"schema": "public",
		"table": "customers"
	},
	"op": "u",
	"ts_ms": 1767200000100,
	"transaction": null
}`

// mongoDebeziumChange is one schemaless Debezium MongoDB change on
// inventory.customers; images travel as the connector's JSON strings.
const mongoDebeziumChange = `{
	"before": null,
	"after": "{\"_id\":{\"$oid\":\"663c9c0f0000000000000111\"}}",
	"source": {
		"version": "3.7.0.Final",
		"connector": "mongodb",
		"name": "dbz",
		"ts_ms": 1767200000000,
		"snapshot": "last",
		"db": "inventory",
		"rs": "rs0",
		"collection": "customers",
		"ord": 42,
		"stage": "RS0_8012"
	},
	"op": "c",
	"ts_ms": 1767200000100,
	"transaction": null
}`

// decodeCDCEvent runs one Debezium value through envelope.Decode (no format
// marker) and returns the normalized event the matcher sees.
func decodeCDCEvent(t *testing.T, value string) map[string]any {
	t.Helper()
	res, err := envelope.Decode("", []byte(value))
	if err != nil {
		t.Fatalf("envelope.Decode: %v", err)
	}
	if res.Kind != envelope.KindEvent {
		t.Fatalf("Kind = %v, want KindEvent", res.Kind)
	}
	return res.Event
}

// locationTemplate renders a one-rule template whose pattern addresses one
// location field, e.g. `tableName: [customers]`.
func locationTemplate(field, location string) string {
	return `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      ` + field + `: [` + location + `]
`
}

// TestCDCEventMatchesConnectorLocationPatternOnly pins that a decoded CDC
// event matches a parsed pattern naming its own connector's location field and
// never the other connector's field: PostgreSQL exposes tableName (never
// collectionName), MongoDB exposes collectionName (never tableName).
func TestCDCEventMatchesConnectorLocationPatternOnly(t *testing.T) {
	cases := []struct {
		name     string
		value    string
		location string
		ownKey   string
		otherKey string
	}{
		{"postgresql", postgresDebeziumChange, "customers",
			"tableName", "collectionName"},
		{"mongodb", mongoDebeziumChange, "customers",
			"collectionName", "tableName"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeCDCEvent(t, tc.value)

			own := mustParse(t, locationTemplate(tc.ownKey, tc.location))
			other := mustParse(t, locationTemplate(tc.otherKey, tc.location))

			if !matches(t, own, got) {
				t.Errorf("expected the %s pattern to match the %s event",
					tc.ownKey, tc.name)
			}
			if matches(t, other, got) {
				t.Errorf("expected no match: the %s event does not expose %s",
					tc.name, tc.otherKey)
			}
		})
	}
}

// Package envelope decodes the logical envelope carried by one Relay Redis
// stream entry.
//
// A stream entry carries its payload in `value` and an optional `format`
// marker. Every other entry field — including the Debezium Redis sink's `key`
// and uppercased headers — is not part of the Relay envelope and is ignored by
// this package. Decoding is deliberately independent of Redis extraction, event
// matching, execution, and invocation state: this package only turns an
// extracted value into one of three outcomes:
//
//   - an ordinary event map (raw JSON or normalized Debezium change), with the
//     exact logical handler payload;
//   - a validated schedule occurrence, with its handler payload;
//   - a confirmed Debezium tombstone that may be acknowledged without
//     execution.
//
// Any other input is a non-retryable decode error. An optional `format=raw`
// marker overrides structure recognition and forces a raw JSON object decode.
//
// Debezium input requires the Redis sink's extended message format, but Relay
// reads only `value`; the change is there and the record key is not consumed.
package envelope

import (
	"bytes"
	"encoding/json"
	"fmt"

	"relay/internal/schedule"
)

// FormatRaw is the explicit transport marker that forces raw JSON decode. It
// takes precedence over schedule and Debezium structure recognition.
const FormatRaw = "raw"

// The Debezium Redis sink default for a null value in extended mode. A
// tombstone is recognized only when `value` is exactly this sentinel. The
// transport `key` is not part of the Relay envelope, so it cannot confirm or
// deny a tombstone. Only this exact default is recognized: a custom
// `debezium.sink.redis.null.value` is never silently skipped.
const defaultNullValue = "default"

// The supported Debezium source connectors.
const (
	connectorMongoDB    = "mongodb"
	connectorPostgreSQL = "postgresql"
)

// Kind is the decoded kind of one stream envelope.
type Kind int

const (
	// KindEvent is an ordinary event (raw JSON or normalized Debezium change).
	KindEvent Kind = iota
	// KindSchedule is a structurally valid schedule occurrence.
	KindSchedule
	// KindTombstone is a reliably identified Debezium tombstone to ignore.
	KindTombstone
)

// Result is the decoded envelope. Event is set for KindEvent, Occurrence for
// KindSchedule, and Payload is the exact logical handler payload for both.
type Result struct {
	Kind       Kind
	Event      map[string]any
	Occurrence schedule.Occurrence
	Payload    []byte
}

// Decode decodes one extracted transport envelope. format is the optional
// `format` field (empty when absent) and value is the required `value` payload.
// The caller applies its raw byte cap to value before calling here.
func Decode(format string, value []byte) (Result, error) {
	switch format {
	case "":
		return decodeStructured(value)
	case FormatRaw:
		return decodeRaw(value)
	default:
		return Result{}, fmt.Errorf("unknown envelope format %q", format)
	}
}

// decodeRaw forces a JSON object decode, ignoring any structure that would
// otherwise look like a schedule envelope or a Debezium change.
func decodeRaw(value []byte) (Result, error) {
	event, err := decodeJSONObject(value)
	if err != nil {
		return Result{}, err
	}
	return rawResult(event)
}

// decodeStructured applies structural recognition in precedence order: a
// confirmed tombstone, a schedule claim, a Debezium change, then raw JSON.
// Tombstone recognition uses only the exact default null-value sentinel; a
// custom Debezium null.value cannot be distinguished from a real payload and
// follows the normal decode path. Any other malformed value still fails
// non-retryably rather than being skipped.
func decodeStructured(value []byte) (Result, error) {
	if bytes.Equal(value, []byte(defaultNullValue)) {
		return Result{Kind: KindTombstone}, nil
	}

	event, err := decodeJSONObject(value)
	if err != nil {
		return Result{}, err
	}
	if source, ok := event["source"].(string); ok && source == "relay.schedule" {
		return decodeSchedule(event)
	}
	if payload, claimed := debeziumPayload(event); claimed {
		return decodeDebezium(payload, value)
	}
	return rawResult(event)
}

// decodeJSONObject decodes the envelope value into a JSON object.
func decodeJSONObject(value []byte) (map[string]any, error) {
	var event map[string]any
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, fmt.Errorf("decode envelope value: %w", err)
	}
	if event == nil {
		return nil, fmt.Errorf("envelope value is not a JSON object")
	}
	return event, nil
}

// rawResult returns an ordinary event. The replay payload is the same canonical
// JSON the live runner hands to the handler (json.Marshal of the decoded map).
func rawResult(event map[string]any) (Result, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return Result{}, fmt.Errorf("encode raw event payload: %w", err)
	}
	return Result{Kind: KindEvent, Event: event, Payload: payload}, nil
}

// decodeSchedule validates a schedule claim. The occurrence identity is derived
// from the envelope fields and cross-checked inside ClassifyScheduleEnvelope, so
// a schedule entry cannot be redirected to a different occurrence. The handler
// payload is Occurrence.Payload(), never the coordination envelope.
func decodeSchedule(event map[string]any) (Result, error) {
	occ, kind, err := schedule.ClassifyScheduleEnvelope(event)
	if kind != schedule.ValidScheduleEnvelope {
		if err == nil {
			err = fmt.Errorf("schedule envelope is invalid")
		}
		return Result{}, fmt.Errorf("schedule envelope: %w", err)
	}
	return Result{
		Kind:       KindSchedule,
		Occurrence: occ,
		Payload:    occ.Payload(),
	}, nil
}

// debeziumPayload extracts the record payload from a schema-enabled wrapper or
// a schemaless object. A schema wrapper is itself Debezium evidence when the
// payload has a source connector and a CDC marker; a schemaless object needs
// stronger source evidence so a raw object cannot be claimed by `source`/`op`
// coincidence alone.
//
// The claim deliberately does NOT require a valid `op` or a valid source
// location, so a recognized record with a missing, empty, unknown, or ill-typed
// operation, location, or image is a non-retryable error rather than a raw
// event. A raw object that merely contains `source`, `op`, or a partial CDC
// shape is not claimed and stays raw unless `format=raw` forced that path.
func debeziumPayload(event map[string]any) (map[string]any, bool) {
	if schema, hasSchema := event["schema"]; hasSchema {
		payload, ok := event["payload"].(map[string]any)
		if ok {
			if isDebeziumPayload(payload) {
				return payload, true
			}
			if _, schemaIsObject := schema.(map[string]any); schemaIsObject &&
				isDebeziumWrapperPayload(payload) {
				return payload, true
			}
		}
	}
	if isDebeziumPayload(event) {
		return event, true
	}
	return nil, false
}

// isDebeziumPayload reports whether payload is a clear schemaless CDC claim. It
// requires a `source` object naming a connector, a CDC marker, and source
// evidence beyond the connector: a connector location (`collection`/`table`) or
// another known Debezium source field. A schema wrapper supplies the extra
// evidence instead (see isDebeziumWrapperPayload). The claim deliberately does
// not require valid field values, so a recognized record with a missing,
// empty, unknown, or ill-typed operation, location, or image is rejected by
// decodeDebezium rather than becoming raw.
func isDebeziumPayload(payload map[string]any) bool {
	source, ok := payload["source"].(map[string]any)
	if !ok {
		return false
	}
	if _, hasConnector := source["connector"]; !hasConnector {
		return false
	}
	if !hasCDCEnvelopeShape(payload) {
		return false
	}
	return hasConnectorLocation(source) || hasDebeziumSourceMetadata(source)
}

// isDebeziumWrapperPayload reports whether a schema wrapper's payload has a
// source connector and a CDC marker. The wrapper schema is the additional
// evidence, so a missing or malformed source location still claims Debezium
// structure and is rejected by decodeDebezium rather than falling back to raw.
func isDebeziumWrapperPayload(payload map[string]any) bool {
	source, ok := payload["source"].(map[string]any)
	if !ok {
		return false
	}
	if _, ok := source["connector"]; !ok {
		return false
	}
	return hasCDCEnvelopeShape(payload)
}

// hasConnectorLocation reports whether the source names a table or collection.
// It checks key presence only: either field being ill-typed is a malformed
// recognized claim, not a reason to fall back to raw.
func hasConnectorLocation(source map[string]any) bool {
	_, hasCollection := source["collection"]
	_, hasTable := source["table"]
	return hasCollection || hasTable
}

// hasDebeziumSourceMetadata reports whether the source carries a Debezium source
// field other than the connector/location. Presence is enough; values are
// validated by decodeDebezium once the record has claimed Debezium structure.
func hasDebeziumSourceMetadata(source map[string]any) bool {
	for _, field := range [...]string{
		"version", "name", "ts_ms", "snapshot", "db", "schema",
		"sequence", "rs", "change_lsn", "commit_lsn", "event_serial_no",
	} {
		if _, ok := source[field]; ok {
			return true
		}
	}
	return false
}

// hasCDCEnvelopeShape reports whether the payload has a CDC envelope shape: an
// `op` field, or both image fields. A record with only a source connector and
// location, or with only one partial image field, remains raw; a record with an
// operation or a complete before/after shape is a CDC claim whose operation and
// images are then validated by decodeDebezium.
func hasCDCEnvelopeShape(payload map[string]any) bool {
	if _, hasOp := payload["op"]; hasOp {
		return true
	}
	_, hasBefore := payload["before"]
	_, hasAfter := payload["after"]
	return hasBefore && hasAfter
}

// decodeDebezium normalizes one clear Debezium change. A missing, empty, or
// ill-typed operation, an unsupported connector, a missing or ill-typed source
// location, or an invalid non-null image is non-retryable rather than falling
// back to raw matching, because the record already claimed Debezium structure.
// A missing image field is treated as null; only non-null values that are not
// objects (or invalid MongoDB JSON strings) are rejected.
//
// raw is the original envelope value. Once the connector is known to be
// MongoDB, the payload is re-decoded with json.Decoder.UseNumber so integer
// values in direct object images are not rounded through float64. No other
// connector, and no raw event, takes that path.
func decodeDebezium(payload map[string]any, raw []byte) (Result, error) {
	source, ok := payload["source"].(map[string]any)
	if !ok {
		return Result{}, fmt.Errorf("debezium: source is not an object")
	}
	connector, ok := source["connector"].(string)
	if !ok || connector == "" {
		return Result{}, fmt.Errorf(
			"debezium: source.connector is missing or not a non-empty string")
	}
	if connector == connectorMongoDB && raw != nil {
		exact, err := decodeMongoPayload(raw)
		if err != nil {
			return Result{}, fmt.Errorf("debezium: %w", err)
		}
		payload = exact
		source, ok = payload["source"].(map[string]any)
		if !ok {
			return Result{}, fmt.Errorf("debezium: source is not an object")
		}
	}
	op, ok := payload["op"].(string)
	if !ok || op == "" {
		return Result{}, fmt.Errorf(
			"debezium: op is missing or not a non-empty string")
	}
	recordType, ok := recordTypeForOp(op)
	if !ok {
		return Result{}, fmt.Errorf("debezium: unsupported op %q", op)
	}
	collection, err := collectionName(source, connector)
	if err != nil {
		return Result{}, err
	}
	newImage, err := normalizedImage("newImage", payload["after"], connector)
	if err != nil {
		return Result{}, err
	}
	oldImage, err := normalizedImage("oldImage", payload["before"], connector)
	if err != nil {
		return Result{}, err
	}
	event := map[string]any{
		"collectionName": collection,
		"recordType":     recordType,
		"newImage":       newImage,
		"oldImage":       oldImage,
	}
	data, err := json.Marshal(event)
	if err != nil {
		return Result{}, fmt.Errorf("encode debezium event payload: %w", err)
	}
	return Result{Kind: KindEvent, Event: event, Payload: data}, nil
}

// recordTypeForOp maps Debezium operations onto the normalized record types. A
// snapshot read (r) is an existing captured row and is treated as an INSERT.
func recordTypeForOp(op string) (string, bool) {
	switch op {
	case "c", "r":
		return "INSERT", true
	case "u":
		return "UPDATE", true
	case "d":
		return "DELETE", true
	default:
		return "", false
	}
}

func collectionName(source map[string]any, connector string) (string, error) {
	switch connector {
	case connectorMongoDB:
		collection, ok := source["collection"].(string)
		if !ok || collection == "" {
			return "", fmt.Errorf(
				"debezium: mongodb source has no collection name")
		}
		return collection, nil
	case connectorPostgreSQL:
		table, ok := source["table"].(string)
		if !ok || table == "" {
			return "", fmt.Errorf(
				"debezium: postgresql source has no table name")
		}
		return table, nil
	default:
		return "", fmt.Errorf(
			"debezium: unsupported source connector %q", connector)
	}
}

// normalizedImage decodes a MongoDB JSON-string image and validates that the
// resulting image is an object or null. A claimed Debezium record must never
// carry a scalar (or other malformed) image silently into the handler payload.
func normalizedImage(field string, value any, connector string) (any, error) {
	if connector == connectorMongoDB {
		decoded, err := decodeMongoImage(value)
		if err != nil {
			return nil, fmt.Errorf("debezium: %s: %w", field, err)
		}
		value = decoded
	}
	if value == nil {
		return nil, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"debezium: %s must be a JSON object or null", field)
	}
	if connector == connectorMongoDB {
		// A top-level Extended JSON wrapper is not a document image, so
		// validate the normalized result rather than only its input shape.
		normalized, err := normalizeMongoImage(object)
		if err != nil {
			return nil, fmt.Errorf("debezium: %s: %w", field, err)
		}
		value = normalized
	}
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return v, nil
	default:
		return nil, fmt.Errorf(
			"debezium: %s must be a JSON object or null", field)
	}
}

// decodeMongoImage decodes the JSON string in which the MongoDB connector wraps
// a document image. Decoding is strict: a serialized image that is not valid
// JSON, or that decodes to anything except an object or null, is an error
// rather than an opaque/scalar value. A non-string image is returned unchanged
// so normalizedImage can validate its shape.
func decodeMongoImage(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		decoded, err := decodeMongoJSON([]byte(v))
		if err != nil {
			return nil, fmt.Errorf("decode JSON image: %w", err)
		}
		switch decoded.(type) {
		case nil, map[string]any:
			return decoded, nil
		default:
			return nil, fmt.Errorf(
				"decoded image is not a JSON object or null")
		}
	default:
		return value, nil
	}
}

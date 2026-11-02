package envelope

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// MongoDB Extended JSON normalization. Only an exact single-key wrapper is
// treated as a BSON value: an application document such as
// {"$date": "...", "note": "..."} is ordinary data and stays untouched.
//
// Every conversion is exact or is not performed at all. A malformed wrapper is
// preserved rather than guessed, except for $date: a recognized date that
// cannot be represented is a non-retryable error, because silently keeping a
// different instant would be worse than failing the delivery.

// normalizeMongoImage normalizes one MongoDB document image recursively. The
// result can be a scalar, so the caller must still validate that the image is a
// document (an exact top-level $oid wrapper, for example, is not one).
func normalizeMongoImage(value map[string]any) (any, error) {
	return normalizeMongoObject(value)
}

func normalizeMongoObject(value map[string]any) (any, error) {
	if len(value) == 1 {
		for key, raw := range value {
			switch key {
			case "$oid":
				if oid, ok := mongoObjectID(raw); ok {
					return oid, nil
				}
				return value, nil
			case "$date":
				return mongoDate(raw)
			case "$numberInt":
				if n, ok := mongoNumberInt(raw); ok {
					return n, nil
				}
				return value, nil
			case "$numberLong":
				if n, ok := mongoNumberLong(raw); ok {
					return n, nil
				}
				return value, nil
			case "$numberDouble":
				if n, ok := mongoNumberDouble(raw); ok {
					return n, nil
				}
				return value, nil
			case "$numberDecimal":
				// Decimal128 has no exact Go/JSON numeric equivalent; keep
				// the textual wrapper rather than approximating with float64.
				return value, nil
			case "$binary":
				// Preserve base64/subtype exactly; there is no safe flattening.
				return value, nil
			case "$timestamp":
				// Keep both fields and the wrapper; normalizing the numeric
				// fields lets the matcher compare t/i without a float64 step.
				if fields, ok := raw.(map[string]any); ok {
					for field, child := range fields {
						normalized, err := normalizeMongoValue(child)
						if err != nil {
							return nil, err
						}
						fields[field] = normalized
					}
				}
				return value, nil
			}
		}
	}
	for key, child := range value {
		normalized, err := normalizeMongoValue(child)
		if err != nil {
			return nil, err
		}
		value[key] = normalized
	}
	return value, nil
}

func normalizeMongoValue(value any) (any, error) {
	switch v := value.(type) {
	case map[string]any:
		return normalizeMongoObject(v)
	case []any:
		for i, child := range v {
			normalized, err := normalizeMongoValue(child)
			if err != nil {
				return nil, err
			}
			v[i] = normalized
		}
		return v, nil
	case json.Number:
		return normalizeMongoJSONNumber(v), nil
	default:
		return value, nil
	}
}

// normalizeMongoJSONNumber turns a plain JSON number into a native Go numeric
// value when its JSON numeric spelling preserves the value, so matcher numeric
// rules keep working. Values that would be rounded are retained as json.Number
// instead of being rejected by json.Marshal.
func normalizeMongoJSONNumber(number json.Number) any {
	text := number.String()
	if isSignedDecimalInteger(text) {
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return n
		}
		if n, err := strconv.ParseUint(text, 10, 64); err == nil {
			return n
		}
		return number
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return number
	}
	input, ok := new(big.Rat).SetString(text)
	if !ok {
		return number
	}
	spelling, ok := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
	if !ok || input.Cmp(spelling) != 0 {
		return number
	}
	return f
}

// mongoObjectID validates the canonical lowercase 24-hex ObjectId form before
// exposing it as a string.
func mongoObjectID(value any) (string, bool) {
	oid, ok := value.(string)
	if !ok || len(oid) != 24 {
		return "", false
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	return oid, true
}

func mongoNumberInt(value any) (int64, bool) {
	text, ok := value.(string)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(text, 10, 32)
	if err != nil {
		return 0, false
	}
	return n, true
}

func mongoNumberLong(value any) (int64, bool) {
	text, ok := value.(string)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// mongoNumberDouble converts only finite doubles. JSON cannot represent NaN or
// infinities, so those wrappers are preserved exactly.
func mongoNumberDouble(value any) (float64, bool) {
	text, ok := value.(string)
	if !ok || !isJSONNumber(text) {
		return 0, false
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// isJSONNumber accepts only the standard JSON number grammar. ParseFloat also
// accepts Go-specific spellings (such as hexadecimal floats and digit
// separators) that are not valid Extended JSON $numberDouble values.
func isJSONNumber(text string) bool {
	if text == "" {
		return false
	}
	i := 0
	if text[i] == '-' {
		i++
		if i == len(text) {
			return false
		}
	}
	if text[i] == '0' {
		i++
	} else if text[i] >= '1' && text[i] <= '9' {
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
	} else {
		return false
	}
	if i < len(text) && text[i] == '.' {
		i++
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			i++
		}
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(text)
}

const (
	maxInt64Value = uint64(1<<63 - 1)
	// two63 is exactly representable as float64, unlike MaxInt64.
	two63 = float64(1 << 63)
)

// mongoDate normalizes one exact {"$date": ...} wrapper to an RFC3339 UTC
// string. MongoDB accepts integer Unix milliseconds, an ISO-8601 string, or a
// canonical nested {"$numberLong":"..."} wrapper; a recognized date that
// cannot be represented exactly is an error, never a silently different
// instant.
func mongoDate(value any) (any, error) {
	switch v := value.(type) {
	case string:
		return mongoDateString(v)
	case map[string]any:
		return mongoDateFromNumberLong(v)
	case json.Number:
		ms, ok := mongoDateMillis(v.String())
		if !ok {
			return nil, fmt.Errorf("invalid MongoDB $date milliseconds %q", v)
		}
		return mongoDateFromMillis(ms)
	case float64:
		return mongoDateFromFloatingMillis(v)
	case float32:
		return mongoDateFromFloatingMillis(float64(v))
	default:
		ms, ok := mongoDateIntegerMillis(value)
		if !ok {
			return nil, fmt.Errorf("invalid MongoDB $date value %v (%T)", value, value)
		}
		return mongoDateFromMillis(ms)
	}
}

func mongoDateFromNumberLong(value map[string]any) (any, error) {
	if len(value) != 1 {
		return nil, fmt.Errorf(
			"invalid MongoDB $date: nested value is not an exact $numberLong wrapper")
	}
	raw, ok := value["$numberLong"]
	if !ok {
		return nil, fmt.Errorf(
			"invalid MongoDB $date: nested value is not an exact $numberLong wrapper")
	}
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf(
			"invalid MongoDB $date $numberLong value %v (%T)", raw, raw)
	}
	ms, ok := mongoDateMillis(text)
	if !ok {
		return nil, fmt.Errorf("invalid MongoDB $date $numberLong %q", text)
	}
	return mongoDateFromMillis(ms)
}

func mongoDateString(text string) (any, error) {
	if !mongoDateFractionFitsNanoseconds(text) {
		return nil, fmt.Errorf(
			"MongoDB $date %q has precision beyond nanoseconds", text)
	}
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil, fmt.Errorf("invalid MongoDB $date %q: %w", text, err)
	}
	return mongoDateFromTime(t)
}

// mongoDateFractionFitsNanoseconds reports whether an ISO timestamp's
// fractional seconds are exactly representable at nanosecond precision. Go
// truncates longer fractions silently, which must not happen to a date.
func mongoDateFractionFitsNanoseconds(text string) bool {
	dot := strings.IndexByte(text, '.')
	if dot < 0 {
		return true
	}
	end := dot + 1
	for end < len(text) && text[end] >= '0' && text[end] <= '9' {
		end++
	}
	for i := dot + 10; i < end; i++ {
		if text[i] != '0' {
			return false
		}
	}
	return true
}

func mongoDateFromMillis(ms int64) (any, error) {
	return mongoDateFromTime(time.UnixMilli(ms))
}

func mongoDateFromTime(t time.Time) (any, error) {
	t = t.UTC()
	if t.Year() < 0 || t.Year() > 9999 {
		return nil, fmt.Errorf(
			"MongoDB $date %s is outside the RFC3339 range",
			t.Format(time.RFC3339Nano))
	}
	return t.Format(time.RFC3339Nano), nil
}

func mongoDateFromFloatingMillis(ms float64) (any, error) {
	if math.IsNaN(ms) || math.IsInf(ms, 0) || math.Trunc(ms) != ms ||
		ms < -two63 || ms >= two63 {
		return nil, fmt.Errorf("invalid MongoDB $date milliseconds %v", ms)
	}
	return mongoDateFromMillis(int64(ms))
}

func mongoDateMillis(text string) (int64, bool) {
	if !isSignedDecimalInteger(text) {
		return 0, false
	}
	ms, err := strconv.ParseInt(text, 10, 64)
	return ms, err == nil
}

func mongoDateIntegerMillis(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return mongoDateUnsignedMillis(uint64(v))
	case uint8:
		return mongoDateUnsignedMillis(uint64(v))
	case uint16:
		return mongoDateUnsignedMillis(uint64(v))
	case uint32:
		return mongoDateUnsignedMillis(uint64(v))
	case uint64:
		return mongoDateUnsignedMillis(v)
	default:
		return 0, false
	}
}

func mongoDateUnsignedMillis(value uint64) (int64, bool) {
	if value > maxInt64Value {
		return 0, false
	}
	return int64(value), true
}

func isSignedDecimalInteger(text string) bool {
	if text == "" {
		return false
	}
	if text[0] == '+' || text[0] == '-' {
		text = text[1:]
		if text == "" {
			return false
		}
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return false
		}
	}
	return true
}

// decodeMongoJSON decodes MongoDB JSON without the float64 conversion that
// json.Unmarshal performs, so plain integer values and date milliseconds keep
// their exact literal until the normalizer chooses a native Go type.
func decodeMongoJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing JSON value")
		}
		return nil, err
	}
	return decoded, nil
}

// decodeMongoPayload re-decodes a claimed MongoDB envelope with exact numbers.
// It is called only after structure recognition has established the MongoDB
// connector, so raw and PostgreSQL decoding keep their existing numeric
// semantics.
func decodeMongoPayload(raw []byte) (map[string]any, error) {
	decoded, err := decodeMongoJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("decode MongoDB envelope: %w", err)
	}
	event, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("MongoDB envelope is not a JSON object")
	}
	payload, claimed := debeziumPayload(event)
	if !claimed {
		return nil, fmt.Errorf(
			"MongoDB envelope no longer claims Debezium structure")
	}
	return payload, nil
}

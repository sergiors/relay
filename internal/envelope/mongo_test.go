package envelope

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// The tests below pin the MongoDB Extended JSON normalization contract that
// only MongoDB envelopes and images take: every EXACT single-key wrapper
// normalizes to the native Go/JSON value it exactly represents, and anything
// else — an object that merely carries a wrapper-like key, a malformed or
// unknown wrapper, another connector, a raw event — is preserved rather than
// guessed. A recognized date that cannot be represented (an out-of-range or
// over-precise $date) is an error, never a silently different instant.

// sampleMillis is a fixed Unix-milliseconds instant used across the date
// tests; its expected normalized spelling is always computed from it, so the
// expectations never encode a hand-written date that could drift.
const sampleMillis = int64(1767200000123)

// dateOfMillis renders the exact normalized RFC3339Nano UTC string the
// normalizer must produce for one Unix-milliseconds instant.
func dateOfMillis(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

// mustNormalizeValue runs one scalar/child normalization, failing the test on
// any error (plain numbers and scalars cannot error).
func mustNormalizeValue(t *testing.T, v any) any {
	t.Helper()
	out, err := normalizeMongoValue(v)
	if err != nil {
		t.Fatalf("normalizeMongoValue(%v): %v", v, err)
	}
	return out
}

// mustNormalize runs one image normalization, failing the test on any error.
// The normalizer mutates its input in place and may return a scalar.
func mustNormalize(t *testing.T, in map[string]any) any {
	t.Helper()
	out, err := normalizeMongoImage(in)
	if err != nil {
		t.Fatalf("normalizeMongoImage(%s): %v", normalizeAsJSON(t, in), err)
	}
	return out
}

// normalizeAsJSON renders a value as canonical JSON, for wrapper-map
// comparisons that must not depend on key order.
func normalizeAsJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return string(b)
}

// assertNormalizedString asserts got is exactly a string with the given value
// (type and content), e.g. an unwrapped ObjectId or a normalized date.
func assertNormalizedString(t *testing.T, label string, got any, want string) {
	t.Helper()
	s, ok := got.(string)
	if !ok {
		t.Fatalf("%s = %v (%T), want string %q", label, got, got, want)
	}
	if s != want {
		t.Fatalf("%s = %q, want %q", label, s, want)
	}
}

// assertNormalizedInt64 asserts got is exactly an int64 (never float64 or
// json.Number) with the given value, so pinned conversions stay typed.
func assertNormalizedInt64(t *testing.T, label string, got any, want int64) {
	t.Helper()
	n, ok := got.(int64)
	if !ok {
		t.Fatalf("%s = %v (%T), want int64 %d", label, got, got, want)
	}
	if n != want {
		t.Fatalf("%s = %d, want %d", label, n, want)
	}
}

// assertNormalizedFloat64 asserts got is exactly a float64 with the value.
func assertNormalizedFloat64(t *testing.T, label string, got any, want float64) {
	t.Helper()
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("%s = %v (%T), want float64 %v", label, got, got, want)
	}
	if f != want {
		t.Fatalf("%s = %v, want %v", label, f, want)
	}
}

// assertPreservedWrapper asserts the normalization left the exact wrapper map
// in place, byte-for-byte, rather than guessing at a native value.
func assertPreservedWrapper(t *testing.T, in map[string]any) any {
	t.Helper()
	out := mustNormalize(t, in)
	if got := normalizeAsJSON(t, out); got != normalizeAsJSON(t, in) {
		t.Fatalf("wrapper %s was mutated: normalized to %s", normalizeAsJSON(t, in), got)
	}
	return out
}

// TestMongoOidWrapperNormalizesCanonicalFormOnly pins the ObjectId contract: a
// canonical 24-character lowercase hex wrapper unwraps to exactly a string;
// anything else stays the wrapper.
func TestMongoOidWrapperNormalizesCanonicalFormOnly(t *testing.T) {
	t.Run("canonical form unwraps to exactly a string", func(t *testing.T) {
		const oid = "663c9c0f0000000000000111"
		assertNormalizedString(t, "$oid", mustNormalize(t, map[string]any{"$oid": oid}), oid)
	})

	t.Run("non-canonical forms are preserved, not guessed", func(t *testing.T) {
		for _, bad := range []string{
			"663C9C0F0000000000000111",  // uppercase hex
			"663c9c0f000000000000011",   // 23 chars
			"663c9c0f00000000000001112", // 25 chars
			"663c9c0f0000000000000g11",  // non-hex character
			"",                          // empty string
		} {
			assertPreservedWrapper(t, map[string]any{"$oid": bad})
		}
	})

	t.Run("non-string wrapper value is preserved", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{"$oid": json.Number("663")})
	})
}

// TestMongoDateStringWrapperNormalizesToUTC pins every accepted ISO-8601 $date
// string form: offsets fold to UTC, fractions survive through nanosecond
// precision, and every output is the RFC3339Nano UTC ("Z") spelling.
func TestMongoDateStringWrapperNormalizesToUTC(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"Z, no fraction", "2026-01-17T10:33:00Z", "2026-01-17T10:33:00Z"},
		{"positive offset folds to UTC", "2026-01-17T11:33:00+01:00", "2026-01-17T10:33:00Z"},
		{"negative offset folds to UTC", "2026-01-17T05:33:00-05:00", "2026-01-17T10:33:00Z"},
		{"one fraction digit", "2026-01-17T10:33:00.5Z", "2026-01-17T10:33:00.5Z"},
		{"nine fraction digits", "2026-01-17T10:33:00.123456789Z", "2026-01-17T10:33:00.123456789Z"},
		{"ten digits, zero tenth", "2026-01-17T10:33:00.0123456780Z", "2026-01-17T10:33:00.012345678Z"},
		{"twelve digits, zero tail", "2026-01-17T10:33:00.123000000000Z", "2026-01-17T10:33:00.123Z"},
		{"already normalized RFC3339Nano", "2026-03-05T07:08:09.000000001Z", "2026-03-05T07:08:09.000000001Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := mustNormalize(t, map[string]any{"$date": tc.in})
			assertNormalizedString(t, "$date", out, tc.want)
			if !strings.HasSuffix(tc.want, "Z") {
				t.Fatalf("test bug: every normalized date must be UTC (Z): %q", tc.want)
			}
		})
	}
}

// TestMongoDateMillisFormsNormalizeToUTC pins the numeric and nested-$numberLong
// millisecond forms: exact integer Unix milliseconds normalize to the same
// RFC3339Nano UTC instant whichever representation carried them.
func TestMongoDateMillisFormsNormalizeToUTC(t *testing.T) {
	want := dateOfMillis(sampleMillis)
	negative := dateOfMillis(-sampleMillis)

	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"nested $numberLong wrapper", map[string]any{"$numberLong": "1767200000123"}, want},
		{"nested $numberLong negative", map[string]any{"$numberLong": "-1767200000123"}, negative},
		{"plain JSON number", json.Number("1767200000123"), want},
		{"plain JSON number negative", json.Number("-1767200000123"), negative},
		{"float64 whole ms", float64(sampleMillis), want},
		{"native int", int(sampleMillis), want},
		{"int64", int64(sampleMillis), want},
		{"uint64 in range", uint64(sampleMillis), want},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := mongoDate(tc.value)
			if err != nil {
				t.Fatalf("mongoDate(%v): %v", tc.value, err)
			}
			assertNormalizedString(t, "$date", out, tc.want)
		})
	}

	t.Run("wrapper dispatch reaches every form", func(t *testing.T) {
		for _, value := range []any{
			map[string]any{"$numberLong": "1767200000123"},
			json.Number("1767200000123"),
			float64(sampleMillis),
			int64(sampleMillis),
		} {
			out := mustNormalize(t, map[string]any{"$date": value})
			assertNormalizedString(t, "$date", out, want)
		}
	})
}

// TestMongoDateUnrepresentableValuesError pins the error boundary: an invalid,
// over-precise, or out-of-RFC3339-range date is a non-retryable error naming
// the problem — never a silently different instant and never a preserved
// wrapper (a preserved $date would be a different instant on every read).
func TestMongoDateUnrepresentableValuesError(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"empty string", "", "invalid MongoDB $date"},
		{"date only", "2026-01-17", "invalid MongoDB $date"},
		{"space separator", "2026-01-17 10:33:00Z", "invalid MongoDB $date"},
		{"garbage", "not-a-date", "invalid MongoDB $date"},
		{"month overflow", "2026-13-01T00:00:00Z", "invalid MongoDB $date"},
		{"fraction beyond nanoseconds", "2026-01-17T10:33:00.1234567891Z", "precision beyond nanoseconds"},
		{"eleven digits, nonzero tail", "2026-01-17T10:33:00.00000000001Z", "precision beyond nanoseconds"},
		{"year 10000 via negative offset", "9999-12-31T23:59:59.999999999-01:00", "outside the RFC3339 range"},
		{"boolean", true, "invalid MongoDB $date value"},
		{"nil", nil, "invalid MongoDB $date value"},
		{"array", []any{"x"}, "invalid MongoDB $date value"},
		{"nested empty map", map[string]any{}, "not an exact $numberLong wrapper"},
		{"nested extra keys", map[string]any{"$numberLong": "1", "x": "y"}, "not an exact $numberLong wrapper"},
		{"nested wrong key", map[string]any{"$numberInt": "1"}, "not an exact $numberLong wrapper"},
		{"nested non-string", map[string]any{"$numberLong": 1}, "invalid MongoDB $date $numberLong"},
		{"nested non-integer text", map[string]any{"$numberLong": "1.5"}, "invalid MongoDB $date $numberLong"},
		{"nested overflowing ms", map[string]any{"$numberLong": "9223372036854775808"}, "invalid MongoDB $date $numberLong"},
		{"int64 max ms outside range", map[string]any{"$numberLong": "9223372036854775807"}, "outside the RFC3339 range"},
		{"int64 min ms outside range", map[string]any{"$numberLong": "-9223372036854775808"}, "outside the RFC3339 range"},
		{"plain number overflowing ms", json.Number("9223372036854775808"), "invalid MongoDB $date milliseconds"},
		{"float64 fractional ms", float64(1767200000123.5), "invalid MongoDB $date milliseconds"},
		{"float64 NaN", math.NaN(), "invalid MongoDB $date milliseconds"},
		{"float64 +Inf", math.Inf(1), "invalid MongoDB $date milliseconds"},
		{"float64 -Inf", math.Inf(-1), "invalid MongoDB $date milliseconds"},
		{"float64 just above 2^63 ms", math.Nextafter(math.Ldexp(1, 63), math.Inf(1)), "invalid MongoDB $date milliseconds"},
		{"float64 just below -2^63 ms", math.Nextafter(-math.Ldexp(1, 63), math.Inf(-1)), "invalid MongoDB $date milliseconds"},
		{"exactly -2^63 ms outside RFC3339 range", -math.Ldexp(1, 63), "outside the RFC3339 range"},
		{"uint64 max", uint64(math.MaxUint64), "invalid MongoDB $date value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := mongoDate(tc.value)
			if err == nil {
				t.Fatalf("mongoDate(%v) = %v (%T), want an error", tc.value, out, out)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want an error naming %q", err, tc.want)
			}
		})
	}

	t.Run("the wrapper path propagates the date error", func(t *testing.T) {
		in := map[string]any{"$date": "2026-13-01T00:00:00Z"}
		if out, err := normalizeMongoImage(in); err == nil {
			t.Fatalf("normalizeMongoImage(%v) = %v, want the invalid-date error", in, out)
		} else if !strings.Contains(err.Error(), "invalid MongoDB $date") {
			t.Fatalf("err = %v, want the invalid-date error", err)
		}
	})
}

// TestMongoNumberIntAndLongWrappers pins the integer wrappers: the parsed
// int64 value replaces the wrapper exactly within range, and out-of-range or
// ill-typed values are preserved rather than guessed.
func TestMongoNumberIntAndLongWrappers(t *testing.T) {
	t.Run("$numberInt converts within int32 range", func(t *testing.T) {
		assertNormalizedInt64(t, "$numberInt", mustNormalize(t, map[string]any{"$numberInt": "42"}), 42)
		assertNormalizedInt64(t, "$numberInt", mustNormalize(t, map[string]any{"$numberInt": "-42"}), -42)
		assertNormalizedInt64(t, "$numberInt", mustNormalize(t, map[string]any{"$numberInt": "2147483647"}), math.MaxInt32)
		assertNormalizedInt64(t, "$numberInt", mustNormalize(t, map[string]any{"$numberInt": "-2147483648"}), math.MinInt32)
	})

	t.Run("$numberInt out of range or non-integer is preserved", func(t *testing.T) {
		for _, bad := range []any{
			"2147483648", // one beyond int32 max
			"-2147483649",
			"1.5",
			"abc",
			json.Number("42"), // JSON numbers are not strings: never read as one
		} {
			assertPreservedWrapper(t, map[string]any{"$numberInt": bad})
		}
	})

	t.Run("$numberLong converts the full int64 range", func(t *testing.T) {
		assertNormalizedInt64(t, "$numberLong", mustNormalize(t, map[string]any{"$numberLong": "9223372036854775807"}), math.MaxInt64)
		assertNormalizedInt64(t, "$numberLong", mustNormalize(t, map[string]any{"$numberLong": "-9223372036854775808"}), math.MinInt64)
		assertNormalizedInt64(t, "$numberLong", mustNormalize(t, map[string]any{"$numberLong": "0"}), 0)
	})

	t.Run("$numberLong beyond int64 is preserved exact", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{"$numberLong": "9223372036854775808"})
		assertPreservedWrapper(t, map[string]any{"$numberLong": "-9223372036854775809"})
		assertPreservedWrapper(t, map[string]any{"$numberLong": ""})
	})
}

// TestMongoNumberDoubleWrapper pins the double wrapper: only finite doubles
// become float64; JSON cannot represent NaN/infinities (or overflows), so
// those wrappers are preserved exactly.
func TestMongoNumberDoubleWrapper(t *testing.T) {
	t.Run("finite doubles convert to float64", func(t *testing.T) {
		assertNormalizedFloat64(t, "$numberDouble", mustNormalize(t, map[string]any{"$numberDouble": "1.5"}), 1.5)
		assertNormalizedFloat64(t, "$numberDouble", mustNormalize(t, map[string]any{"$numberDouble": "3"}), 3)
		assertNormalizedFloat64(t, "$numberDouble", mustNormalize(t, map[string]any{"$numberDouble": "-0.25"}), -0.25)
		assertNormalizedFloat64(t, "$numberDouble", mustNormalize(t, map[string]any{"$numberDouble": "1e308"}), 1e308)
	})

	t.Run("nonfinite and invalid doubles are preserved", func(t *testing.T) {
		for _, bad := range []any{
			"NaN",
			"Infinity",
			"-Infinity",
			"+Inf",
			"-Inf",
			"1e309",   // ParseFloat range error -> +-Inf, never representable exactly
			"0x1p-2",  // Go hexadecimal float, not a JSON number
			"1_000.5", // Go digit separator, not a JSON number
			"+1.5",    // JSON numbers do not allow a leading plus
			"abc",
			"",
			json.Number("1.5"), // non-string value: never guessed
		} {
			assertPreservedWrapper(t, map[string]any{"$numberDouble": bad})
		}
	})
}

// TestMongoPreservedWrappers pins the wrappers with no exact native equivalent:
// $numberDecimal, $binary, and $timestamp survive byte-for-byte, with the
// timestamp's numeric t/i normalized to exact integers so matcher comparisons
// never take a float64 step.
func TestMongoPreservedWrappers(t *testing.T) {
	t.Run("$numberDecimal preserved exact", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{"$numberDecimal": "112.50"})
	})

	t.Run("$binary base64 and subtype unchanged", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{
			"$binary": map[string]any{"base64": "SGVsbG8=", "subType": "00"},
		})
	})

	t.Run("$timestamp keeps the wrapper and normalizes t/i to exact integers", func(t *testing.T) {
		out := mustNormalize(t, map[string]any{
			"$timestamp": map[string]any{"t": json.Number("1767200000"), "i": json.Number("1")},
		})
		wrapper, ok := out.(map[string]any)
		if !ok || len(wrapper) != 1 {
			t.Fatalf("$timestamp wrapper = %v (%T), want the one-key wrapper preserved", out, out)
		}
		fields, ok := wrapper["$timestamp"].(map[string]any)
		if !ok {
			t.Fatalf("$timestamp fields = %v (%T), want the object preserved", wrapper["$timestamp"], wrapper["$timestamp"])
		}
		assertNormalizedInt64(t, "t", fields["t"], 1767200000)
		assertNormalizedInt64(t, "i", fields["i"], 1)
	})

	t.Run("$timestamp recursively normalizes its field values", func(t *testing.T) {
		out := mustNormalize(t, map[string]any{
			"$timestamp": map[string]any{"i": map[string]any{"$numberLong": "5"}},
		})
		fields := out.(map[string]any)["$timestamp"].(map[string]any)
		assertNormalizedInt64(t, "i", fields["i"], 5)
	})

	t.Run("$timestamp with a non-object value is preserved", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{"$timestamp": json.Number("7")})
	})
}

// TestMongoPlainJSONNumbersNativeOrExact pins the plain-number rule (Mongo-only
// UseNumber decoding): a number becomes int64, uint64, or float64 when that
// preserves it exactly, and stays an exact json.Number when no native type
// can — never rounded.
func TestMongoPlainJSONNumbersNativeOrExact(t *testing.T) {
	cases := []struct {
		name string
		text string
		want any
	}{
		{"small integer", "42", int64(42)},
		{"negative integer", "-42", int64(-42)},
		{"zero", "0", int64(0)},
		{"int64 max", "9223372036854775807", int64(math.MaxInt64)},
		{"uint64 range", "18446744073709551615", uint64(math.MaxUint64)},
		{"beyond uint64 stays exact", "18446744073709551616", json.Number("18446744073709551616")},
		{"negative beyond int64 stays exact", "-9223372036854775809", json.Number("-9223372036854775809")},
		{"fraction becomes float64", "1.5", float64(1.5)},
		{"exponent fraction becomes float64", "1.5e0", float64(1.5)},
		{"high-precision fraction stays exact", "1.234567890123456789", json.Number("1.234567890123456789")},
		{"high-precision exponent stays exact", "1.234567890123456789e0", json.Number("1.234567890123456789e0")},
		{"float overflow stays exact", "1e400", json.Number("1e400")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mustNormalizeValue(t, json.Number(tc.text))
			switch want := tc.want.(type) {
			case json.Number:
				n, ok := got.(json.Number)
				if !ok || n.String() != want.String() {
					t.Fatalf("normalizeMongoValue(%s) = %v (%T), want exact json.Number(%s)", tc.text, got, got, want)
				}
			case int64:
				assertNormalizedInt64(t, tc.text, got, want)
			case uint64:
				n, ok := got.(uint64)
				if !ok || n != want {
					t.Fatalf("normalizeMongoValue(%s) = %v (%T), want uint64 %d", tc.text, got, got, want)
				}
			case float64:
				assertNormalizedFloat64(t, tc.text, got, want)
			}
		})
	}

	t.Run("non-numeric scalars pass through untouched", func(t *testing.T) {
		for _, v := range []any{"hello", true, false, nil} {
			if got := mustNormalizeValue(t, v); got != v {
				t.Fatalf("normalizeMongoValue(%v) = %v (%T), want it untouched", v, got, got)
			}
		}
	})
}

// TestMongoExactWrapperGuardKeepsOrdinaryDocuments pins the guard against
// misreading application data as BSON: an object carrying a wrapper-like key
// PLUS other fields is ordinary data. Its raw values stay raw (a $date string
// is NOT converted to a date), while its ordinary children still normalize as
// ordinary data.
func TestMongoExactWrapperGuardKeepsOrdinaryDocuments(t *testing.T) {
	t.Run("$date with extra fields is ordinary data", func(t *testing.T) {
		in := map[string]any{"$date": "2026-01-17T10:33:00Z", "note": "birthday"}
		out := mustNormalize(t, in)
		assertNormalizedString(t, "$date", out.(map[string]any)["$date"], "2026-01-17T10:33:00Z")
		if got := normalizeAsJSON(t, out); got != `{"$date":"2026-01-17T10:33:00Z","note":"birthday"}` {
			t.Fatalf("guard document changed: %s", got)
		}
	})

	t.Run("$oid with extra fields stays a wrapper while siblings normalize", func(t *testing.T) {
		in := map[string]any{"$oid": "663c9c0f0000000000000111", "extra": json.Number("1")}
		out := mustNormalize(t, in)
		if got := normalizeAsJSON(t, out); got != `{"$oid":"663c9c0f0000000000000111","extra":1}` {
			t.Fatalf("guard document changed: %s", got)
		}
		assertNormalizedInt64(t, "extra", out.(map[string]any)["extra"], 1)
	})

	t.Run("$numberLong with extra fields stays a wrapper", func(t *testing.T) {
		in := map[string]any{"$numberLong": "1767200000123", "unit": "ms"}
		out := mustNormalize(t, in)
		fields := out.(map[string]any)
		assertNormalizedString(t, "$numberLong", fields["$numberLong"], "1767200000123")
	})

	t.Run("unknown single-key $-wrapper is ordinary data", func(t *testing.T) {
		assertPreservedWrapper(t, map[string]any{"$weird": "x"})
	})

	t.Run("empty object stays empty", func(t *testing.T) {
		out := mustNormalize(t, map[string]any{})
		if got := normalizeAsJSON(t, out); got != "{}" {
			t.Fatalf("empty object became %s", got)
		}
	})
}

// TestMongoNormalizationIsIdempotent pins that an already-normalized image
// re-normalizes to itself: handlers and replay can re-run normalization safely.
func TestMongoNormalizationIsIdempotent(t *testing.T) {
	normalized := map[string]any{
		"_id":          "663c9c0f0000000000000111",
		"completed_at": "2026-01-17T10:33:00Z",
		"count":        int64(3),
		"price":        1.5,
		"big":          uint64(math.MaxUint64),
		"exact":        json.Number("18446744073709551616"),
		"tags":         []any{"rush", "663c9c0f00000000000007c5", int64(7)},
		"seller":       map[string]any{"tier": "gold", "_id": "663c9c0f00000000000009a1"},
	}
	out := mustNormalize(t, normalized)
	if got := normalizeAsJSON(t, out); got != normalizeAsJSON(t, normalized) {
		t.Fatalf("re-normalization changed the image: %s, want %s", got, normalizeAsJSON(t, normalized))
	}
	fields := out.(map[string]any)
	assertNormalizedInt64(t, "count", fields["count"], 3)
	assertNormalizedFloat64(t, "price", fields["price"], 1.5)
	assertNormalizedString(t, "completed_at", fields["completed_at"], "2026-01-17T10:33:00Z")
}

// TestMongoRecursiveNormalizationThroughDecode pins the end-to-end MongoDB
// change contract: JSON-string images decode with exact numbers and normalize
// recursively (nested documents, arrays, every wrapper), the event keeps
// exactly the four CDC fields, and the payload is the canonical handler JSON.
func TestMongoRecursiveNormalizationThroughDecode(t *testing.T) {
	afterDoc := func() map[string]any {
		return map[string]any{
			"_id":          map[string]any{"$oid": "663c9c0f0000000000000111"},
			"completed_at": map[string]any{"$date": map[string]any{"$numberLong": "1767200000123"}},
			"created":      map[string]any{"$date": "2026-01-17T11:33:00+01:00"},
			"qty":          map[string]any{"$numberInt": "-3"},
			"score":        map[string]any{"$numberDouble": "9.75"},
			"big":          map[string]any{"$numberLong": "9223372036854775807"},
			"total":        map[string]any{"$numberDecimal": "112.50"},
			"checksum":     map[string]any{"$binary": map[string]any{"base64": "SGVsbG8=", "subType": "00"}},
			"clock":        map[string]any{"$timestamp": map[string]any{"t": 1767200000, "i": 1}},
			"tags":         []any{"rush", map[string]any{"$oid": "663c9c0f00000000000007c5"}, map[string]any{"$date": "2026-02-01T00:00:00Z"}},
			"seller":       map[string]any{"tier": "gold", "_id": map[string]any{"$oid": "663c9c0f00000000000009a1"}},
		}
	}
	beforeDoc := func() map[string]any {
		return map[string]any{
			"_id":  map[string]any{"$oid": "663c9c0f0000000000000ddd"},
			"note": map[string]any{"$date": map[string]any{"$numberLong": "1767200000000"}},
		}
	}

	res, err := Decode("", compactJSON(t, mongoChange("u",
		mongoImage(t, beforeDoc()), mongoImage(t, afterDoc()), "orders")))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if res.Kind != KindEvent {
		t.Fatalf("Kind = %v, want KindEvent", res.Kind)
	}

	wantNew := map[string]any{
		"_id":          "663c9c0f0000000000000111",
		"completed_at": dateOfMillis(sampleMillis),
		"created":      "2026-01-17T10:33:00Z",
		"qty":          int64(-3),
		"score":        float64(9.75),
		"big":          int64(math.MaxInt64),
		"total":        map[string]any{"$numberDecimal": "112.50"},
		"checksum":     map[string]any{"$binary": map[string]any{"base64": "SGVsbG8=", "subType": "00"}},
		"clock":        map[string]any{"$timestamp": map[string]any{"t": int64(1767200000), "i": int64(1)}},
		"tags":         []any{"rush", "663c9c0f00000000000007c5", "2026-02-01T00:00:00Z"},
		"seller":       map[string]any{"tier": "gold", "_id": "663c9c0f00000000000009a1"},
	}
	wantOld := map[string]any{
		"_id":  "663c9c0f0000000000000ddd",
		"note": dateOfMillis(1767200000000),
	}
	assertFourFieldCDCEvent(t, res.Event, "orders", "UPDATE", wantNew, wantOld)

	// Exact Go types the matcher will see: integers are int64, doubles are
	// float64, dates are plain strings.
	newImage := res.Event["newImage"].(map[string]any)
	assertNormalizedInt64(t, "qty", newImage["qty"], -3)
	assertNormalizedFloat64(t, "score", newImage["score"], 9.75)
	assertNormalizedInt64(t, "big", newImage["big"], math.MaxInt64)
	assertNormalizedInt64(t, "clock.t", newImage["clock"].(map[string]any)["$timestamp"].(map[string]any)["t"], 1767200000)
	assertNormalizedString(t, "completed_at", newImage["completed_at"], dateOfMillis(sampleMillis))

	if string(res.Payload) != normalizeAsJSON(t, res.Event) {
		t.Fatalf("Payload = %s, want the canonical event %s", res.Payload, normalizeAsJSON(t, res.Event))
	}

	t.Run("wire value still carries wrappers", func(t *testing.T) {
		// Fixture sanity for the test above: the wire bytes predate any handler
		// payload normalization, so the $date wrapper — not its normalized
		// spelling — is what travels on the stream and what a DLQ entry would
		// preserve verbatim.
		raw := compactJSON(t, mongoChange("u", mongoImage(t, beforeDoc()), mongoImage(t, afterDoc()), "orders"))
		if strings.Contains(string(raw), dateOfMillis(sampleMillis)) {
			t.Fatalf("test bug: the wire value must still carry wrappers, not normalized dates: %s", raw)
		}
		if !strings.Contains(string(raw), `$date`) {
			t.Fatalf("test bug: the wire value must carry the $date wrapper: %s", raw)
		}
	})
}

// TestMongoObjectImageNormalizesPlainNumbers pins the direct-object image path:
// an image delivered as a JSON object (not the connector's string form) is
// re-decoded with exact numbers and normalized the same way — plain numbers
// become int64/uint64/float64 when exact, json.Number otherwise.
func TestMongoObjectImageNormalizesPlainNumbers(t *testing.T) {
	doc := map[string]any{
		"_id":    map[string]any{"$oid": "663c9c0f0000000000000111"},
		"count":  3,
		"price":  1.5,
		"maxu":   json.Number("18446744073709551615"),
		"beyond": json.Number("18446744073709551616"),
	}
	res, err := Decode("", compactJSON(t, mongoChange("c", nil, doc, "orders")))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	newImage := res.Event["newImage"].(map[string]any)
	assertNormalizedString(t, "_id", newImage["_id"], "663c9c0f0000000000000111")
	assertNormalizedInt64(t, "count", newImage["count"], 3)
	assertNormalizedFloat64(t, "price", newImage["price"], 1.5)
	if n, ok := newImage["maxu"].(uint64); !ok || n != math.MaxUint64 {
		t.Fatalf("maxu = %v (%T), want uint64 MaxUint64", newImage["maxu"], newImage["maxu"])
	}
	if n, ok := newImage["beyond"].(json.Number); !ok || n.String() != "18446744073709551616" {
		t.Fatalf("beyond = %v (%T), want exact json.Number", newImage["beyond"], newImage["beyond"])
	}
	assertFourFieldCDCEvent(t, res.Event, "orders", "INSERT", newImage, nil)
}

// TestMongoMalformedWrapperInImageFailsNonRetryably pins the image-level error
// boundary: a recognized-but-unrepresentable wrapper inside an image fails the
// whole decode (non-retryable, naming the CDC field), while unknown or
// malformed wrappers that ARE safely preservable never fail — and a scalar-only
// wrapper image is never silently handed to a handler as a scalar.
func TestMongoMalformedWrapperInImageFailsNonRetryably(t *testing.T) {
	t.Run("invalid $date inside image fails naming newImage", func(t *testing.T) {
		bad := map[string]any{
			"_id": map[string]any{"$oid": "663c9c0f0000000000000111"},
			"d":   map[string]any{"$date": "2026-13-45T00:00:00Z"},
		}
		res, err := Decode("", compactJSON(t, mongoChange("c", nil, mongoImage(t, bad), "orders")))
		if err == nil {
			t.Fatalf("Decode = %v, want the invalid-date error", res)
		}
		if !strings.Contains(err.Error(), "debezium: newImage") || !strings.Contains(err.Error(), "invalid MongoDB $date") {
			t.Fatalf("err = %v, want the debezium newImage invalid-date error", err)
		}
	})

	t.Run("out-of-range $date inside image fails", func(t *testing.T) {
		bad := map[string]any{"d": map[string]any{"$date": map[string]any{"$numberLong": "9223372036854775807"}}}
		if _, err := Decode("", compactJSON(t, mongoChange("c", nil, mongoImage(t, bad), "orders"))); err == nil {
			t.Fatal("Decode succeeded for an out-of-range $date, want an error")
		}
	})

	t.Run("non-exact $oid wrapper inside image is preserved, not an error", func(t *testing.T) {
		doc := map[string]any{
			"_id":   "663c9c0f0000000000000111",
			"other": map[string]any{"oid": map[string]any{"$oid": "NOThex", "n": 1}},
		}
		want := map[string]any{
			"_id":   "663c9c0f0000000000000111",
			"other": map[string]any{"oid": map[string]any{"$oid": "NOThex", "n": int64(1)}},
		}
		res, err := Decode("", compactJSON(t, mongoChange("c", nil, mongoImage(t, doc), "orders")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertFourFieldCDCEvent(t, res.Event, "orders", "INSERT", want, nil)
	})

	t.Run("malformed numeric wrappers inside image are preserved, not errors", func(t *testing.T) {
		doc := map[string]any{
			"a": map[string]any{"$numberInt": "not-an-int"},
			"b": map[string]any{"$numberDouble": "NaN"},
			"c": map[string]any{"$numberLong": "9223372036854775808"},
		}
		res, err := Decode("", compactJSON(t, mongoChange("c", nil, mongoImage(t, doc), "orders")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		assertFourFieldCDCEvent(t, res.Event, "orders", "INSERT", doc, nil)
	})

	t.Run("date-only image is not a document image", func(t *testing.T) {
		res, err := Decode("", compactJSON(t, mongoChange("c", nil, `{"$date":"2026-01-17T10:33:00Z"}`, "orders")))
		if err == nil {
			t.Fatalf("Decode = %v, want the scalar-image error", res)
		}
		if !strings.Contains(err.Error(), "newImage must be a JSON object or null") {
			t.Fatalf("err = %v, want the image-shape error", err)
		}
	})

	t.Run("numberLong-only image is not a document image", func(t *testing.T) {
		res, err := Decode("", compactJSON(t, mongoChange("c", nil, `{"$numberLong":"1767200000123"}`, "orders")))
		if err == nil {
			t.Fatalf("Decode = %v, want the scalar-image error", res)
		}
		if !strings.Contains(err.Error(), "newImage must be a JSON object or null") {
			t.Fatalf("err = %v, want the image-shape error", err)
		}
	})
}

// TestMongoNormalizationDoesNotTouchOtherSources pins that Mongo-only
// normalization never leaks: PostgreSQL images, raw events, and format=raw
// records keep Extended JSON wrappers and their own numeric semantics.
func TestMongoNormalizationDoesNotTouchOtherSources(t *testing.T) {
	t.Run("postgresql image wrappers are preserved verbatim", func(t *testing.T) {
		after := map[string]any{
			"id":    1,
			"d":     map[string]any{"$date": map[string]any{"$numberLong": "1767200000123"}},
			"ref":   map[string]any{"$oid": "663c9c0f0000000000000111"},
			"qty":   map[string]any{"$numberInt": "5"},
			"total": map[string]any{"$numberDecimal": "112.50"},
		}
		res, err := Decode("", compactJSON(t, pgChange("u", pgCustomersRow(1, "old@acme.com"), after, "customers")))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		// Every wrapper survives: PostgreSQL decoding does not normalize, and
		// its numbers keep the plain-json.Unmarshal float64 semantics.
		assertFourFieldCDCEvent(t, res.Event, "customers", "UPDATE", after, pgCustomersRow(1, "old@acme.com"))
	})

	t.Run("raw event wrappers are preserved", func(t *testing.T) {
		raw := `{"_id":{"$oid":"663c9c0f0000000000000111"},"d":{"$date":"2026-01-17T10:33:00Z"},"n":{"$numberLong":"9223372036854775807"}}`
		res, err := Decode("", []byte(raw))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if res.Kind != KindEvent {
			t.Fatalf("Kind = %v, want KindEvent", res.Kind)
		}
		if got := normalizeAsJSON(t, res.Event); got != normalizeAsJSON(t, decodeObject(t, raw)) {
			t.Fatalf("raw event changed: %s, want %s", got, raw)
		}
	})

	t.Run("format=raw keeps a mongo change and its images unnormalized", func(t *testing.T) {
		after := mongoImage(t, map[string]any{
			"_id": map[string]any{"$oid": "663c9c0f0000000000000111"},
			"d":   map[string]any{"$date": map[string]any{"$numberLong": "1767200000123"}},
		})
		change := mongoChange("u", nil, after, "orders")
		res, err := Decode(FormatRaw, compactJSON(t, change))
		if err != nil {
			t.Fatalf("Decode(raw): %v", err)
		}
		if res.Event["after"] != after {
			t.Fatalf("after image = %v, want the raw JSON string verbatim", res.Event["after"])
		}
		if _, isCDC := res.Event["collectionName"]; isCDC {
			t.Fatalf("Event = %v, want NO CDC normalization under format=raw", res.Event)
		}
	})
}

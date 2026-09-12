package function

import (
	"strings"
	"testing"
	"time"
)

// fixedNow is the fixed clock used by the temporal tests below: a concrete UTC
// instant with no dependence on the host machine's local timezone.
var fixedNow = time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

// mustParseWithClock parses a template with an explicit temporal clock, so a
// test can pin the wall clock without any package-level mutation. Each parsed
// comparisonMatcher keeps this clock and consults it per match, so different
// templates can carry different clocks (safe under t.Parallel).
func mustParseWithClock(t *testing.T, yaml string, now func() time.Time) *Template {
	t.Helper()
	tmpl, err := parseTemplateWithClock([]byte(yaml), now)
	if err != nil {
		t.Fatalf("parse template with clock: %v", err)
	}
	return tmpl
}

// timestamp parses an RFC3339 event value into the same layout production
// events carry (RFC3339), so tests can express the same instant with different
// offsets.
func timestamp(t *testing.T, s string) string {
	t.Helper()
	if _, err := time.Parse(time.RFC3339, s); err != nil {
		t.Fatalf("bad test timestamp %q: %v", s, err)
	}
	return s
}

// TestParseNowOperand pins the exact now-syntax rules. It is pure syntax —
// it returns only the signed relative offset and never consults a clock.
func TestParseNowOperand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want time.Duration // signed offset (baked-in sign); zero => clock() itself
		temp bool
	}{
		{"plain now", "now", 0, true},
		{"now minus 5m", "now-5m", -5 * time.Minute, true},
		{"now plus 5m", "now+5m", 5 * time.Minute, true},
		{"now plus 1h", "now+1h", time.Hour, true},
		{"compound duration", "now-1h30m", -90 * time.Minute, true},
		{"compound mixed", "now-2h45m30s", -(2*time.Hour + 45*time.Minute + 30*time.Second), true},
		{"zero duration", "now+0s", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, temp, err := parseNowOperand(tc.in)
			if err != nil {
				t.Fatalf("parseNowOperand(%q) error: %v", tc.in, err)
			}
			if !temp {
				t.Errorf("parseNowOperand(%q) = temporal=%v, want %v", tc.in, temp, tc.temp)
			}
			if temp && got != tc.want {
				t.Errorf("parseNowOperand(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}

	// Non-now strings are simply "not a temporal operand" (zero, false, nil) —
	// they are rejected by the caller, not as a now-syntax error.
	for _, in := range []string{"hello", "2026-09-12T10:00:00Z", "utc_now()", "${now}", "now - 5m"} {
		if in == "now - 5m" {
			// "now - 5m" starts with "now" but has a space -> a malformed
			// now-expression, so it IS an error, not a plain non-temporal string.
			if _, _, err := parseNowOperand(in); err == nil {
				t.Errorf("parseNowOperand(%q) expected error for whitespace", in)
			}
			continue
		}
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, false)", in, got, temp)
		}
	}

	// Malformed now-expressions are errors.
	for _, in := range []string{"now-", "now+", "now-foo", "now - 5m", "now--5m", "now+-5m", "now1h", "now1"} {
		if _, _, err := parseNowOperand(in); err == nil {
			t.Errorf("parseNowOperand(%q) expected an error, got nil", in)
		}
	}
}

func TestNumericGT(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      score:
        gt: 70
`)
	if !matches(t, tmpl, map[string]any{"score": float64(92)}) {
		t.Error("expected match for 92 > 70")
	}
	if matches(t, tmpl, map[string]any{"score": float64(69)}) {
		t.Error("expected no match for 69 > 70")
	}
	// int event value compares numerically too.
	if !matches(t, tmpl, map[string]any{"score": 92}) {
		t.Error("expected match for int 92 > 70")
	}
	// Exact equality does not satisfy gt.
	if matches(t, tmpl, map[string]any{"score": float64(70)}) {
		t.Error("expected no match for 70 > 70")
	}
	// Non-numeric event value -> no match.
	if matches(t, tmpl, map[string]any{"score": "ninety-two"}) {
		t.Error("expected no match for string event value against numeric operand")
	}
}

func TestNumericLTE(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      price:
        lte: 100.5
`)
	if !matches(t, tmpl, map[string]any{"price": float64(100.5)}) {
		t.Error("expected match: equality satisfies lte")
	}
	if !matches(t, tmpl, map[string]any{"price": float64(50)}) {
		t.Error("expected match: 50 <= 100.5")
	}
	if matches(t, tmpl, map[string]any{"price": float64(101)}) {
		t.Error("expected no match: 101 > 100.5")
	}
}

func TestNumericOperandListsOR(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      score:
        gte: [60, 80]
`)
	if !matches(t, tmpl, map[string]any{"score": 65}) {
		t.Error("expected match: 65 >= 60")
	}
	if !matches(t, tmpl, map[string]any{"score": 85}) {
		t.Error("expected match: 85 >= 80")
	}
	if matches(t, tmpl, map[string]any{"score": 30}) {
		t.Error("expected no match: 30 < both thresholds")
	}
}

func TestTemporalGTNow(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at:
        gt: "now-5m"
`, func() time.Time { return fixedNow })
	// Cutoff = fixedNow - 5m = 09:55:00Z. An event after that matches.
	if !matches(t, tmpl, map[string]any{"created_at": timestamp(t, "2026-09-12T10:00:00Z")}) {
		t.Error("expected match: event at 10:00:00Z > 09:55:00Z cutoff")
	}
	// An event before the cutoff does not match (09:54 < 09:55).
	if matches(t, tmpl, map[string]any{"created_at": timestamp(t, "2026-09-12T09:54:00Z")}) {
		t.Error("expected no match: event before cutoff")
	}
	// Exact cutoff instant does not satisfy gt.
	if matches(t, tmpl, map[string]any{"created_at": timestamp(t, "2026-09-12T09:55:00Z")}) {
		t.Error("expected no match: exact cutoff does not satisfy gt")
	}
}

// templateYAML wraps an operator line into a minimal parseable template.
func templateYAML(operatorLine string) string {
	return `runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at:
        ` + operatorLine + "\n"
}

func TestTemporalGteLteExactCutoff(t *testing.T) {
	t.Parallel()
	cutoff := "2026-09-12T09:55:00Z" // now-5m from 10:00:00Z
	before := "2026-09-12T09:54:00Z"
	after := "2026-09-12T10:00:00Z"
	clock := func() time.Time { return fixedNow }

	gte := mustParseWithClock(t, templateYAML("gte: \"now-5m\""), clock)
	if !matches(t, gte, map[string]any{"created_at": cutoff}) {
		t.Error("expected gte to match at exact cutoff")
	}
	if !matches(t, gte, map[string]any{"created_at": after}) {
		t.Error("expected gte to match after cutoff")
	}
	if matches(t, gte, map[string]any{"created_at": before}) {
		t.Error("expected gte no match before cutoff")
	}

	lt := mustParseWithClock(t, templateYAML("lt: \"now-5m\""), clock)
	if matches(t, lt, map[string]any{"created_at": cutoff}) {
		t.Error("expected lt no match at exact cutoff")
	}
	if !matches(t, lt, map[string]any{"created_at": before}) {
		t.Error("expected lt match before cutoff")
	}

	lte := mustParseWithClock(t, templateYAML("lte: \"now-5m\""), clock)
	if !matches(t, lte, map[string]any{"created_at": cutoff}) {
		t.Error("expected lte to match at exact cutoff")
	}
	if !matches(t, lte, map[string]any{"created_at": before}) {
		t.Error("expected lte to match before cutoff")
	}
	if matches(t, lte, map[string]any{"created_at": after}) {
		t.Error("expected lte no match after cutoff")
	}
}

func TestTemporalOffsetsAreInstants(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at:
        gt: "now-20m"
`, func() time.Time { return fixedNow })
	// now-20m = 09:40:00Z. All of these are the SAME instant (10:00:00Z) written
	// with Z and positive/negative offsets, so all should match gt.
	for _, s := range []string{
		"2026-09-12T10:00:00Z",
		"2026-09-12T12:00:00+02:00",
		"2026-09-12T05:00:00-05:00",
	} {
		if !matches(t, tmpl, map[string]any{"created_at": timestamp(t, s)}) {
			t.Errorf("expected offset instant %q to match gt now-20m", s)
		}
	}
	// 09:30:00Z (before cutoff 09:40) must not match, in any offset spelling.
	for _, s := range []string{
		"2026-09-12T09:30:00Z",
		"2026-09-12T11:30:00+02:00",
		"2026-09-12T04:30:00-05:00",
	} {
		if matches(t, tmpl, map[string]any{"created_at": timestamp(t, s)}) {
			t.Errorf("expected offset instant %q NOT to match gt now-20m", s)
		}
	}
}

func TestTemporalBadEventValues(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, templateYAML("gt: \"now-5m\""), func() time.Time { return fixedNow })
	// Missing field.
	if matches(t, tmpl, map[string]any{}) {
		t.Error("expected no match for missing field")
	}
	// Null field.
	if matches(t, tmpl, map[string]any{"created_at": nil}) {
		t.Error("expected no match for null field")
	}
	// Non-string (number).
	if matches(t, tmpl, map[string]any{"created_at": 42}) {
		t.Error("expected no match for numeric event value")
	}
	// Invalid RFC3339 string.
	if matches(t, tmpl, map[string]any{"created_at": "not-a-timestamp"}) {
		t.Error("expected no match for invalid RFC3339")
	}
	// A date-only-ish string is also not valid RFC3339 and must not be parsed
	// heuristically.
	if matches(t, tmpl, map[string]any{"created_at": "2026-09-12"}) {
		t.Error("expected no match for non-RFC3339 date string")
	}
}

func TestTemporalNested(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      metadata:
        timestamps:
          updated_at:
            gt: "now-1h"
`, func() time.Time { return fixedNow })
	if !matches(t, tmpl, map[string]any{"metadata": map[string]any{"timestamps": map[string]any{"updated_at": "2026-09-12T10:00:00Z"}}}) {
		t.Error("expected nested match")
	}
	if matches(t, tmpl, map[string]any{"metadata": map[string]any{"timestamps": map[string]any{"updated_at": "2026-09-12T08:00:00Z"}}}) {
		t.Error("expected nested no match (before one-hour cutoff)")
	}
}

func TestTemporalInteractsWithExists(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at:
        gt: "now-5m"
        exists: true
`, func() time.Time { return fixedNow })
	// Both operators on the same field are OR. exists:true passes for any
	// present key, so a present but stale created_at still matches.
	if !matches(t, tmpl, map[string]any{"created_at": "2020-01-01T00:00:00Z"}) {
		t.Error("expected match via exists:true OR even when gt fails")
	}
	if matches(t, tmpl, map[string]any{}) {
		t.Error("expected no match: absent key satisfies neither gt nor exists:true")
	}
}

func TestTemporalNowEvaluatedAtMatchTime(t *testing.T) {
	t.Parallel()
	// The clock is a mutable-but-test-owned closure over a local var, not any
	// package-level state. One template instance holds this closure and consults
	// it on every Match; advancing it between calls must change the result
	// without re-parsing, proving the cutoff is read from the clock at match
	// time.
	cutoff1 := time.Date(2026, 9, 12, 9, 59, 0, 0, time.UTC) // base now = 10:04:00Z
	now := cutoff1.Add(5 * time.Minute)
	tmpl := mustParseWithClock(t, templateYAML("gt: \"now-5m\""), func() time.Time { return now })
	event := map[string]any{"created_at": "2026-09-12T10:00:00Z"}

	// At now=10:04:00Z, cutoff=09:59:00Z -> event 10:00 matches.
	if !matches(t, tmpl, event) {
		t.Error("expected match at now=10:04:00Z")
	}
	// Move the clock forward to now=10:07:00Z (cutoff=10:02:00Z); the SAME event
	// 10:00 is now before the cutoff -> no match. Same template instance.
	now = cutoff1.Add(8 * time.Minute)
	if matches(t, tmpl, event) {
		t.Error("expected no match after clock moved without re-parsing template")
	}
}

func TestTemporalLocalTimezoneIndependent(t *testing.T) {
	t.Parallel()
	// The cutoff arithmetic operates on UTC instants only. fixedNow has no local
	// timezone dependence, and expected values are computed with time.Date in
	// UTC, so this test passes regardless of the host TZ.
	tmpl := mustParseWithClock(t, templateYAML("gte: \"now-5m\""), func() time.Time { return fixedNow })
	// now-5m in UTC is 09:59:00Z. Construct the expected cutoff explicitly in UTC.
	expectedCutoff := fixedNow.Add(-5 * time.Minute)
	if !matches(t, tmpl, map[string]any{"created_at": expectedCutoff.Format(time.RFC3339)}) {
		t.Error("expected gte to match exact UTC-constructed cutoff regardless of host TZ")
	}
}

// TestComparisonParseValidation pins strict parse-time validation of comparison
// operands: malformed values are rejected at ParseTemplate, naming the field.
func TestComparisonParseValidation(t *testing.T) {
	bad := []struct {
		name  string
		line  string
		field string // expected in the error (field path)
		sub   string // expected substring in the error
	}{
		{"gt malformed now", `gt: "now-"`, "created_at", "now"},
		{"gt now+foo", `gt: "now+foo"`, "created_at", "now"},
		{"gt now with space", `gt: "now - 5m"`, "created_at", "now"},
		{"gt plain string", `gt: "hello"`, "created_at", "must be a number"},
		{"gt looks like date", `gt: "2026-09-12T10:00:00Z"`, "created_at", "must be a number"},
		{"gt abc", `gt: "abc"`, "created_at", "must be a number"},
		{"gt null", `gt: null`, "created_at", "got null"},
		{"gt null in list", `gt: [null]`, "created_at", "got null"},
		{"lt bool", `lt: true`, "created_at", "must be a number"},
		{"gt empty list", `gt: []`, "created_at", "empty list"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(templateYAML(tc.line)))
			if err == nil {
				t.Fatalf("expected parse error for %s", tc.name)
			}
			for _, want := range []string{tc.field, tc.sub} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q should mention %q", err, want)
				}
			}
		})
	}
}

// TestComparisonValidParses pins that valid numeric and now operands (scalar
// and list, mixed kinds, zero duration) all parse without error.
func TestComparisonValidParses(t *testing.T) {
	templates := []string{
		templateYAML(`gt: 70`),
		templateYAML(`lte: 100.5`),
		templateYAML(`gte: "now-1h"`),
		templateYAML(`lt: ["now-5m", 100]`),
		templateYAML(`gt: "now"`),
		templateYAML(`gt: "now+0s"`),
		templateYAML(`gte: [60, "now-2h45m30s"]`),
	}
	for i, yaml := range templates {
		if _, err := ParseTemplate([]byte(yaml)); err != nil {
			t.Errorf("template %d should parse: %v", i, err)
		}
	}
}

// TestGteLtORSemantics documents and pins the OR behavior of two comparison
// operators on the same field.
func TestGteLtORSemantics(t *testing.T) {
	t.Parallel()
	// gte now-1h -> passes for instants >= 09:00:00Z.
	// lt now-2h -> passes for instants < 08:00:00Z.
	// As OR, an instant in neither bucket ([08:00, 09:00)) matches neither. This
	// is NOT a range constraint (that would be AND); it is two independent
	// alternatives.
	tmpl := mustParseWithClock(t, templateYAML("gte: \"now-1h\"\n        lt: \"now-2h\""), func() time.Time { return fixedNow })
	// 09:30 satisfies gte -> match.
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T09:30:00Z"}) {
		t.Error("expected match via gte now-1h")
	}
	// 07:00 satisfies lt -> match.
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T07:00:00Z"}) {
		t.Error("expected match via lt now-2h")
	}
	// 08:30 is in the gap [08:00, 09:00) -> satisfies neither.
	if matches(t, tmpl, map[string]any{"created_at": "2026-09-12T08:30:00Z"}) {
		t.Error("expected no match: 08:30 satisfies neither gte-now-1h nor lt-now-2h")
	}
}

// TestTemporalProductionPath verifies ParseTemplate (the production entry point)
// evaluates temporal rules against the real wall clock. Instants are chosen far
// enough from any plausible "now" that the result is stable regardless of when
// the test runs: a 26-year-old instant is certainly before now, and a 900-year
// future instant is certainly after.
func TestTemporalProductionPath(t *testing.T) {
	tmpl := mustParse(t, templateYAML("gt: \"now\""))
	if matches(t, tmpl, map[string]any{"created_at": "2000-01-01T00:00:00Z"}) {
		t.Error("expected no match: a past instant is not gt now")
	}
	if !matches(t, tmpl, map[string]any{"created_at": "3000-01-01T00:00:00Z"}) {
		t.Error("expected match: a far-future instant is gt now")
	}
}

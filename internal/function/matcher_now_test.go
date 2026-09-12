package function

import (
	"encoding/json"
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

// TestParseNowOperand pins the exact now() syntax rules. It is pure syntax —
// it returns only the signed relative offset and never consults a clock.
func TestParseNowOperand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want time.Duration // signed offset (baked-in sign); zero => clock() itself
		temp bool
	}{
		{"plain now()", "now()", 0, true},
		{"now() minus 5m", "now()-5m", -5 * time.Minute, true},
		{"now() plus 5m", "now()+5m", 5 * time.Minute, true},
		{"now() plus 1h", "now()+1h", time.Hour, true},
		{"compound duration", "now()-1h30m", -90 * time.Minute, true},
		{"compound mixed", "now()-2h45m30s", -(2*time.Hour + 45*time.Minute + 30*time.Second), true},
		{"zero duration", "now()+0s", 0, true},
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

	// Non-now() strings (including the removed bare `now`/`now±duration`
	// syntax and uppercase variants) are simply "not a temporal operand"
	// (zero, false, nil) — they are rejected by the caller, not as a now()
	// syntax error.
	for _, in := range []string{
		"hello",
		"2026-09-12T10:00:00Z",
		"utc_now()",
		"${now}",
		"now - 5m",
		"now",
		"now-5m",
		"now+1h",
		"NOW()",
		"Now()",
	} {
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, false)", in, got, temp)
		}
	}

	// Malformed now() expressions are errors.
	for _, in := range []string{
		"now()-",
		"now()+",
		"now()-foo",
		"now() - 5m",
		"now()--5m",
		"now()+-5m",
		"now()5m",
		"now() ",
		"now()  ",
	} {
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
        gt: "now()-5m"
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
	cutoff := "2026-09-12T09:55:00Z" // now()-5m from 10:00:00Z
	before := "2026-09-12T09:54:00Z"
	after := "2026-09-12T10:00:00Z"
	clock := func() time.Time { return fixedNow }

	gte := mustParseWithClock(t, templateYAML("gte: \"now()-5m\""), clock)
	if !matches(t, gte, map[string]any{"created_at": cutoff}) {
		t.Error("expected gte to match at exact cutoff")
	}
	if !matches(t, gte, map[string]any{"created_at": after}) {
		t.Error("expected gte to match after cutoff")
	}
	if matches(t, gte, map[string]any{"created_at": before}) {
		t.Error("expected gte no match before cutoff")
	}

	lt := mustParseWithClock(t, templateYAML("lt: \"now()-5m\""), clock)
	if matches(t, lt, map[string]any{"created_at": cutoff}) {
		t.Error("expected lt no match at exact cutoff")
	}
	if !matches(t, lt, map[string]any{"created_at": before}) {
		t.Error("expected lt match before cutoff")
	}

	lte := mustParseWithClock(t, templateYAML("lte: \"now()-5m\""), clock)
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
        gt: "now()-20m"
`, func() time.Time { return fixedNow })
	// now()-20m = 09:40:00Z. All of these are the SAME instant (10:00:00Z) written
	// with Z and positive/negative offsets, so all should match gt.
	for _, s := range []string{
		"2026-09-12T10:00:00Z",
		"2026-09-12T12:00:00+02:00",
		"2026-09-12T05:00:00-05:00",
	} {
		if !matches(t, tmpl, map[string]any{"created_at": timestamp(t, s)}) {
			t.Errorf("expected offset instant %q to match gt now()-20m", s)
		}
	}
	// 09:30:00Z (before cutoff 09:40) must not match, in any offset spelling.
	for _, s := range []string{
		"2026-09-12T09:30:00Z",
		"2026-09-12T11:30:00+02:00",
		"2026-09-12T04:30:00-05:00",
	} {
		if matches(t, tmpl, map[string]any{"created_at": timestamp(t, s)}) {
			t.Errorf("expected offset instant %q NOT to match gt now()-20m", s)
		}
	}
}

func TestTemporalBadEventValues(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, templateYAML("gt: \"now()-5m\""), func() time.Time { return fixedNow })
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
            gt: "now()-1h"
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
        gt: "now()-5m"
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
	tmpl := mustParseWithClock(t, templateYAML("gt: \"now()-5m\""), func() time.Time { return now })
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
	tmpl := mustParseWithClock(t, templateYAML("gte: \"now()-5m\""), func() time.Time { return fixedNow })
	// now()-5m in UTC is 09:59:00Z. Construct the expected cutoff explicitly in UTC.
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
		{"gt malformed now()", `gt: "now()-"`, "created_at", "now()"},
		{"gt now()+foo", `gt: "now()+foo"`, "created_at", "now()"},
		{"gt now() with space", `gt: "now() - 5m"`, "created_at", "now()"},
		{"gt bare old now", `gt: "now"`, "created_at", "must be a number"},
		{"gt old now-5m", `gt: "now-5m"`, "created_at", "must be a number"},
		{"gt old now+1h", `gt: "now+1h"`, "created_at", "must be a number"},
		{"gt uppercase NOW()", `gt: "NOW()"`, "created_at", "must be a number"},
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
		templateYAML(`gte: "now()-1h"`),
		templateYAML(`lt: ["now()-5m", 100]`),
		templateYAML(`gt: "now()"`),
		templateYAML(`gt: "now()+0s"`),
		templateYAML(`gte: [60, "now()-2h45m30s"]`),
	}
	for i, yaml := range templates {
		if _, err := ParseTemplate([]byte(yaml)); err != nil {
			t.Errorf("template %d should parse: %v", i, err)
		}
	}
}

// TestTemporalNowZeroOffset pins now() zero-offset semantics: the cutoff equals
// the clock() instant itself. At the gte boundary the event exactly at clock()
// matches (cutoff included); for gt it does not (strictly after). This isolates
// the plain now() case from the offset matrix, which uses now()-5m.
func TestTemporalNowZeroOffset(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 12, 10, 4, 0, 0, time.UTC)
	atNow := now.Format(time.RFC3339)
	before := now.Add(-time.Minute).Format(time.RFC3339)
	after := now.Add(time.Minute).Format(time.RFC3339)

	// gt now(): event strictly after now() matches; at/equal or before does not.
	gt := mustParseWithClock(t, templateYAML("gt: \"now()\""), func() time.Time { return now })
	if !matches(t, gt, map[string]any{"created_at": after}) {
		t.Error("expected gt now() to match event after now()")
	}
	if matches(t, gt, map[string]any{"created_at": atNow}) {
		t.Error("expected gt now() NOT to match event exactly equal to now()")
	}
	if matches(t, gt, map[string]any{"created_at": before}) {
		t.Error("expected gt now() NOT to match event before now()")
	}

	// gte now(): event == now() satisfies gte (equality passes).
	gte := mustParseWithClock(t, templateYAML("gte: \"now()\""), func() time.Time { return now })
	if !matches(t, gte, map[string]any{"created_at": atNow}) {
		t.Error("expected gte now() to match event equal to now()")
	}
	if matches(t, gte, map[string]any{"created_at": before}) {
		t.Error("expected gte now() NOT to match event before now()")
	}

	// lt now(): event == now() does not satisfy strict lt; before does.
	lt := mustParseWithClock(t, templateYAML("lt: \"now()\""), func() time.Time { return now })
	if matches(t, lt, map[string]any{"created_at": atNow}) {
		t.Error("expected lt now() NOT to match event equal to now()")
	}
	if !matches(t, lt, map[string]any{"created_at": before}) {
		t.Error("expected lt now() to match event before now()")
	}

	// lte now(): event == now() satisfies lte.
	lte := mustParseWithClock(t, templateYAML("lte: \"now()\""), func() time.Time { return now })
	if !matches(t, lte, map[string]any{"created_at": atNow}) {
		t.Error("expected lte now() to match event equal to now()")
	}
	if matches(t, lte, map[string]any{"created_at": after}) {
		t.Error("expected lte now() NOT to match event after now()")
	}
}

// TestGteLtORSemantics documents and pins the OR behavior of two comparison
// operators on the same field.
func TestGteLtORSemantics(t *testing.T) {
	t.Parallel()
	// gte now()-1h -> passes for instants >= 09:00:00Z.
	// lt now()-2h -> passes for instants < 08:00:00Z.
	// As OR, an instant in neither bucket ([08:00, 09:00)) matches neither. This
	// is NOT a range constraint (that would be AND); it is two independent
	// alternatives.
	tmpl := mustParseWithClock(t, templateYAML("gte: \"now()-1h\"\n        lt: \"now()-2h\""), func() time.Time { return fixedNow })
	// 09:30 satisfies gte -> match.
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T09:30:00Z"}) {
		t.Error("expected match via gte now()-1h")
	}
	// 07:00 satisfies lt -> match.
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T07:00:00Z"}) {
		t.Error("expected match via lt now()-2h")
	}
	// 08:30 is in the gap [08:00, 09:00) -> satisfies neither.
	if matches(t, tmpl, map[string]any{"created_at": "2026-09-12T08:30:00Z"}) {
		t.Error("expected no match: 08:30 satisfies neither gte-now()-1h nor lt-now()-2h")
	}
}

// TestTemporalProductionPath verifies ParseTemplate (the production entry point)
// evaluates temporal rules against the real wall clock. Instants are chosen far
// enough from any plausible "now" that the result is stable regardless of when
// the test runs: a 26-year-old instant is certainly before now, and a 900-year
// future instant is certainly after.
func TestTemporalProductionPath(t *testing.T) {
	tmpl := mustParse(t, templateYAML("gt: \"now()\""))
	if matches(t, tmpl, map[string]any{"created_at": "2000-01-01T00:00:00Z"}) {
		t.Error("expected no match: a past instant is not gt now()")
	}
	if !matches(t, tmpl, map[string]any{"created_at": "3000-01-01T00:00:00Z"}) {
		t.Error("expected match: a far-future instant is gt now")
	}
}

// TestParseNowOperandExtra extends TestParseNowOperand with additional pure
// syntax coverage: case sensitivity, whitespace variants, empty input, missing
// sign, sub-second units, large offsets, and zero offsets. It is pure syntax and
// never consults a clock.
func TestParseNowOperandExtra(t *testing.T) {
	t.Parallel()

	// Uppercase "NOW()" and other "NOW(...)" prefixed variants are NOT temporal
	// (case-sensitive), returning (0, false, nil) with no error.
	for _, in := range []string{"NOW()", "Now()", "NOW()-5m", "Now", "NOW"} {
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, false)", in, got, temp)
		}
	}

	// Whitespace anywhere in the expression is malformed -> error.
	for _, in := range []string{
		"now() -5m", "now() +5m", "now()\t-5m", "now()+ 5m", "now()- 5m",
		"now() - 5m", "now() +5m", "now()",
	} {
		wantErr := in != "now()"
		_, temp, err := parseNowOperand(in)
		if wantErr && err == nil {
			t.Errorf("parseNowOperand(%q) expected an error, got nil", in)
		}
		if !wantErr && err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if !wantErr && !temp {
			t.Errorf("parseNowOperand(%q) = temporal=%v, want true", in, temp)
		}
	}

	// Empty string is not temporal.
	got, temp, err := parseNowOperand("")
	if err != nil {
		t.Errorf("parseNowOperand(\"\") unexpected error: %v", err)
	}
	if temp || got != 0 {
		t.Errorf("parseNowOperand(\"\") = (%v, %v), want (0, false)", got, temp)
	}

	// "now()now", "now()5m", "now()" followed by anything other than a sign is
	// malformed -> error.
	for _, in := range []string{"now()now", "now()5m", "now()x"} {
		if _, _, err := parseNowOperand(in); err == nil {
			t.Errorf("parseNowOperand(%q) expected an error, got nil", in)
		}
	}

	// The removed bare `now`/`now±duration` syntax is NOT temporal (parses as
	// a plain non-temporal string, not an error) — rejections happen in the
	// caller.
	for _, in := range []string{"now", "now-5m", "now+1h"} {
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, false)", in, got, temp)
		}
	}

	// Sub-second units are valid (ParseDuration supports ms/us/ns).
	sub := []struct {
		in   string
		want time.Duration
	}{
		{"now()-500ms", -500 * time.Millisecond},
		{"now()+250us", 250 * time.Microsecond},
		{"now()-1ns", -1 * time.Nanosecond},
	}
	for _, tc := range sub {
		got, temp, err := parseNowOperand(tc.in)
		if err != nil {
			t.Fatalf("parseNowOperand(%q) error: %v", tc.in, err)
		}
		if !temp || got != tc.want {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (%v, true)", tc.in, got, temp, tc.want)
		}
	}

	// Large offsets parse without overflow concerns.
	big := []struct {
		in   string
		want time.Duration
	}{
		{"now()+24h", 24 * time.Hour},
		{"now()-72h30m", -(72*time.Hour + 30*time.Minute)},
		{"now()+168h", 168 * time.Hour},
	}
	for _, tc := range big {
		got, temp, err := parseNowOperand(tc.in)
		if err != nil {
			t.Fatalf("parseNowOperand(%q) error: %v", tc.in, err)
		}
		if !temp || got != tc.want {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (%v, true)", tc.in, got, temp, tc.want)
		}
	}

	// Zero offsets remain valid temporal operands (cutoff == clock()).
	for _, in := range []string{"now()-0m", "now()+0h"} {
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Fatalf("parseNowOperand(%q) error: %v", in, err)
		}
		if !temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, true)", in, got, temp)
		}
	}
}

// TestNumericGteLt extends numeric coverage to the gte operator (previously only
// gt was tested) and pinpoints lt strictness, lte, and non-numeric event values
// across every ordering operator. Numeric matchers are clock-free, so these use
// the production parse path via mustParse.
func TestNumericGteLt(t *testing.T) {
	gte := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      score:
        gte: 70
`)
	// Equality satisfies gte, for both int and float64 event kinds.
	if !matches(t, gte, map[string]any{"score": 70}) {
		t.Error("expected gte match on equality (int event value)")
	}
	if !matches(t, gte, map[string]any{"score": float64(70)}) {
		t.Error("expected gte match on equality (float64 event value)")
	}
	if matches(t, gte, map[string]any{"score": 69}) {
		t.Error("expected no gte match for 69")
	}
	if !matches(t, gte, map[string]any{"score": 71}) {
		t.Error("expected gte match for 71")
	}
	for _, v := range []any{"seventy", true, nil} {
		if matches(t, gte, map[string]any{"score": v}) {
			t.Errorf("expected no gte match for non-numeric %v", v)
		}
	}

	lt := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      price:
        lt: 100.5
`)
	if matches(t, lt, map[string]any{"price": 100.5}) {
		t.Error("expected no lt match on strict equality")
	}
	if !matches(t, lt, map[string]any{"price": 100}) {
		t.Error("expected lt match for 100")
	}
	if matches(t, lt, map[string]any{"price": 101}) {
		t.Error("expected no lt match for 101")
	}
	for _, v := range []any{"100.5", true, nil} {
		if matches(t, lt, map[string]any{"price": v}) {
			t.Errorf("expected no lt match for non-numeric %v", v)
		}
	}

	lte := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      count:
        lte: 10
`)
	if !matches(t, lte, map[string]any{"count": 10}) {
		t.Error("expected lte match on equality")
	}
	if matches(t, lte, map[string]any{"count": 11}) {
		t.Error("expected no lte match for 11")
	}
	for _, v := range []any{"10", true, nil} {
		if matches(t, lte, map[string]any{"count": v}) {
			t.Errorf("expected no lte match for non-numeric %v", v)
		}
	}
}

// TestTemporalCutoffMatrix pins After/Before/Equal semantics exhaustively across
// all four ordering operators against the same cutoff. Each uses "now()-5m"
// (cutoff 09:55:00Z at the fixed clock) and events straddling that exact
// instant: 09:54:59Z (before), 09:55:00Z (equal), 09:55:01Z (after).
func TestTemporalCutoffMatrix(t *testing.T) {
	t.Parallel()
	clock := func() time.Time { return fixedNow }

	cases := []struct {
		name    string
		op      string // operator key rendered into the YAML
		instant string // event instant relative to cutoff
		want    bool
	}{
		{"gt before", "gt", "2026-09-12T09:54:59Z", false},
		{"gt at cutoff", "gt", "2026-09-12T09:55:00Z", false},
		{"gt after", "gt", "2026-09-12T09:55:01Z", true},

		{"gte before", "gte", "2026-09-12T09:54:59Z", false},
		{"gte at cutoff", "gte", "2026-09-12T09:55:00Z", true},
		{"gte after", "gte", "2026-09-12T09:55:01Z", true},

		{"lt before", "lt", "2026-09-12T09:54:59Z", true},
		{"lt at cutoff", "lt", "2026-09-12T09:55:00Z", false},
		{"lt after", "lt", "2026-09-12T09:55:01Z", false},

		{"lte before", "lte", "2026-09-12T09:54:59Z", true},
		{"lte at cutoff", "lte", "2026-09-12T09:55:00Z", true},
		{"lte after", "lte", "2026-09-12T09:55:01Z", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := mustParseWithClock(t, templateYAML(tc.op+": \"now()-5m\""), clock)
			got := matches(t, tmpl, map[string]any{"created_at": tc.instant})
			if got != tc.want {
				t.Errorf("%s now()-5m vs instant %s = %v, want %v", tc.op, tc.instant, got, tc.want)
			}
		})
	}
}

// TestTemporalPositiveOffset covers now()+DURATION cutoffs (cutoff in the future):
// lt/lte/gte strictness around the positive cutoff 11:00:00Z from now()+1h.
func TestTemporalPositiveOffset(t *testing.T) {
	t.Parallel()

	ltTmpl := mustParseWithClock(t, templateYAML("lt: \"now()+1h\""), func() time.Time { return fixedNow })
	if !matches(t, ltTmpl, map[string]any{"created_at": "2026-09-12T10:59:00Z"}) {
		t.Error("expected lt match before positive cutoff")
	}
	if matches(t, ltTmpl, map[string]any{"created_at": "2026-09-12T11:00:00Z"}) {
		t.Error("expected no lt match at positive cutoff (strict)")
	}
	if matches(t, ltTmpl, map[string]any{"created_at": "2026-09-12T11:01:00Z"}) {
		t.Error("expected no lt match after positive cutoff")
	}

	lteTmpl := mustParseWithClock(t, templateYAML("lte: \"now()+1h\""), func() time.Time { return fixedNow })
	if !matches(t, lteTmpl, map[string]any{"created_at": "2026-09-12T11:00:00Z"}) {
		t.Error("expected lte match at positive cutoff")
	}
	if matches(t, lteTmpl, map[string]any{"created_at": "2026-09-12T11:01:00Z"}) {
		t.Error("expected no lte match after positive cutoff")
	}

	gtTmpl := mustParseWithClock(t, templateYAML("gt: \"now()+1h\""), func() time.Time { return fixedNow })
	if !matches(t, gtTmpl, map[string]any{"created_at": "2026-09-12T12:00:00Z"}) {
		t.Error("expected gt match after positive cutoff")
	}
	if matches(t, gtTmpl, map[string]any{"created_at": "2026-09-12T10:00:00Z"}) {
		t.Error("expected no gt match before positive cutoff")
	}
}

// TestTemporalOperandListOR covers OR semantics across operand lists that mix
// temporal cutoffs as well as temporal + numeric kinds, plus the equivalence
// between a single-element list and a scalar.
func TestTemporalOperandListOR(t *testing.T) {
	t.Parallel()
	clock := func() time.Time { return fixedNow }

	// gte ["now()-1h", "now()-5m"]: OR. now()-1h cutoff 09:00, now()-5m cutoff 09:55.
	tmpl := mustParseWithClock(t, templateYAML("gte: [\"now()-1h\", \"now()-5m\"]"), clock)
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T09:30:00Z"}) {
		t.Error("expected match: 09:30 >= 09:00 via first operand")
	}
	if matches(t, tmpl, map[string]any{"created_at": "2026-09-12T08:00:00Z"}) {
		t.Error("expected no match: 08:00 below both cutoffs")
	}
	if !matches(t, tmpl, map[string]any{"created_at": "2026-09-12T09:55:00Z"}) {
		t.Error("expected match: 09:55 at the now()-5m cutoff satisfies both")
	}

	// Mixed temporal + numeric list is OR across kinds: lt ["now()", 100].
	mixed := mustParseWithClock(t, templateYAML("lt: [\"now()\", 100]"), clock)
	if !matches(t, mixed, map[string]any{"created_at": "2026-09-12T09:00:00Z"}) {
		t.Error("expected match via temporal arm (09:00 < now())")
	}
	if !matches(t, mixed, map[string]any{"created_at": 50}) {
		t.Error("expected match via numeric arm (50 < 100)")
	}
	if matches(t, mixed, map[string]any{"created_at": "2026-09-12T11:00:00Z"}) {
		t.Error("expected no match: 11:00 not < now() and not numeric")
	}

	// A single-element list behaves exactly like a scalar.
	single := mustParseWithClock(t, templateYAML("gt: [\"now()-5m\"]"), clock)
	if !matches(t, single, map[string]any{"created_at": "2026-09-12T10:00:00Z"}) {
		t.Error("expected single-element list to behave like scalar gt")
	}
	if matches(t, single, map[string]any{"created_at": "2026-09-12T09:50:00Z"}) {
		t.Error("expected single-element list no match before cutoff")
	}
}

// TestTemporalJSONDecoded exercises the production decode path: json.Unmarshal
// turns RFC3339 strings into strings and numbers into float64. Temporal gt must
// match on an RFC3339 string field, never on a numeric one, and must accept
// fractional-seconds RFC3339.
func TestTemporalJSONDecoded(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, templateYAML("gt: \"now()-5m\""), func() time.Time { return fixedNow })

	var event map[string]any
	if err := json.Unmarshal([]byte(`{"created_at": "2026-09-12T10:00:00Z"}`), &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if !matches(t, tmpl, event) {
		t.Error("expected JSON-decoded RFC3339 string to match gt now()-5m")
	}

	var numEvent map[string]any
	if err := json.Unmarshal([]byte(`{"created_at": 42}`), &numEvent); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if matches(t, tmpl, numEvent) {
		t.Error("expected no match for JSON-decoded numeric created_at")
	}

	var fracEvent map[string]any
	if err := json.Unmarshal([]byte(`{"created_at": "2026-09-12T09:58:30.123Z"}`), &fracEvent); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if !matches(t, tmpl, fracEvent) {
		t.Error("expected fractional-seconds RFC3339 to match gt now()-5m")
	}
}

// TestTemporalDeepNesting covers three-level nesting, sibling AND between a
// temporal field and an equality field, and that a nested field literally named
// "gt" alongside siblings stays a NESTED CONDITION (type-preserving equality),
// not an operator.
func TestTemporalDeepNesting(t *testing.T) {
	t.Parallel()
	clock := func() time.Time { return fixedNow }

	deep := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      a:
        b:
          c:
            gt: "now()-1h"
`, clock)
	if !matches(t, deep, map[string]any{"a": map[string]any{"b": map[string]any{"c": "2026-09-12T10:00:00Z"}}}) {
		t.Error("expected deep nested match")
	}
	if matches(t, deep, map[string]any{"a": map[string]any{"b": map[string]any{"c": "2026-09-12T08:00:00Z"}}}) {
		t.Error("expected no deep nested match (before one-hour cutoff)")
	}

	// Sibling AND: both the temporal field and the status field must hold.
	and := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at:
        gt: "now()-5m"
      status: [COMPLETED]
`, clock)
	if !matches(t, and, map[string]any{"created_at": "2026-09-12T10:00:00Z", "status": "COMPLETED"}) {
		t.Error("expected match when both siblings hold")
	}
	if matches(t, and, map[string]any{"created_at": "2026-09-12T08:00:00Z", "status": "COMPLETED"}) {
		t.Error("expected no match when temporal sibling fails")
	}
	if matches(t, and, map[string]any{"created_at": "2026-09-12T10:00:00Z", "status": "FAILED"}) {
		t.Error("expected no match when status sibling fails")
	}

	// A nested map with a "gt" key alongside non-operator siblings is nested
	// field conditions (mirror of TestNestedFieldNamedEquals): the event needs
	// data.gt == 5 (numeric) AND data.status == "OK".
	nested := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      data:
        gt: 5
        status: [OK]
`, clock)
	if !matches(t, nested, map[string]any{"data": map[string]any{"gt": 5, "status": "OK"}}) {
		t.Error("expected match for nested field named gt with numeric equality")
	}
	if matches(t, nested, map[string]any{"data": map[string]any{"gt": "5", "status": "OK"}}) {
		t.Error("expected no match: type-preserving equality rejects string \"5\" against numeric 5")
	}
}

// TestParseTemplateNilClockDefaults verifies parseTemplateWithClock(data, nil)
// falls back to the real wall clock (time.Now().UTC), matching the production
// ParseTemplate path. Instants far in the past and far future make the
// assertions wall-clock stable.
func TestParseTemplateNilClockDefaults(t *testing.T) {
	t.Parallel()
	tmpl, err := parseTemplateWithClock([]byte(templateYAML("gt: \"now()\"")), nil)
	if err != nil {
		t.Fatalf("parse with nil clock: %v", err)
	}
	if matches(t, tmpl, map[string]any{"created_at": "2000-01-01T00:00:00Z"}) {
		t.Error("expected no match: a past instant is not gt wall-clock now")
	}
	if !matches(t, tmpl, map[string]any{"created_at": "3000-01-01T00:00:00Z"}) {
		t.Error("expected match: a far-future instant is gt wall-clock now")
	}
}

// TestComparisonParseValidationContext extends TestComparisonParseValidation to
// malformed-now on gte/lte/lt keys and verifies the error carries the field path
// AND the rule handler context, plus non-string operand shapes and the strict
// numeric-looking-string rejection.
func TestComparisonParseValidationContext(t *testing.T) {
	bad := []struct {
		name  string
		line  string
		field string
		sub   string
	}{
		{"gte malformed now()", `gte: "now()-"`, "created_at", "gte"},
		{"lte now()+foo", `lte: "now()+foo"`, "created_at", "lte"},
		{"lt now() with interior space", `lt: "now() - 5m"`, "created_at", "lt"},
		{"map operand", `gt: {a: 1}`, "created_at", "must be a number"},
		{"list-of-list operand", `gt: [[1]]`, "created_at", "must be a number"},
		{"numeric-looking string", `gt: "70"`, "created_at", "must be a number"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(templateYAML(tc.line)))
			if err == nil {
				t.Fatalf("expected parse error for %s", tc.name)
			}
			for _, want := range []string{tc.field, "handler.main", tc.sub} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q should mention %q", err, want)
				}
			}
		})
	}

	// Nested malformed operand carries the dotted nested path and handler context.
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        created_at:
          gt: "now()-"
`))
	if err == nil {
		t.Fatal("expected parse error for nested malformed now()")
	}
	for _, want := range []string{"new_image.created_at", "handler.main", "gt"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("nested error %q should mention %q", err, want)
		}
	}

	// A float operand parses fine (yaml gives float64); docs the strict rule
	// that numbers must be YAML numbers, never numeric-looking strings.
	if _, err := ParseTemplate([]byte(templateYAML("gt: 1.5"))); err != nil {
		t.Errorf("gt: 1.5 should parse: %v", err)
	}
}

// TestTemporalMultipleRules exercises MatchingRules with temporal rules: two
// rules, one temporal and one numeric, asserting exactly which rule handlers are
// returned when one, both, or neither pattern matches.
func TestTemporalMultipleRules(t *testing.T) {
	t.Parallel()
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.aging
    pattern:
      created_at:
        gt: "now()-5m"
  - handler: handler.score
    pattern:
      score:
        gte: 90
`, func() time.Time { return fixedNow })

	// Only rule A (created_at fresh, score low).
	onlyA := tmpl.MatchingRules(map[string]any{"created_at": "2026-09-12T10:00:00Z", "score": 50})
	if len(onlyA) != 1 || onlyA[0].Handler != "handler.aging" {
		t.Errorf("MatchingRules single temporal rule = %v, want only handler.aging", ruleHandlers(onlyA))
	}

	// Both rules.
	both := tmpl.MatchingRules(map[string]any{"created_at": "2026-09-12T10:00:00Z", "score": 95})
	if len(both) != 2 {
		t.Fatalf("MatchingRules both = %d rules, want 2", len(both))
	}
	if both[0].Handler != "handler.aging" || both[1].Handler != "handler.score" {
		t.Errorf("MatchingRules both = %v, want handler.aging then handler.score", ruleHandlers(both))
	}

	// Neither rule.
	none := tmpl.MatchingRules(map[string]any{"created_at": "2026-09-12T08:00:00Z", "score": 50})
	if len(none) != 0 {
		t.Errorf("MatchingRules neither = %d rules, want 0", len(none))
	}
}

// ruleHandlers returns the handler names of a slice of rules, for readably
// reporting mismatches in error messages.
func ruleHandlers(rules []Rule) []string {
	names := make([]string, len(rules))
	for i, r := range rules {
		names[i] = r.Handler
	}
	return names
}

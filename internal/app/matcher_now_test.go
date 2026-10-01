package app

import (
	"strings"
	"testing"
	"time"
)

// templateYAML wraps a whole field condition into a minimal parseable template.
func templateYAML(condition string) string {
	return `runtime: python3.14
events:
  - handler: handler.main
    pattern:
      created_at: ` + condition + "\n"
}

// TestParseNowOperand pins the exact now() syntax rules. It is pure syntax —
// it returns only the signed relative offset and never consults a clock. It
// covers the valid offset matrix (including sub-second, large, and zero
// offsets), the non-temporal strings that are simply "not a temporal operand",
// and the malformed forms that are errors.
func TestParseNowOperand(t *testing.T) {
	t.Parallel()

	valid := []struct {
		name string
		in   string
		want time.Duration // signed offset (baked-in sign); zero => clock() itself
	}{
		{"plain now()", "now()", 0},
		{"now() minus 5m", "now()-5m", -5 * time.Minute},
		{"now() plus 5m", "now()+5m", 5 * time.Minute},
		{"now() plus 1h", "now()+1h", time.Hour},
		{"compound duration", "now()-1h30m", -90 * time.Minute},
		{"compound mixed", "now()-2h45m30s", -(2*time.Hour + 45*time.Minute + 30*time.Second)},
		{"zero duration", "now()+0s", 0},
		{"sub-second ms", "now()-500ms", -500 * time.Millisecond},
		{"sub-second us", "now()+250us", 250 * time.Microsecond},
		{"sub-second ns", "now()-1ns", -1 * time.Nanosecond},
		{"large offset 24h", "now()+24h", 24 * time.Hour},
		{"large offset 72h30m", "now()-72h30m", -(72*time.Hour + 30*time.Minute)},
		{"large offset 168h", "now()+168h", 168 * time.Hour},
		{"zero offset minutes", "now()-0m", 0},
		{"zero offset hours", "now()+0h", 0},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, temp, err := parseNowOperand(tc.in)
			if err != nil {
				t.Fatalf("parseNowOperand(%q) error: %v", tc.in, err)
			}
			if !temp {
				t.Errorf("parseNowOperand(%q) = temporal=false, want true", tc.in)
			}
			if got != tc.want {
				t.Errorf("parseNowOperand(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}

	// Non-now() strings (including the removed bare `now`/`now±duration`
	// syntax, uppercase variants, and empty input) are simply "not a temporal
	// operand" (zero, false, nil) — they are rejected by the caller, not as a
	// now() syntax error.
	for _, in := range []string{
		"",
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
		"NOW()-5m",
		"Now",
		"NOW",
	} {
		got, temp, err := parseNowOperand(in)
		if err != nil {
			t.Errorf("parseNowOperand(%q) unexpected error: %v", in, err)
		}
		if temp || got != 0 {
			t.Errorf("parseNowOperand(%q) = (%v, %v), want (0, false)", in, got, temp)
		}
	}

	// Malformed now() expressions are errors (whitespace anywhere, a missing or
	// negative duration, or trailing garbage after the parentheses).
	for _, in := range []string{
		"now()-",
		"now()+",
		"now()-foo",
		"now() - 5m",
		"now() -5m",
		"now() +5m",
		"now()\t-5m",
		"now()+ 5m",
		"now()- 5m",
		"now()--5m",
		"now()+-5m",
		"now()5m",
		"now()now",
		"now()x",
		"now() ",
		"now()  ",
	} {
		if _, _, err := parseNowOperand(in); err == nil {
			t.Errorf("parseNowOperand(%q) expected an error, got nil", in)
		}
	}
}

// TestComparisonParseValidation pins strict parse-time validation of comparison
// operands: malformed values are rejected at ParseTemplate, naming the field.
func TestComparisonParseValidation(t *testing.T) {
	bad := []struct {
		name      string
		condition string
		field     string // expected in the error (field path)
		sub       string // expected substring in the error
	}{
		{"gt malformed now()", `[{gt: "now()-"}]`, "created_at", "now()"},
		{"gt now()+foo", `[{gt: "now()+foo"}]`, "created_at", "now()"},
		{"gt now() with space", `[{gt: "now() - 5m"}]`, "created_at", "now()"},
		{"gt bare old now", `[{gt: "now"}]`, "created_at", "must be a number"},
		{"gt old now-5m", `[{gt: "now-5m"}]`, "created_at", "must be a number"},
		{"gt old now+1h", `[{gt: "now+1h"}]`, "created_at", "must be a number"},
		{"gt uppercase NOW()", `[{gt: "NOW()"}]`, "created_at", "must be a number"},
		{"gt plain string", `[{gt: "hello"}]`, "created_at", "must be a number"},
		{"gt looks like date", `[{gt: "2026-09-12T10:00:00Z"}]`, "created_at", "must be a number"},
		{"gt abc", `[{gt: "abc"}]`, "created_at", "must be a number"},
		{"gt null", `[{gt: null}]`, "created_at", "got null"},
		{"gt null in list", `[{gt: [null]}]`, "created_at", "must be a number"},
		{"lt bool", `[{lt: true}]`, "created_at", "must be a number"},
		{"gt empty list", `[{gt: []}]`, "created_at", "must be a number"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(templateYAML(tc.condition)))
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

// TestComparisonValidParses pins that valid numeric and now operands (single and
// multiple alternatives, mixed kinds, zero duration) all parse without error.
func TestComparisonValidParses(t *testing.T) {
	templates := []string{
		templateYAML(`[{gt: 70}]`),
		templateYAML(`[{lte: 100.5}]`),
		templateYAML(`[{gte: "now()-1h"}]`),
		templateYAML(`[{lt: "now()-5m"}, {lt: 100}]`),
		templateYAML(`[{gt: "now()"}]`),
		templateYAML(`[{gt: "now()+0s"}]`),
		templateYAML(`[{gte: 60}, {gte: "now()-2h45m30s"}]`),
	}
	for i, yaml := range templates {
		if _, err := ParseTemplate([]byte(yaml)); err != nil {
			t.Errorf("template %d should parse: %v", i, err)
		}
	}
}

// TestComparisonParseValidationContext extends TestComparisonParseValidation to
// malformed-now on gte/lte/lt keys and verifies the error carries the field path
// AND the rule handler context, plus non-string operand shapes and the strict
// numeric-looking-string rejection.
func TestComparisonParseValidationContext(t *testing.T) {
	bad := []struct {
		name      string
		condition string
		field     string
		sub       string
	}{
		{"gte malformed now()", `[{gte: "now()-"}]`, "created_at", "gte"},
		{"lte now()+foo", `[{lte: "now()+foo"}]`, "created_at", "lte"},
		{"lt now() with interior space", `[{lt: "now() - 5m"}]`, "created_at", "lt"},
		{"map operand", `[{gt: {a: 1}}]`, "created_at", "must be a number"},
		{"list operand", `[{gt: [1]}]`, "created_at", "must be a number"},
		{"numeric-looking string", `[{gt: "70"}]`, "created_at", "must be a number"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(templateYAML(tc.condition)))
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
        created_at: [{gt: "now()-"}]
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
	if _, err := ParseTemplate([]byte(templateYAML(`[{gt: 1.5}]`))); err != nil {
		t.Errorf("gt: 1.5 should parse: %v", err)
	}
}

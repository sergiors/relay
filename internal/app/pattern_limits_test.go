package app

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"unicode/utf8"
)

// depthTemplateYAML builds a template whose single pattern has exactly `depth`
// nested field levels, the deepest holding a literal list. depth=1 is a bare
// top-level field; depth=9 exceeds MaxPatternDepth.
func depthTemplateYAML(depth int) string {
	var b strings.Builder
	b.WriteString("runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n")
	indent := "      "
	for i := 1; i < depth; i++ {
		fmt.Fprintf(&b, "%sf%d:\n", indent, i)
		indent += "  "
	}
	fmt.Fprintf(&b, "%sstatus: [x]\n", indent)
	return b.String()
}

// compositeTemplateYAML builds a template whose pattern field `tags` carries a
// composite literal of `n` nested lists inside the OR wrapper, i.e.
// `tags: [[...[1]...]]`. n=7 lands exactly on the depth limit; n=8 exceeds it.
func compositeTemplateYAML(n int) string {
	inner := strings.Repeat("[", n) + "1" + strings.Repeat("]", n)
	return "runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      tags: [" + inner + "]\n"
}

// stringSizeTemplateYAML builds a template whose single pattern field `status`
// holds one string literal of the given value.
func stringSizeTemplateYAML(value string) string {
	return "runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      status: [\"" + value + "\"]\n"
}

// nonStringKeyedTemplateYAML builds a template whose pattern field `tags` holds a
// composite literal map with a non-string (integer) key. YAML decodes such a map
// to map[any]any, which JSON cannot encode and which can never equal a JSON event
// object.
func nonStringKeyedTemplateYAML(value string) string {
	return "runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      tags: [[{1: \"" + value + "\"}]]\n"
}

// chainValue returns a raw field VALUE forming n nested field levels below its
// owning field (the owning field itself is one further level). chainValue(0) is
// a literal list leaf.
func chainValue(n int) any {
	if n == 0 {
		return []any{"x"}
	}
	return map[string]any{fmt.Sprintf("f%d", n): chainValue(n - 1)}
}

// nestedChain returns a raw top-level pattern whose single chain has n field
// levels.
func nestedChain(n int) map[string]any {
	return map[string]any{"f1": chainValue(n - 1)}
}

// deepList returns a raw composite literal of n nested single-element lists.
func deepList(n int) any {
	var v any = 1
	for i := 0; i < n; i++ {
		v = []any{v}
	}
	return v
}

// TestParsePatternDepthBoundary pins the field-level depth cap end to end: depth
// 1, 7, and 8 parse; depth 9 is rejected with an error naming the rule, the
// offending path, and the explicit limit.
func TestParsePatternDepthBoundary(t *testing.T) {
	t.Parallel()
	for _, depth := range []int{1, 7, 8} {
		depth := depth
		t.Run(fmt.Sprintf("depth-%d-accepted", depth), func(t *testing.T) {
			t.Parallel()
			if _, err := ParseTemplate([]byte(depthTemplateYAML(depth))); err != nil {
				t.Fatalf("depth %d should parse: %v", depth, err)
			}
		})
	}
	t.Run("depth-9-rejected", func(t *testing.T) {
		t.Parallel()
		_, err := ParseTemplate([]byte(depthTemplateYAML(9)))
		if err == nil {
			t.Fatal("depth 9 should be rejected")
		}
		for _, want := range []string{"handler.main", "depth", fmt.Sprintf("%d", MaxPatternDepth)} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should mention %q", err, want)
			}
		}
	})
}

// TestPatternDepthSemantics pins the structural-depth rule on hand-built raw
// patterns: siblings, OR alternatives, and operator maps do not accumulate
// depth; nested children and composite-literal containers do.
func TestPatternDepthSemantics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		pattern map[string]any
		ok      bool
	}{
		{"depth-1", nestedChain(1), true},
		{"depth-8", nestedChain(8), true},
		{"depth-9", nestedChain(9), false},
		// Siblings do not accumulate: three depth-8 branches are still depth 8.
		{"siblings-do-not-accumulate", map[string]any{
			"a": chainValue(7), "b": chainValue(7), "c": chainValue(7),
		}, true},
		// OR alternatives do not accumulate: several depth-8 branches OR'd.
		{"alternatives-do-not-accumulate", map[string]any{
			"status": []any{chainValue(7), chainValue(7), "x"},
		}, true},
		// A one-key operator map consumes no level.
		{"operator-map-no-depth", map[string]any{
			"id": []any{map[string]any{"prefix": "user_"}},
		}, true},
		// A nested-map alternative introduces its child fields at the next level.
		{"nested-map-alternative-depth-2", map[string]any{
			"new_image": []any{map[string]any{"status": []any{"COMPLETED"}}},
		}, true},
		{"nested-map-alternative-depth-8", map[string]any{
			"new_image": []any{chainValue(7)},
		}, true},
		{"nested-map-alternative-depth-9", map[string]any{
			"new_image": []any{chainValue(8)},
		}, false},
		// Composite literal containers consume one level per nesting.
		{"composite-list-depth-2", map[string]any{
			"tags": []any{deepList(1)},
		}, true},
		{"composite-list-depth-8", map[string]any{
			"tags": []any{deepList(7)},
		}, true},
		{"composite-list-depth-9", map[string]any{
			"tags": []any{deepList(8)},
		}, false},
		// A map nested inside a composite list is bounded the same way.
		{"composite-map-in-list", map[string]any{
			"tags": []any{[]any{map[string]any{"a": []any{"x"}}}},
		}, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validatePatternLimits(tc.pattern)
			if tc.ok && err != nil {
				t.Fatalf("pattern should be accepted: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("pattern should be rejected")
			}
		})
	}
}

// TestParsePatternCompositeDepthBoundary pins the composite-literal cap end to
// end and confirms ordinary operator arguments and the OR wrapper are not
// rejected.
func TestParsePatternCompositeDepthBoundary(t *testing.T) {
	t.Parallel()
	t.Run("normal-shapes-accepted", func(t *testing.T) {
		t.Parallel()
		for i, yaml := range []string{
			// A normal OR wrapper of scalars.
			"runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      status: [COMPLETED, FAILED]\n",
			// A normal single-key operator argument.
			"runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      id: [{prefix: \"user_\"}]\n",
			// A shallow composite literal (an equality value).
			"runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n      tags: [[1, 2]]\n",
		} {
			if _, err := ParseTemplate([]byte(yaml)); err != nil {
				t.Fatalf("template %d should parse: %v", i, err)
			}
		}
	})
	t.Run("composite-7-accepted", func(t *testing.T) {
		t.Parallel()
		if _, err := ParseTemplate([]byte(compositeTemplateYAML(7))); err != nil {
			t.Fatalf("7 nested composite lists should parse: %v", err)
		}
	})
	t.Run("composite-8-rejected", func(t *testing.T) {
		t.Parallel()
		_, err := ParseTemplate([]byte(compositeTemplateYAML(8)))
		if err == nil {
			t.Fatal("8 nested composite lists should be rejected")
		}
		if !strings.Contains(err.Error(), "depth") || !strings.Contains(err.Error(), fmt.Sprintf("%d", MaxPatternDepth)) {
			t.Errorf("error %q should mention depth and the limit %d", err, MaxPatternDepth)
		}
	})
}

// TestPatternRuneSizeMatchesJSONConvention pins the size measurement convention:
// the counted size equals the rune count of encoding/json's compact, key-sorted
// serialization for JSON-representable patterns, and non-finite floats count as
// the null literal.
func TestPatternRuneSizeMatchesJSONConvention(t *testing.T) {
	t.Parallel()
	patterns := []map[string]any{
		{"status": []any{"COMPLETED", "FAILED"}},
		{"b": []any{1}, "a": []any{"é", "😀"}},
		{"html": []any{"<>&"}},
		{"nested": map[string]any{"x": []any{nil, true, 1.5}}},
	}
	for i, p := range patterns {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatalf("pattern %d: json.Marshal: %v", i, err)
		}
		want := utf8.RuneCount(b)
		if got := patternRuneSize(p); got != want {
			t.Fatalf("pattern %d: patternRuneSize = %d, want json rune count %d (%s)", i, got, want, b)
		}
	}
	// Non-finite floats cannot be marshaled by encoding/json; they are measured
	// as `null` and must not error or panic.
	nanPattern := map[string]any{"status": []any{math.NaN(), math.Inf(1)}}
	if got, want := patternRuneSize(nanPattern), patternRuneSize(map[string]any{"status": []any{nil, nil}}); got != want {
		t.Fatalf("non-finite float size = %d, want null size %d", got, want)
	}
}

// TestNonStringKeyedCompositeMapRejected pins that a composite literal map with
// non-string keys (YAML's map[any]any) is a parse error, never measured as the
// four-rune null literal. Such a map has no canonical JSON encoding and can never
// match a JSON object, so even a subtree far larger than the size cap must be
// rejected — it must not slip past the 4096-character limit.
func TestNonStringKeyedCompositeMapRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"small", "x"},
		// A subtree whose serialized size would be far over the cap if it were
		// measurable at all.
		{"over-size-cap", strings.Repeat("x", 2*MaxPatternSize)},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseTemplate([]byte(nonStringKeyedTemplateYAML(tc.value)))
			if err == nil {
				t.Fatal("a non-string-keyed composite map must be rejected")
			}
			if !strings.Contains(err.Error(), "non-string keys") {
				t.Errorf("error %q should mention non-string keys", err)
			}
		})
	}
	// The structural rejection holds for the raw pattern, and the size counter
	// fails closed for a non-string-keyed map even if reached directly, so a large
	// such subtree can never be measured as the null literal.
	huge := map[string]any{"tags": []any{map[any]any{1: strings.Repeat("x", 2*MaxPatternSize)}}}
	if err := validatePatternLimits(huge); err == nil || !strings.Contains(err.Error(), "non-string keys") {
		t.Fatalf("validatePatternLimits = %v, want a non-string-keys error", err)
	}
	if got := patternRuneSize(huge); got <= MaxPatternSize {
		t.Fatalf("patternRuneSize(non-string-keyed) = %d, want over-limit", got)
	}
}

// TestParsePatternSizeBoundary pins the exact size boundaries: 4095 and 4096
// canonical Unicode characters parse; 4097 is rejected with an error naming the
// rule and the limit. The value is multibyte, proving the count is in code
// points rather than UTF-8 bytes.
func TestParsePatternSizeBoundary(t *testing.T) {
	t.Parallel()
	// The canonical size of `{"status":["<s>"]}` is its JSON rune count.
	base := utf8.RuneCount(mustJSON(t, map[string]any{"status": []any{""}}))
	if base <= 0 || base >= MaxPatternSize {
		t.Fatalf("unexpected base size %d", base)
	}
	exact := MaxPatternSize - base
	multibyte := "é"
	if len(multibyte) == utf8.RuneCountInString(multibyte) {
		t.Fatal("test value must be multibyte")
	}
	value := func(n int) string { return strings.Repeat(multibyte, n) }

	// Self-check the arithmetic against the measurement convention.
	for _, n := range []int{exact - 1, exact, exact + 1} {
		size := patternRuneSize(map[string]any{"status": []any{value(n)}})
		if size != base+n {
			t.Fatalf("value length %d: size = %d, want %d", n, size, base+n)
		}
	}

	for _, tc := range []struct {
		name string
		n    int
		ok   bool
	}{
		{"4095-accepted", exact - 1, true},
		{"4096-accepted", exact, true},
		{"4097-rejected", exact + 1, false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseTemplate([]byte(stringSizeTemplateYAML(value(tc.n))))
			if tc.ok && err != nil {
				t.Fatalf("%d characters should parse: %v", tc.n, err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatalf("%d characters should be rejected", tc.n)
				}
				for _, want := range []string{"handler.main", "size", fmt.Sprintf("%d", MaxPatternSize)} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q should mention %q", err, want)
					}
				}
			}
		})
	}
}

// TestParsePatternSizePerPatternScope pins that the size cap is per pattern: two
// patterns whose COMBINED size exceeds 4096 both parse, because neither pattern
// individually exceeds the cap.
func TestParsePatternSizePerPatternScope(t *testing.T) {
	t.Parallel()
	base := utf8.RuneCount(mustJSON(t, map[string]any{"status": []any{""}}))
	perPattern := MaxPatternSize - base - 100 // comfortably under the cap
	value := strings.Repeat("é", perPattern)
	if got := patternRuneSize(map[string]any{"status": []any{value}}); got > MaxPatternSize {
		t.Fatalf("per-pattern size %d exceeds the cap", got)
	}
	tmpl := `
runtime: python3.14
events:
  - handler: handler.first
    pattern:
      status: ["` + value + `"]
  - handler: handler.second
    pattern:
      status: ["` + value + `"]
`
	parsed, err := ParseTemplate([]byte(tmpl))
	if err != nil {
		t.Fatalf("two sub-limit patterns should parse: %v", err)
	}
	if len(parsed.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(parsed.Events))
	}
}

// TestMalformedNestedPatternsUseParserErrors pins that malformed nested patterns
// within the depth cap still fail through the existing parser error path, and
// that an over-deep pattern is rejected for depth rather than panicking.
func TestMalformedNestedPatternsUseParserErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		condition string
		want      string
	}{
		{"null child", "{b: null}", "condition is null"},
		{"empty child map", "{b: {}}", "condition map is empty"},
		{"empty child list", "{b: []}", "condition list is empty"},
		{"scalar child", "{b: 1}", "condition must be a non-empty list"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      a: ` + tc.condition + "\n"))
			if err == nil {
				t.Fatal("malformed nested pattern should fail parsing")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q should contain %q", err, tc.want)
			}
		})
	}
	// A malformed pattern that is ALSO over the depth cap is rejected for depth
	// (the cap is checked first), never by recursing until the stack overflows.
	var deep strings.Builder
	deep.WriteString("runtime: python3.14\nevents:\n  - handler: handler.main\n    pattern:\n")
	indent := "      "
	for i := 1; i <= 9; i++ {
		fmt.Fprintf(&deep, "%sf%d:\n", indent, i)
		indent += "  "
	}
	deep.WriteString(indent + "status: null\n")
	_, err := ParseTemplate([]byte(deep.String()))
	if err == nil || !strings.Contains(err.Error(), "depth") {
		t.Fatalf("over-deep malformed pattern should fail for depth, got %v", err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

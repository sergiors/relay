package function

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// indexHandlers returns the handler names of a rule slice, for order-sensitive
// equivalence assertions.
func indexHandlers(rules []EventRule) []string {
	out := make([]string, len(rules))
	for i, r := range rules {
		out[i] = r.Handler
	}
	return out
}

// assertIndexEquivalent asserts the indexed matcher returns exactly the same
// rules, in the same order, as a full exact scan for every event in the battery.
// It is the core false-negative guard: a divergence means the index dropped a
// rule the exact matcher would have kept (or produced a spurious rule).
func assertIndexEquivalent(t *testing.T, tmpl *Template, events []map[string]any) {
	t.Helper()
	ix := NewRuleIndex(tmpl.Events)
	// The index is bound to a rule slice; a full scan through the template must
	// produce the same rules.
	for ei, event := range events {
		got := ix.MatchingEventRules(event)
		want := tmpl.MatchingEventRules(event)
		if len(got) != len(want) {
			t.Fatalf("event %d %v: index returned %d rules, full scan %d (indexed=%v full=%v)",
				ei, event, len(got), len(want), indexHandlers(got), indexHandlers(want))
		}
		for i := range got {
			if got[i].Handler != want[i].Handler {
				t.Fatalf("event %d %v: rule %d handler = %q, want %q (indexed=%v full=%v)",
					ei, event, i, got[i].Handler, want[i].Handler, indexHandlers(got), indexHandlers(want))
			}
		}
	}
}

// equivalenceEventsForPatterns is the shared event battery used by the
// equivalence test below. It deliberately mixes every scalar JSON kind, null,
// composite values, missing fields, nested maps, and RFC3339 strings.
func equivalenceEventsForPatterns() []map[string]any {
	return []map[string]any{
		{},
		{"status": "COMPLETED"},
		{"status": "FAILED"},
		{"status": "PENDING"},
		{"status": nil},
		{"status": false},
		{"status": 0},
		{"status": ""},
		{"status": []any{}},
		{"status": map[string]any{}},
		{"event_name": "MODIFY", "table_name": "enrollments"},
		{
			"event_name": "MODIFY",
			"table_name": "enrollments",
			"new_image": map[string]any{
				"id":     "ENROLLMENT#123",
				"status": "COMPLETED",
				"email":  "user@example.com",
				"score":  92,
			},
		},
		{"new_image": map[string]any{"cnpj": nil}},
		{"new_image": map[string]any{"name": "ACME"}},
		{"new_image": nil},
		{"a": map[string]any{"b": map[string]any{"c": "x"}}},
		{"a": map[string]any{"b": map[string]any{}}},
		{"a": map[string]any{"b": nil}},
		{"field": "hello"},
		{"field": 42},
		{"field": float64(1.5)},
		{"field": 0},
		{"field": false},
		{"field": true},
		{"field": ""},
		{"field": nil},
		{"field": []any{}},
		{"field": map[string]any{}},
		{"field": "1"},
		{"field": 1},
		{"field": "true"},
		{"score": 70},
		{"score": float64(70)},
		{"score": "70"},
		{"active": true},
		{"active": false},
		{"active": "true"},
		{"id": "ENROLLMENT#123"},
		{"id": "OTHER#123"},
		{"email": "user@example.com"},
		{"email": "user@other.com"},
		{"created_at": "2026-09-12T10:00:00Z"},
		{"created_at": "2026-09-12T08:00:00Z"},
		{"created_at": 42},
		{"created_at": nil},
		{"cnpj": "12abc"},
		{"cnpj": "ab"},
		{"cnpj": nil},
		{"price": 100.5},
		{"price": 50},
		{"count": 10},
		{"count": 11},
	}
}

// TestIndexEquivalence exercises the indexed matcher against a broad battery of
// patterns and events, asserting exact rule-set and order equivalence with a full
// scan. This is the primary false-negative guard for the index.
func TestIndexEquivalence(t *testing.T) {
	t.Parallel()
	templates := []string{
		// Match-all empty pattern.
		`runtime: python3.14
events:
  - handler: handler.main
    pattern: {}
`,
		// Implicit and explicit equality (now always a literal list).
		`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED, IN_PROGRESS]
  - handler: handler.b
    pattern:
      status: [FAILED]
`,
		// Prefix/suffix/range fallback rules.
		`runtime: python3.14
events:
  - handler: handler.prefix
    pattern:
      id: [{prefix: "ENROLLMENT#"}]
  - handler: handler.suffix
    pattern:
      email: [{suffix: "@example.com"}]
  - handler: handler.range
    pattern:
      score: [{gt: 70}, {lte: 100}]
`,
		// OR mix and exists polarities.
		`runtime: python3.14
events:
  - handler: handler.mix
    pattern:
      cnpj: [{exists: false}, {prefix: "12"}]
  - handler: handler.present
    pattern:
      field: [{exists: true}]
  - handler: handler.absent
    pattern:
      field: [{exists: false}]
`,
		// Nested equality, nested exists, deep nesting.
		`runtime: python3.14
events:
  - handler: handler.nested
    pattern:
      new_image:
        status: [COMPLETED]
        id: [{prefix: "ENROLLMENT#"}]
        email: [{suffix: "@example.com"}]
  - handler: handler.nestedexists
    pattern:
      new_image:
        cnpj: [{exists: true}]
  - handler: handler.deep
    pattern:
      a:
        b:
          c: [{exists: true}]
`,
		// Multiple top-level AND fields.
		`runtime: python3.14
events:
  - handler: handler.and
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
`,
		// Typed equality: numeric/bool/string/null collisions.
		`runtime: python3.14
events:
  - handler: handler.num
    pattern:
      field: [1]
  - handler: handler.str
    pattern:
      field: ["1"]
  - handler: handler.bool
    pattern:
      field: [true]
  - handler: handler.null
    pattern:
      field: [null]
  - handler: handler.float
    pattern:
      count: [10]
`,
	}
	for ti, yaml := range templates {
		tmpl := mustParse(t, yaml)
		t.Run(fmt.Sprintf("template-%d", ti), func(t *testing.T) {
			assertIndexEquivalent(t, tmpl, equivalenceEventsForPatterns())
		})
	}
}

// TestIndexEquivalenceTemporal pins that temporal (`now()`-relative) rules are
// never cached: the index routes them through the exact matcher, whose clock is
// consulted per call. It advances the clock between two matches on the SAME
// template instance and asserts the indexed result tracks the change.
func TestIndexEquivalenceTemporal(t *testing.T) {
	t.Parallel()
	now := fixedNow
	tmpl := mustParseWithClock(t, `
runtime: python3.14
events:
  - handler: handler.fresh
    pattern:
      created_at: [{gt: "now()-5m"}]
  - handler: handler.stale
    pattern:
      created_at: [{lt: "now()-1h"}]
`, func() time.Time { return now })

	ix := NewRuleIndex(tmpl.Events)
	event := map[string]any{"created_at": "2026-09-12T09:58:00Z"}

	// At 10:00:00Z, cutoff gt is 09:55:00Z -> fresh matches.
	if got := indexHandlers(ix.MatchingEventRules(event)); strings.Join(got, ",") != "handler.fresh" {
		t.Fatalf("at fixedNow = %v, want [handler.fresh]", got)
	}
	// Move the clock forward 20m: cutoff gt = 10:15:00Z -> 09:58 no longer fresh.
	now = fixedNow.Add(20 * time.Minute)
	if got := indexHandlers(ix.MatchingEventRules(event)); len(got) != 0 {
		t.Fatalf("after clock advance, want no matches, got %v", got)
	}
	// Equivalence after the move: index and full scan agree.
	assertIndexEquivalent(t, tmpl, []map[string]any{event})
}

// TestRuleAnchorSelection pins the deterministic one-anchor policy: equality over
// exists, and lexicographically smallest normalized path within a kind.
func TestRuleAnchorSelection(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		build func() Pattern
		want  anchor
		ok    bool
	}{
		{
			name: "equality preferred over exists",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      z:
        status: [ok]
      top: [{exists: true}]
`)
				return tmpl.Events[0].Pattern
			},
			want: anchor{kind: anchorEquals, path: "z" + pathSeparator + "status", keys: []string{`s:"ok"`}},
			ok:   true,
		},
		{
			name: "lexicographically smallest equals path",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      b: [x]
      a: [y]
`)
				return tmpl.Events[0].Pattern
			},
			want: anchor{kind: anchorEquals, path: "a", keys: []string{`s:"y"`}},
			ok:   true,
		},
		{
			name: "exists false only falls back",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [{exists: false}]
`)
				return tmpl.Events[0].Pattern
			},
			ok: false,
		},
		{
			name: "OR mix falls back",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: [{exists: false}, {prefix: "12"}]
`)
				return tmpl.Events[0].Pattern
			},
			ok: false,
		},
		{
			name: "literal plus prefix mix falls back",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id: [SPECIAL, {prefix: "ENROLLMENT#"}]
`)
				return tmpl.Events[0].Pattern
			},
			ok: false,
		},
		{
			name: "all-literal set is a multi-key equality anchor",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED, FAILED]
`)
				return tmpl.Events[0].Pattern
			},
			want: anchor{kind: anchorEquals, path: "status", keys: []string{`s:"COMPLETED"`, `s:"FAILED"`}},
			ok:   true,
		},
		{
			name: "non-scalar literal operand falls back",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [[1, 2]]
`)
				return tmpl.Events[0].Pattern
			},
			ok: false,
		},
		{
			name: "lone positive exists is an anchor",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: true}]
`)
				return tmpl.Events[0].Pattern
			},
			want: anchor{kind: anchorExists, path: "new_image"},
			ok:   true,
		},
		{
			name: "match-all empty pattern has no anchor",
			build: func() Pattern {
				tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern: {}
`)
				return tmpl.Events[0].Pattern
			},
			ok: false,
		},
		{
			name: "child anchor survives an unindexable parent alternative set",
			build: func() Pattern {
				// A parsed template never emits alternatives AND children on one
				// condition (the YAML is either a list or a child map), so this
				// hand-built shape is the only way to exercise the AND property
				// directly: the parent's lone prefix alternative is not an anchor,
				// but the ANDed child equality still is.
				return Pattern{
					"new_image": FieldCondition{
						Alternatives: []Alternative{{Operator: prefixMatcher{prefix: "x"}}},
						Children: map[string]FieldCondition{
							"status": {Alternatives: []Alternative{{Literal: &Literal{Value: "COMPLETED"}}}},
						},
					},
				}
			},
			want: anchor{kind: anchorEquals, path: "new_image" + pathSeparator + "status", keys: []string{`s:"COMPLETED"`}},
			ok:   true,
		},
		{
			name: "lone nested group recurses into its children",
			build: func() Pattern {
				// A field whose lone alternative is a nested group: the group's
				// children are ANDed and so necessary, and the child equality is
				// an anchor.
				return Pattern{
					"new_image": FieldCondition{
						Alternatives: []Alternative{{Operator: childrenMatcher{children: map[string]FieldCondition{
							"status": {Alternatives: []Alternative{{Literal: &Literal{Value: "COMPLETED"}}}},
						}}}},
					},
				}
			},
			want: anchor{kind: anchorEquals, path: "new_image" + pathSeparator + "status", keys: []string{`s:"COMPLETED"`}},
			ok:   true,
		},
		{
			name: "two nested-group alternatives fall back",
			build: func() Pattern {
				// Each group is an OR alternative, so neither group's children are
				// necessary — no anchor.
				return Pattern{
					"new_image": FieldCondition{
						Alternatives: []Alternative{
							{Operator: childrenMatcher{children: map[string]FieldCondition{
								"status": {Alternatives: []Alternative{{Literal: &Literal{Value: "COMPLETED"}}}},
							}}},
							{Operator: childrenMatcher{children: map[string]FieldCondition{
								"status": {Alternatives: []Alternative{{Literal: &Literal{Value: "FAILED"}}}},
							}}},
						},
					},
				}
			},
			ok: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ruleAnchor(tc.build())
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if got.kind != tc.want.kind || got.path != tc.want.path {
				t.Fatalf("anchor = {%d %q}, want {%d %q}", got.kind, got.path, tc.want.kind, tc.want.path)
			}
			if strings.Join(got.keys, ",") != strings.Join(tc.want.keys, ",") {
				t.Fatalf("keys = %v, want %v", got.keys, tc.want.keys)
			}
		})
	}
}

// TestScalarKeyCollisionFree pins that the type-tagged keys never collide across
// string, numeric, bool, and null, and that all numeric kinds normalize the way
// the equality matcher compares them.
func TestScalarKeyCollisionFree(t *testing.T) {
	t.Parallel()
	// Exactly one representative per semantically DISTINCT value. Numeric kinds
	// of the same number intentionally share a key (that is the matcher's
	// semantics), so they are checked separately below rather than here.
	distinct := []any{"1", 1, true, "true", false, "false", nil, 0, "0", 1.5}
	keys := map[string]string{}
	for _, v := range distinct {
		k, ok := scalarKey(v)
		if !ok {
			t.Fatalf("scalarKey(%#v) not scalar", v)
		}
		if prev, dup := keys[k]; dup {
			t.Fatalf("key collision: %#v and %s both -> %q", v, prev, k)
		}
		keys[k] = fmt.Sprintf("%#v", v)
	}
	// Numeric equality (the matcher's semantics): every numeric spelling of 1
	// shares one key, and +0/-0 share one key.
	for _, v := range []any{int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1)} {
		if mustKey(t, v) != mustKey(t, 1) {
			t.Fatalf("numeric kind %T(1) must share the numeric key of 1", v)
		}
	}
	if mustKey(t, -0.0) != mustKey(t, 0.0) {
		t.Fatal("-0 and +0 must share a key (numericEqual treats them equal)")
	}
	// Non-scalar values are rejected.
	for _, v := range []any{map[string]any{}, []any{}, struct{}{}} {
		if _, ok := scalarKey(v); ok {
			t.Fatalf("scalarKey(%#v) reported scalar", v)
		}
	}
}

func mustKey(t *testing.T, v any) string {
	t.Helper()
	k, ok := scalarKey(v)
	if !ok {
		t.Fatalf("scalarKey(%#v) not scalar", v)
	}
	return k
}

// TestLiteralAlternativeKeysRejectsNonLiteral pins that any operator or
// non-scalar literal in the alternative set forces the whole rule to fall back
// (the matcher may still match it, so a scalar key must never narrow it).
func TestLiteralAlternativeKeysRejectsNonLiteral(t *testing.T) {
	t.Parallel()
	if _, ok := literalAlternativeKeys([]Alternative{{Operator: prefixMatcher{prefix: "x"}}}); ok {
		t.Fatal("operator alternative must force fallback")
	}
	if _, ok := literalAlternativeKeys([]Alternative{{Literal: &Literal{Value: []any{1}}}}); ok {
		t.Fatal("non-scalar literal must force fallback")
	}
	if _, ok := literalAlternativeKeys([]Alternative{{Literal: &Literal{Value: "x"}}, {Literal: &Literal{Value: nil}}}); !ok {
		t.Fatal("string+nil literals are all scalar")
	}
	if _, ok := literalAlternativeKeys(nil); ok {
		t.Fatal("empty alternative set must force fallback")
	}
}

// TestIndexDuplicateEqualsIndexedOnce pins that a rule anchored under several
// equality keys appears exactly once in the candidate set.
func TestIndexDuplicateEqualsIndexedOnce(t *testing.T) {
	t.Parallel()
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED, FAILED]
  - handler: handler.b
    pattern:
      status: [FAILED]
`)
	ix := NewRuleIndex(tmpl.Events)
	for _, event := range []map[string]any{{"status": "COMPLETED"}, {"status": "FAILED"}} {
		cands := ix.Candidates(event)
		seen := map[int]int{}
		for _, i := range cands {
			seen[i]++
		}
		for i, n := range seen {
			if n != 1 {
				t.Fatalf("event %v: rule %d appeared %d times in candidates", event, i, n)
			}
		}
	}

	// The multi-key rule must be a candidate when only its second key matches.
	ix = NewRuleIndex(mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED, FAILED]
`).Events)
	if got := ix.Candidates(map[string]any{"status": "FAILED"}); len(got) != 1 || got[0] != 0 {
		t.Fatalf("second equality key must include the rule, got %v", got)
	}
}

// TestIndexOrderPreserved pins that candidates and matched rules keep declaration
// order regardless of map iteration order inside a pattern.
func TestIndexOrderPreserved(t *testing.T) {
	t.Parallel()
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.first
    pattern:
      b: [x]
      a: [y]
  - handler: handler.second
    pattern:
      a: [y]
  - handler: handler.third
    pattern:
      c: [{prefix: "z"}]
  - handler: handler.fourth
    pattern: {}
`)
	ix := NewRuleIndex(tmpl.Events)
	event := map[string]any{"a": "y", "b": "x", "c": "zebra"}
	got := indexHandlers(ix.MatchingEventRules(event))
	want := []string{"handler.first", "handler.second", "handler.third", "handler.fourth"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", got, want)
	}
	assertIndexEquivalent(t, tmpl, []map[string]any{event})
}

// TestIndexEquivalenceRandomized is a deterministic property test: for a fixed
// set of templates it feeds many pseudo-random events (built from a pool that
// includes every scalar kind, null, composite values, and missing fields) and
// asserts the indexed matcher agrees with a full scan on every one.
func TestIndexEquivalenceRandomized(t *testing.T) {
	t.Parallel()
	templates := []*Template{
		mustParse(t, `
runtime: python3.14
events:
  - handler: handler.eq
    pattern:
      status: [COMPLETED, FAILED]
  - handler: handler.num
    pattern:
      count: [10, 20]
  - handler: handler.mixed
    pattern:
      event_name: [MODIFY]
      new_image:
        status: [COMPLETED]
  - handler: handler.prefix
    pattern:
      id: [{prefix: "ENR"}]
  - handler: handler.exists
    pattern:
      new_image:
        cnpj: [{exists: true}]
`),
		mustParse(t, `
runtime: python3.14
events:
  - handler: handler.nullish
    pattern:
      field: [null]
  - handler: handler.bools
    pattern:
      field: [true]
  - handler: handler.strings
    pattern:
      field: ["1", "true", "null"]
  - handler: handler.deep
    pattern:
      a:
        b:
          c: [x]
  - handler: handler.neg
    pattern:
      field: [{exists: false}]
`),
	}

	rng := rand.New(rand.NewSource(20260929))
	values := []any{
		nil, true, false, "COMPLETED", "FAILED", "ENR#1", "x", "1", "true", "null",
		0, 1, 10, 20, 1.5, float64(10), -0.0,
		map[string]any{"status": "COMPLETED", "cnpj": nil},
		map[string]any{"b": map[string]any{"c": "x"}},
		[]any{}, []any{"x"},
	}
	fields := []string{"status", "count", "event_name", "id", "new_image", "field", "a", "b", "cnpj", "table_name"}
	for i := 0; i < 3000; i++ {
		event := map[string]any{}
		n := rng.Intn(4)
		for j := 0; j < n; j++ {
			event[fields[rng.Intn(len(fields))]] = values[rng.Intn(len(values))]
		}
		// Occasionally nest a map under new_image/a so nested paths are exercised.
		if rng.Intn(3) == 0 {
			event["new_image"] = map[string]any{"status": values[rng.Intn(len(values))], "cnpj": values[rng.Intn(len(values))]}
		}
		if rng.Intn(4) == 0 {
			event["a"] = map[string]any{"b": map[string]any{"c": values[rng.Intn(len(values))]}}
		}
		for _, tmpl := range templates {
			got := indexHandlers(NewRuleIndex(tmpl.Events).MatchingEventRules(event))
			want := indexHandlers(tmpl.MatchingEventRules(event))
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("indexed=%v full=%v event=%v", got, want, event)
			}
		}
	}
}

// TestIndexAnchorNarrowsExactVerifies pins the core index contract: a rule is a
// candidate when its anchor's necessary condition holds, but the exact matcher
// still verifies the remaining conditions, so an anchor-passing rule whose other
// field fails is a candidate that does NOT match. It also pins that a rule whose
// anchor fails is not a candidate (a genuine prune, not just a filter).
func TestIndexAnchorNarrowsExactVerifies(t *testing.T) {
	t.Parallel()
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.anchored
    pattern:
      status: [COMPLETED]
      table_name: [enrollments]
      id: [{prefix: "ENR"}]
  - handler: handler.always
    pattern: {}
`)
	ix := NewRuleIndex(tmpl.Events)

	// The anchor (status == COMPLETED) holds, so the rule is a candidate; the
	// exact matcher then rejects it because table_name/prefix fail.
	cands := ix.Candidates(map[string]any{"status": "COMPLETED", "table_name": "users", "id": "OTHER"})
	if !containsIndex(cands, 0) {
		t.Fatalf("anchor-passing rule must be a candidate, got %v", cands)
	}
	if got := indexHandlers(ix.MatchingEventRules(map[string]any{"status": "COMPLETED", "table_name": "users", "id": "OTHER"})); strings.Join(got, ",") != "handler.always" {
		t.Fatalf("exact matcher must reject the anchor-passing rule, got %v", got)
	}

	// The anchor fails (status differs): the rule is pruned, not even a candidate.
	pruned := ix.Candidates(map[string]any{"status": "FAILED", "table_name": "enrollments", "id": "ENR#1"})
	if containsIndex(pruned, 0) {
		t.Fatalf("anchor-failing rule must be pruned, got %v", pruned)
	}

	// The fallback rule is always a candidate.
	if !containsIndex(cands, 1) || !containsIndex(pruned, 1) {
		t.Fatalf("fallback rule must always be a candidate: cands=%v pruned=%v", cands, pruned)
	}

	// Full equivalence across the battery.
	assertIndexEquivalent(t, tmpl, []map[string]any{
		{"status": "COMPLETED", "table_name": "enrollments", "id": "ENR#1"},
		{"status": "COMPLETED", "table_name": "users", "id": "ENR#1"},
		{"status": "FAILED", "table_name": "enrollments", "id": "ENR#1"},
		{},
	})
}

func containsIndex(idxs []int, want int) bool {
	for _, i := range idxs {
		if i == want {
			return true
		}
	}
	return false
}

// FuzzIndexMatchesFullScan is a Go native fuzz target: for any template and any
// JSON object event, the indexed matcher must return exactly the rules a full
// exact scan returns, in the same order. Parse/decode failures are skipped.
// Seeds run under plain `go test`; run with -fuzz for deeper exploration.
func FuzzIndexMatchesFullScan(f *testing.F) {
	seeds := []struct {
		tmpl  string
		event string
	}{
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED]
`, `{"status":"COMPLETED"}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      field: [1]
  - handler: handler.b
    pattern:
      field: ["1"]
`, `{"field":1}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      new_image:
        cnpj: [{exists: true}]
`, `{"new_image":{"cnpj":null}}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      id: [{prefix: "ENR"}]
`, `{"id":"ENR#1"}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern: {}
`, `{}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED]
      score: [{gt: 10}]
`, `{"status":"COMPLETED","score":42}`},
		{`runtime: python3.14
events:
  - handler: handler.a
    pattern:
      field: [null]
`, `{"field":null}`},
	}
	for _, s := range seeds {
		f.Add(s.tmpl, s.event)
	}
	f.Fuzz(func(t *testing.T, tmplYAML, eventJSON string) {
		tmpl, err := parseTemplateWithClock([]byte(tmplYAML), func() time.Time { return fixedNow })
		if err != nil {
			t.Skip()
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(eventJSON), &event); err != nil {
			t.Skip()
		}
		if event == nil {
			event = map[string]any{}
		}
		got := indexHandlers(NewRuleIndex(tmpl.Events).MatchingEventRules(event))
		want := indexHandlers(tmpl.MatchingEventRules(event))
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("indexed=%v full=%v\npattern yaml:\n%s\nevent: %s", got, want, tmplYAML, eventJSON)
		}
	})
}

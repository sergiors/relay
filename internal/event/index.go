package event

import (
	"sort"
	"strconv"
	"strings"

	"relay/internal/app"
)

// This file implements the V1 candidate index: a conservative, false-positive-only
// prefilter over a template's event rules. It never changes matching semantics —
// RuleIndex.MatchingEventRules re-runs the exact matchPattern for every candidate
// — it only avoids exact-testing rules that cannot possibly match.
//
// INVARIANT (false-positive only): if exact matching says a rule matches, the
// index MUST report that rule as a candidate. A candidate that does not match is
// filtered by the exact matcher afterwards; a non-candidate is never exact-tested,
// so a missed candidate would be a silent false negative. Every anchor below is
// therefore a condition that is logically REQUIRED for its rule to match.
//
// ONE ANCHOR PER RULE: a rule is indexed under exactly one anchor (or falls back
// to always-candidate). Selection is deterministic: equality anchors are preferred
// over positive-exists anchors, and within a kind the lexicographically smallest
// normalized field path wins.
//
// INDEXED ALTERNATIVES: only a literal equality and a positive exists, because a
// field's alternatives are OR and only a LONE alternative is a necessary
// condition. A lone positive exists is also necessary (the key must be present).
// Everything else — prefix/suffix/range/temporal, a lone exists:false, an OR mix
// of alternatives, an equality whose literal is non-scalar/composite — does not
// yield a usable anchor at that path. Nested children are ANDed with their
// parent's alternatives, so an eligible child anchor is still necessary even
// when the parent carries an unindexable alternative set.

// anchorKind distinguishes the two necessary-condition shapes the index uses.
type anchorKind uint8

const (
	// anchorEquals requires the event value at the anchor path to equal (with the
	// matcher's type-preserving/numeric-coercing comparison) one of keys.
	anchorEquals anchorKind = iota
	// anchorExists requires the anchor path to be present, whatever the value.
	anchorExists
)

// pathSeparator joins normalized anchor path segments into one index key. It is a
// unit separator, so a path prefix can never be confused with a sibling field
// name ("a" vs "a.b") during prefix pruning.
const pathSeparator = "\x1f"

// anchor is one rule's conservative necessary condition. path is the normalized
// joined field path (top-level or nested); keys is the set of scalar keys an
// equal value must carry (anchorEquals only).
type anchor struct {
	kind anchorKind
	path string
	keys []string
}

// scalarKey returns a type-tagged, collision-free key for a scalar value and
// whether the value is scalar (eligible for an equality anchor). It mirrors
// valuesEqual's semantics exactly: every numeric kind is normalized through
// float64 the way numericEqual does, so two values produce the same key whenever
// the matcher considers them equal. Strings, bools, and nil carry distinct type
// tags, so the string "1", the number 1, the bool true, the string "true", and
// null can never collide. A missing key has no key at all — it is never conflated
// with a present null.
func scalarKey(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "z:", true
	case string:
		// Quote so a string can never collide with another type's tag or with a
		// path separator.
		return "s:" + strconv.Quote(x), true
	case bool:
		if x {
			return "b:1", true
		}
		return "b:0", true
	default:
		f, ok := app.ToFloat(v)
		if !ok {
			return "", false
		}
		if f == 0 {
			// numericEqual treats -0 and +0 as equal, so their keys must agree.
			f = 0
		}
		return "n:" + strconv.FormatFloat(f, 'x', -1, 64), true
	}
}

// anchorLess orders anchors deterministically: equality before exists, then the
// lexicographically smallest normalized path.
func anchorLess(a, b anchor) bool {
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	return a.path < b.path
}

// ruleAnchor returns the single conservative anchor for pattern, if one exists.
// It walks every top-level field and, recursively, every nested child. A
// FieldCondition with exactly one operator contributes an anchor when that
// operator is an equality matcher (with all-scalar operands) or a positive exists
// matcher; children are always walked because they AND with the parent. The
// collected candidates are reduced to one by anchorLess. An empty pattern (a
// match-all rule) yields no anchor and falls back.
func ruleAnchor(pattern app.Pattern) (anchor, bool) {
	var (
		best  anchor
		found bool
	)
	better := func(a anchor) {
		if !found || anchorLess(a, best) {
			best, found = a, true
		}
	}
	for field, cond := range pattern {
		collectAnchors(cond, field, better)
	}
	return best, found
}

// collectAnchors visits cond at path and every descendant, offering each usable
// anchor to better.
func collectAnchors(cond app.FieldCondition, path string, better func(anchor)) {
	// A field's alternatives are OR. A LONE alternative is a necessary condition
	// for the field; so is a set composed ENTIRELY of literal alternatives (the
	// event value must then be one of those literals). Any other mix of
	// alternatives yields no necessary scalar condition at this path.
	if len(cond.Alternatives) == 1 {
		collectAlternativeAnchor(cond.Alternatives[0], path, better)
	} else if len(cond.Alternatives) > 1 {
		if keys, ok := literalAlternativeKeys(cond.Alternatives); ok {
			better(anchor{kind: anchorEquals, path: path, keys: keys})
		}
	}
	// Map-level children are ANDed and therefore always necessary, regardless of
	// how many alternatives the field carries (or whether that set is
	// indexable).
	for field, child := range cond.Children {
		collectAnchors(child, joinPath(path, field), better)
	}
}

// collectAlternativeAnchor offers the anchor (if any) of a single alternative.
// A literal equality yields an equality anchor when its value is scalar; a lone
// positive exists yields a presence anchor; a lone nested group recurses (its
// children are ANDed and so necessary). Every other operator has no necessary
// scalar condition and contributes nothing.
func collectAlternativeAnchor(alt app.Alternative, path string, better func(anchor)) {
	if alt.Literal != nil {
		if key, ok := scalarKey(alt.Literal.Value); ok {
			better(anchor{kind: anchorEquals, path: path, keys: []string{key}})
		}
		return
	}
	if alt.Nested != nil {
		// The lone alternative is a nested group: its children are ANDed and so
		// are necessary for the alternative — and therefore for the field — to
		// hold.
		for field, child := range alt.Nested {
			collectAnchors(child, joinPath(path, field), better)
		}
		return
	}
	if pm, ok := alt.Operator.(presenceMatcher); ok {
		// A positive exists (want=true) is a necessary presence condition; a
		// lone exists:false is not (the key may be absent), so it falls back.
		if pm.MatchPresent(true) {
			better(anchor{kind: anchorExists, path: path})
		}
	}
}

// literalAlternativeKeys returns the typed scalar keys of an all-literal
// alternative set. It reports false if any alternative is an operator or carries
// a non-scalar literal, because then the field does not require the value to be
// one of a scalar set and the whole rule must fall back rather than risk a
// false negative.
func literalAlternativeKeys(alts []app.Alternative) ([]string, bool) {
	if len(alts) == 0 {
		return nil, false
	}
	keys := make([]string, 0, len(alts))
	seen := make(map[string]struct{}, len(alts))
	for _, alt := range alts {
		if alt.Literal == nil {
			return nil, false
		}
		k, ok := scalarKey(alt.Literal.Value)
		if !ok {
			return nil, false
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys, true
}

// joinPath appends field to a normalized path.
func joinPath(prefix, field string) string {
	if prefix == "" {
		return field
	}
	return prefix + pathSeparator + field
}

// anchorPrefixes returns every prefix of a normalized path, including the path
// itself, so candidate extraction can prune event subtrees that contain no anchor.
func anchorPrefixes(path string) []string {
	parts := strings.Split(path, pathSeparator)
	out := make([]string, 0, len(parts))
	for i := range parts {
		out = append(out, strings.Join(parts[:i+1], pathSeparator))
	}
	return out
}

// RuleIndex is an immutable, false-positive-only candidate index over one
// template's event rules (in declaration order). It is built once per published
// app generation and never mutated; a rebuilt generation gets a fresh
// index. It is safe for concurrent use by any number of readers.
//
// The index does not cache match results: MatchingEventRules always re-evaluates
// the exact matcher for every candidate, so temporal (`now()`-relative) rules stay
// dynamic and the exact matcher remains the single authority on whether a rule
// matches.
type RuleIndex struct {
	// events is the exact rule slice this index was built from. It is retained so
	// MatchingEventRules can return the rules themselves, bound to this
	// generation rather than re-resolved by identity.
	events []app.EventRule
	// prefixes holds every anchor path and its proper prefixes, for pruning.
	prefixes map[string]struct{}
	// equals maps an anchor path to a typed-key bucket of rule indices.
	equals map[string]map[string][]int
	// exists maps an anchor path to the rule indices anchored on its presence.
	exists map[string][]int
	// fallback holds rules with no usable anchor; they are always candidates.
	fallback []int
}

// NewRuleIndex builds the candidate index for events. It never mutates events and
// never matches; it only extracts anchors. A nil/empty slice yields an index that
// reports no candidates.
func NewRuleIndex(events []app.EventRule) *RuleIndex {
	ix := &RuleIndex{events: events}
	for i := range events {
		a, ok := ruleAnchor(events[i].Pattern)
		if !ok {
			ix.fallback = append(ix.fallback, i)
			continue
		}
		if ix.prefixes == nil {
			ix.prefixes = make(map[string]struct{}, 1)
		}
		for _, p := range anchorPrefixes(a.path) {
			ix.prefixes[p] = struct{}{}
		}
		switch a.kind {
		case anchorEquals:
			if ix.equals == nil {
				ix.equals = make(map[string]map[string][]int, 1)
			}
			byKey := ix.equals[a.path]
			if byKey == nil {
				byKey = make(map[string][]int, len(a.keys))
				ix.equals[a.path] = byKey
			}
			for _, k := range a.keys {
				byKey[k] = append(byKey[k], i)
			}
		case anchorExists:
			if ix.exists == nil {
				ix.exists = make(map[string][]int, 1)
			}
			ix.exists[a.path] = append(ix.exists[a.path], i)
		}
	}
	return ix
}

// Candidates returns the declaration-ordered indices of every rule that COULD
// match event: rules whose anchor's necessary condition holds, plus every fallback
// rule. It is conservative — it may include rules that do not match — but it never
// omits a rule the exact matcher would match. The result is deterministic and
// deduplicated (a rule indexed under several equality keys still appears once).
func (ix *RuleIndex) Candidates(event map[string]any) []int {
	if ix == nil || len(ix.events) == 0 {
		return nil
	}
	if len(ix.prefixes) == 0 {
		// No rule has an anchor: every rule is a fallback candidate.
		out := make([]int, len(ix.events))
		for i := range out {
			out[i] = i
		}
		return out
	}
	capacity := len(ix.fallback) + 4
	candidates := make([]int, 0, capacity)
	seen := make(map[int]struct{}, capacity)
	add := func(idxs []int) {
		for _, i := range idxs {
			if _, dup := seen[i]; dup {
				continue
			}
			seen[i] = struct{}{}
			candidates = append(candidates, i)
		}
	}
	add(ix.fallback)
	ix.walk(event, "", add)
	sort.Ints(candidates)
	return candidates
}

// MatchingEventRules returns every rule in the index whose pattern matches event,
// in declaration order. It is the indexed equivalent of MatchingEventRules: it
// pre-filters with the anchors, then runs the exact matcher on each candidate, so
// the result is identical to a full scan. It allocates only for matched rules.
func (ix *RuleIndex) MatchingEventRules(event map[string]any) []app.EventRule {
	if ix == nil {
		return nil
	}
	candidates := ix.Candidates(event)
	if len(candidates) == 0 {
		return nil
	}
	var matched []app.EventRule
	for _, i := range candidates {
		r := ix.events[i]
		if matchPattern(r.Pattern, event) {
			matched = append(matched, r)
		}
	}
	return matched
}

// walk visits the event's present fields whose normalized path is anchored (or a
// prefix of an anchored path) and offers each matching posting to add. Pruning on
// the prefix set keeps the cost proportional to the event's anchored subtrees,
// not its full size or the rule count.
func (ix *RuleIndex) walk(obj map[string]any, prefix string, add func([]int)) {
	if obj == nil {
		return
	}
	for field, value := range obj {
		path := joinPath(prefix, field)
		if _, ok := ix.prefixes[path]; !ok {
			continue
		}
		if idxs, ok := ix.exists[path]; ok {
			add(idxs)
		}
		if key, ok := scalarKey(value); ok {
			if byKey, ok := ix.equals[path]; ok {
				if idxs, ok := byKey[key]; ok {
					add(idxs)
				}
			}
		}
		// Nested children are only reachable through a map, exactly as the matcher
		// resolves them; a scalar or array parent means every child is absent.
		if child, ok := value.(map[string]any); ok && len(child) > 0 {
			ix.walk(child, path, add)
		}
	}
}

package app

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// MaxPatternDepth is the fixed, unconfigurable maximum structural depth of one
// event pattern (a single rule's `pattern`). Depth counts field nesting plus
// nested composite-literal containers; the grammar wrappers that add no
// structure — a field's OR-alternative list and a single-key operator map — do
// not consume a level, and siblings/alternatives do not accumulate. Each
// top-level pattern field is level 1, so a pattern whose deepest field is at
// level 8 is accepted and one at level 9 is rejected.
//
// The cap exists so pattern parsing, matching, and candidate-index traversal
// cannot be driven into unbounded recursion by an operator-authored template. It
// is enforced once, at the template parse boundary (validatePatternLimits),
// before the pattern is parsed into the model or indexed; nothing downstream
// re-validates.
const MaxPatternDepth = 8

// MaxPatternSize is the fixed, unconfigurable maximum size of one event pattern,
// measured in Unicode code points (never UTF-8 bytes) of its canonical compact
// JSON serialization. The serialization is encoding/json's compact form with
// object keys sorted: JSON punctuation, quotes, and every escape encoding/json
// emits (including the HTML-sensitive escapes such as `<` -> `\u003c`) all
// count. The cap is per `pattern` — never the whole template, and never the sum
// across a rule's patterns. Non-finite YAML floats (`.nan`/`.inf`), which
// encoding/json cannot represent, are measured as the four-rune literal null;
// their parse semantics are unchanged. A shape with no canonical JSON encoding —
// a mapping with non-string keys, which YAML decodes to map[any]any — is not
// measured at all: it is rejected as a parse error (see validatePatternStructure).
// Like MaxPatternDepth it is enforced once at the parse boundary.
const MaxPatternSize = 4096

// validatePatternLimits enforces the fixed per-pattern structural and size caps
// on a RAW (pre-parse) pattern. It runs before parseFieldCondition, so an
// adversarial pattern is rejected without being materialized into the model, and
// before any candidate index is built. It is value-preserving: every accepted
// pattern parses, matches, and is indexed exactly as before.
//
// Structural validation comes first: the depth cap bounds the nesting the size
// counter must walk, and a shape the size counter cannot serialize is rejected
// before it is measured.
func validatePatternLimits(pattern map[string]any) error {
	if err := validatePatternStructure(pattern); err != nil {
		return err
	}
	if patternRuneSize(pattern) > MaxPatternSize {
		return fmt.Errorf("pattern exceeds max size %d characters", MaxPatternSize)
	}
	return nil
}

// patternFrameKind selects how a stack frame's raw value is interpreted while
// validating pattern structure.
type patternFrameKind uint8

const (
	// patternFieldValue is a field's raw condition value: a top-level pattern
	// field or a nested child field. Its OR-alternative list is a grammar
	// wrapper that does not consume a level.
	patternFieldValue patternFrameKind = iota
	// patternAlternative is one element of a field's OR-alternative list. A
	// nested-map element introduces its child fields at the next level; a list
	// element is a composite literal and consumes a level.
	patternAlternative
	// patternComposite is a composite-literal container (a list or map the
	// parser stores as a Literal value and compares with reflect.DeepEqual).
	// Every container nesting consumes one level, so a deeply nested composite
	// cannot bypass the cap.
	patternComposite
)

// patternFrame is one node of the explicit structure walk.
type patternFrame struct {
	value any
	depth int
	path  string
	kind  patternFrameKind
}

// validatePatternStructure rejects the two structural shapes a raw pattern must
// never reach the parser or the size counter with: a node deeper than
// MaxPatternDepth, and a mapping with non-string keys. It returns an error naming
// the dotted field path of the first offending node.
//
// A non-string-keyed mapping — YAML decodes one to map[any]any — can never match
// a JSON event object, whose keys are always strings, and has no canonical JSON
// encoding. It is rejected with a clear parse error rather than being measured as
// the four-rune null literal, so an arbitrarily large such subtree cannot slip
// past the size cap. A non-finite float is NOT rejected here: it is a leaf the
// size counter measures as null (see addJSON).
//
// The walk uses an explicit stack rather than recursion, so an adversarial
// pattern cannot overflow the Go stack before the cap is enforced.
func validatePatternStructure(pattern map[string]any) error {
	stack := make([]patternFrame, 0, len(pattern))
	for field, value := range pattern {
		stack = append(stack, patternFrame{value: value, depth: 1, path: field, kind: patternFieldValue})
	}
	for len(stack) > 0 {
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if f.depth > MaxPatternDepth {
			return fmt.Errorf("%s: pattern depth exceeds max %d", f.path, MaxPatternDepth)
		}
		if _, ok := f.value.(map[any]any); ok {
			return fmt.Errorf("%s: pattern contains a mapping with non-string keys, which cannot match a JSON object", f.path)
		}
		switch f.kind {
		case patternFieldValue:
			switch v := f.value.(type) {
			case []any:
				stack = pushAlternatives(stack, v, f.depth, f.path)
			case map[string]any:
				if key, operand, ok := operatorEntry(v); ok {
					stack = append(stack, patternFrame{value: operand, depth: f.depth, path: f.path + "." + key, kind: patternComposite})
				} else {
					stack = pushChildren(stack, v, f.depth+1, f.path)
				}
			case nil:
				// A null condition is rejected by the parser; it has no structure.
			}
		case patternAlternative:
			switch v := f.value.(type) {
			case map[string]any:
				if key, operand, ok := operatorEntry(v); ok {
					stack = append(stack, patternFrame{value: operand, depth: f.depth, path: f.path + "." + key, kind: patternComposite})
				} else {
					stack = pushChildren(stack, v, f.depth+1, f.path)
				}
			case []any:
				// A list as an alternative is a composite literal: it consumes one
				// level below the owning field.
				stack = append(stack, patternFrame{value: v, depth: f.depth + 1, path: f.path, kind: patternComposite})
			case nil:
			}
		case patternComposite:
			stack = pushContainers(stack, f.value, f.depth, f.path)
		}
	}
	return nil
}

// pushContainers pushes the composite-container children of v one level deeper
// than v. Scalar leaves consume no depth of their own: only container nesting
// counts, so a composite literal's depth is the number of nested lists/maps it
// contains. A non-string-keyed map child is pushed too, so the next step rejects
// it rather than descending into it.
func pushContainers(stack []patternFrame, v any, depth int, path string) []patternFrame {
	push := func(child any) {
		if isContainer(child) {
			stack = append(stack, patternFrame{value: child, depth: depth + 1, path: path, kind: patternComposite})
		}
	}
	switch x := v.(type) {
	case []any:
		for _, e := range x {
			push(e)
		}
	case map[string]any:
		for _, cv := range x {
			push(cv)
		}
	}
	return stack
}

// isContainer reports whether v is a composite container the structure walk must
// consider: a []any or map[string]any composite is descended into, and a
// non-string-keyed map[any]any is pushed so it is rejected at the next step.
func isContainer(v any) bool {
	switch v.(type) {
	case []any, map[string]any, map[any]any:
		return true
	default:
		return false
	}
}

// pushAlternatives pushes each element of an OR-alternative list at the owning
// field's depth (the list wrapper itself consumes no level).
func pushAlternatives(stack []patternFrame, list []any, depth int, path string) []patternFrame {
	for _, e := range list {
		stack = append(stack, patternFrame{value: e, depth: depth, path: path, kind: patternAlternative})
	}
	return stack
}

// pushChildren pushes each child field of a nested condition map one level
// deeper than the owning field.
func pushChildren(stack []patternFrame, m map[string]any, depth int, path string) []patternFrame {
	for field, value := range m {
		stack = append(stack, patternFrame{value: value, depth: depth, path: path + "." + field, kind: patternFieldValue})
	}
	return stack
}

// operatorEntry reports whether m is a single-key operator wrapper and returns
// the operator key and its operand. Removed operators count as operator-like
// (the parser rejects them) so they are never mistaken for nested children.
func operatorEntry(m map[string]any) (key string, operand any, ok bool) {
	if len(m) != 1 {
		return "", nil, false
	}
	for k, v := range m {
		if known, removed := conditionOperator(k); known || removed {
			return k, v, true
		}
	}
	return "", nil, false
}

// patternRuneSize returns the number of Unicode code points in the canonical
// compact JSON serialization of pattern (see MaxPatternSize). It stops counting
// once the count exceeds MaxPatternSize, so an overlarge or very wide pattern is
// never fully serialized. It must be called only on a pattern that passed
// validatePatternStructure: a non-string-keyed mapping has no JSON encoding and is
// rejected there (and, defensively, counted as over-limit here).
func patternRuneSize(pattern map[string]any) int {
	c := patternSizeCounter{limit: MaxPatternSize}
	c.add(pattern)
	return c.count
}

// patternSizeCounter incrementally counts the runes of a compact JSON encoding,
// aborting once the count exceeds limit.
type patternSizeCounter struct {
	count int
	limit int
	done  bool
}

// add measures one JSON value, descending into objects and arrays. Map keys are
// NOT sorted: the canonical serialization sorts them, but its rune count is the
// sum of the entries' rune counts plus the fixed braces and len-1 commas, which
// is independent of key order. Iterating the map directly therefore yields the
// exact canonical rune count while avoiding materializing and sorting every key
// before the early stop, and the over-limit verdict is likewise order-invariant.
func (c *patternSizeCounter) add(v any) {
	if c.done {
		return
	}
	switch x := v.(type) {
	case map[string]any:
		c.addRunes("{")
		first := true
		for k, val := range x {
			if c.done {
				return
			}
			if !first {
				c.addRunes(",")
			}
			first = false
			c.addJSON(k)
			c.addRunes(":")
			c.add(val)
		}
		c.addRunes("}")
	case []any:
		c.addRunes("[")
		for i, e := range x {
			if c.done {
				return
			}
			if i > 0 {
				c.addRunes(",")
			}
			c.add(e)
		}
		c.addRunes("]")
	case map[any]any:
		// A non-string-keyed mapping has no canonical JSON encoding and is
		// rejected by validatePatternStructure before this runs. Fail closed
		// (over-limit) rather than counting an arbitrarily large subtree as the
		// four-rune null literal, so it can never slip past the size cap.
		c.count = c.limit + 1
		c.done = true
	default:
		c.addJSON(v)
	}
}

// addRunes adds the code-point count of s and trips the early-stop flag once the
// limit is passed.
func (c *patternSizeCounter) addRunes(s string) {
	c.count += utf8.RuneCountInString(s)
	if c.count > c.limit {
		c.done = true
	}
}

// addJSON measures one leaf (non-container) value with encoding/json, matching
// the canonical serialization exactly. The only YAML-decoded leaf encoding/json
// cannot represent is a non-finite float (`.nan`/`.inf`); it is measured as the
// four-rune JSON null literal and its parse semantics are unchanged. Containers
// are handled by add; a non-string-keyed mapping never reaches here.
func (c *patternSizeCounter) addJSON(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		c.addRunes("null")
		return
	}
	c.addRunes(string(b))
}

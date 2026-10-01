package event

import (
	"reflect"

	"relay/internal/app"
)

// MatchingEventRules returns every rule whose pattern matches the event, in
// declaration order. Multiple rules may match; no deduplication is performed.
// A parsed template never contains two rules with the same handler (rejected at
// parse time), so each matched rule is a distinct invocation.
func MatchingEventRules(rules []app.EventRule, event map[string]any) []app.EventRule {
	var matched []app.EventRule
	for i := range rules {
		if matchPattern(rules[i].Pattern, event) {
			matched = append(matched, rules[i])
		}
	}
	return matched
}

// matchPattern reports whether the pattern matches. All top-level fields are
// ANDed.
//
// Instead of failing immediately on a missing top-level field, each field is
// evaluated with its presence flag: a missing key is a "present, but the value
// is nil" condition to every condition. This lets presence-aware operators
// (exists) see absence, while ordinary value operators still fail on a missing
// key (they are not presence-aware, so a non-present key yields no match). The
// short-circuit on the first non-matching field is preserved.
func matchPattern(p app.Pattern, event map[string]any) bool {
	for field, cond := range p {
		value, ok := event[field]
		if !ok {
			value = nil
		}
		if !matchFieldCondition(cond, value, ok) {
			return false
		}
	}
	return true
}

// presenceMatcher is implemented by value matchers that care about the presence
// of a key rather than (or in addition to) its value. MatchPresent receives the
// presence flag only — the matcher resolves entirely from whether the key was
// found. Only the exists operator implements it. Value-dependent operators do
// not, and therefore never match an absent key.
type presenceMatcher interface {
	MatchPresent(present bool) bool
}

// matchFieldCondition evaluates a FieldCondition against a decoded value and its
// presence flag. Alternatives are OR; children are ANDed with each other and with
// the alternatives. Children are only looked up in the parent map when the parent
// is present and actually a map; otherwise every child is evaluated against an
// absent key, so an `exists: false` child can still match when its parent map is
// missing.
func matchFieldCondition(c app.FieldCondition, value any, present bool) bool {
	// Alternatives on the same field are OR.
	if len(c.Alternatives) > 0 {
		matched := false
		for _, alt := range c.Alternatives {
			if matchAlternative(alt, value, present) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Nested children are ANDed.
	if len(c.Children) > 0 {
		if !matchChildren(c.Children, value, present) {
			return false
		}
	}

	return true
}

// matchAlternative evaluates the alternative against a decoded value and the
// field's presence flag. A value matcher (operator or literal) never matches an
// absent key; presence-aware operators (exists) resolve from the presence flag
// alone, and a nested-map alternative resolves its children from the parent
// value.
func matchAlternative(a app.Alternative, value any, present bool) bool {
	if a.Literal != nil {
		if !present {
			return false
		}
		return valuesEqual(value, a.Literal.Value)
	}
	if a.Nested != nil {
		return matchChildren(a.Nested, value, present)
	}
	if a.Operator == nil {
		return false
	}
	if pm, ok := a.Operator.(presenceMatcher); ok {
		return pm.MatchPresent(present)
	}
	if !present {
		return false
	}
	return a.Operator.Match(value)
}

// matchChildren evaluates a child set against a parent value. present reports
// whether the parent key existed; it only matters when the parent is a map (the
// children are then looked up in it), otherwise every child is evaluated as
// absent.
func matchChildren(children map[string]app.FieldCondition, value any, present bool) bool {
	var obj map[string]any
	if present {
		obj, _ = value.(map[string]any)
	}
	for field, child := range children {
		var childValue any
		childPresent := false
		if obj != nil {
			childValue, childPresent = obj[field]
			if !childPresent {
				childValue = nil
			}
		}
		if !matchFieldCondition(child, childValue, childPresent) {
			return false
		}
	}
	return true
}

// valuesEqual compares two decoded values. Numeric kinds are compared
// numerically; all other values with strict equality.
func valuesEqual(a, b any) bool {
	if app.IsNumeric(a) && app.IsNumeric(b) {
		return numericEqual(a, b)
	}
	return reflect.DeepEqual(a, b)
}

// numericEqual compares two numeric values numerically.
func numericEqual(a, b any) bool {
	af, aok := app.ToFloat(a)
	bf, bok := app.ToFloat(b)
	if !aok || !bok {
		return false
	}
	return af == bf
}

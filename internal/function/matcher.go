package function

import (
	"reflect"
)

// MatchingRules returns every rule whose pattern matches the event, in
// declaration order. Multiple rules may match; no deduplication is performed,
// so two matching rules referencing the same handler are both returned.
func (t *Template) MatchingRules(event map[string]any) []Rule {
	var matched []Rule
	for _, r := range t.Rules {
		if r.Pattern.match(event) {
			matched = append(matched, r)
		}
	}
	return matched
}

// match reports whether the pattern matches. All top-level fields are ANDed.
//
// Instead of failing immediately on a missing top-level field, each field is
// evaluated with its presence flag: a missing key is a "present, but the value
// is nil" condition to every condition. This lets presence-aware operators
// (exists) see absence, while ordinary value operators still fail on a missing
// key (they are not presence-aware, so a non-present key yields no match). The
// short-circuit on the first non-matching field is preserved.
func (p Pattern) match(event map[string]any) bool {
	for field, cond := range p {
		value, ok := event[field]
		if !ok {
			value = nil
		}
		if !cond.match(value, ok) {
			return false
		}
	}
	return true
}

// presenceMatcher is implemented by value matchers that care about the presence
// of a key rather than (or in addition to) its value. MatchPresent receives the
// presence flag only — the matcher resolves entirely from whether the key was
// found. Only existsMatcher implements it. Value-dependent operators do not, and
// therefore never match an absent key, exactly as before.
type presenceMatcher interface {
	MatchPresent(present bool) bool
}

// match evaluates a FieldCondition against a decoded value and its presence
// flag. Operators are alternatives (OR); children are ANDed with each other
// and with the operators. A parent that is absent or not a map still evaluates
// its children against (nil, absent) so that presence-oriented child conditions
// (exists: false) can succeed on a missing nested field.
func (c FieldCondition) match(value any, present bool) bool {
	// Operators on the same field are alternatives (OR).
	if len(c.Operators) > 0 {
		matched := false
		for _, op := range c.Operators {
			if pm, ok := op.(presenceMatcher); ok {
				// Presence-aware operators (exists) resolve from the
				// key-presence flag alone.
				if pm.MatchPresent(present) {
					matched = true
					break
				}
				continue
			}
			// A plain value operator never matches an absent key. This preserves
			// the historical behavior where a missing field failed the pattern.
			if !present {
				continue
			}
			if op.Match(value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Nested children are ANDed. They are only looked up in the parent map when
	// the parent is present and actually a map; otherwise every child is
	// evaluated against an absent key, so an `exists: false` child can still
	// match when its parent map is missing.
	if len(c.Children) > 0 {
		var obj map[string]any
		if present {
			obj, _ = value.(map[string]any)
		}
		for field, child := range c.Children {
			var childValue any
			childPresent := false
			if obj != nil {
				childValue, childPresent = obj[field]
				if !childPresent {
					childValue = nil
				}
			}
			if !child.match(childValue, childPresent) {
				return false
			}
		}
	}

	return true
}

// valuesEqual compares two decoded values. Numeric kinds are compared
// numerically; all other values with strict equality.
func valuesEqual(a, b any) bool {
	if isNumeric(a) && isNumeric(b) {
		return numericEqual(a, b)
	}
	return reflect.DeepEqual(a, b)
}

// isNumeric reports whether the value is a numeric kind.
func isNumeric(v any) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
}

// numericEqual compares two numeric values numerically.
func numericEqual(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if !aok || !bok {
		return false
	}
	return af == bf
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

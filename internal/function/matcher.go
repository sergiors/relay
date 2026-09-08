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
func (p Pattern) match(event map[string]any) bool {
	for field, cond := range p {
		value, ok := event[field]
		if !ok {
			return false
		}
		if !cond.match(value) {
			return false
		}
	}
	return true
}

// match evaluates a FieldCondition against a decoded value.
func (c FieldCondition) match(value any) bool {
	// Operators on the same field are alternatives (OR).
	if len(c.Operators) > 0 {
		matched := false
		for _, op := range c.Operators {
			if op.Match(value) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Nested children are ANDed; the value must be a map.
	if len(c.Children) > 0 {
		obj, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for field, child := range c.Children {
			childValue, ok := obj[field]
			if !ok {
				return false
			}
			if !child.match(childValue) {
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

package function

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultTimeout is applied to a rule that omits an explicit timeout.
const DefaultTimeout = 6 * time.Second

// operatorKeys are the only keys treated as operators when they are the only
// keys present in a map; any other key is treated as a nested field.
var operatorKeys = map[string]bool{
	"equals": true,
	"prefix": true,
	"suffix": true,
}

var supportedRuntimes = map[string]bool{
	"python3.14": true,
	"node24":     true,
}

// Template is a parsed template.yaml file: the runtime plus a list of rules,
// each pairing a handler with a pattern invoked when the pattern matches.
type Template struct {
	Runtime string
	Rules   []Rule
}

// Rule pairs a handler (module.function) with a matching pattern and a resolved
// invocation timeout. Timeout is always non-zero after ParseTemplate; omitted
// rules default to DefaultTimeout.
type Rule struct {
	Handler string
	Pattern Pattern
	// Timeout bounds a single invocation of this rule's handler.
	Timeout time.Duration
}

// Pattern maps top-level event fields to their conditions.
type Pattern map[string]FieldCondition

// FieldCondition is how one field is evaluated: value operators are
// alternatives (OR), and child conditions AND with each other and the operators.
type FieldCondition struct {
	Operators []ValueMatcher
	Children  map[string]FieldCondition
}

type ValueMatcher interface {
	Match(value any) bool
}

// equalityMatcher matches values equal to any of the given values. Comparison
// is type-preserving except that numeric kinds are compared numerically.
type equalityMatcher struct {
	values []any
}

func (m equalityMatcher) Match(value any) bool {
	for _, want := range m.values {
		if valuesEqual(value, want) {
			return true
		}
	}
	return false
}

// prefixMatcher matches string values that start with any of the given
// prefixes. Non-string values never match.
type prefixMatcher struct {
	prefixes []string
}

func (m prefixMatcher) Match(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	for _, p := range m.prefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return true
		}
	}
	return false
}

// suffixMatcher matches string values that end with any of the given suffixes.
// Non-string values never match.
type suffixMatcher struct {
	suffixes []string
}

func (m suffixMatcher) Match(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	for _, suf := range m.suffixes {
		if len(s) >= len(suf) && s[len(s)-len(suf):] == suf {
			return true
		}
	}
	return false
}

// ParseTemplate builds a Template from raw YAML, validating the runtime and
// every rule's handler.
func ParseTemplate(data []byte) (*Template, error) {
	var raw struct {
		Runtime string `yaml:"runtime"`
		Events  []struct {
			Handler string         `yaml:"handler"`
			Pattern map[string]any `yaml:"pattern"`
			Timeout string         `yaml:"timeout"`
		} `yaml:"events"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse template yaml: %w", err)
	}

	t := &Template{Runtime: raw.Runtime}

	if t.Runtime == "" {
		return nil, fmt.Errorf("runtime is required")
	}
	if !supportedRuntimes[t.Runtime] {
		return nil, fmt.Errorf("unsupported runtime %q", t.Runtime)
	}
	if len(raw.Events) == 0 {
		return nil, fmt.Errorf("events must contain at least one rule")
	}

	for _, ev := range raw.Events {
		if ev.Handler == "" {
			return nil, fmt.Errorf("rule is missing a handler")
		}
		if ev.Pattern == nil {
			return nil, fmt.Errorf("rule %q is missing a pattern", ev.Handler)
		}
		if err := validateHandler(ev.Handler); err != nil {
			return nil, err
		}
		timeout, err := resolveTimeout(ev.Timeout)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
		}
		pattern := make(Pattern, len(ev.Pattern))
		for field, cond := range ev.Pattern {
			pattern[field] = parseFieldCondition(cond)
		}
		t.Rules = append(t.Rules, Rule{Handler: ev.Handler, Pattern: pattern, Timeout: timeout})
	}
	return t, nil
}

// resolveTimeout parses an optional rule timeout. An empty string yields the
// default; zero, negative, or unparseable values are rejected.
func resolveTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return DefaultTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q", raw)
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout %q must be positive", raw)
	}
	return d, nil
}

// validateHandler requires the form module.function (splitting at the last dot)
// with both parts non-empty and no whitespace.
func validateHandler(handler string) error {
	if handler == "" {
		return fmt.Errorf("handler is empty")
	}
	if strings.ContainsAny(handler, " \t\r\n") {
		return fmt.Errorf("handler %q contains whitespace", handler)
	}
	idx := strings.LastIndex(handler, ".")
	if idx < 0 {
		return fmt.Errorf("handler %q must be of the form module.function", handler)
	}
	module := handler[:idx]
	fn := handler[idx+1:]
	if module == "" {
		return fmt.Errorf("handler %q has an empty module", handler)
	}
	if fn == "" {
		return fmt.Errorf("handler %q has an empty function", handler)
	}
	return nil
}

// parseFieldCondition converts a decoded YAML node into a FieldCondition.
//   - a plain list -> implicit equality (OR across the values)
//   - a map with only operator keys (equals/prefix/suffix) -> operators
//   - a map with other keys -> nested field conditions (AND with siblings)
func parseFieldCondition(v any) FieldCondition {
	switch val := v.(type) {
	case []any:
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: val}}}
	case map[string]any:
		if isOperatorMap(val) {
			return FieldCondition{Operators: parseOperators(val)}
		}
		children := make(map[string]FieldCondition, len(val))
		for field, child := range val {
			children[field] = parseFieldCondition(child)
		}
		return FieldCondition{Children: children}
	default:
		// A bare scalar is treated as implicit equality with a single value.
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: []any{val}}}}
	}
}

// isOperatorMap reports whether the map has only operator keys. If so, it is a
// set of operators rather than nested field conditions.
func isOperatorMap(m map[string]any) bool {
	if len(m) == 0 {
		return false
	}
	for k := range m {
		if !operatorKeys[k] {
			return false
		}
	}
	return true
}

// parseOperators converts a map of operator keys to ValueMatchers.
func parseOperators(m map[string]any) []ValueMatcher {
	var matchers []ValueMatcher
	if vals, ok := m["equals"].([]any); ok {
		matchers = append(matchers, equalityMatcher{values: vals})
	}
	if vals, ok := m["prefix"].([]any); ok {
		matchers = append(matchers, prefixMatcher{prefixes: toStrings(vals)})
	}
	if vals, ok := m["suffix"].([]any); ok {
		matchers = append(matchers, suffixMatcher{suffixes: toStrings(vals)})
	}
	return matchers
}

// toStrings converts decoded YAML values to strings, skipping non-strings.
func toStrings(vals []any) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

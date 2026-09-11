package function

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultTimeout is applied to a rule that omits an explicit timeout.
const DefaultTimeout = 6 * time.Second

// DefaultRetries is the number of additional executions a rule attempts after
// its initial attempt (0 = only the initial attempt). A rule that omits
// `retries` resolves to this value, so by default a failing invocation is
// attempted 1 + DefaultRetries = 5 times in total before it is considered
// exhausted and the message is routed to the DLQ.
const DefaultRetries = 4

// MaxTimeout is the upper bound on any rule's handler timeout. It is the same
// value as stream.MaxRuleTimeout (kept in sync; function is a leaf package and
// stream may import it, not the reverse). The stream layer derives its
// MinPendingIdle reclaim threshold from this cap so a handler that legitimately
// runs up to the cap is never reclaimed mid-flight.
const MaxTimeout = 5 * time.Minute

// operatorKeys are the only keys treated as operators when they are the only
// keys present in a map; any other key is treated as a nested field.
var operatorKeys = map[string]bool{
	"equals": true,
	"prefix": true,
	"suffix": true,
	"exists": true,
}

var supportedRuntimes = map[string]bool{
	"python3.14": true,
	"node24":     true,
}

// SecretRef is a reference to a secret by name. It is a distinct type from a
// plain string so a secret reference can never be confused with a literal env
// value: the two maps (Env and Secrets) are structurally different, and the
// compiler enforces it. String() returns the reference name.
type SecretRef string

// String returns the secret reference name.
func (s SecretRef) String() string { return string(s) }

// EnvVar is one literal environment variable: the env-var name and its literal
// string value. It is returned (name-ordered) by Template.EnvList.
type EnvVar struct {
	Name  string
	Value string
}

// SecretBinding is one secret binding: the env-var name it is exposed as and
// the secret reference it resolves to. It is returned (name-ordered) by
// Template.SecretList.
type SecretBinding struct {
	Name string
	Ref  SecretRef
}

// Template is a parsed template.yaml file: the runtime plus a list of rules,
// each pairing a handler with a pattern invoked when the pattern matches.
type Template struct {
	Runtime string
	Rules   []Rule
	// Env maps an env-var name to a literal string value, injected into every
	// execution container at runtime. It is never baked into the image.
	Env map[string]string
	// Secrets maps an env-var name to a secret reference name, resolved to a
	// value immediately before each execution and injected into the container
	// at runtime. The reference name (never the resolved value) is part of the
	// function's configuration; the value lives outside template.yaml.
	Secrets map[string]SecretRef
}

// Rule pairs a handler (module.function) with a matching pattern and a resolved
// invocation timeout. Timeout is always non-zero after ParseTemplate; omitted
// rules default to DefaultTimeout.
type Rule struct {
	Handler string
	Pattern Pattern
	// Timeout bounds a single invocation of this rule's handler. It is always
	// positive and never exceeds MaxTimeout after ParseTemplate.
	Timeout time.Duration
	// Retries is the number of additional executions attempted after the
	// initial one (0 = only the initial attempt). It is always non-negative
	// after ParseTemplate; omitted rules default to DefaultRetries. The total
	// number of attempts for a failing invocation is 1 + Retries.
	Retries int
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

// existsMatcher matches key presence only. It ignores the value entirely:
// null, false, 0, "", {}, [] all count as existing. The boolean records the
// polarity: true = key present, false = key absent.
//
// The matcher never inspects the value because the dispatcher loop routes it
// through presenceMatcher (MatchPresent), which resolves the polarity from the
// presence flag alone — the value is irrelevant to a presence test. Match here
// is only reachable when the key is present (an absent key never reaches a
// value operator), so returning want outright yields the correct result: a
// present key with want=true matches and want=false does not.
type existsMatcher struct{ want bool }

// Match fulfills ValueMatcher. See the type comment: this is value-independent
// and is only invoked on a present key, so it returns the polarity directly.
func (m existsMatcher) Match(value any) bool { return m.want }

// MatchPresent fulfills presenceMatcher. An absent key with want=false (exists:
// false) matches; an absent key with want=true (exists: true) does not, and a
// present key matches exactly when want is true.
func (m existsMatcher) MatchPresent(present bool) bool { return present == m.want }

// ParseTemplate builds a Template from raw YAML, validating the runtime and
// every rule's handler.
func ParseTemplate(data []byte) (*Template, error) {
	var raw struct {
		Runtime string            `yaml:"runtime"`
		Env     map[string]string `yaml:"env"`
		Secrets map[string]string `yaml:"secrets"`
		Events  []struct {
			Handler string         `yaml:"handler"`
			Pattern map[string]any `yaml:"pattern"`
			Timeout string         `yaml:"timeout"`
			// Retries is decoded as `any` (not `*int`) so a non-integer value
			// (e.g. "abc", "1.5", true) is distinguishable from an omitted one
			// and rejected with a clear message instead of being silently
			// truncated or coerced by yaml.v3.
			Retries any `yaml:"retries"`
		} `yaml:"events"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse template yaml: %w", err)
	}

	t := &Template{Runtime: raw.Runtime}

	// Parse and validate env/secrets. Both are optional; when present, every
	// key must be a valid env-var name, every secret reference a valid secret
	// name, and no variable may be defined in both maps (a duplicate would be
	// ambiguous — which value wins?).
	if err := parseEnvSecrets(t, raw.Env, raw.Secrets); err != nil {
		return nil, err
	}

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
		retries, err := resolveRetries(ev.Retries)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
		}
		pattern := make(Pattern, len(ev.Pattern))
		for field, cond := range ev.Pattern {
			parsed, err := parseFieldCondition(field, cond)
			if err != nil {
				return nil, fmt.Errorf("rule %q: %w", ev.Handler, err)
			}
			pattern[field] = parsed
		}
		t.Rules = append(t.Rules, Rule{Handler: ev.Handler, Pattern: pattern, Timeout: timeout, Retries: retries})
	}
	return t, nil
}

// envVarNamePattern restricts the characters an env-var name may contain. It is
// the POSIX-ish shell variable charset: a letter or underscore first, then
// letters, digits, or underscores. This is deliberately stricter than what the
// OS would accept so a template cannot smuggle a name that a shell or a
// container runtime would interpret differently (e.g. one containing '=' or a
// path separator).
var envVarNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnvVarNames are env-var names Relay owns; a template may never set
// them. RELAY_HANDLER carries the rule's handler identity from the platform —
// letting a template override it would let the template redefine which handler
// the runtime invokes, subverting the per-rule invocation contract.
var reservedEnvVarNames = map[string]bool{"RELAY_HANDLER": true}

// parseEnvSecrets validates and stores the template's env and secrets maps.
// Both are optional. Rules:
//   - env keys must be valid env-var names; values are literal strings (empty
//     values are allowed — flag-like variables are legitimate).
//   - secrets keys must be valid env-var names; the reference (the value) must
//     be a valid secret name and non-empty.
//   - a variable may not be defined in both env and secrets.
//   - Relay-reserved variable names (RELAY_HANDLER) may not be set.
//
// All errors are value-free: they name the offending key/reference but never
// any secret value.
func parseEnvSecrets(t *Template, env, secrets map[string]string) error {
	if len(env) > 0 {
		t.Env = make(map[string]string, len(env))
		for name, value := range env {
			if err := validateEnvVarName(name); err != nil {
				return err
			}
			if reservedEnvVarNames[name] {
				return fmt.Errorf("env var %q is reserved by Relay", name)
			}
			t.Env[name] = value
		}
	}
	if len(secrets) > 0 {
		t.Secrets = make(map[string]SecretRef, len(secrets))
		for name, ref := range secrets {
			if err := validateEnvVarName(name); err != nil {
				return err
			}
			if reservedEnvVarNames[name] {
				return fmt.Errorf("secret variable %q is reserved by Relay", name)
			}
			if err := ValidSecretName(ref); err != nil {
				return fmt.Errorf("secret reference for %q: %w", name, err)
			}
			t.Secrets[name] = SecretRef(ref)
		}
	}
	// A variable defined in both maps is ambiguous: reject it.
	for name := range t.Env {
		if _, dup := t.Secrets[name]; dup {
			return fmt.Errorf("env and secrets define the same variable %q", name)
		}
	}
	return nil
}

// validateEnvVarName reports whether name is a legal env-var name. It rejects
// empty names and anything outside the POSIX-ish charset.
func validateEnvVarName(name string) error {
	if name == "" {
		return fmt.Errorf("env var name is empty")
	}
	if !envVarNamePattern.MatchString(name) {
		return fmt.Errorf("env var name %q is invalid", name)
	}
	return nil
}

// EnvList returns the template's literal env variables as a name-ordered slice,
// so callers iterate deterministically regardless of YAML map ordering.
func (t *Template) EnvList() []EnvVar {
	if len(t.Env) == 0 {
		return nil
	}
	names := make([]string, 0, len(t.Env))
	for name := range t.Env {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]EnvVar, 0, len(names))
	for _, name := range names {
		out = append(out, EnvVar{Name: name, Value: t.Env[name]})
	}
	return out
}

// SecretList returns the template's secret bindings as a name-ordered slice, so
// callers iterate deterministically regardless of YAML map ordering.
func (t *Template) SecretList() []SecretBinding {
	if len(t.Secrets) == 0 {
		return nil
	}
	names := make([]string, 0, len(t.Secrets))
	for name := range t.Secrets {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]SecretBinding, 0, len(names))
	for _, name := range names {
		out = append(out, SecretBinding{Name: name, Ref: t.Secrets[name]})
	}
	return out
}

// resolveTimeout parses an optional rule timeout. An empty string yields the
// default; zero, negative, unparseable, or over-MaxTimeout values are rejected.
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
	if d > MaxTimeout {
		return 0, fmt.Errorf("timeout %q exceeds max %s", raw, MaxTimeout)
	}
	return d, nil
}

// resolveRetries parses an optional rule retry count. A nil value (omitted)
// yields the default; any non-integer value (a string, a float, a bool, ...) or
// a negative integer is rejected. Zero is valid (only the initial attempt).
func resolveRetries(raw any) (int, error) {
	if raw == nil {
		return DefaultRetries, nil
	}
	n, ok := raw.(int)
	if !ok {
		return 0, fmt.Errorf("retries must be a non-negative integer, got %v", raw)
	}
	if n < 0 {
		return 0, fmt.Errorf("retries %d must be non-negative", n)
	}
	return n, nil
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
//   - a map with only operator keys (equals/prefix/suffix/exists) -> operators
//   - a map with other keys -> nested field conditions (AND with siblings)
//
// path is the dotted field path (e.g. "new_image.cnpj") used to give errors from
// strict operator validation (exists) enough context to locate the offending
// value. It must be non-empty for every field; nested fields append their name.
func parseFieldCondition(path string, v any) (FieldCondition, error) {
	switch val := v.(type) {
	case []any:
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: val}}}, nil
	case map[string]any:
		if isOperatorMap(val) {
			return buildOperators(val, path)
		}
		children := make(map[string]FieldCondition, len(val))
		for field, child := range val {
			childCond, err := parseFieldCondition(path+"."+field, child)
			if err != nil {
				return FieldCondition{}, err
			}
			children[field] = childCond
		}
		return FieldCondition{Children: children}, nil
	default:
		// A bare scalar is treated as implicit equality with a single value.
		return FieldCondition{Operators: []ValueMatcher{equalityMatcher{values: []any{val}}}}, nil
	}
}

// buildOperators converts an operator-only map into a FieldCondition of
// operators. equals/prefix/suffix reuse the legacy silent-skip convention: a
// malformed value (e.g. prefix: "x" instead of a list) is dropped, never an
// error. exists is validated strictly instead: its value must be a YAML boolean
// (a Go bool after yaml.v3 decode — YAML true/false), and anything else is
// rejected with a pointing error rather than silently ignored. This asymmetry is
// deliberate: a missing operator key is a legitimate "this operator not used";
// a non-boolean exists value is almost certainly a template authoring mistake.
func buildOperators(m map[string]any, path string) (FieldCondition, error) {
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
	if raw, ok := m["exists"]; ok {
		want, ok := raw.(bool)
		if !ok {
			// yaml.v3 decodes YAML booleans into Go bool, so any non-bool here is
			// a "true", 1, 1.5, null, list, or similar — none is a valid polarity.
			return FieldCondition{}, fmt.Errorf("%s: exists must be a boolean, got %v", path, raw)
		}
		matchers = append(matchers, existsMatcher{want: want})
	}
	return FieldCondition{Operators: matchers}, nil
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

package function

import (
	"strings"
	"testing"
	"time"
)

func TestParseRuntimePython(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Runtime != "python3.14" {
		t.Errorf("expected runtime python3.14, got %q", tmpl.Runtime)
	}
}

func TestParseRuntimeNode(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Runtime != "node24" {
		t.Errorf("expected runtime node24, got %q", tmpl.Runtime)
	}
}

func TestParseUnsupportedRuntimeRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.12
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
	if err == nil {
		t.Fatal("expected error for unsupported runtime")
	}
}

func TestParseMissingRuntimeRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
	if err == nil {
		t.Fatal("expected error for missing runtime")
	}
}

func TestParseRuleMissingHandler(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - pattern:
      status: [COMPLETED]
`))
	if err == nil {
		t.Fatal("expected error for missing handler")
	}
}

func TestParseRuleMissingPattern(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
`))
	if err == nil {
		t.Fatal("expected error for missing pattern")
	}
}

func TestParseEventsEmpty(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events: []
`))
	if err == nil {
		t.Fatal("expected error for empty events")
	}
}

func TestParseHandlerValid(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if len(tmpl.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(tmpl.Rules))
	}
	if tmpl.Rules[0].Handler != "handler.main" {
		t.Errorf("expected handler.main, got %q", tmpl.Rules[0].Handler)
	}
}

func TestParseHandlerNestedModule(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: src.email.send
    pattern:
      status: [COMPLETED]
`)
	if len(tmpl.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(tmpl.Rules))
	}
	if tmpl.Rules[0].Handler != "src.email.send" {
		t.Errorf("expected src.email.send, got %q", tmpl.Rules[0].Handler)
	}
}

func TestParseHandlerInvalid(t *testing.T) {
	cases := []struct {
		name    string
		handler string
	}{
		{"no dot", "handler"},
		{"empty module", ".main"},
		{"empty function", "handler."},
		{"whitespace", "handler main"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: ` + tc.handler + `
    pattern:
      status: [COMPLETED]
`))
			if err == nil {
				t.Fatalf("expected error for handler %q", tc.handler)
			}
		})
	}
}

func TestMatchingRulesExactlyOne(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
  - handler: handler.failed
    pattern:
      status: [FAILED]
`)
	rules := tmpl.MatchingRules(map[string]any{"status": "COMPLETED"})
	if len(rules) != 1 {
		t.Fatalf("expected 1 matching rule, got %d", len(rules))
	}
	if rules[0].Handler != "handler.completed" {
		t.Errorf("expected handler.completed, got %q", rules[0].Handler)
	}
}

func TestMatchingRulesMultiple(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED]
  - handler: handler.b
    pattern:
      status: [COMPLETED]
`)
	rules := tmpl.MatchingRules(map[string]any{"status": "COMPLETED"})
	if len(rules) != 2 {
		t.Fatalf("expected 2 matching rules, got %d", len(rules))
	}
	if rules[0].Handler != "handler.a" || rules[1].Handler != "handler.b" {
		t.Errorf("expected handler.a then handler.b, got %q then %q", rules[0].Handler, rules[1].Handler)
	}
}

func TestMatchingRulesDifferentHandlers(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
  - handler: handler.failed
    pattern:
      status: [FAILED]
`)
	rules := tmpl.MatchingRules(map[string]any{"status": "FAILED"})
	if len(rules) != 1 {
		t.Fatalf("expected 1 matching rule, got %d", len(rules))
	}
	if rules[0].Handler != "handler.failed" {
		t.Errorf("expected handler.failed, got %q", rules[0].Handler)
	}
}

func TestMatchingRulesSameHandler(t *testing.T) {
	// Two matching rules referencing the same handler: both are returned.
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.notify
    pattern:
      status: [COMPLETED]
  - handler: handler.notify
    pattern:
      status: [COMPLETED]
`)
	rules := tmpl.MatchingRules(map[string]any{"status": "COMPLETED"})
	if len(rules) != 2 {
		t.Fatalf("expected 2 matching rules, got %d", len(rules))
	}
	if rules[0].Handler != "handler.notify" || rules[1].Handler != "handler.notify" {
		t.Errorf("expected both to be handler.notify, got %q and %q", rules[0].Handler, rules[1].Handler)
	}
}

func TestMatchingRulesNone(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
`)
	rules := tmpl.MatchingRules(map[string]any{"status": "FAILED"})
	if len(rules) != 0 {
		t.Fatalf("expected 0 matching rules, got %d", len(rules))
	}
}

func TestParseRuleMissingTimeoutDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if got := tmpl.Rules[0].Timeout; got != DefaultTimeout {
		t.Errorf("missing timeout rule = %s, want default %s", got, DefaultTimeout)
	}
}

// TestParseRuleMissingRetriesDefaults pins the default retry count for a rule
// that omits `retries`.
func TestParseRuleMissingRetriesDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if got := tmpl.Rules[0].Retries; got != DefaultRetries {
		t.Errorf("missing retries rule = %d, want default %d", got, DefaultRetries)
	}
}

// TestParseRuleExplicitRetries verifies an explicit non-negative `retries` is
// honored, including zero (only the initial attempt).
func TestParseRuleExplicitRetries(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    retries: 2
`)
	if got := tmpl.Rules[0].Retries; got != 2 {
		t.Errorf("explicit retries = %d, want 2", got)
	}

	zero := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    retries: 0
`)
	if got := zero.Rules[0].Retries; got != 0 {
		t.Errorf("retries: 0 = %d, want 0", got)
	}
}

// TestParseRuleRetriesRejected verifies that a negative or non-integer `retries`
// fails template validation with a clear message. yaml.v3 decodes "1.5" as a
// float and "abc"/"true" as non-integers, so they must be rejected rather than
// silently truncated or coerced.
func TestParseRuleRetriesRejected(t *testing.T) {
	cases := []struct {
		name    string
		retries string
	}{
		{"negative", "-1"},
		{"string", "abc"},
		{"float", "1.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    retries: ` + tc.retries + `
`))
			if err == nil {
				t.Fatalf("expected error for retries %q", tc.retries)
			}
			if !strings.Contains(err.Error(), "retries") {
				t.Errorf("expected error to mention retries, got: %v", err)
			}
		})
	}
}

func TestParseRuleExplicitTimeout(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    timeout: 20s
`)
	got := tmpl.Rules[0].Timeout
	if want := 20 * time.Second; got != want {
		t.Errorf("explicit timeout rule = %s, want %s", got, want)
	}
}

func TestParseRuleTimeoutRejected(t *testing.T) {
	cases := []struct {
		name    string
		timeout string
	}{
		{"zero", "0s"},
		{"negative", "-5s"},
		{"unparseable", "soon"},
		{"above max", "6m"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    timeout: ` + tc.timeout + `
`))
			if err == nil {
				t.Fatalf("expected error for timeout %q", tc.timeout)
			}
		})
	}
}

// TestParseRuleTimeoutMaxBoundary verifies the MaxTimeout cap: exactly MaxTimeout
// parses, while anything above it is rejected with an error mentioning the max.
func TestParseRuleTimeoutMaxBoundary(t *testing.T) {
	// Exactly MaxTimeout is accepted.
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    timeout: 5m
`)
	if got := tmpl.Rules[0].Timeout; got != MaxTimeout {
		t.Errorf("timeout = %s, want MaxTimeout %s", got, MaxTimeout)
	}

	// One second over MaxTimeout is rejected and the error mentions the max.
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
    timeout: 5m1s
`))
	if err == nil {
		t.Fatal("expected error for timeout above MaxTimeout")
	}
	if !strings.Contains(err.Error(), "max") {
		t.Errorf("expected error to mention the max, got: %v", err)
	}
}

// TestParseEnvAndSecrets verifies both maps parse and that the secrets map
// holds SecretRef values (a distinct type from a plain string).
func TestParseEnvAndSecrets(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
env:
  API_URL: https://api.example.com
  FLAG: ""
secrets:
  DATABASE_URL: database-url
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Env["API_URL"] != "https://api.example.com" {
		t.Errorf("env API_URL = %q, want https://api.example.com", tmpl.Env["API_URL"])
	}
	// Empty env values are allowed (flag-like variables).
	if v, ok := tmpl.Env["FLAG"]; !ok || v != "" {
		t.Errorf("env FLAG = %q, ok=%v; want empty value present", v, ok)
	}
	ref, ok := tmpl.Secrets["DATABASE_URL"]
	if !ok {
		t.Fatal("expected secrets DATABASE_URL")
	}
	if ref.String() != "database-url" {
		t.Errorf("secret ref = %q, want database-url", ref.String())
	}
	// The value must be a SecretRef, not a plain string.
	if _, isRef := any(ref).(SecretRef); !isRef {
		t.Errorf("secrets value is %T, want SecretRef", ref)
	}
}

// TestParseEnvSecretsDuplicateRejected verifies a variable defined in both env
// and secrets fails validation.
func TestParseEnvSecretsDuplicateRejected(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
env:
  FOO: bar
secrets:
  FOO: some-secret
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
	if err == nil {
		t.Fatal("expected error for duplicate env/secrets variable")
	}
	if !strings.Contains(err.Error(), "FOO") {
		t.Errorf("expected error to name FOO, got: %v", err)
	}
}

// TestParseReservedEnvVarRejected verifies that Relay-reserved variables
// (RELAY_HANDLER — the platform-owned handler identity) cannot be set by a
// template's env or secrets maps.
func TestParseReservedEnvVarRejected(t *testing.T) {
	for _, tc := range []struct {
		name, block string
	}{
		{"env", "env:\n  RELAY_HANDLER: evil"},
		{"secrets", "secrets:\n  RELAY_HANDLER: some-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
` + tc.block + `
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
			if err == nil {
				t.Fatal("expected error for reserved env var")
			}
			if !strings.Contains(err.Error(), "reserved") {
				t.Errorf("expected error to mention reserved, got: %v", err)
			}
		})
	}
}

// TestParseEnvVarNameInvalid verifies invalid env-var names are rejected.
func TestParseEnvVarNameInvalid(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"leading dash", "-bad"},
		{"leading digit", "1bad"},
		{"space", "A B"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
env:
  ` + tc.key + `: value
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
			if err == nil {
				t.Fatalf("expected error for env var name %q", tc.key)
			}
			if !strings.Contains(err.Error(), "env var name") {
				t.Errorf("expected error to mention env var name, got: %v", err)
			}
		})
	}
}

// TestParseSecretRefInvalid verifies invalid secret references are rejected.
func TestParseSecretRefInvalid(t *testing.T) {
	cases := []struct {
		name string
		ref  string
	}{
		{"path traversal", "../etc"},
		{"absolute", "/abs"},
		{"uppercase with slash", "UPPER-with-slash"},
		{"empty", ""},
		{"trailing dot", "trail."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
secrets:
  TOKEN: ` + tc.ref + `
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`))
			if err == nil {
				t.Fatalf("expected error for secret ref %q", tc.ref)
			}
		})
	}
}

// TestTemplateEnvPairsSorted verifies EnvList and SecretList return
// name-ordered slices regardless of YAML map ordering.
func TestTemplateEnvPairsSorted(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
env:
  ZETA: z
  ALPHA: a
  MID: m
secrets:
  BETA: beta-secret
  ALPHA_SECRET: alpha-secret
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	env := tmpl.EnvList()
	if len(env) != 3 {
		t.Fatalf("env list len = %d, want 3", len(env))
	}
	for i, want := range []string{"ALPHA", "MID", "ZETA"} {
		if env[i].Name != want {
			t.Errorf("env[%d].Name = %q, want %q", i, env[i].Name, want)
		}
	}
	sec := tmpl.SecretList()
	if len(sec) != 2 {
		t.Fatalf("secret list len = %d, want 2", len(sec))
	}
	if sec[0].Name != "ALPHA_SECRET" || sec[1].Name != "BETA" {
		t.Errorf("secret list order = %q,%q; want ALPHA_SECRET,BETA", sec[0].Name, sec[1].Name)
	}
	if sec[0].Ref.String() != "alpha-secret" {
		t.Errorf("secret ref = %q, want alpha-secret", sec[0].Ref.String())
	}
}

func TestMatchingRulesANDSemantics(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
`)
	if len(tmpl.MatchingRules(map[string]any{"event_name": "MODIFY", "table_name": "enrollments"})) != 1 {
		t.Error("expected match when both AND fields present")
	}
	if len(tmpl.MatchingRules(map[string]any{"event_name": "MODIFY", "table_name": "users"})) != 0 {
		t.Error("expected no match when one AND field differs")
	}
}

func TestMatchingRulesORSemantics(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED, FAILED]
`)
	if len(tmpl.MatchingRules(map[string]any{"status": "COMPLETED"})) != 1 {
		t.Error("expected match for COMPLETED via OR")
	}
	if len(tmpl.MatchingRules(map[string]any{"status": "FAILED"})) != 1 {
		t.Error("expected match for FAILED via OR")
	}
	if len(tmpl.MatchingRules(map[string]any{"status": "PENDING"})) != 0 {
		t.Error("expected no match for PENDING")
	}
}

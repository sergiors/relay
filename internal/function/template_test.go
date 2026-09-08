package function

import (
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

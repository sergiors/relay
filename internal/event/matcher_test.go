package event

import (
	"encoding/json"
	"testing"
	"time"

	"relay/internal/app"
)

// mustParse parses a template or fails, so matcher tests can build rules with
// real parsed matchers (the declarative model lives in internal/app).
func mustParse(t *testing.T, yaml string) *app.Template {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(yaml))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

// matches reports whether any of tmpl's event rules matches event.
func matches(t *testing.T, tmpl *app.Template, event map[string]any) bool {
	t.Helper()
	return len(MatchingEventRules(tmpl.Events, event)) > 0
}

// TestMatchingEventRulesExactlyOne pins that a multi-rule template returns only
// the matching rule.
func TestMatchingEventRulesExactlyOne(t *testing.T) {
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
	rules := MatchingEventRules(tmpl.Events, map[string]any{"status": "COMPLETED"})
	if len(rules) != 1 {
		t.Fatalf("expected 1 matching rule, got %d", len(rules))
	}
	if rules[0].Handler != "handler.completed" {
		t.Errorf("expected handler.completed, got %q", rules[0].Handler)
	}
}

// TestMatchingEventRulesMultiple pins that multiple matching rules are returned
// in declaration order.
func TestMatchingEventRulesMultiple(t *testing.T) {
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
	rules := MatchingEventRules(tmpl.Events, map[string]any{"status": "COMPLETED"})
	if len(rules) != 2 {
		t.Fatalf("expected 2 matching rules, got %d", len(rules))
	}
	if rules[0].Handler != "handler.a" || rules[1].Handler != "handler.b" {
		t.Errorf("expected handler.a then handler.b, got %q then %q", rules[0].Handler, rules[1].Handler)
	}
}

// TestMatchingEventRulesDifferentHandlers pins that the matching rule is the one
// whose pattern matches, not the first declared.
func TestMatchingEventRulesDifferentHandlers(t *testing.T) {
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
	rules := MatchingEventRules(tmpl.Events, map[string]any{"status": "FAILED"})
	if len(rules) != 1 {
		t.Fatalf("expected 1 matching rule, got %d", len(rules))
	}
	if rules[0].Handler != "handler.failed" {
		t.Errorf("expected handler.failed, got %q", rules[0].Handler)
	}
}

// TestMatchingEventRulesNone pins that no matching rule yields an empty result.
func TestMatchingEventRulesNone(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      status: [COMPLETED]
`)
	rules := MatchingEventRules(tmpl.Events, map[string]any{"status": "FAILED"})
	if len(rules) != 0 {
		t.Fatalf("expected 0 matching rules, got %d", len(rules))
	}
}

// TestMatchingEventRulesANDSemantics pins the AND across a rule's top-level
// fields.
func TestMatchingEventRulesANDSemantics(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
`)
	if len(MatchingEventRules(tmpl.Events, map[string]any{"event_name": "MODIFY", "table_name": "enrollments"})) != 1 {
		t.Error("expected match when both AND fields present")
	}
	if len(MatchingEventRules(tmpl.Events, map[string]any{"event_name": "MODIFY", "table_name": "users"})) != 0 {
		t.Error("expected no match when one AND field differs")
	}
}

// TestMatchingEventRulesORSemantics pins the OR across a field's alternatives.
func TestMatchingEventRulesORSemantics(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      status: [COMPLETED, FAILED]
`)
	if len(MatchingEventRules(tmpl.Events, map[string]any{"status": "COMPLETED"})) != 1 {
		t.Error("expected match for COMPLETED via OR")
	}
	if len(MatchingEventRules(tmpl.Events, map[string]any{"status": "FAILED"})) != 1 {
		t.Error("expected match for FAILED via OR")
	}
	if len(MatchingEventRules(tmpl.Events, map[string]any{"status": "PENDING"})) != 0 {
		t.Error("expected no match for PENDING")
	}
}

// TestEventMatchingUnaffectedByScheduleAndServiceNames pins that adding schedule
// and service names (and a service sharing a source) does not change event
// matching: events remain keyed by handler and matched purely by pattern. This is
// the event-behavior regression guard for the name-identity change.
func TestEventMatchingUnaffectedByScheduleAndServiceNames(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
schedules:
  - name: cleanup
    handler: events.created.handler
    cron: "0 3 * * *"
  - name: report
    handler: events.created.handler
    cron: "0 4 * * *"
services:
  - name: api
    entrypoint: service.js
  - name: worker
    entrypoint: service.js
`)
	insert := MatchingEventRules(tmpl.Events, map[string]any{"event_name": "INSERT"})
	if len(insert) != 1 || insert[0].Handler != "events.created.handler" {
		t.Fatalf("INSERT matches = %+v, want the created handler", insert)
	}
	modify := MatchingEventRules(tmpl.Events, map[string]any{"event_name": "MODIFY"})
	if len(modify) != 1 || modify[0].Handler != "events.updated.handler" {
		t.Fatalf("MODIFY matches = %+v, want the updated handler", modify)
	}
	if got := MatchingEventRules(tmpl.Events, map[string]any{"event_name": "REMOVE"}); len(got) != 0 {
		t.Fatalf("REMOVE matches = %+v, want none", got)
	}
}

// TestLiteralListEquality pins the primary literal form: a field's condition is
// an ordered list of alternatives, here two bare literals.
func TestLiteralListEquality(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED, IN_PROGRESS]
`)
	if !matches(t, tmpl, map[string]any{"status": "COMPLETED"}) {
		t.Error("expected match")
	}
	if !matches(t, tmpl, map[string]any{"status": "IN_PROGRESS"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"status": "FAILED"}) {
		t.Error("expected no match")
	}
}

func TestMultipleEqualityValuesOR(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED, FAILED]
`)
	if !matches(t, tmpl, map[string]any{"status": "COMPLETED"}) {
		t.Error("expected match for COMPLETED")
	}
	if !matches(t, tmpl, map[string]any{"status": "FAILED"}) {
		t.Error("expected match for FAILED")
	}
	if matches(t, tmpl, map[string]any{"status": "PENDING"}) {
		t.Error("expected no match for PENDING")
	}
}

func TestPrefix(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id: [{prefix: "ENROLLMENT#"}]
`)
	if !matches(t, tmpl, map[string]any{"id": "ENROLLMENT#123"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"id": "OTHER#123"}) {
		t.Error("expected no match")
	}
}

func TestMultiplePrefixes(t *testing.T) {
	// Multiple prefixes are expressed as multiple single-operator alternatives;
	// the list is OR.
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id: [{prefix: "ENROLLMENT#"}, {prefix: "ENR#"}]
`)
	if !matches(t, tmpl, map[string]any{"id": "ENROLLMENT#123"}) {
		t.Error("expected match for ENROLLMENT#")
	}
	if !matches(t, tmpl, map[string]any{"id": "ENR#9"}) {
		t.Error("expected match for ENR#")
	}
	if matches(t, tmpl, map[string]any{"id": "X#9"}) {
		t.Error("expected no match")
	}
}

func TestSuffix(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      email: [{suffix: "@example.com"}]
`)
	if !matches(t, tmpl, map[string]any{"email": "user@example.com"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"email": "user@other.com"}) {
		t.Error("expected no match")
	}
}

func TestMultipleSuffixes(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      email: [{suffix: "@example.com"}, {suffix: "@test.org"}]
`)
	if !matches(t, tmpl, map[string]any{"email": "a@example.com"}) {
		t.Error("expected match for @example.com")
	}
	if !matches(t, tmpl, map[string]any{"email": "a@test.org"}) {
		t.Error("expected match for @test.org")
	}
	if matches(t, tmpl, map[string]any{"email": "a@other.net"}) {
		t.Error("expected no match")
	}
}

// TestLiteralAndOperatorAlternatives pins that a field's list may mix bare
// literals and operator alternatives; they are OR.
func TestLiteralAndOperatorAlternatives(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id: [SPECIAL, {prefix: "ENROLLMENT#"}]
`)
	if !matches(t, tmpl, map[string]any{"id": "SPECIAL"}) {
		t.Error("expected match via literal")
	}
	if !matches(t, tmpl, map[string]any{"id": "ENROLLMENT#123"}) {
		t.Error("expected match via prefix")
	}
	if matches(t, tmpl, map[string]any{"id": "OTHER"}) {
		t.Error("expected no match")
	}
}

func TestNestedObjects(t *testing.T) {
	// A bare map value is shorthand for a single nested condition group.
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        status: [COMPLETED]
`)
	if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"status": "COMPLETED"}}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"new_image": map[string]any{"status": "FAILED"}}) {
		t.Error("expected no match")
	}
}

// TestNestedGroupAsAlternative pins a nested group as one alternative of a
// field's list, ORed with a sibling literal.
func TestNestedGroupAsAlternative(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        - status: [COMPLETED]
        - status: [FAILED]
`)
	if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"status": "COMPLETED"}}) {
		t.Error("expected match via first nested group")
	}
	if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"status": "FAILED"}}) {
		t.Error("expected match via second nested group")
	}
	if matches(t, tmpl, map[string]any{"new_image": map[string]any{"status": "PENDING"}}) {
		t.Error("expected no match")
	}
}

func TestMultipleFieldsAND(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
`)
	if !matches(t, tmpl, map[string]any{"event_name": "MODIFY", "table_name": "enrollments"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"event_name": "MODIFY", "table_name": "users"}) {
		t.Error("expected no match when one field differs")
	}
	if matches(t, tmpl, map[string]any{"event_name": "DELETE", "table_name": "enrollments"}) {
		t.Error("expected no match when one field differs")
	}
}

func TestMultiplePatternsOR(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.a
    pattern:
      event_name: [MODIFY]
  - handler: handler.b
    pattern:
      event_name: [DELETE]
`)
	if !matches(t, tmpl, map[string]any{"event_name": "MODIFY"}) {
		t.Error("expected match for MODIFY")
	}
	if !matches(t, tmpl, map[string]any{"event_name": "DELETE"}) {
		t.Error("expected match for DELETE")
	}
	if matches(t, tmpl, map[string]any{"event_name": "INSERT"}) {
		t.Error("expected no match for INSERT")
	}
}

func TestMissingField(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if matches(t, tmpl, map[string]any{}) {
		t.Error("expected no match when field missing")
	}
}

func TestExtraEventFields(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if !matches(t, tmpl, map[string]any{"status": "COMPLETED", "extra": "ignored", "nested": map[string]any{"a": 1}}) {
		t.Error("expected match despite extra fields")
	}
}

func TestStringValues(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      name: [alice]
`)
	if !matches(t, tmpl, map[string]any{"name": "alice"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"name": "bob"}) {
		t.Error("expected no match")
	}
}

func TestNumericValues(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      score: [70]
`)
	// Event JSON decodes numbers as float64.
	if !matches(t, tmpl, map[string]any{"score": float64(70)}) {
		t.Error("expected match for 70")
	}
	if matches(t, tmpl, map[string]any{"score": float64(92)}) {
		t.Error("expected no match for 92")
	}
	// int comparison also works.
	if !matches(t, tmpl, map[string]any{"score": 70}) {
		t.Error("expected match for int 70")
	}
}

func TestBooleanValues(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      active: [true]
`)
	if !matches(t, tmpl, map[string]any{"active": true}) {
		t.Error("expected match for true")
	}
	if matches(t, tmpl, map[string]any{"active": false}) {
		t.Error("expected no match for false")
	}
}

func TestIncompatibleTypes(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if matches(t, tmpl, map[string]any{"status": 42}) {
		t.Error("expected no match for int vs string")
	}
	if matches(t, tmpl, map[string]any{"status": true}) {
		t.Error("expected no match for bool vs string")
	}
}

func TestPrefixAgainstNonString(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id: [{prefix: "ENROLLMENT#"}]
`)
	if matches(t, tmpl, map[string]any{"id": 123}) {
		t.Error("expected no match for non-string prefix")
	}
	if matches(t, tmpl, map[string]any{"id": true}) {
		t.Error("expected no match for non-string prefix")
	}
}

func TestSuffixAgainstNonString(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      email: [{suffix: "@example.com"}]
`)
	if matches(t, tmpl, map[string]any{"email": 123}) {
		t.Error("expected no match for non-string suffix")
	}
}

// TestRemovedEqualsRejected is covered in internal/app (parse-time validation).

func TestExampleEventMatchesEnrollmentTemplate(t *testing.T) {
	eventJSON := `{
		"event_id": "1757-0",
		"event_name": "MODIFY",
		"table_name": "enrollments",
		"new_image": {
			"id": "ENROLLMENT#123",
			"status": "COMPLETED",
			"email": "user@example.com",
			"score": 92
		}
	}`
	var event map[string]any
	if err := json.Unmarshal([]byte(eventJSON), &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}

	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.completed
    pattern:
      event_name: [MODIFY]
      table_name: [enrollments]
      new_image:
        status: [COMPLETED, FAILED]
        id:
          - prefix: "ENROLLMENT#"
        email:
          - suffix: "@example.com"
`)
	if !matches(t, tmpl, event) {
		t.Error("expected example event to match enrollment template")
	}
}

func TestExistsPresentFields(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [{exists: true}]
`)
	cases := []struct {
		name  string
		value any
	}{
		{"string", "hello"},
		{"number", 42},
		{"float", 1.5},
		{"bool false", false},
		{"bool true", true},
		{"empty string", ""},
		{"empty object", map[string]any{}},
		{"empty array", []any{}},
		{"null", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !matches(t, tmpl, map[string]any{"field": tc.value}) {
				t.Errorf("exists: true should match a present %s field", tc.name)
			}
		})
	}
}

// TestExistsFalsePresentFields is the mirror: exists: false must reject the same
// present fields that exists: true accepted.
func TestExistsFalsePresentFields(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [{exists: false}]
`)
	cases := []struct {
		name  string
		value any
	}{
		{"string", "hello"},
		{"number", 42},
		{"float", 1.5},
		{"bool false", false},
		{"bool true", true},
		{"empty string", ""},
		{"empty object", map[string]any{}},
		{"empty array", []any{}},
		{"null", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if matches(t, tmpl, map[string]any{"field": tc.value}) {
				t.Errorf("exists: false should not match a present %s field", tc.name)
			}
		})
	}
}

func TestExistsMissingField(t *testing.T) {
	trueTmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [{exists: true}]
`)
	if matches(t, trueTmpl, map[string]any{}) {
		t.Error("exists: true should not match a missing field")
	}

	falseTmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: [{exists: false}]
`)
	if !matches(t, falseTmpl, map[string]any{}) {
		t.Error("exists: false should match a missing field")
	}
}

func TestExistsTopLevel(t *testing.T) {
	t.Run("exists true present", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: true}]
`)
		if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"a": 1}}) {
			t.Error("expected match when new_image present")
		}
		if !matches(t, tmpl, map[string]any{"new_image": nil}) {
			t.Error("expected match when new_image present as null")
		}
	})
	t.Run("exists true absent", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: true}]
`)
		if matches(t, tmpl, map[string]any{}) {
			t.Error("expected no match when new_image absent")
		}
	})
	t.Run("exists false absent", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: false}]
`)
		if !matches(t, tmpl, map[string]any{}) {
			t.Error("expected match when new_image absent")
		}
	})
	t.Run("exists false present", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: false}]
`)
		if matches(t, tmpl, map[string]any{"new_image": map[string]any{}}) {
			t.Error("expected no match when new_image present")
		}
	})
}

func TestExistsNested(t *testing.T) {
	t.Run("true matches null child", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: true}]
`)
		// A JSON null key counts as present.
		if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"cnpj": nil}}) {
			t.Error("expected match for cnpj present as null")
		}
	})
	t.Run("true fails wrong child", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: true}]
`)
		// cnpj is absent; another key does not stand in for it.
		if matches(t, tmpl, map[string]any{"new_image": map[string]any{"name": "ACME"}}) {
			t.Error("expected no match when cnpj absent")
		}
	})
	t.Run("true fails missing parent", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: true}]
`)
		if matches(t, tmpl, map[string]any{}) {
			t.Error("expected no match when new_image (the parent) is absent")
		}
	})
	t.Run("false matches wrong child", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: false}]
`)
		// cnpj is absent inside new_image -> exists:false matches.
		if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"name": "ACME"}}) {
			t.Error("expected match when cnpj absent")
		}
	})
	t.Run("false matches missing parent", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: false}]
`)
		// new_image absent -> cnpj is necessarily absent too -> exists:false matches.
		if !matches(t, tmpl, map[string]any{}) {
			t.Error("expected match when parent new_image is absent")
		}
	})
	t.Run("false fails present child", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: false}]
`)
		if matches(t, tmpl, map[string]any{"new_image": map[string]any{"cnpj": 123}}) {
			t.Error("expected no match when cnpj present")
		}
	})
}

func TestExistsDeeplyNested(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      a:
        b:
          c: [{exists: true}]
`)
	if !matches(t, tmpl, map[string]any{"a": map[string]any{"b": map[string]any{"c": "x"}}}) {
		t.Error("expected match for deeply nested present key")
	}
	if matches(t, tmpl, map[string]any{"a": map[string]any{"b": map[string]any{}}}) {
		t.Error("expected no match for deeply nested absent key")
	}
	if matches(t, tmpl, map[string]any{}) {
		t.Error("expected no match for deeply nested with all parents absent")
	}

	falseTmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      a:
        b:
          c: [{exists: false}]
`)
	if !matches(t, falseTmpl, map[string]any{"a": map[string]any{"b": map[string]any{"name": "x"}}}) {
		t.Error("expected match for deeply nested absent key (exists:false)")
	}
	if !matches(t, falseTmpl, map[string]any{}) {
		t.Error("expected match for deeply nested with all parents absent (exists:false)")
	}
	if matches(t, falseTmpl, map[string]any{"a": map[string]any{"b": map[string]any{"c": 1}}}) {
		t.Error("expected no match for deeply nested present key (exists:false)")
	}
}

// TestExistsWithValueOperators pins the OR-combination semantics. Alternatives
// on the same field are OR, so exists:true makes the condition pass for any
// present key regardless of value; exists:false makes it pass only when the key
// is absent OR another alternative matches a present key's value.
func TestExistsWithValueOperators(t *testing.T) {
	t.Run("exists true OR prefix", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: [{exists: true}, {prefix: "12"}]
`)
		if !matches(t, tmpl, map[string]any{"cnpj": "12abc"}) {
			t.Error("expected match: key present with matching prefix")
		}
		// OR implies exists:true alone passes for any present key, whatever the value.
		if !matches(t, tmpl, map[string]any{"cnpj": "xyz"}) {
			t.Error("expected match: key present, exists:true passes via OR")
		}
		if matches(t, tmpl, map[string]any{}) {
			t.Error("expected no match: key absent")
		}
	})
	t.Run("exists false OR prefix", func(t *testing.T) {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: [{exists: false}, {prefix: "12"}]
`)
		// Absent key: exists:false passes.
		if !matches(t, tmpl, map[string]any{}) {
			t.Error("expected match: key absent via exists:false")
		}
		// Present key matching the prefix: the prefix alternative passes (OR).
		if !matches(t, tmpl, map[string]any{"cnpj": "12abc"}) {
			t.Error("expected match: present key with matching prefix passes via OR")
		}
		// Present key NOT matching the prefix: neither alternative passes.
		if matches(t, tmpl, map[string]any{"cnpj": "ab"}) {
			t.Error("expected no match: present key, no matching alternative")
		}
	})
}

// TestExistsAlongsideOtherFields confirms an exists operator participates in the
// AND across top-level fields exactly like any other condition.
func TestExistsAlongsideOtherFields(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
      new_image:
        cnpj: [{exists: true}]
`)
	if !matches(t, tmpl, map[string]any{"event_name": "MODIFY", "new_image": map[string]any{"cnpj": nil}}) {
		t.Error("expected match when both fields satisfy their conditions")
	}
	// event_name present but cnpj missing -> no match.
	if matches(t, tmpl, map[string]any{"event_name": "MODIFY", "new_image": map[string]any{}}) {
		t.Error("expected no match when cnpj missing")
	}
	// event_name wrong -> no match even though cnpj present.
	if matches(t, tmpl, map[string]any{"event_name": "DELETE", "new_image": map[string]any{"cnpj": "1"}}) {
		t.Error("expected no match when event_name differs")
	}
}

// TestOperatorNamesReservedAsNestedFields is covered in internal/app (parse-time
// validation).

// TestExistsJSONDecoded pins the JSON round-trip: json.Unmarshal decodes a null
// value into untyped nil, yet {exists:true} must still match because the key is
// present. This is the same decode path production events take.
func TestExistsJSONDecoded(t *testing.T) {
	trueTmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: true}]
`)
	falseTmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: false}]
`)

	var withNull map[string]any
	if err := json.Unmarshal([]byte(`{"new_image": {"cnpj": null}}`), &withNull); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if !matches(t, trueTmpl, withNull) {
		t.Error("exists:true should match a JSON-decoded null key")
	}
	if matches(t, falseTmpl, withNull) {
		t.Error("exists:false should not match a JSON-decoded present null key")
	}
}

// TestExistsInvalidValues and TestExistsValidParses are covered in internal/app
// (parse-time validation).

// TestValidConditionFormsParseAndMatch pins that every valid condition form
// parses and then matches a representative event.
func TestValidConditionFormsParseAndMatch(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		event     map[string]any
	}{
		{"literal", `["x"]`, map[string]any{"field": "x"}},
		{"prefix", `[{prefix: "x"}]`, map[string]any{"field": "xyz"}},
		{"suffix", `[{suffix: "z"}]`, map[string]any{"field": "xyz"}},
		{"exists true", `[{exists: true}]`, map[string]any{"field": nil}},
		{"exists false", `[{exists: false}]`, map[string]any{}},
		{"gt", `[{gt: 1}]`, map[string]any{"field": 2}},
		{"gte", `[{gte: 2}]`, map[string]any{"field": 2}},
		{"lt", `[{lt: 3}]`, map[string]any{"field": 2}},
		{"lte", `[{lte: 2}]`, map[string]any{"field": 2}},
		{"temporal", `[{gt: "now()-5m"}]`, map[string]any{"field": time.Now().UTC().Format(time.RFC3339)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := app.ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: ` + tc.condition + "\n"))
			if err != nil {
				t.Fatalf("valid condition rejected: %v", err)
			}
			if !matches(t, tmpl, tc.event) {
				t.Fatalf("expected event %v to match", tc.event)
			}
		})
	}
}

// TestMalformedConditionNeverMatches is covered in internal/app (parse-time
// validation).

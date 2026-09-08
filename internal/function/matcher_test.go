package function

import (
	"encoding/json"
	"testing"
)

func mustParse(t *testing.T, yaml string) *Template {
	t.Helper()
	tmpl, err := ParseTemplate([]byte(yaml))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

// matches reports whether any rule in the template matches the event.
func matches(t *testing.T, tmpl *Template, event map[string]any) bool {
	t.Helper()
	return len(tmpl.MatchingRules(event)) > 0
}

func TestImplicitEquality(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if !matches(t, tmpl, map[string]any{"status": "COMPLETED"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"status": "FAILED"}) {
		t.Error("expected no match")
	}
}

func TestExplicitEquals(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status:
        equals: [COMPLETED]
`)
	if !matches(t, tmpl, map[string]any{"status": "COMPLETED"}) {
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
      id:
        prefix: ["ENROLLMENT#"]
`)
	if !matches(t, tmpl, map[string]any{"id": "ENROLLMENT#123"}) {
		t.Error("expected match")
	}
	if matches(t, tmpl, map[string]any{"id": "OTHER#123"}) {
		t.Error("expected no match")
	}
}

func TestMultiplePrefixes(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id:
        prefix: ["ENROLLMENT#", "ENR#"]
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
      email:
        suffix: ["@example.com"]
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
      email:
        suffix: ["@example.com", "@test.org"]
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

func TestEqualsAndPrefixSameField(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      id:
        equals: ["SPECIAL"]
        prefix: ["ENROLLMENT#"]
`)
	if !matches(t, tmpl, map[string]any{"id": "SPECIAL"}) {
		t.Error("expected match via equals")
	}
	if !matches(t, tmpl, map[string]any{"id": "ENROLLMENT#123"}) {
		t.Error("expected match via prefix")
	}
	if matches(t, tmpl, map[string]any{"id": "OTHER"}) {
		t.Error("expected no match")
	}
}

func TestNestedObjects(t *testing.T) {
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
      id:
        prefix: ["ENROLLMENT#"]
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
      email:
        suffix: ["@example.com"]
`)
	if matches(t, tmpl, map[string]any{"email": 123}) {
		t.Error("expected no match for non-string suffix")
	}
}

func TestNestedFieldNamedEquals(t *testing.T) {
	// A nested object that happens to have a field named "equals" alongside
	// other fields must be treated as nested conditions, not operators.
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        equals: [x]
        status: [COMPLETED]
`)
	// "equals" here is a nested field, so the event must have new_image.equals == "x"
	// AND new_image.status == "COMPLETED".
	if !matches(t, tmpl, map[string]any{"new_image": map[string]any{"equals": "x", "status": "COMPLETED"}}) {
		t.Error("expected match for nested equals field")
	}
	if matches(t, tmpl, map[string]any{"new_image": map[string]any{"equals": "y", "status": "COMPLETED"}}) {
		t.Error("expected no match when nested equals differs")
	}
}

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
          prefix: ["ENROLLMENT#"]
        email:
          suffix: ["@example.com"]
`)
	if !matches(t, tmpl, event) {
		t.Error("expected example event to match enrollment template")
	}
}

package app

import (
	"strings"
	"testing"
)

// mustParse mirrors the historical app-test helper: parse a template or fail.
// Event-matcher behavior now lives in internal/event, but app tests across the
// parser, loader, schedules, and services still build templates this way.
func mustParse(t *testing.T, yaml string) *Template {
	t.Helper()
	tmpl, err := ParseTemplate([]byte(yaml))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

// TestRemovedEqualsRejected pins that `equals` is no longer an operator: using
// it anywhere (as the whole field condition or as a list alternative) is a
// parse error with a precise message pointing at the bare-literal replacement.
func TestRemovedEqualsRejected(t *testing.T) {
	templates := []string{
		`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status:
        equals: [COMPLETED]
`,
		`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [{equals: [COMPLETED]}]
`,
	}
	for i, yaml := range templates {
		_, err := ParseTemplate([]byte(yaml))
		if err == nil {
			t.Fatalf("template %d: expected equals to be rejected", i)
		}
		if !strings.Contains(err.Error(), "equals") {
			t.Errorf("template %d: error %q should mention equals", i, err)
		}
	}
}

// TestOperatorNamesReservedAsNestedFields pins that operator names can no longer
// be used as nested field names: an operator inside a nested condition map is
// rejected, because operators belong inside a field's condition list.
func TestOperatorNamesReservedAsNestedFields(t *testing.T) {
	for _, field := range []string{"exists", "prefix", "suffix", "gt", "equals"} {
		yaml := `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        ` + field + `: x
        status: [COMPLETED]
`
		if _, err := ParseTemplate([]byte(yaml)); err == nil {
			t.Errorf("nested field named %q must be rejected", field)
		}
	}
}

// TestExistsInvalidValues pins strict parse-time validation. Non-boolean exists
// values are rejected during ParseTemplate (not at match time) with an error
// naming the field and the offending value.
func TestExistsInvalidValues(t *testing.T) {
	cases := []struct {
		name   string
		yaml   string
		expect string // substring required in the error message
	}{
		{"string true", "{exists: \"true\"}", "boolean"},
		{"int", "{exists: 1}", "boolean"},
		{"float", "{exists: 1.5}", "boolean"},
		{"null", "{exists: null}", "boolean"},
		{"list", "{exists: [true]}", "boolean"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The exists alternative must be an entry of the cnpj field's list.
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: [` + tc.yaml + `]
`))
			if err == nil {
				t.Fatalf("expected parse error for %s exists, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), "exists") {
				t.Errorf("error %q should mention exists", err)
			}
			if !strings.Contains(err.Error(), tc.expect) {
				t.Errorf("error %q should mention %q", err, tc.expect)
			}
			// Strict validation should carry the offending field path so the author
			// can find the mistake.
			if !strings.Contains(err.Error(), "cnpj") {
				t.Errorf("error %q should mention the field path cnpj", err)
			}
		})
	}
}

// TestExistsValidParses confirms that valid boolean exists values at top level,
// nested, and combined with other operators all parse without error.
func TestExistsValidParses(t *testing.T) {
	templates := []string{
		`runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: true}]
`,
		`runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image: [{exists: false}]
`,
		`runtime: python3.14
events:
  - handler: handler.main
    pattern:
      new_image:
        cnpj: [{exists: true}]
`,
		`runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: [{exists: false}, {prefix: "12"}]
`,
	}
	for i, yaml := range templates {
		if _, err := ParseTemplate([]byte(yaml)); err != nil {
			t.Errorf("template %d should parse: %v", i, err)
		}
	}
}

// TestMalformedConditionsRejected pins the invalid shapes of the list grammar:
// operator maps as whole field values, operand lists, empty conditions, null,
// scalar shorthand, and unknown operator-like keys.
func TestMalformedConditionsRejected(t *testing.T) {
	cases := []struct {
		name      string
		condition string
		wantError string
	}{
		{"operator map as field value", `{prefix: "12"}`, "condition operator"},
		{"operator operand list", `[{prefix: ["12"]}]`, "must be a string"},
		{"empty map", `{}`, "condition map is empty"},
		{"empty map as list element", `[{}]`, "condition map is empty"},
		{"empty list", `[]`, "condition list is empty"},
		{"null", `null`, "condition is null"},
		{"scalar shorthand", `"12"`, "condition must be a non-empty list"},
		{"unknown only", `[{equalz: "12"}]`, "unknown condition operator"},
		{"prefix typo", `[{prefx: "12"}]`, "unknown condition operator"},
		{"unknown in nested map", "{prefx: \"12\"}", "unknown condition operator"},
		{"mixed operator and child in list element", `[{prefix: "12", status: [OK]}]`, "condition operator"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      cnpj: ` + tc.condition + "\n"))
			if err == nil {
				t.Fatalf("expected malformed condition to fail parsing")
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantError)
			}
			if !strings.Contains(err.Error(), "cnpj") {
				t.Errorf("error %q should mention the field path cnpj", err)
			}
		})
	}
}

func TestMalformedConditionNeverMatches(t *testing.T) {
	for _, condition := range []string{`[{prefx: "x"}]`, `{}`, `[]`, `null`} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      field: ` + condition + "\n"))
		if err == nil {
			t.Fatalf("condition %q unexpectedly parsed", condition)
		}
	}
}

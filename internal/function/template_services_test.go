package function

import (
	"os"
	"strings"
	"testing"
)

// A template without a `services` key renders a nil Services slice and parses
// exactly as before.
func TestParseNoServicesNil(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	if tmpl.Services != nil {
		t.Fatalf("expected nil Services, got %#v", tmpl.Services)
	}
	if len(tmpl.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(tmpl.Rules))
	}
}

// Omitted port and replicas resolve to their defaults (80 and 1).
func TestParseServiceDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
`)
	if len(tmpl.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "service.js" {
		t.Fatalf("entrypoint = %q", s.Entrypoint)
	}
	if s.Port != DefaultServicePort {
		t.Fatalf("omitted port = %d, want default %d", s.Port, DefaultServicePort)
	}
	if s.Replicas != DefaultServiceReplicas {
		t.Fatalf("omitted replicas = %d, want default %d", s.Replicas, DefaultServiceReplicas)
	}
}

// Explicit port and replicas are honored.
func TestParseServiceExplicitPortAndReplicas(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`)
	if len(tmpl.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Port != 3000 {
		t.Fatalf("port = %d, want 3000", s.Port)
	}
	if s.Replicas != 2 {
		t.Fatalf("replicas = %d, want 2", s.Replicas)
	}
}

// Multiple services with distinct ports resolve independently.
func TestParseMultipleServices(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: api.js
    port: 3000
  - entrypoint: worker.js
    port: 4000
    replicas: 3
`)
	if len(tmpl.Services) != 2 {
		t.Fatalf("expected 2 services, got %d", len(tmpl.Services))
	}
	if tmpl.Services[0].Entrypoint != "api.js" || tmpl.Services[0].Port != 3000 || tmpl.Services[0].Replicas != DefaultServiceReplicas {
		t.Fatalf("service 0 = %+v", tmpl.Services[0])
	}
	if tmpl.Services[1].Entrypoint != "worker.js" || tmpl.Services[1].Port != 4000 || tmpl.Services[1].Replicas != 3 {
		t.Fatalf("service 1 = %+v", tmpl.Services[1])
	}
}

// An invalid (non-integer or out-of-range) port is rejected with a message
// naming the field and the service.
func TestParseServicePortRejected(t *testing.T) {
	cases := []struct {
		name string
		port string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"above max", "70000"},
		{"string", "abc"},
		{"float", "1.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    port: ` + tc.port + `
`))
			if err == nil {
				t.Fatalf("expected error for port %q", tc.port)
			}
			if !strings.Contains(err.Error(), "port") {
				t.Errorf("expected error to mention port, got: %v", err)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
		})
	}
}

// An invalid (non-integer or non-positive) replica count is rejected with a
// message naming the field and the service.
func TestParseServiceReplicasRejected(t *testing.T) {
	cases := []struct {
		name     string
		replicas string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"string", "abc"},
		{"float", "2.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
    replicas: ` + tc.replicas + `
`))
			if err == nil {
				t.Fatalf("expected error for replicas %q", tc.replicas)
			}
			if !strings.Contains(err.Error(), "replicas") {
				t.Errorf("expected error to mention replicas, got: %v", err)
			}
			if !strings.Contains(err.Error(), "service.js") {
				t.Errorf("expected error to name the service, got: %v", err)
			}
		})
	}
}

// A missing entrypoint is rejected; an entrypoint with whitespace is rejected.
func TestParseServiceMissingEntrypoint(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - port: 3000
`))
	if err == nil || !strings.Contains(err.Error(), "service is missing an entrypoint") {
		t.Fatalf("err = %v, want missing-entrypoint error", err)
	}
}

func TestParseServiceEntrypointWhitespace(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: "my service.js"
`))
	if err == nil || !strings.Contains(err.Error(), "contains whitespace") {
		t.Fatalf("err = %v, want whitespace rejection", err)
	}
}

// Duplicate entrypoints within the services list are rejected: the entrypoint
// string is the service identity, and duplicates would be ambiguous for
// reconciliation.
func TestParseServiceDuplicateEntrypoint(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: service.js
  - entrypoint: service.js
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate service entrypoint") {
		t.Fatalf("err = %v, want duplicate-entrypoint rejection", err)
	}
}

// A template with both events AND services parses both correctly.
func TestParseEventsAndServices(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`)
	if len(tmpl.Rules) != 2 {
		t.Fatalf("rules = %d, want 2", len(tmpl.Rules))
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	if tmpl.Services[0].Entrypoint != "service.js" || tmpl.Services[0].Port != 3000 || tmpl.Services[0].Replicas != 2 {
		t.Fatalf("service = %+v", tmpl.Services[0])
	}
}

// A template with services AND schedules co-exists: all of rules, schedules,
// and services parse.
func TestParseServicesAndSchedules(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
schedules:
  - handler: jobs.cleanup.handler
    cron: 0 3 * * *
services:
  - entrypoint: service.js
    port: 8080
`)
	if len(tmpl.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(tmpl.Rules))
	}
	if len(tmpl.Schedules) != 1 || tmpl.Schedules[0].Handler != "jobs.cleanup.handler" {
		t.Fatalf("schedules = %+v", tmpl.Schedules)
	}
	if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != "service.js" || tmpl.Services[0].Port != 8080 {
		t.Fatalf("services = %+v", tmpl.Services)
	}
}

// A port boundary: the minimum (1) and maximum (65535) are accepted.
func TestParseServicePortBoundaries(t *testing.T) {
	tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: min.js
    port: 1
  - entrypoint: max.js
    port: 65535
`)
	if tmpl.Services[0].Port != 1 || tmpl.Services[1].Port != 65535 {
		t.Fatalf("boundary ports = %d, %d; want 1 and 65535", tmpl.Services[0].Port, tmpl.Services[1].Port)
	}
}

// A services-only template (no events key) parses: a persistent service does
// not consume events, so `services:` alone is a complete, valid template.
func TestParseServicesOnlyTemplate(t *testing.T) {
	tmpl, err := ParseTemplate([]byte(`
runtime: node24
services:
  - entrypoint: service.js
    port: 3000
    replicas: 2
`))
	if err != nil {
		t.Fatalf("services-only template must parse: %v", err)
	}
	if len(tmpl.Rules) != 0 {
		t.Fatalf("rules = %d, want 0", len(tmpl.Rules))
	}
	if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != "service.js" ||
		tmpl.Services[0].Port != 3000 || tmpl.Services[0].Replicas != 2 {
		t.Fatalf("services = %+v", tmpl.Services)
	}
}

// A template with neither events nor services is inert and rejected.
func TestParseInertTemplateRejected(t *testing.T) {
	if _, err := ParseTemplate([]byte("runtime: node24\n")); err == nil {
		t.Fatal("expected error for a template with no events and no services")
	}
	if _, err := ParseTemplate([]byte("runtime: node24\nevents: []\nservices: []\n")); err == nil {
		t.Fatal("expected error for explicitly empty events and services")
	}
}

// The shipped users-api-node example parses with its declared values — the
// example is services-only, so this also pins the services-only contract.
func TestExampleUsersAPITemplateParses(t *testing.T) {
	data, err := os.ReadFile("../../examples/functions/users-api-node/template.yaml")
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse example template: %v", err)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "service.js" || s.Port != 3000 || s.Replicas != 1 {
		t.Fatalf("service = %+v, want {service.js 3000 1}", s)
	}
}

// The fastapi-service example is a python3.14 service whose entrypoint is a
// nested package module (app/main.py), demonstrating module execution.
func TestExampleFastAPITemplateParses(t *testing.T) {
	data, err := os.ReadFile("../../examples/functions/fastapi-service/template.yaml")
	if err != nil {
		t.Skipf("example not present: %v", err)
	}
	tmpl, err := ParseTemplate(data)
	if err != nil {
		t.Fatalf("parse example template: %v", err)
	}
	if tmpl.Runtime != "python3.14" {
		t.Fatalf("runtime = %q, want python3.14", tmpl.Runtime)
	}
	if len(tmpl.Services) != 1 {
		t.Fatalf("services = %d, want 1", len(tmpl.Services))
	}
	s := tmpl.Services[0]
	if s.Entrypoint != "app/main.py" || s.Port != 8000 || s.Replicas != 1 {
		t.Fatalf("service = %+v, want {app/main.py 8000 1}", s)
	}
}

// Nested entrypoints (a relative path inside the application directory) parse
// and round-trip into the Service entrypoint.
func TestParseServiceNestedEntrypoint(t *testing.T) {
	for _, ep := range []string{"app/service.js", "app/main.py", "a/b/c.js"} {
		t.Run(ep, func(t *testing.T) {
			tmpl := mustParse(t, `
runtime: node24
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
services:
  - entrypoint: `+ep+`
    port: 3000
`)
			if len(tmpl.Services) != 1 || tmpl.Services[0].Entrypoint != ep {
				t.Fatalf("services = %+v, want entrypoint %q", tmpl.Services, ep)
			}
			if tmpl.Services[0].Port != 3000 {
				t.Fatalf("port = %d, want 3000", tmpl.Services[0].Port)
			}
		})
	}
}

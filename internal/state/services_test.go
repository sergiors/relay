package state

import (
	"testing"
	"time"
)

const servicesTmpl = `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - name: service
    entrypoint: service.js
    host: api.example.com
    path: /v2
  - name: api
    entrypoint: api.js
    port: 3000
    replicas: 3
`

// TestServiceRoundTrip seeds a template with services through
// RecordReconcileSuccess and asserts GetApp returns the resolved rows
// (entrypoint file, canonical path, effective port, desired replicas; defaults
// applied).
func TestServiceRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(detail.Services))
	}
	// Rows are ordered by NAME: api sorts before service.
	s0 := detail.Services[0]
	if s0.Name != "api" || s0.Entrypoint != "api.js" || s0.Port != 3000 || s0.Replicas != 3 || s0.Path != "" {
		t.Fatalf("service 0 = %+v, want api: api.js port=3000 replicas=3 path=\"\"", s0)
	}
	s1 := detail.Services[1]
	if s1.Name != "service" || s1.Entrypoint != "service.js" || s1.Port != 80 || s1.Replicas != 1 || s1.Path != "/v2" {
		t.Fatalf("service 1 = %+v, want service: path=/v2 defaults port=80 replicas=1", s1)
	}
}

// Template change replacing services updates the rows.
func TestServiceReplacementUpdatesRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	// Change: drop one service, change the other's port/replicas/path.
	changed := mustTemplate(t, `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - name: api
    entrypoint: api.js
    host: api.example.com
    path: /v3
    port: 8080
    replicas: 5
`)
	st.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Services) != 1 {
		t.Fatalf("services = %d, want 1 after replacement", len(detail.Services))
	}
	s := detail.Services[0]
	if s.Entrypoint != "api.js" || s.Port != 8080 || s.Replicas != 5 || s.Path != "/v3" {
		t.Fatalf("service after change = %+v, want path=/v3 port=8080 replicas=5", s)
	}
}

// Removal clears the service rows.
func TestServiceRemovalClearsRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	st.RecordRemoved("demo")
	if _, ok := st.GetApp("demo"); ok {
		t.Fatal("function row should be gone after removal")
	}
}

// A template without services stores none.
func TestServiceEmptyStored(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl) // no services key
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Services) != 0 {
		t.Fatalf("services = %d, want 0 for a template without services", len(detail.Services))
	}
}

const sourceServicesTmpl = `runtime: node24
services:
  - name: service
    entrypoint: service.js
    port: 3000
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 9090
`

// stateServiceName returns a persisted service's name (its stable identity).
// The persisted row stores it directly now.
func stateServiceName(s Service) string { return s.Name }

// TestServiceSourceKindsRoundTrip seeds a template whose services use both
// source kinds and asserts each row round-trips its name, source, and key.
func TestServiceSourceKindsRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, sourceServicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(detail.Services))
	}
	// Rows are ordered by name: api sorts before service.
	byName := map[string]Service{}
	for _, s := range detail.Services {
		byName[stateServiceName(s)] = s
	}
	ep, ok := byName["service"]
	if !ok || ep.Entrypoint != "service.js" || ep.Image != "" || ep.Port != 3000 {
		t.Fatalf("entrypoint service = %+v", ep)
	}
	im, ok := byName["api"]
	if !ok || im.Image != "ghcr.io/acme/api:1.2" || im.Entrypoint != "" || im.Port != 9090 {
		t.Fatalf("image service = %+v", im)
	}
}

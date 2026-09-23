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
  - entrypoint: service.js
    host: api.example.com
    path: /v2
  - entrypoint: api.js
    port: 3000
    replicas: 3
`

// TestServiceRoundTrip seeds a template with services through
// RecordReconcileSuccess and asserts GetFunction returns the resolved rows
// (entrypoint file, canonical path, effective port, desired replicas; defaults
// applied).
func TestServiceRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 2 {
		t.Fatalf("services = %d, want 2", len(d.Services))
	}
	// Rows are ordered by entrypoint, so api.js sorts before service.js.
	s0 := d.Services[0]
	if s0.Entrypoint != "api.js" || s0.Port != 3000 || s0.Replicas != 3 || s0.Path != "" {
		t.Fatalf("service 0 = %+v, want api.js port=3000 replicas=3 path=\"\"", s0)
	}
	s1 := d.Services[1]
	if s1.Entrypoint != "service.js" || s1.Port != 80 || s1.Replicas != 1 || s1.Path != "/v2" {
		t.Fatalf("service 1 = %+v, want path=/v2 defaults port=80 replicas=1", s1)
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
  - entrypoint: api.js
    host: api.example.com
    path: /v3
    port: 8080
    replicas: 5
`)
	st.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 1 {
		t.Fatalf("services = %d, want 1 after replacement", len(d.Services))
	}
	s := d.Services[0]
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
	if _, ok := st.GetFunction("demo"); ok {
		t.Fatal("function row should be gone after removal")
	}
}

// A template without services stores none.
func TestServiceEmptyStored(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl) // no services key
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 0 {
		t.Fatalf("services = %d, want 0 for a template without services", len(d.Services))
	}
}

const sourceServicesTmpl = `runtime: node24
services:
  - entrypoint: service.js
    port: 3000
  - build: docker/Dockerfile.prod
    port: 8080
    replicas: 2
  - image: ghcr.io/acme/api:1.2
    port: 9090
`

// stateServiceIdentity derives a persisted service's identity from its source
// fields — whichever of entrypoint/build/image is set — mirroring
// function.Service.SourceRef.
func stateServiceIdentity(s Service) string {
	switch {
	case s.Build != "":
		return s.Build
	case s.Image != "":
		return s.Image
	default:
		return s.Entrypoint
	}
}

// TestServiceSourceKindsRoundTrip seeds a template whose services use all three
// source kinds and asserts each row round-trips its source and keyed identity.
func TestServiceSourceKindsRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, sourceServicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	d, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(d.Services) != 3 {
		t.Fatalf("services = %d, want 3", len(d.Services))
	}
	// Rows are ordered by source.
	byIdentity := map[string]Service{}
	for _, s := range d.Services {
		byIdentity[stateServiceIdentity(s)] = s
	}
	ep, ok := byIdentity["service.js"]
	if !ok || ep.Entrypoint != "service.js" || ep.Build != "" || ep.Image != "" || ep.Port != 3000 {
		t.Fatalf("entrypoint service = %+v", ep)
	}
	bd, ok := byIdentity["docker/Dockerfile.prod"]
	if !ok || bd.Build != "docker/Dockerfile.prod" || bd.Entrypoint != "" || bd.Image != "" || bd.Port != 8080 || bd.Replicas != 2 {
		t.Fatalf("build service = %+v", bd)
	}
	im, ok := byIdentity["ghcr.io/acme/api:1.2"]
	if !ok || im.Image != "ghcr.io/acme/api:1.2" || im.Entrypoint != "" || im.Build != "" || im.Port != 9090 {
		t.Fatalf("image service = %+v", im)
	}
}

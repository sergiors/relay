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
  - handler: service.js
  - handler: api.js
    port: 3000
    replicas: 3
`

// TestServiceRoundTrip seeds a template with services through
// RecordReconcileSuccess and asserts GetFunction returns the resolved rows
// (entrypoint file, effective port, desired replicas; defaults applied).
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
	// Rows are ordered by handler, so api.js sorts before service.js.
	s0 := d.Services[0]
	if s0.Handler != "api.js" || s0.Port != 3000 || s0.Replicas != 3 {
		t.Fatalf("service 0 = %+v, want api.js port=3000 replicas=3", s0)
	}
	s1 := d.Services[1]
	if s1.Handler != "service.js" || s1.Port != 80 || s1.Replicas != 1 {
		t.Fatalf("service 1 = %+v, want defaults port=80 replicas=1", s1)
	}
}

// Template change replacing services updates the rows.
func TestServiceReplacementUpdatesRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, servicesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	// Change: drop one service, change the other's port/replicas.
	changed := mustTemplate(t, `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
services:
  - handler: api.js
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
	if s.Handler != "api.js" || s.Port != 8080 || s.Replicas != 5 {
		t.Fatalf("service after change = %+v", s)
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

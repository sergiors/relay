package state

import (
	"testing"
	"time"
)

const networksTmpl = `runtime: node24
networks:
  - backend
  - frontend
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`

// TestNetworksRoundTrip seeds a template with top-level networks and asserts
// GetFunction returns them normalized (sorted, deduped) in the persisted detail.
func TestNetworksRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, networksTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Networks) != 2 || detail.Networks[0] != "backend" || detail.Networks[1] != "frontend" {
		t.Fatalf("networks = %v, want [backend frontend]", detail.Networks)
	}
}

// A template without networks stores none.
func TestNetworksEmptyStored(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if detail.Networks != nil {
		t.Fatalf("networks = %v, want nil", detail.Networks)
	}
}

// Changing the network set updates the persisted detail.
func TestNetworksReplacementUpdates(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, networksTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	changed := mustTemplate(t, `runtime: node24
networks: [onlynet]
events:
  - handler: index.main
    pattern:
      status: [COMPLETED]
`)
	st.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	detail, ok := st.GetFunction("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Networks) != 1 || detail.Networks[0] != "onlynet" {
		t.Fatalf("networks = %v, want [onlynet]", detail.Networks)
	}
}

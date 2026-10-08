package reconciler

import (
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/routing"
)

// runningSourceIDs returns the relay.source identities of the running containers
// for fn/service, in container-id order.
func runningSourceIDs(f *fakeDocker, fn, service string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for _, c := range f.ctrs {
		if c.appName == fn && c.entrypoint == service && c.state == container.StateRunning {
			ids = append(ids, c.sourceID)
		}
	}
	return ids
}

// TestReconcileReplacesOnSourceMountChange pins the SOURCE_MOUNT reconciliation
// contract: an unchanged source fingerprint keeps the running service (no
// churn), while a changed fingerprint — which never changes the image reference —
// replaces the container via the existing start-before-stop path.
func TestReconcileReplacesOnSourceMountChange(t *testing.T) {
	f := newFakeDocker()
	f.setMountIdentity("service.py", "fp1")
	tmpl := serviceTemplate("python3.14", app.Service{Name: "svc", Entrypoint: "service.py", Port: 8000, Replicas: 1})

	if _, err := reconcile(t, f, "fn", tmpl, "relay-app-fn:tag", routing.TraefikConfig{}); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if got := runningSourceIDs(f, "fn", "svc"); len(got) != 1 || got[0] != "fp1" {
		t.Fatalf("running relay.source = %v, want [fp1]", got)
	}

	// Unchanged source: the converged container is kept, so no further start/stop.
	events := len(f.order())
	if _, err := reconcile(t, f, "fn", tmpl, "relay-app-fn:tag", routing.TraefikConfig{}); err != nil {
		t.Fatalf("unchanged reconcile: %v", err)
	}
	if got := len(f.order()); got != events {
		t.Fatalf("unchanged source mount caused %d container transitions, want none", got-events)
	}

	// Source-only change: same image reference, new fingerprint -> replacement.
	f.setMountIdentity("service.py", "fp2")
	if _, err := reconcile(t, f, "fn", tmpl, "relay-app-fn:tag", routing.TraefikConfig{}); err != nil {
		t.Fatalf("changed reconcile: %v", err)
	}
	if got := len(f.order()); got <= events {
		t.Fatalf("changed source mount caused no replacement (transitions %d -> %d)", events, got)
	}
	if got := runningSourceIDs(f, "fn", "svc"); len(got) != 1 || got[0] != "fp2" {
		t.Fatalf("running relay.source after change = %v, want [fp2]", got)
	}
}

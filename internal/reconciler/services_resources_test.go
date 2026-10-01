package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/routing"
	"relay/internal/testutil"
)

// noRuntimeImageTemplate renders a services-only template that needs no runtime:
// its sole service uses an external `image` source, so Relay never builds or
// prepares an app image for it. resources is the raw YAML resources block
// (empty to omit).
func noRuntimeImageTemplate(resources string) string {
	body := `services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8080
    replicas: 1
`
	if resources != "" {
		body += "resources:\n" + resources
	}
	return body
}

// TestReconcileNoRuntimeExternalImageResourceHotChange pins the resource-only
// hot-change path END TO END for a no-runtime external-image service: a template
// that needs no runtime (its only service uses an `image` source) is discovered
// and converged without any image build, and editing ONLY `resources` must
//   - not move the template-only fingerprint, so no Prepare/rebuild is issued
//     and the external image reference stays unchanged;
//   - publish the new effective limits to the runtime's resource seam; and
//   - replace the running service container with one carrying the new
//     relay.resources fingerprint (the image reference itself is unchanged).
//
// Finally an unchanged third pass must not churn. It reuses the existing service
// reconciler fakeDocker for the container side and the resourceRecordingBuilder
// for the image/resources seam, mirroring production's Manager-implements-both
// wiring.
func TestReconcileNoRuntimeExternalImageResourceHotChange(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ext")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	write := func(tmpl string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
			t.Fatalf("write template: %v", err)
		}
	}
	write(noRuntimeImageTemplate("  memory: 128MiB\n"))

	f := newFakeDocker()
	b := &resourceRecordingBuilder{}
	var svcErrs []error
	svc := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), testReconcileTimeout)
	r, reg := newTestReconciler(t, root, b, nil, func(cfg *Config) {
		cfg.UpdateServices = func(name string, tmpl *app.Template, image string) {
			if err := svc.Apply(context.Background(), name, tmpl, image, nil); err != nil {
				svcErrs = append(svcErrs, err)
			}
		}
	})
	const externalRef = "ghcr.io/acme/api:1.2"
	identity := "api" // the service name (its stable identity); externalRef is its source

	// Discovery: no image is prepared beyond the one the no-runtime app
	// resolves to (the fake's placeholder), and one service container runs on the
	// EXTERNAL image reference.
	r.reconcileApp("ext")
	if b.prepareCount() != 1 {
		t.Fatalf("prepare count = %d, want 1", b.prepareCount())
	}
	if got := reg.GetByName("ext"); got == nil || got.Prepared() == nil {
		t.Fatal("no-runtime function must still be available")
	}
	first := f.lastStartedFor("ext", identity)
	if first == nil {
		t.Fatal("no service container started")
	}
	if first.image != externalRef {
		t.Fatalf("container image = %q, want the external ref %q", first.image, externalRef)
	}
	firstResource := app.ResourceLimits{MemoryBytes: 128 << 20, NanoCPUs: app.DefaultResourceNanoCPUs, PidsLimit: app.DefaultResourcePidsLimit}.Fingerprint()
	if first.resources != firstResource {
		t.Fatalf("relay.resources = %q, want %q", first.resources, firstResource)
	}
	preparesBefore := b.prepareCount()
	preparedImageBefore := reg.GetByName("ext").Prepared().Image
	stopsSoFar := len(f.stops)

	// Resource-only edit: memory 128MiB -> 1GiB. The template-only fingerprint is
	// unchanged, so this lands on the skip path.
	write(noRuntimeImageTemplate("  memory: 1GiB\n"))
	r.reconcileApp("ext")
	if len(svcErrs) != 0 {
		t.Fatalf("service reconcile errors: %v", svcErrs)
	}
	if b.prepareCount() != preparesBefore {
		t.Fatalf("a resource-only change triggered a rebuild: prepares = %d, want %d", b.prepareCount(), preparesBefore)
	}
	// The prepared handle (and therefore any Relay-owned image reference) is
	// unchanged: a no-runtime external-image app has no app image to
	// rebuild, and the edit must not invent one.
	if got := reg.GetByName("ext").Prepared().Image; got != preparedImageBefore {
		t.Fatalf("prepared image changed on a resource-only edit: %q -> %q", preparedImageBefore, got)
	}
	// The old-config container was replaced (stopped), and exactly one new
	// container now runs on the SAME external image with the new fingerprint.
	newResource := app.ResourceLimits{MemoryBytes: 1 << 30, NanoCPUs: app.DefaultResourceNanoCPUs, PidsLimit: app.DefaultResourcePidsLimit}.Fingerprint()
	if newResource == firstResource {
		t.Fatal("test resource configs must differ")
	}
	if len(f.stops) != stopsSoFar+1 || f.stops[stopsSoFar] != first.id {
		t.Fatalf("stops = %v, want the old-config container %q replaced", f.stops, first.id)
	}
	second := f.lastStartedFor("ext", identity)
	if second == nil || second.id == first.id {
		t.Fatal("no replacement container")
	}
	if second.image != externalRef {
		t.Fatalf("replacement image = %q, want the unchanged external ref %q", second.image, externalRef)
	}
	if second.resources != newResource {
		t.Fatalf("replacement relay.resources = %q, want %q", second.resources, newResource)
	}
	if got := f.runningCount("ext", identity); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	// The new limits reached the runtime's resource seam.
	published := b.publishedFor("ext")
	if len(published) == 0 || published[len(published)-1] != (app.ResourceLimits{MemoryBytes: 1 << 30, NanoCPUs: app.DefaultResourceNanoCPUs, PidsLimit: app.DefaultResourcePidsLimit}) {
		t.Fatalf("published resources = %+v, want the 1GiB limits", published)
	}

	// Unchanged third pass: no rebuild, no replacement (no churn).
	stopsAfterChange := len(f.stops)
	preparesAfterChange := b.prepareCount()
	r.reconcileApp("ext")
	if b.prepareCount() != preparesAfterChange {
		t.Fatalf("unchanged pass rebuilt: prepares = %d, want %d", b.prepareCount(), preparesAfterChange)
	}
	if len(f.stops) != stopsAfterChange {
		t.Fatalf("unchanged resources churned the container: stops %d -> %d", stopsAfterChange, len(f.stops))
	}
	if got := f.lastStartedFor("ext", identity); got == nil || got.id != second.id {
		t.Fatalf("unchanged pass replaced the container: %+v", got)
	}
}

// TestReconcileResourceChangeReplacesContainer pins that a resource-only template
// change (same image, same env, same everything else) is detected from the
// relay.resources label and replaces the running service container, for BOTH
// source kinds. It also pins that the replacement carries the new fingerprint.
func TestReconcileResourceChangeReplacesContainer(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec app.Service
	}{
		{name: "entrypoint", spec: app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1}},
		{name: "image", spec: app.Service{Name: "ghcr.io/acme/api:1.2", Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeDocker()
			identity := tc.spec.SourceRef()

			// Template with explicit resources, then without (defaults): the
			// desired fingerprint changes and the running container is replaced.
			withResources := serviceTemplate("node24", tc.spec)
			withResources.Resources = app.ResourceLimits{MemoryBytes: 512 << 20, NanoCPUs: 2_000_000_000, PidsLimit: 64}
			if _, err := reconcile(t, f, "fn", withResources, "img-1", routing.TraefikConfig{}); err != nil {
				t.Fatalf("reconcile 1: %v", err)
			}
			first := f.lastStartedFor("fn", identity)
			if first == nil {
				t.Fatal("no container started")
			}
			if first.resources != withResources.Resources.Fingerprint() {
				t.Fatalf("relay.resources = %q, want %q", first.resources, withResources.Resources.Fingerprint())
			}

			// Change only the resource config (same image, env, port).
			defaulted := serviceTemplate("node24", tc.spec)
			changed, err := reconcile(t, f, "fn", defaulted, "img-1", routing.TraefikConfig{})
			if err != nil {
				t.Fatalf("reconcile 2: %v", err)
			}
			if !changed {
				t.Fatal("a resource-only change must be corrective (changed = true)")
			}
			if len(f.stops) != 1 || f.stops[0] != first.id {
				t.Fatalf("stops = %v, want the old-config container %q replaced", f.stops, first.id)
			}
			second := f.lastStartedFor("fn", identity)
			if second == nil || second.id == first.id {
				t.Fatal("no replacement container")
			}
			if second.resources != app.DefaultResourceLimits().Fingerprint() {
				t.Fatalf("replacement relay.resources = %q, want the default fingerprint", second.resources)
			}
		})
	}
}

// TestReconcileUnchangedResourcesNoChurn pins that an unchanged resource config
// never replaces the container: the periodic reconcile stays a no-op.
func TestReconcileUnchangedResourcesNoChurn(t *testing.T) {
	f := newFakeDocker()
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	tmpl.Resources = app.ResourceLimits{MemoryBytes: 256 << 20, NanoCPUs: 1_000_000_000, PidsLimit: 128}
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile 1: %v", err)
	}
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed || len(f.stops) != stopsSoFar {
		t.Fatalf("an unchanged resource config must keep the container: changed=%v stops %d->%d",
			changed, stopsSoFar, len(f.stops))
	}
}

// TestReconcileLegacyContainerWithoutResourcesReplaced pins the missing-label
// semantics: a running container with no relay.resources is replaced once (like
// a missing relay.env_hash), and is NOT churned again afterwards.
func TestReconcileLegacyContainerWithoutResourcesReplaced(t *testing.T) {
	f := newFakeDocker()
	f.ctrs["legacy-1"] = &fakeContainer{
		id: "legacy-1", appName: "fn", entrypoint: "service.js",
		image: "img-1", port: 80, replica: 0, state: container.StateRunning,
		envHash: serviceEnvHash(80), // env correct, resources label absent
	}
	tmpl := serviceTemplate("node24", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
	if _, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(f.stops) != 1 || f.stops[0] != "legacy-1" {
		t.Fatalf("stops = %v, want [legacy-1] (a missing relay.resources is always stale)", f.stops)
	}
	c := f.lastStartedFor("fn", "service.js")
	if c == nil || c.resources != app.DefaultResourceLimits().Fingerprint() {
		t.Fatalf("replacement = %+v, want a stamped relay.resources", c)
	}

	// A second pass is now converged: no further churn.
	stopsSoFar := len(f.stops)
	changed, err := reconcile(t, f, "fn", tmpl, "img-1", routing.TraefikConfig{})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if changed || len(f.stops) != stopsSoFar {
		t.Fatalf("the replacement must converge: changed=%v stops %d->%d", changed, stopsSoFar, len(f.stops))
	}
	if got := f.runningCount("fn", "service.js"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}

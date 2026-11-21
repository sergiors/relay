package reconciler

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// servicesRecorder records UpdateServices/RemoveServices/RemoveAppResources
// invocations in order so the reconciler's service-convergence hooks can be
// asserted (when they fire, with what template service count / image, and — for
// removal — ordering between the eager resource drop and the asynchronous
// service removal).
type servicesRecorder struct {
	mu      sync.Mutex
	events  []string
	retired map[string]func()
}

// update records an UpdateServices call: "update <name>=<serviceCount>@<image>".
func (s *servicesRecorder) update(name string, tmpl *app.Template, image string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "update "+name+"="+strconv.Itoa(len(tmpl.Services))+"@"+image)
}

// remove records a RemoveServices call; the settlement callback is captured so
// a test can drive the deferred retirement explicitly.
func (s *servicesRecorder) remove(name string, retired func()) {
	s.mu.Lock()
	s.events = append(s.events, "remove "+name)
	if retired != nil {
		if s.retired == nil {
			s.retired = map[string]func(){}
		}
		s.retired[name] = retired
	}
	s.mu.Unlock()
}

// removeResources records an eager RemoveAppResources call.
func (s *servicesRecorder) removeResources(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "remove-resources "+name)
}

// settle drives the captured settlement callback for one app, if any.
func (s *servicesRecorder) settle(name string) bool {
	s.mu.Lock()
	retired := s.retired[name]
	s.mu.Unlock()
	if retired == nil {
		return false
	}
	retired()
	return true
}

func (s *servicesRecorder) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

// withServiceHooks wires UpdateServices, RemoveAppResources, and RemoveServices
// to the given recorder.
func withServiceHooks(rec *servicesRecorder) func(*Config) {
	return func(cfg *Config) {
		cfg.UpdateServices = rec.update
		cfg.RemoveAppResources = rec.removeResources
		cfg.RemoveServices = rec.remove
	}
}

const servicesTemplate = "runtime: nodejs24.x\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\nservices:\n  - name: service\n    entrypoint: service.js\n    port: 3000\n"

// writeServicesDir writes an app directory whose template declares a service.
func writeServicesDir(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(servicesTemplate), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "service.js"), []byte("setInterval(() => {}, 1<<30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}
	return dir
}

// UpdateServices fires on the discovery path (a brand-new app with
// services) and on the update path (a changed app with services), with the
// freshly built image.
func TestReconcileUpdateServicesFiresOnDiscoveryAndUpdate(t *testing.T) {
	root := t.TempDir()
	rec := &servicesRecorder{}

	// Discovery: brand-new app with services declared.
	writeServicesDir(t, root, "svc-new")
	r, _ := newTestReconciler(t, root, &versionedBuilder{version: 0}, nil, withServiceHooks(rec))
	r.reconcileApp("svc-new")
	got := rec.all()
	if len(got) != 1 || got[0] != "update svc-new=1@img-svc-new-v1" {
		t.Fatalf("UpdateServices after discovery = %v, want [update svc-new=1@img-svc-new-v1]", got)
	}

	// Update: change a file so the fingerprint changes -> rebuild & UpdateServices.
	dir := writeServicesDir(t, root, "svc-upd")
	b := &versionedBuilder{version: 1}
	fn := initialServicesFn("svc-upd", dir, "img-svc-upd-v1", b)
	r2, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, withServiceHooks(rec))
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r2.reconcileApp("svc-upd")
	got = rec.all()
	if len(got) != 2 || got[1] != "update svc-upd=1@img-svc-upd-v2" {
		t.Fatalf("UpdateServices after update = %v, want second call [update svc-upd=1@img-svc-upd-v2]", got)
	}
}

// initialServicesFn builds a prepared app whose template declares one
// service, alongside the given image and builder.
func initialServicesFn(name, dir, image string, b Builder) *runner.PreparedApp {
	tmpl := mustParse(servicesTemplate)
	return runner.NewPrepared(
		app.App{Name: name, Dir: dir, Template: tmpl},
		&runtime.Prepared{Name: name, Image: image},
		b,
	)
}

// UpdateServices does NOT fire on the skip path when the app has no
// services, but DOES fire on the skip path when it does (crash-recovery within
// the periodic cadence).
func TestReconcileUpdateServicesFiresOnSkipPathWhenServicesDefined(t *testing.T) {
	root := t.TempDir()
	rec := &servicesRecorder{}

	// (a) No services: skip path must NOT call UpdateServices.
	dirNo := writeFnDir(t, root, "plain")
	fnNo := initialFn("plain", dirNo)
	rd, _ := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{fnNo}, withServiceHooks(rec))
	rd.reconcileApp("plain") // skip (unchanged, no services)
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("UpdateServices fired on skip path for a function WITHOUT services: %v", got)
	}

	// (b) With services: skip path MUST call UpdateServices (crash recovery).
	dirSvc := writeServicesDir(t, root, "svc-stable")
	svcFn := initialServicesFn("svc-stable", dirSvc, "img-svc-stable-v1", &fakeBuilder{})
	rs, _ := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{svcFn}, withServiceHooks(rec))
	rs.reconcileApp("svc-stable") // skip (unchanged but services declared)
	got := rec.all()
	if len(got) != 1 || got[0] != "update svc-stable=1@img-svc-stable-v1" {
		t.Fatalf("UpdateServices on skip-with-services = %v, want [update svc-stable=1@img-svc-stable-v1]", got)
	}
}

// On removal, the eager non-image resource drop fires BEFORE RemoveServices
// starts (and RemoveServices receives the settlement callback that defers image
// retirement), so stale schedules/metrics cannot outlive the pump pass while
// images stay governed by the removal settlement.
func TestReconcileRemoveAppResourcesFireBeforeRemoveServices(t *testing.T) {
	root := t.TempDir()
	dir := writeServicesDir(t, root, "svc-gone")

	svcFn := initialServicesFn("svc-gone", dir, "img-svc-gone-v1", &fakeBuilder{})
	rec := &servicesRecorder{}
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{svcFn}, withServiceHooks(rec))

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	r.reconcileApp("svc-gone")

	got := rec.all()
	if len(got) != 2 || got[0] != "remove-resources svc-gone" || got[1] != "remove svc-gone" {
		t.Fatalf("removal order = %v, want [remove-resources svc-gone, remove svc-gone]",
			got)
	}
	if !rec.settle("svc-gone") {
		t.Fatal("RemoveServices did not receive a settlement callback")
	}
}

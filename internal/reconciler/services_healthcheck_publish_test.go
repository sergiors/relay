package reconciler

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
)

const healthcheckPublishTemplate = `runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
services:
  - name: service
    entrypoint: service.js
    port: 3000
    healthcheck:
      path: /health
`

// editedHealthcheckTemplate is byte-identical to
// healthcheckPublishTemplate except for the probe block: the exact edit shape
// whose content fingerprint must NOT change (runtime-only service
// configuration) while the published service configuration MUST.
const editedHealthcheckTemplate = `runtime: nodejs24.x
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
services:
  - name: service
    entrypoint: service.js
    port: 3000
    healthcheck:
      path: /ready
      interval: 20s
`

// publishRecord captures one UpdateServices publication: the image it named
// and the effective service configuration the coordinator would converge.
type publishRecord struct {
	image    string
	path     string
	interval time.Duration
	replicas int
}

// publishRecorder records every UpdateServices publication in order.
type publishRecorder struct {
	mu    sync.Mutex
	calls []publishRecord
}

func (p *publishRecorder) record(_ string, tmpl *app.Template, image string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	call := publishRecord{image: image, replicas: len(tmpl.Services)}
	if len(tmpl.Services) > 0 && tmpl.Services[0].Healthcheck != nil {
		call.path = tmpl.Services[0].Healthcheck.Path
		call.interval = tmpl.Services[0].Healthcheck.Interval
	}
	p.calls = append(p.calls, call)
}

func (p *publishRecorder) all() []publishRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]publishRecord(nil), p.calls...)
}

// writeTemplateAppDir writes an app directory whose template is the given
// raw template.yaml plus the source files it references.
func writeTemplateAppDir(t *testing.T, root, name, raw string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(raw), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"),
		[]byte("export function hi(e, context){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "service.js"),
		[]byte("setInterval(() => {}, 1<<30);\n"), 0o644); err != nil {
		t.Fatalf("write service: %v", err)
	}
	return dir
}

// A healthcheck-only template edit is runtime-only service configuration: the
// image fingerprint deliberately does not change, so no rebuild happens — but
// the reconciler's skip path must still PUBLISH the updated configuration
// (the new probe) through UpdateServices with the unchanged image, so the
// service reconciler can converge the running containers to it.
func TestReconcilePublishesHealthcheckOnlyEditWithoutRebuild(t *testing.T) {
	root := t.TempDir()
	dir := writeTemplateAppDir(t, root, "svc-hc", healthcheckPublishTemplate)

	builder := &versionedBuilder{}
	initial := runner.NewPrepared(
		app.App{Name: "svc-hc", Dir: dir, Template: mustParse(healthcheckPublishTemplate)},
		&runtime.Prepared{Name: "svc-hc", Image: "img-svc-hc-v1"},
		builder,
	)
	rec := &publishRecorder{}
	r, _ := newTestReconciler(t, root, builder, []*runner.PreparedApp{initial},
		func(cfg *Config) { cfg.UpdateServices = rec.record })

	// Baseline pass publishes the configured probe once.
	r.reconcileApp("svc-hc")
	calls := rec.all()
	if len(calls) != 1 {
		t.Fatalf("publications after the baseline pass = %d, want 1: %+v", len(calls), calls)
	}
	if calls[0].path != "/health" || calls[0].interval != app.DefaultServiceHealthcheckInterval {
		t.Fatalf("baseline publication = %+v, want path /health at the default interval", calls[0])
	}
	if calls[0].image != "img-svc-hc-v1" {
		t.Fatalf("baseline image = %q, want the prepared img-svc-hc-v1", calls[0].image)
	}

	// Edit ONLY the healthcheck block on disk.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
		[]byte(editedHealthcheckTemplate), 0o644); err != nil {
		t.Fatalf("write edited template: %v", err)
	}
	r.reconcileApp("svc-hc")

	calls = rec.all()
	if len(calls) != 2 {
		t.Fatalf("publications after the edit = %+v, want exactly one more", calls)
	}
	edited := calls[1]
	if edited.path != "/ready" || edited.interval != 20*time.Second {
		t.Fatalf("published probe = path %q interval %s, want the edited /ready at 20s",
			edited.path, edited.interval)
	}
	if edited.replicas != 1 {
		t.Fatalf("published services = %d, want 1", edited.replicas)
	}
	if edited.image != calls[0].image {
		t.Fatalf("published image = %q, want the unchanged %q (no rebuild)",
			edited.image, calls[0].image)
	}
	builder.mu.Lock()
	builds := builder.version
	builder.mu.Unlock()
	if builds != 0 {
		t.Fatalf("Prepare calls = %d, want 0 (a healthcheck-only edit must not rebuild)", builds)
	}
}

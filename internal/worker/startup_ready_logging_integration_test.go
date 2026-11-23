//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// startupReadyDocker converges both service source kinds without a daemon: an
// external `image` service resolves to its own reference, an entrypoint service
// resolves through the runtime entrypoint table, and every start returns its
// own container id.
type startupReadyDocker struct {
	svcDeadlineDocker
	mu  sync.Mutex
	seq int
}

func (d *startupReadyDocker) ResolveServiceImage(
	_ context.Context, _ string, tmpl *app.Template, svc app.Service, appImage string,
) (runtime.ServiceImage, error) {
	if svc.Source() == app.ServiceSourceImage {
		return runtime.ServiceImage{Ref: svc.SourceRef()}, nil
	}
	entry, err := runtime.ServiceEntry(tmpl.Runtime, svc.Entrypoint)
	if err != nil {
		return runtime.ServiceImage{}, err
	}
	return runtime.ServiceImage{Ref: appImage, Entry: entry}, nil
}

func (d *startupReadyDocker) StartService(_ context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	return fmt.Sprintf("startup-svc-%d", d.seq), nil
}

// Both templates declare two services running five replicas, so a readiness
// line reporting the replica total instead of the declared service count is
// distinguishable from the correct value.
const startupManagedServicesTemplate = `runtime: nodejs24.x
events:
  - handler: index.handler
    pattern:
      event_name: [INSERT]
services:
  - name: web
    entrypoint: service.js
    port: 8080
    replicas: 2
  - name: jobs
    entrypoint: jobs.js
    port: 8081
    replicas: 3
`

const startupExternalServicesTemplate = `services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8080
    replicas: 2
  - name: worker
    image: ghcr.io/acme/worker:2.0
    port: 9090
    replicas: 3
`

func startupReadyApp(t *testing.T, name, templateText string) app.App {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(templateText))
	if err != nil {
		t.Fatalf("parse %s template: %v", name, err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(templateText), 0o644); err != nil {
		t.Fatalf("write %s template: %v", name, err)
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

// startupReadyAttrs returns the raw attribute map of the single "App: ready"
// record captured for the named app, failing when it is absent or duplicated.
func startupReadyAttrs(t *testing.T, logs *testutil.SyncBuffer, name string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("captured log line is not JSON: %q: %v", line, err)
		}
		if record["msg"] == "App: ready" && record["app"] == name {
			found = append(found, record)
		}
	}
	if len(found) != 1 {
		t.Fatalf("App: ready records for %s = %d, want exactly one: %+v", name, len(found), found)
	}
	return found[0]
}

// TestStartupReadyLogCarriesImageOnlyWhenTheGenerationHasOne pins the startup
// readiness line's image rule against the persisted path: a managed generation
// reports the image it serves, a no-runtime (external-image) generation omits
// the attribute rather than logging an empty one, and both report the DECLARED
// service count rather than the replica total.
func TestStartupReadyLogCarriesImageOnlyWhenTheGenerationHasOne(t *testing.T) {
	st := testutil.NewState(t)
	managed := startupReadyApp(t, "startup-managed", startupManagedServicesTemplate)
	external := startupReadyApp(t, "startup-external", startupExternalServicesTemplate)
	tokens := map[string]state.FencingToken{
		managed.Name:  seedFencingToken(t, st, managed),
		external.Name: seedFencingToken(t, st, external),
	}
	logger, logs := captureStartupLog()

	svcCtrl := reconciler.NewServiceReconciler(
		&startupReadyDocker{}, nil, logger, reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, svcCtrl)
	defer stop()

	prepared := []*runner.PreparedApp{
		runner.NewPrepared(managed, &runtime.Prepared{
			Name: managed.Name, Image: "relay-app-startup-managed:v1",
			Fingerprint: "fp-startup-managed",
		}, nil),
		runner.NewPrepared(external, &runtime.Prepared{
			Name: external.Name, Fingerprint: "fp-startup-external",
		}, nil),
	}
	enqueueStartupServices(
		context.Background(), prepared, coordinator, st, tokens, logger,
	)

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	managedReady := startupReadyAttrs(t, logs, managed.Name)
	if managedReady["image"] != "relay-app-startup-managed:v1" {
		t.Errorf("managed ready attrs = %+v, want the served image", managedReady)
	}
	if managedReady["fingerprint"] != "fp-startup-managed" || managedReady["outcome"] != "ready" {
		t.Errorf("managed ready attrs = %+v, want its fingerprint and outcome=ready", managedReady)
	}
	if managedReady["services"] != float64(2) {
		t.Errorf("managed ready services = %v, want the 2 declared services (5 replicas)", managedReady["services"])
	}

	externalReady := startupReadyAttrs(t, logs, external.Name)
	if value, ok := externalReady["image"]; ok {
		t.Errorf("no-runtime ready carries image=%v; the attribute must be omitted entirely", value)
	}
	if externalReady["fingerprint"] != "fp-startup-external" || externalReady["outcome"] != "ready" {
		t.Errorf("no-runtime ready attrs = %+v, want its fingerprint and outcome=ready", externalReady)
	}
	if externalReady["services"] != float64(2) {
		t.Errorf("no-runtime ready services = %v, want the 2 declared services (5 replicas)", externalReady["services"])
	}
}

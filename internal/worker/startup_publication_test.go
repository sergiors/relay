package worker

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/state"
)

type startupPublicationFake struct {
	current       map[string]string
	published     []string
	refreshes     int
	callbackCalls int
}

func (f *startupPublicationFake) PublishPrepared(_ context.Context, prepared *runtime.Prepared, publish func() error) error {
	if prepared.ImageSelection != f.current[prepared.Runtime] {
		return runtime.ErrRuntimeImageSelectionStale
	}
	f.published = append(f.published, prepared.Name)
	f.callbackCalls++
	return publish()
}

func (f *startupPublicationFake) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	f.refreshes++
	return nil, nil
}

func (f *startupPublicationFake) RuntimeImageSelectionIdentity(runtimeID string) string {
	return f.current[runtimeID]
}

type startupPublicationStateFake struct {
	successes []string
	failures  []string
}

func (f *startupPublicationStateFake) RecordReconcileSuccess(
	_ context.Context, name, _, _ string, _ time.Time, _ app.App, _ state.FencingToken,
) (bool, error) {
	f.successes = append(f.successes, name)
	return true, nil
}

func (f *startupPublicationStateFake) RecordReconcileFailure(
	_ context.Context, name string, _ error, _ state.FencingToken,
) (bool, error) {
	f.failures = append(f.failures, name)
	return true, nil
}

func startupTestApp(t *testing.T, name string, withServices bool) app.App {
	t.Helper()
	text := "runtime: nodejs24.x\nevents:\n  - handler: index.handler\n    pattern:\n      event_name: [INSERT]\n"
	tmpl, err := app.ParseTemplate([]byte(text))
	if err != nil {
		t.Fatalf("parse startup template: %v", err)
	}
	if withServices {
		tmpl.Services = []app.Service{{Entrypoint: "service.js", Port: 8080, Replicas: 1}}
	}
	return app.App{Name: name, Dir: t.TempDir(), Template: tmpl}
}

func startupPreparedCandidate(fn app.App, image, selection string) *runner.PreparedApp {
	return runner.NewPrepared(fn, &runtime.Prepared{
		Name: fn.Name, Image: image, Fingerprint: "fp-" + fn.Name,
		Runtime: fn.Template.Runtime, ImageSelection: selection,
	}, nil)
}

func TestPublishStartupAppsRejectsStaleAndKeepsOnlyAcceptedGenerations(t *testing.T) {
	selectionA := "node:24-alpine@sha256:old"
	selectionB := "node:24-alpine@sha256:new"
	accepted := startupPreparedCandidate(startupTestApp(t, "accepted", false), "relay-app-accepted:v1", selectionB)
	acceptedService := startupPreparedCandidate(startupTestApp(t, "accepted-service", true), "relay-app-service:v1", selectionB)
	stale := startupPreparedCandidate(startupTestApp(t, "stale-service", true), "relay-app-stale:v1", selectionA)
	registry := &runner.Registry{}
	publisher := &startupPublicationFake{current: map[string]string{"nodejs24.x": selectionB}}
	stateSink := &startupPublicationStateFake{}
	startup := startupAppSet{fencingTokens: map[string]state.FencingToken{
		"accepted": 1, "accepted-service": 2, "stale-service": 3,
	}}

	published, retry := publishStartupApps(
		context.Background(), []*runner.PreparedApp{accepted, acceptedService, stale}, registry, publisher, stateSink, startup,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if got, want := publisher.published, []string{"accepted", "accepted-service"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("accepted publication callbacks = %v, want %v", got, want)
	}
	if publisher.callbackCalls != 2 || publisher.refreshes != 1 {
		t.Fatalf("publication callbacks/refreshes = %d/%d, want 2/1", publisher.callbackCalls, publisher.refreshes)
	}
	if got, want := retry, []string{"stale-service"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry names = %v, want %v", got, want)
	}
	if len(published) != 3 {
		t.Fatalf("published registry view has %d apps, want 2 accepted plus stale unavailable placeholder", len(published))
	}
	if registry.GetByName("accepted") != accepted || registry.GetByName("accepted-service") != acceptedService {
		t.Fatal("accepted generations were not installed in the registry")
	}
	placeholder := registry.GetByName("stale-service")
	if placeholder == nil || placeholder.Prepared() != nil {
		t.Fatalf("stale app registry entry = %+v, want an unavailable placeholder", placeholder)
	}
	if placeholder.App().Template == nil || len(placeholder.App().Template.Events) == 0 {
		t.Fatal("stale placeholder lost its desired event rules, so matching would be dropped")
	}

	if got, want := stateSink.successes, []string{"accepted"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("startup success writes = %v, want only accepted no-service app %v", got, want)
	}
	if got, want := stateSink.failures, []string{"stale-service"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("startup failure writes = %v, want stale app %v", got, want)
	}

	seeds := startupSeedImageSelections(published)
	if seeds["accepted"] != selectionB || seeds["accepted-service"] != selectionB || seeds["stale-service"] != "" {
		t.Fatalf("startup selection seeds = %v, want accepted pins and empty stale identity", seeds)
	}
	images := startupPreparedImages(published)
	if got, want := images, []string{"relay-app-accepted:v1", "relay-app-service:v1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("startup prepared-image keep input = %v, want only accepted images %v", got, want)
	}
	keep := startupImageKeepSet(images, nil, nil)
	if keep["relay-app-stale:v1"] || len(keep) != 2 {
		t.Fatalf("startup keep-set retained a stale image: %v", keep)
	}

	// The same published slice feeds startup service enqueue. The stale app has
	// service declarations but its unavailable placeholder must not enqueue a
	// service request; the accepted service app is the only one with a service.
	docker := &svcDeadlineDocker{}
	serviceReconciler := reconciler.NewServiceReconciler(docker, nil, routing.TraefikConfig{}, discardLogger(), reconcileTimeout)
	coordinator, stop := startCoordinatorFixture(t, serviceReconciler)
	defer stop()
	enqueueStartupServices(published, coordinator, discardLogger())
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait for accepted startup service: %v", err)
	}
	if got := len(docker.recordedDeadlines()); got != 2 {
		// The accepted no-service app also publishes a remove/verify request; the
		// stale placeholder contributes none. A stale request would make this 3.
		t.Fatalf("service reconciliation operations = %d, want 2 accepted requests and no stale request", got)
	}
}

func TestStartupSeedImageSelectionsUsesPublishedHandlesOnly(t *testing.T) {
	accepted := startupPreparedCandidate(startupTestApp(t, "accepted-seed", false), "img-accepted", "node:24-alpine@sha256:abc")
	noRuntimeApp := app.App{Name: "no-runtime", Template: &app.Template{}}
	noRuntime := runner.NewPrepared(noRuntimeApp, &runtime.Prepared{Name: noRuntimeApp.Name, Image: "", Fingerprint: "template-fp"}, nil)
	unavailable := runner.NewUnavailable(startupTestApp(t, "unavailable-seed", false))

	got := startupSeedImageSelections([]*runner.PreparedApp{accepted, noRuntime, unavailable})
	want := map[string]string{
		"accepted-seed":    "node:24-alpine@sha256:abc",
		"no-runtime":       "",
		"unavailable-seed": "",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("startup selection seeds = %v, want %v", got, want)
	}
}

var _ startupSelectionPublisher = (*startupPublicationFake)(nil)
var _ startupPublicationState = (*startupPublicationStateFake)(nil)

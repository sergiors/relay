package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

type deploymentLogRecord struct {
	Msg   string         `json:"msg"`
	Level string         `json:"level"`
	Attrs map[string]any `json:"-"`
	raw   map[string]any
}

func deploymentLogRecords(t *testing.T, capture *testutil.SyncBuffer) []deploymentLogRecord {
	t.Helper()
	var records []deploymentLogRecord
	for _, line := range strings.Split(capture.String(), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("invalid structured log line %q: %v", line, err)
		}
		records = append(records, deploymentLogRecord{
			Msg:   stringValue(raw["msg"]),
			Level: stringValue(raw["level"]),
			Attrs: raw,
			raw:   raw,
		})
	}
	return records
}

func stringValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func deploymentRecord(records []deploymentLogRecord, msg string) *deploymentLogRecord {
	for i := range records {
		if records[i].Msg == msg {
			return &records[i]
		}
	}
	return nil
}

// requireNoAppImageAttrs fails when a record carries an app-level image
// identity at all — empty or not. A generation whose services bring their own
// images has no app image to correlate, and an empty value would read as a real
// (broken) identity rather than as "not applicable".
func requireNoAppImageAttrs(t *testing.T, msg string, raw map[string]any) {
	t.Helper()
	for _, key := range []string{"image", "active_image", "previous_image"} {
		if value, ok := raw[key]; ok {
			t.Errorf("%s carries %s=%v; omit the attribute entirely",
				msg, key, value)
		}
	}
}

func deploymentLogHarness(
	t *testing.T,
	root string,
	initial []*runner.PreparedApp,
	builder Builder,
	hook func(*Config),
) (*Reconciler, *testutil.SyncBuffer) {
	t.Helper()
	var logs testutil.SyncBuffer
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour}
	if hook != nil {
		hook(&cfg)
	}
	r := New(cfg, reg, builder, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	for _, prepared := range initial {
		fn := prepared.App()
		fp, err := app.FingerprintApp(fn.Dir, fn.Template)
		if err != nil {
			t.Fatalf("fingerprint initial app: %v", err)
		}
		r.Seed(fn, fp)
	}
	return r, &logs
}

func writeLoggedApp(t *testing.T, root, name, templateText string) (string, *app.Template) {
	t.Helper()
	dir := writeFnDir(t, root, name)
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(templateText), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(templateText))
	if err != nil {
		t.Fatalf("parse template fixture: %v", err)
	}
	return dir, tmpl
}

func TestApplicationPreparationLogsOnlyForDesiredChanges(t *testing.T) {
	root := t.TempDir()
	dir, tmpl := writeLoggedApp(t, root, "logging-change", template)
	fn := app.App{Name: "logging-change", Dir: dir, Template: tmpl}
	initial := runner.NewPrepared(fn, &runtime.Prepared{Name: fn.Name, Image: "img-before"}, &fakeBuilder{})
	r, logs := deploymentLogHarness(t, root, []*runner.PreparedApp{initial}, &fakeBuilder{}, nil)

	r.reconcileApp(fn.Name)
	records := deploymentLogRecords(t, logs)
	if deploymentRecord(records, "App: preparing") != nil {
		t.Fatal("unchanged verification emitted App: preparing")
	}
	if unchanged := deploymentRecord(records, "App: unchanged; reconcile skipped"); unchanged == nil || unchanged.Level != "DEBUG" {
		t.Fatalf("unchanged verification log = %+v, want DEBUG App: unchanged; reconcile skipped", unchanged)
	}

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ return 'changed'; }\n"), 0o644); err != nil {
		t.Fatalf("change app source: %v", err)
	}
	r.reconcileApp(fn.Name)
	records = deploymentLogRecords(t, logs)
	preparing := deploymentRecord(records, "App: preparing")
	if preparing == nil || preparing.Level != "INFO" {
		t.Fatalf("changed desired generation preparing log = %+v, want INFO", preparing)
	}
	if deploymentRecord(records, "App: image published") == nil || deploymentRecord(records, "App: ready") == nil {
		t.Fatalf("changed desired generation must publish and become ready; records: %+v", records)
	}
}

func TestImagePublicationIsDistinctFromStatusCapableReadiness(t *testing.T) {
	for _, tc := range []struct {
		name     string
		template string
	}{
		{name: "with services", template: servicesTemplate},
		{name: "empty desired services", template: template},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeLoggedApp(t, root, "async-ready", tc.template)
			var completion func(error)
			var hookCalls int
			r, logs := deploymentLogHarness(t, root, nil, &fakeBuilder{}, func(cfg *Config) {
				cfg.UpdateServicesWithStatus = func(_ string, _ *app.Template, _ string, _ func(), onComplete func(error)) {
					hookCalls++
					completion = onComplete
				}
			})

			r.reconcileApp("async-ready")
			records := deploymentLogRecords(t, logs)
			if deploymentRecord(records, "App: image published") == nil {
				t.Fatal("image publication was not logged")
			}
			if deploymentRecord(records, "App: ready") != nil {
				t.Fatal("deployment became ready before service completion")
			}
			if hookCalls != 1 || completion == nil {
				t.Fatalf("status-capable service hook calls = %d, completion set = %t; want one callback even for empty desired services", hookCalls, completion != nil)
			}

			completion(nil)
			records = deploymentLogRecords(t, logs)
			if ready := deploymentRecord(records, "App: ready"); ready == nil || ready.Level != "INFO" {
				t.Fatalf("completion ready log = %+v, want INFO after callback", ready)
			}
		})
	}
}

func TestServiceFailureLogsDeploymentOutcomeWithoutReady(t *testing.T) {
	for _, withPrior := range []bool{true, false} {
		name := "without prior generation"
		if withPrior {
			name = "with active prior generation"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir, tmpl := writeLoggedApp(t, root, "service-failure", servicesTemplate)
			fn := app.App{Name: "service-failure", Dir: dir, Template: tmpl}
			var initial []*runner.PreparedApp
			if withPrior {
				initial = []*runner.PreparedApp{
					runner.NewPrepared(fn, &runtime.Prepared{Name: fn.Name, Image: "img-prior"}, &fakeBuilder{}),
				}
			}
			var completion func(error)
			r, logs := deploymentLogHarness(t, root, initial, &fakeBuilder{}, func(cfg *Config) {
				cfg.UpdateServicesWithStatus = func(_ string, _ *app.Template, _ string, _ func(), onComplete func(error)) {
					completion = onComplete
				}
			})
			if withPrior {
				if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e, context){ return 'v2'; }\n"), 0o644); err != nil {
					t.Fatalf("change app source: %v", err)
				}
			}

			r.reconcileApp(fn.Name)
			if completion == nil {
				t.Fatal("service completion callback was not captured")
			}
			completion(errors.New("service convergence failed"))
			records := deploymentLogRecords(t, logs)
			if deploymentRecord(records, "App: image published") == nil {
				t.Fatal("successful image preparation was not published before service failure")
			}
			if deploymentRecord(records, "App: ready") != nil {
				t.Fatal("service failure must never log ready")
			}
			wantMessage, wantOutcome, wantLevel := "App: deployment failed", "failed", "ERROR"
			if withPrior {
				wantMessage, wantOutcome, wantLevel = "App: deployment degraded", "degraded", "WARN"
			}
			failure := deploymentRecord(records, wantMessage)
			if failure == nil || failure.Level != wantLevel || failure.raw["outcome"] != wantOutcome {
				t.Fatalf("deployment failure record = %+v, want %s level/outcome %s/%s", failure, wantLevel, wantOutcome, wantMessage)
			}
		})
	}
}

func TestCancelledServiceCompletionLogsCancelledDeployment(t *testing.T) {
	root := t.TempDir()
	writeLoggedApp(t, root, "cancelled-service", servicesTemplate)
	var completion func(error)
	r, logs := deploymentLogHarness(t, root, nil, &fakeBuilder{}, func(cfg *Config) {
		cfg.UpdateServicesWithStatus = func(_ string, _ *app.Template, _ string, _ func(), onComplete func(error)) {
			completion = onComplete
		}
	})
	r.reconcileApp("cancelled-service")
	if completion == nil {
		t.Fatal("service completion callback was not captured")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx = ctx
	cancel()
	completion(context.Canceled)

	records := deploymentLogRecords(t, logs)
	cancelled := deploymentRecord(records, "App: deployment cancelled")
	if cancelled == nil || cancelled.Level != "DEBUG" || cancelled.raw["outcome"] != "cancelled" {
		t.Fatalf("cancelled deployment record = %+v, want DEBUG outcome=cancelled", cancelled)
	}
	if deploymentRecord(records, "App: deployment failed") != nil ||
		deploymentRecord(records, "App: deployment degraded") != nil {
		t.Fatalf("lifecycle cancellation emitted a failure outcome: %+v", records)
	}
}

type cancelledResolveDocker struct{ *fakeDocker }

func (d cancelledResolveDocker) ResolveServiceImage(
	ctx context.Context,
	_ string,
	_ *app.Template,
	_ app.Service,
	_ string,
) (runtime.ServiceImage, error) {
	return runtime.ServiceImage{}, ctx.Err()
}

type partialStopCancelDocker struct {
	*fakeDocker
	cancel context.CancelFunc
}

func (d partialStopCancelDocker) StopServiceContainersWithResult(
	context.Context,
	[]runtime.ServiceContainer,
) (int, error) {
	d.cancel()
	return 1, context.Canceled
}

func TestServiceLoggingDistinguishesCorrectiveWorkNoOpAndErrors(t *testing.T) {
	t.Run("corrective summary keeps transitions, drops image", func(t *testing.T) {
		docker := newFakeDocker()
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(docker, nil, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if result := services.ApplyResult(context.Background(), "fn", tmpl, "img-current", nil, nil); result.Err != nil {
			t.Fatalf("apply: %v", result.Err)
		}
		records := deploymentLogRecords(t, &logs)
		start := deploymentRecord(records, "Service: reconciling")
		if start == nil || start.Level != "INFO" {
			t.Fatalf("reconciling record = %+v, want INFO", start)
		}
		requireNoAppImageAttrs(t, "Service: reconciling", start.raw)
		if start.raw["services"] != float64(1) || start.raw["outcome"] != "started" {
			t.Errorf("reconciling attrs = %+v, want services=1 outcome=started",
				start.raw)
		}
		summary := deploymentRecord(records, "Service: reconciled")
		if summary == nil || summary.Level != "INFO" {
			t.Fatalf("changed summary = %+v, want INFO", summary)
		}
		requireNoAppImageAttrs(t, "Service: reconciled", summary.raw)
		if summary.raw["services"] != float64(1) || summary.raw["replicas"] != float64(1) {
			t.Errorf("summary scope = services:%v replicas:%v, want 1/1",
				summary.raw["services"], summary.raw["replicas"])
		}
		if summary.raw["started"] != float64(1) || summary.raw["restarted"] != float64(0) || summary.raw["stopped"] != float64(0) {
			t.Errorf("committed transition counts = started:%v restarted:%v stopped:%v, want 1/0/0",
				summary.raw["started"], summary.raw["restarted"], summary.raw["stopped"])
		}
		if summary.raw["outcome"] != "changed" {
			t.Errorf("summary outcome = %v, want changed", summary.raw["outcome"])
		}
	})

	t.Run("no-op is debug-only", func(t *testing.T) {
		docker := newFakeDocker()
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(docker, nil, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if result := services.ApplyResult(context.Background(), "fn", tmpl, "img-current", nil, nil); result.Err != nil {
			t.Fatalf("initial apply: %v", result.Err)
		}
		before := len(deploymentLogRecords(t, &logs))
		if result := services.ApplyResult(context.Background(), "fn", tmpl, "img-current", nil, nil); result.Err != nil {
			t.Fatalf("no-op apply: %v", result.Err)
		}
		records := deploymentLogRecords(t, &logs)[before:]
		if len(records) != 1 || records[0].Msg != "Service: unchanged" || records[0].Level != "DEBUG" {
			t.Fatalf("no-op records = %+v, want only DEBUG Service: unchanged", records)
		}
	})

	t.Run("request cancellation is debug-only", func(t *testing.T) {
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(
			cancelledResolveDocker{fakeDocker: newFakeDocker()},
			nil, serviceLog, testReconcileTimeout,
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		tmpl := serviceTemplate("nodejs24.x", app.Service{
			Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		})
		if result := services.ApplyResult(ctx, "fn", tmpl, "img-current", nil, nil); !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("ApplyResult error = %v, want context.Canceled", result.Err)
		}
		records := deploymentLogRecords(t, &logs)
		cancelled := deploymentRecord(records, "Service: reconciliation cancelled")
		if cancelled == nil || cancelled.Level != "DEBUG" || cancelled.raw["outcome"] != "cancelled" {
			t.Fatalf("cancellation record = %+v, want DEBUG outcome=cancelled", cancelled)
		}
		requireNoAppImageAttrs(t, "Service: reconciliation cancelled", cancelled.raw)
		if cancelled.raw["services"] != float64(1) {
			t.Errorf("cancellation services = %v, want 1 declared service",
				cancelled.raw["services"])
		}
		if deploymentRecord(records, "Service: reconciled with errors") != nil {
			t.Fatalf("cancellation emitted the warning summary: %+v", records)
		}
		if deploymentRecord(records, "Service: cannot resolve source; keeping existing containers") != nil {
			t.Fatalf("cancellation emitted a source-resolution warning: %+v", records)
		}
	})

	t.Run("cancellation summary retains known partial stop count", func(t *testing.T) {
		f := newFakeDocker()
		seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		ctx, cancel := context.WithCancel(context.Background())
		services := NewServiceReconciler(
			partialStopCancelDocker{fakeDocker: f, cancel: cancel},
			nil, serviceLog, testReconcileTimeout,
		)
		tmpl := serviceTemplate("nodejs24.x", app.Service{
			Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		})
		if result := services.ApplyResult(ctx, "fn", tmpl, "img-new", nil, nil); !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("ApplyResult error = %v, want context.Canceled", result.Err)
		}
		records := deploymentLogRecords(t, &logs)
		cancelled := deploymentRecord(records, "Service: reconciliation cancelled")
		if cancelled == nil || cancelled.Level != "DEBUG" ||
			cancelled.raw["outcome"] != "cancelled" || cancelled.raw["stopped"] != float64(1) {
			t.Fatalf("cancellation summary = %+v, want DEBUG outcome=cancelled stopped=1", cancelled)
		}
	})

	t.Run("service errors are visible", func(t *testing.T) {
		docker := newFakeDocker()
		docker.resolveErr["service.js"] = errors.New("source unavailable")
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(docker, nil, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if result := services.ApplyResult(context.Background(), "fn", tmpl, "img-current", nil, nil); result.Err == nil {
			t.Fatal("ApplyResult unexpectedly succeeded")
		}
		records := deploymentLogRecords(t, &logs)
		summary := deploymentRecord(records, "Service: reconciled with errors")
		if summary == nil || summary.Level != "WARN" || summary.raw["outcome"] != "error" {
			t.Fatalf("service error summary = %+v, want WARN outcome=error", summary)
		}
		requireNoAppImageAttrs(t, "Service: reconciled with errors", summary.raw)
		if summary.raw["services"] != float64(1) || summary.raw["replicas"] != float64(1) {
			t.Errorf("error summary scope = services:%v replicas:%v, want 1/1",
				summary.raw["services"], summary.raw["replicas"])
		}
		if !strings.Contains(stringValue(summary.raw["error"]), "source unavailable") {
			t.Errorf("service error summary error = %v, want source failure", summary.raw["error"])
		}
	})
}

// TestServiceSupersededSummaryOmitsAppImage pins the superseded edge of the
// aggregate service logs: a pass abandoned at its commit boundary still reports
// its committed transition counts and explicit outcome, but carries no
// app-level image attribute.
func TestServiceSupersededSummaryOmitsAppImage(t *testing.T) {
	f, tmpl, c, _, logs := newSupersessionSummaryHarness(t)
	superseded := false
	f.mu.Lock()
	f.startGate = func(string, runtime.ServiceSpec, int) { superseded = true }
	f.mu.Unlock()
	ctx := withReconcileAuthority(context.Background(), func() bool {
		return !superseded
	})

	if result := c.ApplyResult(ctx, "fn", tmpl, "img-new", nil, nil); result.Err != nil {
		t.Fatalf("apply: %v", result.Err)
	}

	records := deploymentLogRecords(t, logs)
	if start := deploymentRecord(records, "Service: reconciling"); start == nil {
		t.Fatal("missing Service: reconciling: the pass issued no corrective work")
	} else {
		requireNoAppImageAttrs(t, "Service: reconciling", start.raw)
	}
	summary := deploymentRecord(records, "Service: superseded")
	if summary == nil {
		t.Fatalf("missing Service: superseded summary: %+v", records)
	}
	requireNoAppImageAttrs(t, "Service: superseded", summary.raw)
	if summary.raw["outcome"] != "superseded" {
		t.Errorf("superseded summary outcome = %v, want superseded", summary.raw["outcome"])
	}
	if summary.raw["services"] != float64(1) || summary.raw["replicas"] != float64(1) {
		t.Errorf("superseded scope = services:%v replicas:%v, want 1/1",
			summary.raw["services"], summary.raw["replicas"])
	}
	if summary.raw["started"] != float64(0) || summary.raw["stopped"] != float64(0) {
		t.Errorf("superseded transitions = started:%v stopped:%v (%+v), want none committed",
			summary.raw["started"], summary.raw["stopped"], summary.raw)
	}
}

// noRuntimePreparationBuilder mirrors production's Manager.Prepare for a
// template that needs no runtime: it prepares a generation WITHOUT an app
// image, because the app's external-image services bring their own.
type noRuntimePreparationBuilder struct {
	mu       sync.Mutex
	prepared int
	failWith error
}

func (b *noRuntimePreparationBuilder) Prepare(
	_ context.Context, fn app.App,
) (*runtime.Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepared++
	if b.failWith != nil {
		return nil, b.failWith
	}
	return &runtime.Prepared{Name: fn.Name}, nil
}

func (b *noRuntimePreparationBuilder) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

func (b *noRuntimePreparationBuilder) prepares() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepared
}

// The services-only template: no runtime, two external images, five replicas in
// total. The replica total deliberately differs from the declared service count
// so a `services` attribute that counted replicas would be caught.
const noRuntimeServicesTemplate = `services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8080
    replicas: 2
  - name: worker
    image: ghcr.io/acme/worker:2.0
    port: 9090
    replicas: 3
`

// noRuntimeServicesTemplateChanged differs from the template above so the
// second pass is a desired-generation change (a no-runtime fingerprint covers
// template.yaml alone).
const noRuntimeServicesTemplateChanged = `services:
  - name: api
    image: ghcr.io/acme/api:1.2
    port: 8081
    replicas: 2
  - name: worker
    image: ghcr.io/acme/worker:2.0
    port: 9090
    replicas: 3
`

// TestNoRuntimeAppLifecycleLogsOmitEmptyImageAttributes pins the app-level
// image rule for a generation that has no app image: a services-only app never
// logs `image`/`active_image`/`previous_image` — not on preparation, not on
// publication, not on readiness, and not when a preparation failure degrades
// against the retained generation — while every other correlation field stays.
// The aggregate service lines of the same pass are checked alongside them, and
// `services` reports the two DECLARED services, not their five replicas.
func TestNoRuntimeAppLifecycleLogsOmitEmptyImageAttributes(t *testing.T) {
	root := t.TempDir()
	dir, tmpl := writeLoggedApp(t, root, "external-only", noRuntimeServicesTemplate)
	fn := app.App{Name: "external-only", Dir: dir, Template: tmpl}
	builder := &noRuntimePreparationBuilder{}

	var serviceLogs testutil.SyncBuffer
	services := NewServiceReconciler(
		newFakeDocker(), nil, slog.New(slog.NewJSONHandler(&serviceLogs,
			&slog.HandlerOptions{Level: slog.LevelDebug})),
		testReconcileTimeout,
	)
	r, logs := deploymentLogHarness(t, root, nil, builder, func(cfg *Config) {
		cfg.UpdateServicesWithStatus = func(
			name string, tmpl *app.Template, image string,
			_ func(), onComplete func(error),
		) {
			onComplete(services.ApplyResult(context.Background(), name, tmpl, image, nil, nil).Err)
		}
	})

	// given the app is discovered and converges end to end
	r.reconcileApp(fn.Name)
	records := deploymentLogRecords(t, logs)

	// then preparation claims no app image identity
	preparing := deploymentRecord(records, "App: preparing")
	if preparing == nil || preparing.Level != "INFO" ||
		preparing.raw["outcome"] != "discovered" {
		t.Fatalf("discovery preparing = %+v, want INFO outcome=discovered", preparing)
	}
	requireNoAppImageAttrs(t, "App: preparing", preparing.raw)
	if preparing.raw["fingerprint"] == "" {
		t.Errorf("preparing attrs = %+v, want the desired fingerprint", preparing.raw)
	}

	// then publication reports the transition without inventing an image
	published := deploymentRecord(records, "App: image published")
	if published == nil || published.Level != "INFO" ||
		published.raw["outcome"] != "discovered" {
		t.Fatalf("publication record = %+v, want INFO outcome=discovered", published)
	}
	requireNoAppImageAttrs(t, "App: image published", published.raw)
	if published.raw["fingerprint"] == "" {
		t.Errorf("publication attrs = %+v, want the built fingerprint", published.raw)
	}

	// then readiness correlates the generation but carries no image, and counts
	// the declared services rather than their replicas
	ready := deploymentRecord(records, "App: ready")
	if ready == nil || ready.Level != "INFO" || ready.raw["outcome"] != "ready" {
		t.Fatalf("ready record = %+v, want INFO outcome=ready", ready)
	}
	requireNoAppImageAttrs(t, "App: ready", ready.raw)
	if ready.raw["services"] != float64(2) {
		t.Errorf("ready services = %v, want the 2 declared services, not 5 replicas",
			ready.raw["services"])
	}
	if ready.raw["fingerprint"] == "" {
		t.Errorf("ready attrs = %+v, want the built fingerprint", ready.raw)
	}

	// then the aggregate service lines of the same pass omit the app image too
	// while reporting the declared service and replica counts
	serviceRecords := deploymentLogRecords(t, &serviceLogs)
	reconciling := deploymentRecord(serviceRecords, "Service: reconciling")
	if reconciling == nil {
		t.Fatalf("missing Service: reconciling: %+v", serviceRecords)
	}
	requireNoAppImageAttrs(t, "Service: reconciling", reconciling.raw)
	if reconciling.raw["services"] != float64(2) {
		t.Errorf("reconciling services = %v, want the 2 declared services",
			reconciling.raw["services"])
	}
	reconciled := deploymentRecord(serviceRecords, "Service: reconciled")
	if reconciled == nil || reconciled.Level != "INFO" {
		t.Fatalf("service summary = %+v, want INFO", reconciled)
	}
	requireNoAppImageAttrs(t, "Service: reconciled", reconciled.raw)
	if reconciled.raw["services"] != float64(2) || reconciled.raw["replicas"] != float64(5) {
		t.Errorf("service scope = services:%v replicas:%v, want 2 declared / 5 replicas",
			reconciled.raw["services"], reconciled.raw["replicas"])
	}
	if reconciled.raw["started"] != float64(5) {
		t.Errorf("service summary started = %v, want one start per replica",
			reconciled.raw["started"])
	}

	// when the desired template changes and the next preparation fails
	before := len(records)
	changedTemplate := []byte(noRuntimeServicesTemplateChanged)
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), changedTemplate, 0o644); err != nil {
		t.Fatalf("change template: %v", err)
	}
	prepareErr := errors.New("no-runtime prepare refused")
	builder.failWith = prepareErr
	r.reconcileApp(fn.Name)
	changed := deploymentLogRecords(t, logs)[before:]

	// then the retrying preparation still reports the retained generation
	// without claiming an image for it
	retry := deploymentRecord(changed, "App: preparing")
	if retry == nil || retry.raw["outcome"] != "updated" {
		t.Fatalf("changed preparing = %+v, want outcome=updated", retry)
	}
	requireNoAppImageAttrs(t, "App: preparing (changed)", retry.raw)
	activeFP := ready.raw["fingerprint"]
	if retry.raw["active_fingerprint"] != activeFP {
		t.Errorf("active_fingerprint = %v, want the still-active %v",
			retry.raw["active_fingerprint"], activeFP)
	}
	if retry.raw["fingerprint"] == retry.raw["active_fingerprint"] {
		t.Errorf("fingerprint = %v did not move with the template change",
			retry.raw["fingerprint"])
	}

	// then the degraded record keeps every prior correlation field and omits the
	// image identity the retained generation does not have
	degradedMsg := "App: deployment degraded; retaining active generation"
	degraded := deploymentRecord(changed, degradedMsg)
	if degraded == nil || degraded.Level != "ERROR" ||
		degraded.raw["outcome"] != "degraded" {
		t.Fatalf("degraded record = %+v, want ERROR outcome=degraded", degraded)
	}
	requireNoAppImageAttrs(t, degradedMsg, degraded.raw)
	if degraded.raw["fingerprint"] == "" ||
		degraded.raw["fingerprint"] == degraded.raw["active_fingerprint"] {
		t.Errorf("degraded fingerprint = %v (active %v), want the failed desired identity",
			degraded.raw["fingerprint"], degraded.raw["active_fingerprint"])
	}
	if degraded.raw["active_fingerprint"] != activeFP {
		t.Errorf("degraded active_fingerprint = %v, want the still-active %v",
			degraded.raw["active_fingerprint"], activeFP)
	}
	if _, ok := degraded.raw["duration"]; !ok {
		t.Errorf("degraded record lost its duration: %+v", degraded.raw)
	}
	if !strings.Contains(stringValue(degraded.raw["error"]), prepareErr.Error()) {
		t.Errorf("degraded error = %v, want %q", degraded.raw["error"], prepareErr)
	}

	// then a failed preparation never publishes or reports readiness
	if deploymentRecord(changed, "App: image published") != nil ||
		deploymentRecord(changed, "App: ready") != nil {
		t.Errorf("failed preparation logged a transition it did not make: %+v", changed)
	}
	if builder.prepares() != 2 {
		t.Errorf("preparations = %d, want discovery plus the failed retry",
			builder.prepares())
	}
}

// TestRuntimeAppLifecycleLogsCarryManagedImage is the control for the
// no-runtime omission: a generation that DOES build an app image keeps that
// identity on its publication and readiness lines.
func TestRuntimeAppLifecycleLogsCarryManagedImage(t *testing.T) {
	root := t.TempDir()
	writeLoggedApp(t, root, "managed-ready", template)
	r, logs := deploymentLogHarness(t, root, nil, &fakeBuilder{}, func(cfg *Config) {
		cfg.UpdateServicesWithStatus = func(
			_ string, _ *app.Template, _ string, _ func(), onComplete func(error),
		) {
			onComplete(nil)
		}
	})

	r.reconcileApp("managed-ready")
	records := deploymentLogRecords(t, logs)

	published := deploymentRecord(records, "App: image published")
	if published == nil || published.raw["image"] != "img-managed-ready" {
		t.Fatalf("publication record = %+v, want the managed image", published)
	}
	ready := deploymentRecord(records, "App: ready")
	if ready == nil || ready.Level != "INFO" ||
		ready.raw["image"] != "img-managed-ready" {
		t.Fatalf("ready record = %+v, want INFO carrying the managed image", ready)
	}
	if ready.raw["outcome"] != "ready" || ready.raw["fingerprint"] == "" {
		t.Errorf("ready attrs = %+v, want fingerprint and outcome=ready", ready.raw)
	}
}

// TestSubsequentReconcileReadinessTracksGenerationIdentity pins the lifecycle
// attrs across a SECOND successful pass: a managed swap keeps its image
// lineage (the still-serving active image, then the new image beside its
// predecessor), while a no-runtime update stays imageless end to end and
// readiness carries the new fingerprint plus the declared service count.
func TestSubsequentReconcileReadinessTracksGenerationIdentity(t *testing.T) {
	t.Run("managed generation swap keeps image lineage", func(t *testing.T) {
		root := t.TempDir()
		dir, _ := writeLoggedApp(t, root, "managed-update", template)
		r, logs := deploymentLogHarness(t, root, nil, &versionedBuilder{},
			func(cfg *Config) {
				cfg.UpdateServicesWithStatus = func(
					_ string, _ *app.Template, _ string, _ func(),
					onComplete func(error),
				) {
					onComplete(nil)
				}
			})

		// given the first generation was discovered and became ready
		r.reconcileApp("managed-update")
		first := deploymentLogRecords(t, logs)
		firstReady := deploymentRecord(first, "App: ready")
		if firstReady == nil ||
			firstReady.raw["image"] != "img-managed-update-v1" {
			t.Fatalf("discovery ready = %+v, want image v1", firstReady)
		}

		// when the source changes and the next preparation succeeds
		if err := os.WriteFile(filepath.Join(dir, "index.js"),
			[]byte("export function hi(e, context){ return 'v2'; }\n"),
			0o644,
		); err != nil {
			t.Fatalf("change app source: %v", err)
		}
		before := len(first)
		r.reconcileApp("managed-update")
		changed := deploymentLogRecords(t, logs)[before:]

		// then preparation names the still-serving generation it replaces
		preparing := deploymentRecord(changed, "App: preparing")
		if preparing == nil || preparing.raw["outcome"] != "updated" {
			t.Fatalf("update preparing = %+v, want outcome=updated", preparing)
		}
		if preparing.raw["active_image"] != "img-managed-update-v1" {
			t.Errorf("active_image = %v, want the still-serving v1",
				preparing.raw["active_image"])
		}
		if preparing.raw["fingerprint"] ==
			preparing.raw["active_fingerprint"] {
			t.Errorf("fingerprint = %v did not move past the active %v",
				preparing.raw["fingerprint"],
				preparing.raw["active_fingerprint"])
		}

		// then publication and readiness carry the new image beside its
		// predecessor
		published := deploymentRecord(changed, "App: image published")
		if published == nil ||
			published.raw["image"] != "img-managed-update-v2" ||
			published.raw["previous_image"] != "img-managed-update-v1" {
			t.Errorf("publication = %+v, want image v2 preceded by v1",
				published)
		}
		ready := deploymentRecord(changed, "App: ready")
		if ready == nil || ready.Level != "INFO" ||
			ready.raw["outcome"] != "ready" {
			t.Fatalf("update ready = %+v, want INFO outcome=ready", ready)
		}
		if ready.raw["image"] != "img-managed-update-v2" ||
			ready.raw["previous_image"] != "img-managed-update-v1" {
			t.Errorf("ready lineage = %+v, want image v2 preceded by v1",
				ready.raw)
		}
		if ready.raw["fingerprint"] == "" ||
			ready.raw["fingerprint"] == ready.raw["previous_fingerprint"] {
			t.Errorf(
				"ready fingerprint = %v (previous %v), want the new identity",
				ready.raw["fingerprint"], ready.raw["previous_fingerprint"],
			)
		}
	})

	t.Run("no-runtime update stays imageless end to end", func(t *testing.T) {
		root := t.TempDir()
		dir, _ := writeLoggedApp(t, root, "external-update",
			noRuntimeServicesTemplate)
		r, logs := deploymentLogHarness(t, root, nil,
			&noRuntimePreparationBuilder{}, func(cfg *Config) {
				cfg.UpdateServicesWithStatus = func(
					_ string, _ *app.Template, _ string, _ func(),
					onComplete func(error),
				) {
					onComplete(nil)
				}
			})

		// given the services-only app became ready from its first pass
		r.reconcileApp("external-update")
		first := deploymentLogRecords(t, logs)
		firstReady := deploymentRecord(first, "App: ready")
		if firstReady == nil {
			t.Fatal("discovery never reported ready")
		}
		requireNoAppImageAttrs(t, "App: ready (discovery)", firstReady.raw)

		// when the desired template changes and the update succeeds
		before := len(first)
		if err := os.WriteFile(filepath.Join(dir, "template.yaml"),
			[]byte(noRuntimeServicesTemplateChanged), 0o644); err != nil {
			t.Fatalf("change template: %v", err)
		}
		r.reconcileApp("external-update")
		changed := deploymentLogRecords(t, logs)[before:]

		// then the updating preparation correlates the retained generation
		// without claiming an image for it
		preparing := deploymentRecord(changed, "App: preparing")
		if preparing == nil || preparing.raw["outcome"] != "updated" {
			t.Fatalf("update preparing = %+v, want outcome=updated", preparing)
		}
		requireNoAppImageAttrs(t, "App: preparing (updated)", preparing.raw)
		if preparing.raw["active_fingerprint"] == "" ||
			preparing.raw["fingerprint"] ==
				preparing.raw["active_fingerprint"] {
			t.Errorf(
				"preparing = %+v, want the new fingerprint beside the retained",
				preparing.raw,
			)
		}

		// then publication and readiness stay imageless — no image and no
		// previous_image for the imageless predecessor — while the declared
		// service count and the new fingerprint carry through
		published := deploymentRecord(changed, "App: image published")
		if published == nil {
			t.Fatal("update was never published")
		}
		requireNoAppImageAttrs(t, "App: image published (updated)",
			published.raw)
		ready := deploymentRecord(changed, "App: ready")
		if ready == nil || ready.Level != "INFO" ||
			ready.raw["outcome"] != "ready" {
			t.Fatalf("update ready = %+v, want INFO outcome=ready", ready)
		}
		requireNoAppImageAttrs(t, "App: ready (updated)", ready.raw)
		if ready.raw["services"] != float64(2) {
			t.Errorf("ready services = %v, want the 2 declared services",
				ready.raw["services"])
		}
		if ready.raw["fingerprint"] == "" ||
			ready.raw["fingerprint"] == ready.raw["previous_fingerprint"] {
			t.Errorf(
				"ready fingerprint = %v (previous %v), want the new identity",
				ready.raw["fingerprint"], ready.raw["previous_fingerprint"],
			)
		}
	})
}

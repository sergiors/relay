package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/routing"
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
	t.Run("corrective summary records committed transitions and image", func(t *testing.T) {
		docker := newFakeDocker()
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if err := services.Apply(context.Background(), "fn", tmpl, "img-current", nil); err != nil {
			t.Fatalf("apply: %v", err)
		}
		records := deploymentLogRecords(t, &logs)
		start := deploymentRecord(records, "Service: reconciling")
		if start == nil || start.Level != "INFO" || start.raw["image"] != "img-current" {
			t.Fatalf("reconciling record = %+v, want INFO with current image", start)
		}
		summary := deploymentRecord(records, "Service: reconciled")
		if summary == nil || summary.Level != "INFO" || summary.raw["image"] != "img-current" {
			t.Fatalf("changed summary = %+v, want INFO with current image", summary)
		}
		if summary.raw["started"] != float64(1) || summary.raw["restarted"] != float64(0) || summary.raw["stopped"] != float64(0) {
			t.Errorf("committed transition counts = started:%v restarted:%v stopped:%v, want 1/0/0",
				summary.raw["started"], summary.raw["restarted"], summary.raw["stopped"])
		}
	})

	t.Run("no-op is debug-only", func(t *testing.T) {
		docker := newFakeDocker()
		var logs testutil.SyncBuffer
		serviceLog := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if err := services.Apply(context.Background(), "fn", tmpl, "img-current", nil); err != nil {
			t.Fatalf("initial apply: %v", err)
		}
		before := len(deploymentLogRecords(t, &logs))
		if err := services.Apply(context.Background(), "fn", tmpl, "img-current", nil); err != nil {
			t.Fatalf("no-op apply: %v", err)
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
			nil, routing.TraefikConfig{}, serviceLog, testReconcileTimeout,
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		tmpl := serviceTemplate("nodejs24.x", app.Service{
			Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		})
		if err := services.Apply(ctx, "fn", tmpl, "img-current", nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply error = %v, want context.Canceled", err)
		}
		records := deploymentLogRecords(t, &logs)
		cancelled := deploymentRecord(records, "Service: reconciliation cancelled")
		if cancelled == nil || cancelled.Level != "DEBUG" || cancelled.raw["outcome"] != "cancelled" {
			t.Fatalf("cancellation record = %+v, want DEBUG outcome=cancelled", cancelled)
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
			nil, routing.TraefikConfig{}, serviceLog, testReconcileTimeout,
		)
		tmpl := serviceTemplate("nodejs24.x", app.Service{
			Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
		})
		if err := services.Apply(ctx, "fn", tmpl, "img-new", nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply error = %v, want context.Canceled", err)
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
		services := NewServiceReconciler(docker, nil, routing.TraefikConfig{}, serviceLog, testReconcileTimeout)
		tmpl := serviceTemplate("nodejs24.x", app.Service{Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1})
		if err := services.Apply(context.Background(), "fn", tmpl, "img-current", nil); err == nil {
			t.Fatal("Apply unexpectedly succeeded")
		}
		records := deploymentLogRecords(t, &logs)
		summary := deploymentRecord(records, "Service: reconciled with errors")
		if summary == nil || summary.Level != "WARN" || summary.raw["outcome"] != "error" {
			t.Fatalf("service error summary = %+v, want WARN outcome=error", summary)
		}
		if !strings.Contains(stringValue(summary.raw["error"]), "source unavailable") {
			t.Errorf("service error summary error = %v, want source failure", summary.raw["error"])
		}
	})
}

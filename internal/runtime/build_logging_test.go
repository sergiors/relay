package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime/plan"
)

type buildLogRecord struct {
	msg   string
	level slog.Level
	attrs map[string]slog.Value
}

type buildLogCapture struct {
	mu      sync.Mutex
	records []buildLogRecord
	notify  chan buildLogRecord
}

func newBuildLogCapture() *buildLogCapture {
	return &buildLogCapture{notify: make(chan buildLogRecord, 64)}
}

func (c *buildLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *buildLogCapture) Handle(_ context.Context, record slog.Record) error {
	entry := buildLogRecord{
		msg:   record.Message,
		level: record.Level,
		attrs: make(map[string]slog.Value),
	}
	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value
		return true
	})
	c.mu.Lock()
	c.records = append(c.records, entry)
	c.mu.Unlock()
	select {
	case c.notify <- entry:
	default:
	}
	return nil
}

func (c *buildLogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *buildLogCapture) WithGroup(string) slog.Handler      { return c }

func (c *buildLogCapture) snapshot() []buildLogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]buildLogRecord(nil), c.records...)
}

func (c *buildLogCapture) waitMessage(t *testing.T, msg string) buildLogRecord {
	t.Helper()
	for {
		select {
		case record := <-c.notify:
			if record.msg == msg {
				return record
			}
		case <-deadlockGuard(t):
			t.Fatalf("timed out waiting for log message %q", msg)
		}
	}
}

func buildLogManager(t *testing.T, lifecycle context.Context) (*Manager, *buildLogCapture) {
	t.Helper()
	capture := newBuildLogCapture()
	m := &Manager{
		log:        slog.New(capture),
		lifecycle:  lifecycle,
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, capture
}

func assertBuildLogFields(t *testing.T, record buildLogRecord, appName, image, kind, fingerprint string) {
	t.Helper()
	for key, want := range map[string]string{
		"app":         appName,
		"image":       image,
		"kind":        kind,
		"fingerprint": fingerprint,
	} {
		if got := record.attrs[key].String(); got != want {
			t.Errorf("%s %s = %q, want %q", record.msg, key, got, want)
		}
	}
}

func TestManagedBuildLifecycleLogsStructuredOutcomes(t *testing.T) {
	for _, kind := range []string{"app", "dependency"} {
		for _, outcome := range []string{"completed", "failed", "timed out", "cancelled"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				lifecycle, cancelLifecycle := context.WithCancel(context.Background())
				defer cancelLifecycle()
				m, logs := buildLogManager(t, lifecycle)
				image := "relay-app-example:abc"
				fingerprint := "app-fingerprint"
				if kind == "dependency" {
					image = depImageRef("dependency-fingerprint")
					fingerprint = "dependency-fingerprint"
				}

				entered := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					_, err := m.runImageBuild(context.Background(), "example", image, func(ctx context.Context, _ string) error {
						close(entered)
						switch outcome {
						case "failed":
							return errors.New("install failed")
						case "timed out":
							return context.DeadlineExceeded
						case "cancelled":
							<-ctx.Done()
							return ctx.Err()
						default:
							return nil
						}
					}, fingerprint)
					done <- err
				}()

				<-entered
				if outcome == "cancelled" {
					cancelLifecycle()
				}
				err := <-done
				switch outcome {
				case "completed":
					if err != nil {
						t.Fatalf("runImageBuild: %v", err)
					}
				case "failed":
					if err == nil || !strings.Contains(err.Error(), "install failed") {
						t.Fatalf("runImageBuild error = %v, want install failure", err)
					}
				case "timed out":
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("runImageBuild error = %v, want deadline exceeded", err)
					}
				case "cancelled":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("runImageBuild error = %v, want canceled", err)
					}
				}

				records := logs.snapshot()
				started := findBuildLog(records, "Build: started")
				if started == nil {
					t.Fatal("missing Build: started")
				}
				assertBuildLogFields(t, *started, "example", image, kind, fingerprint)

				terminalMessage := "Build: " + outcome
				terminal := findBuildLog(records, terminalMessage)
				if terminal == nil {
					t.Fatalf("missing %s", terminalMessage)
				}
				assertBuildLogFields(t, *terminal, "example", image, kind, fingerprint)
				if _, ok := terminal.attrs["duration"]; !ok {
					t.Errorf("%s is missing duration", terminalMessage)
				}
				wantOutcome := map[string]string{
					"completed": "success",
					"failed":    "failed",
					"timed out": "timeout",
					"cancelled": "cancelled",
				}[outcome]
				if got := terminal.attrs["outcome"].String(); got != wantOutcome {
					t.Errorf("%s outcome = %q, want %q", terminalMessage, got, wantOutcome)
				}
				if outcome == "timed out" {
					if _, ok := terminal.attrs["timeout"]; !ok {
						t.Error("timeout record is missing timeout")
					}
				}
			})
		}
	}
}

func TestBuildProgressCadenceAndLifecycleBoundaries(t *testing.T) {
	for _, kind := range []string{"app", "dependency"} {
		t.Run(kind, func(t *testing.T) {
			lifecycle, cancelLifecycle := context.WithCancel(context.Background())
			defer cancelLifecycle()
			m, logs := buildLogManager(t, lifecycle)
			m.buildProgressInitialDelay = time.Millisecond
			m.buildProgressInterval = 8 * time.Millisecond
			image := "relay-app-progress:abc"
			if kind == "dependency" {
				image = depImageRef("progress-fingerprint")
			}
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := m.runImageBuild(context.Background(), "progress-app", image, func(context.Context, string) error {
					close(entered)
					<-release
					return nil
				}, "progress-fingerprint")
				done <- err
			}()
			<-entered
			first := logs.waitMessage(t, "Build: progress")
			second := logs.waitMessage(t, "Build: progress")
			assertBuildLogFields(t, first, "progress-app", image, kind, "progress-fingerprint")
			assertBuildLogFields(t, second, "progress-app", image, kind, "progress-fingerprint")
			firstElapsed := first.attrs["elapsed"].Duration()
			secondElapsed := second.attrs["elapsed"].Duration()
			if secondElapsed-firstElapsed < 4*time.Millisecond {
				t.Fatalf("progress interval = %v, want bounded cadence near the configured %v", secondElapsed-firstElapsed, m.buildProgressInterval)
			}

			close(release)
			if err := <-done; err != nil {
				t.Fatalf("runImageBuild: %v", err)
			}
			if findBuildLog(logs.snapshot(), "Build: completed") == nil {
				t.Fatal("missing completion record")
			}
			progressCount := countBuildLogs(logs.snapshot(), "Build: progress")
			for len(logs.notify) > 0 {
				record := <-logs.notify
				if record.msg == "Build: progress" {
					t.Fatal("progress was emitted after the build completed")
				}
			}
			if got := countBuildLogs(logs.snapshot(), "Build: progress"); got != progressCount {
				t.Fatalf("progress count changed after completion: %d -> %d", progressCount, got)
			}
		})
	}
}

func TestBuildLogsWaitForPermitAndDoNotLogForReusedImages(t *testing.T) {
	m, logs := buildLogManager(t, context.Background())
	limiter := newBuildLimiter(1)
	m.buildLimit.Store(limiter)
	if err := limiter.acquire(context.Background()); err != nil {
		t.Fatalf("occupy build permit: %v", err)
	}
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := m.runImageBuild(context.Background(), "waiting-app", "relay-app-waiting:fp", func(context.Context, string) error {
			close(entered)
			return nil
		}, "waiting-fingerprint")
		done <- err
	}()

	if got := logs.snapshot(); len(got) != 0 {
		t.Fatalf("build logged before obtaining a permit: %+v", got)
	}
	limiter.release()
	<-entered
	if err := <-done; err != nil {
		t.Fatalf("runImageBuild: %v", err)
	}
	if findBuildLog(logs.snapshot(), "Build: started") == nil {
		t.Fatal("build did not log after obtaining a permit")
	}
}

func TestSharedDependencyWaiterOnlyLogsJoin(t *testing.T) {
	m, logs := buildLogManager(t, context.Background())
	depRef := depImageRef("shared-fingerprint")
	entered, release := make(chan struct{}), make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		leaderDone <- m.buildDependencyOnce(context.Background(), depRef, func() error {
			_, err := m.runImageBuild(context.Background(), "leader-app", depRef, func(context.Context, string) error {
				close(entered)
				<-release
				return nil
			}, "shared-fingerprint")
			return err
		})
	}()
	<-entered

	waiterDone := make(chan error, 1)
	go func() {
		waiterDone <- m.buildDependencyOnce(context.Background(), depRef, func() error {
			return errors.New("waiter unexpectedly became leader")
		})
	}()
	join := logs.waitMessage(t, "Dependency build already in flight; waiting")
	if join.level != slog.LevelDebug {
		t.Fatalf("dependency join level = %s, want DEBUG", join.level)
	}
	if got := join.attrs["dep_image"].String(); got != depRef {
		t.Fatalf("join dep_image = %q, want %q", got, depRef)
	}
	for _, record := range logs.snapshot() {
		if record.msg == "Build: started" {
			assertBuildLogFields(t, record, "leader-app", depRef, "dependency", "shared-fingerprint")
		}
	}
	if got := countBuildLogs(logs.snapshot(), "Build: started"); got != 1 {
		t.Fatalf("Build: started count while waiter joined = %d, want 1", got)
	}
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader: %v", err)
	}
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter: %v", err)
	}
	records := logs.snapshot()
	if len(records) != 3 {
		t.Fatalf("shared dependency emitted %d records, want exactly join, start, and completion: %+v", len(records), records)
	}
	if got := countBuildLogs(records, "Build: started"); got != 1 {
		t.Errorf("Build: started count = %d, want 1", got)
	}
	if got := countBuildLogs(records, "Build: completed"); got != 1 {
		t.Errorf("Build: completed count = %d, want 1", got)
	}
}

func TestImageReuseInfoFieldsAndBuildMetrics(t *testing.T) {
	t.Run("app reuse", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(event, context){}\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		fn := app.App{Name: "app-reuse-log", Dir: dir, Template: &app.Template{Runtime: "nodejs24.x"}}
		fp, err := app.FingerprintApp(dir, fn.Template)
		if err != nil {
			t.Fatalf("fingerprint: %v", err)
		}
		image := ImageRef(fn.Name, fp)
		bootstrap := bootstrapForTest(t, fn)
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + image + "/json", body: imageInspectJSON(image, bootstrap)},
		)
		m, logs := buildLogManager(t, context.Background())
		m.cli = cli
		if _, err := m.Prepare(context.Background(), fn); err != nil {
			t.Fatalf("prepare: %v", err)
		}
		reused := findBuildLog(logs.snapshot(), "App: image reused")
		if reused == nil {
			t.Fatal("missing app image reuse Info record")
		}
		for key, want := range map[string]string{
			"app": fn.Name, "image": image, "fingerprint": fp, "outcome": "reused",
		} {
			if got := reused.attrs[key].String(); got != want {
				t.Errorf("app reuse %s = %q, want %q", key, got, want)
			}
		}
		if got := countBuildLogs(logs.snapshot(), "Build: started"); got != 0 {
			t.Fatalf("reused image emitted %d build-start records, want 0", got)
		}
	})

	t.Run("dependency reuse", func(t *testing.T) {
		const fingerprint = "dependency-reuse-fingerprint"
		depRef := depImageRef(fingerprint)
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", body: `{"Id":"sha256:abc"}`},
		)
		m, logs := buildLogManager(t, context.Background())
		m.cli = cli
		spec, err := lookup("python3.14")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		deps := plan.Deps{Install: "pip install -r requirements.txt", Dir: "/app", Files: []string{"requirements.txt"}}
		fn := app.App{Name: "dep-reuse-log", Dir: t.TempDir(), Template: &app.Template{Runtime: "python3.14"}}
		snap := testDependencySnapshot(t, map[string]string{"requirements.txt": "six==1.16.0\n"})
		if _, err := m.ensureDependencyImage(context.Background(), fn, spec, deps, snap, fingerprint, depRef); err != nil {
			t.Fatalf("ensure dependency image: %v", err)
		}
		reused := findBuildLog(logs.snapshot(), "Dependency image reused")
		if reused == nil {
			t.Fatal("missing dependency image reuse Info record")
		}
		for key, want := range map[string]string{
			"app": fn.Name, "dep_image": depRef, "fingerprint": fingerprint, "outcome": "reused",
		} {
			if got := reused.attrs[key].String(); got != want {
				t.Errorf("dependency reuse %s = %q, want %q", key, got, want)
			}
		}
	})

	t.Run("failed build metrics", func(t *testing.T) {
		fn := writeBuildApp(t, "build-metric-app")
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"missing"}`},
			dockerRoute{method: http.MethodPost, path: "/build", body: `{"errorDetail":{"message":"build failed"}}`},
		)
		registry := metrics.New()
		m, _ := buildLogManager(t, context.Background())
		m.cli = cli
		m.metrics = registry
		if _, err := m.Prepare(context.Background(), fn); err == nil {
			t.Fatal("Prepare unexpectedly succeeded")
		}
		if got := registry.CounterLabels(metrics.MetricAppBuildFailures, []metrics.Label{{Name: "app", Value: fn.Name}}); got != 1 {
			t.Fatalf("app build failures = %d, want 1", got)
		}
		if count, _ := registry.HistogramLabels(metrics.MetricAppBuild, []metrics.Label{{Name: "app", Value: fn.Name}}); count != 1 {
			t.Fatalf("app build duration observations = %d, want 1", count)
		}
	})
}

func findBuildLog(records []buildLogRecord, msg string) *buildLogRecord {
	for i := range records {
		if records[i].msg == msg {
			return &records[i]
		}
	}
	return nil
}

func countBuildLogs(records []buildLogRecord, msg string) int {
	n := 0
	for _, record := range records {
		if record.msg == msg {
			n++
		}
	}
	return n
}

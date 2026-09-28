package runtime

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/moby/moby/client"

	"relay/internal/function"
	"relay/internal/runtime/plan"
)

// newLogCapturingManager builds a Manager directly (no constructor/Docker) whose
// Debug-level logs are captured into the returned buffer, so a test can assert
// the inspection-timing line on which a Prepare outcome logged.
func newLogCapturingManager(t *testing.T, cli *client.Client) (*Manager, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	m := &Manager{
		log:        slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		cli:        cli,
		hostname:   "test-host",
		lifecycle:  context.Background(),
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, &buf
}

// TestPrepareLogsInspectionTimingOnReuseAndMiss is the observability regression
// for the Docker inspect probe: a REUSE outcome and a BUILD (miss) outcome must
// BOTH emit their Debug line carrying inspect_duration, so the probe cost is
// visible during startup triage on either branch — and the miss path logs one
// line (not one per staged content file).
func TestPrepareLogsInspectionTimingOnReuseAndMiss(t *testing.T) {
	// --- Reuse: the image exists with a matching bootstrap label. ---
	t.Run("reuse", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		fn := function.Function{Name: "reuse-timing", Dir: dir, Template: &function.Template{Runtime: "node24"}}
		fp, err := function.FingerprintFunction(dir, fn.Template)
		if err != nil {
			t.Fatalf("fingerprint: %v", err)
		}
		image := ImageRef(fn.Name, fp)
		bootstrap := bootstrapForTest(t, fn)

		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + image + "/json", body: imageInspectJSON(image, bootstrap)},
		)
		m, logs := newLogCapturingManager(t, cli)

		if _, err := m.Prepare(context.Background(), fn); err != nil {
			t.Fatalf("prepare reuse: %v", err)
		}
		assertSingleInspectTimingLine(t, logs.String(), "image exists; reusing")
	})

	// --- Miss/build: the image is absent, so the probe misses and a build runs. ---
	t.Run("miss", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		// A second content file so a per-file logging bug would be visible: the
		// miss path must still emit exactly ONE timing line.
		if err := os.WriteFile(filepath.Join(dir, "util.js"), []byte("export const u = 1;\n"), 0o644); err != nil {
			t.Fatalf("write util: %v", err)
		}
		fn := function.Function{Name: "miss-timing", Dir: dir, Template: &function.Template{Runtime: "node24"}}

		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			dockerRoute{method: http.MethodPost, path: "/build", body: `{}`},
		)
		m, logs := newLogCapturingManager(t, cli)

		if _, err := m.Prepare(context.Background(), fn); err != nil {
			t.Fatalf("prepare miss: %v", err)
		}
		assertSingleInspectTimingLine(t, logs.String(), "image absent or stale; building")
	})
}

// TestEnsureDependencyImageLogsInspectionTimingOnReuseAndMiss is the
// observability regression for the dependency image probe: a dependency-layer
// REUSE outcome and a BUILD (miss) outcome must BOTH emit a Debug line carrying
// inspect_duration, so the dependency probe cost is visible during startup
// triage on either branch — without a line per staged manifest.
func TestEnsureDependencyImageLogsInspectionTimingOnReuseAndMiss(t *testing.T) {
	const sentinel = "dependency-timing-sentinel"
	depRef := depImageRef(sentinel)

	spec, err := lookup("python3.14")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	deps := plan.Deps{Install: "pip install -r requirements.txt", Dir: "/app", Files: []string{"requirements.txt"}}

	// --- Reuse: the dependency image is already present. ---
	t.Run("reuse", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", body: `{"Id":"sha256:abc"}`},
		)
		m, logs := newLogCapturingManager(t, cli)

		fn := function.Function{Name: "dep-reuse-timing", Dir: t.TempDir(), Template: &function.Template{Runtime: "python3.14"}}
		snap := dependencySnapshot{files: []dependencyManifest{{name: "requirements.txt", content: []byte("six==1.16.0\n")}}}

		if _, err := m.ensureDependencyImage(context.Background(), fn, spec, deps, snap, sentinel, depRef); err != nil {
			t.Fatalf("ensure dependency image (reuse): %v", err)
		}
		assertSingleInspectTimingLine(t, logs.String(), "Dependency image exists; reusing")
	})

	// --- Miss/build: the dependency image is absent, so the probe misses. ---
	t.Run("miss", func(t *testing.T) {
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			dockerRoute{method: http.MethodPost, path: "/build", body: `{}`},
		)
		m, logs := newLogCapturingManager(t, cli)

		fn := function.Function{Name: "dep-miss-timing", Dir: t.TempDir(), Template: &function.Template{Runtime: "python3.14"}}
		snap := dependencySnapshot{files: []dependencyManifest{{name: "requirements.txt", content: []byte("six==1.16.0\n")}}}

		if _, err := m.ensureDependencyImage(context.Background(), fn, spec, deps, snap, sentinel, depRef); err != nil {
			t.Fatalf("ensure dependency image (miss): %v", err)
		}
		assertSingleInspectTimingLine(t, logs.String(), "Dependency image absent; building")
	})
}

// assertSingleInspectTimingLine asserts msg appears exactly once and its line
// carries inspect_duration, so the Docker-inspection timing is observable on
// that Prepare outcome without a line per content file.
func assertSingleInspectTimingLine(t *testing.T, logs, msg string) {
	t.Helper()
	if n := strings.Count(logs, msg); n != 1 {
		t.Fatalf("log line %q occurred %d times, want exactly 1\nlogs:\n%s", msg, n, logs)
	}
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, msg) {
			if !strings.Contains(line, "inspect_duration=") {
				t.Fatalf("inspection timing missing from %q line: %q", msg, line)
			}
			return
		}
	}
	t.Fatalf("log line %q not found", msg)
}

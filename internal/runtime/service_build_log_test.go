package runtime

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/function"
	"relay/internal/testutil"
)

// TestServiceBuildLogsBeforeImageBuild proves the service build path emits a
// concise structured pre-build log immediately before the Dockerfile build is
// issued (and before the completion log), so a slow build is visible while it
// runs. The scripted /build route fails the build, so no completion log is
// emitted — the pre-build line must still be present.
func TestServiceBuildLogsBeforeImageBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}

	var buf testutil.SyncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		buildRoute(nil),
	)
	m := &Manager{
		log:        logger,
		cli:        cli,
		hostname:   "test-host",
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.ResolveServiceImage(context.Background(), "fn", dir, &function.Template{Runtime: "node24"}, svc, ""); err == nil {
		t.Fatal("expected the scripted build failure to surface")
	}

	out := buf.String()
	if !strings.Contains(out, "Service: building image") {
		t.Fatalf("expected the pre-build log \"Service: building image\", got:\n%s", out)
	}
	// The log must name the function and the service identity so a slow build is
	// attributable.
	if !strings.Contains(out, "function=fn") || !strings.Contains(out, "service=Dockerfile") {
		t.Fatalf("pre-build log missing function/service attributes, got:\n%s", out)
	}
	// A failed build must not emit the success completion log.
	if strings.Contains(out, "Service: build image built") {
		t.Fatalf("a failed build must not log success, got:\n%s", out)
	}
}

// TestServiceBuildLogsCompletionAfterBuild proves the completion log is retained
// and still emitted after a successful build. It is exercised through the reuse
// short-circuit's sibling: a build that the scripted daemon reports as done.
func TestServiceBuildLogsCompletionAfterBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}
	svc := function.Service{Build: "Dockerfile", Port: 80, Replicas: 1}

	var buf testutil.SyncBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodPost, path: "/build", body: `{"stream":"Successfully built abc\n"}`},
	)
	m := &Manager{
		log:        logger,
		cli:        cli,
		hostname:   "test-host",
		containers: newContainerCache(),
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.ResolveServiceImage(context.Background(), "fn", dir, &function.Template{Runtime: "node24"}, svc, ""); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	out := buf.String()
	buildIdx := strings.Index(out, "Service: building image")
	doneIdx := strings.Index(out, "Service: build image built")
	if buildIdx < 0 || doneIdx < 0 {
		t.Fatalf("expected both pre-build and completion logs, got:\n%s", out)
	}
	if buildIdx > doneIdx {
		t.Fatalf("pre-build log must precede the completion log, got:\n%s", out)
	}
}

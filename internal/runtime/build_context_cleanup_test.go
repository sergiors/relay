package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
)

// buildContextTempPrefixes are the transient build-context temp directories the
// runtime creates (one per app image build, one per dependency build) and
// must ALWAYS remove via its deferred os.RemoveAll; a leftover is a disk leak.
var buildContextTempPrefixes = []string{"relay-build-", "relay-dep-build-"}

// leakedBuildContextTempDir reports the name of a build-context temp directory
// the runtime creates that still exists directly under base. It is safe to call
// from any goroutine (no *testing.T), so the ImageBuild route callbacks — which
// run on the HTTP transport goroutine — can observe the directory's presence.
// A read error is reported as "not found"; the test-goroutine assertions below
// re-read and will surface a genuine read error there.
func leakedBuildContextTempDir(base string) (string, bool) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return "", false
	}
	for _, e := range entries {
		for _, prefix := range buildContextTempPrefixes {
			if strings.HasPrefix(e.Name(), prefix) {
				return e.Name(), true
			}
		}
	}
	return "", false
}

// isolatedTmpdir points TMPDIR at a fresh directory and returns it, so the
// runtime's os.MkdirTemp("", "relay-build-*") lands where the test can observe
// whether it was removed. t.Setenv also restores the previous value.
func isolatedTmpdir(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	t.Setenv("TMPDIR", base)
	return base
}

// TestBuildContextTempDirRemovedOnBuildSuccess proves the app image's
// transient build directory is removed after a successful build. The temp dir is
// observed present at the ImageBuild request (so the assertion cannot pass
// vacuously if a future refactor stops creating one) and absent once Prepare
// returns.
func TestBuildContextTempDirRemovedOnBuildSuccess(t *testing.T) {
	base := isolatedTmpdir(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	seen := false
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{}`,
			onRequest: func(*http.Request) { _, seen = leakedBuildContextTempDir(base) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := app.App{Name: "ctx-success", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	if _, err := m.Prepare(context.Background(), fn); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !seen {
		t.Fatal("no build context temp dir existed at ImageBuild time; the cleanup assertion would be vacuous")
	}
	if name, leaked := leakedBuildContextTempDir(base); leaked {
		t.Fatalf("build context temp dir %q leaked after a successful build", name)
	}
}

// TestBuildContextTempDirRemovedOnBuildFailure proves the app image's
// transient build directory is removed when the build fails (the daemon returns
// an error message in the stream), so a failing build cannot accumulate temp
// directories.
func TestBuildContextTempDirRemovedOnBuildFailure(t *testing.T) {
	base := isolatedTmpdir(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	seen := false
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"errorDetail":{"message":"boom"}}`,
			onRequest: func(*http.Request) { _, seen = leakedBuildContextTempDir(base) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())

	fn := app.App{Name: "ctx-failure", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("expected the scripted build failure to surface")
	}
	if !seen {
		t.Fatal("no build context temp dir existed at ImageBuild time; the cleanup assertion would be vacuous")
	}
	if name, leaked := leakedBuildContextTempDir(base); leaked {
		t.Fatalf("build context temp dir %q leaked after a failed build", name)
	}
}

// TestBuildContextTempDirRemovedOnBuildCancellation proves the app image's
// transient build directory is removed when the build is cancelled mid-flight
// (manager lifecycle cancellation while the daemon request is blocked). This is
// the cancellation half of "no temp leaks": the deferred removal in buildImage
// runs on the cancellation path exactly as on success and failure.
func TestBuildContextTempDirRemovedOnBuildCancellation(t *testing.T) {
	base := isolatedTmpdir(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	entered := make(chan struct{})
	seen := false
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(*http.Request) {
				_, seen = leakedBuildContextTempDir(base)
				close(entered)
			},
			fail: func(req *http.Request) error {
				// Block until the build's context is cancelled, exactly like a
				// long-running daemon image build interrupted by shutdown.
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)
	m := newLifecycleManager(t, cli, lifecycle)

	fn := app.App{Name: "ctx-cancel", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	done := make(chan error, 1)
	go func() {
		_, err := m.Prepare(context.Background(), fn)
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ImageBuild was not entered")
	}
	cancelLifecycle()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("build error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the active build promptly")
	}
	if !seen {
		t.Fatal("no build context temp dir existed at ImageBuild time; the cleanup assertion would be vacuous")
	}
	if name, leaked := leakedBuildContextTempDir(base); leaked {
		t.Fatalf("build context temp dir %q leaked after a cancelled build", name)
	}
}

// TestDependencyBuildContextTempDirRemoved pins the dependency build context's
// lifecycle for both outcomes: snapshotDependency stages the manifests into a
// relay-dep-build-* private root (the build context), buildDependencyImage builds
// from it, and the caller's release removes it on success and on failure.
func TestDependencyBuildContextTempDirRemoved(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "success", body: `{}`},
		{name: "failure", body: `{"errorDetail":{"message":"boom"}}`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := isolatedTmpdir(t)
			fnDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(fnDir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
				t.Fatalf("write requirements: %v", err)
			}
			seen := false
			cli := newScriptedDockerClient(t,
				dockerRoute{
					method: http.MethodPost, path: "/build", body: tc.body,
					onRequest: func(*http.Request) { _, seen = leakedBuildContextTempDir(base) },
				},
			)
			spec := pythonSpec()
			deps := pythonRequirementsDeps()
			snap, err := snapshotDependency(fnDir, deps)
			if err != nil {
				t.Fatalf("snapshot dependency: %v", err)
			}

			err = buildDependencyImage(context.Background(), cli, spec, deps, snap, depImageRef("fp"), "fp", arch, nil)
			if tc.wantErr && err == nil {
				t.Fatal("expected the scripted dependency build failure to surface")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("buildDependencyImage: %v", err)
			}
			if !seen {
				t.Fatal("no dependency build context temp dir existed at ImageBuild time; the cleanup assertion would be vacuous")
			}
			// The caller (Prepare) releases the snapshot's root after the build;
			// do the same here and assert it is gone.
			snap.release()
			if name, leaked := leakedBuildContextTempDir(base); leaked {
				t.Fatalf("dependency build context temp dir %q leaked", name)
			}
		})
	}
}

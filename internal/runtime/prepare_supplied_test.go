package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
)

// TestPrepareWithFingerprintRuntimeUsesSupplied pins the startup optimization:
// a runtime-backed function's image identity comes from the SUPPLIED
// fingerprint, not from a fresh scan of the tree. The directory is left empty of
// any source matching a full fingerprint, so only the supplied value can explain
// the returned tag.
func TestPrepareWithFingerprintRuntimeUsesSupplied(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	const supplied = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	fn := function.Function{Name: "supplied-fp", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	cli := newScriptedDockerClient(t,
		// No existing image -> build path; an empty successful build response.
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodPost, path: "/build", body: `{}`},
	)
	m := newLifecycleManager(t, cli, context.Background())

	got, err := m.PrepareWithFingerprint(context.Background(), fn, supplied)
	if err != nil {
		t.Fatalf("prepare with supplied fingerprint: %v", err)
	}
	if got.Fingerprint != supplied {
		t.Fatalf("fingerprint = %q, want the supplied %q (must not rescan)", got.Fingerprint, supplied)
	}
	if want := ImageRef(fn.Name, supplied); got.Image != want {
		t.Fatalf("image = %q, want %q (tag must be the supplied fingerprint)", got.Image, want)
	}
}

// TestPrepareWithFingerprintNoRuntimeNeverWalksTree pins the no-runtime half:
// with a supplied fingerprint Prepare touches no filesystem at all, so it
// succeeds even when the function directory does not exist. An empty
// fingerprint still falls back to computing one.
func TestPrepareWithFingerprintNoRuntimeNeverWalksTree(t *testing.T) {
	const supplied = "template-only-supplied"
	fn := function.Function{
		Name:     "externals",
		Dir:      filepath.Join(t.TempDir(), "does-not-exist"),
		Template: &function.Template{Services: []function.Service{{Image: "nginx:alpine"}}},
	}
	if fn.Template.NeedsRuntime() {
		t.Fatal("fixture precondition: template must not need a runtime")
	}
	// A nil Docker client is fine: a no-runtime prepare never touches the daemon.
	m := newClockManager(t, nil, time.Now)

	got, err := m.PrepareWithFingerprint(context.Background(), fn, supplied)
	if err != nil {
		t.Fatalf("prepare no-runtime with supplied fingerprint: %v", err)
	}
	if got.Fingerprint != supplied {
		t.Fatalf("fingerprint = %q, want supplied %q", got.Fingerprint, supplied)
	}
	if got.Image != "" {
		t.Fatalf("image = %q, want empty", got.Image)
	}

	// An empty supplied fingerprint falls back to computing one, which for a
	// no-runtime template reads template.yaml alone; the missing dir surfaces an
	// error rather than silently succeeding with a walk.
	if _, err := m.PrepareWithFingerprint(context.Background(), fn, ""); err == nil {
		t.Fatal("empty supplied fingerprint must fall back to a real fingerprint and fail on a missing dir")
	}
}

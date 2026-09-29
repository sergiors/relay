package runtime

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/source"
)

// TestPrepareWithFingerprintRuntimeUsesSnapshotIdentity pins the snapshot
// contract: for a runtime-backed function the image identity is derived from the
// source snapshot captured within Prepare, NOT taken verbatim from the supplied
// value. The supplied fingerprint is the caller's pre-build scan; the returned
// Prepared.Fingerprint and the image tag are the digest over exactly the bytes
// the build staged, so a caller that persisted the supplied value would be
// labelling bytes it did not build. Here the supplied value is a sentinel that
// cannot come from the tree, so only the snapshot-derived digest can explain the
// tag.
func TestPrepareWithFingerprintRuntimeUsesSnapshotIdentity(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	const supplied = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	fn := function.Function{Name: "supplied-fp", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	snapshot, err := function.CaptureSourceSnapshot(mustSelect(t, dir))
	if err != nil {
		t.Fatalf("capture reference snapshot: %v", err)
	}
	want := snapshot.Fingerprint()
	t.Cleanup(snapshot.Discard)

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
	if got.Fingerprint == supplied {
		t.Fatalf("fingerprint = %q, must not be the caller's supplied value (the build stages the snapshot)", got.Fingerprint)
	}
	if got.Fingerprint != want {
		t.Fatalf("fingerprint = %q, want the snapshot-derived %q", got.Fingerprint, want)
	}
	if want := ImageRef(fn.Name, want); got.Image != want {
		t.Fatalf("image = %q, want %q (tag must be the snapshot fingerprint)", got.Image, want)
	}
}

// mustSelect resolves a source selection for dir, failing the test on error.
func mustSelect(t *testing.T, dir string) *source.Selection {
	t.Helper()
	selection, err := source.ForDir(dir)
	if err != nil {
		t.Fatalf("select %q: %v", dir, err)
	}
	return selection
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
		t.Fatalf("fingerprint = %q, want the supplied template-only %q (no tree is walked)", got.Fingerprint, supplied)
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

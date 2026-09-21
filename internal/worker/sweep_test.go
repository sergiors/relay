package worker

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/function"
	"relay/internal/runtime"
)

// TestStartupImageKeepSet pins the startup sweep's keep-set policy without
// Docker: each on-disk function's fingerprinted image, every running service
// container's image, and every recorded last-active image are kept; blanks are
// ignored.
func TestStartupImageKeepSet(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "fn")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fp, err := function.Fingerprint(dir)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wantFn := runtime.ImageRef("fn", fp)

	keep := startupImageKeepSet(
		[]function.Function{{Name: "fn", Dir: dir}},
		[]string{"svc-img", ""},
		[]string{"recorded-img", ""},
	)

	for _, want := range []string{wantFn, "svc-img", "recorded-img"} {
		if !keep[want] {
			t.Errorf("keep set missing %q: %v", want, keep)
		}
	}
	if keep[""] {
		t.Errorf("keep set must not contain a blank image: %v", keep)
	}
	if len(keep) != 3 {
		t.Errorf("keep set = %v, want exactly 3 entries", keep)
	}
}

// TestStartupImageKeepSetFingerprintErrorSkipsFunction verifies a function whose
// directory cannot be fingerprinted contributes no keep entry (the sweep falls
// back to the service/recorded images only).
func TestStartupImageKeepSetFingerprintErrorSkipsFunction(t *testing.T) {
	keep := startupImageKeepSet(
		[]function.Function{{Name: "missing", Dir: filepath.Join(t.TempDir(), "nope")}},
		nil,
		nil,
	)
	if len(keep) != 0 {
		t.Fatalf("keep set = %v, want empty for an unfingerprintable function", keep)
	}
}

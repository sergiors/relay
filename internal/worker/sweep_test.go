package worker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestStartupImageKeepSetRetainsRecordedImageAfterRediscovery is the startup
// keep-set regression for the rediscovery-preservation fix: after a successful
// generation is recorded and the function is rediscovered at startup, the
// recorded last-active image must still be gathered (Detail.Image survives
// discovery) and kept, so the startup sweep never deletes the image a crashed
// swap may still be serving. It mirrors the gather in sweepStartupImages using a
// real state handle.
func TestStartupImageKeepSetRetainsRecordedImageAfterRediscovery(t *testing.T) {
	st := openTempState(t)
	root := t.TempDir()
	dir := filepath.Join(root, "fn")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	fn := stateFunction("fn", dir)

	// A prior process recorded an active generation; startup rediscovery then
	// refreshes the desired fingerprint but must preserve the active image.
	st.RecordReconcileSuccess(fn.Name, "recorded-active-img", "fp-active", time.Now(), fn)
	st.RecordDiscovered(fn)

	detail, ok := st.GetFunction(fn.Name)
	if !ok {
		t.Fatal("expected state row after rediscovery")
	}
	if detail.Image != "recorded-active-img" {
		t.Fatalf("recorded image = %q after rediscovery, want preserved recorded-active-img", detail.Image)
	}

	// The production gather (sweepStartupImages) collects Detail.Image for each
	// function still on disk; feed it to the pure keep-set policy.
	var recordedImages []string
	if detail.Image != "" {
		recordedImages = append(recordedImages, detail.Image)
	}
	keep := startupImageKeepSet([]function.Function{fn}, nil, recordedImages)
	if !keep["recorded-active-img"] {
		t.Fatalf("keep set = %v, want the preserved recorded image retained", keep)
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

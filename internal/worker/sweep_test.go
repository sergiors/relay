package worker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestStartupImageKeepSet pins the startup sweep's keep-set policy without
// Docker: the exact image each app was prepared with this boot, every
// running service container's image, and every recorded last-active image are
// kept; blanks are ignored. It takes prepared refs verbatim, so no source tree
// is re-hashed to reconstruct an expected tag.
func TestStartupImageKeepSet(t *testing.T) {
	// An image ref that is deliberately NOT derivable by hashing a tree: the
	// keep-set must retain exactly what it is given rather than recomputing a
	// tag.
	const preparedImg = "relay-app-fn:deadbeefdeadbeef"
	keep := startupImageKeepSet(
		[]string{preparedImg, ""},
		[]string{"svc-img", ""},
		[]string{"recorded-img", ""},
	)

	for _, want := range []string{preparedImg, "svc-img", "recorded-img"} {
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

// TestStartupImageKeepSetDoesNotRehashSource is the regression for the
// startup-sweep optimization: the keep-set takes the ACTUAL prepared image
// references and never touches the filesystem, so it stays correct even for a
// app whose directory has since vanished. An app that failed to build
// contributes no prepared image (its still-serving version is covered by the
// recorded state image instead).
func TestStartupImageKeepSetDoesNotRehashSource(t *testing.T) {
	keep := startupImageKeepSet(
		[]string{"relay-app-gone:0123456789abcdef"},
		nil,
		[]string{"relay-app-gone:previousserving"},
	)
	if !keep["relay-app-gone:0123456789abcdef"] {
		t.Fatalf("prepared image not kept: %v", keep)
	}
	if !keep["relay-app-gone:previousserving"] {
		t.Fatalf("recorded image not kept: %v", keep)
	}
	if len(keep) != 2 {
		t.Fatalf("keep set = %v, want exactly the two supplied refs", keep)
	}
}

// TestStartupImageKeepSetRetainsRecordedImageAfterRediscovery is the startup
// keep-set regression for the rediscovery-preservation fix: after a successful
// generation is recorded and the app is rediscovered at startup, the
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
	fn := stateApp("fn", dir)

	// A prior process recorded an active generation; startup rediscovery then
	// refreshes the desired fingerprint but must preserve the active image.
	st.RecordReconcileSuccess(fn.Name, "recorded-active-img", "fp-active", time.Now(), fn)
	st.RecordDiscovered(fn)

	detail, ok := st.GetApp(fn.Name)
	if !ok {
		t.Fatal("expected state row after rediscovery")
	}
	if detail.Image != "recorded-active-img" {
		t.Fatalf("recorded image = %q after rediscovery, want preserved recorded-active-img", detail.Image)
	}

	// The production gather (sweepStartupImages) collects Detail.Image for each
	// app still on disk; feed it to the pure keep-set policy. The
	// rediscovered app was not prepared this boot, so it contributes no
	// prepared image and only the recorded active image is kept.
	var recordedImages []string
	if detail.Image != "" {
		recordedImages = append(recordedImages, detail.Image)
	}
	keep := startupImageKeepSet(nil, nil, recordedImages)
	if !keep["recorded-active-img"] {
		t.Fatalf("keep set = %v, want the preserved recorded image retained", keep)
	}
}

// TestStartupImageKeepSetEmptyInputs verifies an unavailable/no-runtime boot
// with no services and no recorded state contributes no keep entry.
func TestStartupImageKeepSetEmptyInputs(t *testing.T) {
	keep := startupImageKeepSet(nil, nil, nil)
	if len(keep) != 0 {
		t.Fatalf("keep set = %v, want empty", keep)
	}
}

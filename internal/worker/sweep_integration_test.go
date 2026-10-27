//go:build integration

package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/testutil"
)

// TestStartupImageKeepSetRetainsRecordedImageAfterRediscovery is the startup
// keep-set regression for the rediscovery-preservation fix: after a successful
// generation is recorded and the app is rediscovered at startup, the
// recorded last-active image must still be gathered (Detail.Image survives
// discovery) and kept, so the startup sweep never deletes the image a crashed
// swap may still be serving. It mirrors the gather in sweepStartupImages using a
// real state handle.
func TestStartupImageKeepSetRetainsRecordedImageAfterRediscovery(t *testing.T) {
	testutil.RequireStateDatabase(t)
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
	fencingToken := seedFencingToken(t, st, fn)
	if _, err := st.RecordReconcileSuccess(context.Background(), fn.Name, "recorded-active-img", "fp-active", time.Now(), fn, fencingToken); err != nil {
		t.Errorf("state write error: %v", err)
	}
	if _, err := st.RecordDiscovered(context.Background(), fn); err != nil {
		t.Errorf("state write error: %v", err)
	}

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

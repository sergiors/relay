package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/testutil"
)

// TestSeedSuppliedFingerprintSkipsRebuild pins the supplied-seed contract: Seed
// stores the caller's fingerprint verbatim, so an app whose content still
// matches that value skips the first reconcile instead of rebuilding. The
// reconciler still rescans on reconcile (change detection is authoritative
// there); Seed itself performs no scan.
func TestSeedSuppliedFingerprintSkipsRebuild(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "seeded")

	fn := initialFn("seeded", dir)
	fp, err := app.FingerprintApp(dir, fn.App().Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	b := &fakeBuilder{}
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{fn})
	r := New(Config{Root: root, Debounce: time.Millisecond, Interval: time.Hour}, reg, b, testutil.DiscardLogger())
	r.Seed(fn.App(), fp)

	r.reconcileApp("seeded")

	if b.prepares() != 0 {
		t.Fatalf("prepare count = %d, want 0 for a matching supplied seed", b.prepares())
	}
}

// TestSeedStaleFingerprintForcesRebuild pins the safety direction of the
// supplied seed: an OLDER (stale) fingerprint seeded before the watcher was
// established does not suppress a rebuild. The first reconcile rescans, sees the
// change, and rebuilds. This is the change-detection race the seed must not
// silently trust away.
func TestSeedStaleFingerprintForcesRebuild(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stale")

	fn := initialFn("stale", dir)
	b := &fakeBuilder{}
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{fn})
	r := New(Config{Root: root, Debounce: time.Millisecond, Interval: time.Hour}, reg, b, testutil.DiscardLogger())

	// Seed the fingerprint of the ORIGINAL content, then change the content as
	// if the edit landed between the caller's scan and the watcher.
	orig, err := app.FingerprintApp(dir, fn.App().Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	r.Seed(fn.App(), orig)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	r.reconcileApp("stale")

	if b.prepares() != 1 {
		t.Fatalf("prepare count = %d, want 1 (a stale seed must not suppress the rebuild)", b.prepares())
	}
}

// TestPrepareWatchEstablishesWatchesSynchronouslyAndStartReuses pins the
// ordering seam the worker relies on: PrepareWatch installs the recursive
// watches before returning (no goroutine timing), and Start reuses that same
// watcher instead of installing a second one.
func TestPrepareWatchEstablishesWatchesSynchronouslyAndStartReuses(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "watched")
	nested := filepath.Join(dir, "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.PrepareWatch(ctx); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	// Synchronously watched: no polling needed.
	if !allWatched(r, []string{root, dir, nested}) {
		t.Fatalf("PrepareWatch did not install the recursive watches synchronously")
	}
	preparedWatcher := r.w
	if preparedWatcher == nil {
		t.Fatal("PrepareWatch left the watcher nil")
	}

	go r.Start(ctx)
	// Start must reuse the prepared watcher, not replace it.
	if r.w != preparedWatcher {
		t.Fatal("Start replaced the watcher PrepareWatch established; it must reuse it")
	}
}

// TestPrepareWatchThenSeedDetectsSubsequentChange is the end-to-end race proof:
// the watcher is established BEFORE the supplied fingerprint is seeded, so a
// file change after seeding is still delivered as an fsnotify event and
// reconciled.
func TestPrepareWatchThenSeedDetectsSubsequentChange(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "raced")

	fn := initialFn("raced", dir)
	fp, err := app.FingerprintApp(dir, fn.App().Template)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	b := &fakeBuilder{}
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{fn})
	r := New(Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour}, reg, b, testutil.DiscardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Worker ordering: watch first, then seed, then start.
	if err := r.PrepareWatch(ctx); err != nil {
		t.Fatalf("prepare watch: %v", err)
	}
	r.Seed(fn.App(), fp)
	go r.Start(ctx)

	// A change after the seed must reach the reconciler through the watcher.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for b.prepares() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.prepares() < 1 {
		t.Fatalf("change after PrepareWatch+Seed was not reconciled: %d prepares", b.prepares())
	}
}

// TestSeedDoesNotScanSourceTree pins the optimization itself: Seed must store
// the supplied fingerprint verbatim without touching the filesystem. A
// non-existent directory would make any internal FingerprintApp call fail
// silently (leaving no entry); the entry must instead exist with the supplied
// value.
func TestSeedDoesNotScanSourceTree(t *testing.T) {
	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, t.TempDir(), b, nil, nil)

	fn := app.App{
		Name:     "absent",
		Dir:      filepath.Join(t.TempDir(), "does-not-exist"),
		Template: mustParse(template),
	}
	r.Seed(fn, "supplied-verbatim")

	r.mu.Lock()
	got, ok := r.fingerprints[fn.Name]
	r.mu.Unlock()
	if !ok || got != "supplied-verbatim" {
		t.Fatalf("seed = (%q, %v), want the supplied value stored without a scan", got, ok)
	}
}

// TestSeedEmptyFingerprintStillRebuilds pins that an empty supplied fingerprint
// (an app the startup scan could not hash) is never treated as "unchanged":
// the first reconcile sees the mismatch and rebuilds.
func TestSeedEmptyFingerprintStillRebuilds(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "nofp")

	fn := initialFn("nofp", dir)
	b := &fakeBuilder{}
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{fn})
	r := New(Config{Root: root, Debounce: time.Millisecond, Interval: time.Hour}, reg, b, testutil.DiscardLogger())
	r.Seed(fn.App(), "")

	r.reconcileApp("nofp")

	if b.prepares() != 1 {
		t.Fatalf("prepare count = %d, want 1 for an empty supplied fingerprint", b.prepares())
	}
}

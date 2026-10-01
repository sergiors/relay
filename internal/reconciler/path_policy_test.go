package reconciler

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/runner"
)

// TestReloadRejectsSymlinkedAppDirRetainsLoaded pins the shared path policy
// on the live/periodic reload path: an app directory replaced by a symlink
// (including one pointing outside the root at a directory holding a valid
// template) is never loaded, and a previously-loaded healthy app is
// RETAINED rather than removed or replaced.
func TestReloadRejectsSymlinkedAppDirRetainsLoaded(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "linked")

	fn := initialFn("linked", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// Replace the real directory with a symlink to an OUTSIDE directory whose
	// template/source differ (so a followed link would rebuild/replace).
	outside := t.TempDir()
	writeFnDir(t, outside, "outside-target")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "outside-target"), dir); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	r.reconcileApp("linked")

	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (a symlinked path must never be loaded)", b.prepares())
	}
	if pf := reg.GetByName("linked"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the previously-loaded function must be retained, not removed")
	}
}

// TestReloadInvalidNameRetainsLoaded pins that a reload of an illegal name
// (never a valid app directory) retains a previously-loaded entry rather
// than dropping it.
func TestReloadInvalidNameRetainsLoaded(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "good")

	fn := initialFn("good", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// A name with a path separator / illegal characters must not resolve to
	// anything loadable.
	r.reconcileApp("../good")
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0", b.prepares())
	}
	if pf := reg.GetByName("good"); pf == nil || pf.Prepared() == nil {
		t.Fatal("an invalid-name reload must not drop the loaded function")
	}
}

// TestPeriodicReconcileSkipsSymlinkedDirAndKeepsHealthy pins that the periodic
// backstop (reconcileAll) does not remove or replace a healthy app whose
// directory has been replaced by a symlink, matching the live-event path. A
// real sibling whose content changed is dispatched AFTER the symlinked name
// (sorted entry order), so waiting for the sibling's prepare proves the pump
// processed past the symlinked entry.
func TestPeriodicReconcileSkipsSymlinkedDirAndKeepsHealthy(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")
	validDir := writeFnDir(t, root, "zzz-valid")

	stable := initialFn("stable", dir)
	valid := initialFn("zzz-valid", validDir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{stable, valid}, nil)

	outside := t.TempDir()
	writeFnDir(t, outside, "target")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "target"), dir); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	// Change the valid sibling so it must be rebuilt, proving the pump ran.
	if err := os.WriteFile(filepath.Join(validDir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	go r.pump()
	r.reconcileAll()
	waitForPrepare(t, b, 1)

	if pf := reg.GetByName("stable"); pf == nil || pf.Prepared() == nil {
		t.Fatal("periodic reconcile must retain the healthy symlinked-over function")
	}
	if pf := reg.GetByName("zzz-valid"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the valid sibling must remain prepared")
	}
}

// TestReloadValidDirectChildStillWorks pins that the stricter path policy does
// not break the normal reload path: a valid direct-child directory with changed
// content is still rebuilt.
func TestReloadValidDirectChildStillWorks(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "normal")

	fn := initialFn("normal", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("normal")

	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want 1 for a changed valid function", b.prepares())
	}
	if pf := reg.GetByName("normal"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the valid function must remain prepared")
	}
}

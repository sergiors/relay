package reconciler

import (
	"os"
	"path/filepath"
	"testing"

	"relay/internal/runner"
	"relay/internal/testutil"
)

// TestReloadSymlinkedAppDirRemovesKnown pins the live reload path: an app
// directory replaced by a symlink (including one pointing outside the root at a
// directory holding a valid template) is an unsupported root entry, not an app
// candidate, so a previously-known app takes the NORMAL REMOVAL path — its
// registry entry is dropped and nothing is loaded, replaced, or built.
func TestReloadSymlinkedAppDirRemovesKnown(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "linked")

	fn := initialFn("linked", dir)
	var removeApps []string
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
		cfg.RemoveApp = func(name string) { removeApps = append(removeApps, name) }
	})

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
	if pf := reg.GetByName("linked"); pf != nil {
		t.Fatalf("a known app behind a symlinked directory must be removed, registry = %+v", pf)
	}
	if len(removeApps) != 1 || removeApps[0] != "linked" {
		t.Fatalf("RemoveApp calls = %v, want one normal removal", removeApps)
	}
}

func TestReloadRejectsSymlinkedTemplateRetainsLoaded(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "linked-template")
	fn := initialFn("linked-template", dir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	targetDir := t.TempDir()
	target := filepath.Join(targetDir, "template.yaml")
	if err := os.WriteFile(target, []byte("runtime: node24\nevents:\n  - handler: index.other\n    pattern: {event_name: [OTHER]}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	template := filepath.Join(dir, "template.yaml")
	if err := os.Remove(template); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, template); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	r.reconcileApp("linked-template")

	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (a symlinked template must never be read)", b.prepares())
	}
	if pf := reg.GetByName("linked-template"); pf == nil || pf.Prepared() == nil {
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

// TestPeriodicReconcileRemovesKnownOverSymlinkedDirAndKeepsHealthy pins the
// periodic backstop: a known app whose directory was replaced by a symlink is
// an unsupported root entry and takes the normal removal path (registry entry
// dropped), while the valid sibling whose real directory content changed is
// still rebuilt — sorted so it is dispatched AFTER the symlinked name, proving
// the pump processed past it.
func TestPeriodicReconcileRemovesKnownOverSymlinkedDirAndKeepsHealthy(t *testing.T) {
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

	if pf := reg.GetByName("stable"); pf != nil {
		t.Fatalf("periodic reconcile must remove the known app behind a symlinked dir, registry = %+v", pf)
	}
	if pf := reg.GetByName("zzz-valid"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the valid sibling must remain prepared")
	}
}

// TestReloadUnsupportedRootEntryRemovesKnown pins the live reload path against
// root entries that real os-level filesystem inspection (os.Lstat) rejects
// before any template read: a real app directory replaced by a plain FILE, or
// by a symlink whose target has vanished (a dangling link), is an unsupported
// root entry, not an app candidate. A known name takes the NORMAL removal path
// — its registry entry is dropped, nothing is loaded, and no build runs.
func TestReloadUnsupportedRootEntryRemovesKnown(t *testing.T) {
	t.Run("replaced by a plain file", func(t *testing.T) {
		root := t.TempDir()
		dir := writeFnDir(t, root, "now-a-file")
		fn := initialFn("now-a-file", dir)
		var removeApps []string
		b := &fakeBuilder{}
		r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config) {
			cfg.RemoveApp = func(name string) { removeApps = append(removeApps, name) }
		})

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("remove dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "now-a-file"), []byte("not an app\n"), 0o644); err != nil {
			t.Fatalf("write file over app dir: %v", err)
		}

		r.reconcileApp("now-a-file")

		if b.prepares() != 0 {
			t.Fatalf("prepares = %d, want 0 (a file is never loaded)", b.prepares())
		}
		if pf := reg.GetByName("now-a-file"); pf != nil {
			t.Fatalf("a known app replaced by a plain file must be removed, registry = %+v", pf)
		}
		if len(removeApps) != 1 || removeApps[0] != "now-a-file" {
			t.Fatalf("RemoveApp calls = %v, want one normal removal", removeApps)
		}
	})

	t.Run("replaced by a dangling symlink", func(t *testing.T) {
		root := t.TempDir()
		dir := writeFnDir(t, root, "dangling")
		fn := initialFn("dangling", dir)
		b := &fakeBuilder{}
		r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("remove dir: %v", err)
		}
		if err := os.Symlink(filepath.Join(root, "vanished-target"), filepath.Join(root, "dangling")); err != nil {
			t.Skipf("symlink unsupported: %v", err)
		}

		r.reconcileApp("dangling")

		if b.prepares() != 0 {
			t.Fatalf("prepares = %d, want 0 (a dangling link is never followed)", b.prepares())
		}
		if pf := reg.GetByName("dangling"); pf != nil {
			t.Fatalf("a known app replaced by a dangling symlink must be removed, registry = %+v", pf)
		}
	})
}

// TestPeriodicReconcileUnsupportedEntriesRemoveKnownApps pins the periodic
// backstop against real os-level unsupported entries: known apps whose
// directories became a plain file or a dangling symlink take the NORMAL removal
// path (registry entries dropped), an unknown non-app file is never dispatched
// at all (it stays out of the registry), and the valid sibling (whose real
// directory content changed) is still rebuilt — proving the pump ran past every
// unsupported entry.
func TestPeriodicReconcileUnsupportedEntriesRemoveKnownApps(t *testing.T) {
	root := t.TempDir()
	knownFileDir := writeFnDir(t, root, "known-now-file")
	danglingDir := writeFnDir(t, root, "known-dangling")
	validDir := writeFnDir(t, root, "zzz-valid")

	knownFile := initialFn("known-now-file", knownFileDir)
	dangling := initialFn("known-dangling", danglingDir)
	valid := initialFn("zzz-valid", validDir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{knownFile, dangling, valid}, nil)

	if err := os.RemoveAll(knownFileDir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "known-now-file"), []byte("not an app\n"), 0o644); err != nil {
		t.Fatalf("write file over app dir: %v", err)
	}
	if err := os.RemoveAll(danglingDir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "vanished-target"), filepath.Join(root, "known-dangling")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "stranger"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write unknown file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(validDir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	go r.pump()
	r.reconcileAll()
	waitForPrepare(t, b, 1)

	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want only the valid sibling's rebuild", b.prepares())
	}
	for _, name := range []string{"known-now-file", "known-dangling"} {
		if pf := reg.GetByName(name); pf != nil {
			t.Errorf("periodic pass must remove known app %q over an unsupported entry, registry = %+v", name, pf)
		}
	}
	if reg.GetByName("stranger") != nil {
		t.Fatal("an unknown non-app file must never be dispatched for reconcile")
	}
}

// TestShouldEnqueueUnsupportedEntries pins the watcher's debounce filter for
// root entries that can never be app candidates: an unknown plain file, a
// dangling symlink, or a symlinked directory never arms a timer (no bogus
// reconcile churn), while the SAME unsupported entries enqueue once the name is
// already known, so the removal is observed.
func TestShouldEnqueueUnsupportedEntries(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(filepath.Join(root, "aliased-inner"), filepath.Join(root, "aliased")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "vanished-target"), filepath.Join(root, "dangling")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "stranger"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write stranger file: %v", err)
	}

	// Unknown names: no enqueue at all.
	rUnknown := New(Config{Root: root}, &runner.Registry{}, nil, testutil.DiscardLogger())
	for _, name := range []string{"stranger", "dangling", "aliased"} {
		if rUnknown.shouldEnqueue(name) {
			t.Errorf("an unknown non-app root entry %q must not enqueue a reconcile", name)
		}
	}

	// Already-known names: enqueue so the removal is observed.
	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{
		initialFn("stranger", filepath.Join(root, "stranger")),
		initialFn("dangling", filepath.Join(root, "dangling")),
		initialFn("aliased", filepath.Join(root, "aliased")),
	})
	rKnown := New(Config{Root: root}, reg, nil, testutil.DiscardLogger())
	for _, name := range []string{"stranger", "dangling", "aliased"} {
		if !rKnown.shouldEnqueue(name) {
			t.Errorf("a known name over an unsupported root entry %q must enqueue so the removal is observed", name)
		}
	}
}

// TestPeriodicReconcileKeepsAppWhenTemplateIsSymlinked pins the periodic
// backstop on the template-entry policy: with the app directory still real but
// template.yaml replaced by a symlink, HasTemplate reports a present invalid
// candidate, so the known app is dispatched and RETAINED by the single-app path
// (never removed), while a changed valid sibling is rebuilt and proves the
// pump ran.
func TestPeriodicReconcileKeepsAppWhenTemplateIsSymlinked(t *testing.T) {
	root := t.TempDir()
	guardedDir := writeFnDir(t, root, "guarded")
	validDir := writeFnDir(t, root, "zzz-valid")

	guarded := initialFn("guarded", guardedDir)
	valid := initialFn("zzz-valid", validDir)
	b := &fakeBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{guarded, valid}, nil)

	targetDir := t.TempDir()
	target := filepath.Join(targetDir, "template.yaml")
	if err := os.WriteFile(target, []byte("runtime: node24\nevents:\n  - handler: index.other\n    pattern: {event_name: [OTHER]}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(guardedDir, "template.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(guardedDir, "template.yaml")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.WriteFile(filepath.Join(validDir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}

	go r.pump()
	r.reconcileAll()
	waitForPrepare(t, b, 1)

	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want only the changed sibling", b.prepares())
	}
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a real app directory whose template.yaml is a symlink must keep serving its active version")
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

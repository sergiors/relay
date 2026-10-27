//go:build integration

package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestReconcileUnsupportedEntryRemovesStateRow pins that an unsupported root
// entry over a known app is a NORMAL removal for the persisted state view too:
// replacing a live app directory with a plain file deletes the app's row (the
// documented removal path) instead of retaining it as a degraded invalid desired
// state.
func TestReconcileUnsupportedEntryRemovesStateRow(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "filed-over")

	fn := initialFn("filed-over", dir)
	b := &fakeBuilder{}
	var removeApps []string
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
		cfg.RemoveApp = func(name string) { removeApps = append(removeApps, name) }
	})
	fencingToken := recordSuccess(t, st, "filed-over", "img-old", "fp-old", time.Now(), fn.App())
	r.SeedFencingToken("filed-over", fencingToken)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "filed-over"), []byte("not an app\n"), 0o644); err != nil {
		t.Fatalf("write file over app dir: %v", err)
	}

	r.reconcileApp("filed-over")

	if _, ok := st.GetApp("filed-over"); ok {
		t.Fatal("an unsupported entry over a known app must delete its state row (normal removal), not record an invalid desired view")
	}
	if pf := reg.GetByName("filed-over"); pf != nil {
		t.Fatalf("registry must drop the removed app, got %+v", pf)
	}
	if len(removeApps) != 1 || removeApps[0] != "filed-over" {
		t.Fatalf("RemoveApp calls = %v, want one normal removal", removeApps)
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (a removal never builds)", b.prepares())
	}
}

// TestReconcileUnknownUnsupportedEntryDoesNothing pins that reconciliation of a
// name that was never an app and whose root entry cannot be an app candidate
// (a plain file, a dangling link, or an absent path) is a no-op: no registry
// entry, no state row, no invalid desired write, no build.
func TestReconcileUnknownUnsupportedEntryDoesNothing(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "stranger"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write stranger file: %v", err)
	}
	if err := os.Symlink(filepath.Join(root, "vanished-target"), filepath.Join(root, "dangling")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)

	r.reconcileApp("stranger")
	r.reconcileApp("dangling")
	r.reconcileApp("never-existed")

	for _, name := range []string{"stranger", "dangling", "never-existed"} {
		if pf := reg.GetByName(name); pf != nil {
			t.Errorf("an unknown unsupported entry %q must never enter the registry: %+v", name, pf)
		}
		if _, ok := st.GetApp(name); ok {
			t.Errorf("an unknown unsupported entry %q must not create a state row", name)
		}
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0", b.prepares())
	}
}

// TestWatcherIgnoresUnknownFileEvents pins the live watcher path end to end:
// unknown plain-file and dangling-symlink events directly under the apps root
// never enqueue a reconcile, touch the registry, or create a state row. Ordering
// is event-driven, not sleep-based: once a LATER valid app on the same root
// watch has reconciled, the earlier unsupported-entry events have necessarily
// been observed, so the absence assertions are deterministic.
func TestWatcherIgnoresUnknownFileEvents(t *testing.T) {
	root := t.TempDir()
	realDir := writeFnDir(t, root, "real")

	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b,
		[]*runner.PreparedApp{initialFn("real", realDir)}, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)
	waitAllWatched(t, r, []string{root})

	// Unsupported root entry created while the watcher is live. (A dangling
	// symlink is deliberately not exercised here: on macOS its creation makes
	// fsnotify stop delivering further events on the same watch, so the
	// sentinel-based observation below could never be deterministic. The
	// dangling-link filtering is pinned by TestShouldEnqueueUnsupportedEntries
	// and the reconcile-level tests.)
	if err := os.WriteFile(filepath.Join(root, "stranger"), []byte("noise\n"), 0o644); err != nil {
		t.Fatalf("write stranger file: %v", err)
	}
	// A sentinel valid app created AFTER the unsupported entry on the same
	// root watch: once it reconciles, eventLoop has already observed it.
	writeFnDir(t, root, "zzz-sentinel")

	waitForPrepare(t, b, 1)

	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want exactly 1 (the sentinel; unsupported entries must not reconcile)", b.prepares())
	}
	for _, name := range []string{"stranger"} {
		if reg.GetByName(name) != nil {
			t.Errorf("an unknown unsupported entry %q must never enter the registry", name)
		}
		if _, ok := st.GetApp(name); ok {
			t.Errorf("an unknown unsupported entry %q must never create a state row", name)
		}
	}
}

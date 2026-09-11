package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Runs the full reconciler (real fsnotify) with a tiny debounce, enqueues an
// event, cancels mid-debounce, then waits beyond the debounce window. Under
// -race this would surface the old send-on-closed-channel panic; the
// done-channel dispatch must swallow the timer fire and the process must not
// crash. The timer is fired right at the teardown boundary: enqueue, then
// cancel, then sleep past the debounce.
func TestShutdownNoSendOnClosedChannel(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "race-me")

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go r.Start(ctx)

	// Wait (bounded) for the watcher to install the root watch rather than a
	// fixed sleep, so the debounce timer below is armed against a live watcher.
	waitAllWatched(t, r, []string{root})

	// Enqueue so a debounce timer is armed, then cancel immediately (mid-debounce).
	r.Enqueue("race-me")
	cancel()

	// Sleep well beyond the debounce so the pending timer, if it were to fire,
	// must cross the done/close boundary.
	time.Sleep(15 * time.Millisecond)

	// If we got here without a panic, the dispatch path is safe on shutdown.
}

// Renaming a parent directory clears the stale watch handles beneath the old
// path (a rename is a removal for the old path). It uses a real fsnotify watcher
// so we assert the watches map reflects exactly what the OS watcher holds.
func TestWatchCleanupOnRenameRemovesDescendants(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "a") // root/a
	if err := os.MkdirAll(filepath.Join(root, "a", "b", "c"), 0o755); err != nil {
		t.Fatalf("mkdir a/b/c: %v", err)
	}

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)

	oldSub := func() []string {
		return []string{
			filepath.Join(root, "a"),
			filepath.Join(root, "a", "b"),
			filepath.Join(root, "a", "b", "c"),
		}
	}

	// Wait until the full subtree is watched.
	waitAllWatched(t, r, oldSub())

	// Rename the top-level function directory -> its watch + all descendants
	// become stale.
	newName := filepath.Join(root, "renamed")
	if err := os.Rename(filepath.Join(root, "a"), newName); err != nil {
		t.Fatalf("rename parent: %v", err)
	}

	// The old subtree must be gone (resource hygiene). Poll because rename
	// handling is asynchronous through the eventLoop.
	waitNoWatched(t, r, oldSub())
}

// A recreated directory under the renamed path gets re-watched (Create event ->
// addWatchRecursive).
func TestWatchRecleanupOnRecreate(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "a")
	if err := os.MkdirAll(filepath.Join(root, "a", "b"), 0o755); err != nil {
		t.Fatalf("mkdir a/b: %v", err)
	}
	old := filepath.Join(root, "a", "b")

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)

	waitAllWatched(t, r, []string{old})

	// Remove the subtree entirely, then recreate it as a new dir. It must be
	// re-watched after the Create event arrives.
	if err := os.RemoveAll(old); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	// Wait for cleanup to observe removal.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := r.watches.Load(old); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch for removed dir never cleaned up")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := os.MkdirAll(old, 0o755); err != nil {
		t.Fatalf("recreate dir: %v", err)
	}
	waitAllWatched(t, r, []string{old})
	if _, ok := r.watches.Load(old); !ok {
		t.Fatal("recreated directory was not re-watched")
	}
}

// A symlink loop inside the functions tree cannot hang addWatchRecursive:
// WalkDir does not follow links, so the walk terminates and, per policy, the
// symlinked dir is simply not watched.
func TestAddWatchRecursiveSymlinkLoopBounded(t *testing.T) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatalf("fsnotify watcher: %v", err)
	}
	defer w.Close()

	r := &Reconciler{w: w, watches: sync.Map{}}

	root := t.TempDir()
	// Create a nested dir and a self-referential symlink loop a/loop -> a.
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o755); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	loop := filepath.Join(root, "a", "loop")
	if err := os.Symlink(filepath.Join(root, "a"), loop); err != nil {
		t.Fatalf("symlink loop: %v", err)
	}

	done := make(chan struct{})
	go func() {
		r.addWatchRecursive(root)
		close(done)
	}()

	select {
	case <-done:
		// Bounded: returned promptly despite the loop.
	case <-time.After(3 * time.Second):
		t.Fatal("addWatchRecursive hung on a symlink loop")
	}

	// The symlinked dir must not be watched (WalkDir treats it as a file).
	if _, ok := r.watches.Load(loop); ok {
		t.Fatal("symlinked directory should not be added as a watch")
	}
}

// --- helpers ---

func waitAllWatched(t *testing.T, r *Reconciler, paths []string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if allWatched(r, paths) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for watches on %v", paths)
}

func waitNoWatched(t *testing.T, r *Reconciler, paths []string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if noneWatched(r, paths) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for watches on %v to be released", paths)
}

func allWatched(r *Reconciler, paths []string) bool {
	for _, p := range paths {
		if _, ok := r.watches.Load(p); !ok {
			return false
		}
	}
	return true
}

func noneWatched(r *Reconciler, paths []string) bool {
	for _, p := range paths {
		if _, ok := r.watches.Load(p); ok {
			return false
		}
	}
	return true
}

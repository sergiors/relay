package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/runner"
)

// TestDebounceCoalescesRapidEvents: many quick Enqueue calls for the same
// function should fire exactly one reconcile (the pump consumes the debounced
// name once). Distinct functions queue independently.
func TestDebounceCoalescesRapidEvents(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "coalesced")

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil)

	// Seed a fingerprint so the very first reconcile for the pre-written dir
	// would be skipped (it was never built though). Instead we drive reconcile
	// through Enqueue only and count Prepare calls: a single successful prepare
	// per burst proves coalescing.
	r.startDebounceForTest()

	// A burst of rapid events for the same function.
	for i := 0; i < 20; i++ {
		r.Enqueue("coalesced")
	}

	// Give the single debounce timer + pump enough time to run once.
	deadline := time.Now().Add(2 * time.Second)
	for b.prepares() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// Allow a little extra time to catch a spurious second reconcile.
	time.Sleep(150 * time.Millisecond)

	if got := b.prepares(); got != 1 {
		t.Fatalf("expected exactly 1 reconcile from a burst, got %d prepares", got)
	}
}

// TestDebounceDistinctFunctionsIndependent ensures two functions each reconcile
// independently after their own debounce windows.
func TestDebounceDistinctFunctionsIndependent(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "a")
	writeFnDir(t, root, "b")

	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil)
	r.startDebounceForTest()

	r.Enqueue("a")
	r.Enqueue("b")
	r.Enqueue("a")

	deadline := time.Now().Add(2 * time.Second)
	for b.prepares() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	// a: coalesced (2 events -> 1); b: 1. Total exactly 2.
	if got := b.prepares(); got != 2 {
		t.Fatalf("expected exactly 2 reconciles (a coalesced, b once), got %d", got)
	}
}

// TestFunctionForPath covers the event-path-to-function-name mapping.
func TestFunctionForPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "functions")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	r := New(Config{Root: root}, nil, nil, nil)

	cases := []struct {
		path string
		name string
		ok   bool
	}{
		{root + "/user-events/events/created.py", "user-events", true},
		{root + "/welcome/index.js", "welcome", true},
		{root + "/toplevel", "toplevel", true}, // new top-level dir
		{root, "", false},                      // the root itself
		{root + "/", "", false},
	}
	for _, c := range cases {
		name, ok := r.functionForPath(c.path)
		if ok != c.ok || name != c.name {
			t.Errorf("functionForPath(%q) = (%q,%v), want (%q,%v)", c.path, name, ok, c.name, c.ok)
		}
	}
}

// TestWatcherAddsNestedDirWatch uses a real fsnotify watcher on a temp dir to
// verify that creating a nested directory results in a watch that surfaces an
// event for a file written there later (proving recursive watch maintenance).
func TestWatcherAddsNestedDirWatch(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "base")

	// Register base as a healthy, seeded function so an unchanged base would not
	// rebuild; only the nested change forces a rebuild, proving the watcher saw it.
	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, []*runner.PreparedFunction{initialFn("base", dir)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)
	// Give the watcher a moment to set up the root watch.
	time.Sleep(100 * time.Millisecond)

	// Create a new nested subdirectory under a function.
	nested := filepath.Join(root, "base", "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	// Wait until the reconciler has registered a watch on the nested dir (the
	// Create event is buffered by fsnotify and handled asynchronously). Only then
	// do we write, so the write is guaranteed to be observed.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := r.watches.Load(nested); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nested dir watch never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Write a file deep inside; the freshly added watch must see it.
	deepFile := filepath.Join(nested, "data.txt")
	if err := os.WriteFile(deepFile, []byte("content"), 0o644); err != nil {
		t.Fatalf("write deep file: %v", err)
	}

	// The debounced reconcile of base must fire because the fingerprint changed.
	deadline = time.Now().Add(3 * time.Second)
	for b.prepares() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if b.prepares() < 1 {
		t.Fatalf("expected nested file change to trigger a reconcile, got %d prepares", b.prepares())
	}
}

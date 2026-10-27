//go:build integration

package reconciler

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// TestReconcileAppIgnoresStagingDir pins the defensive guard: even if a
// staging name reaches reconcileApp directly (a stale timer or a direct
// caller), it is inert — no prepare, no runtime registry entry, and no state row
// (neither a discovery nor an invalid desired definition). This is what stops a
// live sync's transient ".sync-*" directory from recording degraded/unavailable
// state.
func TestReconcileAppIgnoresStagingDir(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	writeStageDir(t, root, ".sync-123456")

	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b, nil, &logs)

	r.reconcileApp(".sync-123456")

	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 for a staging directory", b.prepares())
	}
	if reg.GetByName(".sync-123456") != nil {
		t.Fatal("a staging directory must never enter the runtime registry")
	}
	if _, ok := st.GetApp(".sync-123456"); ok {
		t.Fatal("a staging directory must never get a state row (not even invalid)")
	}
	if logs := logs.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("a staging directory must not be logged, logs = %q", logs)
	}
}

// TestReconcileAllIgnoresStagingDirKeepsNeighborAndInvalid pins the periodic
// backstop: while a staging directory exists, reconcileAll dispatches the valid
// neighbor (which rebuilds) and a genuinely invalid user directory (which still
// records its invalid desired state), but never the staging directory — no
// registry entry, no state row, no warning.
func TestReconcileAllIgnoresStagingDirKeepsNeighborAndInvalid(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	writeStageDir(t, root, ".sync-abc")
	writeFnDir(t, root, "valid")
	// A genuinely invalid user directory (bad template): must still be recorded.
	badDir := writeFnDir(t, root, "bad-user")
	if err := os.WriteFile(filepath.Join(badDir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("break bad-user template: %v", err)
	}

	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b, nil, &logs)

	go r.pump()
	r.reconcileAll()
	waitForPrepare(t, b, 1)

	if pf := reg.GetByName("valid"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the valid neighbor must still be discovered by the periodic pass")
	}
	if reg.GetByName(".sync-abc") != nil {
		t.Fatal("the staging directory must never be dispatched into the registry")
	}
	if _, ok := st.GetApp(".sync-abc"); ok {
		t.Fatal("the staging directory must never get a state row")
	}
	// The genuinely invalid user directory keeps its invalid desired state.
	if got, ok := st.GetApp("bad-user"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad-user = %+v ok=%v, want unavailable (invalid user behavior preserved)", got, ok)
	}
	if logs := logs.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("a staging directory must not be logged, logs = %q", logs)
	}
}

// TestWatcherIgnoresStagingDirEvents pins the live watcher path end to end with a
// real fsnotify watcher: creating and populating a staging directory directly
// under the root must not install watches, enqueue a reconcile, touch the
// registry, or write any state row. Ordering is event-driven, not sleep-based:
// once a LATER Create on the same root watch (a sentinel valid app) has been
// reconciled, the earlier stage events on that watch have necessarily been
// observed, so the absence assertion is deterministic.
func TestWatcherIgnoresStagingDirEvents(t *testing.T) {
	testutil.RequireStateDatabase(t)
	root := t.TempDir()
	realDir := writeFnDir(t, root, "real")

	// Seed "real" as an already-built, unchanged registry entry so it stays
	// quiet; any prepare counted would be staging-induced.
	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b,
		[]*runner.PreparedApp{initialFn("real", realDir)}, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)
	waitAllWatched(t, r, []string{root})

	// Create and populate a staging directory while the watcher is live.
	writeStageDir(t, root, ".sync-live")
	if err := os.WriteFile(filepath.Join(root, ".sync-live", "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("rewrite stage index: %v", err)
	}
	// A sentinel valid app created AFTER the stage on the same root watch:
	// once it reconciles, eventLoop has already processed the stage's Create.
	writeFnDir(t, root, "zzz-sentinel")

	waitForPrepare(t, b, 1)

	if b.prepares() != 1 {
		t.Fatalf("prepares = %d, want exactly 1 (the sentinel; staging must not reconcile)", b.prepares())
	}
	if _, ok := r.watches.Load(filepath.Join(root, ".sync-live")); ok {
		t.Fatal("a staging directory must never be watched")
	}
	if _, ok := r.watches.Load(filepath.Join(root, ".sync-live", "index.js")); ok {
		t.Fatal("a staging descendant must never be watched")
	}
	if reg.GetByName(".sync-live") != nil {
		t.Fatal("a staging directory must never enter the runtime registry")
	}
	if _, ok := st.GetApp(".sync-live"); ok {
		t.Fatal("a staging directory must never get a state row")
	}
	if logs := logs.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("a staging directory must not be logged, logs = %q", logs)
	}
}

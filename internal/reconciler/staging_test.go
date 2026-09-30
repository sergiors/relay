package reconciler

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"

	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// writeStageDir creates a git staging directory (function.StagingPrefix) directly
// under root with a valid template, mirroring what internal/git/materialize.go
// briefly does via os.MkdirTemp(dst, ".sync-*") while copying a function in.
func writeStageDir(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir stage %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(template), 0o644); err != nil {
		t.Fatalf("write stage template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write stage index: %v", err)
	}
	return dir
}

// newTestStateReconcilerLogging mirrors newTestStateReconciler but routes the
// reconciler's logger through the supplied buffer so a test can assert on the
// absence of staging warnings.
func newTestStateReconcilerLogging(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedFunction,
	logs *testutil.SyncBuffer,
) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour, State: st}
	r := New(cfg, reg, builder, slog.New(slog.NewTextHandler(logs, nil)))
	for _, pf := range initial {
		seedCurrent(r, pf.Function())
		st.RecordDiscovered(pf.Function())
	}
	return r, reg, st
}

// TestFunctionForPathIgnoresStagingDir pins the event-mapping filter: a path
// under a Relay-owned staging directory never maps to a function name, so no
// debounce timer is armed and no reconcile can run for it. Normal paths (nested
// and top-level) are unaffected.
func TestFunctionForPathIgnoresStagingDir(t *testing.T) {
	root := t.TempDir()
	r := New(Config{Root: root}, nil, nil, testutil.DiscardLogger())

	cases := []struct {
		path string
		name string
		ok   bool
	}{
		{root + "/.sync-123456/template.yaml", "", false},
		{root + "/.sync-123456", "", false},
		{root + "/.sync-x/deep/file.js", "", false},
		{root + "/real/index.js", "real", true},
		{root + "/real", "real", true},
	}
	for _, c := range cases {
		name, ok := r.functionForPath(c.path)
		if ok != c.ok || name != c.name {
			t.Errorf("functionForPath(%q) = (%q,%v), want (%q,%v)", c.path, name, ok, c.name, c.ok)
		}
	}
}

// TestReconcileFunctionIgnoresStagingDir pins the defensive guard: even if a
// staging name reaches reconcileFunction directly (a stale timer or a direct
// caller), it is inert — no prepare, no runtime registry entry, and no state row
// (neither a discovery nor an invalid desired definition). This is what stops a
// live sync's transient ".sync-*" directory from recording degraded/unavailable
// state.
func TestReconcileFunctionIgnoresStagingDir(t *testing.T) {
	root := t.TempDir()
	writeStageDir(t, root, ".sync-123456")

	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b, nil, &logs)

	r.reconcileFunction(".sync-123456")

	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 for a staging directory", b.prepares())
	}
	if reg.GetByName(".sync-123456") != nil {
		t.Fatal("a staging directory must never enter the runtime registry")
	}
	if _, ok := st.GetFunction(".sync-123456"); ok {
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
	if _, ok := st.GetFunction(".sync-abc"); ok {
		t.Fatal("the staging directory must never get a state row")
	}
	// The genuinely invalid user directory keeps its invalid desired state.
	if got, ok := st.GetFunction("bad-user"); !ok || got.Status != state.StatusUnavailable {
		t.Fatalf("bad-user = %+v ok=%v, want unavailable (invalid user behavior preserved)", got, ok)
	}
	if logs := logs.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("a staging directory must not be logged, logs = %q", logs)
	}
}

// TestIsReservedDirPath pins the first-segment rule used by watch pruning: only
// a reserved ROOT CHILD (and everything beneath it) is reserved. A nested real
// directory whose own name merely looks reserved is NOT, so a valid function
// named e.g. ".sync-x" inside a real function stays watchable — though such a
// name is itself invalid as a function directory.
func TestIsReservedDirPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "functions")
	r := New(Config{Root: root}, nil, nil, testutil.DiscardLogger())

	cases := []struct {
		path string
		want bool
	}{
		{filepath.Join(root, ".sync-abc"), true},
		{filepath.Join(root, ".sync-abc", "deep"), true},
		{filepath.Join(root, ".sync-abc", "deep", "file.js"), true},
		{filepath.Join(root, "valid"), false},
		{filepath.Join(root, "valid", ".sync-nested"), false},
		{root, false},
	}
	for _, c := range cases {
		if got := r.isReservedDirPath(c.path); got != c.want {
			t.Errorf("isReservedDirPath(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

// TestAddWatchRecursiveSkipsReservedSubtree pins the initial-watch path: a
// pre-existing Relay-owned staging directory under the root and its whole
// subtree are never watched, while the root itself and a valid neighbor (plus
// its descendants) are. This is asserted directly on the watches map (no sleeps,
// no event timing).
func TestAddWatchRecursiveSkipsReservedSubtree(t *testing.T) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatalf("fsnotify watcher: %v", err)
	}
	defer w.Close()

	root := t.TempDir()
	// A reserved root child with a nested descendant, as a mid-copy stage has.
	if err := os.MkdirAll(filepath.Join(root, ".sync-initial", "deep"), 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}
	// A valid neighbor with a nested descendant that MUST be watched.
	if err := os.MkdirAll(filepath.Join(root, "valid", "deep"), 0o755); err != nil {
		t.Fatalf("mkdir valid: %v", err)
	}

	r := &Reconciler{w: w, root: root, watches: sync.Map{}}
	r.addWatchRecursive(root)

	for _, want := range []string{
		root,
		filepath.Join(root, "valid"),
		filepath.Join(root, "valid", "deep"),
	} {
		if _, ok := r.watches.Load(want); !ok {
			t.Errorf("expected a watch on %q", want)
		}
	}
	for _, banned := range []string{
		filepath.Join(root, ".sync-initial"),
		filepath.Join(root, ".sync-initial", "deep"),
	} {
		if _, ok := r.watches.Load(banned); ok {
			t.Errorf("reserved path %q must not be watched", banned)
		}
	}
}

// TestAddWatchRecursiveSkipsReservedSubtreeCalledDirectly pins the dynamic path:
// when addWatchRecursive is invoked on a reserved directory (as the eventLoop
// would for a Create), neither it nor its descendants are watched. The eventLoop
// also short-circuits such paths via isReservedDirPath, so this defends the
// helper itself against a direct call.
func TestAddWatchRecursiveSkipsReservedSubtreeCalledDirectly(t *testing.T) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatalf("fsnotify watcher: %v", err)
	}
	defer w.Close()

	root := t.TempDir()
	stage := filepath.Join(root, ".sync-live")
	if err := os.MkdirAll(filepath.Join(stage, "deep"), 0o755); err != nil {
		t.Fatalf("mkdir stage: %v", err)
	}

	r := &Reconciler{w: w, root: root, watches: sync.Map{}}
	// Watch the root first (as PrepareWatch does), then simulate the dynamic
	// Create-driven call for the stage directory.
	r.addWatchRecursive(root)
	before := watchCount(r)
	r.addWatchRecursive(stage)

	if got := watchCount(r); got != before {
		t.Fatalf("watch count changed from %d to %d: a reserved subtree must add no watches", before, got)
	}
	if _, ok := r.watches.Load(stage); ok {
		t.Errorf("reserved path %q must not be watched", stage)
	}
	if _, ok := r.watches.Load(filepath.Join(stage, "deep")); ok {
		t.Errorf("reserved descendant %q must not be watched", filepath.Join(stage, "deep"))
	}
}

func watchCount(r *Reconciler) int {
	n := 0
	r.watches.Range(func(_, _ any) bool { n++; return true })
	return n
}

// TestWatcherIgnoresStagingDirEvents pins the live watcher path end to end with a
// real fsnotify watcher: creating and populating a staging directory directly
// under the root must not install watches, enqueue a reconcile, touch the
// registry, or write any state row. Ordering is event-driven, not sleep-based:
// once a LATER Create on the same root watch (a sentinel valid function) has been
// reconciled, the earlier stage events on that watch have necessarily been
// observed, so the absence assertion is deterministic.
func TestWatcherIgnoresStagingDirEvents(t *testing.T) {
	root := t.TempDir()
	realDir := writeFnDir(t, root, "real")

	// Seed "real" as an already-built, unchanged registry entry so it stays
	// quiet; any prepare counted would be staging-induced.
	var logs testutil.SyncBuffer
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconcilerLogging(t, root, b,
		[]*runner.PreparedFunction{initialFn("real", realDir)}, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Start(ctx)
	waitAllWatched(t, r, []string{root})

	// Create and populate a staging directory while the watcher is live.
	writeStageDir(t, root, ".sync-live")
	if err := os.WriteFile(filepath.Join(root, ".sync-live", "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("rewrite stage index: %v", err)
	}
	// A sentinel valid function created AFTER the stage on the same root watch:
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
	if _, ok := st.GetFunction(".sync-live"); ok {
		t.Fatal("a staging directory must never get a state row")
	}
	if logs := logs.String(); strings.Contains(logs, ".sync-") {
		t.Fatalf("a staging directory must not be logged, logs = %q", logs)
	}
}

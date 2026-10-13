package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/buildtemp"
	"relay/internal/testutil"
)

// TestSnapshotDependencyInStagesUnderParent proves the dependency snapshot's
// private root is created inside an explicit parent (the runtime Manager's
// build-temp session) rather than directly under os.TempDir, and that release
// still removes it. This is what lets a crashed process's dependency residue be
// reclaimed by the session's scavenger.
func TestSnapshotDependencyInStagesUnderParent(t *testing.T) {
	fnDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(fnDir, "requirements.txt"), []byte("six==1.16.0\n"), 0o644); err != nil {
		t.Fatalf("write requirements: %v", err)
	}
	parent := t.TempDir()

	snap, err := snapshotDependencyIn(parent, fnDir, pythonRequirementsDeps())
	if err != nil {
		t.Fatalf("snapshotDependencyIn: %v", err)
	}
	if filepath.Dir(snap.root) != parent {
		t.Fatalf("dependency root %q is not directly under parent %q", snap.root, parent)
	}
	if !strings.HasPrefix(filepath.Base(snap.root), "relay-dep-build-") {
		t.Fatalf("dependency root %q does not carry the dep-build prefix", filepath.Base(snap.root))
	}
	if _, err := os.Stat(filepath.Join(snap.root, "requirements.txt")); err != nil {
		t.Fatalf("staged manifest missing: %v", err)
	}
	root := snap.root
	snap.release()
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dependency root not removed by release: %v", err)
	}
}

// TestPrepareStagesBuildRootsInsideBuildTempSession proves the Manager's
// build-temp session is the parent of the staged source root, that normal
// per-preparation cleanup still removes it, and that Manager.Close removes the
// session. It attaches a session to a directly-constructed Manager exactly the
// way NewManager does, so the wiring is exercised without a daemon.
func TestPrepareStagesBuildRootsInsideBuildTempSession(t *testing.T) {
	base := t.TempDir()
	t.Setenv("TMPDIR", base)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){}\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	store, err := buildtemp.Open(buildtemp.DefaultRoot())
	if err != nil {
		t.Fatalf("buildtemp.Open: %v", err)
	}
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	seen := false
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{}`,
			onRequest: func(*http.Request) { _, seen = leakedBuildContextTempDir(sess.Dir()) },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.buildTempStore = store
	m.buildTemp = sess

	fn := app.App{Name: "session-build", Dir: dir, Template: &app.Template{Runtime: "node24"}}
	if _, err := m.Prepare(context.Background(), fn); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !seen {
		t.Fatal("no build root existed inside the manager's session at ImageBuild time; the assertion would be vacuous")
	}
	if name, leaked := leakedBuildContextTempDir(sess.Dir()); leaked {
		t.Fatalf("build root %q leaked inside the session after prepare", name)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Lstat(sess.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session dir survived Manager.Close: %v", err)
	}
}

// TestManagerScavengeBuildRootsReclaimsOrphans proves the Manager's startup
// reclaim delegates to the shared root: an abandoned session (a `session-*`
// directory whose owner lock no one holds, as a crash leaves) is removed, while
// the Manager's own live session is not.
func TestManagerScavengeBuildRootsReclaimsOrphans(t *testing.T) {
	base := t.TempDir()
	t.Setenv("TMPDIR", base)

	store, err := buildtemp.Open(buildtemp.DefaultRoot())
	if err != nil {
		t.Fatalf("buildtemp.Open: %v", err)
	}
	// The buildtemp session prefix is "session-"; a directory of that shape with
	// no lock holder is exactly what a crashed process leaves behind.
	orphan := filepath.Join(store.Root(), "session-orphan")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}
	live, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = live.Close(context.Background()) }()

	m := &Manager{log: testutil.DiscardLogger(), containers: newContainerCache()}
	m.buildTempStore = store

	n, err := m.ScavengeBuildRoots(context.Background())
	if err != nil {
		t.Fatalf("ScavengeBuildRoots: %v", err)
	}
	if n != 1 {
		t.Fatalf("ScavengeBuildRoots removed %d, want 1", n)
	}
	if _, err := os.Lstat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan session survived: %v", err)
	}
	if _, err := os.Lstat(live.Dir()); err != nil {
		t.Fatalf("live session was removed: %v", err)
	}
}

// TestManagerScavengeBuildRootsWithoutStoreIsNoop pins the os.TempDir fallback:
// a Manager with no build-temp store (a direct-construction test, or a process
// where the root could not be established) reports nothing to reclaim rather
// than failing.
func TestManagerScavengeBuildRootsWithoutStoreIsNoop(t *testing.T) {
	m := &Manager{log: testutil.DiscardLogger()}
	n, err := m.ScavengeBuildRoots(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("ScavengeBuildRoots (no store) = (%d, %v), want (0, nil)", n, err)
	}
}

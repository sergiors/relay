package buildtemp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"relay/internal/processlock"
)

// newStore opens a Store over a fresh isolated root (RootName under t.TempDir),
// so every test operates on its own private tree and never scans or deletes the
// host temp dir. It returns the store's RESOLVED root (what Open anchored), so
// tests build session paths that match the store's own path exactly.
func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), RootName)
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store, store.Root()
}

// mustLstat reports whether path exists as whatever kind of entry it is.
func mustLstat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", path, err)
	}
	return info
}

// resolvedRoot returns the root path Open stores: RootName under the resolved
// (symlink-free) parent.
func resolvedRoot(t *testing.T, root string) string {
	t.Helper()
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", filepath.Dir(root), err)
	}
	return filepath.Join(parent, filepath.Base(root))
}

// TestDefaultRootHonorsTempDir proves the root is a fixed name directly under
// os.TempDir(), so it is isolated per TMPDIR without any package state.
func TestDefaultRootHonorsTempDir(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	if got, want := DefaultRoot(), filepath.Join(tmp, RootName); got != want {
		t.Fatalf("DefaultRoot() = %q, want %q", got, want)
	}
}

// TestOpenDefaultRoot proves the worker's own entry point — DefaultRoot() under
// an isolated TMPDIR — opens successfully and supports a full session lifecycle,
// so scavenging is enabled in the real startup path (the parent is a
// current-user-owned directory with no group/other write bit).
func TestOpenDefaultRoot(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	store, err := Open(DefaultRoot())
	if err != nil {
		t.Fatalf("Open(DefaultRoot()): %v", err)
	}
	if got := filepath.Base(store.Root()); got != RootName {
		t.Fatalf("Root() base = %q, want %q", got, RootName)
	}
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession under DefaultRoot: %v", err)
	}
	if err := sess.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestOpenCreatesPrivateRoot pins that Open creates the root owner-only and
// returns a handle whose Root() is the resolved path under the validated parent.
func TestOpenCreatesPrivateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), RootName)
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if want := resolvedRoot(t, root); store.Root() != want {
		t.Fatalf("Root() = %q, want the resolved %q", store.Root(), want)
	}
	info, err := os.Stat(store.Root())
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("root is not a directory")
	}
	if got := info.Mode().Perm(); got != RootPerm {
		t.Fatalf("root mode = %o, want %o", got, RootPerm)
	}
}

// TestOpenAcceptsStickyParent proves a sticky (e.g. /tmp-shaped) parent is
// accepted even when it is world-writable: the sticky bit prevents another user
// from swapping a Relay-owned entry.
func TestOpenAcceptsStickyParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "shared")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	if err := os.Chmod(parent, os.ModeSticky|0o777); err != nil {
		t.Fatalf("make parent sticky+writable: %v", err)
	}
	store, err := Open(filepath.Join(parent, RootName))
	if err != nil {
		t.Fatalf("Open(sticky parent): %v", err)
	}
	if _, err := os.Stat(store.Root()); err != nil {
		t.Fatalf("root not created under sticky parent: %v", err)
	}
}

// TestOpenRejectsUnsafeWritableParent proves a non-sticky, world-writable parent
// — where another user could swap the root for a symlink after Open — fails
// closed, and no root is created.
func TestOpenRejectsUnsafeWritableParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatalf("mkdir parent: %v", err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatalf("widen parent: %v", err)
	}
	root := filepath.Join(parent, RootName)
	if _, err := Open(root); err == nil {
		t.Fatal("Open(unsafe writable parent): want error, got nil")
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open created a root under an unsafe parent: %v", err)
	}
}

// TestOpenRejectsMissingParent proves an unresolvable (missing) parent fails
// closed rather than being created implicitly.
func TestOpenRejectsMissingParent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing", RootName)
	if _, err := Open(root); err == nil {
		t.Fatal("Open(missing parent): want error, got nil")
	}
}

// TestOpenResolvesSymlinkedParent proves a TMPDIR-style symlinked parent is
// resolved to a stable real path, and the store anchors under that real parent
// (never the link), so later path-based operations cannot traverse a re-pointable
// symlink.
func TestOpenResolvesSymlinkedParent(t *testing.T) {
	real := filepath.Join(t.TempDir(), "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatalf("mkdir real parent: %v", err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink parent: %v", err)
	}
	store, err := Open(filepath.Join(link, RootName))
	if err != nil {
		t.Fatalf("Open(symlinked parent): %v", err)
	}
	realResolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatalf("EvalSymlinks(real): %v", err)
	}
	if got := filepath.Dir(store.Root()); got != realResolved {
		t.Fatalf("store root parent = %q, want the resolved real parent %q", got, realResolved)
	}
}

// TestOpenRejectsUnsafeSymlinkedParent proves a symlink that resolves to an
// unsafe parent fails closed.
func TestOpenRejectsUnsafeSymlinkedParent(t *testing.T) {
	unsafe := filepath.Join(t.TempDir(), "unsafe")
	if err := os.Mkdir(unsafe, 0o700); err != nil {
		t.Fatalf("mkdir unsafe parent: %v", err)
	}
	if err := os.Chmod(unsafe, 0o777); err != nil {
		t.Fatalf("widen unsafe parent: %v", err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(unsafe, link); err != nil {
		t.Fatalf("symlink parent: %v", err)
	}
	if _, err := Open(filepath.Join(link, RootName)); err == nil {
		t.Fatal("Open(symlink to unsafe parent): want error, got nil")
	}
}

// TestOpenRejectsSymlinkRoot proves the session area can never be redirected: a
// symlink at the root path is rejected rather than followed.
func TestOpenRejectsSymlinkRoot(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := Open(link); err == nil {
		t.Fatal("Open(symlink root): want error, got nil")
	}
}

// TestOpenRejectsPreexistingFileRoot proves a regular file at the root path is
// rejected (O_DIRECTORY), never opened or chmodded.
func TestOpenRejectsPreexistingFileRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), RootName)
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatalf("seed file root: %v", err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("Open(file root): want error, got nil")
	}
	info, err := os.Lstat(root)
	if err != nil {
		t.Fatalf("root file removed: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file root mode = %o, want it unmutated at 600", info.Mode().Perm())
	}
}

// TestOpenAcceptsPreexistingPrivateRoot proves a pre-existing owner-only
// directory is accepted unchanged (no chmod).
func TestOpenAcceptsPreexistingPrivateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), RootName)
	if err := os.Mkdir(root, RootPerm); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open(preexisting private root): %v", err)
	}
	if store.Root() != resolvedRoot(t, root) {
		t.Fatalf("Root() = %q, want the resolved %q", store.Root(), resolvedRoot(t, root))
	}
}

// TestOpenRejectsWidenedPreexistingRootWithoutMutatingIt proves a pre-existing
// root with any group/other permission bit is rejected FAIL-CLOSED and left
// exactly as found (never chmodded).
func TestOpenRejectsWidenedPreexistingRootWithoutMutatingIt(t *testing.T) {
	root := filepath.Join(t.TempDir(), RootName)
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("Open(widened root): want error, got nil")
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root removed: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("widened root mode = %o, want it unmutated at 755", got)
	}
}

// TestOpenRejectsForeignOwnedRoot proves a root owned by another user is
// rejected fail-closed and not chmodded. It needs root to create a foreign-owned
// directory, so it is skipped when that is not possible.
func TestOpenRejectsForeignOwnedRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to create a foreign-owned directory")
	}
	root := filepath.Join(t.TempDir(), RootName)
	if err := os.Mkdir(root, RootPerm); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	if err := os.Chown(root, 1, 1); err != nil {
		t.Skipf("cannot chown root to a foreign uid: %v", err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("Open(foreign-owned root): want error, got nil")
	}
}

// TestNewSessionCreatesLockedSession proves a session is a directory directly
// under the root whose owner lock is held (a second acquire of the same lock
// file fails).
func TestNewSessionCreatesLockedSession(t *testing.T) {
	store, root := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = sess.Close(context.Background()) }()

	dir := sess.Dir()
	if dir == "" {
		t.Fatal("session dir is empty")
	}
	if filepath.Dir(dir) != root {
		t.Fatalf("session dir %q is not directly under root %q", dir, root)
	}
	if !isSessionName(filepath.Base(dir)) {
		t.Fatalf("session dir name %q does not match the session pattern", filepath.Base(dir))
	}
	if !mustLstat(t, dir).IsDir() {
		t.Fatalf("session path %q is not a directory", dir)
	}
	if _, err := processlock.AcquireNoFollow(filepath.Join(dir, ownerLockName)); !errors.Is(err, processlock.ErrAlreadyLocked) {
		t.Fatalf("owner lock not held: %v", err)
	}
}

// TestSessionMkdirTempCreatesRootsInsideSession proves both build-root kinds are
// staged inside the session directory (the parent the snapshot APIs use).
func TestSessionMkdirTempCreatesRootsInsideSession(t *testing.T) {
	store, _ := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = sess.Close(context.Background()) }()

	for _, pattern := range []string{"relay-build-*", "relay-dep-build-*"} {
		dir, err := sess.MkdirTemp(pattern)
		if err != nil {
			t.Fatalf("MkdirTemp(%q): %v", pattern, err)
		}
		if filepath.Dir(dir) != sess.Dir() {
			t.Fatalf("root %q not inside session %q", dir, sess.Dir())
		}
		prefix := strings.TrimSuffix(pattern, "*")
		if !strings.HasPrefix(filepath.Base(dir), prefix) {
			t.Fatalf("root %q does not match pattern %q", filepath.Base(dir), pattern)
		}
	}
}

// TestScavengeSkipsActiveSession is the core safety property: a session whose
// owner lock is held (a live owner) is never removed, however old it looks.
func TestScavengeSkipsActiveSession(t *testing.T) {
	store, _ := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = sess.Close(context.Background()) }()
	if _, err := sess.MkdirTemp("relay-build-*"); err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d sessions, want 0 (the session is active)", n)
	}
	if _, err := os.Lstat(sess.Dir()); err != nil {
		t.Fatalf("active session was removed: %v", err)
	}
}

// TestScavengeReclaimsAbandonedSession simulates a process that created a
// session and then died (no SIGKILL needed): the session directory and its
// now-unlocked owner lock file remain, and both build-root kinds are present.
// The scavenger must reclaim exactly it.
func TestScavengeReclaimsAbandonedSession(t *testing.T) {
	store, root := newStore(t)
	dir := filepath.Join(root, sessionPrefix+"crashed")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	// A stale lock file with no flock holder, exactly as a crashed owner leaves.
	if err := os.WriteFile(filepath.Join(dir, ownerLockName), nil, 0o600); err != nil {
		t.Fatalf("seed stale lock: %v", err)
	}
	for _, sub := range []string{"relay-build-abc", "relay-dep-build-def"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", sub, err)
		}
		if err := os.WriteFile(filepath.Join(dir, sub, "payload"), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed %s: %v", sub, err)
		}
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 1 {
		t.Fatalf("Scavenge removed %d, want 1", n)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned session still present: %v", err)
	}
}

// TestScavengeReclaimsSessionWithoutLockFile covers a crash in the narrow window
// before the owner lock file was created. It is reclaimable because the scan
// holds the coordination lock, which proves no live session can lack its owner
// lock (NewSession creates and locks it under that same lock).
func TestScavengeReclaimsSessionWithoutLockFile(t *testing.T) {
	store, root := newStore(t)
	dir := filepath.Join(root, sessionPrefix+"nolock")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 1 {
		t.Fatalf("Scavenge removed %d, want 1", n)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lockless session still present: %v", err)
	}
}

// TestScavengeDoesNotMutateActiveSession proves the owner-lock probe does not
// touch a session it cannot reclaim: a live session whose directory and lock
// file were widened is left in place with its modes UNCHANGED (the historical
// probe would have chmodded both back to 0700/0600).
func TestScavengeDoesNotMutateActiveSession(t *testing.T) {
	store, _ := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer func() { _ = sess.Close(context.Background()) }()

	lockPath := filepath.Join(sess.Dir(), ownerLockName)
	// Widen both so a mutating probe would be observable.
	if err := os.Chmod(sess.Dir(), 0o777); err != nil {
		t.Fatalf("widen session dir: %v", err)
	}
	if err := os.Chmod(lockPath, 0o666); err != nil {
		t.Fatalf("widen owner lock: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d, want 0 (live session)", n)
	}
	if _, err := os.Lstat(sess.Dir()); err != nil {
		t.Fatalf("live session was removed: %v", err)
	}
	if got := mustLstat(t, sess.Dir()).Mode().Perm(); got != 0o777 {
		t.Fatalf("session dir mode = %o, want unmutated 777", got)
	}
	if got := mustLstat(t, lockPath).Mode().Perm(); got != 0o666 {
		t.Fatalf("owner lock mode = %o, want unmutated 666", got)
	}
}

// TestScavengeDoesNotMutateUnreclaimableCandidate proves a candidate whose owner
// lock cannot be opened (here it is a directory, so O_RDWR fails) is left
// untouched and no lock file is created or replaced.
func TestScavengeDoesNotMutateUnreclaimableCandidate(t *testing.T) {
	store, root := newStore(t)
	dir := filepath.Join(root, sessionPrefix+"weird")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	// A directory at the owner-lock path cannot be opened O_RDWR.
	lockDir := filepath.Join(dir, ownerLockName)
	if err := os.Mkdir(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir lock path: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d, want 0 (unreclaimable candidate)", n)
	}
	info, err := os.Lstat(lockDir)
	if err != nil || !info.IsDir() {
		t.Fatalf("unreclaimable lock path was mutated: %v (dir=%v)", err, info)
	}
}

// TestScavengeSkipsSymlinkedOwnerLock proves a session whose owner lock is a
// symlink is skipped (O_NOFOLLOW), left untouched, and never has the symlink
// followed or its target modified.
func TestScavengeSkipsSymlinkedOwnerLock(t *testing.T) {
	store, root := newStore(t)
	target := filepath.Join(t.TempDir(), "target.lock")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	dir := filepath.Join(root, sessionPrefix+"linklock")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, ownerLockName)); err != nil {
		t.Fatalf("symlink owner lock: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d, want 0 (symlinked lock is unprovable)", n)
	}
	if got := mustLstat(t, filepath.Join(dir, ownerLockName)); got.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("owner-lock symlink no longer a symlink: %v", got.Mode())
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("symlink target removed: %v", err)
	}
}

// TestScavengeLeavesUnrelatedEntriesAndSymlinks proves the scan only ever acts
// on real session directories: unrelated files/directories, a legacy-style
// `relay-build-*` root, and symlinks (never followed) all survive, including a
// symlink's target outside the root.
func TestScavengeLeavesUnrelatedEntriesAndSymlinks(t *testing.T) {
	store, root := newStore(t)

	filePath := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(filePath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	otherDir := filepath.Join(root, "other")
	if err := os.MkdirAll(otherDir, 0o700); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// A regular file whose name matches the session prefix: still not a dir.
	fileSession := filepath.Join(root, sessionPrefix+"file")
	if err := os.WriteFile(fileSession, []byte("keep"), 0o600); err != nil {
		t.Fatalf("seed session-named file: %v", err)
	}
	// A legacy-style root (never a session) directly under the root.
	legacy := filepath.Join(root, "relay-build-legacy")
	if err := os.MkdirAll(legacy, 0o700); err != nil {
		t.Fatalf("seed legacy root: %v", err)
	}
	// A symlink named like a session, pointing at a real directory OUTSIDE the
	// root that must survive untouched.
	target := t.TempDir()
	targetFile := filepath.Join(target, "keep")
	if err := os.WriteFile(targetFile, []byte("keep"), 0o600); err != nil {
		t.Fatalf("seed symlink target: %v", err)
	}
	symlinkSession := filepath.Join(root, sessionPrefix+"evil")
	if err := os.Symlink(target, symlinkSession); err != nil {
		t.Fatalf("symlink session: %v", err)
	}
	// A symlink to a file.
	symlinkFile := filepath.Join(root, sessionPrefix+"link")
	if err := os.Symlink(filePath, symlinkFile); err != nil {
		t.Fatalf("symlink file: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d, want 0", n)
	}
	for _, p := range []string{filePath, otherDir, fileSession, legacy, symlinkSession, symlinkFile} {
		mustLstat(t, p)
	}
	if _, err := os.Stat(targetFile); err != nil {
		t.Fatalf("symlink target file removed: %v", err)
	}
	if got := mustLstat(t, symlinkSession); got.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("session symlink is no longer a symlink: %v", got.Mode())
	}
}

// TestScavengeRemovesOrphanWithoutFollowingInnerSymlink proves removing an
// orphan session never follows a symlink it contains: the session is reclaimed
// but the symlink's target outside the root survives.
func TestScavengeRemovesOrphanWithoutFollowingInnerSymlink(t *testing.T) {
	store, root := newStore(t)
	target := t.TempDir()
	targetFile := filepath.Join(target, "keep")
	if err := os.WriteFile(targetFile, []byte("keep"), 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	dir := filepath.Join(root, sessionPrefix+"orphan")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link")); err != nil {
		t.Fatalf("inner symlink: %v", err)
	}

	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if n != 1 {
		t.Fatalf("Scavenge removed %d, want 1", n)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan session still present: %v", err)
	}
	if _, err := os.Stat(targetFile); err != nil {
		t.Fatalf("inner symlink target removed: %v", err)
	}
}

// TestScavengeReclaimsAfterOwnerLockReleased is the release/reacquire property:
// while an owner lock is held the session is skipped; once it is released (the
// owner exited) the very next scavenge reclaims it.
func TestScavengeReclaimsAfterOwnerLockReleased(t *testing.T) {
	store, root := newStore(t)
	dir := filepath.Join(root, sessionPrefix+"held")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}
	lock, err := processlock.AcquireNoFollow(filepath.Join(dir, ownerLockName))
	if err != nil {
		t.Fatalf("hold owner lock: %v", err)
	}

	if n, err := store.Scavenge(context.Background()); err != nil || n != 0 {
		t.Fatalf("Scavenge while held = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := os.Lstat(dir); err != nil {
		t.Fatalf("held session was removed: %v", err)
	}

	if err := lock.Close(); err != nil {
		t.Fatalf("release owner lock: %v", err)
	}
	n, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge after release: %v", err)
	}
	if n != 1 {
		t.Fatalf("Scavenge after release removed %d, want 1", n)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released session still present: %v", err)
	}
}

// TestScavengeFailsClosedWhenCoordLockHeld proves the coordination lock is
// load-bearing: while another process holds it, a scavenge whose context is
// cancelled removes nothing and returns the error. An orphan that WOULD be
// reclaimed is left in place.
func TestScavengeFailsClosedWhenCoordLockHeld(t *testing.T) {
	store, root := newStore(t)
	dir := filepath.Join(root, sessionPrefix+"orphan")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir session: %v", err)
	}

	coord, err := processlock.AcquireBlocking(context.Background(), filepath.Join(root, coordLockName))
	if err != nil {
		t.Fatalf("hold coordination lock: %v", err)
	}
	defer coord.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := store.Scavenge(cancelled)
	if err == nil {
		t.Fatal("Scavenge with the coordination lock held and a cancelled ctx: want error, got nil")
	}
	if n != 0 {
		t.Fatalf("Scavenge removed %d, want 0 (must fail closed)", n)
	}
	if _, serr := os.Lstat(dir); serr != nil {
		t.Fatalf("orphan removed despite fail-closed: %v", serr)
	}
}

// TestScavengeMissingRootIsNoop proves a root that does not exist yields (0,
// nil) and is NOT created as a side effect.
func TestScavengeMissingRootIsNoop(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent")
	store := &Store{root: root}
	n, err := store.Scavenge(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("Scavenge(missing root) = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Scavenge created the root: %v", err)
	}
}

// TestConcurrentSessionsAreIndependent proves concurrent session creation is
// serialized safely (the coordination lock) and that no live session is swept:
// every session is distinct and locked, and a scavenge reclaims none.
func TestConcurrentSessionsAreIndependent(t *testing.T) {
	store, root := newStore(t)
	const n = 8
	sessions := make([]*Session, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessions[i], errs[i] = store.NewSession(context.Background())
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("NewSession[%d]: %v", i, errs[i])
		}
		if sessions[i] == nil {
			t.Fatalf("NewSession[%d] returned nil", i)
		}
		dir := sessions[i].Dir()
		if seen[dir] {
			t.Fatalf("duplicate session dir %q", dir)
		}
		seen[dir] = true
	}

	got, err := store.Scavenge(context.Background())
	if err != nil {
		t.Fatalf("Scavenge: %v", err)
	}
	if got != 0 {
		t.Fatalf("Scavenge removed %d, want 0 (all sessions active)", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	// n sessions plus the coordination lock file; no other residue.
	if len(entries) != n+1 {
		t.Fatalf("root entries = %d, want %d", len(entries), n+1)
	}

	for i := range sessions {
		if err := sessions[i].Close(context.Background()); err != nil {
			t.Fatalf("Close[%d]: %v", i, err)
		}
	}
	// Every session is gone after Close, but the root and its coordination lock
	// survive.
	remaining, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root after close: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Name() != coordLockName {
		t.Fatalf("root after close = %v, want only %s", names(remaining), coordLockName)
	}
}

// TestSessionCloseRemovesSessionButKeepsRoot pins normal cleanup: Close removes
// the session directory (with whatever roots it held) and is idempotent, while
// the shared root survives for the next session.
func TestSessionCloseRemovesSessionButKeepsRoot(t *testing.T) {
	store, root := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	dir := sess.Dir()
	if _, err := sess.MkdirTemp("relay-build-*"); err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if err := sess.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("session dir still present after Close: %v", err)
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("root must survive Close: %v", err)
	}
	if err := sess.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestMkdirTempAfterCloseFails proves a preparation racing shutdown cannot
// create a root after the session directory was removed.
func TestMkdirTempAfterCloseFails(t *testing.T) {
	store, _ := newStore(t)
	sess, err := store.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sess.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := sess.MkdirTemp("relay-build-*"); !errors.Is(err, errSessionClosed) {
		t.Fatalf("MkdirTemp after Close = %v, want errSessionClosed", err)
	}
}

// names returns the entry names for a readable failure message.
func names(entries []os.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

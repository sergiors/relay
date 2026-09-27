package processlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestAcquireFirstSucceeds covers the first-acquire case: a fresh path yields a
// lock and creates the lock file.
func TestAcquireFirstSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer lock.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file not created: %v", err)
	}
}

// TestSecondAcquireFailsWhileHeld covers the contention case: while the first
// lock is held, a second Acquire on the same path (a distinct open file
// description, as another process would use) fails with ErrAlreadyLocked.
func TestSecondAcquireFailsWhileHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer first.Close()

	if _, err := Acquire(path); !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("second Acquire error = %v, want ErrAlreadyLocked", err)
	}
}

// TestClosePermitsReacquire covers release: once the first lock is closed, a new
// Acquire on the same path succeeds (the file was left on disk).
func TestClosePermitsReacquire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Acquire(path)
	if err != nil {
		t.Fatalf("re-Acquire after close: %v", err)
	}
	defer second.Close()
}

// TestStaleFileIsReusable covers the leftover-file case: a lock file from a
// previous (dead) process holds no kernel lock, so Acquire succeeds on it. Close
// leaves the file in place for the next Acquire.
func TestStaleFileIsReusable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("seed stale lock file: %v", err)
	}

	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire over stale file: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file must survive Close: %v", err)
	}
}

// TestAcquireCreatesParentDirs covers directory creation: Acquire MkdirAll's
// missing parent directories before opening the lock file.
func TestAcquireCreatesParentDirs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "relay.lock")
	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire with missing parents: %v", err)
	}
	defer lock.Close()

	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("parent dir not created: %v", err)
	}
}

// TestAcquireEnforcesPermissions covers the explicit runtime-directory and
// lock-file modes: a fresh acquire leaves owner-only modes, and an existing
// directory/lock file whose modes were widened is tightened on acquire. The
// assertions use the exported constants so the test and implementation cannot
// drift.
func TestAcquireEnforcesPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run", "relay")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("widen dir: %v", err)
	}
	path := filepath.Join(dir, "relay.lock")
	if err := os.WriteFile(path, nil, 0o666); err != nil {
		t.Fatalf("seed lock file: %v", err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatalf("widen lock file: %v", err)
	}

	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer lock.Close()

	lockInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat lock file: %v", err)
	}
	if got := lockInfo.Mode().Perm(); got != LockFilePerm {
		t.Fatalf("lock file mode = %o, want %o", got, LockFilePerm)
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat lock dir: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != DirPerm {
		t.Fatalf("lock dir mode = %o, want %o", got, DirPerm)
	}
}

// TestUnrelatedPathsDoNotConflict covers independence: locks on different paths
// are unrelated, so holding one never blocks acquiring another.
func TestUnrelatedPathsDoNotConflict(t *testing.T) {
	a := filepath.Join(t.TempDir(), "a.lock")
	b := filepath.Join(t.TempDir(), "b.lock")

	lockA, err := Acquire(a)
	if err != nil {
		t.Fatalf("Acquire(a): %v", err)
	}
	defer lockA.Close()

	lockB, err := Acquire(b)
	if err != nil {
		t.Fatalf("Acquire(b) while a is held: %v", err)
	}
	defer lockB.Close()
}

// TestAcquireOpenErrorDifferentiated covers the open/create error: a path whose
// parent exists as a regular file cannot be created or opened, and the error is
// distinct from ErrAlreadyLocked so callers can report an initialization
// failure rather than "already running".
func TestAcquireOpenErrorDifferentiated(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("seed blocker file: %v", err)
	}
	_, err := Acquire(filepath.Join(blocker, "relay.lock"))
	if err == nil {
		t.Fatal("Acquire under a regular file: want error, got nil")
	}
	if errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("open/create error must not be ErrAlreadyLocked: %v", err)
	}
}

// TestCloseIsIdempotentAndNilSafe pins that Close can be called twice and on a
// nil lock without panicking, so the CLI can defer it unconditionally.
func TestCloseIsIdempotentAndNilSafe(t *testing.T) {
	var nilLock *Lock
	if err := nilLock.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}

	path := filepath.Join(t.TempDir(), "relay.lock")
	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

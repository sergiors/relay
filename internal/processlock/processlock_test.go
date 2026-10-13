package processlock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
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

// TestAcquireNoFollowLocksRegularFile is the positive control: an ordinary lock
// file is acquired exactly like Acquire, and contention is still reported as
// ErrAlreadyLocked.
func TestAcquireNoFollowLocksRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	lock, err := AcquireNoFollow(path)
	if err != nil {
		t.Fatalf("AcquireNoFollow: %v", err)
	}
	defer lock.Close()

	if _, err := AcquireNoFollow(path); !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("second AcquireNoFollow = %v, want ErrAlreadyLocked", err)
	}
}

// TestAcquireNoFollowRejectsSymlink covers symlink safety: a symlink at the lock
// path is NOT followed (O_NOFOLLOW), so AcquireNoFollow fails instead of taking
// a lock on the link target. The target must remain untouched and unlocked.
func TestAcquireNoFollowRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.lock")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	link := filepath.Join(dir, "link.lock")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err := AcquireNoFollow(link)
	if err == nil {
		t.Fatal("AcquireNoFollow on a symlink: want error, got nil")
	}
	if errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("symlink rejection must not be ErrAlreadyLocked: %v", err)
	}
	// The link target was never locked, so an ordinary Acquire takes it.
	lock, err := Acquire(target)
	if err != nil {
		t.Fatalf("Acquire(target) after rejected symlink: %v", err)
	}
	_ = lock.Close()
}

// TestAcquireBlockingCancelledContextFailsClosed pins that a cancelled context
// is checked before the first attempt: AcquireBlocking returns the context error
// and never creates the lock file, so a caller that is already shutting down
// leaves no state behind.
func TestAcquireBlockingCancelledContextFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := AcquireBlocking(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireBlocking(cancelled) = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("AcquireBlocking(cancelled) created the lock file: %v", err)
	}
}

// TestAcquireBlockingWaitsForRelease proves the blocking semantics: while the
// lock is held a bounded wait fails closed with the context error (never
// ErrAlreadyLocked), and once the holder releases it a waiting acquire succeeds.
func TestAcquireBlockingWaitsForRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	held, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	_, err = AcquireBlocking(waitCtx, path)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireBlocking while held = %v, want context.DeadlineExceeded", err)
	}

	if err := held.Close(); err != nil {
		t.Fatalf("release: %v", err)
	}
	lock, err := AcquireBlocking(context.Background(), path)
	if err != nil {
		t.Fatalf("AcquireBlocking after release: %v", err)
	}
	_ = lock.Close()
}

// TestTryAcquireExistingMissingDoesNotCreate proves the probe is non-mutating
// when the lock file is absent: it reports ErrLockMissing and creates neither
// the lock file nor its parent directory.
func TestTryAcquireExistingMissingDoesNotCreate(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "absent")
	path := filepath.Join(parent, "relay.lock")

	_, err := TryAcquireExisting(path)
	if !errors.Is(err, ErrLockMissing) {
		t.Fatalf("TryAcquireExisting(missing) = %v, want ErrLockMissing", err)
	}
	if _, err := os.Lstat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("probe created the parent directory: %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("probe created the lock file: %v", err)
	}
}

// TestTryAcquireExistingDoesNotChmod proves the probe never chmods the lock file
// or its parent directory: a widened lock file and parent are left exactly as
// found.
func TestTryAcquireExistingDoesNotChmod(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "locks")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("widen dir: %v", err)
	}
	path := filepath.Join(dir, "relay.lock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("seed lock: %v", err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatalf("widen lock: %v", err)
	}

	lock, err := TryAcquireExisting(path)
	if err != nil {
		t.Fatalf("TryAcquireExisting: %v", err)
	}
	defer lock.Close()

	if got := statPerm(t, path); got != 0o666 {
		t.Fatalf("lock file mode = %o, want unmutated 666", got)
	}
	if got := statPerm(t, dir); got != 0o777 {
		t.Fatalf("lock dir mode = %o, want unmutated 777", got)
	}
}

// TestTryAcquireExistingHeldReportsContention proves an existing lock held by
// another holder is reported as ErrAlreadyLocked (not ErrLockMissing).
func TestTryAcquireExistingHeldReportsContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	held, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer held.Close()

	if _, err := TryAcquireExisting(path); !errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("TryAcquireExisting(held) = %v, want ErrAlreadyLocked", err)
	}
}

// TestTryAcquireExistingRejectsSymlink proves the probe never follows a symlink
// at the lock path: it fails (not ErrLockMissing, not ErrAlreadyLocked) and
// leaves the link target untouched.
func TestTryAcquireExistingRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.lock")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	link := filepath.Join(dir, "link.lock")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	_, err := TryAcquireExisting(link)
	if err == nil {
		t.Fatal("TryAcquireExisting(symlink): want error, got nil")
	}
	if errors.Is(err, ErrLockMissing) || errors.Is(err, ErrAlreadyLocked) {
		t.Fatalf("symlink probe error = %v, want neither ErrLockMissing nor ErrAlreadyLocked", err)
	}
	// The target was never locked, so an ordinary Acquire still takes it.
	lock, err := Acquire(target)
	if err != nil {
		t.Fatalf("Acquire(target) after rejected symlink: %v", err)
	}
	_ = lock.Close()
}

// statPerm returns path's permission bits.
func statPerm(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm()
}

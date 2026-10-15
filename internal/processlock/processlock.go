// Package processlock provides advisory locks backed by flock(2) on a lock file.
//
// Its primary use is Relay's process-singleton guard: `relay start` acquires the
// lock at the process boundary, before the worker does any work, and holds it for
// the whole runtime lifetime, so at most one long-running Relay process runs per
// lock path. The same primitive also backs the coordination and ownership locks
// of Relay's private build-temp area (internal/buildtemp), which are keyed by a
// path under the shared temp root rather than the fixed runtime directory. The
// kernel owns every lock — it is released when the retained file descriptor is
// closed (explicitly by Close, or automatically when the process exits) — so a
// crashed process never leaves a stuck lock and a leftover lock file is inert.
package processlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// DefaultDir is the fixed application-convention directory for Relay's
// EPHEMERAL runtime files. It is deliberately separate from the PERSISTENT
// /var/lib/relay state volume (the secrets store and git material; PostgreSQL
// state lives in the database, not this volume): /run/relay is tmpfs-backed
// process state that must not survive a reboot. It holds the process lock below
// and the worker's live query socket (worker.SocketPath), which is derived from
// this directory so the two ephemeral runtime paths stay centralized without a
// separate path package.
const DefaultDir = "/run/relay"

// DirPerm is the mode for Relay's ephemeral runtime directory (DefaultDir). It
// is owner-only: the directory holds the process lock and the worker's control
// socket, and no other user has a reason to traverse it. It is applied both at
// creation and enforced on an existing directory, so a path created world- or
// group-readable by a previous version (or a container base image) is tightened
// on the next startup.
const DirPerm = 0o700

// LockFilePerm is the mode for the process lock file. It is owner-only to match
// the runtime directory: the file carries no content, only the kernel flock, so
// nothing beyond the owning process should read or write it. It is enforced on
// an existing lock file as well as a freshly created one.
const LockFilePerm = 0o600

// DefaultPath is the fixed application-convention lock file for the long-running
// Relay process. It lives under the ephemeral DefaultDir: the kernel releases
// flock when the process exits, so a leftover lock file on tmpfs is inert, and
// ephemeral process coordination is kept out of the PERSISTENT /var/lib/relay
// volume. Acquire happily takes a lock on a pre-existing file.
const DefaultPath = DefaultDir + "/relay.lock"

// ErrAlreadyLocked reports that the lock is held by another open file
// description — another process, or another Acquire within this process.
// Callers detect it with errors.Is to map it to an operator-facing
// "already running" message. The wrapped error also carries the lock path.
var ErrAlreadyLocked = errors.New("lock already held")

// ErrLockMissing reports that TryAcquireExisting found no lock file at the given
// path. It is distinct from ErrAlreadyLocked and from an open/lock failure so a
// caller (the build-temp scavenger) can tell "no lock file" from "held" or
// "unreadable".
var ErrLockMissing = errors.New("lock file missing")

// Lock is an acquired advisory lock. It retains the open file descriptor for as
// long as it is held: flock is released only when that descriptor is closed
// (explicitly via Close, or by the kernel when the process exits). The lock
// file is never removed — deleting it would let a concurrent Acquire create a
// second file and take an independent lock, defeating the guard.
type Lock struct {
	file *os.File
}

// Acquire creates the lock file's parent directory, opens (creating if absent)
// the lock file, and takes an exclusive, non-blocking flock on it.
//
// It also makes the runtime directory and lock file owner-only (DirPerm /
// LockFilePerm). MkdirAll and O_CREATE apply their mode only at creation and are
// masked by the process umask, so Acquire tightens both explicitly; that keeps
// the directory and lock at the documented mode even when a previous version or
// a container base image left them more permissive.
//
// Errors are differentiated:
//   - ErrAlreadyLocked (detect with errors.Is) when another holder owns the
//     lock;
//   - an "open lock file" error when the file cannot be created or opened;
//   - an "acquire lock" error for any other flock failure.
func Acquire(path string) (*Lock, error) {
	return acquire(path, false)
}

// AcquireNoFollow is Acquire with O_NOFOLLOW: it never follows a symbolic link
// at path, failing instead. It is for a lock file whose location may be inside a
// directory another party could populate (Relay's shared build-temp root, whose
// session directories are trusted only because Relay created them under an
// owner-only root), so a symlinked lock file can never redirect the lock — or a
// later removal — outside the tree. Every other behavior (directory creation,
// owner-only modes, non-blocking contention semantics, error differentiation)
// matches Acquire.
func AcquireNoFollow(path string) (*Lock, error) {
	return acquire(path, true)
}

// acquire is the shared implementation behind Acquire and AcquireNoFollow.
// noFollow adds O_NOFOLLOW to the open flags.
func acquire(path string, noFollow bool) (*Lock, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	if err := os.Chmod(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("set lock dir permissions: %w", err)
	}
	flags := os.O_CREATE | os.O_RDWR
	if noFollow {
		flags |= unix.O_NOFOLLOW
	}
	f, err := os.OpenFile(path, flags, LockFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		// A failed acquisition must not itself hold the lock, so close the
		// descriptor before returning (closing also discards the open file
		// description that flock is associated with).
		_ = f.Close()
		// EWOULDBLOCK is the documented flock contention error; EAGAIN is the
		// same errno on every supported platform, listed for clarity.
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyLocked, path)
		}
		return nil, fmt.Errorf("acquire lock %s: %w", path, err)
	}
	// Enforce the lock file's mode on the opened descriptor (O_CREATE's mode is
	// masked by umask and applies only when the file is created). A failure
	// releases the just-acquired flock by closing the descriptor.
	if err := f.Chmod(LockFilePerm); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("set lock file permissions %s: %w", path, err)
	}
	return &Lock{file: f}, nil
}

// TryAcquireExisting takes the flock on an EXISTING lock file at path without
// creating it and without mutating its parent directory or its mode. It opens
// path O_RDWR|O_NOFOLLOW (never following a symlink at the final component),
// verifies the opened inode is a regular file, and attempts LOCK_EX|LOCK_NB.
//
// Unlike Acquire/AcquireNoFollow it performs no MkdirAll, no parent chmod, no
// O_CREATE, and no file chmod: it is a read-only probe of an existing lock, for
// a caller that must decide whether an existing resource is still owned WITHOUT
// altering it (Relay's build-temp scavenger probing a candidate session). It
// reports:
//   - (*Lock, nil): the lock was free and is now held by the caller;
//   - (nil, ErrLockMissing): no lock file exists at path;
//   - (nil, ErrAlreadyLocked): the file exists and another holder owns it;
//   - (nil, other): the file exists but could not be opened or is not a regular
//     file (a symlink, a directory, an unreadable mode), so it cannot be proven
//     free. The caller must treat every non-nil error except ErrLockMissing as
//     "not reclaimable" and leave the candidate untouched.
func TryAcquireExisting(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrLockMissing, path)
		}
		return nil, fmt.Errorf("open existing lock file %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat existing lock file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("existing lock file %s is not a regular file", path)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrAlreadyLocked, path)
		}
		return nil, fmt.Errorf("acquire existing lock %s: %w", path, err)
	}
	return &Lock{file: f}, nil
}

// blockingAcquireRetryInterval is the pause between LOCK_NB attempts in
// AcquireBlocking. flock has no portable context-aware blocking call, so a
// waiting acquire polls; the interval only bounds how promptly a freed lock is
// noticed, and the coordination windows this serializes are short.
const blockingAcquireRetryInterval = 5 * time.Millisecond

// AcquireBlocking acquires the lock at path, waiting until it becomes available
// or ctx is done. It shares Acquire's directory/file setup and error
// classification, except that contention is waited out rather than reported as
// ErrAlreadyLocked: it returns ctx.Err() when ctx is cancelled first. A
// cancelled ctx is checked before the first attempt, so a caller that is
// already shutting down never creates the lock file or takes the lock. It exists
// for coordination locks that must serialize a short critical section (Relay's
// build-temp session creation, scavenging, and teardown) rather than fail fast
// on contention.
func AcquireBlocking(ctx context.Context, path string) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	if err := os.Chmod(dir, DirPerm); err != nil {
		return nil, fmt.Errorf("set lock dir permissions: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, LockFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	ticker := time.NewTicker(blockingAcquireRetryInterval)
	defer ticker.Stop()
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = f.Close()
			return nil, fmt.Errorf("acquire lock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
	if err := f.Chmod(LockFilePerm); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("set lock file permissions %s: %w", path, err)
	}
	return &Lock{file: f}, nil
}

// Close releases the lock by closing the retained descriptor, which is all
// flock requires. It is idempotent and safe on a nil receiver. The lock file is
// deliberately left on disk; a later Acquire reuses it.
func (l *Lock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	return f.Close()
}

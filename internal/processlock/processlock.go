// Package processlock provides a process-level advisory lock backed by
// flock(2) (LOCK_EX|LOCK_NB) on a lock file.
//
// Its single purpose is to keep at most one long-running Relay process per
// lock path: `relay start` acquires the lock at the process boundary, before
// the worker does any work, and holds it for the whole runtime lifetime. The
// kernel owns the lock — it is released when the retained file descriptor is
// closed (explicitly by Close, or automatically when the process exits) — so a
// crashed process never leaves a stuck lock and a leftover lock file is inert.
package processlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// DefaultPath is the fixed application-convention lock file for the long-running
// Relay process. It lives in the same /var/lib/relay directory as the state
// database and secrets store, so the compose volume mount persists it across
// container restarts. Persistence is harmless: flock is released by the kernel
// when the process exits, and Acquire happily takes a lock on a pre-existing
// file.
const DefaultPath = "/var/lib/relay/relay.lock"

// ErrAlreadyLocked reports that the lock is held by another open file
// description — another process, or another Acquire within this process.
// Callers detect it with errors.Is to map it to an operator-facing
// "already running" message. The wrapped error also carries the lock path.
var ErrAlreadyLocked = errors.New("lock already held")

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
// Errors are differentiated:
//   - ErrAlreadyLocked (detect with errors.Is) when another holder owns the
//     lock;
//   - an "open lock file" error when the file cannot be created or opened;
//   - an "acquire lock" error for any other flock failure.
func Acquire(path string) (*Lock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
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

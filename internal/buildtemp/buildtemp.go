// Package buildtemp owns Relay's private on-disk build-temp area: the transient
// source-snapshot and dependency-snapshot roots a preparation stages into.
//
// A preparation copies an app's selected source (and, when declared, its
// dependency manifests) into a private directory that IS the Docker build
// context. Historically those directories were created directly under
// os.TempDir() and removed by a deferred os.RemoveAll, which cleans up on every
// normal return but leaks on a process crash. Under a shared temp dir (several
// isolated Relay containers or processes sharing TMPDIR) a startup sweeper
// cannot tell an abandoned root from one an active process is still writing, so
// deleting by name or age alone would corrupt a live build.
//
// This package replaces that with an ownership protocol that is safe across
// processes sharing a filesystem:
//
//   - One Relay-private ROOT under os.TempDir() (DefaultRoot) holds every
//     session. The root is owner-only (RootPerm) and never holds anything but
//     sessions and the coordination lock.
//   - The root's PARENT is validated once, at Open: it must be sticky (e.g.
//     /tmp) or a private directory owned by the current user with no group/other
//     write bit, and it is resolved through any TMPDIR symlink to a stable real
//     path. This is what makes the root unswappable by another user after Open,
//     so every path-based operation that follows (coordination lock, session
//     creation, scavenging, teardown) cannot be redirected through the root. An
//     unsafe or unverifiable parent fails closed, so the caller falls back to
//     plain os.TempDir roots without scavenging.
//   - A SESSION is one owner's private directory (a random `session-` name)
//     holding that owner's build roots. The session directory is the parent the
//     snapshot creation APIs stage into, so both root kinds live inside it.
//   - The owning process holds an OS advisory flock on the session's owner lock
//     file (ownerLockName) for the session's whole lifetime. The kernel releases
//     it when the process exits, however abruptly, so a crashed process's
//     session becomes lockable again with no stale state.
//   - A single coordination lock (coordLockName) serializes session creation,
//     scavenging, and teardown. A creator holds it while it creates the session
//     directory AND acquires the owner lock, so a scavenger can never observe a
//     just-created but not-yet-locked session: by the time the scavenger holds
//     the coordination lock, every visible session is either already owner-locked
//     (alive) or was left by a dead process (lockable).
//
// Scavenging (Store.Scavenge) removes a session only when its owner lock can be
// acquired, which proves no live process owns it. The probe NEVER mutates a
// candidate it cannot reclaim: it opens the existing owner lock read-only, with
// no O_CREATE and no chmod. It never uses age, name ordering, or any other
// heuristic, and it FAILS CLOSED: if the coordination lock cannot be taken, or
// the root cannot be read, it removes nothing and returns the error. A session
// directory with no owner lock file is reclaimed only under the coordination
// lock, which proves no live session can lack one (see probeOwner). Entries that
// are not session directories — unrelated files, directories, and symlinks
// (which are never followed) — are left untouched.
//
// Legacy roots created directly under os.TempDir() by an older Relay (a bare
// `relay-build-*` / `relay-dep-build-*` directory with no session and no
// ownership metadata) are deliberately NEVER scavenged: under a shared temp dir
// an older, still-running Relay could own one, and there is no lock to prove
// otherwise. They are inert residue for an operator to remove; a future version
// could add a proven-inactive reclaim if a safe signal exists.
package buildtemp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"relay/internal/processlock"
)

const (
	// RootName is the fixed name of Relay's private temp root under os.TempDir.
	// It deliberately does NOT share the `relay-build-` / `relay-dep-build-`
	// prefixes of the transient roots themselves, so it can never be mistaken
	// for a legacy build directory by a scanner (or by the runtime's existing
	// build-context cleanup tests).
	RootName = "relay-tmp"

	// sessionPrefix names a per-owner session directory inside the root. Only
	// entries with this prefix are ever considered by a scavenger; everything
	// else under the root is untouched.
	sessionPrefix = "session-"

	// ownerLockName is the session's owner lock file. The owning process holds
	// an exclusive flock on it for the session's lifetime; the kernel releases
	// it on process exit.
	ownerLockName = ".owner.lock"

	// coordLockName is the single coordination lock file inside the root. It is
	// held only for the short window of session creation, scavenging, or
	// teardown, and is never removed (an inert lock file, like processlock's).
	coordLockName = ".coord.lock"

	// RootPerm is the mode of the private root: owner-only, matching Relay's
	// other ephemeral runtime directories. It is enforced on an existing root as
	// well as a freshly created one.
	RootPerm = 0o700
)

// errSessionClosed reports use of a session after it was closed.
var errSessionClosed = errors.New("buildtemp: session closed")

// DefaultRoot returns Relay's private temp root: RootName directly under
// os.TempDir(). os.TempDir() is read on every call, so a test that points TMPDIR
// at an isolated directory observes an isolated root without any package state.
func DefaultRoot() string {
	return filepath.Join(os.TempDir(), RootName)
}

// Store is a handle to a Relay-private temp root. It is safe for concurrent use;
// the root path is immutable after Open.
type Store struct {
	root string
}

// Open returns a handle to Relay's private temp root, creating it owner-only
// (RootPerm) when absent and otherwise validating the existing one.
//
// It establishes the PARENT BOUNDARY once, here, because every later operation
// (the coordination lock, session creation, scavenging, and teardown) is
// path-based under the root and must not be redirectable by another user:
//
//   - the root's parent is resolved with filepath.EvalSymlinks, so a TMPDIR that
//     is itself a symlink (e.g. macOS's /var -> /private/var) is resolved to a
//     stable real path, and the RESOLVED path becomes the root's parent from
//     then on — no later path-based operation traverses a symlink whose target
//     could be re-pointed;
//   - the resolved parent must be a directory that is either STICKY (e.g. /tmp,
//     where another user cannot rename or replace an entry they do not own) or
//     owned by the current user with no group/other write bit. Any other parent
//     — a shared, non-sticky, writable directory — fails closed, so the caller
//     falls back to plain os.TempDir roots WITHOUT scavenging rather than risk
//     the root being swapped for a symlink after this validation.
//
// The root itself is then created atomically or validated descriptor-safely:
//
//   - os.Mkdir creates the root when it is absent; only a root this call CREATED
//     is ever chmodded (owner-only), and that chmod is done through the opened
//     directory descriptor, never by path;
//   - when the path already exists, the root is opened O_NOFOLLOW|O_DIRECTORY
//     (so a symlink or a non-directory fails instead of being followed), the
//     opened inode is fstat'd, and it must be a real directory owned by the
//     current user with no group/other permission bits. An existing root is
//     NEVER chmodded — a widened, foreign-owned, symlinked, or otherwise
//     unverifiable root fails closed.
//
// A failure here is not fatal to a caller that can fall back to plain os.TempDir
// roots; it only means scavenging is unavailable for that process.
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("buildtemp: empty root")
	}
	realParent, err := resolveTempParent(filepath.Dir(root))
	if err != nil {
		return nil, err
	}
	// Anchor the root under the resolved, symlink-free parent so no later
	// path-based operation can be redirected through a re-pointed symlink.
	root = filepath.Join(realParent, filepath.Base(root))

	created := false
	if err := os.Mkdir(root, RootPerm); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("buildtemp: create root %s: %w", root, err)
		}
	} else {
		created = true
	}
	// Open the root without following a symlink and requiring a directory. The
	// descriptor is the real opened inode, so a later fstat/fchmod can never be
	// redirected by a path swap between the checks.
	f, err := openRootDir(root)
	if err != nil {
		return nil, fmt.Errorf("buildtemp: open root %s: %w", root, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("buildtemp: stat root %s: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("buildtemp: root %s is not a directory", root)
	}
	if err := verifyRootOwner(root, info); err != nil {
		return nil, err
	}
	if created {
		// Ownership is established (we just created it): enforce owner-only mode
		// through the descriptor, independent of the process umask.
		if err := f.Chmod(RootPerm); err != nil {
			return nil, fmt.Errorf("buildtemp: set root permissions %s: %w", root, err)
		}
		return &Store{root: root}, nil
	}
	// Pre-existing root: validate only, never mutate. No group/other access
	// proves no other user can traverse it or inject entries.
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("buildtemp: root %s has mode %o; want owner-only", root, info.Mode().Perm())
	}
	return &Store{root: root}, nil
}

// resolveTempParent resolves a root's parent to a stable, symlink-free real path
// and verifies it is a safe boundary for a Relay-owned child: a STICKY directory
// (another user cannot rename or replace an entry they do not own) or a
// directory owned by the current user with no group/other write bit. It fails
// closed on any resolution error or any other parent, so the caller disables
// scavenging rather than risk a root swapped for a symlink after validation.
func resolveTempParent(parent string) (string, error) {
	real, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", fmt.Errorf("buildtemp: resolve parent %s: %w", parent, err)
	}
	info, err := os.Stat(real)
	if err != nil {
		return "", fmt.Errorf("buildtemp: stat parent %s: %w", real, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("buildtemp: parent %s is not a directory", real)
	}
	if info.Mode()&os.ModeSticky != 0 {
		return real, nil
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("buildtemp: parent %s ownership unverifiable", real)
	}
	if st.Uid == uint32(os.Geteuid()) && info.Mode().Perm()&0o022 == 0 {
		return real, nil
	}
	return "", fmt.Errorf("buildtemp: parent %s is neither sticky nor a private directory owned by the current user", real)
}

// openRootDir opens root as a directory without following a symlink at the final
// component (O_NOFOLLOW) and requiring it to be a directory (O_DIRECTORY), so a
// symlink or a regular file at the root path fails instead of being followed.
func openRootDir(root string) (*os.File, error) {
	return os.OpenFile(root, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
}

// verifyRootOwner fails closed unless info describes a real directory owned by
// the current user. Ownership is read from the fstat'd inode, so it reflects the
// opened directory, not a path that could have been swapped. An unverifiable
// owner (a platform where the stat owner cannot be read) also fails closed.
func verifyRootOwner(root string, info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("buildtemp: root %s ownership unverifiable", root)
	}
	if st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("buildtemp: root %s is owned by uid %d, not the current user (%d)", root, st.Uid, os.Geteuid())
	}
	return nil
}

// Root returns the store's private root path — the RESOLVED, symlink-free path
// anchored under the validated parent — or "" for a nil store.
func (s *Store) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

// coordPath is the coordination lock file's path.
func (s *Store) coordPath() string {
	return filepath.Join(s.root, coordLockName)
}

// Session is one owner's private build-temp directory. Its directory is the
// parent the snapshot creation APIs stage their roots into; its owner lock is
// held for the session's whole lifetime. A Session is not safe to Close
// concurrently with MkdirTemp from the same process unless the caller has first
// quiesced its preparations (the Manager does: CloseContext cancels and drains
// every preparation before closing the session); the internal mutex only makes a
// racing MkdirTemp fail cleanly instead of creating a directory after removal.
type Session struct {
	store *Store

	mu     sync.Mutex
	dir    string
	lock   *processlock.Lock
	closed bool
}

// NewSession creates a fresh session directory under the store's root and takes
// its owner lock, all while holding the coordination lock so a concurrent
// scavenger cannot observe (and remove) the half-created session. It returns the
// live session; the caller owns it and must Close it.
func (s *Store) NewSession(ctx context.Context) (*Session, error) {
	if s == nil {
		return nil, errors.New("buildtemp: nil store")
	}
	coord, err := processlock.AcquireBlocking(ctx, s.coordPath())
	if err != nil {
		return nil, fmt.Errorf("buildtemp: coordinate session creation: %w", err)
	}
	defer coord.Close()

	dir, err := os.MkdirTemp(s.root, sessionPrefix)
	if err != nil {
		return nil, fmt.Errorf("buildtemp: create session: %w", err)
	}
	// AcquireNoFollow: the session directory is ours, but a symlinked lock file
	// must never redirect the flock outside it.
	lock, err := processlock.AcquireNoFollow(filepath.Join(dir, ownerLockName))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("buildtemp: lock session %s: %w", dir, err)
	}
	return &Session{store: s, dir: dir, lock: lock}, nil
}

// Dir returns the session's private directory, or "" once it is closed (or for a
// nil session). Callers pass it as the parent for a build root.
func (s *Session) Dir() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

// MkdirTemp creates a private build root inside the session using pattern
// (exactly like os.MkdirTemp). It fails if the session is already closed, so a
// preparation racing shutdown can never create a root after the session's
// directory was removed.
func (s *Session) MkdirTemp(pattern string) (string, error) {
	if s == nil {
		return "", errSessionClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", errSessionClosed
	}
	return os.MkdirTemp(s.dir, pattern)
}

// Close releases the owner lock and removes the session directory. It is
// idempotent and nil-safe. The coordination lock is taken first when available
// so teardown cannot race a scavenger; if it cannot be taken (a cancelled ctx or
// a filesystem error) the session is still released and removed, because
// removing our own session is never a delete-safety hazard and a concurrent
// scavenger's removal of the same directory is harmless (os.RemoveAll treats an
// already-removed path as success, so no benign race is reported as a failure).
// Removal does not follow symlinks (os.RemoveAll removes them).
func (s *Session) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	dir, lock := s.dir, s.lock
	s.dir, s.lock = "", nil
	s.mu.Unlock()

	if s.store != nil {
		if coord, err := processlock.AcquireBlocking(ctx, s.store.coordPath()); err == nil {
			defer coord.Close()
			return errors.Join(lock.Close(), os.RemoveAll(dir))
		}
	}
	return errors.Join(lock.Close(), os.RemoveAll(dir))
}

// Scavenge removes every session under the root whose owner lock can be
// acquired — that is, every session abandoned by a process that has exited —
// and returns how many were reclaimed.
//
// It holds the coordination lock for the whole scan-and-remove pass, so it can
// never race a session creation (which holds the same lock while it creates and
// owner-locks its directory) and concurrent Relay processes never sweep each
// other's live sessions. It never removes anything based on age or name: only a
// session whose owner lock it can actually take is reclaimed.
//
// It fails closed. If the coordination lock cannot be taken, or the root cannot
// be read, nothing is removed and the error is returned. A candidate session
// whose owner lock cannot be taken (a live owner, or a lock file that cannot be
// opened — e.g. a symlink, rejected by O_NOFOLLOW, a directory, or an unreadable
// mode) is left untouched without failing the pass, and the probe NEVER mutates
// the candidate: it opens the existing lock read-only, creates nothing, and
// chmods nothing. Non-session entries (unrelated files, directories, symlinks)
// are ignored entirely.
//
// A session directory with NO owner lock file is reclaimable because the
// coordination lock held by this scan proves no live session can lack one: see
// probeOwner.
func (s *Store) Scavenge(ctx context.Context) (int, error) {
	if s == nil {
		return 0, errors.New("buildtemp: nil store")
	}
	// Nothing to reclaim if the root does not exist; do NOT create it here.
	if _, err := os.Lstat(s.root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("buildtemp: stat root %s: %w", s.root, err)
	}
	coord, err := processlock.AcquireBlocking(ctx, s.coordPath())
	if err != nil {
		// Fail closed: without the coordination lock, a session may be in the
		// middle of creation, so the scan is unsafe.
		return 0, fmt.Errorf("buildtemp: scavenge coordinate: %w", err)
	}
	defer coord.Close()

	entries, err := os.ReadDir(s.root)
	if err != nil {
		return 0, fmt.Errorf("buildtemp: read root %s: %w", s.root, err)
	}
	removed := 0
	var errs []error
	for _, entry := range entries {
		if !isSessionName(entry.Name()) {
			continue
		}
		dir := filepath.Join(s.root, entry.Name())
		// Lstat (not Stat): a symlink is never a session and is never followed.
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() {
			continue
		}
		lock, reclaimable := probeOwner(dir)
		if !reclaimable {
			// Held by a live owner, or unopenable (a symlink, a directory, an
			// unreadable mode): leave the candidate exactly as found. The probe
			// never creates, chmods, or otherwise mutates it.
			continue
		}
		if rerr := os.RemoveAll(dir); rerr != nil {
			errs = append(errs, fmt.Errorf("buildtemp: remove orphan session %s: %w", dir, rerr))
		} else {
			removed++
		}
		_ = lock.Close() // nil-safe: nil when the owner lock file was absent
	}
	return removed, errors.Join(errs...)
}

// probeOwner reports whether the session directory dir is reclaimable and, when
// it is, returns the owner lock the probe now holds (nil when the owner lock
// file was absent). It never mutates dir: TryAcquireExisting opens the existing
// lock with no O_CREATE and no chmod (and no parent mkdir/chmod), so a candidate
// whose lock cannot be taken — held by a live owner, a symlink, a directory, an
// unreadable mode — is reported not reclaimable and left exactly as found.
//
// A MISSING owner lock is reclaimable only because this scan holds the
// coordination lock: NewSession creates the session directory AND takes its
// owner lock while holding that same lock, so a live session can never lack its
// owner lock. A session observed without one therefore belongs to a process that
// crashed in the create window, and no live process can race its removal.
func probeOwner(dir string) (*processlock.Lock, bool) {
	lock, err := processlock.TryAcquireExisting(filepath.Join(dir, ownerLockName))
	switch {
	case err == nil:
		return lock, true
	case errors.Is(err, processlock.ErrLockMissing):
		return nil, true
	default:
		return nil, false
	}
}

// isSessionName reports whether name is a session directory name (the prefix
// plus a non-empty suffix). It intentionally does not validate the suffix's
// characters: only directories that are also owner-lockable are ever removed.
func isSessionName(name string) bool {
	return len(name) > len(sessionPrefix) && strings.HasPrefix(name, sessionPrefix)
}

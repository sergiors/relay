package reconciler

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"relay/internal/app"
)

// Watch-recovery bounds. A failed recursive registration is retried on one
// timer owned by eventLoop, with exponential backoff capped well below the
// periodic reconcile interval; retries are per-path so one unwatchable subtree
// cannot starve the rest of the tree. Watch-error gap recovery is coalesced:
// fsnotify can report a burst of read/overflow errors, and one rescan plus one
// reconciliation request is enough to cover the resulting delivery gap.
const (
	watchRetryInitial       = 100 * time.Millisecond
	watchRetryMax           = 10 * time.Second
	watchGapRecoveryBackoff = 1 * time.Second
)

// errWatchEventsClosed and errWatchErrorsClosed report a watcher that stopped
// delivering after Start. fsnotify closes its channels on Close; while the
// reconciler is running an unexpected close is terminal (it is not proof that
// only one path lost its watch), so the event loop stops and Start returns the
// error to its caller instead of silently degrading to the periodic pass.
var (
	errWatchEventsClosed = errors.New("fsnotify events channel closed")
	errWatchErrorsClosed = errors.New("fsnotify errors channel closed")
)

// watchEntry is the identity of the inode a path watch currently refers to.
// fsnotify watches inodes, so after an atomic directory exchange the same path
// can map to a different tree. Remembering the identity lets a duplicate
// Create/Rename notification skip a redundant recursive refresh without ever
// skipping a genuinely detached tree.
type watchEntry struct {
	info os.FileInfo
}

// watchRetryState is the bounded-backoff state for one path whose recursive
// registration (or walk) failed. attempts is the number of consecutive
// failures; next is when eventLoop should try again.
type watchRetryState struct {
	attempts int
	next     time.Time
}

// eventLoop forwards fsnotify events into the debounce queue, mapping their paths
// to app names and maintaining watches on created/removed directories. Watch
// maintenance releases a removed/renamed path's old tree before registering the
// currently installed tree at that path, so an atomic per-app directory exchange
// (which can emit duplicate or reordered Rename/Create events) converges on the
// new inode rather than leaving only the detached old tree watched.
//
// It also owns the single bounded-backoff retry timer for directory
// registrations that failed during a walk. Registration, removal, refreshes,
// and retries all run on this goroutine, so the fsnotify watch set and the
// in-process watch index have one writer and cannot race. Filesystem walks and
// fsnotify calls are deliberately outside watchMu, which only guards the small
// retry index.
//
// fsnotify reports recoverable read/overflow errors on Errors (it keeps reading
// afterwards on the current backends), so those trigger a rescan of missing
// watches plus a coalesced full reconcile to cover the events lost during the
// gap. An unexpected close of either channel is terminal: it is logged and sent
// to Start, which tears the loops down and returns the failure to the worker.
func (r *Reconciler) eventLoop() {
	defer r.loops.Done()
	var watchErrors <-chan error = r.w.Errors
	if r.watchErrors != nil {
		watchErrors = r.watchErrors
	}

	// One reusable timer, armed at the earliest pending retry. An unarmed timer
	// exposes a nil channel to the select below.
	retryTimer := time.NewTimer(time.Hour)
	if !retryTimer.Stop() {
		<-retryTimer.C
	}
	var retryC <-chan time.Time
	armRetry := func() {
		if retryC != nil {
			if !retryTimer.Stop() {
				select {
				case <-retryTimer.C:
				default:
				}
			}
			retryC = nil
		}
		next, ok := r.nextWatchRetry()
		if !ok {
			return
		}
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		retryTimer.Reset(delay)
		retryC = retryTimer.C
	}
	defer retryTimer.Stop()
	armRetry()

	var lastGapRecovery time.Time
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.watchWake:
			armRetry()
		case ev, ok := <-r.w.Events:
			if !ok {
				r.watchStopped(errWatchEventsClosed)
				return
			}
			r.handleWatchEvent(ev)
			armRetry()
		case err, ok := <-watchErrors:
			if !ok {
				r.watchStopped(errWatchErrorsClosed)
				return
			}
			// A backend error means delivery is degraded, not necessarily that
			// a particular watch was removed: on inotify a read error is
			// followed by continued reads, and overflow means events were
			// dropped while the watch set itself survives. Re-register anything
			// missing and reconcile all candidates to cover the gap. Coalesce
			// bursts so one error storm cannot cause a walk per message.
			now := time.Now()
			if now.Sub(lastGapRecovery) >= watchGapRecoveryBackoff {
				r.warnWatch("Reconciler: watch error; rescanning", "error", err)
				r.rescanWatches()
				// The walk is synchronous, so throttle from its completion rather
				// than its start. Errors queued during a slow walk then coalesce
				// instead of immediately starting another full traversal.
				lastGapRecovery = time.Now()
				if r.rctx().Err() == nil {
					r.requestResync()
				}
			} else if r.log != nil {
				r.log.Debug("Reconciler: watch error", "error", err)
			}
			armRetry()
		case <-retryC:
			retryC = nil
			r.retryDueWatches(time.Now())
			armRetry()
		}
	}
}

// handleWatchEvent applies one fsnotify event: map it to an app, maintain its
// directory watches, and close the inspect/register race by scheduling a
// reconcile AFTER a refresh that actually (re)registered the current tree. That
// post-refresh enqueue is what makes a change landing between the event handler's
// inspection and the registration converge on the next fingerprint scan. It
// reports whether the event caused a watch refresh.
func (r *Reconciler) handleWatchEvent(ev fsnotify.Event) bool {
	// Chmod events are excluded: permission-only changes don't trigger a
	// rebuild, only actual content/tree changes do.
	if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename|fsnotify.Write) == 0 {
		return false
	}
	if name, affected := r.appForPath(ev.Name); affected && r.shouldEnqueue(name) {
		r.Enqueue(name)
	}
	// Maintain watches on created/removed/renamed directories (recursive
	// watch). A Relay-owned reserved directory (git's ".sync-*") is never
	// watched, and neither will addWatchRecursive descend into it, so a dynamic
	// Create for a stage dir spends no handle; addWatchRecursive itself also
	// prunes reserved subtrees defensively.
	if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 && !r.isReservedDirPath(ev.Name) {
		refreshed := r.refreshWatch(ev.Name)
		if refreshed {
			// A duplicate exchange notification can describe the inode that is
			// already watched; refreshWatch then does nothing. Only a real
			// refresh needs the post-refresh reconcile.
			r.enqueueWatchPath(ev.Name)
		}
		return refreshed
	}
	return false
}

// refreshWatch inspects path and, when it currently names a real directory whose
// watched inode differs from the registered one, drops the detached subtree and
// registers the current tree. It returns true when it performed a refresh. A
// path that is absent, non-directory, symlinked, or reserved has any stale
// watches removed and is not registered; a currently-watched identical inode is
// left alone so duplicate exchange notifications coalesce instead of re-walking
// the tree.
func (r *Reconciler) refreshWatch(path string) bool {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		// Only a previously watched directory can have descendants here; a
		// plain file Create is the common case and must not scan the watch map.
		if _, ok := r.watches.Load(path); ok {
			r.removeWatchRecursive(path)
		}
		return false
	}
	if r.watchIsCurrent(path, info) {
		return false
	}
	r.removeWatchRecursive(path)
	r.addWatchRecursive(path)
	return true
}

// watchIsCurrent reports whether path's registered watch still refers to info's
// inode and remains registered with fsnotify. An entry without a recorded
// identity (a test-constructed reconciler) relies on the backend registration.
func (r *Reconciler) watchIsCurrent(path string, info os.FileInfo) bool {
	v, ok := r.watches.Load(path)
	if !ok {
		return false
	}
	entry, ok := v.(watchEntry)
	if !ok || entry.info == nil || info == nil {
		return r.backendWatchesPath(path)
	}
	return os.SameFile(entry.info, info) && r.backendWatchesPath(path)
}

// backendWatchesPath checks fsnotify's authoritative registration list. This
// check belongs only on the event refresh path, not in addWatchRecursive's full
// tree walk: WatchList is linear in the number of watches.
func (r *Reconciler) backendWatchesPath(path string) bool {
	if r.w == nil {
		return false
	}
	for _, watched := range r.w.WatchList() {
		if watched == path {
			return true
		}
	}
	return false
}

// appForPath maps an event path to the affected app name: the first
// path segment under the root. A Relay-owned transient staging directory
// (app.IsReservedDir, e.g. git's ".sync-*") is never an app, so its
// events are filtered here and no debounce timer is ever armed for it.
func (r *Reconciler) appForPath(path string) (string, bool) {
	name, ok := r.rootChildName(path)
	if !ok || app.IsReservedDir(name) {
		return "", false
	}
	return name, true
}

// shouldEnqueue reports whether an event for name is worth a debounced reconcile.
// Real app directories are enqueued when a template is present or its status is
// ambiguous; known names are also enqueued to observe removals. Unsupported or
// missing root entries are only enqueued when already known, so unrelated files
// and symlinks cannot create bogus app state.
func (r *Reconciler) shouldEnqueue(name string) bool {
	path := filepath.Join(r.root, name)
	realDir, err := app.IsRealAppDir(path)
	if err != nil {
		return true
	}
	if !realDir {
		return r.knownLocally(name)
	}
	if present, err := app.HasTemplate(path); err != nil || present {
		return true
	}
	return r.knownLocally(name)
}

// rootChildName returns the first path segment of p relative to the apps
// root, and whether p is at or below the root with a non-empty relative path.
// It is the shared basis for mapping an event path to an app name and for
// deciding whether a path lies inside a reserved root child, so the two can
// never disagree about which root child a path belongs to.
func (r *Reconciler) rootChildName(p string) (string, bool) {
	rel, err := filepath.Rel(r.root, p)
	if err != nil {
		return "", false
	}
	if rel == "." || rel == "" {
		return "", false
	}
	if i := strings.IndexByte(rel, os.PathSeparator); i >= 0 {
		return rel[:i], true
	}
	return rel, true
}

// isReservedDirPath reports whether p is, or is inside, a Relay-owned reserved
// directory directly under the apps root (app.IsReservedDir). The
// check is on the FIRST path segment under r.root, so a reserved ROOT CHILD and
// all of its descendants are skipped, while a nested real directory whose name
// merely looks reserved (e.g. root/<valid-fn>/.sync-x) is not.
func (r *Reconciler) isReservedDirPath(p string) bool {
	name, ok := r.rootChildName(p)
	return ok && app.IsReservedDir(name)
}

// addWatchRecursive watches root and every subdirectory so events beneath nested
// dirs are seen. A Relay-owned reserved directory directly under the root
// (app.IsReservedDir, git's ".sync-*") and its whole subtree are
// deliberately NOT watched: such a directory is never an app, so watching it
// would only consume inotify handles and surface events that appForPath
// immediately discards. filepath.WalkDir is used with SkipDir to prune the
// reserved subtree, which also means its descendants are never visited.
//
// Symlinked directories are likewise not followed: WalkDir does not follow links
// (d.IsDir() is false for a symlink entry), so a symlink loop inside the tree
// cannot make this unbounded — this is why we use the DirEntry (not os.Stat)
// form of the walk. A symlinked dir is simply never watched, which is
// acceptable.
//
// A failed registration is indexed for bounded-backoff retry rather than being
// only logged. This is what makes a PARTIAL recursive failure recover: the
// walk can continue past one bad directory, every other subtree stays watched,
// and the failing path is retried later. Errors are per-path, so one vanished
// or unreadable directory cannot abandon the whole tree. A path already watched
// at the same inode is skipped; a path whose watched inode was replaced (an
// exchange notification that raced the refresh) drops the stale handle and
// registers the current tree.
func (r *Reconciler) addWatchRecursive(path string) {
	walkDir := r.walkDir
	if walkDir == nil {
		walkDir = filepath.WalkDir
	}
	_ = walkDir(path, func(p string, d os.DirEntry, err error) error {
		if r.rctx().Err() != nil {
			return fs.SkipAll
		}
		if err != nil {
			if r.rctx().Err() != nil {
				return fs.SkipAll
			}
			// A concurrent rename/removal can make a path vanish between the
			// event and this walk. There is nothing to watch in that case; any
			// other traversal error means part of the live tree may be unwatched
			// and must be retried.
			if !errors.Is(err, fs.ErrNotExist) {
				r.recordWatchFailure(p, err)
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		// Prune a reserved root child's whole subtree, including the child
		// itself, so no watch handle is spent there. isReservedDirPath keys on
		// the first segment under the root, so a nested real directory whose
		// name merely looks reserved (root/<valid-fn>/.sync-x) is NOT pruned.
		if p != r.root && r.isReservedDirPath(p) {
			return filepath.SkipDir
		}
		info, infoErr := d.Info()
		if v, ok := r.watches.Load(p); ok {
			if entry, ok := v.(watchEntry); ok && entry.info != nil && infoErr == nil && info != nil {
				if !os.SameFile(entry.info, info) {
					// The path now names a different inode (typically one half
					// of an atomic exchange arriving after a duplicate event or
					// a lost notification). Drop the whole detached subtree
					// before registering the current tree; the ongoing walk
					// re-adds the new descendants, so old detached handles
					// cannot survive at paths absent from the replacement.
					r.removeWatchRecursive(p)
				} else {
					r.clearWatchRetry(p)
					return nil
				}
			} else {
				// No identity to compare (test constructors) or the info read
				// failed: keep the historical presence-only skip.
				r.clearWatchRetry(p)
				return nil
			}
		}
		if r.rctx().Err() != nil {
			return fs.SkipAll
		}
		if err := r.w.Add(p); err != nil {
			if r.rctx().Err() != nil {
				return fs.SkipAll
			}
			// Do not silently claim a complete recursive refresh: without this
			// watch, nested edits below p are invisible until the periodic audit
			// or a retry. Track the exact path so only the failed subtree is
			// retried.
			r.recordWatchFailure(p, err)
			return nil
		}
		r.watches.Store(p, watchEntry{info: info})
		r.clearWatchRetry(p)
		return nil
	})
}

// recordWatchFailure indexes a failed path registration or walk for retry. It
// is a no-op for a path that is now absent, non-directory, symlinked, reserved,
// or outside the root: there is nothing to watch and no reason to spin. A path
// that still names a directory is retried even when the failure was a read
// error, so a transient permission/I/O/handle-limit problem recovers.
//
// Only the first consecutive failure for a path is logged at Warn; later
// attempts are Debug, and recovery is Info. This keeps an unrecoverable subtree
// from flooding the log at every backoff interval.
func (r *Reconciler) recordWatchFailure(path string, err error) {
	if path == "" {
		return
	}
	if !r.watchRetryEligible(path) {
		r.clearWatchRetry(path)
		r.removeWatchRecursive(path)
		return
	}
	now := time.Now()
	r.watchMu.Lock()
	if r.watchRetries == nil {
		r.watchRetries = map[string]*watchRetryState{}
	}
	retry := r.watchRetries[path]
	first := retry == nil
	if retry == nil {
		retry = &watchRetryState{}
		r.watchRetries[path] = retry
	}
	retry.attempts++
	retry.next = now.Add(watchRetryDelay(retry.attempts))
	attempts, next := retry.attempts, retry.next
	r.watchMu.Unlock()

	if first {
		r.warnWatch(
			"Reconciler: watch registration failed; retrying",
			"path", path,
			"error", err,
			"retry_in", time.Until(next),
		)
	} else if r.log != nil {
		r.log.Debug(
			"Reconciler: watch registration retry scheduled",
			"path", path,
			"error", err,
			"attempt", attempts,
			"retry_in", time.Until(next),
		)
	}
	r.wakeWatchRetry()
}

// watchRetryDelay is the bounded exponential backoff for the attempt-th
// consecutive failure (1-based).
func watchRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := watchRetryInitial
	for i := 1; i < attempt; i++ {
		if delay >= watchRetryMax {
			return watchRetryMax
		}
		delay *= 2
	}
	if delay > watchRetryMax {
		delay = watchRetryMax
	}
	return delay
}

// watchRetryEligible reports whether path is still a watch candidate: inside
// the apps root, not a reserved root namespace, and currently a real
// (non-symlink) directory. An inspection error is treated as retryable — a
// permission or transient I/O failure is exactly what the retry exists for —
// while a definite absence, symlink, or non-directory entry discards it.
func (r *Reconciler) watchRetryEligible(path string) bool {
	if path == "" {
		return false
	}
	rel, err := filepath.Rel(r.root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return false
	}
	if path != r.root && r.isReservedDirPath(path) {
		return false
	}
	realDir, err := app.IsRealAppDir(path)
	if err != nil {
		// A path whose parent stopped being a directory can never become a
		// watch again; treat it as gone rather than retrying an ENOTDIR forever.
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return false
		}
		return true
	}
	return realDir
}

// wakeWatchRetry coalesces retry-scheduling notifications to eventLoop.
func (r *Reconciler) wakeWatchRetry() {
	if r.watchWake == nil {
		return
	}
	select {
	case r.watchWake <- struct{}{}:
	default:
	}
}

// clearWatchRetry removes any pending retry for path. It is called on every
// successful registration so a recovered path cannot be removed and re-added
// by an already-armed timer.
func (r *Reconciler) clearWatchRetry(path string) {
	r.watchMu.Lock()
	delete(r.watchRetries, path)
	r.watchMu.Unlock()
}

// nextWatchRetry returns the earliest pending retry deadline.
func (r *Reconciler) nextWatchRetry() (time.Time, bool) {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	var next time.Time
	found := false
	for _, retry := range r.watchRetries {
		if retry == nil {
			continue
		}
		if !found || retry.next.Before(next) {
			next = retry.next
			found = true
		}
	}
	return next, found
}

// retryDueWatches retries every path whose backoff has elapsed. Paths are
// collected under watchMu and processed outside it, so walks never hold the
// lock.
func (r *Reconciler) retryDueWatches(now time.Time) {
	r.watchMu.Lock()
	var due []string
	for path, retry := range r.watchRetries {
		if retry != nil && !retry.next.After(now) {
			due = append(due, path)
		}
	}
	r.watchMu.Unlock()
	for _, path := range due {
		r.retryWatch(path)
	}
}

// retryWatch re-registers one failed path from its CURRENT tree. The subtree is
// dropped first because the failure may describe a detached inode (an exchange
// that happened before or during the failed walk); re-adding without the drop
// would skip stale entries through the presence/identity check. If the path no
// longer qualifies, the retry and any lingering watches are discarded. A
// successful restore schedules a reconcile of the affected app (or a full
// resync when the root itself was restored) so changes made while monitoring
// was degraded are observed immediately rather than at the next period.
func (r *Reconciler) retryWatch(path string) {
	if r.rctx().Err() != nil {
		return
	}
	if !r.watchRetryEligible(path) {
		r.clearWatchRetry(path)
		r.removeWatchRecursive(path)
		return
	}
	attempts, _ := r.watchAttempts(path)
	r.removeWatchRecursive(path)
	r.addWatchRecursive(path)
	if _, ok := r.watches.Load(path); !ok {
		// addWatchRecursive recorded the new failure with a longer backoff.
		return
	}
	r.clearWatchRetry(path)
	if attempts > 0 {
		if r.log != nil {
			r.log.Info(
				"Reconciler: watch registration restored",
				"path", path,
				"attempts", attempts,
			)
		}
	}
	if path == r.root {
		r.requestResync()
		return
	}
	r.enqueueWatchPath(path)
}

// watchAttempts returns the consecutive failure count for path and whether a
// retry was pending.
func (r *Reconciler) watchAttempts(path string) (int, bool) {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	retry, ok := r.watchRetries[path]
	if !ok || retry == nil {
		return 0, false
	}
	return retry.attempts, true
}

// requestResync asks the ticker goroutine for an immediate full discovery
// sweep. It is intentionally non-blocking and coalesced: eventLoop must never
// wait for the pump, and a burst of watch errors needs only one sweep.
func (r *Reconciler) requestResync() {
	if r.resync == nil {
		return
	}
	select {
	case r.resync <- struct{}{}:
	default:
	}
}

// enqueueWatchPath debounces the app containing path, using the same app and
// candidacy policy as an fsnotify event. Paths outside an app (the root itself,
// a reserved staging path, an unknown non-app entry) enqueue nothing.
func (r *Reconciler) enqueueWatchPath(path string) {
	if name, affected := r.appForPath(path); affected && r.shouldEnqueue(name) {
		r.Enqueue(name)
	}
}

// rescanWatches reconciles the in-process watch index with fsnotify's actual
// watch list, then walks the current tree to register anything missing. It is
// the recovery used after a recoverable backend error: a read error does not by
// itself prove that a particular watch disappeared, so the authoritative
// WatchList is consulted instead of blindly re-adding or assuming failure. A
// path that is no longer watched by the backend but still exists is re-added by
// the walk from its current inode; a stale path that vanished stays dropped.
func (r *Reconciler) rescanWatches() {
	if r.w == nil || r.rctx().Err() != nil {
		return
	}
	live := r.w.WatchList()
	if live == nil {
		// The watcher is closed (or closing); the channel/context path owns
		// teardown, and there is no watch set to repair.
		return
	}
	watched := make(map[string]struct{}, len(live))
	for _, path := range live {
		if r.rctx().Err() != nil {
			return
		}
		watched[path] = struct{}{}
	}
	r.watches.Range(func(key, _ any) bool {
		if r.rctx().Err() != nil {
			return false
		}
		path, ok := key.(string)
		if !ok {
			return true
		}
		if _, stillWatched := watched[path]; !stillWatched {
			r.watches.Delete(path)
		}
		return true
	})
	if r.rctx().Err() == nil {
		r.addWatchRecursive(r.root)
	}
}

// watchStopped surfaces a terminal watcher/channel failure. A close racing
// lifecycle cancellation is normal shutdown, not a failure. Otherwise it is
// logged at Error and handed to Start, which joins the loops and returns it.
func (r *Reconciler) watchStopped(err error) {
	if r.rctx().Err() != nil {
		return
	}
	if r.log != nil {
		r.log.Error("Reconciler: watch delivery stopped", "error", err)
	}
	if r.watchTerm == nil {
		return
	}
	select {
	case r.watchTerm <- err:
	default:
	}
}

// warnWatch logs a watch-maintenance failure. It is nil-safe because a few
// tests construct a Reconciler with only the watcher fields.
func (r *Reconciler) warnWatch(msg string, args ...any) {
	if r.log != nil {
		r.log.Warn(msg, args...)
	}
}

// removeWatchRecursive drops the watch for path and for every descendant watch
// registered beneath it (a parent-directory removal implicitly removes children,
// so their handles would otherwise leak). It mirrors addWatchRecursive's walk.
func (r *Reconciler) removeWatchRecursive(path string) {
	prefix := path
	if prefix != "" && !strings.HasSuffix(prefix, string(os.PathSeparator)) {
		prefix += string(os.PathSeparator)
	}
	var toRemove []string
	r.watches.Range(func(key, _ any) bool {
		p := key.(string)
		if p == path || strings.HasPrefix(p, prefix) {
			toRemove = append(toRemove, p)
		}
		return true
	})
	for _, p := range toRemove {
		_ = r.w.Remove(p)
		r.watches.Delete(p)
	}
}

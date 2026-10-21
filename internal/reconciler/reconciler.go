package reconciler

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
	"relay/internal/state"
)

// Defaults for debounce and periodic reconciliation. They are fixed application
// constants; Config fields override them when non-zero (for tests).
const (
	DefaultDebounce  = 750 * time.Millisecond
	DefaultInterval  = 30 * time.Second
	DefaultQueueSize = 64
)

// Builder prepares an app image and executes invocations against it. It is
// a concrete-need interface so reconcile logic can be unit-tested without Docker
// or a Redis stream; the runtime Manager satisfies it in production.
type Builder interface {
	Prepare(
		ctx context.Context,
		fn app.App,
	) (*runtime.Prepared, error)
	Execute(
		ctx context.Context,
		prepared *runtime.Prepared,
		handler string,
		eventJSON []byte,
		extraEnv []string,
	) error
}

// selectionPreparer is the OPTIONAL extension of Builder implemented by the
// runtime Manager (but not by the test fakes). Its method accepts the source
// selection the reconciler already resolved to compute the app's
// fingerprint, so a rebuild does not re-derive the selection between the hash
// and the build: the tag the reconciler compared and the bytes the build stages
// come from one policy read. A Builder that does not implement it keeps the
// plain Prepare behavior; the reconciler falls back to Prepare for those.
type selectionPreparer interface {
	PrepareWithFingerprintAndSelection(
		ctx context.Context,
		fn app.App,
		fingerprint string,
		selection *source.Selection,
	) (*runtime.Prepared, error)
}

// resourceSetter is the OPTIONAL extension of Builder implemented by the runtime
// Manager (but not by the test fakes). Resource limits deliberately do not
// participate in the image fingerprint, so a resource-only template change does
// NOT trigger a rebuild and therefore never reaches Prepare. The reconciler
// calls SetAppResources on its unchanged-fingerprint skip path so the live
// warm pool rotates containers to the new limits (idle ones discarded, busy ones
// drained) without touching the image. A Builder that does not implement it
// (fakes, non-Manager builders) ignores the change, exactly as before.
type resourceSetter interface {
	SetAppResources(name string, limits app.ResourceLimits)
}

// Config tunes the reconciler. A zero value applies the package defaults.
type Config struct {
	// Root is the apps root. Required.
	Root string
	// Debounce is how long an event storm for one app waits before its
	// single reconcile fires. Defaults to DefaultDebounce.
	Debounce time.Duration
	// Interval is the periodic reconciliation period, a backstop for watches that
	// miss events. Defaults to DefaultInterval.
	Interval time.Duration
	// Fingerprint, when non-nil, replaces the package-level
	// app.SelectAndFingerprintApp that reconcileApp uses to derive
	// an app's source selection and content fingerprint. It is a narrow test
	// seam (production leaves it nil) so a test can count how many times ONE
	// reconcile resolves an app's identity without touching the filesystem.
	Fingerprint func(dir string, tmpl *app.Template) (*source.Selection, string, error)
	// State is an optional state-view sink. When non-nil, reconcile outcomes
	// (discovered/updated/removed/failed/skipped) are recorded in it; when nil
	// the reconciler behaves exactly as before (no state writes). Errors from
	// state calls are logged, never fatal.
	State *state.State
	// Retire, when set, is called after an app's registry entry is swapped
	// to a new image AND its persistent service containers have been converged
	// to that new image, passing the app name and the superseded image
	// reference (the version being replaced). Retiring after service converge
	// guarantees a running service container on the old image has been replaced
	// before the old image is retire-eligible; the runner-side reference guard is
	// the second line of defense for partial failures. It is how the runner
	// learns to retire an image once it is no longer in use. Nil-safe; a stale
	// or equal image is skipped by the caller. Ignoring the name is fine — the
	// image reference already embeds it.
	Retire func(name, oldImage string)
	// RemoveApp, when set, is called after an app directory vanishes
	// (and its registry entry and state are dropped) so the runner can retire
	// every version of that app's images. Nil-safe.
	RemoveApp func(name string)
	// Audit, when set, is called once per periodic audit pass (the ticker's
	// missed-event backstop; see ticker), before that pass reconciles the apps.
	// It is the lifecycle re-drive hook for DEFERRED image cleanup: the runner
	// retains retirement ownership for an image whose bounded removal retries
	// were exhausted (e.g. a service container still referenced it) or whose
	// removal failed, and a periodic pass re-drives removal once the reference
	// state may have changed. It runs on the ticker goroutine and must not block
	// (the runner's re-drive only admits asynchronous cleanup). Nil-safe.
	Audit func()
	// UpdateSchedules, when set, is called after an app's new version is
	// prepared and swapped into the registry (both discovery and update paths;
	// never on the skip path), so the scheduler can converge its cron jobs to
	// the template's schedules. Nil-safe. It does NOT purge previously-published
	// schedule dedup keys or stream entries: those represent occurrences valid
	// when published and expire via the key TTL / stream retention.
	UpdateSchedules func(name string, tmpl *app.Template)
	// UpdateServices, when set, is called after an app's new version is
	// prepared and swapped into the registry (discovery and update paths; never
	// on the skip path and never on build failure), so the service reconciler
	// (services.go) can converge the app's persistent containers to the new
	// template+image.
	// It is ALSO called on the skip path (unchanged, already-available app)
	// when the app declares services, so crashed service replicas are
	// recreated within the periodic reconcile cadence without a separate
	// services-only loop — Reconcile is idempotent, so this is a cheap no-op
	// when converged. Nil-safe.
	//
	// It is the STATUS-LESS form: it reports no reconcile-start/complete
	// lifecycle to the state sink. The production worker wires
	// UpdateServicesWithStatus / UpdateServicesObservationWithStatus instead, so
	// this one is the fallback for a Builder, test, or embedding host that wants
	// service convergence without status reporting. When any status-aware hook is
	// set it takes precedence (see reconcileApp), so the two are never both
	// invoked for one convergence.
	UpdateServices                      func(name string, tmpl *app.Template, image string)
	UpdateServicesWithStatus            func(name string, tmpl *app.Template, image string, onReconcileStart func(), onComplete func(error))
	UpdateServicesObservationWithStatus func(name string, tmpl *app.Template, image string, onReconcileStart func(), onComplete func(error))
	// RemoveServices, when set, is called in remove() immediately BEFORE
	// RemoveApp and the app's images are retired. The ordering
	// invariant: running service containers reference the app's images, so
	// those containers must be stopped and removed before the images are
	// retire-eligible. Nil-safe.
	RemoveServices func(name string)
}

// Watches Root, debounces per-app events, and swaps the registry when a
// app's content fingerprint changes.
type Reconciler struct {
	root     string
	debounce time.Duration
	interval time.Duration

	reg     *runner.Registry
	builder Builder
	log     *slog.Logger
	st      *state.State
	// fingerprint resolves an app's selection and content fingerprint. It
	// defaults to app.SelectAndFingerprintApp and is overridden by
	// Config.Fingerprint for tests that need to observe (or count) the ONE
	// resolution per reconcile.
	fingerprint func(dir string, tmpl *app.Template) (*source.Selection, string, error)
	// retire/removeApp/updateSchedules/updateServices/removeServices are
	// optional image-lifecycle, schedule-convergence, and service-convergence
	// hooks (see Config).
	retire                              func(name, oldImage string)
	removeApp                           func(name string)
	audit                               func()
	updateSchedules                     func(name string, tmpl *app.Template)
	updateServices                      func(name string, tmpl *app.Template, image string)
	updateServicesWithStatus            func(name string, tmpl *app.Template, image string, onReconcileStart func(), onComplete func(error))
	updateServicesObservationWithStatus func(name string, tmpl *app.Template, image string, onReconcileStart func(), onComplete func(error))
	removeServices                      func(name string)

	mu           sync.Mutex
	fingerprints map[string]string // name -> last-reconciled fingerprint
	generations  map[string]uint64
	// fencingTokens carries the PostgreSQL-authoritative fencing token returned
	// by the desired-generation write that started each app's current
	// generation. It is deliberately SEPARATE from generations: generations is a
	// process-local counter used only to short-circuit this Reconciler's own
	// stale callbacks, while fencingTokens is the cross-worker fence persisted in
	// apps.fencing_token. A zero token means no guarded write may land.
	fencingTokens map[string]state.FencingToken
	timers        map[string]*time.Timer

	incoming chan string   // debounced, per-app trigger queue
	done     chan struct{} // closed on shutdown to unblock pump/timer sends

	ctx     context.Context
	w       *fsnotify.Watcher
	watches sync.Map // dir path -> struct{} for tracked watches

	// loops tracks the pump, ticker, and eventLoop goroutines Start launches, so
	// Start returns only after every loop has exited. This is what lets the
	// worker join the reconciler before closing the runtime manager and the state
	// DB: no reconcile can be pumped into a closed resource.
	loops sync.WaitGroup
}

// New builds a Reconciler. The registry must already be populated with the
// startup-loaded apps (available or not) so reconciliation can compare
// against and swap them.
func New(cfg Config, reg *runner.Registry, builder Builder, logger *slog.Logger) *Reconciler {
	if cfg.Debounce == 0 {
		cfg.Debounce = DefaultDebounce
	}

	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}

	// The identity seam defaults to the package fingerprint entry point; a test
	// may inject one to observe the single per-reconcile resolution.
	fingerprint := cfg.Fingerprint
	if fingerprint == nil {
		fingerprint = app.SelectAndFingerprintApp
	}

	return &Reconciler{
		root:                                cfg.Root,
		debounce:                            cfg.Debounce,
		interval:                            cfg.Interval,
		reg:                                 reg,
		builder:                             builder,
		log:                                 logger,
		st:                                  cfg.State,
		fingerprint:                         fingerprint,
		retire:                              cfg.Retire,
		removeApp:                           cfg.RemoveApp,
		audit:                               cfg.Audit,
		updateSchedules:                     cfg.UpdateSchedules,
		updateServices:                      cfg.UpdateServices,
		updateServicesWithStatus:            cfg.UpdateServicesWithStatus,
		updateServicesObservationWithStatus: cfg.UpdateServicesObservationWithStatus,
		removeServices:                      cfg.RemoveServices,
		fingerprints:                        map[string]string{},
		generations:                         map[string]uint64{},
		fencingTokens:                       map[string]state.FencingToken{},
		timers:                              map[string]*time.Timer{},
		incoming:                            make(chan string, DefaultQueueSize),
		done:                                make(chan struct{}),
	}
}

// Seed records the fingerprint for a currently-loaded app so the first
// reconcile pass does not rebuild an app that was already prepared at
// startup. The fingerprint is SUPPLIED by the caller: it is the identity the
// prepared image was ACTUALLY built from (runtime.Prepared.Fingerprint, derived
// from the source snapshot Prepare captured), so Seed never re-reads the source
// tree and the seed describes what is really baked even if the tree changed
// during the build.
//
// It is called once during wiring, after PrepareWatch has established change
// detection and before Start. A change that lands between the caller's
// fingerprint computation and PrepareWatch, or DURING the build, is still
// detected: the first reconcile rescans the tree and finds it different from the
// recorded built identity, so it observes the change as a rebuild — never a
// missed update. An empty value is stored as-is; because no real fingerprint is
// empty, the first reconcile simply treats the app as changed and rebuilds
// it (the desired behavior for an unprepared app).
func (r *Reconciler) Seed(fn app.App, fingerprint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fingerprints[fn.Name] = fingerprint
}

// SeedFencingToken records the PostgreSQL fencing token returned by the exact
// startup discovery write for name. It seeds the cross-worker fence for the
// first live pass (notably the unchanged-service observation path) and must come
// from the caller's own discovery/persisting result — never from a later read of
// apps.fencing_token, which could claim another worker's generation. A zero
// token clears any seeded value, so guarded writes stay suppressed rather than
// guessing.
func (r *Reconciler) SeedFencingToken(name string, fencingToken state.FencingToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fencingToken > 0 {
		r.fencingTokens[name] = fencingToken
		return
	}
	delete(r.fencingTokens, name)
}

// PrepareWatch creates the fsnotify watcher and installs the recursive watches
// SYNCHRONOUSLY, without starting the background loops. It is the ordering seam
// the worker uses to establish change detection BEFORE trusting the supplied
// startup fingerprints: the worker calls PrepareWatch, then Seed for each
// startup app, then Start. Any change after the watch is installed is
// observed as an fsnotify event; a change between the startup fingerprint scan
// and this point is also caught, because Seed stores the identity the image was
// ACTUALLY built from (the snapshot capture inside Prepare), and the first
// reconcile rescans the live tree and finds it different.
//
// It is idempotent: a watcher already prepared (Start called after
// PrepareWatch, or a second PrepareWatch) is reused as-is. An error creating the
// watcher is returned and leaves the reconciler unwatched; Start logs it and
// returns without starting the loops, matching the historical fsnotify-failure
// behavior.
func (r *Reconciler) PrepareWatch(ctx context.Context) error {
	r.ctx = ctx
	if r.w != nil {
		return nil
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	r.w = w
	r.addWatchRecursive(r.root)
	return nil
}

// Start runs the watch loop, the debounce pump, and the periodic ticker in
// background goroutines until ctx is cancelled, then JOINS them before
// returning. It is intended to be called concurrently with the stream consumer;
// the caller can therefore use Start's return (or the worker's reconciler
// shutdown step) as a barrier that no further reconcile is in flight.
//
// Start first calls PrepareWatch, so a caller that already established the
// watch (the worker, to seed the supplied fingerprints safely) reuses it rather
// than installing a second one; a caller that did not (tests, standalone
// callers) gets the watcher created here.
func (r *Reconciler) Start(ctx context.Context) {
	if err := r.PrepareWatch(ctx); err != nil {
		r.log.Error("Reconciler: fsnotify error", "error", err)
		return
	}

	r.loops.Add(3)
	go r.pump()
	go r.ticker()
	go r.eventLoop()

	<-ctx.Done()
	// Close done FIRST: it releases a pump parked on an empty queue (or a
	// dispatch blocked on a full queue) so the pump observes shutdown and stops.
	close(r.done)
	_ = r.w.Close()
	// Stop any still-pending debounce timers so they cannot fire and dispatch a
	// stale name after we've begun tearing down.
	r.mu.Lock()
	for name, t := range r.timers {
		t.Stop()
		delete(r.timers, name)
	}
	r.mu.Unlock()
	// Join every loop. After this, no reconcile can be dispatched into the
	// runtime manager or the state DB, so the worker may close them.
	r.loops.Wait()
}

// Enqueue debounces an event for name: only one reconcile fires after the
// debounce window, coalescing rapid editor saves.
func (r *Reconciler) Enqueue(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.timers[name]; ok {
		t.Reset(r.debounce)
		return
	}
	t := time.AfterFunc(r.debounce, func() {
		r.dispatch(name)
	})
	r.timers[name] = t
}

// dispatch feeds an app into the single reconciler goroutine (the pump).
// The send blocks when the queue is full (there is no default case): the caller
// is a debounce timer goroutine, so backpressure simply delays that timer
// rather than dropping the reconcile. Both the debounce timers and the periodic
// pass converge here so no two reconciles of the same app ever run
// concurrently. incoming is never closed; done lets a sender parked on a full
// queue unblock at shutdown. If both cases are ready the select chooses at
// random, so done is not strictly prioritized, but either outcome is safe: a
// timer firing during teardown either enqueues into a queue the pump will stop
// draining, or returns without sending. Neither panics on a closed channel.
func (r *Reconciler) dispatch(name string) {
	select {
	case r.incoming <- name:
	case <-r.done:
	}
}

// pump consumes app names serially, so distinct apps can queue
// independently but no app is ever reconciled twice concurrently. It exits
// when done is closed, without relying on incoming ever being closed.
func (r *Reconciler) pump() {
	defer r.loops.Done()
	for {
		select {
		case name := <-r.incoming:
			r.mu.Lock()
			if t := r.timers[name]; t != nil {
				t.Stop()
				delete(r.timers, name)
			}
			r.mu.Unlock()
			r.reconcileApp(name)
		case <-r.done:
			return
		}
	}
}

// ticker periodically reconciles every known app, catching events the
// watcher missed. This periodic audit INTENTIONALLY re-hashes each app's
// selected content (reconcileApp's SelectAndFingerprintApp) even when
// no fsnotify event fired: it is the backstop for a missed create/add/remove,
// an fsnotify overflow, or a transient "unavailable" retry, and it is what makes
// change detection correct on Docker Desktop bind mounts, where fsnotify
// delivery is unreliable and stat-metadata-only schemes (mtime/size) miss
// same-size rapid edits. Every optimization in this file leaves that periodic
// hash in place; a no-event or metadata-only assumption would weaken
// correctness, so it is deliberately retained. An unchanged audit finds the
// same fingerprint and skips (no build), so the cost is the single read the
// backstop exists to perform.
//
// Each pass first fires the Config.Audit hook (when set): it is the re-drive
// point for deferred image cleanup, so an image whose bounded removal retries
// were exhausted while a service container still referenced it is removed once
// the reference state changes, without a timer or goroutine per image.
func (r *Reconciler) ticker() {
	defer r.loops.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.audit != nil {
				r.audit()
			}
			r.reconcileAll()
		}
	}
}

// eventLoop forwards fsnotify events into the debounce queue, mapping their paths
// to app names and maintaining watches on created/removed directories. Watch
// maintenance releases a removed/renamed path's old tree before registering the
// currently installed tree at that path, so an atomic per-app directory exchange
// (which can emit duplicate or reordered Rename/Create events) converges on the
// new inode rather than leaving only the detached old tree watched.
func (r *Reconciler) eventLoop() {
	defer r.loops.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case ev, ok := <-r.w.Events:
			if !ok {
				return
			}
			// Chmod events are excluded: permission-only changes don't trigger a
			// rebuild, only actual content/tree changes do.
			if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename|fsnotify.Write) == 0 {
				continue
			}
			if name, affected := r.appForPath(ev.Name); affected && r.shouldEnqueue(name) {
				r.Enqueue(name)
			}
			// Release watches for a removed or renamed (moved-away) directory
			// FIRST. fsnotify watches inodes, so after an atomic per-app exchange
			// the path-keyed watches still refer to the DETACHED old tree; they
			// must be dropped before the current tree at the same path can be
			// registered. Doing this first also makes a Rename+Create notification
			// pair idempotent instead of leaving only the old inode watched.
			if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
				r.removeWatchRecursive(ev.Name)
			}
			// Maintain watches on created/removed/renamed directories (recursive
			// watch). A Relay-owned reserved directory (git's ".sync-*") is never
			// watched, and neither will addWatchRecursive descend into it, so a
			// dynamic Create for a stage dir spends no handle; addWatchRecursive
			// itself also prunes reserved subtrees defensively.
			//
			// A Create (or a Remove for a path that is already occupied again) can
			// describe the NEW inode installed at a path that is already watched:
			// that is one half of an atomic exchange (possibly duplicated), not
			// necessarily a brand-new directory. Re-register from the currently
			// installed tree, removing first so a stale detached-inode watch (and
			// any of its old descendants) cannot survive and duplicates cannot leak
			// handles. The removal above may already have done this; repeating it
			// is a no-op. A genuinely removed path fails the stat and is not added.
			if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 && !r.isReservedDirPath(ev.Name) {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					r.removeWatchRecursive(ev.Name)
					r.addWatchRecursive(ev.Name)
				}
			}
		case err, ok := <-r.w.Errors:
			if !ok {
				return
			}
			r.log.Warn("Reconciler: watch error", "error", err)
		}
	}
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

// shouldEnqueue reports whether an event for name is worth a debounced reconcile:
// the directory currently has a template.yaml (a creation or edit), or the name
// is already known locally (so a template deletion/rename is observed as a
// removal). A directory that has never been an app and still has no template
// never arms a timer, so writes to unrelated files there (a README, say) cannot
// cause reconcile churn.
func (r *Reconciler) shouldEnqueue(name string) bool {
	if present, err := app.HasTemplate(filepath.Join(r.root, name)); err == nil && present {
		return true
	}
	return r.knownLocally(name)
}

// knownLocally reports whether this Reconciler already holds state for name: an
// active or pending registry entry, or a fencing token from startup
// discovery/invalid persistence. It deliberately consults no database:
// PostgreSQL is a view and must never decide whether an app is removed.
func (r *Reconciler) knownLocally(name string) bool {
	if r.reg != nil && (r.reg.GetByName(name) != nil || r.reg.HasPending(name)) {
		return true
	}
	r.mu.Lock()
	_, ok := r.fencingTokens[name]
	r.mu.Unlock()
	return ok
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
// A failed watch registration is logged with its path. It is not reported as a
// completed refresh: the periodic reconciliation remains the backstop until the
// path is registered successfully (for example on a later Create/Rename).
func (r *Reconciler) addWatchRecursive(path string) {
	_ = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			// A concurrent rename/removal can make a path vanish between the
			// event and this walk. There is nothing to watch in that case; any
			// other traversal error means part of the live tree may be unwatched
			// and must be surfaced.
			if !errors.Is(err, fs.ErrNotExist) {
				r.warnWatch("Reconciler: watch refresh failed", "path", p, "error", err)
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
		if _, ok := r.watches.Load(p); ok {
			return nil
		}
		if err := r.w.Add(p); err != nil {
			// Do not silently claim a complete recursive refresh: without this
			// watch, nested edits below p are invisible until the periodic audit.
			r.warnWatch("Reconciler: watch registration failed", "path", p, "error", err)
			return nil
		}
		r.watches.Store(p, struct{}{})
		return nil
	})
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

// reconcileAll discovers current app candidates and reconciles each, so periodic
// checks also handle removal (a name present in the registry/state tokens but no
// longer an app candidate on disk) and retry previously failed builds. A direct
// child directory is a candidate only when template.yaml is present; a
// template-less directory is not an app and is handled by the missing-name sweep
// when it was previously known. Each candidate is dispatched through the same
// single pump so reconciles stay serialized per app.
func (r *Reconciler) reconcileAll() {
	seen := map[string]bool{}
	entries, err := os.ReadDir(r.root)
	if err != nil {
		r.log.Warn(
			"Reconciler: read root",
			"root", r.root,
			"error", err,
		)
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if app.IsReservedDir(name) {
			// A Relay-owned transient staging directory (git's ".sync-*") is
			// never an app and never a desired definition: it is neither
			// dispatched for reconcile nor recorded as seen, so it cannot
			// produce a warning or an invalid state row while it briefly exists.
			continue
		}
		present, statErr := app.HasTemplate(filepath.Join(r.root, name))
		if statErr != nil {
			// An ambiguous template stat is not proof of removal. Keep the name
			// seen and let the normal load classify it (a flaky read is recorded
			// as an invalid desired state and retains the active generation)
			// instead of dispatching it through the removal sweep.
			seen[name] = true
			r.dispatch(name)
			continue
		}
		if !present {
			// No template.yaml: not an app candidate. A previously-known name is
			// dispatched by missingNames as a removal.
			continue
		}
		seen[name] = true
		r.dispatch(name)
	}
	// Apps still registered, pending, or carrying a fencing token but no
	// longer candidates on disk were removed. A brand-new app whose first
	// preparation has not yet completed exists ONLY as a pending desired entry
	// (nothing is published as active until the build succeeds), so PendingNames
	// must be swept too: otherwise a directory that vanishes during preparation
	// would leave a stale pending entry gating events forever. Fencing tokens
	// cover a name that only ever had a startup invalid-desired row and therefore
	// never entered the registry.
	for _, name := range r.missingNames(seen) {
		r.dispatch(name)
	}
}

// missingNames returns the locally-known names (active, pending-only, or
// carrying a startup fencing token) that are NOT app candidates on disk, so
// reconcileAll dispatches them for removal. Fencing tokens are included so a
// name that only ever had a startup invalid-desired row — and therefore never
// entered the registry — is still removed once its definition disappears. An app
// that is BOTH active and pending (an available generation being rebuilt) is
// returned once.
func (r *Reconciler) missingNames(seen map[string]bool) []string {
	var out []string
	added := map[string]bool{}
	add := func(name string) {
		if seen[name] || added[name] {
			return
		}
		added[name] = true
		out = append(out, name)
	}
	for _, name := range r.reg.Names() {
		add(name)
	}
	for _, name := range r.reg.PendingNames() {
		add(name)
	}
	r.mu.Lock()
	fencingTokenNames := make([]string, 0, len(r.fencingTokens))
	for name := range r.fencingTokens {
		fencingTokenNames = append(fencingTokenNames, name)
	}
	r.mu.Unlock()
	for _, name := range fencingTokenNames {
		add(name)
	}
	return out
}

// reconcileApp is the core decision point for one app. It is serialized
// per name by the pump; when called directly (Reconcile/reconcileAll) callers
// coordinate it.
func (r *Reconciler) reconcileApp(name string) {
	// A Relay-owned transient staging directory (git's ".sync-*") is never a
	// app and never a desired definition. It is filtered before dispatch
	// (appForPath, reconcileAll), but guard here too so a direct/stale
	// caller can never turn a stage directory into a warning, an invalid state
	// row, or a removal of a healthy app. It is deliberately not treated as
	// ErrInvalidPath (which would record an invalid desired definition).
	if app.IsReservedDir(name) {
		return
	}
	// LoadSingle enforces the SAME path policy as startup discovery (a legal
	// single-element name, a real direct child of the root, never a symlink), so
	// a reload can never read, fingerprint, or build a path the startup loader
	// would have rejected. A missing directory wraps fs.ErrNotExist (a removal);
	// a directory without template.yaml is ErrNotReady (not an app candidate); an
	// invalid path is ErrInvalidPath and RETAINS the previously-loaded version
	// rather than letting an invalid path remove or replace a healthy app.
	fn, err := app.LoadSingle(r.root, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Directory gone -> remove from registry. In-flight invocations keep
			// the old snapshot; they are not killed.
			r.remove(name)
			return
		}
		if errors.Is(err, app.ErrNotReady) {
			// The directory has no template.yaml, so it is not an app candidate.
			// If this Reconciler already knows the name (an active/pending
			// registry entry, or a startup fencing token for an invalid row),
			// losing its template is a normal removal. A never-app directory
			// holding only unrelated files is a no-op: no warning, no invalid
			// state write, no generation churn.
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
		// Every other load failure (invalid path, flaky read, unreadable/invalid
		// template) means the desired definition is not a valid generation to
		// prepare, so no desired pending rules are manufactured. Any stale
		// pending entry from a previous, now-invalid desired state is cleared so
		// removed rules cannot keep gating events; the previously-active
		// generation (if any) is retained exactly as today.
		r.reg.ClearPending(name)
		if errors.Is(err, app.ErrInvalidPath) {
			// An invalid path (bad name, symlink, non-directory, not a direct
			// child) can never be an app; retain any previously-loaded
			// version rather than removing or replacing it. The present invalid
			// desired definition is recorded so its stale ready view is not left
			// behind.
			r.log.Warn(
				"App: invalid path; retaining previous version",
				"app", name,
				"error", err,
			)
			r.recordInvalidDesired(name, err)
			return
		}
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// app, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched).
		r.log.Warn(
			"App: load error; retaining previous version",
			"app", name,
			"error", err,
		)
		r.recordInvalidDesired(name, err)
		return
	}
	dir := fn.Dir

	// Publish the desired generation's event rules into the registry as PENDING
	// at the earliest safe point after the template parsed, BEFORE the
	// (potentially long) source fingerprint walk and image build. Matching is
	// registry-only, so without this a delivery arriving during first
	// preparation of a brand-new app (or during the rebuild of an existing one)
	// would match nothing and be ACKed as unmatched — the event-loss window this
	// closes. The pending representation is non-runnable (NewUnavailable): it
	// contributes matched-but-unavailable invocations so the message stays
	// pending, never executes, claims a TryStart, exhausts, ACKs, or DLQs.
	//
	// The representation depends on the CURRENT active entry:
	//   - available active generation (v1 being rebuilt to v2): keep v1 active
	//     and executable, and add v2's rules as pending, so v1-only rules run and
	//     v2-only rules hold.
	//   - unavailable active placeholder (a startup/previous build failure, no
	//     image): replace it in place with the current desired template so its
	//     unavailable rules are coherent (a removed old rule stops gating) and
	//     clear any pending entry. There is no executable generation to preserve.
	//   - absent (brand-new app): add the desired rules as pending only; nothing
	//     is published as active until preparation succeeds.
	//
	// A template with no event rules (a service-only/no-runtime app) has nothing
	// to hold, so any stale pending entry is cleared instead: service behavior is
	// never disturbed and no empty pending entry lingers.
	cur := r.reg.GetByName(name)
	switch {
	case cur != nil && !isAvailable(cur):
		r.reg.Replace(name, runner.NewUnavailable(fn))
	case len(fn.Template.Events) == 0:
		r.reg.ClearPending(name)
	default:
		r.reg.SetPending(name, fn)
	}

	// Compute the fingerprint AND (for a runtime-backed app) the resolved
	// source selection in ONE traversal, then carry BOTH into the rebuild below.
	// The selection is handed to the Manager's selection-aware Prepare so the
	// build stages exactly the source this hash was derived from, with no second
	// policy read that a concurrent .gitignore edit could diverge. For a
	// no-runtime template the selection is nil (no image is built) and the
	// template-only fingerprint is passed through untouched. The resolver is the
	// injected seam (production: app.SelectAndFingerprintApp), invoked
	// exactly once per reconcile.
	selection, fp, err := r.fingerprint(dir, fn.Template)
	if err != nil {
		r.log.Warn(
			"App: fingerprint error; retaining previous version",
			"app", name,
			"error", err,
		)
		// The desired definition is present and parseable but its source could
		// not be fingerprinted, so the desired generation cannot be trusted.
		// Record the failed view while retaining the active generation and
		// leaving the runtime registry untouched. The pending desired rules
		// published above are deliberately RETAINED: the configuration is valid
		// and preparation may succeed on a later pass, so holding matching
		// events pending (rather than ACKing them as unmatched) is the
		// at-least-once direction.
		r.recordInvalidDesired(name, err)
		return
	}

	r.mu.Lock()
	known, hasFingerprint := r.fingerprints[name]
	r.mu.Unlock()

	// Skip a rebuild only when the current build is healthy AND content is
	// unchanged. A previously-failed build (unavailable) is retried even if the
	// fingerprint is stable, so a broken app recovers without edits.
	if cur != nil && isAvailable(cur) && hasFingerprint && known == fp {
		// No image transition is needed, so the pending desired rules published
		// above (which, for a byte-identical template, are the same rules as the
		// active generation) are cleared: no stale pending state may linger.
		r.reg.ClearPending(name)
		// Skipped checks are deliberately NOT persisted as the last reconcile:
		// RecordReconcileSuccess/Failure record the last MEANINGFUL operation and
		// its timestamp; a periodic no-op must not hide a recent success or
		// failure. It is surfaced in debug logging only.
		r.log.Debug(
			"App: unchanged; reconcile skipped",
			"app", name,
		)
		// Resource limits do not participate in the image fingerprint, so a
		// resource-only template change lands on this skip path. Publish the
		// current effective limits to the live warm pool so containers rotate to
		// them (idle discarded, busy drained) without a rebuild. The runtime
		// no-ops an unchanged value, so a truly-unchanged periodic tick does not
		// churn. Non-Manager builders ignore it.
		if rs, ok := r.builder.(resourceSetter); ok {
			rs.SetAppResources(name, fn.Template.ResourceLimits())
		}
		// A skip path is NOT a full no-op when the app declares services:
		// without converging here, a crashed service replica would only be
		// repaired on the next content change. Reconcile is idempotent — when
		// the desired set is already running it lists containers once and
		// touches nothing — so calling updateServices on every periodic tick is
		// cheap and gives crash replacement within the default cadence without
		// a separate services-only loop. updateServices is only reachable when
		// the current build is available (this skip branch already guarantees
		// cur.Prepared() != nil via isAvailable). When that converge pass is a
		// no-op (nothing to stop or start), the caller logs it at Debug rather
		// than Info — the summary line only surfaces real state changes.
		if (r.updateServices != nil || r.updateServicesWithStatus != nil || r.updateServicesObservationWithStatus != nil) && len(fn.Template.Services) > 0 {
			if r.updateServicesWithStatus == nil && r.updateServicesObservationWithStatus == nil {
				r.updateServices(name, fn.Template, cur.Prepared().Image)
			} else {
				// This is an observation of the current source generation, not a
				// new desired generation. Its status callbacks must not invalidate
				// meaningful source work that is still completing. They reuse the
				// fencing token from that generation's desired-state write: no new
				// token is allocated, and a newer worker's token makes them stale.
				generation, fencingToken := r.generationAndFencingToken(name)
				// This is a periodic VERIFICATION of an unchanged, already-
				// available app: it must not claim that a new generation is
				// being prepared. No RecordPreparing here — the public status is
				// only moved to preparing when an actual desired-generation change
				// is detected (the rebuild path below).
				//
				// worked tracks whether the service pass actually did anything:
				// onReconcileStart fires only when real corrective container work
				// begins (see services.go). A no-op verification leaves it false,
				// so the completion below writes NOTHING and ready,
				// last_reconcile_status, and updated_at stay untouched. A pass that
				// did real corrective convergence ends ready. The callback is
				// per-generation and generation-guarded.
				worked := false
				updateStatus := r.updateServicesObservationWithStatus
				if updateStatus == nil {
					updateStatus = r.updateServicesWithStatus
				}
				updateStatus(name, fn.Template, cur.Prepared().Image,
					func() {
						worked = true
						if r.st != nil && r.currentGeneration(name, generation) {
							r.st.RecordReconciling(r.rctx(), name, fencingToken)
						}
					}, func(err error) {
						if r.st == nil || !r.currentGeneration(name, generation) {
							return
						}
						if err != nil {
							r.st.RecordServiceFailure(r.rctx(), name, err, fencingToken)
							return
						}
						if !worked {
							// Nothing was converged: a no-op verification must not
							// rewrite last_reconcile_status/last_reconcile_at or
							// touch updated_at.
							return
						}
						r.st.RecordReconcileSuccess(r.rctx(), name, cur.Prepared().Image, known, time.Now(), fn, fencingToken)
					})
			}
		}
		return
	}

	r.log.Debug(
		"App: changed; rebuilding",
		"app", name,
	)
	r.mu.Lock()
	r.generations[name]++
	generation := r.generations[name]
	r.mu.Unlock()
	var fencingToken state.FencingToken
	if r.st != nil {
		// Reuse the fingerprint computed above for this desired generation: it is
		// recorded as DesiredFingerprint without a second filesystem scan, so the
		// state write transaction holds no external I/O. The returned fencing
		// token is the PostgreSQL-authoritative identity of this generation; every
		// callback below carries it so another worker's newer generation wins.
		fencingToken, _ = r.st.RecordPreparingWithFingerprint(r.rctx(), name, fn, fp)
		r.setFencingToken(name, fencingToken)
	}

	start := time.Now()
	built, err := r.prepareImage(r.prepareContext(name, generation, fencingToken), fn, fp, selection)
	if err != nil {
		r.log.Error(
			"App: reload failed; retaining previous version",
			"app", name,
			"error", err,
			"duration", time.Since(start),
			"outcome", "failed",
		)
		// Keep the old active version AND the old fingerprint so a later change
		// (which alters the fingerprint) triggers a fresh attempt.
		if r.st != nil {
			// The prior active version is retained in the state database; only
			// the failure outcome is recorded.
			r.st.RecordReconcileFailure(r.rctx(), name, err, fencingToken)
		}
		return
	}
	pf := runner.NewPrepared(fn, built, r.builder)

	// Determine the superseded image before swapping so we can retire it after
	// the registry points at the new version. In-flight Handles keep the old
	// snapshot's image; the runner's refcount guards removal until idle.
	var oldImage string
	if cur != nil && cur.Prepared() != nil {
		oldImage = cur.Prepared().Image
	}

	r.reg.Replace(name, pf)
	// The skip key and the persisted active fingerprint are the identity the
	// image was ACTUALLY built from (built.Fingerprint, derived inside Prepare
	// from the one immutable source snapshot the build staged), not the pre-build
	// scan `fp`. They can differ when the source mutated between the reconciler's
	// scan and the build's capture; recording the built value means the next
	// reconcile/periodic audit compares against what is really baked and rebuilds
	// the drift, instead of silently treating the mutated tree as current.
	builtFP := built.Fingerprint
	if builtFP == "" {
		// Defensive: a Builder that does not return an identity (hand-built
		// fakes) keeps the scanned value.
		builtFP = fp
	}
	r.mu.Lock()
	r.fingerprints[name] = builtFP
	r.mu.Unlock()

	ready := func() {
		if r.st == nil || !r.currentGeneration(name, generation) {
			return
		}
		r.st.RecordReconcileSuccess(r.rctx(), name, built.Image, builtFP, time.Now(), fn, fencingToken)
	}
	failServices := func(serviceErr error) {
		if serviceErr != nil && r.st != nil && r.currentGeneration(name, generation) {
			r.st.RecordServiceFailure(r.rctx(), name, serviceErr, fencingToken)
		}
	}
	if len(fn.Template.Services) == 0 || r.updateServicesWithStatus == nil {
		if r.st != nil {
			ready()
		}
	}

	// After a successful swap, converge the scheduler's cron jobs to this
	// template's schedules. This runs on both discovery and update, never on
	// the skip path above nor on a build failure (the previous version — and
	// its schedules — are retained).
	if r.updateSchedules != nil {
		r.updateSchedules(name, fn.Template)
	}

	// After the scheduler converges, converge the app's persistent service
	// containers to the freshly prepared template and image. This runs on both
	// discovery and update paths (the registry now serves the new version), and
	// not on the skip path above nor on a build failure (where the previous
	// version — and its service containers — are retained).
	if r.updateServicesWithStatus != nil {
		r.updateServicesWithStatus(name, fn.Template, built.Image,
			func() {
				if r.st != nil && r.currentGeneration(name, generation) {
					r.st.RecordReconciling(r.rctx(), name, fencingToken)
				}
			}, func(err error) {
				if err != nil {
					failServices(err)
					return
				}
				ready()
			})
	} else if r.updateServices != nil {
		r.updateServices(name, fn.Template, built.Image)
	}

	// Retire the superseded version now that the registry serves the new one,
	// persistent services have been converged to it, and — crucially — the
	// service containers that still ran on the old image have been replaced. The
	// hook (when wired) defers removal until the old image is no longer in use;
	// the runner-side reference guard (in-flight refcount plus Relay-owned
	// container references) is the second line of defense for partial service
	// reconcile failures, so a service container left on the old image keeps that
	// image from being removed. Skip a nil hook and an equal image (an
	// unavailable->unavailable retry, or a same-image re-prepare).
	if r.retire != nil && oldImage != "" && oldImage != built.Image {
		r.retire(name, oldImage)
	}

	if cur == nil {
		r.log.Info("App: discovered",
			"app", name,
			"duration", time.Since(start),
			"outcome", "discovered",
		)
	} else {
		r.log.Info("App: updated",
			"app", name,
			"duration", time.Since(start),
			"outcome", "updated",
		)
	}
}

func (r *Reconciler) currentGeneration(name string, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generations[name] == generation
}

// generationAndFencingToken returns the current local generation plus the
// PostgreSQL fencing token for name under one lock, so a periodic observation
// cannot pair one generation's local counter with another's token.
func (r *Reconciler) generationAndFencingToken(name string) (uint64, state.FencingToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generations[name], r.fencingTokens[name]
}

// setFencingToken records the fencing token returned by the desired-state write
// that started the current generation. A failed write (zero) clears it so the
// generation's callbacks fail closed rather than reusing an older fence.
func (r *Reconciler) setFencingToken(name string, fencingToken state.FencingToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fencingToken > 0 {
		r.fencingTokens[name] = fencingToken
		return
	}
	delete(r.fencingTokens, name)
}

// prepareImage builds the app's image, passing the fingerprint the
// reconciler already computed (and, for a runtime-backed app, the source
// selection it was computed from) when the builder supports it. The runtime
// Manager does, so a live rebuild hands it BOTH the tag identity it compared and
// the exact policy to stage: the selection is never re-derived and the
// fingerprint is never re-hashed by the reconciler. The Manager still captures
// the selection once and derives the built identity from that capture; the
// caller records the RETURNED fingerprint. A test or non-Manager Builder that
// only implements Builder falls back to Prepare, which computes them itself;
// those fakes do not care about the identity anyway. fp is "" only for an app
// the reconciler could not hash (which then never reaches here: a fingerprint
// error retains the previous version), so the value supplied is always the one
// just compared.
func (r *Reconciler) prepareImage(
	ctx context.Context,
	fn app.App,
	fp string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	if sp, ok := r.builder.(selectionPreparer); ok {
		return sp.PrepareWithFingerprintAndSelection(ctx, fn, fp, selection)
	}
	return r.builder.Prepare(ctx, fn)
}

// prepareContext roots a Prepare call in the reconciler context and installs the
// app-image build observer that publishes status=building at the ACTUAL
// managed-runtime image build boundary (runtime.WithAppBuildObserver). The
// callback is local-generation-guarded, and the state write itself validates the
// PostgreSQL fencing token, so a stale in-flight build cannot overwrite a newer
// generation's status. When no state sink is wired, the plain reconciler context
// is returned.
func (r *Reconciler) prepareContext(
	name string,
	generation uint64,
	fencingToken state.FencingToken,
) context.Context {
	ctx := r.rctx()
	if r.st == nil {
		return ctx
	}
	return runtime.WithAppBuildObserver(ctx, func() {
		if r.currentGeneration(name, generation) {
			r.st.RecordReconcileBuilding(ctx, name, fencingToken)
		}
	})
}

// recordInvalidDesired records a present-but-unloadable desired definition into
// the state view (degraded with a retained active generation, or unavailable
// without one) while leaving the runtime registry untouched. It is the shared
// seam for every reconcileApp failure that is NOT a removal and NOT a valid
// desired generation that failed to build: ErrInvalidPath, a flaky read/stat,
// and a fingerprint error. A directory without template.yaml is not an app
// candidate and follows the removal/no-op path instead. State is a view
// only — the previously-loaded version stays served — so an operator sees the
// failed desired state instead of a stale ready row. When no state sink is wired
// it is a no-op.
//
// It also FORGETS the stored fingerprint for name and invalidates this
// Reconciler's local generation plus the persisted fencing token. That stored
// fingerprint is the reconciler's skip key: if it were left in place, a
// definition later restored byte-identically would compare equal and take the
// unchanged skip path, leaving the recorded failure visible forever (the skip
// path deliberately writes nothing). Forgetting it forces the next reconcile to
// re-resolve the identity, so a restored definition is re-verified and a real
// success replaces the failure — the same "retry even when the fingerprint is
// stable" behavior an unavailable app already has. Advancing the generation
// prevents a callback captured for the previous desired state from writing after
// the invalid state is recorded. It never writes a success itself.
func (r *Reconciler) recordInvalidDesired(name string, err error) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.generations[name]++
	delete(r.fencingTokens, name)
	r.mu.Unlock()
	if r.st == nil {
		return
	}
	// The invalid desired write advances the persisted fencing token, invalidating
	// older completions from any worker. Keep the returned token as this
	// Reconciler's current state identity; zero (write failure) leaves it cleared,
	// so later callbacks fail closed.
	fencingToken, _ := r.st.RecordInvalidDesired(r.rctx(), name, err)
	r.setFencingToken(name, fencingToken)
}

// remove drops an app from the registry and forgets its fingerprint. It
// invalidates the app's local generation and persisted fencing token before
// deleting, so a captured service completion from the removed generation can
// neither write a status nor recreate the row. Local services and images are
// still retired even when the state delete is suppressed by a newer generation:
// the directory is gone from THIS worker's point of view.
func (r *Reconciler) remove(name string) {
	r.reg.Replace(name, nil)
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.generations[name]++
	fencingToken := r.fencingTokens[name]
	delete(r.fencingTokens, name)
	r.mu.Unlock()
	if r.st != nil {
		r.st.RecordRemoved(r.rctx(), name, fencingToken)
	}
	// The directory is gone, so every version of this app's images is now
	// garbage. Let the runner retire all of them (once idle) when wired. The
	// wired RemoveApp hook also deletes the app's Prometheus series at
	// this same retirement point (the reconciler itself stays metrics-free; the
	// worker wires the metrics deletion by wrapping the hook).
	//
	// RemoveServices runs FIRST, before RemoveApp retires the images: a
	// running service container still references the app's images, so those
	// containers must be stopped and removed before the images become
	// retire-eligible. Nil-safe.
	if r.removeServices != nil {
		r.removeServices(name)
	}
	if r.removeApp != nil {
		r.removeApp(name)
	}
	r.log.Info(
		"App: removed",
		"app", name,
	)
}

// isAvailable reports whether a prepared app has a usable image.
func isAvailable(pf *runner.PreparedApp) bool {
	return pf.Prepared() != nil
}

// rctx returns the reconciler context or context.Background outside Start (tests).
func (r *Reconciler) rctx() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

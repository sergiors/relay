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

	"relay/internal/function"
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

// Builder prepares a function image and executes invocations against it. It is
// a concrete-need interface so reconcile logic can be unit-tested without Docker
// or a Redis stream; the runtime Manager satisfies it in production.
type Builder interface {
	Prepare(
		ctx context.Context,
		fn function.Function,
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
// selection the reconciler already resolved to compute the function's
// fingerprint, so a rebuild does not re-derive the selection between the hash
// and the build: the tag the reconciler compared and the bytes the build stages
// come from one policy read. A Builder that does not implement it keeps the
// plain Prepare behavior; the reconciler falls back to Prepare for those.
type selectionPreparer interface {
	PrepareWithFingerprintAndSelection(
		ctx context.Context,
		fn function.Function,
		fingerprint string,
		selection *source.Selection,
	) (*runtime.Prepared, error)
}

// resourceSetter is the OPTIONAL extension of Builder implemented by the runtime
// Manager (but not by the test fakes). Resource limits deliberately do not
// participate in the image fingerprint, so a resource-only template change does
// NOT trigger a rebuild and therefore never reaches Prepare. The reconciler
// calls SetFunctionResources on its unchanged-fingerprint skip path so the live
// warm pool rotates containers to the new limits (idle ones discarded, busy ones
// drained) without touching the image. A Builder that does not implement it
// (fakes, non-Manager builders) ignores the change, exactly as before.
type resourceSetter interface {
	SetFunctionResources(name string, limits function.ResourceLimits)
}

// Config tunes the reconciler. A zero value applies the package defaults.
type Config struct {
	// Root is the functions root. Required.
	Root string
	// Debounce is how long an event storm for one function waits before its
	// single reconcile fires. Defaults to DefaultDebounce.
	Debounce time.Duration
	// Interval is the periodic reconciliation period, a backstop for watches that
	// miss events. Defaults to DefaultInterval.
	Interval time.Duration
	// Fingerprint, when non-nil, replaces the package-level
	// function.SelectAndFingerprintFunction that reconcileFunction uses to derive
	// a function's source selection and content fingerprint. It is a narrow test
	// seam (production leaves it nil) so a test can count how many times ONE
	// reconcile resolves a function's identity without touching the filesystem.
	Fingerprint func(dir string, tmpl *function.Template) (*source.Selection, string, error)
	// State is an optional state-view sink. When non-nil, reconcile outcomes
	// (discovered/updated/removed/failed/skipped) are recorded in it; when nil
	// the reconciler behaves exactly as before (no state writes). Errors from
	// state calls are logged, never fatal.
	State *state.State
	// Retire, when set, is called after a function's registry entry is swapped
	// to a new image AND its persistent service containers have been converged
	// to that new image, passing the function name and the superseded image
	// reference (the version being replaced). Retiring after service converge
	// guarantees a running service container on the old image has been replaced
	// before the old image is retire-eligible; the runner-side reference guard is
	// the second line of defense for partial failures. It is how the runner
	// learns to retire an image once it is no longer in use. Nil-safe; a stale
	// or equal image is skipped by the caller. Ignoring the name is fine — the
	// image reference already embeds it.
	Retire func(name, oldImage string)
	// RemoveFunction, when set, is called after a function directory vanishes
	// (and its registry entry and state are dropped) so the runner can retire
	// every version of that function's images. Nil-safe.
	RemoveFunction func(name string)
	// UpdateSchedules, when set, is called after a function's new version is
	// prepared and swapped into the registry (both discovery and update paths;
	// never on the skip path), so the scheduler can converge its cron jobs to
	// the template's schedules. Nil-safe. It does NOT purge previously-published
	// schedule dedup keys or stream entries: those represent occurrences valid
	// when published and expire via the key TTL / stream retention.
	UpdateSchedules func(name string, tmpl *function.Template)
	// UpdateServices, when set, is called after a function's new version is
	// prepared and swapped into the registry (discovery and update paths; never
	// on the skip path and never on build failure), so the service reconciler
	// (services.go) can converge the function's persistent containers to the new
	// template+image.
	// It is ALSO called on the skip path (unchanged, already-available function)
	// when the function declares services, so crashed service replicas are
	// recreated within the periodic reconcile cadence without a separate
	// services-only loop — Reconcile is idempotent, so this is a cheap no-op
	// when converged. Nil-safe.
	//
	// It is the STATUS-LESS form: it reports no reconcile-start/complete
	// lifecycle to the state sink. The production worker wires
	// UpdateServicesWithStatus / UpdateServicesObservationWithStatus instead, so
	// this one is the fallback for a Builder, test, or embedding host that wants
	// service convergence without status reporting. When any status-aware hook is
	// set it takes precedence (see reconcileFunction), so the two are never both
	// invoked for one convergence.
	UpdateServices                      func(name string, tmpl *function.Template, image string)
	UpdateServicesWithStatus            func(name string, tmpl *function.Template, image string, onReconcileStart func(), onComplete func(error))
	UpdateServicesObservationWithStatus func(name string, tmpl *function.Template, image string, onReconcileStart func(), onComplete func(error))
	// RemoveServices, when set, is called in remove() immediately BEFORE
	// RemoveFunction and the function's images are retired. The ordering
	// invariant: running service containers reference the function's images, so
	// those containers must be stopped and removed before the images are
	// retire-eligible. Nil-safe.
	RemoveServices func(name string)
}

// Watches Root, debounces per-function events, and swaps the registry when a
// function's content fingerprint changes.
type Reconciler struct {
	root     string
	debounce time.Duration
	interval time.Duration

	reg     *runner.Registry
	builder Builder
	log     *slog.Logger
	st      *state.State
	// fingerprint resolves a function's selection and content fingerprint. It
	// defaults to function.SelectAndFingerprintFunction and is overridden by
	// Config.Fingerprint for tests that need to observe (or count) the ONE
	// resolution per reconcile.
	fingerprint func(dir string, tmpl *function.Template) (*source.Selection, string, error)
	// retire/removeFunction/updateSchedules/updateServices/removeServices are
	// optional image-lifecycle, schedule-convergence, and service-convergence
	// hooks (see Config).
	retire                              func(name, oldImage string)
	removeFunction                      func(name string)
	updateSchedules                     func(name string, tmpl *function.Template)
	updateServices                      func(name string, tmpl *function.Template, image string)
	updateServicesWithStatus            func(name string, tmpl *function.Template, image string, onReconcileStart func(), onComplete func(error))
	updateServicesObservationWithStatus func(name string, tmpl *function.Template, image string, onReconcileStart func(), onComplete func(error))
	removeServices                      func(name string)

	mu           sync.Mutex
	fingerprints map[string]string // name -> last-reconciled fingerprint
	generations  map[string]uint64
	timers       map[string]*time.Timer

	incoming chan string   // debounced, per-function trigger queue
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
// startup-loaded functions (available or not) so reconciliation can compare
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
		fingerprint = function.SelectAndFingerprintFunction
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
		removeFunction:                      cfg.RemoveFunction,
		updateSchedules:                     cfg.UpdateSchedules,
		updateServices:                      cfg.UpdateServices,
		updateServicesWithStatus:            cfg.UpdateServicesWithStatus,
		updateServicesObservationWithStatus: cfg.UpdateServicesObservationWithStatus,
		removeServices:                      cfg.RemoveServices,
		fingerprints:                        map[string]string{},
		generations:                         map[string]uint64{},
		timers:                              map[string]*time.Timer{},
		incoming:                            make(chan string, DefaultQueueSize),
		done:                                make(chan struct{}),
	}
}

// Seed records the fingerprint for a currently-loaded function so the first
// reconcile pass does not rebuild a function that was already prepared at
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
// empty, the first reconcile simply treats the function as changed and rebuilds
// it (the desired behavior for an unprepared function).
func (r *Reconciler) Seed(fn function.Function, fingerprint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fingerprints[fn.Name] = fingerprint
}

// PrepareWatch creates the fsnotify watcher and installs the recursive watches
// SYNCHRONOUSLY, without starting the background loops. It is the ordering seam
// the worker uses to establish change detection BEFORE trusting the supplied
// startup fingerprints: the worker calls PrepareWatch, then Seed for each
// startup function, then Start. Any change after the watch is installed is
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

// dispatch feeds a function into the single reconciler goroutine (the pump).
// The send blocks when the queue is full (there is no default case): the caller
// is a debounce timer goroutine, so backpressure simply delays that timer
// rather than dropping the reconcile. Both the debounce timers and the periodic
// pass converge here so no two reconciles of the same function ever run
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

// pump consumes function names serially, so distinct functions can queue
// independently but no function is ever reconciled twice concurrently. It exits
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
			r.reconcileFunction(name)
		case <-r.done:
			return
		}
	}
}

// ticker periodically reconciles every known function, catching events the
// watcher missed. This periodic audit INTENTIONALLY re-hashes each function's
// selected content (reconcileFunction's SelectAndFingerprintFunction) even when
// no fsnotify event fired: it is the backstop for a missed create/add/remove,
// an fsnotify overflow, or a transient "unavailable" retry, and it is what makes
// change detection correct on Docker Desktop bind mounts, where fsnotify
// delivery is unreliable and stat-metadata-only schemes (mtime/size) miss
// same-size rapid edits. Every optimization in this file leaves that periodic
// hash in place; a no-event or metadata-only assumption would weaken
// correctness, so it is deliberately retained. An unchanged audit finds the
// same fingerprint and skips (no build), so the cost is the single read the
// backstop exists to perform.
func (r *Reconciler) ticker() {
	defer r.loops.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.reconcileAll()
		}
	}
}

// eventLoop forwards fsnotify events into the debounce queue, mapping their paths
// to function names and maintaining watches on newly created/removed directories.
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
			if name, affected := r.functionForPath(ev.Name); affected {
				r.Enqueue(name)
			}
			// Maintain watches on created/renamed directories (recursive watch).
			// A Relay-owned reserved directory (git's ".sync-*") is never
			// watched, and neither will addWatchRecursive descend into it, so a
			// dynamic Create for a stage dir spends no handle; addWatchRecursive
			// itself also prunes reserved subtrees defensively.
			if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 && !r.isReservedDirPath(ev.Name) {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					r.addWatchRecursive(ev.Name)
				}
			}
			// A removed or renamed (to elsewhere) directory leaves stale watch
			// handles that leak file descriptors over time. Clear the watch for
			// the path (and, for a parent-dir removal, every descendant watch
			// below it). Only directories are ever in the watches map, so a file
			// event here simply matches nothing. Rename is treated like remove:
			// the old path is gone even if it reappears elsewhere, and a recreated
			// directory is re-watched on its Create event via the branch above.
			if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
				r.removeWatchRecursive(ev.Name)
			}
		case err, ok := <-r.w.Errors:
			if !ok {
				return
			}
			r.log.Warn("Reconciler: watch error", "error", err)
		}
	}
}

// functionForPath maps an event path to the affected function name: the first
// path segment under the root. A Relay-owned transient staging directory
// (function.IsReservedDir, e.g. git's ".sync-*") is never a function, so its
// events are filtered here and no debounce timer is ever armed for it.
func (r *Reconciler) functionForPath(path string) (string, bool) {
	name, ok := r.rootChildName(path)
	if !ok || function.IsReservedDir(name) {
		return "", false
	}
	return name, true
}

// rootChildName returns the first path segment of p relative to the functions
// root, and whether p is at or below the root with a non-empty relative path.
// It is the shared basis for mapping an event path to a function name and for
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
// directory directly under the functions root (function.IsReservedDir). The
// check is on the FIRST path segment under r.root, so a reserved ROOT CHILD and
// all of its descendants are skipped, while a nested real directory whose name
// merely looks reserved (e.g. root/<valid-fn>/.sync-x) is not.
func (r *Reconciler) isReservedDirPath(p string) bool {
	name, ok := r.rootChildName(p)
	return ok && function.IsReservedDir(name)
}

// addWatchRecursive watches root and every subdirectory so events beneath nested
// dirs are seen. A Relay-owned reserved directory directly under the root
// (function.IsReservedDir, git's ".sync-*") and its whole subtree are
// deliberately NOT watched: such a directory is never a function, so watching it
// would only consume inotify handles and surface events that functionForPath
// immediately discards. filepath.WalkDir is used with SkipDir to prune the
// reserved subtree, which also means its descendants are never visited.
//
// Symlinked directories are likewise not followed: WalkDir does not follow links
// (d.IsDir() is false for a symlink entry), so a symlink loop inside the tree
// cannot make this unbounded — this is why we use the DirEntry (not os.Stat)
// form of the walk. A symlinked dir is simply never watched, which is
// acceptable.
func (r *Reconciler) addWatchRecursive(path string) {
	_ = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
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
		if err := r.w.Add(p); err == nil {
			r.watches.Store(p, struct{}{})
		}
		return nil
	})
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

// reconcileAll discovers current child dirs and reconciles each, so periodic
// checks also handle removal (a dir present in the registry but gone from disk)
// and retry previously failed builds. Each discovered function is dispatched
// through the same single pump so reconciles stay serialized per function.
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
		if function.IsReservedDir(e.Name()) {
			// A Relay-owned transient staging directory (git's ".sync-*") is
			// never a function and never a desired definition: it is neither
			// dispatched for reconcile nor recorded as seen, so it cannot
			// produce a warning or an invalid state row while it briefly exists.
			continue
		}
		seen[e.Name()] = true
		r.dispatch(e.Name())
	}
	// Functions still registered but no longer on disk were removed.
	for _, name := range r.reg.Names() {
		if !seen[name] {
			r.dispatch(name)
		}
	}
}

// reconcileFunction is the core decision point for one function. It is serialized
// per name by the pump; when called directly (Reconcile/reconcileAll) callers
// coordinate it.
func (r *Reconciler) reconcileFunction(name string) {
	// A Relay-owned transient staging directory (git's ".sync-*") is never a
	// function and never a desired definition. It is filtered before dispatch
	// (functionForPath, reconcileAll), but guard here too so a direct/stale
	// caller can never turn a stage directory into a warning, an invalid state
	// row, or a removal of a healthy function. It is deliberately not treated as
	// ErrInvalidPath (which would record an invalid desired definition).
	if function.IsReservedDir(name) {
		return
	}
	// LoadSingle enforces the SAME path policy as startup discovery (a legal
	// single-element name, a real direct child of the root, never a symlink), so
	// a reload can never read, fingerprint, or build a path the startup loader
	// would have rejected. A missing directory wraps fs.ErrNotExist (a removal);
	// an invalid path is ErrInvalidPath and RETAINS the previously-loaded
	// version rather than letting an invalid path remove or replace a healthy
	// function.
	fn, err := function.LoadSingle(r.root, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Directory gone -> remove from registry. In-flight invocations keep
			// the old snapshot; they are not killed.
			r.remove(name)
			return
		}
		if errors.Is(err, function.ErrNotReady) {
			// Directory exists but template isn't there yet (mid-copy); wait for
			// more events rather than dropping a previously-active function. The
			// desired definition is present but not yet loadable, so record the
			// invalid/failed view (retaining any active generation) while the
			// runtime registry is untouched: a mid-copy never removes a healthy
			// function.
			r.recordInvalidDesired(name, err)
			return
		}
		if errors.Is(err, function.ErrInvalidPath) {
			// An invalid path (bad name, symlink, non-directory, not a direct
			// child) can never be a function; retain any previously-loaded
			// version rather than removing or replacing it. The present invalid
			// desired definition is recorded so its stale ready view is not left
			// behind.
			r.log.Warn(
				"Function: invalid path; retaining previous version",
				"function", name,
				"error", err,
			)
			r.recordInvalidDesired(name, err)
			return
		}
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// function, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched).
		r.log.Warn(
			"Function: load error; retaining previous version",
			"function", name,
			"error", err,
		)
		r.recordInvalidDesired(name, err)
		return
	}
	dir := fn.Dir

	// Compute the fingerprint AND (for a runtime-backed function) the resolved
	// source selection in ONE traversal, then carry BOTH into the rebuild below.
	// The selection is handed to the Manager's selection-aware Prepare so the
	// build stages exactly the source this hash was derived from, with no second
	// policy read that a concurrent .gitignore edit could diverge. For a
	// no-runtime template the selection is nil (no image is built) and the
	// template-only fingerprint is passed through untouched. The resolver is the
	// injected seam (production: function.SelectAndFingerprintFunction), invoked
	// exactly once per reconcile.
	selection, fp, err := r.fingerprint(dir, fn.Template)
	if err != nil {
		r.log.Warn(
			"Function: fingerprint error; retaining previous version",
			"function", name,
			"error", err,
		)
		// The desired definition is present and parseable but its source could
		// not be fingerprinted, so the desired generation cannot be trusted.
		// Record the failed view while retaining the active generation and
		// leaving the runtime registry untouched.
		r.recordInvalidDesired(name, err)
		return
	}

	r.mu.Lock()
	known, hasFingerprint := r.fingerprints[name]
	r.mu.Unlock()

	cur := r.reg.GetByName(name)
	// Skip a rebuild only when the current build is healthy AND content is
	// unchanged. A previously-failed build (unavailable) is retried even if the
	// fingerprint is stable, so a broken function recovers without edits.
	if cur != nil && isAvailable(cur) && hasFingerprint && known == fp {
		// Skipped checks are deliberately NOT persisted as the last reconcile:
		// RecordReconcileSuccess/Failure record the last MEANINGFUL operation and
		// its timestamp; a periodic no-op must not hide a recent success or
		// failure. It is surfaced in debug logging only.
		r.log.Debug(
			"Function: unchanged; reconcile skipped",
			"function", name,
		)
		// Resource limits do not participate in the image fingerprint, so a
		// resource-only template change lands on this skip path. Publish the
		// current effective limits to the live warm pool so containers rotate to
		// them (idle discarded, busy drained) without a rebuild. The runtime
		// no-ops an unchanged value, so a truly-unchanged periodic tick does not
		// churn. Non-Manager builders ignore it.
		if rs, ok := r.builder.(resourceSetter); ok {
			rs.SetFunctionResources(name, fn.Template.ResourceLimits())
		}
		// A skip path is NOT a full no-op when the function declares services:
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
				// meaningful source work that is still completing.
				generation := r.currentGenerationNumber(name)
				// This is a periodic VERIFICATION of an unchanged, already-
				// available function: it must not claim that a new generation is
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
							r.st.RecordReconciling(name)
						}
					}, func(err error) {
						if r.st == nil || !r.currentGeneration(name, generation) {
							return
						}
						if err != nil {
							r.st.RecordServiceFailure(name, err)
							return
						}
						if !worked {
							// Nothing was converged: a no-op verification must not
							// rewrite last_reconcile_status/last_reconcile_at or
							// touch updated_at.
							return
						}
						r.st.RecordReconcileSuccess(name, cur.Prepared().Image, known, time.Now(), fn)
					})
			}
		}
		return
	}

	r.log.Debug(
		"Function: changed; rebuilding",
		"function", name,
	)
	r.mu.Lock()
	r.generations[name]++
	generation := r.generations[name]
	r.mu.Unlock()
	if r.st != nil {
		// Reuse the fingerprint computed above for this desired generation: it is
		// recorded as DesiredFingerprint without a second filesystem scan, so the
		// state write transaction holds no external I/O.
		r.st.RecordPreparingWithFingerprint(name, fn, fp)
	}

	start := time.Now()
	built, err := r.prepareImage(r.prepareContext(name, generation), fn, fp, selection)
	if err != nil {
		r.log.Error(
			"Function: reload failed; retaining previous version",
			"function", name,
			"error", err,
			"duration", time.Since(start),
			"outcome", "failed",
		)
		// Keep the old active version AND the old fingerprint so a later change
		// (which alters the fingerprint) triggers a fresh attempt.
		if r.st != nil {
			// The prior active version is retained in the state database; only
			// the failure outcome is recorded.
			r.st.RecordReconcileFailure(name, err)
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
		r.st.RecordReconcileSuccess(name, built.Image, builtFP, time.Now(), fn)
	}
	failServices := func(serviceErr error) {
		if serviceErr != nil && r.st != nil && r.currentGeneration(name, generation) {
			r.st.RecordServiceFailure(name, serviceErr)
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

	// After the scheduler converges, converge the function's persistent service
	// containers to the freshly prepared template and image. This runs on both
	// discovery and update paths (the registry now serves the new version), and
	// not on the skip path above nor on a build failure (where the previous
	// version — and its service containers — are retained).
	if r.updateServicesWithStatus != nil {
		r.updateServicesWithStatus(name, fn.Template, built.Image,
			func() {
				if r.st != nil && r.currentGeneration(name, generation) {
					r.st.RecordReconciling(name)
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
		r.log.Info("Function: discovered",
			"function", name,
			"duration", time.Since(start),
			"outcome", "discovered",
		)
	} else {
		r.log.Info("Function: updated",
			"function", name,
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

func (r *Reconciler) currentGenerationNumber(name string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generations[name]
}

// prepareImage builds the function's image, passing the fingerprint the
// reconciler already computed (and, for a runtime-backed function, the source
// selection it was computed from) when the builder supports it. The runtime
// Manager does, so a live rebuild hands it BOTH the tag identity it compared and
// the exact policy to stage: the selection is never re-derived and the
// fingerprint is never re-hashed by the reconciler. The Manager still captures
// the selection once and derives the built identity from that capture; the
// caller records the RETURNED fingerprint. A test or non-Manager Builder that
// only implements Builder falls back to Prepare, which computes them itself;
// those fakes do not care about the identity anyway. fp is "" only for a function
// the reconciler could not hash (which then never reaches here: a fingerprint
// error retains the previous version), so the value supplied is always the one
// just compared.
func (r *Reconciler) prepareImage(
	ctx context.Context,
	fn function.Function,
	fp string,
	selection *source.Selection,
) (*runtime.Prepared, error) {
	if sp, ok := r.builder.(selectionPreparer); ok {
		return sp.PrepareWithFingerprintAndSelection(ctx, fn, fp, selection)
	}
	return r.builder.Prepare(ctx, fn)
}

// prepareContext roots a Prepare call in the reconciler context and installs the
// function-image build observer that publishes status=building at the ACTUAL
// managed-runtime image build boundary (runtime.WithFunctionBuildObserver). The
// callback is generation-guarded, so a stale in-flight completion cannot
// overwrite a newer generation's status. When no state sink is wired, the plain
// reconciler context is returned.
func (r *Reconciler) prepareContext(name string, generation uint64) context.Context {
	ctx := r.rctx()
	if r.st == nil {
		return ctx
	}
	return runtime.WithFunctionBuildObserver(ctx, func() {
		if r.currentGeneration(name, generation) {
			r.st.RecordReconcileBuilding(name)
		}
	})
}

// recordInvalidDesired records a present-but-unloadable desired definition into
// the state view (degraded with a retained active generation, or unavailable
// without one) while leaving the runtime registry untouched. It is the shared
// seam for every reconcileFunction failure that is NOT a removal and NOT a valid
// desired generation that failed to build: ErrNotReady (mid-copy),
// ErrInvalidPath, a flaky read/stat, and a fingerprint error. State is a view
// only — the previously-loaded version stays served — so an operator sees the
// failed desired state instead of a stale ready row. When no state sink is wired
// it is a no-op.
//
// It also FORGETS the stored fingerprint for name. That stored value is the
// reconciler's skip key: if it were left in place, a definition later restored
// byte-identically would compare equal and take the unchanged skip path, leaving
// the recorded failure visible forever (the skip path deliberately writes
// nothing). Forgetting it forces the next reconcile to re-resolve the identity,
// so a restored definition is re-verified and a real success replaces the
// failure — the same "retry even when the fingerprint is stable" behavior an
// unavailable function already has. It never writes a success itself.
func (r *Reconciler) recordInvalidDesired(name string, err error) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.mu.Unlock()
	if r.st == nil {
		return
	}
	r.st.RecordInvalidDesired(name, err)
}

// remove drops a function from the registry and forgets its fingerprint.
func (r *Reconciler) remove(name string) {
	r.reg.Replace(name, nil)
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.mu.Unlock()
	if r.st != nil {
		r.st.RecordRemoved(name)
	}
	// The directory is gone, so every version of this function's images is now
	// garbage. Let the runner retire all of them (once idle) when wired. The
	// wired RemoveFunction hook also deletes the function's Prometheus series at
	// this same retirement point (the reconciler itself stays metrics-free; the
	// worker wires the metrics deletion by wrapping the hook).
	//
	// RemoveServices runs FIRST, before RemoveFunction retires the images: a
	// running service container still references the function's images, so those
	// containers must be stopped and removed before the images become
	// retire-eligible. Nil-safe.
	if r.removeServices != nil {
		r.removeServices(name)
	}
	if r.removeFunction != nil {
		r.removeFunction(name)
	}
	r.log.Info(
		"Function: removed",
		"function", name,
	)
}

// isAvailable reports whether a prepared function has a usable image.
func isAvailable(pf *runner.PreparedFunction) bool {
	return pf.Prepared() != nil
}

// rctx returns the reconciler context or context.Background outside Start (tests).
func (r *Reconciler) rctx() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

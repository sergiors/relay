package reconciler

import (
	"context"
	"errors"
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
	// template+image. fnDir is the function's directory (build sources resolve
	// their Dockerfile relative to it).
	// It is ALSO called on the skip path (unchanged, already-available function)
	// when the function declares services, so crashed service replicas are
	// recreated within the periodic reconcile cadence without a separate
	// services-only loop — Reconcile is idempotent, so this is a cheap no-op
	// when converged. Nil-safe.
	UpdateServices           func(name, fnDir string, tmpl *function.Template, image string)
	UpdateServicesWithStatus func(name, fnDir string, tmpl *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error))
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
	// retire/removeFunction/updateSchedules/updateServices/removeServices are
	// optional image-lifecycle, schedule-convergence, and service-convergence
	// hooks (see Config).
	retire                   func(name, oldImage string)
	removeFunction           func(name string)
	updateSchedules          func(name string, tmpl *function.Template)
	updateServices           func(name, fnDir string, tmpl *function.Template, image string)
	updateServicesWithStatus func(name, fnDir string, tmpl *function.Template, image string, onBuildStart, onReconcileStart func(), onComplete func(error))
	removeServices           func(name string)

	mu           sync.Mutex
	fingerprints map[string]string // name -> last-reconciled fingerprint
	generations  map[string]uint64
	timers       map[string]*time.Timer

	incoming chan string   // debounced, per-function trigger queue
	done     chan struct{} // closed on shutdown to unblock pump/timer sends

	ctx     context.Context
	w       *fsnotify.Watcher
	watches sync.Map // dir path -> struct{} for tracked watches
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
	return &Reconciler{
		root:                     cfg.Root,
		debounce:                 cfg.Debounce,
		interval:                 cfg.Interval,
		reg:                      reg,
		builder:                  builder,
		log:                      logger,
		st:                       cfg.State,
		retire:                   cfg.Retire,
		removeFunction:           cfg.RemoveFunction,
		updateSchedules:          cfg.UpdateSchedules,
		updateServices:           cfg.UpdateServices,
		updateServicesWithStatus: cfg.UpdateServicesWithStatus,
		removeServices:           cfg.RemoveServices,
		fingerprints:             map[string]string{},
		generations:              map[string]uint64{},
		timers:                   map[string]*time.Timer{},
		incoming:                 make(chan string, DefaultQueueSize),
		done:                     make(chan struct{}),
	}
}

// Seed records the fingerprint for each currently-loaded function so the first
// reconcile pass does not rebuild functions that were already prepared at
// startup. It is called once during wiring, before Start.
func (r *Reconciler) Seed(fn function.Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fp, err := function.Fingerprint(fn.Dir); err == nil {
		r.fingerprints[fn.Name] = fp
	}
}

// Start runs the watch loop, the debounce pump, and the periodic ticker in
// background goroutines until ctx is cancelled, then returns. It is intended to
// be called concurrently with the stream consumer.
func (r *Reconciler) Start(ctx context.Context) {
	r.ctx = ctx

	w, err := fsnotify.NewWatcher()
	if err != nil {
		r.log.Error("Reconciler: fsnotify error", "error", err)
		return
	}
	r.w = w
	r.addWatchRecursive(r.root)

	go r.pump()
	go r.ticker()
	go r.eventLoop()

	<-ctx.Done()
	_ = r.w.Close()
	// Stop any still-pending debounce timers so they cannot fire and dispatch a
	// stale name after we've begun tearing down.
	r.mu.Lock()
	for name, t := range r.timers {
		t.Stop()
		delete(r.timers, name)
	}
	r.mu.Unlock()
	close(r.done)
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

// dispatch feeds a function into the single reconciler goroutine (the pump),
// dropping it if the queue is full. Both the debounce timers and the periodic
// pass converge here so no two reconciles of the same function ever run
// concurrently. incoming is never closed; on shutdown the done case wins, so a
// timer firing during teardown is safely discarded rather than panicking on a
// closed channel.
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
// watcher missed.
func (r *Reconciler) ticker() {
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
			if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
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
// path segment under the root.
func (r *Reconciler) functionForPath(path string) (string, bool) {
	rel, err := filepath.Rel(r.root, path)
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

// addWatchRecursive watches root and every subdirectory so events beneath nested
// dirs are seen. Symlinked directories are deliberately not followed: WalkDir
// does not follow links (d.IsDir() is false for a symlink entry), so a symlink
// loop inside the tree cannot make this unbounded — this is why we use the
// DirEntry (not os.Stat) form of the walk. A symlinked dir is simply never
// watched, which is acceptable.
func (r *Reconciler) addWatchRecursive(path string) {
	_ = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
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
	dir := filepath.Join(r.root, name)

	// Directory gone -> remove from registry. In-flight invocations keep the old
	// snapshot; they are not killed.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		r.remove(name)
		return
	} else if err != nil {
		// Unexpected stat error (permissions, I/O): don't drop the function on a
		// flaky read, but surface it so staleness isn't silently ignored.
		r.log.Warn(
			"Function: stat error; retaining previous version",
			"function", name,
			"error", err,
		)
		return
	}

	fn, err := function.LoadSingle(dir, name)
	if err != nil {
		if errors.Is(err, function.ErrNotReady) {
			// Directory exists but template isn't there yet (mid-copy); wait for
			// more events rather than dropping a previously-active function.
			return
		}
		r.log.Warn(
			"Function: template invalid; retaining previous version",
			"function", name,
			"error", err,
		)
		return
	}

	fp, err := function.Fingerprint(dir)
	if err != nil {
		r.log.Warn(
			"Function: fingerprint error; retaining previous version",
			"function", name,
			"error", err,
		)
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
		if (r.updateServices != nil || r.updateServicesWithStatus != nil) && len(fn.Template.Services) > 0 {
			if r.updateServicesWithStatus == nil {
				r.updateServices(name, fn.Dir, fn.Template, cur.Prepared().Image)
			} else {
				r.mu.Lock()
				r.generations[name]++
				generation := r.generations[name]
				r.mu.Unlock()
				// This is a periodic VERIFICATION of an unchanged, already-
				// available function: it must not claim that a new generation is
				// being prepared. No RecordPreparing here — the public status is
				// only moved to preparing when an actual desired-generation change
				// is detected (the rebuild path below).
				//
				// worked tracks whether the service pass actually did anything:
				// onBuildStart fires at a real Dockerfile build boundary and
				// onReconcileStart fires only when real corrective container work
				// begins (see services.go). A no-op verification leaves it false,
				// so the completion below writes NOTHING and ready,
				// last_reconcile_status, and updated_at stay untouched. A pass that
				// did real work (a build and/or corrective convergence) ends ready.
				// The callbacks are per-generation and generation-guarded.
				worked := false
				r.updateServicesWithStatus(name, fn.Dir, fn.Template, cur.Prepared().Image,
					func() {
						worked = true
						if r.st != nil && r.currentGeneration(name, generation) {
							r.st.RecordReconcileBuilding(name)
						}
					}, func() {
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
							// Nothing was built or converged: a no-op verification
							// must not rewrite last_reconcile_status/
							// last_reconcile_at or touch updated_at.
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
		r.st.RecordPreparing(name, fn)
	}

	start := time.Now()
	built, err := r.builder.Prepare(r.prepareContext(name, generation), fn)
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
	r.mu.Lock()
	r.fingerprints[name] = fp
	r.mu.Unlock()

	ready := func() {
		if r.st == nil || !r.currentGeneration(name, generation) {
			return
		}
		r.st.RecordReconcileSuccess(name, built.Image, fp, time.Now(), fn)
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
		r.updateServicesWithStatus(name, fn.Dir, fn.Template, built.Image,
			func() {
				if r.st != nil && r.currentGeneration(name, generation) {
					r.st.RecordReconcileBuilding(name)
				}
			}, func() {
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
		r.updateServices(name, fn.Dir, fn.Template, built.Image)
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

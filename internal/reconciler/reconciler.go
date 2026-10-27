package reconciler

import (
	"context"
	"io/fs"
	"log/slog"
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
	// resync is a coalesced request for a full reconcileAll pass. It is used
	// after the initial watch walk, a recoverable watch-delivery gap (an
	// fsnotify error reports lost or unread events), or after the root watch is
	// restored, so missed creates, removals, and content changes are observed
	// without waiting for the next periodic tick. The ticker goroutine consumes
	// it; eventLoop never walks the tree for reconciliation itself.
	resync chan struct{}

	ctx     context.Context
	w       *fsnotify.Watcher
	watches sync.Map // dir path -> watchEntry for tracked watches

	// watchMu guards watchRetries. Watcher mutations still happen only on the
	// eventLoop goroutine (and synchronously in PrepareWatch before it starts),
	// so registration/removal never races across goroutines. The mutex covers
	// only the small retry index; filesystem walks and fsnotify calls stay
	// outside it.
	watchMu      sync.Mutex
	watchRetries map[string]*watchRetryState
	// walkDir is the recursive-watch traversal seam. Production uses
	// filepath.WalkDir; tests may control callback timing without changing the
	// walk's cancellation and registration behavior.
	walkDir     func(string, fs.WalkDirFunc) error
	watchErrors <-chan error // optional eventLoop input seam for deterministic tests
	// watchWake coalesces "a retry was scheduled" notifications to eventLoop.
	// watchTerm carries the terminal watcher/channel failure to Start.
	watchWake chan struct{}
	watchTerm chan error

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
		resync:                              make(chan struct{}, 1),
		watchRetries:                        map[string]*watchRetryState{},
		watchWake:                           make(chan struct{}, 1),
		watchTerm:                           make(chan error, 1),
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
// watcher itself is returned and leaves the reconciler unwatched. Individual
// directory watch registration failures during the initial walk are NOT
// returned and do NOT abandon the watcher: they are indexed for bounded-backoff
// retry (see addWatchRecursive and eventLoop), because one unreadable subtree
// must not disable change detection for every other app.
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
// background goroutines until ctx is cancelled or watch delivery fails
// terminally, then JOINS them before returning. It is intended to be called
// concurrently with the stream consumer; the caller can therefore use Start's
// return (or the worker's reconciler shutdown step) as a barrier that no further
// reconcile is in flight.
//
// Start first calls PrepareWatch, so a caller that already established the
// watch (the worker, to seed the supplied fingerprints safely) reuses it rather
// than installing a second one; a caller that did not (tests, standalone
// callers) gets the watcher created here. A watcher-construction error is
// returned without starting the loops. An unexpected close of the watcher's
// events or errors channel (after Startup, and not caused by ctx) is a terminal
// watcher failure: the event loop stops, Start tears the loops down, and returns
// the error so the caller can surface it instead of leaving a silently running
// reconciler with no change detection.
func (r *Reconciler) Start(ctx context.Context) error {
	if err := r.PrepareWatch(ctx); err != nil {
		return err
	}

	r.loops.Add(3)
	go r.pump()
	go r.ticker()
	go r.eventLoop()

	var watchErr error
	select {
	case <-ctx.Done():
	case watchErr = <-r.watchTerm:
	}
	// Close done FIRST: it releases a pump parked on an empty queue (or a
	// dispatch blocked on a full queue) so the pump observes shutdown and stops.
	// It also releases the ticker (which may otherwise still be waiting on its
	// interval after a terminal watcher failure).
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
	return watchErr
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
//
// The same loop also consumes the coalesced resync requests raised after the
// initial watch walk and by eventLoop after a recoverable watcher gap or a
// restored root watch. They run the same discovery sweep immediately (without
// the audit hook, which is specific to the periodic cadence) instead of waiting
// for the next tick.
func (r *Reconciler) ticker() {
	defer r.loops.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.done:
			return
		case <-r.resync:
			// A recoverable watch gap (or a restored root watch) may have
			// missed creates/removals, not only edits to known apps. Run the
			// same discovery sweep the periodic backstop uses, without waiting
			// for the interval. The eventLoop coalesces these requests.
			r.reconcileAll()
		case <-ticker.C:
			if r.audit != nil {
				r.audit()
			}
			r.reconcileAll()
		}
	}
}

// rctx returns the reconciler context or context.Background outside Start (tests).
func (r *Reconciler) rctx() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
		case err, ok := <-r.w.Errors:
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
				lastGapRecovery = now
				r.warnWatch("Reconciler: watch error; rescanning", "error", err)
				r.rescanWatches()
				r.requestResync()
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
// inspection and the registration converge on the next fingerprint scan.
func (r *Reconciler) handleWatchEvent(ev fsnotify.Event) {
	// Chmod events are excluded: permission-only changes don't trigger a
	// rebuild, only actual content/tree changes do.
	if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename|fsnotify.Write) == 0 {
		return
	}
	if name, affected := r.appForPath(ev.Name); affected && r.shouldEnqueue(name) {
		r.Enqueue(name)
	}
	// Release watches for a removed or renamed (moved-away) directory FIRST.
	// fsnotify watches inodes, so after an atomic per-app exchange the
	// path-keyed watches still refer to the DETACHED old tree; they must be
	// dropped before the current tree at the same path can be registered.
	// Doing this first also makes a Rename+Create notification pair idempotent
	// instead of leaving only the old inode watched. It is deliberately
	// unconditional: an event for a removed/renamed inode is authoritative even
	// if a rapid recreate reused the path (and possibly the inode number), so a
	// stale registration must never survive by identity coincidence.
	if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		r.removeWatchRecursive(ev.Name)
	}
	// Maintain watches on created/removed/renamed directories (recursive
	// watch). A Relay-owned reserved directory (git's ".sync-*") is never
	// watched, and neither will addWatchRecursive descend into it, so a dynamic
	// Create for a stage dir spends no handle; addWatchRecursive itself also
	// prunes reserved subtrees defensively.
	if ev.Op&(fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 && !r.isReservedDirPath(ev.Name) {
		if r.refreshWatch(ev.Name) {
			// A duplicate exchange notification can describe the inode that is
			// already watched; refreshWatch then does nothing. Only a real
			// refresh needs the post-refresh reconcile.
			r.enqueueWatchPath(ev.Name)
		}
	}
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
// inode. An entry without a recorded identity (a test-constructed reconciler)
// is treated as current, matching the historical presence-only check.
func (r *Reconciler) watchIsCurrent(path string, info os.FileInfo) bool {
	v, ok := r.watches.Load(path)
	if !ok {
		return false
	}
	entry, ok := v.(watchEntry)
	if !ok || entry.info == nil || info == nil {
		return true
	}
	return os.SameFile(entry.info, info)
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

// isSinglePathElement reports whether name can safely be joined to the root to
// inspect exactly one child. App-name validation is intentionally separate:
// invalid but single-element names may still identify a previously-known app.
func isSinglePathElement(name string) bool {
	return name != "" && name != "." && name != ".." && !filepath.IsAbs(name) &&
		!strings.ContainsAny(name, "/\\") && name == filepath.Base(name)
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
	_ = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if err != nil {
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
		if err := r.w.Add(p); err != nil {
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
		watched[path] = struct{}{}
	}
	r.watches.Range(func(key, _ any) bool {
		path, ok := key.(string)
		if !ok {
			return true
		}
		if _, stillWatched := watched[path]; !stillWatched {
			r.watches.Delete(path)
		}
		return true
	})
	r.addWatchRecursive(r.root)
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
		name := e.Name()
		if app.IsReservedDir(name) {
			// A Relay-owned transient staging directory (git's ".sync-*") is
			// never an app and never a desired definition: it is neither
			// dispatched for reconcile nor recorded as seen, so it cannot
			// produce a warning or an invalid state row while it briefly exists.
			continue
		}
		path := filepath.Join(r.root, name)
		realDir, dirErr := app.IsRealAppDir(path)
		if dirErr != nil {
			// The entry was enumerated, but its current type could not be
			// determined. Keep it out of the missing-name sweep and let the
			// single-app path record the operational failure without removing an
			// active generation.
			seen[name] = true
			r.dispatch(name)
			continue
		}
		if !realDir {
			// Files, symlinked directories, and broken links are unsupported, not
			// app candidates. Known names are reconciled through normal removal;
			// unknown root entries do not create app state.
			if r.knownLocally(name) {
				seen[name] = true
				r.dispatch(name)
			}
			continue
		}
		present, statErr := app.HasTemplate(path)
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
	// Observe the persisted row BEFORE reading template.yaml. This snapshot is
	// the optimistic guard an invalid observation carries: if another worker
	// advances the app (for example with a valid correction) while this pass is
	// still running, the conditional invalid write is rejected instead of
	// overwriting the newer state. The fast path itself re-reads persisted state
	// before skipping, so it never trusts this snapshot alone.
	observed := r.observeApp(name)
	// Only inspect names that lexically identify one direct child. An unsafe
	// name must never let a removal check touch a sibling or ancestor; the loader
	// below will classify it as ErrInvalidPath instead.
	checkDir := isSinglePathElement(name)
	var templateSource app.TemplateSource
	var err error
	if checkDir {
		realDir, inspectErr := app.IsRealAppDir(filepath.Join(r.root, name))
		if inspectErr != nil {
			err = fmt.Errorf("app %q: inspect directory: %w", name, inspectErr)
		} else if !realDir {
			// A missing path, regular file, or direct-child symlink is not an
			// app. These are normal removals for known names, unlike an invalid
			// template inside a real app directory, which preserves the active
			// generation.
			if r.knownLocally(name) {
				r.remove(name)
			}
			return
		}
	}
	// ReadTemplateSource applies the same no-follow policy as startup discovery
	// and keeps parsing separate so an unchanged deterministic invalid input can
	// avoid a repeat parse. For a real app directory, a missing template is a
	// normal removal; a symlinked/non-regular template is invalid desired input.
	if err == nil {
		templateSource, err = app.ReadTemplateSource(r.root, name)
	}
	if err != nil {
		// Directory paths can change type after the initial no-follow check.
		// Reclassify invalid-path failures so missing/unsupported directories are
		// removed, while invalid templates in real directories are retained.
		if checkDir && errors.Is(err, app.ErrInvalidPath) {
			realDir, inspectErr := app.IsRealAppDir(filepath.Join(r.root, name))
			if inspectErr != nil {
				err = fmt.Errorf("app %q: inspect directory: %w", name, inspectErr)
			} else if !realDir {
				if r.knownLocally(name) {
					r.remove(name)
				}
				return
			}
		}
		if errors.Is(err, fs.ErrNotExist) {
			// Directory gone -> remove from registry. In-flight invocations keep
			// the old snapshot; they are not killed.
			if r.knownLocally(name) {
				r.remove(name)
			}
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
		// Every other load failure (invalid template path, flaky read, unreadable
		// template) means the desired definition is not a valid generation to
		// prepare, so no desired pending rules are manufactured. Any stale
		// pending entry from a previous, now-invalid desired state is cleared so
		// removed rules cannot keep gating events; the previously-active
		// generation (if any) is retained exactly as today.
		r.reg.ClearPending(name)
		if errors.Is(err, app.ErrInvalidPath) {
			// An invalid name or template path inside a real app directory is
			// invalid desired input, not removal. Retain any previously-loaded
			// version and record the invalid view so its stale ready state is not
			// left behind.
			r.log.Warn(
				"App: invalid path; retaining previous version",
				"app", name,
				"error", err,
			)
			// No bytes were read, so there is no deterministic invalid identity:
			// this failure stays retryable and clears any earlier identity.
			r.recordInvalidDesired(name, "", err, observed)
			return
		}
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// app, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched). It must not claim an invalid
		// identity from an earlier, different read.
		r.log.Warn(
			"App: load error; retaining previous version",
			"app", name,
			"error", err,
		)
		r.recordInvalidDesired(name, "", err, observed)
		return
	}
	source := templateSource

	// The bytes were read and hashed. If their exact deterministic-invalid
	// identity is CURRENTLY persisted, skip the parse entirely. This is a fresh
	// persisted-state read (not the pre-read snapshot and never a process-local
	// negative cache): the row is the authority, so an unchanged invalid input is
	// recognized across cycles and restarts, while another worker's valid
	// correction is never overridden by a stale local view.
	if source.Identity != "" && observed != nil && observed.Found &&
		observed.Detail.InvalidSource == source.Identity {
		if current := r.observeApp(name); current != nil &&
			current.Found && current.Detail.InvalidSource == source.Identity {
			r.reg.ClearPending(name)
			r.skipUnchangedInvalidDesired(name, current)
			return
		}
	}

	fn, err := source.Parse()
	if err != nil {
		// Every other load failure (invalid path, flaky read, unreadable/invalid
		// template) means the desired definition is not a valid generation to
		// prepare, so no desired pending rules are manufactured. Any stale
		// pending entry from a previous, now-invalid desired state is cleared so
		// removed rules cannot keep gating events; the previously-active
		// generation (if any) is retained exactly as today.
		r.reg.ClearPending(name)
		// A flaky read/stat (permissions, I/O) is not a removal: don't drop the
		// app, but surface it so staleness isn't silently ignored. The entry
		// is present, so record the failed desired view (the active generation is
		// preserved and the registry untouched).
		r.log.Warn(
			"App: load error; retaining previous version",
			"app", name,
			"error", err,
		)
		identity, _ := app.InvalidTemplateIdentity(err)
		r.recordInvalidDesired(name, identity, err, observed)
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
		// at-least-once direction. No invalid source identity is claimed: a
		// fingerprint/source/dependency failure is operational and must stay
		// retryable (and clear any earlier deterministic invalid identity).
		r.recordInvalidDesired(name, "", err, observed)
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

// observeApp snapshots the persisted row/token for name BEFORE this pass reads
// template.yaml. It is the optimistic guard for an invalid observation: the
// state write is applied only while the row still matches. A read failure is
// logged and returns nil, which callers must treat as "no authority", never as
// "the row is absent": recordInvalidDesired then skips the write instead of
// bypassing the concurrency guard. The fast path re-reads state separately and
// never skips on an unreadable observation.
func (r *Reconciler) observeApp(name string) *state.AppObservation {
	if r.st == nil {
		return nil
	}
	observation, err := r.st.ObserveApp(r.rctx(), name)
	if err != nil {
		r.log.Warn("App: observe state failed", "app", name, "error", err)
		return nil
	}
	return &observation
}

// skipUnchangedInvalidDesired is the LIVE fast path for an invalid template whose
// exact content identity is (still) the persisted invalid desired state. The
// parse, warning, state write, and fencing-token allocation are all skipped; only
// process-local reconciliation bookkeeping is refreshed so a previously captured
// callback cannot keep serving the invalid generation. The persisted row, not
// this local map, remains the authority.
func (r *Reconciler) skipUnchangedInvalidDesired(name string, observation *state.AppObservation) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.generations[name]++
	if observation != nil && observation.Found && observation.FencingToken > 0 {
		r.fencingTokens[name] = observation.FencingToken
	} else {
		delete(r.fencingTokens, name)
	}
	r.mu.Unlock()
	r.log.Debug(
		"App: invalid template unchanged; reconcile skipped",
		"app", name,
	)
}

// recordInvalidDesired records a present-but-unloadable desired definition into
// the state view (degraded with a retained active generation, or unavailable
// without one) while leaving the runtime registry untouched. It is the shared
// seam for every reconcileApp failure that is NOT a removal and NOT a valid
// desired generation that failed to build: ErrInvalidPath, a flaky read/stat, an
// invalid template, and a fingerprint error. A directory without template.yaml
// is not an app candidate and follows the removal/no-op path instead. State is a
// view only — the previously-loaded version stays served — so an operator sees
// the failed desired state instead of a stale ready row. When no state sink is
// wired it is a no-op.
//
// identity is non-empty ONLY for a deterministic parse/validation failure of the
// template bytes that were actually read. The state write then takes its
// read-only fast path when that identity is already persisted (no write, no
// token allocation). Operational failures pass an empty identity and clear any
// earlier identity, so a transient read can never reuse it. observed is the
// pre-read state snapshot: a stale invalid observation (begun before another
// worker's valid change) is rejected by the conditional write. A nil observed
// means the snapshot could not be read; the write is skipped (fail closed)
// instead of bypassing the guard with an unconditional write.
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
func (r *Reconciler) recordInvalidDesired(
	name,
	identity string,
	err error,
	observed *state.AppObservation,
) {
	r.mu.Lock()
	delete(r.fingerprints, name)
	r.generations[name]++
	delete(r.fencingTokens, name)
	r.mu.Unlock()
	if r.st == nil {
		return
	}
	if observed == nil {
		// A failed state read is not an absent row. Without the pre-read
		// snapshot the conditional write has no concurrency authority, so keep
		// this generation's token cleared (callbacks fail closed) and let a later
		// pass retry.
		r.log.Warn(
			"App: invalid desired state not recorded; state observation unavailable",
			"app", name,
			"error", err,
		)
		return
	}
	// The invalid desired write advances the persisted fencing token (except on
	// the unchanged-identity fast path, where no generation advances), invalidating
	// older completions from any worker. Keep the returned token as this
	// Reconciler's current state identity; zero (write failure or a rejected stale
	// observation) leaves it cleared, so later callbacks fail closed.
	result, writeErr := r.st.RecordInvalidDesiredWithSource(
		r.rctx(), name, identity, err, observed)
	if writeErr != nil {
		// The state layer already logged the operational failure. The token
		// stays cleared (fail closed); state is a view, not this pass's authority.
		return
	}
	if result.Stale {
		// Another worker advanced the app after this pass read the template.
		// Discarding the observation is a normal recovery, not a write failure;
		// keep the local token cleared so no captured callback can write.
		r.log.Debug("App: stale invalid desired observation rejected", "app", name)
		return
	}
	r.setFencingToken(name, result.FencingToken)
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

package reconciler

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"relay/internal/function"
	"relay/internal/logging"
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
	Prepare(ctx context.Context, fn function.Function) (*runtime.Prepared, error)
	Execute(ctx context.Context, prepared *runtime.Prepared, handler string, eventJSON []byte) error
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
}

// Watches Root, debounces per-function events, and swaps the registry when a
// function's content fingerprint changes.
type Reconciler struct {
	root     string
	debounce time.Duration
	interval time.Duration

	reg     *runner.Registry
	builder Builder
	log     *log.Logger
	st      *state.State

	mu          sync.Mutex
	fingerprnts map[string]string // name -> last-reconciled fingerprint
	timers      map[string]*time.Timer

	incoming chan string   // debounced, per-function trigger queue
	done     chan struct{} // closed on shutdown to unblock pump/timer sends

	ctx     context.Context
	w       *fsnotify.Watcher
	watches sync.Map // dir path -> struct{} for tracked watches
}

// New builds a Reconciler. The registry must already be populated with the
// startup-loaded functions (available or not) so reconciliation can compare
// against and swap them.
func New(cfg Config, reg *runner.Registry, builder Builder, logger *log.Logger) *Reconciler {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.Debounce == 0 {
		cfg.Debounce = DefaultDebounce
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}
	return &Reconciler{
		root:        cfg.Root,
		debounce:    cfg.Debounce,
		interval:    cfg.Interval,
		reg:         reg,
		builder:     builder,
		log:         logger,
		st:          cfg.State,
		fingerprnts: map[string]string{},
		timers:      map[string]*time.Timer{},
		incoming:    make(chan string, DefaultQueueSize),
		done:        make(chan struct{}),
	}
}

// Seed records the fingerprint for each currently-loaded function so the first
// reconcile pass does not rebuild functions that were already prepared at
// startup. It is called once during wiring, before Start.
func (r *Reconciler) Seed(fn function.Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if fp, err := function.Fingerprint(fn.Dir); err == nil {
		r.fingerprnts[fn.Name] = fp
	}
}

// Start runs the watch loop, the debounce pump, and the periodic ticker in
// background goroutines until ctx is cancelled, then returns. It is intended to
// be called concurrently with the stream consumer.
func (r *Reconciler) Start(ctx context.Context) {
	r.ctx = ctx

	w, err := fsnotify.NewWatcher()
	if err != nil {
		r.log.Printf("reconciler: fsnotify: %v", err)
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

// Reconcile triggers the per-function reconcile for name, bypassing the debounce
// queue. It is exported so tests can drive the logic deterministically and so
// the periodic pass can call it directly.
func (r *Reconciler) Reconcile(name string) {
	r.reconcileFunction(name)
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
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
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
			r.log.Printf("reconciler: watch error: %v", err)
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
// loop inside the tree cannot make this unbounded — this is the WHY why we use
// the DirEntry (not os.Stat) form of the walk. A symlinked dir is simply never
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
		r.log.Printf("reconciler: read root %q: %v", r.root, err)
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
		r.log.Printf("function %q stat error: %v; retaining previous version", name, err)
		return
	}

	fn, err := function.LoadSingle(dir, name)
	if err != nil {
		if errors.Is(err, function.ErrNotReady) {
			// Directory exists but template isn't there yet (mid-copy); wait for
			// more events rather than dropping a previously-active function.
			return
		}
		r.log.Printf("function %q template invalid; retaining previous version: %v", name, err)
		return
	}

	fp, err := function.Fingerprint(dir)
	if err != nil {
		r.log.Printf("function %q fingerprint error; retaining previous version: %v", name, err)
		return
	}

	r.mu.Lock()
	known, hasFingerprint := r.fingerprnts[name]
	r.mu.Unlock()

	cur := r.reg.GetByName(name)
	// Skip a rebuild only when the current build is healthy AND content is
	// unchanged. A previously-failed build (unavailable) is retried even if the
	// fingerprint is stable, so a broken function recovers without edits.
	if cur != nil && isAvailable(cur) && hasFingerprint && known == fp {
		// The state's RecordSkipped only touches an existing row; a function
		// never seeded (no row) is left alone. This writes the outcome view.
		if r.st != nil {
			r.st.RecordSkipped(name)
		}
		return
	}

	r.log.Printf("function %q changed; rebuilding", name)

	start := time.Now()
	built, err := r.builder.Prepare(r.rctx(), fn)
	if err != nil {
		r.log.Printf("function %q reload failed (retaining previous version): %v%s",
			name, err, logging.Fields("function", name, "duration", time.Since(start), "outcome", "failed"))
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

	r.reg.Replace(name, pf)
	r.mu.Lock()
	r.fingerprnts[name] = fp
	r.mu.Unlock()

	if r.st != nil {
		r.st.RecordReconcileSuccess(name, built.Image, fp, time.Now(), fn)
	}

	if cur == nil {
		r.log.Printf("function %q discovered%s",
			name, logging.Fields("function", name, "duration", time.Since(start), "outcome", "discovered"))
	} else {
		r.log.Printf("function %q updated%s",
			name, logging.Fields("function", name, "duration", time.Since(start), "outcome", "updated"))
	}
}

// remove drops a function from the registry and forgets its fingerprint.
func (r *Reconciler) remove(name string) {
	r.reg.Replace(name, nil)
	r.mu.Lock()
	delete(r.fingerprnts, name)
	r.mu.Unlock()
	if r.st != nil {
		r.st.RecordRemoved(name)
	}
	r.log.Printf("function %q removed", name)
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

package reconciler

import (
	"context"
	"sync"

	"relay/internal/function"
)

const serviceReconcileWorkers = 2

type serviceRequest struct {
	remove      bool
	name        string
	tmpl        *function.Template
	image       string
	preparedEnv []string
	// done is closed exactly once, by whoever concludes the request's life:
	// the worker that executes it (after the operation returns), a newer enqueue
	// that supersedes it before it starts, or lifecycle cancellation that drops
	// it. It is what RemoveAndWait waits on, so the wait is tied to the actual
	// operation rather than a caller timeout.
	done             chan struct{}
	id               uint64
	onReconcileStart func()
	onComplete       func(error)
}

// serviceFunctionState is one function's desired-state slot. At most one pass
// runs at a time; a newer desired state replaces a not-yet-started pending
// request (coalescing to the latest). running is true from when a pass is
// scheduled until it (and any chained replacement) finishes, and it is what
// Wait observes. pending is the newest not-yet-started request; a newer enqueue
// closes the superseded request's done, since it will never run. A request a
// worker has picked up is no longer referenced here, so lifecycle cancellation
// can never release an executing request's done early — that worker closes it
// once its bounded operation returns.
type serviceFunctionState struct {
	pending *serviceRequest
	running bool
}

// ServiceCoordinator asynchronously converges persistent services. It permits
// work for different functions to run concurrently, while each function has at
// most one active pass and only its latest pending desired state is retained.
// The fixed worker count deliberately keeps Docker pressure bounded.
//
// Lifecycle: Start roots the workers in a caller lifecycle; cancelling that
// lifecycle stops the workers, drops pending desired states, and releases their
// waiters. A request already executing is never abandoned by cancellation: it
// completes on its bounded operation context and releases its own waiter, and
// Join waits for the workers to exit. RunExclusive is the exclusive housekeeping
// seam the startup pass uses: it waits for current work to settle and pauses
// scheduling while its callback (orphan sweep, image sweep, dependency GC) runs,
// resuming with the latest coalesced desired states.
//
// Desired states are snapshotted at enqueue time (see cloneServiceTemplate), so
// a live reconciler replacing a function's template cannot mutate a queued or
// in-flight request underneath a worker.
type ServiceCoordinator struct {
	services *ServiceReconciler
	ctx      context.Context
	cancel   context.CancelFunc
	jobs     chan string

	mu          sync.Mutex
	states      map[string]*serviceFunctionState
	idle        *sync.Cond
	paused      bool
	stopped     bool
	nextID      uint64
	desiredIDs  map[string]uint64
	wg          sync.WaitGroup
	workersDone chan struct{}
}

// NewServiceCoordinator creates a coordinator with exactly two workers. Start
// must be called before Enqueue or RemoveAndWait.
func NewServiceCoordinator(services *ServiceReconciler) *ServiceCoordinator {
	return &ServiceCoordinator{
		services:   services,
		jobs:       make(chan string),
		states:     make(map[string]*serviceFunctionState),
		desiredIDs: make(map[string]uint64),
	}
}

// Start starts the coordinator workers under lifecycle. It also starts a single
// waiter that closes workersDone once every worker has exited, so Join never
// has to spawn a goroutine that could outlive it when its bound expires.
func (c *ServiceCoordinator) Start(lifecycle context.Context) {
	c.ctx, c.cancel = context.WithCancel(lifecycle)
	c.idle = sync.NewCond(&c.mu)
	c.workersDone = make(chan struct{})
	for i := 0; i < serviceReconcileWorkers; i++ {
		c.wg.Add(1)
		go c.loop()
	}
	go func() {
		c.wg.Wait()
		close(c.workersDone)
	}()
}

// Enqueue publishes the latest desired state for name and returns immediately.
// An in-flight pass is never interrupted; its replacement runs immediately
// afterward using the latest request. The template and prepared env are
// snapshotted (see cloneServiceTemplate) so the live reconciler swapping the
// function's template cannot mutate a queued or in-flight request under a
// worker.
func (c *ServiceCoordinator) Enqueue(
	name string,
	tmpl *function.Template,
	image string,
	preparedEnv []string,
) {
	c.enqueue(&serviceRequest{
		name:        name,
		tmpl:        cloneServiceTemplate(tmpl),
		image:       image,
		preparedEnv: append([]string(nil), preparedEnv...),
	})
}

// EnqueueWithStatus is the status-aware form used by the lifecycle owner. The
// callbacks belong to this exact coalesced request, so an older operation can
// never complete a newer generation's status transition. onReconcileStart fires
// once the source is resolved and container convergence is about to begin.
func (c *ServiceCoordinator) EnqueueWithStatus(
	name string, tmpl *function.Template, image string, preparedEnv []string,
	onReconcileStart func(), onComplete func(error),
) {
	c.enqueue(&serviceRequest{name: name, tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), onReconcileStart: onReconcileStart, onComplete: onComplete})
}

// EnqueueRemove publishes a removal and returns immediately. It is the
// nonblocking counterpart of RemoveAndWait for callers that must not wait on
// Docker work (startup's unavailable/no-services cleanup). Like any desired
// state it serializes behind the function's active pass and is itself replaced
// by a newer enqueue.
func (c *ServiceCoordinator) EnqueueRemove(name string) {
	c.enqueue(&serviceRequest{name: name, remove: true})
}

// RemoveAndWait serializes removal behind any active pass for name and waits
// DETERMINISTICALLY until the request concludes. It is the reconciler's removal
// hook, called before the function's images are retired: returning while the
// container stop was still running (e.g. because a caller timeout expired) would
// let image retirement race the very removal it depends on, so there is no
// early-return timeout path. The wait ends when the worker closes the request's
// done after the removal returns; at shutdown the lifecycle cancels the bounded
// removal context, so the wait is still bounded. A removal superseded by a newer
// enqueue before it starts concludes without running — not reachable from the
// removal hook, which runs after the function is gone and no further desired
// state is published for it.
//
// The removal operation is bounded by a fresh coordinator-derived context
// (reconcileTimeout rooted in the lifecycle), not by the caller, so a queued
// removal still gets its own full bound when it is picked up.
func (c *ServiceCoordinator) RemoveAndWait(name string) {
	<-c.enqueue(&serviceRequest{name: name, remove: true})
}

// Wait waits for all work published before the call to finish. It returns
// ctx.Err() when ctx is cancelled before the barrier clears. Join uses it to
// drain before joining the workers; RunExclusive builds the exclusive
// housekeeping seam on the same idle primitive (it additionally pauses
// scheduling while its callback runs).
//
// Cancellation is handled by a context.AfterFunc watcher that broadcasts the
// condition, so a Wait parked with no worker progress wakes promptly instead of
// depending on a later worker broadcast. At most one short-lived watcher exists
// per call; stop is deferred so the watcher is unregistered on every return.
// stop does not wait for the callback, and the callback takes the same mutex
// only after Wait has released it, so there is no deadlock and no leak.
func (c *ServiceCoordinator) Wait(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitIdleLocked(ctx)
}

// waitIdleLocked parks on the idle condition until no function has running or
// pending work (busyLocked), returning ctx.Err() on cancellation. The caller
// must hold c.mu; it is still held on return. The context.AfterFunc watcher is
// what makes a Wait/RunExclusive parked with no worker progress wake promptly
// when ctx is cancelled; stop is deferred so the watcher is unregistered on
// every return (a callback that races stop only broadcasts, which is harmless).
func (c *ServiceCoordinator) waitIdleLocked(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.idle.Broadcast()
		c.mu.Unlock()
	})
	defer stop()

	for c.busyLocked() {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.idle.Wait()
	}
	return ctx.Err()
}

// RunExclusive is the startup housekeeping seam: it atomically waits until all
// currently published and in-flight service work is idle, then PAUSES scheduling
// of new work for the duration of fn, so a service-dependent cleanup (orphan
// sweep, image sweep, dependency GC) can never overlap an Apply, a Remove, or a
// worker picking up a freshly enqueued request. Requests published while fn runs
// are neither dropped nor run concurrently: they coalesce as pending desired
// states exactly as under normal operation, and the latest state per function is
// scheduled when fn returns.
//
// fn runs with ctx and is expected to be the bounded, lifecycle-aware cleanup
// sequence. A nil return means fn ran (even if ctx was cancelled partway through,
// which fn's own ctx-aware operations observe). A non-nil return is ctx.Err() and
// means the barrier never cleared — fn was NOT invoked — so the caller can skip
// its passes rather than run them against an unconverged or shutting-down world.
// The pause is always released, including on cancellation.
func (c *ServiceCoordinator) RunExclusive(ctx context.Context, fn func(context.Context)) error {
	c.mu.Lock()
	if err := c.waitIdleLocked(ctx); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.stopped || (c.ctx != nil && c.ctx.Err() != nil) {
		c.mu.Unlock()
		return context.Canceled
	}
	// Idle, not cancelled: hold the pause until resume. Enqueue observed this
	// flag under the same mutex, so no request enqueued from here on can start a
	// worker pass while fn runs.
	c.paused = true
	c.mu.Unlock()

	defer c.resume()
	fn(ctx)
	return nil
}

// Join waits for queued and active work to drain (Wait) and then for every
// worker to exit. It is the shutdown counterpart of Wait: the caller cancels
// the lifecycle first, and Join reports the wait/join error if its own bound
// expires. It spawns no goroutine that can outlive the call — workersDone is
// closed by the single waiter Start launched when the workers exit.
func (c *ServiceCoordinator) Join(ctx context.Context) error {
	if err := c.Wait(ctx); err != nil {
		return err
	}
	if c.workersDone == nil {
		// Start was never called; there are no workers to join.
		return nil
	}
	select {
	case <-c.workersDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ServiceCoordinator) enqueue(req *serviceRequest) chan struct{} {
	req.done = make(chan struct{})
	c.mu.Lock()
	// stopped is set by cancelPending under the same lock, so a request racing
	// lifecycle cancellation can never slip in after the pending waiters were
	// released and leave a permanently-open done channel behind.
	if c.stopped || (c.ctx != nil && c.ctx.Err() != nil) {
		c.mu.Unlock()
		close(req.done)
		return req.done
	}
	state := c.states[req.name]
	if state == nil {
		state = &serviceFunctionState{}
		c.states[req.name] = state
	}
	c.nextID++
	req.id = c.nextID
	c.desiredIDs[req.name] = req.id
	// A newer desired state replaces an unstarted pending one; the superseded
	// request will never run, so release its waiter now rather than leaving it
	// open forever.
	if state.pending != nil {
		close(state.pending.done)
	}
	state.pending = req
	// While RunExclusive holds the housekeeping pause, scheduling is deferred:
	// the request is retained (coalescing to the latest) but no worker pass is
	// started, so it cannot overlap the exclusive cleanup. resume schedules the
	// latest pending state per function once the pause lifts.
	schedule := !c.paused && !state.running
	if schedule {
		state.running = true
	}
	c.mu.Unlock()
	if schedule {
		// The state was marked running while holding the mutex; signal outside it
		// so the function reconciler never blocks behind a busy Docker worker.
		go c.signal(req.name)
	}
	return req.done
}

// resume lifts the RunExclusive pause and schedules the latest pending desired
// state for every function that acquired one during the pause. It is only ever
// called by RunExclusive, which is the sole writer of paused, so there is no
// concurrent pause to lose. The signals are launched outside the mutex, matching
// enqueue, so a worker is never asked to wait on a busy reconciler.
func (c *ServiceCoordinator) resume() {
	c.mu.Lock()
	c.paused = false
	var start []string
	for name, state := range c.states {
		if state.pending != nil && !state.running {
			state.running = true
			start = append(start, name)
		}
	}
	c.mu.Unlock()
	for _, name := range start {
		go c.signal(name)
	}
}

func (c *ServiceCoordinator) loop() {
	defer c.wg.Done()
	for {
		select {
		case name := <-c.jobs:
			c.run(name)
		case <-c.ctx.Done():
			c.cancelPending()
			return
		}
	}
}

func (c *ServiceCoordinator) run(name string) {
	c.mu.Lock()
	state := c.states[name]
	req := state.pending
	state.pending = nil
	c.mu.Unlock()

	if req == nil {
		// The request was dropped by lifecycle cancellation (cancelPending)
		// between the signal and this pick-up; its waiter was already released.
		// There is nothing to converge.
		return
	}

	// The request is now in flight: it is deliberately no longer referenced by
	// state, so lifecycle cancellation can never release its waiter early. The
	// worker closes it below, after the (bounded) operation returns.
	if req.remove {
		// Removal is a single quick Docker operation, so it runs on its own
		// fresh bounded context rooted in the coordinator lifecycle. Bounding it
		// here is what keeps RemoveAndWait's deterministic wait safe: the
		// operation observes both its bound and lifecycle cancellation instead
		// of running unbounded. Reconcile still derives its own per-operation
		// bounds from the lifecycle context for Apply.
		opCtx, cancel := c.removalContext()
		c.services.Remove(opCtx, req.name)
		cancel()
	} else {
		// Apply receives the lifecycle context, NOT a pass-wide reconcileTimeout
		// budget: Reconcile derives a fresh bound for each of its Docker
		// operations itself.
		reconcileStarted := req.onReconcileStart
		if reconcileStarted != nil {
			reconcileStarted = func() {
				if c.currentRequest(req) {
					req.onReconcileStart()
				}
			}
		}
		err := c.services.ApplyWithStatus(c.ctx, req.name, req.tmpl, req.image, req.preparedEnv, reconcileStarted)
		if req.onComplete != nil && c.currentRequest(req) {
			req.onComplete(err)
		}
	}

	// Operation complete: release this request's waiter only now, so
	// RemoveAndWait can never observe completion before the operation returned.
	close(req.done)

	c.mu.Lock()
	if state.pending != nil {
		// Keep the function marked running, but signal outside the mutex. This
		// avoids blocking a worker that needs the mutex to finish another job.
		// This tail never runs during a housekeeping pause: RunExclusive acquires
		// the pause only once the coordinator is idle (no running or pending
		// work), so no pass can be in its tail then.
		c.mu.Unlock()
		go c.signal(name)
		return
	}
	state.running = false
	c.idle.Broadcast()
	c.mu.Unlock()
}

func (c *ServiceCoordinator) currentRequest(req *serviceRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.desiredIDs[req.name] == req.id
}

// removalContext derives the bounded context a queued removal runs under: the
// reconciler's reconcileTimeout rooted in the coordinator lifecycle. It is what
// lets RemoveAndWait wait deterministically without hanging: the removal
// observes both its own bound and lifecycle cancellation. A non-positive
// reconcileTimeout leaves only the lifecycle bound. Apply deliberately does NOT
// use this (Reconcile derives its own per-operation bounds).
func (c *ServiceCoordinator) removalContext() (context.Context, context.CancelFunc) {
	if c.services.reconcileTimeout > 0 {
		return context.WithTimeout(c.ctx, c.services.reconcileTimeout)
	}
	return context.WithCancel(c.ctx)
}

// cloneServiceTemplate snapshots the template fields the service reconcile path
// reads — Runtime, Env, Secrets, and Services — so a live reconciler
// replacing a function's *Template while a request is queued or in flight cannot
// mutate the copy a worker is converging. Template is otherwise treated as
// immutable, but the live registry shares the pointer with the reconciler, so a
// copied slice/map is the race-free envelope. Services are value structs, so a
// copied slice fully detaches them; Env/Secrets get fresh maps. A nil template
// clones to nil.
func cloneServiceTemplate(tmpl *function.Template) *function.Template {
	if tmpl == nil {
		return nil
	}
	clone := *tmpl
	clone.Env = cloneStringMap(tmpl.Env)
	clone.Secrets = cloneSecretMap(tmpl.Secrets)
	clone.Services = append([]function.Service(nil), tmpl.Services...)
	return &clone
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneSecretMap(src map[string]function.SecretRef) map[string]function.SecretRef {
	if src == nil {
		return nil
	}
	dst := make(map[string]function.SecretRef, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (c *ServiceCoordinator) signal(name string) {
	select {
	case c.jobs <- name:
	case <-c.ctx.Done():
	}
}

func (c *ServiceCoordinator) busyLocked() bool {
	for _, state := range c.states {
		if state.running || state.pending != nil {
			return true
		}
	}
	return false
}

// cancelPending drops every not-yet-started request on lifecycle cancellation.
// It releases their done (they will never run) and clears running so Wait stops
// blocking on them. A request already picked up by run is NOT referenced here,
// so its done is NOT released: that worker closes it once its bounded operation
// returns, which is what keeps RemoveAndWait from returning while a removal is
// still executing — even at shutdown. Join's own workersDone barrier waits for
// that worker to finish regardless.
func (c *ServiceCoordinator) cancelPending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	for _, state := range c.states {
		if state.pending != nil {
			close(state.pending.done)
			state.pending = nil
		}
		state.running = false
	}
	c.idle.Broadcast()
}

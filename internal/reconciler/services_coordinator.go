package reconciler

import (
	"context"
	"sync"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
)

const serviceReconcileWorkers = 2

// ServiceRequestHooks are the lifecycle callbacks of one logical service
// request. OnComplete reports only a terminal result (success or failure) and
// is suppressed when a newer desired state supersedes the request or shutdown
// cancels it. OnSettled runs exactly once when the logical request concludes —
// terminal completion, supersession, removal, or cancellation — and is the
// resource-release hook (for example, retiring an app image that a pending
// service pass was still converging away from). It never reports a status.
type ServiceRequestHooks struct {
	OnReconcileStart func()
	OnComplete       func(error)
	OnSettled        func()
}

type serviceJob struct {
	name string
	seq  uint64
}

// serviceRequestKind distinguishes the desired-operation authorization a
// request carries. A status-less desired update must still replace older
// logical work, while a periodic observation must not.
type serviceRequestKind uint8

const (
	// serviceRequestDesired publishes a desired service state. It may carry
	// status callbacks or be status-less, but either way it is logical work
	// that replaces an older pending desired state.
	serviceRequestDesired serviceRequestKind = iota
	// serviceRequestObservation is a verification/self-heal hint. It inherits
	// the retained request's operation fence, never replaces logical work, and
	// never clears or bypasses a future retry deadline.
	serviceRequestObservation
	// serviceRequestRemoval removes the app's service containers. It is its own
	// kind because it supersedes logical work but reports no status.
	serviceRequestRemoval
)

type serviceRequest struct {
	kind        serviceRequestKind
	name        string
	tmpl        *app.Template
	image       string
	preparedEnv []string
	// lease is the managed image lease the producer acquired BEFORE enqueue and
	// owns across one worker pass. It is released at the end of every pass,
	// including a pending pass: a pending logical request does not pin the image
	// between wakes, and Docker container references are what keep candidates
	// safe then. Nil for a removal request and for the legacy unleased enqueue
	// paths (tests).
	lease *runtime.ImageLease
	// done is closed exactly once, by whoever concludes the request's logical
	// life, AFTER its callbacks, lease release, and settlement hook finish. A
	// pending request keeps it open because it will be resumed; its callbacks
	// and desired state are retained. RemoveAndWait waits on it, so the wait
	// ends only after the removal operation (or supersession/cancel) and its
	// settlement conclude.
	done chan struct{}
	// opID is the desired-operation fence. Every accepted desired request
	// (including removal) advances it; a coalesced observation inherits the
	// retained request's operation and never advances it. Backend commit
	// authority and terminal callback authorization are decided against this
	// fence, so a newer desired state of any kind supersedes older work.
	opID             uint64
	onReconcileStart func()
	onComplete       func(error)
	// observationComplete is a terminal observer callback composed from a
	// coalesced observation. The observation itself is not retained, but its
	// completion still belongs to this logical request's terminal result (for
	// example, deployment activation progress). It never runs while pending.
	observationComplete func(error)
	onSettled           func()
	finishOnce          sync.Once
	settleOnce          sync.Once
}

func (r *serviceRequest) finish() {
	if r == nil {
		return
	}
	r.finishOnce.Do(func() {
		if r.done != nil {
			close(r.done)
		}
	})
}

func (r *serviceRequest) settle() {
	if r == nil {
		return
	}
	r.settleOnce.Do(func() {
		if r.onSettled != nil {
			r.onSettled()
		}
	})
}

// serviceAppState is one app's desired-state slot. At most one worker pass runs
// at a time (running). executing is worker-owned; desired is the latest logical
// request and may be a replacement queued behind the active pass. After a
// pending pass, desired remains the request to resume. dirty records an explicit
// wake (Notify or a due deadline) that must not be lost if it arrives while a
// pass is active. deadline is the single per-app future wake time; there are no
// per-app timer goroutines.
type serviceAppState struct {
	desired   *serviceRequest
	running   bool
	executing *serviceRequest
	dirty     bool
	deadline  time.Time
	runSeq    uint64
}

// ServiceCoordinator asynchronously converges persistent services. It permits
// work for different apps to run concurrently, while each app has at most one
// active pass and only its latest desired state is retained. The fixed worker
// count deliberately keeps Docker pressure bounded.
//
// Lifecycle: Start roots the workers in a caller lifecycle; cancelling that
// lifecycle stops the workers and the single scheduler, drops pending logical
// requests, releases their waiters and leases, and releases their settlement
// hooks. A request already executing is never abandoned by cancellation: it
// completes on its bounded operation context and release, and Join waits for the
// workers to exit. RunExclusive is the exclusive housekeeping seam the startup
// pass uses: it waits for active backend work to settle and pauses scheduling
// while its callback (orphan sweep, image sweep, dependency GC) runs, resuming
// with the latest dirty desired states.
//
// Externally pending service work ends the worker pass and releases its image
// lease, but its logical request stays in the app slot. Wait and RunExclusive
// observe only ACTIVE backend work, so a pending request never starves
// housekeeping. Resume re-evaluates the stored current desired request through
// Notify or the one shared deadline scheduler; notifications carry no desired
// authorization and never invoke the app reconciler, fingerprint, or build.
type ServiceCoordinator struct {
	services *ServiceReconciler
	ctx      context.Context
	cancel   context.CancelFunc
	jobs     chan serviceJob

	mu sync.Mutex
	// operationIDs is the latest accepted desired-operation fence per app. A
	// desired request advances it; a coalesced observation inherits the
	// retained request's fence and advances neither the fence nor its logical
	// work.
	operationIDs map[string]uint64
	states       map[string]*serviceAppState
	idle         *sync.Cond
	paused       bool
	stopped      bool
	// settling counts out-of-band conclusion work (cancelPending settlement
	// hooks and waiter closures) that runs without holding mu. busyLocked
	// includes it so Wait/RunExclusive cannot observe idle before it finishes.
	settling     int
	nextID       uint64
	wakeSchedule chan struct{}
	cancelOnce   sync.Once
	// terminalDecided is a test-only synchronization seam invoked after the
	// terminal decision and callback authorization are fixed under mu, and
	// before callbacks are dispatched. Production leaves it nil.
	terminalDecided func()
	wg              sync.WaitGroup
	workersDone     chan struct{}
}

// NewServiceCoordinator creates a coordinator with exactly two workers. Start
// must be called before Enqueue or RemoveAndWait.
func NewServiceCoordinator(services *ServiceReconciler) *ServiceCoordinator {
	return &ServiceCoordinator{
		services:     services,
		jobs:         make(chan serviceJob),
		states:       make(map[string]*serviceAppState),
		operationIDs: make(map[string]uint64),
		wakeSchedule: make(chan struct{}, 1),
	}
}

// Start starts the coordinator workers and its single deadline scheduler under
// lifecycle. It also starts one waiter that closes workersDone once every
// worker has exited, so Join never has to spawn a goroutine that could outlive
// it when its bound expires.
func (c *ServiceCoordinator) Start(lifecycle context.Context) {
	c.ctx, c.cancel = context.WithCancel(lifecycle)
	c.idle = sync.NewCond(&c.mu)
	c.workersDone = make(chan struct{})
	for i := 0; i < serviceReconcileWorkers; i++ {
		c.wg.Add(1)
		go c.loop()
	}
	c.wg.Add(1)
	go c.schedulerLoop()
	go func() {
		c.wg.Wait()
		close(c.workersDone)
	}()
}

// Enqueue publishes the latest desired state for name and returns immediately.
// An in-flight pass is never interrupted; its replacement runs immediately
// afterward using the latest request. The template and prepared env are
// snapshotted (see cloneServiceTemplate) so the live reconciler swapping the
// app's template cannot mutate a queued or in-flight request under a worker.
func (c *ServiceCoordinator) Enqueue(
	name string,
	tmpl *app.Template,
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
	name string, tmpl *app.Template, image string, preparedEnv []string,
	onReconcileStart func(), onComplete func(error),
) {
	c.enqueue(&serviceRequest{
		name: name, tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), onReconcileStart: onReconcileStart,
		onComplete: onComplete,
	})
}

// EnqueueLeased publishes the latest desired state for name with an admitted
// managed image lease that the producer acquired BEFORE enqueue. The lease is
// held for one worker pass and released at the end of that pass, including a
// pending pass. lease may be nil (a no-runtime app or a test caller), making
// this identical to Enqueue.
func (c *ServiceCoordinator) EnqueueLeased(
	name string,
	tmpl *app.Template,
	image string,
	preparedEnv []string,
	lease *runtime.ImageLease,
) {
	c.enqueue(&serviceRequest{
		name:        name,
		tmpl:        cloneServiceTemplate(tmpl),
		image:       image,
		preparedEnv: append([]string(nil), preparedEnv...),
		lease:       lease,
	})
}

// EnqueueWithStatusLeased is EnqueueWithStatus with an admitted image lease
// (see EnqueueLeased).
func (c *ServiceCoordinator) EnqueueWithStatusLeased(
	name string, tmpl *app.Template, image string, preparedEnv []string,
	lease *runtime.ImageLease,
	onReconcileStart func(), onComplete func(error),
) {
	c.enqueue(&serviceRequest{
		name: name, tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), lease: lease,
		onReconcileStart: onReconcileStart, onComplete: onComplete,
	})
}

// EnqueueWithHooksLeased is the lifecycle-aware form: it carries the full
// ServiceRequestHooks (including the settlement hook) with an admitted image
// lease. The worker uses this form so an app image whose service convergence is
// pending is not retired until the logical request concludes.
func (c *ServiceCoordinator) EnqueueWithHooksLeased(
	name string, tmpl *app.Template, image string, preparedEnv []string,
	lease *runtime.ImageLease, hooks ServiceRequestHooks,
) {
	c.enqueue(&serviceRequest{
		name: name, tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), lease: lease,
		onReconcileStart: hooks.OnReconcileStart, onComplete: hooks.OnComplete, onSettled: hooks.OnSettled,
	})
}

// EnqueueObservationWithHooksLeased is EnqueueWithHooksLeased for a
// verification/self-heal observation, which inherits the retained request's
// operation fence rather than advancing it.
func (c *ServiceCoordinator) EnqueueObservationWithHooksLeased(
	name string, tmpl *app.Template, image string, preparedEnv []string,
	lease *runtime.ImageLease, hooks ServiceRequestHooks,
) {
	c.enqueue(&serviceRequest{
		kind: serviceRequestObservation, name: name,
		tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), lease: lease,
		onReconcileStart: hooks.OnReconcileStart, onComplete: hooks.OnComplete,
		onSettled: hooks.OnSettled,
	})
}

// EnqueueStatusObservationLeased is EnqueueStatusObservation with an admitted
// image lease (see EnqueueLeased).
func (c *ServiceCoordinator) EnqueueStatusObservationLeased(
	name string, tmpl *app.Template, image string, preparedEnv []string,
	lease *runtime.ImageLease,
	onReconcileStart func(), onComplete func(error),
) {
	c.enqueue(&serviceRequest{
		kind: serviceRequestObservation, name: name,
		tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv: append([]string(nil), preparedEnv...), lease: lease,
		onReconcileStart: onReconcileStart, onComplete: onComplete,
	})
}

// EnqueueStatusObservation publishes a verification or self-heal pass whose
// callbacks share the retained request's operation fence. An observation must
// not make a slow source-generation completion stale merely because a periodic
// tick happened while it was in flight. A newer desired state still supersedes
// both the observation and any older operation.
func (c *ServiceCoordinator) EnqueueStatusObservation(
	name string, tmpl *app.Template, image string, preparedEnv []string,
	onReconcileStart func(), onComplete func(error),
) {
	c.enqueue(&serviceRequest{
		kind: serviceRequestObservation, name: name,
		tmpl: cloneServiceTemplate(tmpl), image: image,
		preparedEnv:      append([]string(nil), preparedEnv...),
		onReconcileStart: onReconcileStart, onComplete: onComplete,
	})
}

// EnqueueRemove publishes a removal and returns immediately. It is the
// nonblocking counterpart of RemoveAndWait for callers that must not wait on
// Docker work (startup's unavailable/no-services cleanup). Like any desired
// state it serializes behind the app's active pass, supersedes a pending
// logical request, and is itself replaced by a newer enqueue.
func (c *ServiceCoordinator) EnqueueRemove(name string) {
	c.enqueue(&serviceRequest{kind: serviceRequestRemoval, name: name})
}

// RemoveAndWait serializes removal behind any active pass for name and waits
// DETERMINISTICALLY until the request concludes. It is the reconciler's removal
// hook, called before the app's images are retired: returning while the
// container stop was still running (e.g. because a caller timeout expired) would
// let image retirement race the very removal it depends on, so there is no
// early-return timeout path. The wait ends when the worker closes the request's
// done after the removal returns; at shutdown the lifecycle cancels the bounded
// removal context, so the wait is still bounded. A removal superseded by a newer
// enqueue before it starts concludes without running — not reachable from the
// removal hook, which runs after the app is gone and no further desired
// state is published for it.
//
// The removal operation is bounded by a fresh coordinator-derived context
// (reconcileTimeout rooted in the lifecycle), not by the caller, so a queued
// removal still gets its own full bound when it is picked up.
func (c *ServiceCoordinator) RemoveAndWait(name string) {
	<-c.enqueue(&serviceRequest{kind: serviceRequestRemoval, name: name})
}

// Notify requests a nonblocking re-evaluation of name's CURRENT stored desired
// request. It carries no desired authorization: if no logical request exists it
// is ignored, and it never invokes the app reconciler, fingerprint, or build.
// A notification arriving while a pass is active sets the coalesced dirty bit
// so it survives the pass's pending transition. An already pending request is
// resumed; a pending request is never retried without an explicit wake.
//
// A terminal callback has already concluded its own request, so Notify then
// finds no current request and intentionally does nothing. A callback that
// publishes new desired work must enqueue it first; a newer desired request
// queued behind the terminal one is retained and Notify's dirty bit applies to
// it normally.
func (c *ServiceCoordinator) Notify(name string) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	state := c.states[name]
	if state == nil || state.desired == nil {
		c.mu.Unlock()
		return
	}
	state.dirty = true
	job, ok := c.scheduleLocked(name, state)
	c.mu.Unlock()
	if ok {
		go c.signal(job)
	}
}

// Schedule installs name's single future wake deadline. A newer desired state
// cancels it, CancelSchedule removes it, and a due deadline only marks the
// current logical request dirty: it never validates or creates desired
// authorization. Deadlines never fire early; they are coalesced per app and
// served by one coordinator scheduler, not a timer per app. A zero deadline is
// equivalent to CancelSchedule.
func (c *ServiceCoordinator) Schedule(name string, deadline time.Time) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	state := c.states[name]
	if state == nil || state.desired == nil {
		c.mu.Unlock()
		return
	}
	if deadline.IsZero() {
		state.deadline = time.Time{}
		c.wakeSchedulerLocked()
		c.mu.Unlock()
		return
	}
	state.deadline = deadline
	var job serviceJob
	scheduled := false
	if !deadline.After(time.Now()) {
		state.deadline = time.Time{}
		state.dirty = true
		job, scheduled = c.scheduleLocked(name, state)
	} else {
		c.wakeSchedulerLocked()
	}
	c.mu.Unlock()
	if scheduled {
		go c.signal(job)
	}
}

// CancelSchedule removes name's future wake deadline, if any.
func (c *ServiceCoordinator) CancelSchedule(name string) {
	c.mu.Lock()
	if state := c.states[name]; state != nil && !state.deadline.IsZero() {
		state.deadline = time.Time{}
		c.wakeSchedulerLocked()
	}
	c.mu.Unlock()
}

// Wait waits for all ACTIVE backend service work published before the call to
// finish, including its terminal callbacks, settlement hooks, and any
// cancellation settlement/waiter closure. A logical request that is externally
// pending does not keep Wait blocked: it holds no worker and no image lease
// between wakes. Wait returns ctx.Err() when ctx is cancelled before the
// barrier clears. RunExclusive builds the exclusive housekeeping seam on the
// same idle primitive.
//
// Cancellation is handled by a context.AfterFunc watcher that broadcasts the
// condition, so a Wait parked with no worker progress wakes promptly instead of
// depending on a later worker broadcast.
func (c *ServiceCoordinator) Wait(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitIdleLocked(ctx)
}

// waitIdleLocked parks on the idle condition until no app has active backend
// work (busyLocked), returning ctx.Err() on cancellation. The caller must hold
// c.mu; it is still held on return.
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
// active service work is idle, then PAUSES scheduling of new work for the
// duration of fn, so a service-dependent cleanup (orphan sweep, image sweep,
// dependency GC) can never overlap an Apply, a Remove, or a worker picking up a
// freshly enqueued request. Requests published while fn runs are neither dropped
// nor run concurrently: they coalesce as dirty desired states and the latest
// state per app is scheduled when fn returns. Externally pending logical
// requests do not block the barrier; Docker references protect their candidate
// images once their per-pass lease has been released.
//
// fn runs with ctx and is expected to be the bounded, lifecycle-aware cleanup
// sequence. A nil return means fn ran. A non-nil return is ctx.Err() and means
// the barrier never cleared — fn was NOT invoked. The pause is always released.
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
	c.paused = true
	c.mu.Unlock()

	defer c.resume()
	fn(ctx)
	return nil
}

// Join waits for active work to drain (Wait) and then for every worker and the
// scheduler to exit. It is the shutdown counterpart of Wait: the caller cancels
// the lifecycle first, and Join reports the wait/join error if its own bound
// expires. workersDone is closed by the single waiter Start launched.
func (c *ServiceCoordinator) Join(ctx context.Context) error {
	if err := c.Wait(ctx); err != nil {
		return err
	}
	if c.workersDone == nil {
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
		req.releaseLease()
		req.settle()
		req.finish()
		return req.done
	}
	state := c.states[req.name]
	if state == nil {
		state = &serviceAppState{}
		c.states[req.name] = state
	}
	// An observation is a hint, never desired authorization: it must not
	// replace retained logical work (a pending desired request or a removal),
	// and it must not clear or run ahead of a future retry deadline. Its
	// terminal observer callback is composed onto the retained request so a
	// coalesced wake still reports activation progress when that request
	// concludes terminally.
	if req.kind == serviceRequestObservation && state.desired != nil {
		retained := state.desired
		if req.onComplete != nil && retained.kind != serviceRequestRemoval {
			retained.observationComplete = req.onComplete
		}
		var job serviceJob
		scheduled := false
		if retained.kind != serviceRequestRemoval {
			switch {
			case state.deadline.After(time.Now()):
				// A future retry remains authoritative: the observation neither
				// wakes early nor clears it.
			case state.executing != nil:
				// Coalesce into the active pass's dirty bit so the hint is not
				// lost at the pending transition.
				state.dirty = true
			case state.deadline.IsZero():
				state.dirty = true
				job, scheduled = c.scheduleLocked(req.name, state)
			}
		}
		c.mu.Unlock()
		req.releaseLease()
		req.settle()
		req.finish()
		if scheduled {
			go c.signal(job)
		}
		return req.done
	}
	c.nextID++
	req.opID = c.nextID
	c.operationIDs[req.name] = req.opID
	// A newer desired state replaces the current logical request whenever the
	// current one is not executing: it will never run (or resume), so conclude
	// it now. An executing request is left to its worker, which observes
	// supersession at the pass tail.
	var superseded *serviceRequest
	if state.desired != nil && state.desired != state.executing {
		superseded = state.desired
		state.desired = nil
	}
	state.desired = req
	state.dirty = true
	// A newer desired state supersedes any future wake for the old logical
	// request. A deadline belongs to the request, never to the app forever.
	state.deadline = time.Time{}
	c.wakeSchedulerLocked()
	job, scheduled := c.scheduleLocked(req.name, state)
	c.mu.Unlock()
	if superseded != nil {
		// Conclude the replaced logical request outside the mutex: its
		// settlement hook is resource cleanup and must never hold the
		// coordinator lock. The waiter is released last, after the logical
		// completion has fully settled.
		superseded.releaseLease()
		superseded.settle()
		superseded.finish()
	}
	if scheduled {
		go c.signal(job)
	}
	return req.done
}

// resume lifts the RunExclusive pause and schedules every app with a dirty
// desired state that acquired one during the pause. It is only ever called by
// RunExclusive, the sole writer of paused. Signals are launched outside the
// mutex, matching enqueue.
func (c *ServiceCoordinator) resume() {
	c.mu.Lock()
	c.paused = false
	var start []serviceJob
	for name, state := range c.states {
		if job, ok := c.scheduleLocked(name, state); ok {
			start = append(start, job)
		}
	}
	c.mu.Unlock()
	for _, job := range start {
		go c.signal(job)
	}
}

func (c *ServiceCoordinator) loop() {
	defer c.wg.Done()
	for {
		select {
		case job := <-c.jobs:
			c.run(job)
		case <-c.ctx.Done():
			c.cancelPending()
			return
		}
	}
}

func (c *ServiceCoordinator) run(job serviceJob) {
	c.mu.Lock()
	state := c.states[job.name]
	if state == nil || !state.running || state.desired == nil || state.runSeq != job.seq {
		// A stale signal (coalesced/superseded) must not retry a pending
		// logical request or start an unscheduled pass.
		c.mu.Unlock()
		return
	}
	req := state.desired
	// This pass covers every wake observed so far. A notification arriving
	// during the pass sets dirty again; that is the coalesced dirty bit.
	state.dirty = false
	state.executing = req
	c.mu.Unlock()

	if req.kind == serviceRequestRemoval {
		c.runRemoval(req)
		return
	}

	reconcileStarted := req.onReconcileStart
	if reconcileStarted != nil {
		reconcileStarted = func() {
			if c.currentOperation(req) {
				req.onReconcileStart()
			}
		}
	}
	applyCtx := c.ctx
	if req.lease != nil {
		applyCtx = runtime.WithImageLease(applyCtx, req.lease)
	}
	applyCtx = withReconcileAuthority(applyCtx, func() bool { return c.currentOperation(req) })
	result := c.services.ApplyResult(
		applyCtx, req.name, req.tmpl, req.image, req.preparedEnv, reconcileStarted,
	)
	// The lease covers exactly one pass. A pending request keeps no image pinned
	// between wakes; Docker references protect candidates from retirement then.
	req.releaseLease()

	var complete, observationComplete func(error)
	settle := false
	conclude := false
	callbacksAuthorized := false
	c.mu.Lock()
	superseded := c.operationIDs[req.name] != req.opID
	switch {
	case c.stopped:
		// Shutdown concluded the request. Its waiter, lease, and settlement
		// hook are released, but no deferred success callback runs and no
		// pending request is retained past the lifecycle.
		settle = true
		conclude = true
		if state.desired == req {
			state.desired = nil
			state.deadline = time.Time{}
		}
	case superseded:
		// A newer desired operation owns convergence. Conclude this logical
		// request without invoking its stale completion callback; its settlement
		// hook still releases deferred resources.
		settle = true
		conclude = true
		if state.desired == req {
			state.desired = nil
			state.deadline = time.Time{}
		}
	case result.Pending():
		if state.desired != req {
			// Defensively conclude a request detached without an operation-fence
			// change; ordinary supersession is handled by the fence above.
			settle = true
			conclude = true
		} else {
			// The logical request, callbacks, and desired state remain for a
			// later Notify/Schedule. No final callback runs and no old
			// generation is stopped. The backend pass is over, so it must no
			// longer look executing: lifecycle cancellation has to be able to
			// drop it.
			state.executing = nil
		}
	case state.dirty && state.desired == req:
		// A wake arrived while this pass ran. Keep the same logical request and
		// rerun the CURRENT stored desired state instead of losing the wake at
		// the pending transition. The backend pass is over; only the coalesced
		// rerun remains scheduled.
		state.executing = nil
	default:
		complete = req.onComplete
		observationComplete = req.observationComplete
		settle = true
		conclude = true
		// Linearize callback authorization with the terminal decision. A
		// status-less desired update may advance operationIDs immediately after
		// this unlock; it must not retroactively suppress a callback for a
		// request whose terminal conclusion is already decided and unlinked.
		callbacksAuthorized = c.liveLocked() && c.operationIDs[req.name] == req.opID
		if state.desired == req {
			state.desired = nil
			state.deadline = time.Time{}
		}
	}
	c.mu.Unlock()

	if callbacksAuthorized && c.terminalDecided != nil {
		c.terminalDecided()
	}

	// The app stays logically busy until its terminal callbacks and settlement
	// hooks finish: a same-app pass must not overlap them, and Wait/RunExclusive
	// must not observe the app idle early. Callbacks stay outside the mutex so a
	// callback-triggered enqueue/Notify is never blocked. The operation fence
	// was already linearized above; liveness is rechecked immediately before
	// EACH callback, so a shutdown that begins while the completion callback is
	// still running also suppresses the composed observer callback. Settlement
	// and waiter closure below always run.
	if callbacksAuthorized {
		if complete != nil && c.callbacksPermitted() {
			complete(result.Err)
		}
		if observationComplete != nil && c.callbacksPermitted() {
			observationComplete(result.Err)
		}
	}
	if settle {
		req.settle()
	}
	// Release the waiter only after the whole logical completion has settled.
	if conclude {
		req.finish()
	}

	c.mu.Lock()
	state.running = false
	state.executing = nil
	next, scheduled := c.scheduleLocked(req.name, state)
	// Waiting for active backend work may proceed now: a pending logical
	// request retains no worker.
	c.idle.Broadcast()
	c.mu.Unlock()

	if scheduled {
		go c.signal(next)
	}
}

func (c *ServiceCoordinator) runRemoval(req *serviceRequest) {
	// Removal is a single quick Docker operation on a fresh bounded context
	// rooted in the coordinator lifecycle. Bounding it here is what keeps
	// RemoveAndWait's deterministic wait safe.
	opCtx, cancel := c.removalContext()
	c.services.Remove(opCtx, req.name)
	cancel()

	req.releaseLease()
	// The operation returned. A newer desired state enqueued while removal ran
	// remains to be scheduled below; the waiter is released only after the
	// logical completion has fully settled.
	c.mu.Lock()
	state := c.states[req.name]
	if state == nil {
		c.mu.Unlock()
		req.settle()
		req.finish()
		return
	}
	if state.desired == req {
		state.desired = nil
		state.deadline = time.Time{}
	}
	c.mu.Unlock()

	// Keep the app logically busy through settlement, like a terminal service
	// pass, but run the hook outside the mutex.
	req.settle()
	req.finish()

	c.mu.Lock()
	state.running = false
	state.executing = nil
	next, scheduled := c.scheduleLocked(req.name, state)
	c.idle.Broadcast()
	c.mu.Unlock()
	if scheduled {
		go c.signal(next)
	}
}

// currentOperation reports whether req still owns its app's desired-operation
// fence AND the coordinator is still live. It is the backend commit authority
// and onReconcileStart gate: a newer desired state of ANY kind (including a
// status-less update) supersedes it, while a coalesced observation inherits the
// retained operation and does not; shutdown or ownership loss also fails it.
func (c *ServiceCoordinator) currentOperation(req *serviceRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveLocked() && c.operationIDs[req.name] == req.opID
}

// callbacksPermitted reports whether terminal status callbacks may still be
// dispatched. The terminal operation decision is linearized separately; this
// only rechecks that coordination has not been stopped or its lifecycle
// cancelled in the gap between the decision and dispatch.
func (c *ServiceCoordinator) callbacksPermitted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveLocked()
}

// liveLocked reports whether coordination still authorizes work and status
// callbacks. The caller must hold c.mu.
func (c *ServiceCoordinator) liveLocked() bool {
	return !c.stopped && (c.ctx == nil || c.ctx.Err() == nil)
}

// removalContext derives the bounded context a queued removal runs under: the
// reconciler's reconcileTimeout rooted in the coordinator lifecycle.
func (c *ServiceCoordinator) removalContext() (context.Context, context.CancelFunc) {
	if c.services.reconcileTimeout > 0 {
		return context.WithTimeout(c.ctx, c.services.reconcileTimeout)
	}
	return context.WithCancel(c.ctx)
}

// scheduleLocked starts one pass iff the app has a dirty desired request and no
// active pass, scheduling is not paused, and the coordinator is live. It
// returns the job to signal outside the mutex.
func (c *ServiceCoordinator) scheduleLocked(name string, state *serviceAppState) (serviceJob, bool) {
	if c.paused || c.stopped || state.running || state.desired == nil || !state.dirty {
		return serviceJob{}, false
	}
	if c.ctx != nil && c.ctx.Err() != nil {
		return serviceJob{}, false
	}
	state.running = true
	state.runSeq++
	return serviceJob{name: name, seq: state.runSeq}, true
}

// schedulerLoop is the single timer goroutine for every app's deadline. It
// scans the bounded per-app state map for the earliest deadline and fires only
// due deadlines; there is no timer or goroutine per app.
func (c *ServiceCoordinator) schedulerLoop() {
	defer c.wg.Done()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		c.mu.Lock()
		now := time.Now()
		var due []serviceJob
		var next time.Time
		for name, state := range c.states {
			if state.deadline.IsZero() {
				continue
			}
			if !state.deadline.After(now) {
				state.deadline = time.Time{}
				state.dirty = true
				if job, ok := c.scheduleLocked(name, state); ok {
					due = append(due, job)
				}
				continue
			}
			if next.IsZero() || state.deadline.Before(next) {
				next = state.deadline
			}
		}
		stopped := c.stopped
		c.mu.Unlock()

		for _, job := range due {
			go c.signal(job)
		}
		if stopped {
			return
		}
		if next.IsZero() {
			select {
			case <-c.wakeSchedule:
				continue
			case <-c.ctx.Done():
				return
			}
		}
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer.Reset(wait)
		select {
		case <-timer.C:
		case <-c.wakeSchedule:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-c.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (c *ServiceCoordinator) wakeSchedulerLocked() {
	select {
	case c.wakeSchedule <- struct{}{}:
	default:
	}
}

func (c *ServiceCoordinator) signal(job serviceJob) {
	select {
	case c.jobs <- job:
	case <-c.ctx.Done():
	}
}

// busyLocked reports whether any app has an ACTIVE worker pass or an
// out-of-band conclusion (cancelPending settlement/waiter closure) is still
// running. A pending logical request (desired != nil with running false) is not
// busy: it retains no worker and no lease between wakes.
func (c *ServiceCoordinator) busyLocked() bool {
	if c.settling > 0 {
		return true
	}
	for _, state := range c.states {
		if state.running {
			return true
		}
	}
	return false
}

// cancelPending drops every not-yet-active logical request on lifecycle
// cancellation. It releases their lease and settlement hook, and only then
// their waiter. A request already picked up by run is NOT referenced here, so
// its worker concludes it after its bounded operation returns, which keeps
// RemoveAndWait from returning while a removal is still executing.
//
// Exactly one caller performs the drop and the post-settlement idle broadcast:
// multiple workers observe lifecycle cancellation, and an early broadcast from
// a second caller could release Wait while the first is still settling.
func (c *ServiceCoordinator) cancelPending() {
	c.cancelOnce.Do(c.cancelPendingOnce)
}

func (c *ServiceCoordinator) cancelPendingOnce() {
	var cancelled []*serviceRequest
	c.mu.Lock()
	c.stopped = true
	for _, state := range c.states {
		state.deadline = time.Time{}
		state.dirty = false
		if state.executing != nil {
			// The worker owns this logical request and concludes it after its
			// bounded operation returns.
			if state.desired != nil && state.desired != state.executing {
				cancelled = append(cancelled, state.desired)
				state.desired = nil
			}
			continue
		}
		// A scheduled-but-not-yet-picked request is safely cancellable: the
		// worker will observe the stale run sequence and return.
		state.running = false
		if state.desired == nil {
			continue
		}
		cancelled = append(cancelled, state.desired)
		state.desired = nil
	}
	if len(cancelled) > 0 {
		// Keep the idle barrier busy while the hooks and waiter closures below
		// run without mu. An executing request on another worker may finish and
		// broadcast in the meantime; Wait/RunExclusive must still wait for this
		// cancellation settlement.
		c.settling++
	}
	c.wakeSchedulerLocked()
	c.mu.Unlock()

	for _, req := range cancelled {
		req.releaseLease()
		req.settle()
		req.finish()
	}

	// Waiter wakeups observe the completed settlement above.
	c.mu.Lock()
	if len(cancelled) > 0 {
		c.settling--
	}
	c.idle.Broadcast()
	c.mu.Unlock()
}

// cloneServiceTemplate snapshots the template fields the service reconcile path
// reads — Runtime, Env, Secrets, and Services — so a live reconciler
// replacing an app's *Template while a request is queued or in flight cannot
// mutate the copy a worker is converging. A nil template clones to nil.
func cloneServiceTemplate(tmpl *app.Template) *app.Template {
	if tmpl == nil {
		return nil
	}
	clone := *tmpl
	clone.Env = cloneStringMap(tmpl.Env)
	clone.Secrets = cloneSecretMap(tmpl.Secrets)
	clone.Services = append([]app.Service(nil), tmpl.Services...)
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

func cloneSecretMap(src map[string]app.SecretRef) map[string]app.SecretRef {
	if src == nil {
		return nil
	}
	dst := make(map[string]app.SecretRef, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// releaseLease drops the request's admitted image lease, if any, exactly once.
// It is idempotent and nil-safe, so the multiple pass-ending paths (pending,
// supersede, cancellation, completion) can each call it without coordination.
func (r *serviceRequest) releaseLease() {
	if r == nil || r.lease == nil {
		return
	}
	lease := r.lease
	r.lease = nil
	lease.Release()
}

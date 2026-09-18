package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// reusableContainer is the seam the per-function container pool programs: the
// production implementation is *executionContainer; tests inject fake
// containers to exercise the pool's get-or-create, leasing, capacity, and
// invalidation logic without a Docker daemon.
type reusableContainer interface {
	// Invoke runs one handler invocation against the container.
	Invoke(ctx context.Context, handler string, eventJSON []byte, env map[string]string) error
	// discard tears the container down (idempotent). reason is one of the
	// documented discard reasons.
	discard(reason string) bool
	// dead reports whether the container has been discarded.
	dead() bool
}

// errPoolClosed is returned when an acquire is attempted on a cache/pool that
// has been closed (graceful shutdown). It is not a container failure: the
// stream layer leaves the invocation pending and a live worker replays it.
var errPoolClosed = errors.New("runtime: container pool closed")

// containerCache owns a bounded warm pool of execution containers per FUNCTION
// NAME (a function never touches another function's containers). Each pool
// holds at most the function's resolved concurrency containers; an invocation
// LEASES one container for the duration of its Invoke and returns it on
// release. Distinct invocations of the same function therefore run
// concurrently on distinct containers (each container still serializes its own
// protocol I/O), which is what makes the reused request/response frames
// well-defined on a shared stdin/stdout pair.
//
// The pool's bound is the function's concurrency, the SAME value the runner's
// per-function semaphore is sized to. It is deliberately NOT a second
// invocation-concurrency limiter: in the runner path the per-function semaphore
// already admits at most `concurrency` concurrent Execute calls, so the pool
// always has capacity and never blocks; direct Execute callers (integration
// tests) that bypass the runner are bounded by the pool itself.
//
// Locking: a manager-wide mu guards the map itself; each function pool carries
// its own mutex. The manager-wide mu is never held during create/Invoke, and a
// pool mutex is never held across start() or Invoke.
type containerCache struct {
	// mu guards pools, retiredImages, and closed.
	mu    sync.Mutex
	pools map[string]*functionPool
	// retiredImages is the cache-level retirement set. invalidateImage records
	// an image here in the SAME critical section that snapshots the pools, and
	// poolFor seeds every newly created pool from it. This closes the race where
	// invalidateImage snapshots the pools, unlocks, and a concurrent first
	// acquire creates a fresh pool that never sees the retirement and pools the
	// retired image. Because recording and seeding both happen under mu, a pool
	// is always covered by either the snapshot (it already existed) or the seed
	// (it is created after the retirement was recorded).
	//
	// Like the per-pool set, retirements are permanent for the process and
	// bounded by the number of distinct image versions retired in a worker's
	// lifetime. Guarded by mu.
	retiredImages map[string]bool
	closed        bool
}

// functionPool is one function's bounded warm container pool.
//
// Capacity accounting: len(idle)+len(busy)+creating must stay <= max.
// creating is the number of capacity reservations in flight (a lazy start that
// has not yet registered its container); it is rolled back if start fails.
type functionPool struct {
	mu   sync.Mutex
	max  int
	idle []*pooledContainer
	// busy holds leased REGULAR containers; each counts toward max and is
	// returned to idle (or discarded) on release.
	busy map[*pooledContainer]struct{}
	// transient holds leased THROWAWAY containers serving a stale request for
	// an already-retired image. They are never pooled and deliberately do NOT
	// count against the regular capacity (a stale request must not be blocked
	// by, nor evict, the current version's containers). They are bounded among
	// themselves by max; transientCreating reserves those starts so concurrent
	// stale acquires cannot race past the bound. close tears them down; release
	// discards them.
	transient         map[*pooledContainer]struct{}
	transientCreating int

	// activeImage is the image version the pool currently serves. When an
	// acquire requests a DIFFERENT image, the pool has moved forward: every
	// container from the superseded version is retired (idle ones immediately,
	// busy ones on release) and the old image is recorded in retiredImages.
	// Guarded by mu.
	activeImage string

	// retiredImages holds image references whose containers must never be
	// pooled again (image retirement/invalidation or a superseded active
	// image). A container created for a retired image is marked retired at
	// creation: it serves its invocation at-least-once and is discarded on
	// release, so no later acquire can reuse it ("no new acquires old image").
	//
	// Retirements are permanent for the process: a content-addressed image
	// whose source is reverted to a previously-retired fingerprint is treated
	// as retired too, so its invocations run on throwaway containers (correct,
	// just not warm). Distinguishing a revert from a stale in-flight request is
	// impossible from Execute alone, and mistaking a stale request for a revert
	// would let it tear down the current version. The set is bounded by the
	// number of distinct function versions retired in a worker's lifetime.
	// Guarded by mu.
	retiredImages map[string]bool

	// creating is the number of in-progress lazy starts holding a capacity
	// reservation. Guarded by mu.
	creating int

	// closed is set once by close; acquire then fails immediately and every
	// current container is discarded.
	closed bool

	// notify is closed (and replaced) on every state change that may unblock a
	// waiter: a release, an idle container becoming available, invalidation
	// freeing capacity, or close. Waiters select on a snapshot of it plus ctx,
	// so blocked acquires are woken by notification, never by polling and never
	// by a goroutine per waiter.
	notify chan struct{}
}

// pooledContainer is one reusable container in a function pool, with its image
// version and retirement state. retired/retireReason are guarded by the owning
// functionPool.mu; membership in idle/busy is the lease state.
type pooledContainer struct {
	c     reusableContainer
	image string
	// retired marks a container that must not be leased again and must be
	// discarded as soon as it is not busy (busy ones are discarded on release).
	retired      bool
	retireReason string
}

func newContainerCache() *containerCache {
	return &containerCache{
		pools:         map[string]*functionPool{},
		retiredImages: map[string]bool{},
	}
}

func newFunctionPool(max int) *functionPool {
	if max < 1 {
		max = 1
	}
	return &functionPool{
		max:           max,
		busy:          map[*pooledContainer]struct{}{},
		transient:     map[*pooledContainer]struct{}{},
		retiredImages: map[string]bool{},
		notify:        make(chan struct{}),
	}
}

// lazyInit ensures the maps exist (a zero-valued Manager from tests must still
// be Close-able). It must be called with cc.mu held.
func (cc *containerCache) lazyInit() {
	if cc.pools == nil {
		cc.pools = map[string]*functionPool{}
	}
	if cc.retiredImages == nil {
		cc.retiredImages = map[string]bool{}
	}
}

// poolFor returns (creating) the pool for fnName. Capacity is first-wins: the
// first acquire fixes the pool size to the function's resolved concurrency; a
// later value is ignored, mirroring the runner's per-function semaphore policy
// (a hot-swapped concurrency change requires a worker restart). A pool requested
// after the cache is closed is returned closed so acquire fails immediately.
func (cc *containerCache) poolFor(fnName string, max int) *functionPool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	if p, ok := cc.pools[fnName]; ok {
		return p
	}
	p := newFunctionPool(max)
	// Seed the new pool from the cache-level retirement set under the same mu
	// that snapshots it below, so an invalidation that already happened (and
	// therefore may have missed this pool) is still honored: a stale acquire
	// that races pool creation can never pool the retired image.
	for retired := range cc.retiredImages {
		p.retiredImages[retired] = true
	}
	if cc.closed {
		p.closed = true
		close(p.notify)
	}
	cc.pools[fnName] = p
	return p
}

// execute runs one invocation through a leased container for fnName. start
// creates a fresh container when the pool has no idle one and capacity remains
// (lazily); it returns the caller's error verbatim on failure. A container that
// the invocation poisoned (timeout/process exit/protocol error/panic) is
// dropped on release so a later call starts fresh; a plain handler error keeps
// the container.
func (cc *containerCache) execute(ctx context.Context, fnName, image string, max int, start func() (reusableContainer, error), handler string, eventJSON []byte, env map[string]string) error {
	lease, err := cc.acquire(ctx, fnName, image, max, start)
	if err != nil {
		return err
	}
	// The lease is always returned, even if the invocation panics: a panic
	// must never leak capacity. invoke poisons the container first when it
	// panics, so release discards rather than reuses a possibly-corrupt one.
	defer lease.release()
	return lease.invoke(ctx, handler, eventJSON, env)
}

// acquire leases one container for fnName/image, blocking (context-aware) when
// the pool is at capacity until a release/close notifies it. It:
//
//   - reaps idle containers that are dead, retired, or from a superseded image
//     version (they are discarded, never leased again). A request for a NEW
//     image retires the previously active image — discarding its idle
//     containers now and its busy ones on release — and moves activeImage
//     forward;
//   - refuses to POOL containers for an image that has been invalidated: a
//     stale request for a RETIRED image is served at-least-once on a throwaway
//     (transient) container that is never pooled and is discarded on release
//     ("no new acquires old image"), without reaping, rewinding, or waiting on
//     the current (newer) version. The runner's per-function semaphore keeps
//     the number of such in-flight stale requests bounded in production;
//   - leases an idle matching container, or lazily starts a new one while
//     capacity remains (reserving the slot before start so concurrent waiters
//     account for it, and rolling the reservation back on start failure).
//
// invalidateImage (the runner's image-retirement path) is what retires busy
// containers for a known-dead image; the forward transition above covers direct
// callers that switch Prepared without an explicit invalidation.
func (cc *containerCache) acquire(ctx context.Context, fnName, image string, max int, start func() (reusableContainer, error)) (*containerLease, error) {
	// A panic escaping start() must not leak a capacity reservation, or the
	// pool would be permanently at capacity. reserved/transientReserved track
	// which reservation is currently held so the unwind path can roll it back.
	var reserved *functionPool
	var transientReserved *functionPool
	defer func() {
		if r := recover(); r != nil {
			if reserved != nil {
				reserved.mu.Lock()
				reserved.creating--
				reserved.signalLocked()
				reserved.mu.Unlock()
			}
			if transientReserved != nil {
				transientReserved.mu.Lock()
				transientReserved.transientCreating--
				transientReserved.signalLocked()
				transientReserved.mu.Unlock()
			}
			panic(r)
		}
	}()

	for {
		p := cc.poolFor(fnName, max)

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}

		// A stale request for an already-invalidated image is served on a
		// throwaway container that is never pooled ("no new acquires old
		// image"). Transients are bounded by max among themselves but do NOT
		// consume the regular pool capacity, so a stale request can neither be
		// blocked by nor evict the current version's idle containers.
		if p.retiredImages[image] {
			if len(p.transient)+p.transientCreating >= p.max {
				notify := p.notify
				p.mu.Unlock()
				select {
				case <-notify:
					continue
				case <-ctx.Done():
					return nil, fmt.Errorf("docker run: %w", ctx.Err())
				}
			}
			p.transientCreating++
			transientReserved = p
			p.mu.Unlock()
			c, err := start()
			p.mu.Lock()
			p.transientCreating--
			transientReserved = nil
			if err != nil {
				p.signalLocked()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				p.signalLocked()
				p.mu.Unlock()
				if !c.dead() {
					c.discard("shutdown")
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image, retired: true, retireReason: "image_changed"}
			p.transient[pc] = struct{}{}
			p.mu.Unlock()
			return &containerLease{pool: p, pc: pc}, nil
		}

		// Forward image transition: a request for a new image supersedes the
		// active version. The old image is retired so its idle containers are
		// discarded now and its busy ones on release, and it can never be
		// pooled again. This also covers direct callers that use a new Prepared
		// without a runner InvalidateImage call.
		var transitioned []*pooledContainer
		if p.activeImage != "" && p.activeImage != image {
			old := p.activeImage
			p.retiredImages[old] = true
			kept := p.idle[:0]
			for _, pc := range p.idle {
				if pc.image == old && !pc.retired {
					pc.retired = true
					pc.retireReason = "image_changed"
					transitioned = append(transitioned, pc)
					continue
				}
				kept = append(kept, pc)
			}
			p.idle = kept
			for pc := range p.busy {
				if pc.image == old && !pc.retired {
					pc.retired = true
					pc.retireReason = "image_changed"
				}
			}
		}
		p.activeImage = image

		// Reap idle containers that may not be leased, before the capacity check
		// so a stale idle container does not consume a slot: dead ones, retired
		// ones, and live ones from a superseded image version.
		var reaped []*pooledContainer
		kept := p.idle[:0]
		for _, pc := range p.idle {
			if pc.c.dead() {
				continue
			}
			if pc.retired || pc.image != image {
				pc.retired = true
				if pc.retireReason == "" {
					pc.retireReason = "image_changed"
				}
				reaped = append(reaped, pc)
				continue
			}
			kept = append(kept, pc)
		}
		p.idle = kept
		reaped = append(reaped, transitioned...)
		if len(transitioned) > 0 {
			p.signalLocked()
		}

		// Prefer an idle matching container.
		for i := len(p.idle) - 1; i >= 0; i-- {
			pc := p.idle[i]
			if pc.image != image || pc.retired {
				continue
			}
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			p.busy[pc] = struct{}{}
			p.mu.Unlock()
			cc.discardReaped(reaped)
			return &containerLease{pool: p, pc: pc}, nil
		}

		// Lazily create a fresh container while total capacity remains.
		if len(p.idle)+len(p.busy)+p.creating < p.max {
			p.creating++
			reserved = p
			p.mu.Unlock()
			cc.discardReaped(reaped)

			c, err := start()
			p.mu.Lock()
			p.creating--
			reserved = nil
			if err != nil {
				// Roll the reservation back and wake a waiter so the freed
				// slot can be used by a fresh start.
				p.signalLocked()
				p.mu.Unlock()
				return nil, err
			}
			if p.closed {
				// Shutdown won the race: never register or lease the fresh
				// container; discard it through the shutdown path.
				p.signalLocked()
				p.mu.Unlock()
				if !c.dead() {
					c.discard("shutdown")
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image}
			if p.retiredImages[image] {
				// Invalidation landed while this start was in flight: serve the
				// invocation, but never pool the container.
				pc.retired = true
				pc.retireReason = "image_changed"
				p.transient[pc] = struct{}{}
			} else {
				p.busy[pc] = struct{}{}
			}
			p.mu.Unlock()
			return &containerLease{pool: p, pc: pc}, nil
		}

		// At capacity: wait for a release/close notification. The pool mutex is
		// dropped while blocked so release/invalidate/close can make progress.
		notify := p.notify
		p.mu.Unlock()
		cc.discardReaped(reaped)

		select {
		case <-notify:
			// State changed: retry under a fresh lock.
		case <-ctx.Done():
			return nil, fmt.Errorf("docker run: %w", ctx.Err())
		}
	}
}

// discardReaped discards (idempotently) containers acquire removed from the
// idle list. It runs outside the pool lock; dead containers are skipped because
// their own discard path already ran. The recorded retire reason is used so an
// image-change discard keeps its cause.
func (cc *containerCache) discardReaped(reaped []*pooledContainer) {
	for _, pc := range reaped {
		if pc.c.dead() {
			continue
		}
		reason := pc.retireReason
		if reason == "" {
			reason = "image_changed"
		}
		pc.c.discard(reason)
	}
}

// invalidateImage retires every pooled container running image across every
// function: idle ones are discarded immediately, busy ones are marked retired
// and discarded when their invocation releases. It never blocks on an in-flight
// invocation (the pool mutex is not held across Invoke) and, once run, no later
// acquire can be handed a pre-invalidation container for that image. It must
// never block image retirement.
func (cc *containerCache) invalidateImage(image string) {
	cc.mu.Lock()
	cc.lazyInit()
	// Record the retirement before releasing mu: a concurrent poolFor that
	// creates a pool after this point seeds retiredImages from here, so it can
	// never pool the retired image even though it was absent from the snapshot
	// below. Recording and snapshotting in one critical section means every
	// pool is covered by the snapshot (existed already) or the seed (created
	// later) — there is no interleaving that misses both.
	cc.retiredImages[image] = true
	pools := make([]*functionPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()
	for _, p := range pools {
		p.invalidateImage(image)
	}
}

// invalidateImage retires image's containers in this pool. Idle matching
// containers are removed and discarded immediately; busy matching containers
// are marked retired so release discards them. Waiters are notified because
// discarding idle containers frees capacity.
func (p *functionPool) invalidateImage(image string) {
	p.mu.Lock()
	// Remember the retirement so any start already in flight (or any later
	// stale acquire for this image) produces a throwaway container that is
	// discarded on release rather than pooled.
	p.retiredImages[image] = true
	var discard []*pooledContainer
	kept := p.idle[:0]
	for _, pc := range p.idle {
		if pc.image == image && !pc.retired {
			pc.retired = true
			pc.retireReason = "image_changed"
			discard = append(discard, pc)
			continue
		}
		kept = append(kept, pc)
	}
	p.idle = kept
	for pc := range p.busy {
		if pc.image == image && !pc.retired {
			pc.retired = true
			pc.retireReason = "image_changed"
		}
	}
	if len(discard) > 0 {
		p.signalLocked()
	}
	p.mu.Unlock()

	for _, pc := range discard {
		if !pc.c.dead() {
			pc.c.discard("image_changed")
		}
	}
}

// close discards EVERY pooled container with reason "shutdown", wakes all
// waiters, and prevents further acquires. Idle containers are discarded
// immediately; busy (active) containers are marked retired AND discarded, so a
// later release is a no-op (discard is idempotent) and no container survives
// shutdown even if a lease is never returned. It is the graceful-shutdown hook
// the worker's defer Manager.Close() flows into.
func (cc *containerCache) close() {
	cc.mu.Lock()
	cc.closed = true
	cc.lazyInit()
	pools := make([]*functionPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()
	for _, p := range pools {
		p.close()
	}
}

// close closes the pool and tears down every container it owns.
func (p *functionPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.notify)

	all := make([]*pooledContainer, 0, len(p.idle)+len(p.busy)+len(p.transient))
	all = append(all, p.idle...)
	p.idle = nil
	for pc := range p.busy {
		pc.retired = true
		all = append(all, pc)
	}
	for pc := range p.transient {
		pc.retired = true
		all = append(all, pc)
	}
	p.mu.Unlock()

	for _, pc := range all {
		if !pc.c.dead() {
			pc.c.discard("shutdown")
		}
	}
}

// signalLocked wakes every waiter. It must be called with p.mu held. A pool
// that is closed has nobody left to wake (its notify was closed by close and
// never replaced), so signalling is a no-op then.
func (p *functionPool) signalLocked() {
	if p.closed {
		return
	}
	close(p.notify)
	p.notify = make(chan struct{})
}

// release returns a leased container to the pool. It is idempotent (a lease is
// released exactly once). A container that is retired, dead, or owned by a
// closed pool is discarded rather than returned to idle, so a stale or
// unhealthy container can never be leased again. Waiters are always notified:
// a release either frees capacity or makes an idle container available.
func (p *functionPool) release(pc *pooledContainer) {
	p.mu.Lock()
	if _, transient := p.transient[pc]; transient {
		delete(p.transient, pc)
		// Wake a stale-request waiter now that a transient slot has freed.
		p.signalLocked()
		p.mu.Unlock()
		if !pc.c.dead() {
			pc.c.discard("image_changed")
		}
		return
	}
	delete(p.busy, pc)
	drop := false
	reason := ""
	switch {
	case p.closed:
		drop, reason = true, "shutdown"
	case pc.retired:
		drop = true
		reason = pc.retireReason
		if reason == "" {
			reason = "image_changed"
		}
	case pc.c.dead():
		// Already discarded by its own path (timeout/process exit/protocol
		// error): drop it, nothing to tear down.
		drop = true
	}
	if !drop {
		p.idle = append(p.idle, pc)
	}
	p.signalLocked()
	p.mu.Unlock()

	if reason != "" && !pc.c.dead() {
		pc.c.discard(reason)
	}
}

// containerLease is one invocation's hold on a pooled container. release must
// be called exactly once; it is idempotent.
type containerLease struct {
	pool *functionPool
	pc   *pooledContainer
	once sync.Once
}

// invoke runs one invocation on the leased container. A panicking Invoke poisons
// the container before the panic is re-raised, so the deferred release discards
// it instead of returning a possibly-corrupt protocol state to the pool.
func (l *containerLease) invoke(ctx context.Context, handler string, eventJSON []byte, env map[string]string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			l.poison("protocol_error")
			panic(r)
		}
	}()
	return l.pc.c.Invoke(ctx, handler, eventJSON, env)
}

// poison marks the leased container retired so release discards it. It is used
// on a panicking invocation.
func (l *containerLease) poison(reason string) {
	l.pool.mu.Lock()
	l.pc.retired = true
	l.pc.retireReason = reason
	l.pool.mu.Unlock()
}

// release returns the container to its pool exactly once.
func (l *containerLease) release() {
	l.once.Do(func() {
		l.pool.release(l.pc)
	})
}

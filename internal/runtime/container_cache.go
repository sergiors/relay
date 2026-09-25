package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"relay/internal/metrics"
)

// reusableContainer is the seam the per-function container pool programs: the
// production implementation is *executionContainer; tests inject fake
// containers to exercise the pool's get-or-create, leasing, capacity,
// invalidation, eviction, and removal logic without a Docker daemon.
type reusableContainer interface {
	// Invoke runs one handler invocation against the container.
	Invoke(ctx context.Context, handler string, eventJSON []byte, env map[string]string) error
	// discard tears the container down (idempotent). reason is one of the
	// documented discard reasons.
	discard(reason string) bool
	// discardReason returns the reason recorded by the container's own teardown
	// path (timeout/process_exit/protocol_error), or "" when the container did
	// not tear itself down. The pool uses it so a self-initiated death is
	// attributed correctly rather than to the pool's fallback reason.
	discardReason() string
	// dead reports whether the container has been discarded.
	dead() bool
}

// Discard reasons. They are labels on the discard path (logs, metrics, tests),
// not a control mechanism: any of them means the container must never be leased
// again. reasonProtocolError (and timeout/process_exit) are recorded by the
// container itself on self-initiated teardown; the others are pool-initiated.
const (
	reasonImageChanged      = "image_changed"
	reasonShutdown          = "shutdown"
	reasonFunctionRemove    = "function_removed"
	reasonIdleTimeout       = "idle_timeout"
	reasonConcurrencyShrink = "concurrency_shrink"
	reasonTimeout           = "timeout"
	reasonProcessExit       = "process_exit"
	reasonProtocolError     = "protocol_error"
)

// errPoolClosed is returned when an acquire is attempted on a cache/pool that
// has been closed (graceful shutdown) or whose function has been removed. It is
// not a container failure: the stream layer leaves the invocation pending and a
// live worker replays it.
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
// pool mutex is never held across start() or Invoke. When a cache-level mutex
// must be combined with a pool mutex the order is always cache.mu -> pool.mu;
// no path takes pool.mu and then cache.mu.
type containerCache struct {
	// mu guards pools, retiredImages, removedFunctions, and closed.
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
	// Like the per-pool set, retirements persist for the process UNLESS a
	// function is successfully re-activated for the same image
	// (activateFunction deletes it), and are bounded by the number of distinct
	// image versions retired in a worker's lifetime. Guarded by mu.
	retiredImages map[string]bool
	// removedFunctions holds function names whose removal has been requested and
	// whose pool may still be draining (busy containers completing) or may
	// already have been deleted once empty. While set, poolFor refuses to create
	// a new pool for the name, so a stale acquire cannot recreate warm state for
	// a removed function. Manager.Prepare clears the mark (activateFunction) only
	// after a successful prepare, so a removed-then-recreated function warms
	// again. Guarded by mu.
	removedFunctions map[string]bool
	// capacity records the last effective per-function concurrency published by
	// a successful Prepare (see setFunctionConcurrency). poolFor uses it when it
	// CREATES a pool, so a stale in-flight acquire that races a reconcile cannot
	// seed a fresh pool with an out-of-date Prepared.Concurrency — the pool is
	// always created at the current effective bound. It is deleted with the
	// function on removal. Guarded by mu.
	capacity map[string]int
	closed   bool

	// idleTimeout is how long a healthy idle pooled container may stay before
	// the maintenance sweep evicts it (see evictIdle). Set once at construction;
	// read without the lock.
	idleTimeout time.Duration
	// now is the injectable clock seam used to stamp idleSince and to decide
	// eviction with a deterministic test clock. A nil clock means time.Now.
	now func() time.Time
	// metrics is the optional observability registry the pool publishes its
	// authoritative state to (see container_metrics.go). A nil registry disables
	// all pool metric recording; every call is a no-op. Set once at construction
	// and read without the lock.
	metrics *metrics.Registry
}

// generation is one image version's set inside a function pool. A version is
// the image reference. A pool has exactly one active generation (the version
// new acquires serve) plus zero or more draining generations: superseded or
// invalidated versions whose busy containers are still completing. Idle
// containers of a non-active generation are never kept — they are discarded the
// moment the generation is superseded — and a draining generation is dropped as
// soon as its last busy container releases. Grouping by generation makes "no
// new acquires of an old version" structural: acquire only ever leases from or
// appends to p.active.
type generation struct {
	image string
	idle  []*pooledContainer
	busy  map[*pooledContainer]struct{}
}

func newGeneration(image string) *generation {
	return &generation{image: image, busy: map[*pooledContainer]struct{}{}}
}

// matches reports whether this generation serves the requested image.
func (g *generation) matches(image string) bool {
	return g.image == image
}

// functionPool is one function's bounded warm container pool.
//
// Capacity accounting: usedLocked() (active idle+busy, draining busy, and
// creating reservations) must stay <= max. creating is the number of capacity
// reservations in flight (a lazy start that has not yet registered its
// container); it is rolled back if start fails.
type functionPool struct {
	// name and cache back the pool's ability to delete itself from the cache
	// once a removal has drained it empty.
	name  string
	cache *containerCache

	mu  sync.Mutex
	max int

	// active is the generation new acquires serve. It is nil until the first
	// acquire fixes the pool's image. Guarded by mu.
	active *generation
	// draining holds superseded/invalidated generations that still have busy
	// containers. An entry is removed once it has no idle and no busy
	// containers. Guarded by mu.
	draining []*generation

	// transient holds leased THROWAWAY containers serving a stale request for
	// an already-retired image. They are never pooled and deliberately do NOT
	// count against the regular capacity (a stale request must not be blocked
	// by, nor evict, the current version's containers). They are bounded among
	// themselves by max; transientCreating reserves those starts so concurrent
	// stale acquires cannot race past the bound. close/removal tear them down;
	// release discards them.
	transient         map[*pooledContainer]struct{}
	transientCreating int

	// retiredImages holds image references whose containers must never be
	// pooled again (image retirement/invalidation or a superseded active
	// image). A container created for a retired image is marked retired at
	// creation: it serves its invocation at-least-once and is discarded on
	// release, so no later acquire can reuse it ("no new acquires old image").
	//
	// Retirements are permanent for the pool's lifetime UNLESS the image is
	// re-activated: a content-addressed image whose source is reverted to a
	// previously-retired fingerprint is treated as retired, so its invocations
	// run on throwaway containers (correct, just not warm), until a successful
	// Prepare for that exact image re-activates it (activateFunction clears the
	// entry), at which point it warms again. The set is bounded by the number of
	// distinct function versions retired in a worker's lifetime (and is
	// discarded with the pool on function removal). Guarded by mu.
	retiredImages map[string]bool

	// creating is the number of in-progress lazy starts holding a capacity
	// reservation. Guarded by mu.
	creating int

	// removing is set once by remove: acquire then fails immediately, idle
	// containers are discarded, and busy ones are retired so release discards
	// them. The pool is deleted from the cache once it is empty.
	removing bool
	// closed is set once by close (graceful shutdown); acquire fails
	// immediately and every current container is discarded.
	closed bool

	// notify is closed (and replaced) on every state change that may unblock a
	// waiter: a release, an idle container becoming available, invalidation or
	// transition freeing capacity, eviction, removal, or close. Waiters select
	// on a snapshot of it plus ctx, so blocked acquires are woken by
	// notification, never by polling and never by a goroutine per waiter.
	notify chan struct{}
}

// pooledContainer is one reusable container in a function pool, with its image
// version, owning generation, and retirement state.
// retired/retireReason/idleSince are guarded by the owning functionPool.mu;
// membership in a generation's idle/busy set (or the pool's transient set) is
// the lease state. gen is nil for transient containers.
type pooledContainer struct {
	c     reusableContainer
	image string
	gen   *generation
	// retired marks a container that must not be leased again and must be
	// discarded as soon as it is not busy (busy ones are discarded on release).
	retired      bool
	retireReason string
	// idleSince is when the container was last returned to a generation's idle
	// list; the maintenance sweep evicts a healthy idle container once
	// now-idleSince reaches the configured timeout.
	idleSince time.Time
	// discardOnce guards the per-container discard metric: several teardown
	// paths may race to discard the same wrapper (lease release, eviction,
	// removal, close), but the discard counter increments exactly once.
	discardOnce sync.Once
}

// matchesVersion reports whether the wrapper's container was created for the
// requested image.
func (pc *pooledContainer) matchesVersion(image string) bool {
	return pc.image == image
}

func newContainerCache() *containerCache {
	return &containerCache{
		pools:            map[string]*functionPool{},
		retiredImages:    map[string]bool{},
		removedFunctions: map[string]bool{},
		capacity:         map[string]int{},
	}
}

func newFunctionPool(name string, max int, cache *containerCache) *functionPool {
	if max < 1 {
		max = 1
	}
	return &functionPool{
		name:          name,
		cache:         cache,
		max:           max,
		transient:     map[*pooledContainer]struct{}{},
		retiredImages: map[string]bool{},
		notify:        make(chan struct{}),
	}
}

// clock returns the cache's current time from the injected seam, or the wall
// clock when unset. It is called with the owning pool's lock held, so the seam
// must be set before the cache is used (tests do).
func (p *functionPool) clock() time.Time {
	if p.cache != nil && p.cache.now != nil {
		return p.cache.now()
	}
	return time.Now()
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
	if cc.removedFunctions == nil {
		cc.removedFunctions = map[string]bool{}
	}
	if cc.capacity == nil {
		cc.capacity = map[string]int{}
	}
}

// poolFor returns (creating) the pool for fnName. max is used only when the pool
// is CREATED: an existing pool's bound is authoritative and is updated in place
// by setFunctionConcurrency when Prepare reconciles the function's resolved
// concurrency, never by a later acquire (a stale Prepared must not resize a live
// pool backwards).
//
// A pool requested after the cache is closed is returned closed so acquire
// fails immediately. A function whose removal has been requested (and whose
// pool may already have been deleted) gets a detached, closed pool so a stale
// acquire can neither recreate warm state nor block: poolFor never inserts a
// pool for a removed function until activateFunction clears the mark.
func (cc *containerCache) poolFor(fnName string, max int) *functionPool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	if p, ok := cc.pools[fnName]; ok {
		return p
	}
	// Prefer the last effective capacity published by a successful Prepare over
	// the caller's max: a stale acquire racing a reconcile must not seed a fresh
	// pool with an out-of-date Prepared.Concurrency. The caller's max is the
	// fallback for direct callers (integration tests) that never go through
	// Prepare.
	if effective, ok := cc.capacity[fnName]; ok {
		max = effective
	}
	if cc.removedFunctions[fnName] {
		p := newFunctionPool(fnName, max, cc)
		p.removing = true
		p.closed = true
		close(p.notify)
		return p
	}
	p := newFunctionPool(fnName, max, cc)
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
	// Publish the pool's bound at registration, before it is shared through the
	// map. Its bound is later updated in place by setFunctionConcurrency when
	// Prepare reconciles a changed concurrency.
	p.publishPoolCapacity()
	return p
}

// setFunctionConcurrency updates fnName's live warm pool bound to max. It is how
// a successful function Prepare propagates a reconciled `concurrency` to an
// ALREADY-CREATED pool without a restart: the pool bound is not first-wins. A
// pool that does not exist yet needs no update (the next acquire creates it with
// the current max, supplied from the current Prepared); a removed or closed pool
// is left alone. The new bound publishes the capacity gauge and wakes blocked
// acquires, and on a DECREASE it immediately retires only EXCESS IDLE containers
// (see setMax and release for the shrink invariant).
func (cc *containerCache) setFunctionConcurrency(fnName string, max int) {
	if max < 1 {
		max = 1
	}
	cc.mu.Lock()
	cc.lazyInit()
	// Record the effective bound under cc.mu so a pool created later (or a stale
	// acquire racing this reconcile) is seeded with it by poolFor, never with a
	// stale Prepared.Concurrency. Recording and pool lookup share one critical
	// section so a concurrent creation is covered by the record or by the setMax
	// below, never missed.
	cc.capacity[fnName] = max
	p := cc.pools[fnName]
	cc.mu.Unlock()
	if p == nil {
		return
	}
	for _, pc := range p.setMax(max) {
		p.discardContainer(pc, reasonConcurrencyShrink)
	}
}

// setMax applies a new bound to this pool and returns the excess IDLE containers
// to discard (the caller tears them down outside the lock). It is a no-op for an
// equal bound or a closed/removing pool. It is the single pool-level bound
// operation, so acquisition, PoolSnapshot/socket/CLI, and the capacity gauge all
// read the same p.max. It must not be called with p.mu held.
func (p *functionPool) setMax(max int) []*pooledContainer {
	p.mu.Lock()
	if p.closed || p.removing || max == p.max {
		p.mu.Unlock()
		return nil
	}
	p.max = max
	p.publishPoolCapacityLocked()
	// Shrink invariant: a decrease retires only EXCESS IDLE containers. Busy
	// containers are never killed; a container returned to an over-bound pool is
	// retired on release instead (see release), so the pool converges as leases
	// drain. An increase is lazy: nothing is started here, only admission opens.
	discard := p.takeExcessIdleLocked(p.usedLocked() - p.max)
	if len(discard) > 0 {
		// The idle gauges must drop with the containers the shrink retires.
		p.publishPoolGaugesLocked()
	}
	p.signalLocked()
	p.mu.Unlock()
	return discard
}

// takeExcessIdleLocked removes up to n idle containers from the active then
// draining generations, marking each retired with the shrink reason, so a
// decreased bound sheds only idle capacity. It must be called with p.mu held; a
// non-positive n removes nothing.
func (p *functionPool) takeExcessIdleLocked(n int) []*pooledContainer {
	if n <= 0 {
		return nil
	}
	var discard []*pooledContainer
	take := func(g *generation) {
		for n > 0 && len(g.idle) > 0 {
			pc := g.idle[len(g.idle)-1]
			g.idle = g.idle[:len(g.idle)-1]
			pc.retired = true
			pc.retireReason = reasonConcurrencyShrink
			discard = append(discard, pc)
			n--
		}
	}
	if p.active != nil {
		take(p.active)
	}
	for _, g := range p.draining {
		if n <= 0 {
			break
		}
		take(g)
	}
	p.pruneDrainingLocked()
	return discard
}

// execute runs one invocation through a leased container for fnName's image
// version. start creates a fresh container when the pool has no idle one and
// capacity remains (lazily); it returns the caller's error verbatim on failure.
// A container that the invocation poisoned (timeout/process exit/protocol
// error/panic) is dropped on release so a later call starts fresh; a plain
// handler error keeps the container.
func (cc *containerCache) execute(
	ctx context.Context,
	fnName, image string,
	max int,
	start func() (reusableContainer, error),
	handler string,
	eventJSON []byte,
	env map[string]string,
) error {
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

// acquire leases one container for fnName's image version, blocking
// (context-aware) when the pool is at capacity until a
// release/close/eviction/transition/removal notifies it. It:
//
//   - refuses to serve a REMOVED or CLOSED pool, returning errPoolClosed;
//   - refuses to POOL containers for an image that has been invalidated: a
//     stale request for a RETIRED image is served at-least-once on a throwaway
//     (transient) container that is never pooled and is discarded on release
//     ("no new acquires old image"), without reaping, rewinding, or waiting on
//     the current (newer) version. The runner's per-function semaphore keeps
//     the number of such in-flight stale requests bounded in production;
//   - on a request for a NEW version (a different image), supersedes the active
//     generation: idle containers of the old version are discarded immediately,
//     its busy ones are moved to a draining generation and discarded on release,
//     and the new version becomes the sole active generation. No further acquire
//     can lease an old-version container, and no new old-version generation is
//     created;
//   - leases an idle matching container from the active generation, or lazily
//     starts a new one while capacity remains (reserving the slot before start
//     so concurrent waiters account for it, and rolling the reservation back on
//     start failure).
//
// invalidateImage (the runner's image-retirement path) is what retires a
// known-dead image's busy containers; the forward transition above covers
// direct callers that switch Prepared without an explicit invalidation.
func (cc *containerCache) acquire(
	ctx context.Context,
	fnName, image string,
	max int,
	start func() (reusableContainer, error),
) (*containerLease, error) {
	// acquiredAt bounds the SUCCESSFUL acquire duration recorded below: it
	// covers the whole call including any capacity wait, but is observed only
	// when a lease is actually returned. waitRecorded makes the waits counter
	// count one logical acquire that had to block, not each retry iteration.
	// Both records happen under the owning pool's lock (see container_metrics.go)
	// so a concurrent removal cannot delete the series underneath a late write.
	acquiredAt := time.Now()
	waitRecorded := false
	recordWait := func(p *functionPool) {
		if !waitRecorded {
			waitRecorded = true
			p.recordWaitLocked()
		}
	}

	// A panic escaping start() must not leak a capacity reservation, or the
	// pool would be permanently at capacity. reserved/transientReserved track
	// which reservation is currently held so the unwind path can roll it back
	// (and delete a pool whose removal landed while the start was in flight).
	var reserved *functionPool
	var transientReserved *functionPool
	defer func() {
		if r := recover(); r != nil {
			if reserved != nil {
				reserved.mu.Lock()
				reserved.rollbackStartLocked(false)
			}
			if transientReserved != nil {
				transientReserved.mu.Lock()
				transientReserved.rollbackStartLocked(true)
			}
			panic(r)
		}
	}()

	for {
		p := cc.poolFor(fnName, max)

		p.mu.Lock()
		if p.closed || p.removing {
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
				recordWait(p)
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
			p.publishPoolGaugesLocked()
			transientReserved = p
			p.mu.Unlock()
			c, err := start()
			p.mu.Lock()
			transientReserved = nil
			if err != nil {
				p.rollbackStartLocked(true)
				return nil, err
			}
			p.transientCreating--
			if p.closed || p.removing {
				reason := poolDiscardReason(p.removing)
				removing := p.removing
				p.publishPoolGaugesLocked()
				p.signalLocked()
				p.mu.Unlock()
				p.discardContainer(&pooledContainer{c: c, image: image}, reason)
				if removing {
					// The removal may have run while this transient was starting,
					// when the pool was not yet empty; it can be deleted now.
					p.cache.maybeDeletePool(p.name, p)
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image, retired: true, retireReason: reasonImageChanged}
			p.transient[pc] = struct{}{}
			p.publishPoolGaugesLocked()
			p.recordAcquireLocked(metrics.RuntimeOutcomeCold, time.Since(acquiredAt))
			p.mu.Unlock()
			return &containerLease{pool: p, pc: pc}, nil
		}

		// Forward version transition: a request for a new image supersedes the
		// active generation. The old version's idle containers are discarded now
		// and its busy ones on release, and it can never be pooled again. This
		// also covers direct callers that use a new Prepared without a runner
		// InvalidateImage call.
		var transitioned []*pooledContainer
		switch {
		case p.active == nil:
			p.active = newGeneration(image)
		case !p.active.matches(image):
			old := p.active
			// A retired IMAGE must not be repooled by any later acquire.
			p.retiredImages[old.image] = true
			for _, pc := range old.idle {
				if !pc.retired {
					pc.retired = true
					pc.retireReason = reasonImageChanged
				}
			}
			transitioned = append(transitioned, old.idle...)
			old.idle = nil
			for pc := range old.busy {
				if !pc.retired {
					pc.retired = true
					pc.retireReason = reasonImageChanged
				}
			}
			if len(old.busy) > 0 {
				p.draining = append(p.draining, old)
			}
			p.active = newGeneration(image)
		}

		// Reap idle containers that may not be leased, before the capacity check
		// so a stale or unhealthy idle container does not consume a slot.
		var reaped []*pooledContainer
		kept := p.active.idle[:0]
		for _, pc := range p.active.idle {
			if pc.c.dead() {
				// The container tore itself down while idle (its own monitor
				// path): drop the slot AND record the discard once, using the
				// reason it recorded. discardReaped skips the redundant teardown.
				reaped = append(reaped, pc)
				continue
			}
			if pc.retired || !pc.matchesVersion(image) {
				pc.retired = true
				if pc.retireReason == "" {
					pc.retireReason = reasonImageChanged
				}
				reaped = append(reaped, pc)
				continue
			}
			kept = append(kept, pc)
		}
		p.active.idle = kept
		reaped = append(reaped, transitioned...)
		if len(transitioned) > 0 {
			p.signalLocked()
		}
		// The transition/reap above may have dropped idle containers (and a
		// forward transition moved busy ones to a draining generation); publish
		// the new counts before any branch that can return or block.
		if len(reaped) > 0 {
			p.publishPoolGaugesLocked()
		}

		// Prefer an idle matching container.
		for i := len(p.active.idle) - 1; i >= 0; i-- {
			pc := p.active.idle[i]
			if !pc.matchesVersion(image) || pc.retired {
				continue
			}
			p.active.idle = append(p.active.idle[:i], p.active.idle[i+1:]...)
			p.active.busy[pc] = struct{}{}
			p.publishPoolGaugesLocked()
			p.recordAcquireLocked(metrics.RuntimeOutcomeWarm, time.Since(acquiredAt))
			p.mu.Unlock()
			p.discardReaped(reaped)
			return &containerLease{pool: p, pc: pc}, nil
		}

		// Lazily create a fresh container while total capacity remains.
		if p.usedLocked() < p.max {
			p.creating++
			p.publishPoolGaugesLocked()
			reserved = p
			p.mu.Unlock()
			p.discardReaped(reaped)

			c, err := start()
			p.mu.Lock()
			reserved = nil
			if err != nil {
				// Roll the reservation back and wake a waiter so the freed
				// slot can be used by a fresh start. A removal that raced this
				// start may have left the pool with nothing else, so delete it.
				p.rollbackStartLocked(false)
				return nil, err
			}
			p.creating--
			if p.closed || p.removing {
				// Shutdown/removal won the race: never register or lease the
				// fresh container; discard it through the appropriate path.
				reason := poolDiscardReason(p.removing)
				removing := p.removing
				p.publishPoolGaugesLocked()
				p.signalLocked()
				p.mu.Unlock()
				p.discardContainer(&pooledContainer{c: c, image: image}, reason)
				if removing {
					// The removal may have run while this start was in flight,
					// when the pool still held the reservation; it can be
					// deleted now that this last slot is gone.
					p.cache.maybeDeletePool(p.name, p)
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image}
			// Invalidation or transition landed while this start was in flight:
			// serve the invocation, but never pool the container. The active
			// generation serves the requested version only when no transition
			// occurred; if it was superseded, this start still loses its slot.
			if p.active == nil || !p.active.matches(image) || p.retiredImages[image] {
				pc.retired = true
				pc.retireReason = reasonImageChanged
				p.transient[pc] = struct{}{}
			} else {
				pc.gen = p.active
				p.active.busy[pc] = struct{}{}
			}
			p.publishPoolGaugesLocked()
			p.recordAcquireLocked(metrics.RuntimeOutcomeCold, time.Since(acquiredAt))
			p.mu.Unlock()
			return &containerLease{pool: p, pc: pc}, nil
		}

		// At capacity: wait for a release/eviction/transition/removal/close
		// notification. The pool mutex is dropped while blocked so those paths
		// can make progress.
		recordWait(p)
		notify := p.notify
		p.mu.Unlock()
		p.discardReaped(reaped)

		select {
		case <-notify:
			// State changed: retry under a fresh lock.
		case <-ctx.Done():
			return nil, fmt.Errorf("docker run: %w", ctx.Err())
		}
	}
}

// usedLocked returns the number of regular capacity slots currently consumed:
// the active generation's containers (idle and busy), every draining
// generation's busy containers (they are real containers still completing, so
// they count), and in-flight start reservations. Transients are deliberately
// excluded. It must be called with p.mu held.
func (p *functionPool) usedLocked() int {
	n := p.creating
	if p.active != nil {
		n += len(p.active.idle) + len(p.active.busy)
	}
	for _, g := range p.draining {
		n += len(g.busy)
	}
	return n
}

// rollbackStartLocked releases an in-flight start reservation after start()
// returned an error (or panicked): it decrements the reservation (regular or
// transient), wakes waiters so the freed slot can be reused, and deletes an
// empty removing pool from the cache. A removal that raced the start may have
// run while this reservation was the only thing keeping the pool non-empty, so
// without the delete the cache would keep serving errPoolClosed from a stale
// empty pool. It must be called with p.mu held; it unlocks p.mu before
// returning, so the caller must not touch p afterwards. maybeDeletePool is
// called without p.mu (it takes cache.mu -> pool.mu, and this path must not
// invert that order).
func (p *functionPool) rollbackStartLocked(transient bool) {
	if transient {
		p.transientCreating--
	} else {
		p.creating--
	}
	removing := p.removing
	p.publishPoolGaugesLocked()
	p.signalLocked()
	p.mu.Unlock()
	if removing {
		p.cache.maybeDeletePool(p.name, p)
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
// containers (active or draining) are removed and discarded immediately; busy
// matching containers are marked retired so release discards them. Waiters are
// notified because discarding idle containers frees capacity.
func (p *functionPool) invalidateImage(image string) {
	p.mu.Lock()
	// Remember the retirement so any start already in flight (or any later
	// stale acquire for this image) produces a throwaway container that is
	// discarded on release rather than pooled.
	p.retiredImages[image] = true
	var discard []*pooledContainer
	if p.active != nil && p.active.image == image {
		discard = append(discard, p.active.idle...)
		p.active.idle = nil
		for pc := range p.active.busy {
			if !pc.retired {
				pc.retired = true
				pc.retireReason = reasonImageChanged
			}
		}
	}
	for _, g := range p.draining {
		if g.image != image {
			continue
		}
		discard = append(discard, g.idle...)
		g.idle = nil
		for pc := range g.busy {
			if !pc.retired {
				pc.retired = true
				pc.retireReason = reasonImageChanged
			}
		}
	}
	p.pruneDrainingLocked()
	if len(discard) > 0 {
		p.publishPoolGaugesLocked()
		p.signalLocked()
	}
	p.mu.Unlock()

	for _, pc := range discard {
		p.discardContainer(pc, reasonImageChanged)
	}
}

// removeFunction requests the removal of fnName's warm container state. The
// request is linearized under the cache lock: it sets the removed mark and, in
// the SAME critical section (cache.mu -> pool.mu), marks the pool removing, so
// a concurrent activateFunction can never be undone by this removal's later
// teardown (activation observes removing and detaches the pool), and no
// concurrent acquire can create warm state after this point (poolFor refuses
// while the mark is set). Idle containers are then discarded outside the locks,
// busy ones were retired so their release discards them, and once the pool is
// empty its state is deleted from the cache.
//
// The function's runtime-pool metric series are deleted in the SAME critical
// section that installs the removal tombstone (p.removing), under BOTH cc.mu and
// p.mu. This is what makes metric cleanup atomic with the lifecycle transition:
// every pool metric writer checks the tombstone under p.mu, so a writer runs
// entirely before the delete (its series are then deleted) or entirely after
// (it observes the tombstone and writes nothing) — it can never recreate a
// series after the delete. Holding cc.mu additionally excludes a concurrent
// reactivation: activateFunction needs cc.mu, so it cannot clear the removal
// mark and let a fresh pool publish between the tombstone and the delete (which
// would delete the reactivated pool's brand-new series). A genuinely reactivated
// function gets a fresh pool after this critical section and publishes normally.
func (cc *containerCache) removeFunction(fnName string) {
	cc.mu.Lock()
	cc.lazyInit()
	cc.removedFunctions[fnName] = true
	delete(cc.capacity, fnName)
	p := cc.pools[fnName]
	var discard []*pooledContainer
	if p != nil {
		p.mu.Lock()
		discard = p.beginRemoveLocked()
		cc.deleteRuntimePoolMetrics(fnName)
		p.mu.Unlock()
	} else {
		// No live pool, but a late writer from an already-detached pool could
		// have left lingering series. The removed mark is set under cc.mu (so no
		// new pool can be created) and any detached pool is tombstoned, so
		// deleting here is safe and completes the removal's cleanup.
		cc.deleteRuntimePoolMetrics(fnName)
	}
	cc.mu.Unlock()

	if p == nil {
		return
	}
	p.teardownDiscards(discard)
	p.cache.maybeDeletePool(p.name, p)
}

// deleteRuntimePoolMetrics deletes fnName's runtime-pool series when a registry
// is attached. It is called from removeFunction's critical section (cc.mu held,
// and p.mu held when a pool exists) so the delete is atomic with the removal
// tombstone; it must not be hoisted outside that section.
func (cc *containerCache) deleteRuntimePoolMetrics(fnName string) {
	if cc.metrics != nil {
		cc.metrics.DeleteRuntimePool(fnName)
	}
}

// activateFunction clears a previous removal mark for fnName so a
// removed-then-recreated function warms again, and un-retires exactly the
// function's own image so a same-image recreation is not permanently treated as
// retired. Clearing the mark, un-retiring the image, and detaching a
// removal-draining pool all happen in one critical section, so:
//   - a concurrent acquire after this point sees the function active;
//   - a removal that already began cannot leave a stale removing pool in the
//     map to permanently serve errPoolClosed (the pool is detached; the next
//     acquire builds a fresh one with the current capacity);
//   - only fnName's own image is touched. A foreign image reference is never
//     cleared, so a stale request for another function's image cannot be
//     un-retired.
//
// image is the image Prepare resolved for this function ("" means "no image to
// un-retire", e.g. a caller that only wants the removal mark cleared).
func (cc *containerCache) activateFunction(fnName, image string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	delete(cc.removedFunctions, fnName)
	if image == "" {
		// No image to un-retire: only the removal mark is cleared.
	} else if name, ok := functionNameFromImage(image); ok && name == fnName {
		// The image is fnName's own: its same-image recreation must warm.
		// Retirements of OTHER versions of this function stay in place so a
		// stale old-version request can never supersede the active version.
		delete(cc.retiredImages, image)
	} else {
		// A foreign or unparseable reference is never un-retired, so a caller
		// cannot clear another function's (or an arbitrary) retirement.
		image = ""
	}
	if p, ok := cc.pools[fnName]; ok {
		if p.activate(image) {
			// Detach the draining pool: a later acquire must build a fresh pool
			// (with the function's current capacity) rather than be served the
			// removed one. Its own maybeDeletePool becomes a no-op because the
			// map no longer points at it.
			delete(cc.pools, fnName)
		}
	}
}

// activate clears the retirement of image on this pool if the pool is live, and
// reports whether the pool must be detached because a removal is (or was)
// draining it. It takes the pool lock, so the caller must not hold it; the
// caller holds cc.mu (the allowed cache -> pool order).
func (p *functionPool) activate(image string) (detach bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.removing {
		return true
	}
	if image != "" {
		delete(p.retiredImages, image)
	}
	return false
}

// beginRemoveLocked marks the pool removed and collects every container that
// must be torn down immediately: active and draining idle containers, while
// active and draining busy containers are marked retired (discarded on release)
// and transient throwaways are marked retired. It is idempotent and returns the
// discards to perform outside the pool lock. It must be called with p.mu held
// (and, for the removeFunction path, with cc.mu held so the mark is linearized
// against activation).
func (p *functionPool) beginRemoveLocked() []*pooledContainer {
	if p.removing {
		return nil
	}
	p.removing = true
	p.signalLocked()

	var discard []*pooledContainer
	retire := func(gens []*generation) {
		for _, g := range gens {
			discard = append(discard, g.idle...)
			g.idle = nil
			for pc := range g.busy {
				if !pc.retired {
					pc.retired = true
					pc.retireReason = reasonFunctionRemove
				}
			}
		}
	}
	if p.active != nil {
		retire([]*generation{p.active})
	}
	retire(p.draining)
	for pc := range p.transient {
		if !pc.retired {
			pc.retired = true
			pc.retireReason = reasonFunctionRemove
		}
	}
	p.pruneDrainingLocked()
	// No gauge publish here: p.removing is now set, so the pool is a metric
	// tombstone (publishPoolGaugesLocked is a no-op). The caller
	// (containerCache.removeFunction) deletes the function's runtime-pool series
	// in this same critical section, which is the removal-visible transition.
	return discard
}

// teardownDiscards discards the containers beginRemoveLocked removed from the
// pool's ownership. It runs outside the pool lock; dead containers are still
// recorded (their own path already tore them down) but not discarded again.
func (p *functionPool) teardownDiscards(discard []*pooledContainer) {
	for _, pc := range discard {
		p.discardContainer(pc, reasonFunctionRemove)
	}
}

// maybeDeletePool deletes p from the cache map when it has been removed and is
// now empty, so a removed function's state does not linger and a later
// reactivation starts from a clean pool. It is a no-op for a pool that is not
// removing, has work in flight, or has already been replaced in the map. Lock
// order is cache.mu -> pool.mu (the pool lock must not be held by the caller).
func (cc *containerCache) maybeDeletePool(fnName string, p *functionPool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	if cc.pools[fnName] != p {
		return
	}
	p.mu.Lock()
	empty := p.removing && p.isEmptyLocked()
	p.mu.Unlock()
	if empty {
		delete(cc.pools, fnName)
	}
}

// pruneDrainingLocked drops draining generations that hold nothing. It must be
// called with p.mu held.
func (p *functionPool) pruneDrainingLocked() {
	if len(p.draining) == 0 {
		return
	}
	kept := p.draining[:0]
	for _, g := range p.draining {
		if len(g.idle) == 0 && len(g.busy) == 0 {
			continue
		}
		kept = append(kept, g)
	}
	p.draining = kept
}

// isEmptyLocked reports whether the pool holds no containers and no in-flight
// starts. It must be called with p.mu held.
func (p *functionPool) isEmptyLocked() bool {
	if p.creating > 0 || p.transientCreating > 0 || len(p.transient) > 0 {
		return false
	}
	if p.active != nil && (len(p.active.idle) > 0 || len(p.active.busy) > 0) {
		return false
	}
	for _, g := range p.draining {
		if len(g.idle) > 0 || len(g.busy) > 0 {
			return false
		}
	}
	return true
}

// evictIdle runs one maintenance pass over every pool. It always reaps idle
// containers that are already dead (a container tore itself down while idle, so
// its slot must be dropped and its gauges republished), plus retired idle
// containers; when timeout is positive it additionally evicts healthy idle
// containers that have been idle at least the timeout. It is the only
// age-eviction path and is driven by Manager's single ticker (never a ticker or
// goroutine per container). Ownership is removed from the idle list under the
// pool lock and the actual teardown runs outside it; a failed teardown is never
// reinserted (the container has already lost its idle slot), so a cleanup
// failure can only leak the container, never resurrect it.
func (cc *containerCache) evictIdle() {
	now := time.Now()
	if cc.now != nil {
		now = cc.now()
	}
	cc.mu.Lock()
	pools := make([]*functionPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()
	for _, p := range pools {
		p.evictIdle(now, cc.idleTimeout)
	}
}

// evictIdle reaps dead/retired idle containers from this pool and, when timeout
// is positive, evicts healthy idle containers older than timeout. Dead idle
// containers are reaped even when age eviction is disabled (a non-positive
// timeout), so a self-terminated container can never leave a stale idle gauge;
// the pool's authoritative counts are republished whenever anything is dropped.
func (p *functionPool) evictIdle(now time.Time, timeout time.Duration) {
	p.mu.Lock()
	if p.closed || p.removing {
		// A closed pool has already zeroed its gauges; a removing pool is a
		// metric tombstone and must not publish (its series were deleted).
		p.mu.Unlock()
		return
	}
	var evict []*pooledContainer
	evictFrom := func(g *generation) {
		kept := g.idle[:0]
		for _, pc := range g.idle {
			switch {
			case pc.retired:
				// Defensive: a retired idle container should already have been
				// discarded; never leave it to be leased again.
				evict = append(evict, pc)
			case pc.c.dead():
				// Already discarded by its own path: the slot is dropped, but
				// the container's death is still counted as one discard (its own
				// reason, e.g. process_exit, is recorded).
				evict = append(evict, pc)
			case timeout > 0 && now.Sub(pc.idleSince) >= timeout:
				evict = append(evict, pc)
			default:
				kept = append(kept, pc)
			}
		}
		g.idle = kept
	}
	if p.active != nil {
		evictFrom(p.active)
	}
	for _, g := range p.draining {
		evictFrom(g)
	}
	p.pruneDrainingLocked()
	if len(evict) > 0 {
		p.publishPoolGaugesLocked()
		p.signalLocked()
	}
	p.mu.Unlock()

	for _, pc := range evict {
		reason := pc.retireReason
		if reason == "" {
			reason = reasonIdleTimeout
		}
		p.discardContainer(pc, reason)
	}
}

// close discards EVERY pooled container with reason "shutdown", wakes all
// waiters, and prevents further acquires. Idle containers are discarded
// immediately; busy (active or draining) and transient containers are marked
// retired AND discarded, so a later release is a no-op (discard is idempotent)
// and no container survives shutdown even if a lease is never returned. It is
// the graceful-shutdown hook the worker's defer Manager.Close() flows into.
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

	all := make([]*pooledContainer, 0, p.usedLocked()+len(p.transient))
	if p.active != nil {
		all = append(all, p.active.idle...)
		p.active.idle = nil
		for pc := range p.active.busy {
			pc.retired = true
			all = append(all, pc)
		}
	}
	for _, g := range p.draining {
		all = append(all, g.idle...)
		g.idle = nil
		for pc := range g.busy {
			pc.retired = true
			all = append(all, pc)
		}
	}
	for pc := range p.transient {
		pc.retired = true
		all = append(all, pc)
	}
	// publishPoolGaugesLocked observes p.closed == true, so it publishes all
	// three state gauges as zero; signalLocked is a no-op on a closed pool.
	p.publishPoolGaugesLocked()
	p.mu.Unlock()

	for _, pc := range all {
		p.discardContainer(pc, reasonShutdown)
	}
}

// poolDiscardReason picks the teardown reason for an unregistered container
// whose pool was removed mid-start: a removed function is reported as
// function_removed, otherwise the pool is shutting down. It is a pure helper
// for the acquire unwind path.
func poolDiscardReason(removing bool) string {
	if removing {
		return reasonFunctionRemove
	}
	return reasonShutdown
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
// released exactly once). A container that is retired, dead, superseded by a
// later generation, or owned by a closed/removed pool is discarded rather than
// returned to idle, so a stale or unhealthy container can never be leased
// again. Waiters are always notified: a release either frees capacity or makes
// an idle container available. A removed pool that becomes empty here is
// deleted from the cache.
func (p *functionPool) release(pc *pooledContainer) {
	p.mu.Lock()
	if _, transient := p.transient[pc]; transient {
		delete(p.transient, pc)
		reason := pc.retireReason
		if reason == "" {
			reason = reasonImageChanged
		}
		removing := p.removing
		if p.closed {
			reason = reasonShutdown
		} else if removing {
			reason = reasonFunctionRemove
		}
		// Wake a stale-request waiter now that a transient slot has freed.
		p.publishPoolGaugesLocked()
		p.signalLocked()
		p.mu.Unlock()
		p.discardContainer(pc, reason)
		if removing {
			p.cache.maybeDeletePool(p.name, p)
		}
		return
	}

	gen := pc.gen
	if gen != nil {
		delete(gen.busy, pc)
	}
	drop := false
	reason := ""
	switch {
	case p.closed:
		drop, reason = true, reasonShutdown
	case p.removing:
		drop, reason = true, reasonFunctionRemove
	case pc.retired:
		drop = true
		reason = pc.retireReason
		if reason == "" {
			reason = reasonImageChanged
		}
	case pc.c.dead():
		// Already discarded by its own path (timeout/process exit/protocol
		// error): drop it, nothing to tear down.
		drop = true
	case gen != p.active:
		// A draining generation's container finished after being superseded:
		// its image is no longer active, so it is discarded rather than pooled.
		drop = true
		reason = reasonImageChanged
	case p.usedLocked() >= p.max:
		// Shrink invariant: the bound was lowered while this container was busy,
		// so returning it to idle would leave the pool above max. Retire it
		// instead; the pool converges to max as the remaining busy leases drain
		// (the decrease itself only retired excess idle containers and never
		// killed a busy one).
		drop = true
		reason = reasonConcurrencyShrink
	}
	if !drop {
		pc.idleSince = p.clock()
		gen.idle = append(gen.idle, pc)
	}
	p.pruneDrainingLocked()
	removing := p.removing
	p.publishPoolGaugesLocked()
	p.signalLocked()
	p.mu.Unlock()

	// Always funnel through discardContainer: when reason is "" the container
	// already tore itself down (timeout/process_exit/protocol_error) and its own
	// recorded reason is used for the discard metric; otherwise the pool reason
	// is applied.
	if drop {
		p.discardContainer(pc, reason)
	}
	if removing {
		p.cache.maybeDeletePool(p.name, p)
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

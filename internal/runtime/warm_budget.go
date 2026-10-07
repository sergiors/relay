package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/observability/metrics"
)

// ErrWarmBudgetSaturated is returned by Manager.Execute (through the cache's
// create-time reservation) when the worker-global warm-container budget is full,
// no idle container can be evicted, and a fresh container would be required. It
// is a distinct sentinel so the runner can leave the delivery pending
// (backpressure) WITHOUT charging a handler retry or DLQ, in the narrow race
// where admission had optimistically admitted the invocation expecting a
// reusable idle container that a concurrent lease then took. It never blocks a
// claimed attempt.
var ErrWarmBudgetSaturated = errors.New("runtime: warm-container budget saturated")

// WarmAdmitter is the optional runtime capability the runner uses to pre-admit
// an invocation against the worker-global warm-container budget
// (MAX_WARM_CONTAINERS) BEFORE it claims a handler attempt. *runtime.Manager
// implements it. A returned nil permit (no *error) means the caller must not
// manage a slot (an unbounded cache); a non-nil permit must be released via
// Release, and is consumed automatically when Manager.Execute attaches it to a
// newly created container.
type WarmAdmitter interface {
	// AcquireWarmPermit admits one invocation against the global
	// warm-container budget, returning a permit (possibly nil when unbounded) or
	// an error when the budget is saturated and cannot be relieved within ctx.
	AcquireWarmPermit(ctx context.Context) (*WarmPermit, error)
}

// reasonWarmEviction is the discard reason recorded when the global
// warm-container budget forces the eviction of a healthy IDLE execution
// container to make room for a new one. It is a label on the existing
// runtime_container_discards_total counter, not a separate mechanism.
const reasonWarmEviction = "warm_eviction"

// AcquireWarmPermit reserves a slot in the cache's global warm-container budget
// for one invocation, blocking (bounded by ctx) when every warm execution
// container is busy and none can be evicted. It returns a nil permit when the
// cache is unbounded. It is the pre-attempt admission the runner uses so a
// saturated warm budget leaves the delivery pending (backpressure) instead of
// charging a failed handler attempt.
func (m *Manager) AcquireWarmPermit(ctx context.Context) (*WarmPermit, error) {
	if m == nil || m.containers == nil {
		return nil, nil
	}
	return m.containers.reserveWarm(ctx)
}

// WarmBudgetCapacity returns the configured MAX_WARM_CONTAINERS bound (0 when
// unbounded, e.g. a Manager constructed directly by tests). It is a read-only
// accessor for diagnostics and tests.
func (m *Manager) WarmBudgetCapacity() int {
	if m == nil || m.containers == nil {
		return 0
	}
	m.containers.warmMu.Lock()
	defer m.containers.warmMu.Unlock()
	return m.containers.maxWarm
}

// WarmPermit is one invocation's reservation against the cache's global
// warm-execution-container budget (MAX_WARM_CONTAINERS). It is acquired BEFORE
// an invocation claims a handler attempt, so a saturated warm budget leaves the
// delivery pending (backpressure) instead of charging a failed attempt.
//
// A permit either HOLDS a counted slot (held) or is a non-counting admission
// token granted while the budget is full but an idle container exists (so the
// acquire can reuse it, or evict it and create, without exceeding the bound). It
// is handed to Manager.Execute on the invocation context (WithWarmPermit);
// Execute either attaches a counted permit to the freshly created container
// (which releases it on discard) or releases it unused when it leases an
// existing idle container. Release is idempotent and consume-aware.
//
// Locking: warmMu is a LEAF lock (warmMu -> cache.mu -> pool.mu is the allowed
// order, and nothing takes warmMu while holding cache.mu or pool.mu).
type WarmPermit struct {
	cc   *containerCache
	held bool
	// consumed records that a container now owns the permit, so the runner's
	// deferred unused-release must not free the slot the container holds.
	consumed atomic.Bool
	once     sync.Once
}

// Release returns the permit's slot if it was never handed to a container. It is
// idempotent and safe as a deferred safety net on every path, including the ones
// where Execute already consumed the permit.
func (p *WarmPermit) Release() {
	if p == nil || p.cc == nil || !p.held {
		return
	}
	if p.consumed.Load() {
		// A container owns the permit now; its discard will release the slot.
		return
	}
	p.once.Do(func() { p.cc.releaseWarmSlot() })
}

// consume marks the permit as owned by a container so a later unused Release is
// a no-op and only the container's discard frees the slot.
func (p *WarmPermit) consume() {
	if p != nil {
		p.consumed.Store(true)
	}
}

// releaseSlot frees the counted slot exactly once, on the owning container's
// discard. Unlike Release it does not check consumed (a discarded container owns
// the permit by definition).
func (p *WarmPermit) releaseSlot() {
	if p == nil || p.cc == nil || !p.held {
		return
	}
	p.once.Do(func() { p.cc.releaseWarmSlot() })
}

// warmPermitCtxKey carries a pre-admitted WarmPermit from the runner to
// Manager.Execute, mirroring the image-lease context seam. It is unexported so
// only the runtime owns the key.
type warmPermitCtxKey struct{}

// WithWarmPermit attaches a pre-admitted warm-budget permit to ctx for
// Manager.Execute. A nil permit (unbounded cache, or a caller that did not
// pre-admit) leaves the context unchanged, so Execute acquires its own.
func WithWarmPermit(ctx context.Context, permit *WarmPermit) context.Context {
	if permit == nil {
		return ctx
	}
	return context.WithValue(ctx, warmPermitCtxKey{}, permit)
}

// WarmPermitFrom returns the pre-admitted permit carried in ctx, or nil.
func WarmPermitFrom(ctx context.Context) *WarmPermit {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(warmPermitCtxKey{}).(*WarmPermit)
	return p
}

// lazyInitWarmLocked ensures the warm-budget notification channel exists. It must
// be called with cc.warmMu held.
func (cc *containerCache) lazyInitWarmLocked() {
	if cc.warmNotify == nil {
		cc.warmNotify = make(chan struct{})
	}
}

// reserveWarm returns a permit for one invocation's execution against the global
// warm-container budget. It returns a nil permit (no-op) when no bound is
// configured (maxWarm <= 0), so a directly constructed cache is unbounded exactly
// as before. It returns errPoolClosed once the cache is closed and a context
// error when ctx is done.
//
// It grants a COUNTED permit while the budget has room. When the budget is full
// it grants a NON-COUNTING permit if any idle execution container exists
// anywhere (the acquire will reuse one, or evict the globally oldest idle
// container and replace it, without exceeding the bound). If every container is
// busy, the budget cannot be relieved without killing a busy container, so the
// call WAITS for a release/eviction/close. That wait is the backpressure path
// that must not charge a handler attempt.
func (cc *containerCache) reserveWarm(ctx context.Context) (*WarmPermit, error) {
	if cc == nil {
		return nil, nil
	}
	waited := false
	for {
		cc.warmMu.Lock()
		cc.lazyInitWarmLocked()
		if cc.warmClosed {
			cc.warmMu.Unlock()
			return nil, errPoolClosed
		}
		if cc.maxWarm <= 0 {
			cc.warmMu.Unlock()
			return nil, nil
		}
		if cc.warmCount < cc.maxWarm {
			cc.warmCount++
			cc.publishWarmGaugesLocked()
			cc.warmMu.Unlock()
			return &WarmPermit{cc: cc, held: true}, nil
		}
		notify := cc.warmNotify
		cc.warmMu.Unlock()

		// At the bound: admit without counting if any idle container exists that
		// the acquire can reuse or evict-and-replace.
		if cc.anyIdleContainer() {
			return &WarmPermit{cc: cc, held: false}, nil
		}

		// Every warm container is busy: wait for a release/eviction/close rather
		// than over-committing. This is the no-attempt-charge backpressure path.
		if !waited {
			waited = true
			if cc.metrics != nil {
				cc.metrics.Inc(metrics.MetricRuntimeWarmWaits)
			}
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return nil, fmt.Errorf("docker run: %w", ctx.Err())
		}
	}
}

// releaseWarmSlot returns one global warm-budget slot and wakes waiters. It is
// called exactly once per counted permit.
func (cc *containerCache) releaseWarmSlot() {
	if cc == nil {
		return
	}
	cc.warmMu.Lock()
	if cc.warmCount > 0 {
		cc.warmCount--
	}
	cc.publishWarmGaugesLocked()
	cc.signalWarmLocked()
	cc.warmMu.Unlock()
}

// signalWarmLocked wakes every warm-budget waiter. It must be called with
// cc.warmMu held. A closed cache has no waiters left to wake.
func (cc *containerCache) signalWarmLocked() {
	if cc.warmClosed || cc.warmNotify == nil {
		return
	}
	close(cc.warmNotify)
	cc.warmNotify = make(chan struct{})
}

// signalWarm wakes every warm-budget waiter. It is the lock-taking form used by
// paths (a container returning to idle) that may create an eviction opportunity
// for a create-time reservation; it is safe to call without holding any other
// lock.
func (cc *containerCache) signalWarm() {
	if cc == nil {
		return
	}
	cc.warmMu.Lock()
	cc.signalWarmLocked()
	cc.warmMu.Unlock()
}

// closeWarm marks the warm budget closed and wakes every waiter, so a cache
// shutdown unblocks a reserveWarm immediately (it then returns errPoolClosed).
// It is idempotent and called from closeContext.
func (cc *containerCache) closeWarm() {
	if cc == nil {
		return
	}
	cc.warmMu.Lock()
	if !cc.warmClosed {
		cc.warmClosed = true
		// Close notify directly: signalWarmLocked no-ops once warmClosed is
		// set, so it cannot be used here.
		if cc.warmNotify != nil {
			close(cc.warmNotify)
			cc.warmNotify = make(chan struct{})
		}
	}
	cc.warmMu.Unlock()
}

// publishWarmGaugesLocked publishes the global warm-budget gauges (capacity and
// current usage). It must be called with cc.warmMu held. An unbounded cache
// (maxWarm <= 0) publishes nothing so a directly constructed cache stays
// invisible, matching the per-app metrics' nil-registry no-op discipline.
func (cc *containerCache) publishWarmGaugesLocked() {
	if cc.metrics == nil || cc.maxWarm <= 0 {
		return
	}
	cc.metrics.SetGauge(metrics.MetricRuntimeWarmCapacity, float64(cc.maxWarm))
	cc.metrics.SetGauge(metrics.MetricRuntimeWarmContainers, float64(cc.warmCount))
}

// publishWarmCapacity publishes the capacity gauge once at construction/setup.
// It is a convenience for NewManager, which sets maxWarm after newContainerCache.
func (cc *containerCache) publishWarmCapacity() {
	if cc == nil {
		return
	}
	cc.warmMu.Lock()
	cc.lazyInitWarmLocked()
	cc.publishWarmGaugesLocked()
	cc.warmMu.Unlock()
}

// anyIdleContainer reports whether any pool holds a healthy, non-retired idle
// execution container: the condition under which a full budget can still admit
// an invocation (reuse, or evict-then-create) without exceeding the bound.
func (cc *containerCache) anyIdleContainer() bool {
	cc.mu.Lock()
	cc.lazyInit()
	pools := make([]*appPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()
	for _, p := range pools {
		if p.hasIdle() {
			return true
		}
	}
	return false
}

// hasIdle reports whether the pool holds a healthy, non-retired idle container.
// It takes p.mu.
func (p *appPool) hasIdle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.removing {
		return false
	}
	healthy := func(gens []*generation) bool {
		for _, g := range gens {
			for _, pc := range g.idle {
				if !pc.retired && !pc.c.dead() {
					return true
				}
			}
		}
		return false
	}
	if p.active != nil && healthy([]*generation{p.active}) {
		return true
	}
	return healthy(p.draining)
}

// ensureWarmSlot reserves a COUNTED global warm slot for a container about to be
// created, evicting the globally oldest idle container when the budget is full.
// It is the create-time enforcement used when a permit was granted non-counting
// (or absent) but the acquire must actually start a container.
//
// It is deliberately NON-BLOCKING on the saturation path: when the budget is
// full and no idle container can be evicted (every container busy, or a
// concurrent lease took the candidate between admission and here), it returns
// ErrWarmBudgetSaturated rather than waiting. This matters because acquireVersion
// runs AFTER the runner has claimed a handler attempt (TryStart); blocking here
// for even slotWait, let alone the handler timeout, could hold a claimed attempt
// while the invocation's running deadline elapses, letting a reclaim charge a
// spurious handler retry. The runner maps this sentinel to a pending skip (no
// retry, no DLQ). A failed eviction never counts capacity as released, so the
// bound is never exceeded.
func (cc *containerCache) ensureWarmSlot(ctx context.Context) (*WarmPermit, error) {
	if cc == nil {
		return nil, nil
	}
	cc.warmMu.Lock()
	cc.lazyInitWarmLocked()
	if cc.warmClosed {
		cc.warmMu.Unlock()
		return nil, errPoolClosed
	}
	if cc.maxWarm <= 0 {
		cc.warmMu.Unlock()
		return nil, nil
	}
	if cc.warmCount < cc.maxWarm {
		cc.warmCount++
		cc.publishWarmGaugesLocked()
		cc.warmMu.Unlock()
		return &WarmPermit{cc: cc, held: true}, nil
	}
	cc.warmMu.Unlock()

	// At the bound: try one synchronous oldest-idle eviction to free a slot.
	if cc.evictOldestIdle(ctx) {
		cc.warmMu.Lock()
		cc.lazyInitWarmLocked()
		if cc.warmClosed {
			cc.warmMu.Unlock()
			return nil, errPoolClosed
		}
		if cc.maxWarm > 0 && cc.warmCount < cc.maxWarm {
			cc.warmCount++
			cc.publishWarmGaugesLocked()
			cc.warmMu.Unlock()
			return &WarmPermit{cc: cc, held: true}, nil
		}
		cc.warmMu.Unlock()
		// The evicted slot was grabbed by a concurrent waiter: no capacity for
		// this create right now.
		return nil, ErrWarmBudgetSaturated
	}
	// Every warm container is busy (or the idle candidate was taken): no
	// capacity can be released without killing a busy container, so report
	// saturation and let the caller leave the delivery pending.
	return nil, ErrWarmBudgetSaturated
}

// evictOldestIdle selects the globally oldest healthy IDLE execution container
// across every pool (active plus draining generations) and evicts it with
// reasonWarmEviction. It reports whether a slot was actually freed: a false
// return means no idle container existed, the candidate was taken concurrently,
// or its teardown could not confirm physical removal, so no capacity is counted
// as freed. Only idle containers are considered — a busy container is never
// evicted.
func (cc *containerCache) evictOldestIdle(ctx context.Context) bool {
	cc.mu.Lock()
	cc.lazyInit()
	pools := make([]*appPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()

	var chosen *pooledContainer
	var chosenPool *appPool
	var chosenSince time.Time
	for _, p := range pools {
		if pc, since := p.oldestIdle(); pc != nil {
			if chosen == nil || since.Before(chosenSince) {
				chosen, chosenPool, chosenSince = pc, p, since
			}
		}
	}
	if chosen == nil {
		return false
	}
	return chosenPool.evictIdleContainer(ctx, chosen)
}

// oldestIdle returns the oldest healthy idle container in the pool's active or
// draining generations plus its idleSince snapshot, or (nil, zero) when it has
// none. The timestamp is returned so the caller can compare candidates across
// pools WITHOUT re-reading the live field outside the lock. Retired or dead idle
// containers are skipped: they are already being reaped by their own path and
// must not be counted as a successful eviction. It takes p.mu.
func (p *appPool) oldestIdle() (*pooledContainer, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.removing {
		return nil, time.Time{}
	}
	var oldest *pooledContainer
	var oldestSince time.Time
	consider := func(gens []*generation) {
		for _, g := range gens {
			for _, pc := range g.idle {
				if pc.retired || pc.c.dead() {
					continue
				}
				if oldest == nil || pc.idleSince.Before(oldestSince) {
					oldest, oldestSince = pc, pc.idleSince
				}
			}
		}
	}
	if p.active != nil {
		consider([]*generation{p.active})
	}
	consider(p.draining)
	return oldest, oldestSince
}

// evictIdleContainer removes pc from this pool's idle accounting under the pool
// lock (so a concurrent acquire can never lease it) and discards it with
// reasonWarmEviction. It reports whether pc's global warm-budget slot was
// actually freed: a false return means a concurrent acquire leased it (or it was
// already reaped), or the physical teardown failed to confirm removal, so the
// caller must NOT treat the slot as freed. The teardown runs outside the pool
// lock via discardContainerContext, which releases the container's global warm
// slot exactly once — and only on confirmed removal.
func (p *appPool) evictIdleContainer(ctx context.Context, pc *pooledContainer) bool {
	p.mu.Lock()
	if p.closed || p.removing || !p.removeIdleLocked(pc) {
		p.mu.Unlock()
		return false
	}
	pc.retired = true
	pc.retireReason = reasonWarmEviction
	p.publishPoolGaugesLocked()
	p.signalLocked()
	p.mu.Unlock()
	return p.discardContainerContext(ctx, pc, reasonWarmEviction)
}

// removeIdleLocked removes pc from the active or draining idle list if present
// and returns whether it was found. It must be called with p.mu held.
func (p *appPool) removeIdleLocked(pc *pooledContainer) bool {
	remove := func(g *generation) bool {
		for i, c := range g.idle {
			if c == pc {
				g.idle = append(g.idle[:i], g.idle[i+1:]...)
				return true
			}
		}
		return false
	}
	if p.active != nil && remove(p.active) {
		return true
	}
	for _, g := range p.draining {
		if remove(g) {
			return true
		}
	}
	return false
}

// releaseContainerWarmSlot releases the global warm-budget reservation held by
// this container exactly once, on its discard, and reports whether THIS call did
// the release. A container created without a reservation (unbounded cache, or a
// transient throwaway) holds none and returns false. It is guarded by warmOnce so
// the many idempotent discard paths can race safely.
func (pc *pooledContainer) releaseContainerWarmSlot() bool {
	if pc.warm == nil {
		return false
	}
	released := false
	pc.warmOnce.Do(func() {
		pc.warm.releaseSlot()
		released = true
	})
	return released
}

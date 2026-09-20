package runtime

import (
	"time"

	"relay/internal/metrics"
)

// This file holds the warm-container pool's observability wiring. It is the
// ONLY bridge between the pool's authoritative state (container_cache.go) and
// the metrics.Registry; the pool itself never inspects labels or names.
//
// Update discipline:
//   - The state GAUGES (runtime_containers, runtime_pool_capacity) are read
//     from the pool's own counts and published while the pool lock is held, so
//     concurrent transitions write in lock order and a stale count can never
//     win the last write.
//   - The monotonic COUNTERS and the acquire HISTOGRAM are recorded at the
//     event site and are order-independent.
//   - A pool that is being REMOVED (or was detached from the cache by
//     reactivation) is a permanent metric tombstone: every writer checks
//     `p.removing` under the pool lock, so a late release/teardown from a
//     draining container can never recreate or clobber the series that
//     containerCache.removeFunction deleted. The delete itself runs in the same
//     critical section (cc.mu + p.mu) that sets `removing`, which is what makes
//     the check-and-write atomic with the deletion: a writer is either entirely
//     before the delete (and its series are deleted) or entirely after (and it
//     observes the tombstone and writes nothing).
//
// All recording is nil-safe: a nil registry (metrics disabled) or a nil cache
// makes every method a no-op.

// PoolSnapshot is a point-in-time view of one function's warm container pool,
// returned by Manager.PoolSnapshot. It is deliberately a plain value: the live
// gauges (Capacity/Containers/Busy/Idle/Starting) come from the pool's
// authoritative state, and the cumulative counters come from the metrics
// registry. The LIVE gauges are NOT persisted to SQLite (a persisted live gauge
// would go stale); only the cumulative counters are persisted (see
// state.FunctionStats), so a standalone CLI can render them without the
// worker's memory. It is a live, worker-local view.
type PoolSnapshot struct {
	Function string
	// Capacity is the pool's regular bound (the function's resolved
	// concurrency). Transient throwaway containers are NOT charged against it,
	// so Containers may briefly exceed Capacity while stale retired-image
	// requests are being served (see Busy).
	Capacity int
	// Containers is the number of live pooled containers, i.e. Busy + Idle.
	// Busy includes transient throwaway containers serving a stale
	// retired-image request, so Containers counts every real container the pool
	// currently owns, and may exceed Capacity while such throwaways run. This
	// matches countsLocked (and therefore the runtime_containers gauges).
	Containers int
	// Busy and Idle split Containers by lease state. Busy counts every leased
	// container: the active/draining generations' busy containers plus
	// transient throwaways. Idle counts regular idle containers only (a
	// throwaway is never idle: it is leased for exactly one invocation).
	Busy int
	Idle int
	// Starting is the number of in-flight starts (capacity reservations): lazy
	// active-generation starts plus transient throwaway starts.
	Starting int
	// WarmAcquires counts successful acquires served by an existing idle
	// container; ColdStarts counts acquires that created a fresh container
	// (including a transient throwaway).
	WarmAcquires int64
	ColdStarts   int64
	// Discarded counts containers discarded by the pool (any reason).
	Discarded int64
}

// snapshot returns a point-in-time view of name's pool. ok is false when no
// pool exists for the function (never warmed, or already removed) and also while
// a pool is mid-removal (a removing/detached pool is a metric tombstone: it owns
// no observable series and must not be reported as live). reg is the optional
// metrics registry the cumulative counters are read from (nil is safe: the
// counters read as zero).
func (cc *containerCache) snapshot(name string, reg *metrics.Registry) (PoolSnapshot, bool) {
	if cc == nil {
		return PoolSnapshot{}, false
	}
	cc.mu.Lock()
	p := cc.pools[name]
	cc.mu.Unlock()
	if p == nil {
		return PoolSnapshot{}, false
	}
	p.mu.Lock()
	if p.removing {
		p.mu.Unlock()
		return PoolSnapshot{}, false
	}
	idle, busy, starting := p.countsLocked()
	s := PoolSnapshot{
		Function: name,
		Capacity: p.max,
		Busy:     busy,
		Idle:     idle,
		Starting: starting,
	}
	p.mu.Unlock()
	s.Containers = s.Busy + s.Idle
	s.WarmAcquires, s.ColdStarts, s.Discarded = reg.RuntimePoolCounters(name)
	return s, true
}

// countsLocked returns the pool's current container counts. It must be called
// with p.mu held. It is the SINGLE source for both the runtime_containers gauges
// and PoolSnapshot, so the two can never disagree:
//   - idle: regular idle containers (active + draining generations). A transient
//     throwaway is never idle — it is leased for exactly one invocation.
//   - busy: every leased container, i.e. active/draining busy PLUS transient
//     throwaways. It is therefore possible for busy (and Containers = busy+idle)
//     to momentarily exceed max while stale retired-image requests run; that is
//     deliberate — throwaways are real containers and are counted as such.
//   - starting: in-flight starts (regular lazy starts + transient starts).
func (p *functionPool) countsLocked() (idle, busy, starting int) {
	starting = p.creating + p.transientCreating
	if p.active != nil {
		idle += len(p.active.idle)
		busy += len(p.active.busy)
	}
	for _, g := range p.draining {
		idle += len(g.idle)
		busy += len(g.busy)
	}
	busy += len(p.transient)
	return idle, busy, starting
}

// publishPoolGaugesLocked sets the function's container gauges to the pool's
// current counts. It must be called with p.mu held so the full-count snapshot is
// published atomically with respect to other transitions, and so the last write
// is always the newest state.
//
// A pool that is being REMOVED does not publish: its function's runtime pool
// series are deleted by Manager.RemoveFunction (and a late release from a
// detached draining pool must never resurrect or clobber a reactivated
// function's gauges). A CLOSED pool has torn every container down, so all three
// gauges are zeroed.
func (p *functionPool) publishPoolGaugesLocked() {
	cache := p.cache
	if cache == nil || cache.metrics == nil {
		return
	}
	// p.removing is the pool's removal tombstone (see the file comment): a
	// removing/detached pool must never (re)create the function's gauges.
	if p.removing {
		return
	}
	var idle, busy, starting int
	if !p.closed {
		idle, busy, starting = p.countsLocked()
	}
	fn := metrics.Label{Name: "function", Value: p.name}
	cache.metrics.SetGaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{fn, {Name: "state", Value: metrics.RuntimeStateIdle}}, float64(idle))
	cache.metrics.SetGaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{fn, {Name: "state", Value: metrics.RuntimeStateBusy}}, float64(busy))
	cache.metrics.SetGaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{fn, {Name: "state", Value: metrics.RuntimeStateStarting}}, float64(starting))
}

// publishPoolCapacity sets the function's pool-capacity gauge. It takes the pool
// lock so it is safe to call once the pool is shared through the cache map (a
// concurrent setFunctionConcurrency may be mutating p.max); it is used at
// registration. A pool created after the cache was closed, or one already marked
// removing (a detached pool is never registered), publishes nothing.
func (p *functionPool) publishPoolCapacity() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.publishPoolCapacityLocked()
}

// publishPoolCapacityLocked sets the function's pool-capacity gauge to the
// pool's CURRENT max. It must be called with p.mu held, so a resize and its
// gauge write are atomic with respect to the pool's other transitions and the
// gauge always reflects the authoritative bound (the same p.max acquisition,
// PoolSnapshot, and the socket read).
func (p *functionPool) publishPoolCapacityLocked() {
	if p.cache == nil || p.cache.metrics == nil || p.closed || p.removing {
		return
	}
	p.cache.metrics.SetGaugeLabels(metrics.MetricRuntimePoolCapacity,
		[]metrics.Label{{Name: "function", Value: p.name}}, float64(p.max))
}

// recordDiscard counts exactly one discard of pc. The reason is resolved from
// the container's own recorded reason when it tore itself down
// (timeout/process_exit/protocol_error via executionContainer, or the poison
// path's protocol_error), falling back to the pool-supplied reason for
// pool-initiated teardown (image_changed/shutdown/function_removed/idle_timeout/
// concurrency_shrink).
// sync.Once on the pooled wrapper guarantees a container is counted once even
// when several teardown paths race.
//
// It takes p.mu to make the removal-tombstone check atomic with the write: a
// late release from a removed/detached pool must not recreate the series that
// removeFunction deleted (see the file comment). It is called without p.mu held.
func (p *functionPool) recordDiscard(pc *pooledContainer, fallback string) {
	cache := p.cache
	if cache == nil || cache.metrics == nil || pc == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Removal tombstone: a removing/detached pool owns no observable series.
	if p.removing {
		return
	}
	fn := p.name
	pc.discardOnce.Do(func() {
		reason := fallback
		if r := pc.c.discardReason(); r != "" {
			reason = r
		}
		if reason == "" {
			// Defensive: a container that died without recording a reason (only
			// reachable through a fake/injected container) is reported as a
			// protocol error rather than inventing a new reason.
			reason = reasonProtocolError
		}
		cache.metrics.IncLabels(metrics.MetricRuntimeContainerDiscards, []metrics.Label{
			{Name: "function", Value: fn},
			{Name: "reason", Value: reason},
		})
	})
}

// discardContainer tears pc down (idempotently) and records the discard metric
// exactly once, using the container's own reason when it already died. It is the
// single disposal helper every pool path funnels through, so no discard can be
// missed or double-counted.
func (p *functionPool) discardContainer(pc *pooledContainer, reason string) {
	if pc == nil {
		return
	}
	if !pc.c.dead() {
		pc.c.discard(reason)
	}
	p.recordDiscard(pc, reason)
}

// discardReaped disposes the containers an acquire removed from the idle list
// or discarded on a transition. It runs outside the pool lock; dead containers
// are still recorded (their own path tore them down, e.g. process_exit while
// idle), but their discard is not attempted again.
func (p *functionPool) discardReaped(reaped []*pooledContainer) {
	for _, pc := range reaped {
		reason := pc.retireReason
		if reason == "" {
			reason = reasonImageChanged
		}
		p.discardContainer(pc, reason)
	}
}

// recordAcquireLocked counts one SUCCESSFUL acquire and observes its end-to-end
// duration (from acquire entry, including any capacity wait) under the function.
// Failures, cancellations, and waits that never acquire are never observed; the
// acquire is classified warm (existing idle leased) or cold (a fresh container
// was started, including a transient throwaway). It must be called with p.mu
// held, so the removal tombstone check and the write are atomic with a
// concurrent removeFunction (which sets p.removing under p.mu).
func (p *functionPool) recordAcquireLocked(outcome string, d time.Duration) {
	if p.cache == nil || p.cache.metrics == nil {
		return
	}
	if p.removing {
		return
	}
	fn := metrics.Label{Name: "function", Value: p.name}
	p.cache.metrics.IncLabels(metrics.MetricRuntimeContainerAcquires,
		[]metrics.Label{fn, {Name: "outcome", Value: outcome}})
	p.cache.metrics.ObserveDurationLabels(metrics.MetricRuntimeContainerAcquireDuration,
		[]metrics.Label{fn}, d)
}

// recordWaitLocked counts one acquire that had to block at the pool bound. It is
// a contention proxy mirroring concurrency_waits_total at the pool layer. It
// must be called with p.mu held (removal tombstone: see recordAcquireLocked).
func (p *functionPool) recordWaitLocked() {
	if p.cache == nil || p.cache.metrics == nil {
		return
	}
	if p.removing {
		return
	}
	p.cache.metrics.IncLabels(metrics.MetricRuntimeContainerWaits,
		[]metrics.Label{{Name: "function", Value: p.name}})
}

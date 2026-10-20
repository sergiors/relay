package runner

import (
	"context"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
)

// semaphore is a channel-based counting semaphore that bounds how many
// invocations may execute concurrently (globally or per app). acquire waits up
// to the configured wait (slotWaitTimeout by default) for a free slot;
// returns false on timeout (the invocation is left pending and reclaimed
// later). A per-app semaphore is REPLACED (not mutated) when the app's
// resolved concurrency changes, so an in-flight acquisition still releases to
// the semaphore pointer it captured while new acquisitions use the resized
// one; the global semaphore is rebuilt by SetMaxConcurrentInvocations.
//
// The per-app semaphore is the ONLY per-app invocation limiter: the
// runtime's warm container pool is sized from the same effective concurrency
// (template concurrency clipped to MAX_CONCURRENT_INVOCATIONS), so the semaphore always
// admits no more concurrent Execute calls than the pool has containers, and the
// pool never blocks in the runner path. The global semaphore is the broader cap
// shared across apps.
type semaphore struct {
	slots chan struct{}
	// capacity is the limit the channel was created with. It is immutable and
	// lets concurrencySems detect a reconciled concurrency change by comparing
	// the desired value against the installed semaphore, so the map entry is
	// replaced only on a real change (never resized per call).
	capacity int
}

func newSemaphore(n int) *semaphore {
	return &semaphore{slots: make(chan struct{}, n), capacity: n}
}

// acquire acquires one slot, blocking up to wait until a slot frees, ctx is
// done, or wait elapses. On success it returns (true, waited false/true) and the
// caller MUST release (via release) exactly once. On timeout/cancel it returns
// (false, ...) and no slot is held. waited reports whether the call had to
// block at all (a slot was not immediately available): it feeds the
// concurrency_waits_total counter.
func (s *semaphore) acquire(ctx context.Context, wait time.Duration) (got bool, waited bool) {
	select {
	case s.slots <- struct{}{}:
		return true, false
	default:
		// Not immediately free: fall through to the blocking wait.
	}
	waited = true
	select {
	case s.slots <- struct{}{}:
		return true, true
	case <-time.After(wait):
		return false, true
	case <-ctx.Done():
		return false, true
	}
}

func (s *semaphore) release() {
	<-s.slots
}

// concurrencySems returns the global and per-app semaphores for the given
// app. The app's semaphore is created on demand and RESIZED in place
// (by replacing the map entry with a freshly sized semaphore) whenever the
// app's resolved concurrency differs from the installed one. Replacement
// rather than channel mutation is deliberate: an in-flight acquisition holds
// the OLD pointer and releases to it, so shrinking can never block a release on
// a full new channel nor lose a slot, and no held slot is ever stranded. The
// global semaphore is non-nil on the runner (normalized on construction).
//
// The resized-to concurrency is resolved from the CURRENT registry entry, not
// only the caller's snapshot value (see currentConcurrency): an in-flight Handle
// holding an older snapshot cannot resize the semaphore backwards after a newer
// snapshot already applied a larger bound. The resolved value is additionally
// clipped to the worker-global MAX_CONCURRENT_INVOCATIONS (see effectiveConcurrency), so a
// app asking for more than the global cap never gets a per-app
// semaphore larger than the global one — and the runtime's warm-pool bound,
// also clipped to the cap, agrees with it.
func (r *Runner) concurrencySems(fnName string, fnConcurrency int) (global *semaphore, fn *semaphore) {
	// Global semaphore: normalized on construction / SetMaxConcurrentInvocations; it is
	// always non-nil in practice. Guard nil defensively (a zero-valued Runner
	// in tests would read nil).
	global = r.globalSem.Load()
	if global == nil {
		global = newSemaphore(DefaultMaxConcurrentInvocations)
	}
	fnConcurrency = r.effectiveConcurrency(fnName, fnConcurrency)
	r.fnSemsMu.Lock()
	defer r.fnSemsMu.Unlock()
	if s, ok := r.fnSems[fnName]; ok {
		if s.capacity == fnConcurrency {
			return global, s
		}
		// Resolved concurrency changed: install a freshly sized semaphore so new
		// acquisitions use the new bound. In-flight holds keep the old pointer
		// and release to it (see the semaphore doc).
		s = newSemaphore(fnConcurrency)
		r.fnSems[fnName] = s
		return global, s
	}
	s := newSemaphore(fnConcurrency)
	r.fnSems[fnName] = s
	return global, s
}

// currentConcurrency returns fnName's current resolved per-app concurrency
// from the registry, falling back to fallback when the app is absent or has
// no parsed template. It makes a semaphore resize authoritative to the latest
// published template rather than a caller's possibly-stale registry snapshot.
func (r *Runner) currentConcurrency(fnName string, fallback int) int {
	if r.reg == nil {
		return fallback
	}
	if pf := r.reg.GetByName(fnName); pf != nil && pf.fn.Template != nil {
		return pf.fn.Template.Concurrency
	}
	return fallback
}

// effectiveConcurrency returns the per-app semaphore capacity for fnName:
// its current resolved template concurrency clipped to the worker-global
// MAX_CONCURRENT_INVOCATIONS. Clipping matters when the template asks for more than the
// global cap (e.g. concurrency 15 with MAX_CONCURRENT_INVOCATIONS=8): the per-app
// semaphore is then sized to the cap, matching the runtime's effective warm-pool
// bound, so the pool and the semaphore never disagree. A zero/negative template
// value falls back to app.DefaultConcurrency; a zero maxConcurrentInvocations (a
// zero-valued Runner in tests) falls back to DefaultMaxConcurrentInvocations, never
// "uncapped". The global value is read fresh, so a SetMaxConcurrentInvocations call is
// reflected on the next acquisition (which resizes the app's semaphore).
func (r *Runner) effectiveConcurrency(fnName string, fallback int) int {
	n := r.currentConcurrency(fnName, fallback)
	if n < 1 {
		n = app.DefaultConcurrency
	}
	limit := int(r.maxConcurrentInvocations.Load())
	if limit < 1 {
		limit = DefaultMaxConcurrentInvocations
	}
	if n > limit {
		return limit
	}
	return n
}

// RemoveAppSemaphore drops an app's per-app semaphore. It is
// called when the app is removed from the registry, so a later recreation
// of the same name starts from a fresh semaphore rather than inheriting a stale
// bound (and so a removed app's map entry does not linger). In-flight
// acquisitions still release to the pointer they captured. The constructor
// guarantees a non-nil Runner.
func (r *Runner) RemoveAppSemaphore(fnName string) {
	r.fnSemsMu.Lock()
	delete(r.fnSems, fnName)
	r.fnSemsMu.Unlock()
}

// reserveSlots acquires both the global and per-app slots for one
// invocation, counting a concurrency_waits_total whenever either slot was not
// immediately free (the acquisition had to block). It returns a release func on
// success (call it after the invocation, releasing both slots and the in-flight
// gauge) and whether the acquisition had to block at all (for a debug log); on
// timeout it returns (nil, ...) and does NOT hold any slot. It must be called
// BEFORE TryStart so a blocked invocation is never counted as an attempt and does
// not persist state.
func (r *Runner) reserveSlots(ctx context.Context, fnName string, fnConcurrency int) (release func(), waited bool) {
	global, fn := r.concurrencySems(fnName, fnConcurrency)

	// Acquire the global slot first (the broader bound). A blocked acquire
	// counts a wait, whether it eventually succeeds or not.
	got, w := global.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		return nil, waited
	}

	// Then the per-app slot. A blocked acquire also counts a wait. If the
	// per-app slot never frees, release the global slot so it does not
	// leak to another app's wait.
	got, w = fn.acquire(ctx, r.slotWait)
	if w {
		r.metrics.Inc(metrics.MetricConcurrencyWaits)
	}
	waited = waited || w
	if !got {
		global.release()
		return nil, waited
	}

	// Both slots held and the invocation is about to execute: publish the
	// in-flight gauge for it.
	r.inFlight.Add(1)
	r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))

	released := false
	release = func() {
		if released {
			return
		}
		released = true
		fn.release()
		global.release()
		// Publish the in-flight gauge AFTER dropping below the cap.
		r.inFlight.Add(-1)
		r.metrics.SetGauge(metrics.MetricInFlightInvocations, float64(r.inFlight.Load()))
	}
	return release, waited
}

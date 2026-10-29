package runtime

import (
	"context"
	"time"
)

// maintenanceInterval derives the single maintenance-loop tick from the idle
// timeout: half the window (so an idle container is evicted within one half
// window of crossing the threshold), capped at one minute so a very large
// timeout still gets prompt eviction, and floored at 10ms so a tiny test
// timeout does not busy-loop. This is the ONLY ticker driving eviction.
func maintenanceInterval(idle time.Duration) time.Duration {
	interval := idle / 2
	if interval > time.Minute {
		interval = time.Minute
	}
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	return interval
}

// startMaintenance launches the single maintenance loop. It is idempotent and
// safe to race with Close: the start/close handshake is serialized by maintMu,
// so at most one goroutine is launched, no loop is started after Close has
// begun, and Close joins exactly the loop that was actually started. It is a
// method so tests that construct a Manager directly (bypassing NewManager/Docker)
// can start the loop with a test clock; production goes through StartMaintenance.
func (m *Manager) startMaintenance(interval time.Duration) {
	m.maintMu.Lock()
	defer m.maintMu.Unlock()
	if m.maintStarted || m.closed {
		return
	}
	if m.done == nil {
		m.done = make(chan struct{})
	}
	if m.maintDone == nil {
		m.maintDone = make(chan struct{})
	}
	m.maintStarted = true
	go m.maintenanceLoop(interval)
}

// StartMaintenance starts the warm-container maintenance loop that NewManager
// suppressed under WithDeferredMaintenance. It is the explicit start the worker
// calls only after every configured NETWORKS network has been verified, so no
// background loop runs before the preflight fails closed. It is idempotent
// (a second call is a no-op) and refuses to start after Close, so a close/start
// race can neither leak a goroutine nor double-close the stop channel. Callers
// that did not request deferred maintenance need not call it: their loop is
// already running.
func (m *Manager) StartMaintenance() {
	interval := m.maintInterval
	if interval <= 0 {
		// A Manager constructed directly (tests) may not carry the resolved
		// interval; fall back to the default-derived tick rather than panic in
		// time.NewTicker.
		interval = maintenanceInterval(DefaultWarmContainerIdleTimeout)
	}
	m.startMaintenance(interval)
}

// maintenanceLoop runs evictIdle on a single ticker until Close, then closes
// maintDone so Close can join it before tearing the cache down. It is the only
// eviction driver: there is no per-container goroutine or ticker. Each pass
// runs on the manager lifecycle (not a detached context), so when Close
// cancels the lifecycle an in-flight eviction's container teardown observes
// cancellation and the pass — and therefore the maintDone join in CloseContext —
// returns promptly instead of running out the per-container operation caps
// serially. A Manager constructed directly by tests (no lifecycle) falls back to
// context.Background, preserving the historical detached behavior.
func (m *Manager) maintenanceLoop(interval time.Duration) {
	defer close(m.maintDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	evictCtx := m.lifecycle
	if evictCtx == nil {
		evictCtx = context.Background()
	}
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.containers.evictIdleContext(evictCtx)
		}
	}
}

// Close stops the maintenance loop, discards every cached execution container
// (reason "shutdown"), then releases the Docker Engine client. It is idempotent
// and safe to call more than once during shutdown. It is the unbounded form of
// CloseContext; callers that own a shutdown budget (the worker) use
// CloseContext.
func (m *Manager) Close() error {
	return m.CloseContext(context.Background())
}

// CloseContext is Close under a caller-supplied bound. It:
//
//  1. cancels the manager-owned build lifecycle (so an in-flight Dockerfile
//     build stops promptly); because the maintenance loop's eviction also runs
//     its teardowns on this lifecycle, an eviction pass already in flight is
//     cancelled too, so the join below returns promptly;
//  2. joins the maintenance loop, so no eviction races the teardown;
//  3. PHASE 1 (normal): closes normal admission atomically (ops.closeNormal),
//     cancelling every normal operation's context, then DRAINS every normal
//     operation (ops.drainNormal). After this point no ordinary Manager-owned
//     Docker operation may begin — a synchronous method, a runner async
//     cleanup, a service pass, an execution-container create, or an
//     execution-container discard/watcher all receive ErrManagerShuttingDown
//     — and no ordinary operation (including a pool discard reserved before
//     close) is still in flight when the cache is torn down. The drain also
//     joins every shutdown-only cleanup operation admitted through
//     beginShutdownCleanupOp (the worker's node-scoped service-container
//     removal, admitted after the lifecycle was cancelled but before this
//     call), even when its shutdown step already timed out and the registry
//     moved on, so the client can never close beneath one. A pool discard that
//     races this transition is refused and leaves its container in the pool,
//     owned by cache teardown. The specialized image-retirement gate is closed
//     here too (beginShutdown);
//  4. PHASE 2 (teardown): opens the teardown phase (ops.openTeardown), which
//     ONLY Manager cache teardown may use, then tears down every cached
//     execution container through a bounded parallel worker pool
//     (containerShutdownConcurrency), each teardown context-aware;
//  5. drops any outstanding image leases (reset); it does not wake removal
//     waiters — waking one would let a remover race the close;
//  6. closes teardown admission atomically (ops.closeTeardown) and DRAINS the
//     teardown operations (ops.drainTeardown), so no operation can be admitted
//     after the final drain starts and the client cannot close beneath one;
//  7. closes the Docker Engine client only after every step above returns.
//
// The caller's ctx bounds only the cooperative work of the teardown phase; the
// drains deliberately take no context, because a best-effort cleanup that
// outlives its bound must still be joined before the client closes.
//
// Idempotent: a second call returns the stored client-close result without
// re-running the teardown, exactly like Close. Safe to call more than once
// during shutdown and from a Manager with no Docker client (direct tests).
func (m *Manager) CloseContext(ctx context.Context) error {
	m.closeOnce.Do(func() {
		// Cancel the manager-owned build lifecycle first: an in-flight
		// Dockerfile build rooted here is cancelled promptly rather than running
		// until buildTimeout. The lifecycle is a manager-owned child of any
		// caller-supplied parent, so this is correct both for a worker build
		// (whose signal ctx is the parent) and for a direct caller.
		if m.lifecycleCancel != nil {
			m.lifecycleCancel()
		}
		if m.done != nil {
			// Serialize with StartMaintenance: once closed is set, a
			// concurrent or later StartMaintenance refuses to launch the loop,
			// so the close below and the (possibly absent) maintDone join are
			// race-free. A deferred-maintenance manager whose loop was never
			// started has nothing to join.
			m.maintMu.Lock()
			m.closed = true
			started := m.maintStarted
			close(m.done)
			m.maintMu.Unlock()
			if started && m.maintDone != nil {
				<-m.maintDone
			}
		} else {
			// A direct-construction manager with no stop channel: record the
			// close so a later StartMaintenance cannot start a loop that would
			// outlive the closed manager.
			m.maintMu.Lock()
			m.closed = true
			m.maintMu.Unlock()
		}
		// Make every Manager-owned Docker operation lifecycle-owned, in explicit
		// phases. opsCoord()/leaseCoord() (not raw field reads) are used so
		// authorities lazily created by a concurrent operation are the exact ones
		// gated and drained.
		ops := m.opsCoord()
		coord := m.leaseCoord()

		// PHASE 1 — normal. Close ordinary admission atomically and cancel every
		// normal operation's context, then DRAIN every normal operation. No
		// normal operation may be admitted after this point, and no cache
		// teardown runs until every normal operation (including execution
		// container teardown and the exit-wait watcher) has concluded.
		ops.closeNormal()
		coord.beginShutdown()
		ops.drainNormal()

		// PHASE 2 — teardown. Open the teardown phase, which ONLY Manager cache
		// teardown may use. Nothing is admitted through it before normal
		// admission is closed and fully drained.
		ops.openTeardown()
		teardownCtx, releaseTeardown, terr := ops.beginTeardown(ctx)
		if terr != nil {
			// Defensive: we just opened the phase, so admission cannot be
			// refused. Fall back to a plain context so cache teardown still runs.
			teardownCtx, releaseTeardown = ctx, func() {}
		}
		if m.containers != nil {
			m.containers.closeContext(teardownCtx)
		}
		// Drop any outstanding image leases: the manager is shutting down, so
		// every admitted reference ends here. This is defense against a caller
		// that never released a Prepared handle it did not publish.
		coord.reset()
		// Close teardown admission atomically, then drain it: after closeTeardown
		// no operation of either kind can be admitted, and the final drain cannot
		// be raced by a late discard.
		ops.closeTeardown()
		releaseTeardown()
		ops.drainTeardown()

		// Release the build-temp session last: every preparation has been
		// cancelled and drained above, so no source or dependency snapshot is
		// still staging into it. A cleanup failure is logged, never fatal (the
		// next startup's scavenger reclaims a session that could not be removed).
		if m.buildTemp != nil {
			if cerr := m.buildTemp.Close(ctx); cerr != nil {
				m.Logger().Warn("Runtime: build temp session cleanup failed", "error", cerr)
			}
		}

		if m.cli == nil {
			// A Manager constructed directly by a test (no Docker) still owns a
			// cache and maintenance loop, so Close must be safe without a client.
			return
		}
		if m.closeClient != nil {
			m.closeErr = m.closeClient(m.cli)
		} else {
			m.closeErr = m.cli.Close()
		}
	})
	return m.closeErr
}

// ScavengeBuildRoots removes build-temp sessions abandoned by a crashed Relay
// process, returning how many were reclaimed. It is the startup reclaim the
// worker runs once, after the Manager (and so this process's own locked session)
// exists and before any app source is prepared, so no in-flight preparation can
// be swept.
//
// A session is removed only when its OS advisory owner lock can be acquired,
// which proves no live process owns it; this process's own session, and any
// session another live Relay process holds, is left untouched. The scan is
// serialized by the shared coordination lock, so it can never race a session
// being created. It never uses age or name as a criterion, and it fails closed:
// without the coordination lock (or on any filesystem error) it removes nothing
// and returns the error for the caller to log.
//
// Legacy roots created directly under os.TempDir by an older Relay (a bare
// `relay-build-*` / `relay-dep-build-*` directory with no session and no
// ownership metadata) are deliberately never touched: under a shared temp dir
// one could still belong to a running older Relay, and there is no lock to prove
// otherwise.
//
// It returns (0, nil) when no build-temp root was established for this Manager
// (the os.TempDir fallback), so a caller need not special-case that.
func (m *Manager) ScavengeBuildRoots(ctx context.Context) (int, error) {
	if m == nil || m.buildTempStore == nil {
		return 0, nil
	}
	return m.buildTempStore.Scavenge(ctx)
}

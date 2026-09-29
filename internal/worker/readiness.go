package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"relay/internal/runtime"
	"relay/internal/stream"
)

// readinessProbeTimeout bounds the live dependency probe a `relay health` query
// runs inside the worker: one Docker daemon ping plus verification of every
// configured NETWORKS network. It is deliberately short — a readiness probe
// must answer promptly — and it never terminates the worker; a dependency that
// does not answer within it is reported not ready (and can recover on a later
// query).
const readinessProbeTimeout = 2 * time.Second

// consumerHealth is the narrow live-consumer view the readiness probe needs:
// whether the stream consumer's last Redis operation succeeded. *stream.Consumer
// satisfies it via its Healthy accessor, which is fed by real operations.
type consumerHealth interface {
	Healthy() bool
}

// dockerReadinessProbe is the narrow runtime-manager view the readiness probe
// needs: a bounded Docker daemon ping and verification that every configured
// NETWORKS network still exists. *runtime.Manager satisfies it. A missing
// network is a real dependency failure — Relay never creates networks, so one
// disappearing after startup means execution containers can no longer be
// created correctly.
type dockerReadinessProbe interface {
	Ping(ctx context.Context) error
	VerifyNetworks(ctx context.Context, networks []string) (string, bool, error)
}

// The production seams must satisfy the readiness probe's narrow views.
var (
	_ consumerHealth       = (*stream.Consumer)(nil)
	_ dockerReadinessProbe = (*runtime.Manager)(nil)
)

// readiness owns the worker's process-lifetime ready flag plus the live
// dependency probe the `relay health` query consults over the control socket.
//
// The flag is the worker's own lifecycle state and starts false: it is set true
// only at the actual ready-to-consume boundary (immediately before Consume,
// once dependency preflight, function loading/preparation, the socket, the
// required listeners and loops, and the consumer/schedule/reconciler/scheduler
// wiring are all complete) and is cleared as the FIRST instruction of the
// shutdown defer, before the lifecycle is cancelled and any teardown runs. It
// deliberately does not wait on asynchronous service convergence/housekeeping,
// optional tracing, SQLite, or per-function success: none of those gate the
// worker's readiness to consume.
//
// The flag alone is not sufficient: the worker lifecycle context is bound in as
// well, because it can be cancelled BEFORE Run reaches its deferred clear (a
// signal arriving during the final startup wiring, or just before Consume
// returns). `Ready` therefore reports not-ready whenever that context is
// cancelled, closing the window in which a query would otherwise observe a
// stale true while shutdown has already begun. The explicit first-instruction
// clear is retained as the primary, ordering-independent signal.
//
// Once the flag is true and the lifecycle is live, `Ready` additionally runs
// the live dependency probe so a steady-state query reflects current dependency
// health (Redis consumer health, a bounded Docker ping, and NETWORKS
// verification) rather than only the startup boundary. The probe never mutates
// the flag and never terminates the worker, so a dependency recovering makes
// readiness recover.
type readiness struct {
	ready atomic.Bool

	// lifecycle is the worker's signal/lifecycle context, cancelled the moment
	// shutdown begins — on SIGINT/SIGTERM or by the deferred stop — which can
	// precede Run's setNotReady. It is read directly (context is concurrency
	// safe); a cancelled lifecycle makes Ready report not-ready even while the
	// flag is still true.
	lifecycle context.Context

	// probe is the live dependency probe, installed at the ready-to-consume
	// boundary by startReady. It is read under mu because a socket query can
	// race the (single, startup-goroutine) install.
	mu    sync.Mutex
	probe func(context.Context) (bool, string)
}

// newReadiness constructs a readiness state whose flag starts false, which is
// the correct value for the whole of startup, bound to the worker lifecycle
// context so a cancellation that precedes the deferred clear already reports
// not-ready. A nil lifecycle is tolerated (the flag and clear still gate).
func newReadiness(lifecycle context.Context) *readiness { return &readiness{lifecycle: lifecycle} }

// setReady marks the worker ready. It is called exactly once, immediately
// before Consume.
func (r *readiness) setReady() {
	if r == nil {
		return
	}
	r.ready.Store(true)
}

// setNotReady clears the worker's ready flag. It is called as the first
// instruction of the shutdown defer, before lifecycle cancellation.
func (r *readiness) setNotReady() {
	if r == nil {
		return
	}
	r.ready.Store(false)
}

// startReady wires the live dependency probe (Redis consumer health, a bounded
// Docker ping, and configured NETWORKS verification) and marks the worker ready
// at the ready-to-consume boundary. It is the explicit seam Run crosses once,
// immediately before Consume.
func (r *readiness) startReady(consumer consumerHealth, manager dockerReadinessProbe, networks []string) {
	if r == nil {
		return
	}
	r.setProbe(func(ctx context.Context) (bool, string) {
		return probeReadiness(ctx, consumer, manager, networks, readinessProbeTimeout)
	})
	r.setReady()
}

// setProbe installs the live dependency probe. Nil-safe.
func (r *readiness) setProbe(probe func(context.Context) (bool, string)) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.probe = probe
	r.mu.Unlock()
}

// Ready implements the control socket's ReadinessChecker. A false flag (a
// worker that is still starting) is reported not-ready without consulting any
// dependency, and a cancelled worker lifecycle is likewise reported not-ready
// even while the flag is still true — the lifecycle can be cancelled before Run
// reaches its deferred setNotReady (a signal during the final startup wiring, or
// just before Consume returns), so the flag alone cannot win that race. A true
// flag on a live lifecycle runs the live probe. It is nil-safe and never blocks
// beyond the probe's own bound.
func (r *readiness) Ready(ctx context.Context) (bool, string) {
	if r == nil || !r.ready.Load() {
		// A false flag means the worker has not crossed the ready-to-consume
		// boundary yet: it is still starting. The socket only exists once the
		// worker is running, so there is no "not running" case here.
		return false, "worker starting"
	}
	if r.lifecycle != nil && r.lifecycle.Err() != nil {
		return false, "worker shutting down"
	}
	r.mu.Lock()
	probe := r.probe
	r.mu.Unlock()
	if probe == nil {
		return false, "dependency probe not wired"
	}
	return probe(ctx)
}

// probeReadiness runs the live dependency checks for an already-ready worker:
// Redis consumer health first (a cheap in-process read that avoids a Docker
// round trip during a Redis outage), then ONE bounded window covering a Docker
// daemon ping and verification of every configured NETWORKS network. A nil
// consumer or manager is a hard dependency failure, not a pass. The returned
// reason is a fixed, low-cardinality string that never carries a dependency's
// raw error, so a readiness response cannot leak operator detail. It is a pure
// function of its injected seams, so it is unit-testable with deterministic
// fakes and no sleeps.
func probeReadiness(
	ctx context.Context,
	consumer consumerHealth,
	manager dockerReadinessProbe,
	networks []string,
	timeout time.Duration,
) (bool, string) {
	if consumer == nil {
		return false, "redis consumer unavailable"
	}
	if !consumer.Healthy() {
		return false, "redis consumer unhealthy"
	}
	if manager == nil {
		return false, "docker manager unavailable"
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := manager.Ping(pctx); err != nil {
		return false, "docker unavailable"
	}
	missing, ok, err := manager.VerifyNetworks(pctx, networks)
	switch {
	case err != nil:
		return false, "network verification failed"
	case !ok:
		return false, fmt.Sprintf("network %q missing", missing)
	}
	return true, ""
}

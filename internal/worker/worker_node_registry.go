package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"relay/internal/config"
	"relay/internal/state"
)

// Global node registry
//
// Every worker participates in the PostgreSQL node registry immediately after
// state ownership is established, whether or not it publishes service endpoints
// and whether or not it has apps. The registry maintains one fenced node lease:
// it is the canonical liveness/incarnation record other nodes read, and the
// service endpoint publisher consumes this lease instead of owning its own.
//
// The lease is fenced in PostgreSQL by the pinned advisory-lock backend and the
// database incarnation (see state's node registry methods), so a superseded
// process cannot heartbeat, publish, or release after losing ownership. A
// crashed process needs no cleanup writer: readers stop seeing its lease and
// endpoints when the DB-time lease expires. Graceful shutdown releases this
// incarnation's rows in one bounded transaction.
const (
	// nodeRegistryHeartbeatInterval is the lease renew cadence. It is well
	// below the TTL so several transient failures are tolerated.
	nodeRegistryHeartbeatInterval = 5 * time.Second
	// nodeRegistryLeaseTTL bounds how long a silent node remains eligible for
	// readers. Reads compare it against the PostgreSQL clock.
	nodeRegistryLeaseTTL = 30 * time.Second
	// nodeRegistryOperationTimeout bounds one claim/heartbeat statement.
	nodeRegistryOperationTimeout = 5 * time.Second
	// nodeRegistryReleaseTimeout bounds the post-join release transaction.
	nodeRegistryReleaseTimeout = 5 * time.Second
	// nodeRegistryPruneInterval is the infrequent bounded stale-row cleanup
	// cadence. Cleanup is global maintenance owned by this lifecycle, never by
	// service-endpoint publication or execution.
	nodeRegistryPruneInterval = 5 * time.Minute
	// nodeRegistryPruneTimeout bounds one cleanup invocation.
	nodeRegistryPruneTimeout = 10 * time.Second
)

// nodeLeaseStore is the consumer-owned persistence seam for the global node
// lease. *state.State implements it; a test can substitute a lightweight fake.
type nodeLeaseStore interface {
	ClaimNodeLease(
		ctx context.Context,
		metadata state.NodeLeaseMetadata,
		ttl time.Duration,
	) (state.NodeLease, error)
	HeartbeatNodeLease(
		ctx context.Context,
		lease state.NodeLease,
	) (state.NodeLease, error)
	ReleaseNodeLease(
		ctx context.Context,
		lease state.NodeLease,
	) error
	PruneExpiredNodeRegistry(
		ctx context.Context,
	) (state.NodeRegistryPruneResult, error)
}

// registryFailureReporter logs only failure/recovery TRANSITIONS, so a
// persistently failing registry operation is operator-visible once instead of
// every heartbeat/sync tick. Expected fence rejections do not use it.
type registryFailureReporter struct {
	log *slog.Logger

	mu      sync.Mutex
	failing map[string]bool
}

func newRegistryFailureReporter(log *slog.Logger) *registryFailureReporter {
	return &registryFailureReporter{log: log}
}

func (r *registryFailureReporter) failure(operation string, err error) {
	if r == nil || r.log == nil {
		return
	}
	r.mu.Lock()
	if r.failing == nil {
		r.failing = make(map[string]bool)
	}
	first := !r.failing[operation]
	r.failing[operation] = true
	r.mu.Unlock()
	if first {
		r.log.Warn("Registry: operation failing",
			"operation", operation, "error", err)
		return
	}
	r.log.Debug("Registry: operation still failing",
		"operation", operation, "error", err)
}

func (r *registryFailureReporter) recovery(operation string) {
	if r == nil || r.log == nil {
		return
	}
	r.mu.Lock()
	failed := r.failing[operation]
	delete(r.failing, operation)
	r.mu.Unlock()
	if failed {
		r.log.Info("Registry: operation recovered", "operation", operation)
	}
}

// nodeRegistry owns this worker's global node lease.
type nodeRegistry struct {
	st       nodeLeaseStore
	log      *slog.Logger
	metadata state.NodeLeaseMetadata
	reporter *registryFailureReporter

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wg     sync.WaitGroup

	mu    sync.Mutex
	lease *state.NodeLease
}

// startNodeRegistry starts the global node lease lifecycle and registers its
// shutdown step. It is independent of SERVICE_BIND_ADDR: every worker registers
// and heartbeats a lease so node availability is observable even for an
// event-only deployment.
func startNodeRegistry(
	lifecycle context.Context,
	logger *slog.Logger,
	cfg config.Config,
	st *state.State,
	shutdown *shutdownRegistry,
) *nodeRegistry {
	if st == nil {
		return nil
	}
	advertiseAddr := cfg.ServiceAdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.ServiceBindAddr
	}
	r := &nodeRegistry{
		st:  st,
		log: logger,
		metadata: state.NodeLeaseMetadata{
			BindAddr:      cfg.ServiceBindAddr,
			AdvertiseAddr: advertiseAddr,
		},
		reporter: newRegistryFailureReporter(logger),
	}
	r.Start(lifecycle)
	if shutdown != nil {
		shutdown.register(nodeRegistryStep(r))
	}
	return r
}

// Start runs the heartbeat and infrequent bounded-cleanup loops under
// lifecycle. done closes once both have returned so shutdown can strictly join
// them before releasing the lease.
func (r *nodeRegistry) Start(lifecycle context.Context) {
	r.ctx, r.cancel = context.WithCancel(lifecycle)
	r.done = make(chan struct{})
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.heartbeatLoop()
	}()
	go func() {
		defer r.wg.Done()
		r.pruneLoop()
	}()
	go func() {
		r.wg.Wait()
		close(r.done)
	}()
}

func (r *nodeRegistry) heartbeatLoop() {
	r.heartbeatOnce()
	ticker := time.NewTicker(nodeRegistryHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.heartbeatOnce()
		}
	}
}

// pruneLoop runs the global stale-row cleanup on a slow cadence. It is
// intentionally separate from the heartbeat so a bounded cleanup cannot delay
// lease renewal, and it starts on the first tick rather than adding startup DB
// load.
func (r *nodeRegistry) pruneLoop() {
	ticker := time.NewTicker(nodeRegistryPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.pruneOnce()
		}
	}
}

func (r *nodeRegistry) pruneOnce() {
	ctx, cancel := context.WithTimeout(r.ctx, nodeRegistryPruneTimeout)
	defer cancel()
	result, err := r.st.PruneExpiredNodeRegistry(ctx)
	if err != nil {
		if r.stopping() {
			// Shutdown cancelled the bounded cleanup; not a registry failure.
			return
		}
		r.reporter.failure("prune", err)
		return
	}
	r.reporter.recovery("prune")
	if result.Endpoints > 0 || result.Nodes > 0 {
		r.log.Info("Node registry: pruned expired rows",
			"endpoints", result.Endpoints, "nodes", result.Nodes)
	}
}

func (r *nodeRegistry) heartbeatOnce() {
	ctx, cancel := context.WithTimeout(r.ctx, nodeRegistryOperationTimeout)
	defer cancel()
	lease := r.currentLease(ctx)
	if lease == nil {
		return
	}
	updated, err := r.st.HeartbeatNodeLease(ctx, *lease)
	if err != nil {
		r.handleError("heartbeat", err)
		return
	}
	r.reporter.recovery("heartbeat")
	r.storeLease(updated)
}

// currentLease returns the active lease, claiming one on first use (and after a
// fence rejection cleared it). A claim is fenced in PostgreSQL, so re-claiming
// cannot steal an identity a successor already owns.
func (r *nodeRegistry) currentLease(ctx context.Context) *state.NodeLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lease != nil {
		return r.lease
	}
	if ctx.Err() != nil {
		return nil
	}
	lease, err := r.st.ClaimNodeLease(ctx, r.metadata, nodeRegistryLeaseTTL)
	if err != nil {
		if errors.Is(err, state.ErrNodeLeaseFence) {
			r.log.Debug("Node registry: claim fenced", "error", err)
			return nil
		}
		if r.stopping() {
			return nil
		}
		r.reporter.failure("claim", err)
		return nil
	}
	r.reporter.recovery("claim")
	r.lease = &lease
	r.log.Debug("Node registry: lease claimed",
		"incarnation", lease.Incarnation, "expires_at", lease.ExpiresAt)
	return r.lease
}

func (r *nodeRegistry) storeLease(lease state.NodeLease) {
	r.mu.Lock()
	r.lease = &lease
	r.mu.Unlock()
}

func (r *nodeRegistry) clearLease() {
	r.mu.Lock()
	r.lease = nil
	r.mu.Unlock()
}

// stopping reports lifecycle cancellation. An operation canceled by shutdown is
// normal termination, not a registry failure, and must not emit a failure
// warning.
func (r *nodeRegistry) stopping() bool {
	return r.ctx != nil && r.ctx.Err() != nil
}

// Lease returns a copy of the current node lease, or nil before the first
// successful claim (or after a fence rejection). The endpoint publisher consumes
// this lease; it must never create a lease of its own.
func (r *nodeRegistry) Lease() *state.NodeLease {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lease == nil {
		return nil
	}
	lease := *r.lease
	return &lease
}

// handleError classifies a fenced-write failure. A fence rejection means the
// lease is no longer ours (or the pinned backend lost the lock); clearing it
// makes the next tick attempt a fenced re-claim, which only succeeds while this
// process still holds the node advisory lock. A database failure is transient:
// the lease is retained so the next tick retries it, and the failure is reported
// on its transition.
func (r *nodeRegistry) handleError(operation string, err error) {
	if errors.Is(err, state.ErrNodeLeaseFence) {
		r.log.Debug("Node registry: lease fenced; re-claiming",
			"operation", operation, "error", err)
		r.clearLease()
		return
	}
	if r.stopping() {
		return
	}
	r.reporter.failure(operation, err)
}

// shutdown stops the heartbeat loop, strictly joins it through the shutdown
// registry's barrier semantics, then releases this incarnation's endpoints and
// node lease with a FRESH bounded context. The step context may already be
// expired by a barrier join timeout; reusing it would skip a withdrawal that is
// now safe to run, while a fresh short bound keeps the DB work bounded.
func (r *nodeRegistry) shutdown(ctx context.Context) error {
	r.cancel()
	joinErr := barrierJoin(ctx, func(joinCtx context.Context) error {
		return waitSignal(joinCtx, r.done)
	})
	releaseCtx, cancel := context.WithTimeout(context.Background(), nodeRegistryReleaseTimeout)
	defer cancel()
	releaseErr := r.release(releaseCtx)
	return errors.Join(joinErr, releaseErr)
}

func (r *nodeRegistry) release(ctx context.Context) error {
	r.mu.Lock()
	lease := r.lease
	r.mu.Unlock()
	if lease == nil {
		return nil
	}
	if err := r.st.ReleaseNodeLease(ctx, *lease); err != nil {
		if errors.Is(err, state.ErrNodeLeaseFence) {
			r.log.Debug("Node registry: release fenced; leaving successor rows",
				"error", err)
			return nil
		}
		r.reporter.failure("release", err)
		return err
	}
	r.clearLease()
	r.reporter.recovery("release")
	return nil
}

func nodeRegistryStep(r *nodeRegistry) shutdownStep {
	return shutdownStep{
		name:    shutdownStepNodeRegistry,
		timeout: shutdownServiceTimeout,
		barrier: true,
		run:     r.shutdown,
	}
}

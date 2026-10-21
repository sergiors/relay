package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"relay/internal/app"
)

// startContainer builds one fresh execution container for an app version.
// It is the containerCache factory, called with the creating invocation's
// parameters: img is the resolved immutable image identity (its createImage is
// handed to Docker so the container is created from exactly the resolved bytes),
// env is the app's plan env (per-app, applied at container create),
// limits is the app's effective per-container resource configuration, and
// meta is the creation-time identity RunMeta stamped as labels (per-invocation
// fields left empty — labels are immutable while the container outlives
// invocations). Every container joins the worker-global network set
// (WithNetworks).
//
// The create+attach+start window is one admitted Manager-owned Docker operation:
// create and its cleanup-on-failure happen inside the tracked op, and if the
// manager begins closing while the create is in flight the fresh container is
// torn down inside the op before it is released, so a create can never hand a
// container to a closing manager and the client is never closed under a
// still-live create. The returned container also carries the manager shutdown
// context as its lifecycle, so its detached exit wait and self-initiated
// teardown observe Manager close instead of running detached.
func (m *Manager) startContainer(
	ctx context.Context,
	fnName string,
	img resolvedImage,
	env []string,
	limits app.ResourceLimits,
	meta RunMeta,
) (reusableContainer, error) {
	if m.startContainerFn != nil {
		return m.startContainerFn(ctx, fnName, img, env, limits, meta)
	}
	opCtx, release, err := m.beginDockerOp(ctx)
	if err != nil {
		return nil, err
	}
	workDir, mounts := m.executionSourceMount(fnName)
	c, err := m.createExecutionContainer(
		opCtx, m.containerOps(), m.cli, m.log, fnName, img.createImage(), env,
		m.networks, limits, meta, workDir, mounts...,
	)
	if err != nil {
		// startExecutionContainer has already removed any container it created
		// before returning the error, so the create result is cleaned up before
		// the operation is released.
		release()
		return nil, err
	}
	if m.opsCoord().isClosing() {
		// Manager shutdown began while this create was in flight: never hand a
		// fresh container back to a closing manager. The cleanup runs INSIDE this
		// still-open create operation — it is not separately admitted and does
		// not use the teardown phase — on a fresh background context, NOT the
		// caller's ctx or the create operation's own context (both of which may
		// already be cancelled by shutdown), so the cleanup actually runs
		// (bounded per call by containerOpTimeout). It is counted by the create
		// operation and therefore joined by drainNormal before the client closes.
		// Report the pool as closed, exactly as the pool's own closed-race path
		// would.
		discardOnContext(c, context.Background(), reasonShutdown)
		release()
		return nil, errPoolClosed
	}
	release()
	return c, nil
}

// createExecutionContainer builds the production execution container, or a
// test's injected alternative (startExecutionContainerFn). The seam exists so
// the create-window admission, the shutdown-race cleanup, and the release path
// in startContainer are unit-testable without a real Docker attach hijack.
func (m *Manager) createExecutionContainer(
	ctx context.Context,
	ops containerOps,
	cli *client.Client,
	log *slog.Logger,
	fn, image string,
	env []string,
	networks []string,
	limits app.ResourceLimits,
	meta RunMeta,
	workDir string,
	mounts ...mount.Mount,
) (reusableContainer, error) {
	if m.startExecutionContainerFn != nil {
		return m.startExecutionContainerFn(ctx, ops, cli, log, fn, image, env, networks, limits, meta, workDir, mounts...)
	}
	return startExecutionContainer(ctx, ops, cli, log, fn, image, env, networks, limits, meta, workDir, mounts...)
}

// containerOps returns the manager-owned lifecycle an execution container uses
// for its detached Docker work. It carries BOTH the manager shutdown context
// (so the container's exit wait is cancelled at Manager close) and the manager
// operation gate (so every teardown and the wait watcher itself are admitted,
// tracked, and drained). A Manager with no operation authority (direct test
// construction) uses the zero value, preserving the historical detached
// behavior.
func (m *Manager) containerOps() containerOps {
	gate := m.opsCoord()
	return containerOps{lifecycle: gate.context(), gate: gate}
}

// Execute runs the given handler invocation against a REUSED execution
// container leased from the app's warm pool (up to Prepared.Concurrency
// containers per app per image version; see container_cache.go and
// execution_container.go). The first invocation for an app starts a
// container (stamping its creation-time identity labels from the RunMeta in
// ctx); concurrent invocations of the same app lease distinct containers,
// and each container serves one invocation at a time over the line-JSON
// invocation protocol until it is discarded (timeout, process exit, protocol
// error, image change). A handler failure (ok:false) does NOT discard it.
//
// The pool's bound is Prepared.Concurrency (the effective value, already
// clipped to MAX_CONCURRENT_INVOCATIONS by Prepare and re-clipped here), the same value
// the runner's per-app semaphore uses, so in the runner path the pool
// never blocks (the semaphore already admits at most that many concurrent
// calls). Direct callers that bypass the runner are bounded by the pool itself;
// when the pool is at capacity, Execute blocks until a lease is released, ctx
// is done, or the Manager is closed (errPoolClosed).
//
// The context must carry the per-invocation timeout; a timeout discards the
// container (kill + remove, reason "timeout") and is treated as an invocation
// failure. The invocation's diagnostic RunMeta is read from ctx (see
// WithRunMeta); when absent the labels are empty except the hostname fallback
// below, which is harmless (labels are diagnostic-only beyond the sweep
// predicate).
//
// extraEnv are additional environment variables carried INTO the request frame
// (see invokeRequest.Env) so per-invocation values — template env values and
// resolved secrets — take effect per invocation on the reused container.
// Later entries win on duplicate names ("K=V" parse; later entries overwrite),
// matching the old container-env semantics, so rotating a secret value never
// requires a rebuild.
func (m *Manager) Execute(
	ctx context.Context,
	prepared *Prepared,
	handler string,
	eventJSON []byte,
	extraEnv []string,
) (retErr error) {
	// A warm-cache acquire can complete WITHOUT issuing a Docker call, so it is
	// not covered by the per-operation admission inside startContainer. Refuse
	// lifecycle-cancelled work here, before any pooled container is handed back
	// or any operation context is derived, exactly as an explicit Docker entry
	// point would.
	if err := m.ordinaryRefused(); err != nil {
		return fmt.Errorf("execute %s: %w", prepared.Name, err)
	}
	// The top-level execution span. It is a child of the invocation's context
	// (which the runner already instrumented with function.invoke), so the
	// runtime's acquire/invoke children nest under it. Payload and env values
	// are never attached.
	ctx, span := startRuntimeSpan(ctx, "runtime.execute", prepared.Name, prepared.Image)
	defer func() { finishRuntimeSpan(span, retErr) }()
	meta := RunMetaFrom(ctx)
	if meta.Hostname == "" {
		// Fall back to the manager's worker identity so a direct caller that
		// did not inject RunMeta still stamps the container's owner (and so an
		// orphaned container from this worker is still attributable at sweep
		// time). The runner always injects the full meta; this is the safety
		// net for direct/integration callers.
		meta.Hostname = m.hostname
	}
	if meta.Image == "" {
		// Same safety net for the image identity: the owning app image is
		// known here, so the label (and RemoveImage's in-use guard) stays
		// accurate for direct callers that never set it.
		meta.Image = prepared.Image
	}
	if meta.App == "" {
		meta.App = prepared.Name
	}

	// Pin the image for the whole execution. An execution admitted before the
	// image's retirement carries that admitted lease on ctx (the runner passes a
	// registry snapshot's publication lease), so it holds admitted authority
	// even while retirement drains. admitLease validates that entitlement: the
	// carried lease must actually be live and pin THIS image, and the execution
	// takes its OWN share of it, so the reference is held for the execution's
	// whole duration even if the caller drops its lease concurrently. A direct
	// caller that carries no (or no matching) lease acquires its own independent
	// one, which a retirement in progress rejects with a retryable
	// ErrImageRetiring rather than letting the execution race ImageRemove. A nil
	// lease (no-runtime image, fake executor) is transparent.
	owned, err := m.admitLease(ctx, prepared.Image)
	if err != nil {
		return fmt.Errorf("execute %s: %w", prepared.Name, err)
	}
	if owned != nil {
		defer owned.Release()
	}

	// Creation-time identity meta: per-invocation fields (Handler, MessageID,
	// EventID, EventName) are EMPTY because container labels are immutable at
	// creation while the reused container outlives individual invocations;
	// per-invocation attribution lives only in the request frame and the
	// in-flight output prefix.
	idMeta := meta
	idMeta.Handler = ""
	idMeta.MessageID = ""
	idMeta.EventID = ""
	idMeta.EventName = ""
	// Resolve the app's effective per-container resource limits ONCE for
	// this execution, from the cache's last published configuration, and derive
	// the config fingerprint from exactly those limits. Passing the same pair to
	// the create and to the pool means the HostConfig a container is created
	// with and the generation it is pooled under always agree, so a resource-only
	// hot change rotates containers without a rebuild.
	limits := m.containers.appResources(prepared.Name)
	config := limits.Fingerprint()
	// Resolve the managed image's IMMUTABLE content identity ONCE for this
	// execution, after the image lease above and BEFORE the pool lease and the
	// container create. The lease pins the reference for the whole inspection, so
	// a concurrent retirement/removal cannot delete the image between the
	// resolution and the create; the generation key uses this identity (reference
	// + content ID + fingerprint metadata), so the same tag resolving to new
	// bytes rotates its warm generation; and the create uses the resolved content
	// ID so the container runs exactly the bytes that were resolved, closing the
	// inspect-then-create TOCTOU window. The mutable reference stays
	// prepared.Image for labels, spans, and image retirement. Resolution failure
	// degrades to the reference identity, never failing the invocation.
	img := m.resolveImageIdentity(ctx, prepared.Image, prepared.Fingerprint)
	start := func() (reusableContainer, error) {
		// Every execution container joins the worker-global network set
		// (WithNetworks), which the worker has already verified exists at
		// startup. A network that disappears between verification and create
		// surfaces as a create error here; Relay never creates networks.
		return m.startContainer(ctx, prepared.Name, img, prepared.Env, limits, idMeta)
	}
	// Prepared.Concurrency is populated by Prepare as the effective bound
	// (template concurrency clipped to MAX_CONCURRENT_INVOCATIONS). A hand-built Prepared
	// (direct/integration callers) may carry the raw template value or leave it
	// zero, so clip it here too: the pool bound can never exceed the
	// worker-global cap, and the same bound the runner's clipped per-app
	// semaphore uses is what the pool enforces, keeping the pool from ever being
	// a stricter limiter than the runner's per-app semaphore.
	max := m.clipConcurrency(prepared.Concurrency)
	return m.containers.executeVersion(
		ctx, prepared.Name, img.identity(), config, max, start, handler, eventJSON, envMap(extraEnv),
	)
}

// InvalidateImage retires any cached execution container running the given
// image, WITHOUT blocking: idle containers are discarded immediately and busy
// ones are marked retired and discarded as soon as their invocation releases.
// No later acquire can be handed a pre-invalidation container for that image.
// It is called by the runner when retiring an image so the image's
// ErrImageInUse container-reference guard clears promptly.
func (m *Manager) InvalidateImage(image string) {
	m.containers.invalidateImage(image)
}

// SetAppResources publishes an app's effective per-container resource
// limits to the live warm pool WITHOUT a rebuild. It is the resource half of a
// hot template change: resource limits intentionally do not participate in the
// image fingerprint, so the reconciler's unchanged-fingerprint skip path calls
// this (through the Builder's optional resourceSetter capability) when only the
// app's `resources` changed. A changed value supersedes the current
// container generation so old-config idle containers are discarded and busy ones
// drain, exactly like an image change but without touching the image reference
// or its ownership lease. It is idempotent for an unchanged value.
func (m *Manager) SetAppResources(name string, limits app.ResourceLimits) {
	if name == "" {
		return
	}
	m.containers.setAppResources(name, limits)
}

// PoolSnapshot returns a point-in-time view of name's live warm-container pool:
// capacity, container counts by lease state, and the cumulative acquire/discard
// counters. The gauges are read from the pool's authoritative in-memory state
// (no Docker round trip); the counters are read from the same metrics registry
// the worker exposes on /metrics. ok is false when the app has never warmed
// a pool (or its pool was already removed), so a caller can omit the section
// rather than render stale zeros. It is safe for concurrent use.
//
// This is a LIVE, worker-local view. It exists for in-process callers (an
// embedded CLI/provider, tests); the standalone `relay app inspect` process
// has no access to the worker's memory and therefore renders only the persisted
// cumulative counters with the live gauges marked unavailable (see
// internal/cli). The live gauges are deliberately NOT persisted to PostgreSQL: a
// persisted live gauge would go stale between flushes.
func (m *Manager) PoolSnapshot(name string) (PoolSnapshot, bool) {
	if m == nil || m.containers == nil {
		return PoolSnapshot{}, false
	}
	return m.containers.snapshot(name, m.metrics)
}

// RemoveApp discards a removed app's warm container state: new
// acquires for the app fail immediately, idle containers are discarded now,
// busy ones are retired and discarded when their invocation releases, and the
// pool's state is deleted once it is empty. A later release of a busy container
// can never recreate the state (the app name is remembered as removed until
// Prepared reactivates it). It is non-blocking, so the reconciler's removal hook
// is never stalled by an in-flight invocation, and it is the runtime half of
// app removal; the runner separately retires the app's images.
//
// The app's runtime-pool metric series are deleted by the cache inside the
// SAME critical section that installs the removal tombstone, so no concurrent
// acquire/discard can recreate them (see containerCache.removeApp). A
// genuinely reactivated app gets a fresh pool (and fresh series) after that
// section. The worker's own metricsInstance.RemoveApp and the flush-time
// SweepAppMetrics still cover the runner's series; the runtime no longer
// performs a second, racy delete here.
func (m *Manager) RemoveApp(name string) {
	if name == "" {
		return
	}
	m.containers.removeApp(name)
	// Drop the app's external-service pull-check records so a later
	// re-added app starts with an immediate remote check and the in-memory
	// map does not grow without bound across removals.
	m.forgetServicePullChecks(name)
	// Drop the app's recorded live source mount so a later re-added app (possibly
	// with a different runtime) never inherits a stale mount.
	m.clearSourceMount(name)
}

// envMap parses "K=V" entries into a map, later entries winning on duplicate
// names (the same semantics the old container env argument had).
func envMap(extraEnv []string) map[string]string {
	if len(extraEnv) == 0 {
		return nil
	}
	env := make(map[string]string, len(extraEnv))
	for _, kv := range extraEnv {
		if k, v, ok := strings.Cut(kv, "="); ok {
			env[k] = v
		}
	}
	return env
}

package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
)

// reusableContainer is the seam the per-app container pool programs: the
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

// contextDiscarder is an optional reusableContainer capability: a container that
// can tear itself down on a caller-supplied context, so a shutdown teardown is
// cancelled promptly (and its Docker kill/remove observe the shutdown bound)
// instead of running on the container's own detached context. The production
// *executionContainer implements it; test fakes that only implement the base
// interface fall back to discard via discardOnContext.
type contextDiscarder interface {
	discardContext(ctx context.Context, reason string) bool
}

// discardOnContext tears c down on ctx when the container supports a
// context-aware teardown, falling back to its detached discard otherwise. Every
// pool teardown funnels through it so the shutdown path is context-bounded while
// non-shutdown paths keep their historical detached behavior.
func discardOnContext(c reusableContainer, ctx context.Context, reason string) bool {
	if cd, ok := c.(contextDiscarder); ok {
		return cd.discardContext(ctx, reason)
	}
	return c.discard(reason)
}

// Discard reasons. They are labels on the discard path (logs, metrics, tests),
// not a control mechanism: any of them means the container must never be leased
// again. reasonProtocolError (and timeout/process_exit) are recorded by the
// container itself on self-initiated teardown; the others are pool-initiated.
const (
	reasonImageChanged      = "image_changed"
	reasonResourcesChanged  = "resources_changed"
	reasonShutdown          = "shutdown"
	reasonAppRemove         = "app_removed"
	reasonIdleTimeout       = "idle_timeout"
	reasonConcurrencyShrink = "concurrency_shrink"
	reasonTimeout           = "timeout"
	reasonProcessExit       = "process_exit"
	reasonProtocolError     = "protocol_error"
)

// errPoolClosed is returned when an acquire is attempted on a cache/pool that
// has been closed (graceful shutdown) or whose app has been removed. It is
// not a container failure: the stream layer leaves the invocation pending and a
// live worker replays it.
var errPoolClosed = errors.New("runtime: container pool closed")

// containerCache owns a bounded warm pool of execution containers per APP
// NAME (an app never touches another app's containers). Each pool
// holds at most the app's resolved concurrency containers; an invocation
// LEASES one container for the duration of its Invoke and returns it on
// release. Distinct invocations of the same app therefore run
// concurrently on distinct containers (each container still serializes its own
// protocol I/O), which is what makes the reused request/response frames
// well-defined on a shared stdin/stdout pair.
//
// The pool's bound is the app's concurrency, the SAME value the runner's
// per-app semaphore is sized to. It is deliberately NOT a second
// invocation-concurrency limiter: in the runner path the per-app semaphore
// already admits at most `concurrency` concurrent Execute calls, so the pool
// always has capacity and never blocks; direct Execute callers (integration
// tests) that bypass the runner are bounded by the pool itself.
//
// Locking: a manager-wide mu guards the map itself; each app pool carries
// its own mutex. The manager-wide mu is never held during create/Invoke, and a
// pool mutex is never held across start() or Invoke. When a cache-level mutex
// must be combined with a pool mutex the order is always cache.mu -> pool.mu;
// no path takes pool.mu and then cache.mu.
type containerCache struct {
	// mu guards pools, retiredImages, removedApps, and closed.
	mu    sync.Mutex
	pools map[string]*appPool
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
	// app is successfully re-activated for the same image reference
	// (activateApp clears the reference and every content identity sharing
	// it), and are bounded by the number of distinct image versions retired in a
	// worker's lifetime. Guarded by mu.
	retiredImages map[string]bool
	// removedApps holds app names whose removal has been requested and
	// whose pool may still be draining (busy containers completing) or may
	// already have been deleted once empty. While set, poolFor refuses to create
	// a new pool for the name, so a stale acquire cannot recreate warm state for
	// a removed app. Manager.Prepare clears the mark (activateApp) only
	// after a successful prepare, so a removed-then-recreated app warms
	// again. Guarded by mu.
	removedApps map[string]bool
	// capacity records the last effective per-app concurrency published by
	// a successful Prepare (see setAppConcurrency). poolFor uses it when it
	// CREATES a pool, so a stale in-flight acquire that races a reconcile cannot
	// seed a fresh pool with an out-of-date Prepared.Concurrency — the pool is
	// always created at the current effective bound. It is deleted with the
	// app on removal. Guarded by mu.
	capacity map[string]int
	// resources records the last effective per-app resource limits published
	// by a successful Prepare or a resource-only reconcile
	// (see setAppResources). poolFor seeds a newly created pool from it, and
	// Execute reads it so a resource-only hot change takes effect on the next
	// container create without a Prepared rebuild. It is deleted with the
	// app on removal. Guarded by mu.
	resources map[string]app.ResourceLimits
	closed    bool

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

// generation is one container VERSION's set inside an app pool. A version is
// the triple (image CONTENT identity, resource config fingerprint): the image
// content determines the code, and the config fingerprint (`relay.resources`, see
// app.ResourceLimits.Fingerprint) determines the per-container resource
// limits. The image half is the IMMUTABLE content identity resolved before the
// lease (see resolvedImage.identity) — the reference plus the Docker image ID and
// the Relay fingerprint metadata — so the same tag whose content changed is a NEW
// version that rotates the generation, while identical content keeps warming. A
// pool has exactly one active generation (the version new acquires serve) plus
// zero or more draining generations: superseded or invalidated versions whose
// busy containers are still completing. Idle containers of a non-active
// generation are never kept — they are discarded the moment the generation is
// superseded — and a draining generation is dropped as soon as its last busy
// container releases. Grouping by generation makes "no new acquires of an old
// version" structural: acquire only ever leases from or appends to p.active.
//
// A resource-only change (same image, new config) is therefore a version change:
// it supersedes the active generation and drains it exactly like an image change,
// WITHOUT retiring the image itself (leases/GC and relay.image are untouched).
//
// The image field holds the resolved content IDENTITY, not the bare reference.
// Retirement is scoped to the mutable REFERENCE, which identityRef recovers from
// the identity key (the key always begins with "<ref>\x00"), so
// InvalidateImage/RemoveImage and Prepare's re-activation keep working on the
// app's own tag regardless of which content identity is currently warm.
type generation struct {
	// image is the immutable content identity (see resolvedImage.identity) that
	// keys generation matching.
	image  string
	config string
	idle   []*pooledContainer
	busy   map[*pooledContainer]struct{}
}

func newGeneration(image, config string) *generation {
	return &generation{image: image, config: config, busy: map[*pooledContainer]struct{}{}}
}

// matches reports whether this generation serves the requested image + config
// version.
func (g *generation) matches(image, config string) bool {
	return g.image == image && g.config == config
}

// identityRef recovers the mutable image reference from a content identity key
// built by imageIdentityKey. A key always begins with the reference followed by a
// NUL separator (or is the bare reference itself), so the first NUL is the
// boundary. It lets reference-scoped retirement (invalidateImage, activation)
// match every content identity of a tag without threading the reference
// separately through the pool.
func identityRef(identity string) string {
	if i := strings.IndexByte(identity, 0); i >= 0 {
		return identity[:i]
	}
	return identity
}

// imageRetired reports whether a requested content identity is retired in the
// set: either the exact identity (a forward transition's superseded content) or
// its bare reference (invalidateImage/RemoveImage retirement of the whole tag).
// A request that carries no content suffix (a Docker-less caller) additionally
// matches ANY retired content identity of the same reference, since a retirement
// of a tag's content retires the tag for such a caller.
func imageRetired(set map[string]bool, identity string) bool {
	if set[identity] {
		return true
	}
	ref := identityRef(identity)
	if set[ref] {
		return true
	}
	if ref != identity {
		return false
	}
	for k := range set {
		if identityRef(k) == ref {
			return true
		}
	}
	return false
}

// clearImageRetirement un-retires every key belonging to ref: the bare reference
// and every content identity sharing it. It is how activation re-warms a reverted
// content address as well as the reference itself.
func clearImageRetirement(set map[string]bool, ref string) {
	delete(set, ref)
	for k := range set {
		if identityRef(k) == ref {
			delete(set, k)
		}
	}
}

// appPool is one app's bounded warm container pool.
//
// Capacity accounting: usedLocked() (active idle+busy, draining busy, and
// creating reservations) must stay <= max. creating is the number of capacity
// reservations in flight (a lazy start that has not yet registered its
// container); it is rolled back if start fails.
type appPool struct {
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

	// retiredImages holds image identities and references whose containers must
	// never be pooled again (image retirement/invalidation or a superseded active
	// image). A container created for a retired identity is marked retired at
	// creation: it serves its invocation at-least-once and is discarded on
	// release, so no later acquire can reuse it ("no new acquires old image").
	//
	// Keys are either a full content identity ("<ref>\x00<id>\x00<fingerprint>",
	// see imageIdentityKey) recorded by a forward version transition — which must
	// retire only the OLD content of a shared tag, not the tag itself — or a bare
	// reference (no NUL) recorded by invalidateImage/retireOwned, which retires
	// every content identity of that tag. versionRetired matches BOTH the exact
	// identity and its reference prefix, so a retagged image whose content moved
	// is served on a throwaway and a dead tag never warms.
	//
	// Retirements are permanent for the pool's lifetime UNLESS the image is
	// re-activated: activateApp clears both the bare reference and every
	// identity key sharing its reference, so a reverted content address warms
	// again. The set is bounded by the number of distinct app versions
	// retired in a worker's lifetime (and is discarded with the pool on app
	// removal). Guarded by mu.
	retiredImages map[string]bool

	// retiredConfigs holds resource-config fingerprints whose containers must
	// never be pooled again because a newer effective resource config superseded
	// them (setAppResources). It parallels retiredImages for the config
	// half of the version identity: a stale request carrying an old config
	// fingerprint (same image) must be served on a throwaway transient rather
	// than re-pooling the old limits, and must not supersede the new active
	// generation. Re-publishing a config (setAppResources, after a revert to
	// a previously-retired fingerprint) clears the entry via replaceConfig so it
	// warms again. Guarded by mu.
	retiredConfigs map[string]bool

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

// pooledContainer is one reusable container in an app pool, with its image
// version, resource-config fingerprint, owning generation, and retirement state.
// retired/retireReason/idleSince are guarded by the owning appPool.mu;
// membership in a generation's idle/busy set (or the pool's transient set) is
// the lease state. gen is nil for transient containers.
type pooledContainer struct {
	c reusableContainer
	// image and config are the container's version identity: image is the
	// immutable content identity (resolvedImage.identity) it was created from,
	// and config is the resource-config fingerprint it was created with (see
	// generation). Both participate in the generation match; the mutable
	// reference for retirement is identityRef(image).
	image  string
	config string
	gen    *generation
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
// requested image content identity AND resource-config version.
func (pc *pooledContainer) matchesVersion(image, config string) bool {
	return pc.image == image && pc.config == config
}

func newContainerCache() *containerCache {
	return &containerCache{
		pools:         map[string]*appPool{},
		retiredImages: map[string]bool{},
		removedApps:   map[string]bool{},
		capacity:      map[string]int{},
		resources:     map[string]app.ResourceLimits{},
	}
}

func newAppPool(name string, max int, cache *containerCache) *appPool {
	if max < 1 {
		max = 1
	}
	return &appPool{
		name:           name,
		cache:          cache,
		max:            max,
		transient:      map[*pooledContainer]struct{}{},
		retiredImages:  map[string]bool{},
		retiredConfigs: map[string]bool{},
		notify:         make(chan struct{}),
	}
}

// clock returns the cache's current time from the injected seam, or the wall
// clock when unset. It is called with the owning pool's lock held, so the seam
// must be set before the cache is used (tests do).
func (p *appPool) clock() time.Time {
	if p.cache != nil && p.cache.now != nil {
		return p.cache.now()
	}
	return time.Now()
}

// lazyInit ensures the maps exist (a zero-valued Manager from tests must still
// be Close-able). It must be called with cc.mu held.
func (cc *containerCache) lazyInit() {
	if cc.pools == nil {
		cc.pools = map[string]*appPool{}
	}
	if cc.retiredImages == nil {
		cc.retiredImages = map[string]bool{}
	}
	if cc.removedApps == nil {
		cc.removedApps = map[string]bool{}
	}
	if cc.capacity == nil {
		cc.capacity = map[string]int{}
	}
	if cc.resources == nil {
		cc.resources = map[string]app.ResourceLimits{}
	}
}

// poolFor returns (creating) the pool for fnName. max is used only when the pool
// is CREATED: an existing pool's bound is authoritative and is updated in place
// by setAppConcurrency when Prepare reconciles the app's resolved
// concurrency, never by a later acquire (a stale Prepared must not resize a live
// pool backwards).
//
// A pool requested after the cache is closed is returned closed so acquire
// fails immediately. An app whose removal has been requested (and whose
// pool may already have been deleted) gets a detached, closed pool so a stale
// acquire can neither recreate warm state nor block: poolFor never inserts a
// pool for a removed app until activateApp clears the mark.
func (cc *containerCache) poolFor(fnName string, max int) *appPool {
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
	if cc.removedApps[fnName] {
		p := newAppPool(fnName, max, cc)
		p.removing = true
		p.closed = true
		close(p.notify)
		return p
	}
	p := newAppPool(fnName, max, cc)
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
	// map. Its bound is later updated in place by setAppConcurrency when
	// Prepare reconciles a changed concurrency.
	p.publishPoolCapacity()
	return p
}

// setAppResources updates fnName's effective per-container resource limits
// and retires any warm containers created under the previous limits, so a
// resource-only template change takes effect on the next container create
// WITHOUT a rebuild. It is how the reconciler's skip path (a resource-only
// change does not move the image fingerprint) propagates resources to the live
// pool.
//
// A pool that does not exist yet needs no action beyond recording (the next
// acquire creates it seeded with the current limits). On a changed value the
// active generation is superseded: idle old-config containers are discarded now,
// busy ones drain on release, and the next acquire opens a new generation. A
// no-op (equal limits) touches nothing, so an unchanged template never churns.
func (cc *containerCache) setAppResources(fnName string, limits app.ResourceLimits) {
	limits = limits.OrDefault()
	cc.mu.Lock()
	cc.lazyInit()
	previous, had := cc.resources[fnName]
	cc.resources[fnName] = limits
	p := cc.pools[fnName]
	cc.mu.Unlock()
	if p == nil || (had && previous == limits) {
		return
	}
	// The pool's generation identity changes with the resource fingerprint, so
	// retire the current active generation now rather than waiting for a lazy
	// acquire: idle old-config containers must not be served, and the drain
	// begins immediately.
	for _, pc := range p.replaceConfig(limits.Fingerprint()) {
		p.discardContainer(pc, reasonResourcesChanged)
	}
}

// replaceConfig supersedes the active generation's resource config with
// newConfig. It un-retires newConfig (a revert to a previously-retired config
// must warm again), retires the currently-active config, discards its idle
// containers immediately, and moves its busy ones to a draining generation. It
// returns the idle containers to discard outside the lock, and must not be called
// with p.mu held.
func (p *appPool) replaceConfig(newConfig string) []*pooledContainer {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.retiredConfigs, newConfig)
	if p.closed || p.removing {
		return nil
	}
	if p.active == nil || p.active.config == newConfig {
		return nil
	}
	old := p.active
	p.retiredConfigs[old.config] = true
	var discard []*pooledContainer
	for _, pc := range old.idle {
		if !pc.retired {
			pc.retired = true
			pc.retireReason = reasonResourcesChanged
		}
	}
	discard = append(discard, old.idle...)
	old.idle = nil
	for pc := range old.busy {
		if !pc.retired {
			pc.retired = true
			pc.retireReason = reasonResourcesChanged
		}
	}
	if len(old.busy) > 0 {
		p.draining = append(p.draining, old)
	}
	// Drop the active generation entirely: the next acquire opens a fresh
	// generation for the new config, so no old-config container can be leased.
	p.active = nil
	p.publishPoolGaugesLocked()
	p.signalLocked()
	return discard
}

// appResources returns fnName's current effective per-container resource
// limits as recorded by the last successful Prepare or resource reconcile,
// defaulting to the package defaults for an app that never published any
// (a direct/integration caller). It is the single read path for both the cache's
// generation identity and the Manager's container create, so the limits a
// container is created with and the generation it is pooled under always agree.
func (cc *containerCache) appResources(fnName string) app.ResourceLimits {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	if limits, ok := cc.resources[fnName]; ok {
		return limits.OrDefault()
	}
	return app.DefaultResourceLimits()
}

// appConfig is shorthand for the resource fingerprint of fnName's current
// effective limits: the config half of the cache's generation identity.
func (cc *containerCache) appConfig(fnName string) string {
	return cc.appResources(fnName).Fingerprint()
}

// setAppConcurrency updates fnName's live warm pool bound to max. It is how
// a successful app Prepare propagates a reconciled `concurrency` to an
// ALREADY-CREATED pool without a restart: the pool bound is not first-wins. A
// pool that does not exist yet needs no update (the next acquire creates it with
// the current max, supplied from the current Prepared); a removed or closed pool
// is left alone. The new bound publishes the capacity gauge and wakes blocked
// acquires, and on a DECREASE it immediately retires only EXCESS IDLE containers
// (see setMax and release for the shrink invariant).
func (cc *containerCache) setAppConcurrency(fnName string, max int) {
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
func (p *appPool) setMax(max int) []*pooledContainer {
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
func (p *appPool) takeExcessIdleLocked(n int) []*pooledContainer {
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
// version. image is the version's image identity: production passes the resolved
// content identity (see resolvedImage.identity) from Manager.Execute, while the
// compatibility form below and direct callers may pass a bare reference (a valid
// identity key with no content suffix). start creates a fresh container when the
// pool has no idle one and capacity remains (lazily); it returns the caller's
// error verbatim on failure. A container that the invocation poisoned
// (timeout/process exit/protocol error/panic) is dropped on release so a later
// call starts fresh; a plain handler error keeps the container.
//
// This compatibility form takes only the image and derives the resource-config
// half of the version identity from the cache's last published effective limits
// (appConfig), so a direct caller that never went through
// setAppResources still pools under a consistent config. The Manager's
// production path uses executeVersion, which passes the config it resolved for
// the create so the HostConfig and the generation identity cannot disagree.
func (cc *containerCache) execute(
	ctx context.Context,
	fnName, image string,
	max int,
	start func() (reusableContainer, error),
	handler string,
	eventJSON []byte,
	env map[string]string,
) (retErr error) {
	return cc.executeVersion(ctx, fnName, image, cc.appConfig(fnName), max, start, handler, eventJSON, env)
}

// executeVersion is execute with an explicit resource-config fingerprint: the
// caller resolved fnName's effective limits for the container create and passes
// their fingerprint so the container is pooled under exactly the configuration
// it was created with. image is the version's image identity (the resolved
// content identity in production; a bare reference from direct callers). It is
// the production path (Manager.Execute).
func (cc *containerCache) executeVersion(
	ctx context.Context,
	fnName, image, config string,
	max int,
	start func() (reusableContainer, error),
	handler string,
	eventJSON []byte,
	env map[string]string,
) (retErr error) {
	// A span around the pool acquire (a warm lease or a cold container start),
	// distinct from the invocation protocol exchange below, so a slow pool wait
	// or cold start is attributable separately. Spans carry the stable mutable
	// reference (identityRef), not the content key's NUL-separated identity, so
	// the attribute stays low-cardinality and greppable.
	spanImage := identityRef(image)
	acquireCtx, acquireSpan := startRuntimeSpan(ctx, "runtime.acquire", fnName, spanImage)
	lease, err := cc.acquireVersion(acquireCtx, fnName, image, config, max, start)
	if err != nil {
		finishRuntimeSpan(acquireSpan, err)
		return err
	}
	acquireSpan.End()
	// The lease is always returned, even if the invocation panics: a panic
	// must never leak capacity. invoke poisons the container first when it
	// panics, so release discards rather than reuses a possibly-corrupt one.
	defer lease.release()
	// The invocation protocol exchange: one request/response frame over the
	// leased container's stdin/stdout.
	invokeCtx, invokeSpan := startRuntimeSpan(ctx, "runtime.invoke", fnName, spanImage)
	defer func() { finishRuntimeSpan(invokeSpan, retErr) }()
	return lease.invoke(invokeCtx, handler, eventJSON, env)
}

// acquire leases one container for fnName's version, blocking (context-aware)
// when the pool is at capacity until a release/close/eviction/transition/removal
// notifies it. It is the compatibility form that derives the resource-config
// fingerprint from the cache's last published limits; the Manager uses
// acquireVersion with the explicit config it also maps into the create.
func (cc *containerCache) acquire(
	ctx context.Context,
	fnName, image string,
	max int,
	start func() (reusableContainer, error),
) (*containerLease, error) {
	return cc.acquireVersion(ctx, fnName, image, cc.appConfig(fnName), max, start)
}

// acquireVersion leases one container for fnName's (image, config) version. It:
//
//   - refuses to serve a REMOVED or CLOSED pool, returning errPoolClosed;
//   - refuses to POOL containers for a version that has been retired: a stale
//     request for a RETIRED image OR a superseded resource config is served
//     at-least-once on a throwaway (transient) container that is never pooled
//     and is discarded on release ("no new acquires old version"), without
//     reaping, rewinding, or waiting on the current (newer) version. The
//     runner's per-app semaphore keeps the number of such in-flight stale
//     requests bounded in production;
//   - on a request for a NEW version (a different image or resource config),
//     supersedes the active generation: idle containers of the old version are
//     discarded immediately, its busy ones are moved to a draining generation
//     and discarded on release, and the new version becomes the sole active
//     generation. No further acquire can lease an old-version container, and no
//     new old-version generation is created;
//   - leases an idle matching container from the active generation, or lazily
//     starts a new one while capacity remains (reserving the slot before start
//     so concurrent waiters account for it, and rolling the reservation back on
//     start failure).
//
// invalidateImage (the runner's image-retirement path) is what retires a
// known-dead image's busy containers; the forward transition above covers
// direct callers that switch Prepared without an explicit invalidation.
func (cc *containerCache) acquireVersion(
	ctx context.Context,
	fnName, image, config string,
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
	recordWait := func(p *appPool) {
		if !waitRecorded {
			waitRecorded = true
			p.recordWaitLocked()
		}
	}

	// A panic escaping start() must not leak a capacity reservation, or the
	// pool would be permanently at capacity. reserved/transientReserved track
	// which reservation is currently held so the unwind path can roll it back
	// (and delete a pool whose removal landed while the start was in flight).
	var reserved *appPool
	var transientReserved *appPool
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

		// A stale request for an already-retired version (a retired image or a
		// superseded resource config) is served on a throwaway container that is
		// never pooled ("no new acquires old version"). Transients are bounded by
		// max among themselves but do NOT consume the regular pool capacity, so a
		// stale request can neither be blocked by nor evict the current version's
		// idle containers.
		if p.versionRetired(image, config) {
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
				p.discardContainer(&pooledContainer{c: c, image: image, config: config}, reason)
				if removing {
					// The removal may have run while this transient was starting,
					// when the pool was not yet empty; it can be deleted now.
					p.cache.maybeDeletePool(p.name, p)
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image, config: config, retired: true, retireReason: p.retiredReasonLocked(image, config)}
			p.transient[pc] = struct{}{}
			p.publishPoolGaugesLocked()
			p.recordAcquireLocked(metrics.RuntimeOutcomeCold, time.Since(acquiredAt))
			p.mu.Unlock()
			return &containerLease{pool: p, pc: pc}, nil
		}

		// Forward version transition: a request for a new (image, config)
		// supersedes the active generation. The old version's idle containers are
		// discarded now and its busy ones on release, and it can never be pooled
		// again. This also covers direct callers that use a new Prepared without a
		// runner InvalidateImage call.
		var transitioned []*pooledContainer
		switch {
		case p.active == nil:
			p.active = newGeneration(image, config)
		case !p.active.matches(image, config):
			old := p.active
			// A superseded version must not be repooled by any later acquire.
			// Each half is retired ONLY when it actually changed: an image-only
			// change must keep the (still-current) resource config poolable, and
			// a config-only change must keep the (still-current) image alive and
			// warm. Retiring both halves unconditionally would make the next
			// acquire for the SAME unchanged half a transient, destroying warm
			// pooling on every image or resource edit.
			//
			// The image half is retired by REFERENCE when the tag moved to a
			// different tag (every content of the old tag is stale) and by exact
			// CONTENT identity when the same tag was retagged to new bytes (only
			// the old bytes are stale, so a later acquire that resolves the tag
			// back to those bytes is served transiently, never pooled).
			if old.image != image {
				if oldRef := identityRef(old.image); oldRef != identityRef(image) {
					p.retiredImages[oldRef] = true
				} else {
					p.retiredImages[old.image] = true
				}
			}
			if old.config != config {
				p.retiredConfigs[old.config] = true
			}
			for _, pc := range old.idle {
				if !pc.retired {
					pc.retired = true
					pc.retireReason = versionReason(old.image, image)
				}
			}
			transitioned = append(transitioned, old.idle...)
			old.idle = nil
			for pc := range old.busy {
				if !pc.retired {
					pc.retired = true
					pc.retireReason = versionReason(old.image, image)
				}
			}
			if len(old.busy) > 0 {
				p.draining = append(p.draining, old)
			}
			p.active = newGeneration(image, config)
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
			if pc.retired || !pc.matchesVersion(image, config) {
				pc.retired = true
				if pc.retireReason == "" {
					pc.retireReason = versionReason(pc.image, image)
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
			if !pc.matchesVersion(image, config) || pc.retired {
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
				p.discardContainer(&pooledContainer{c: c, image: image, config: config}, reason)
				if removing {
					// The removal may have run while this start was in flight,
					// when the pool still held the reservation; it can be
					// deleted now that this last slot is gone.
					p.cache.maybeDeletePool(p.name, p)
				}
				return nil, errPoolClosed
			}
			pc := &pooledContainer{c: c, image: image, config: config}
			// Invalidation or transition landed while this start was in flight:
			// serve the invocation, but never pool the container. The active
			// generation serves the requested version only when no transition
			// occurred; if it was superseded, this start still loses its slot.
			if p.active == nil || !p.active.matches(image, config) || p.versionRetired(image, config) {
				pc.retired = true
				pc.retireReason = p.retiredReasonLocked(image, config)
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

// versionReason picks the discard reason for a superseded version: an image
// content change keeps the historical reason (metrics and tests depend on it),
// while a resource-only change (same image identity, different config) reports
// reasonResourcesChanged. A retag of the same reference to new content is an
// image change (the code differs) even though the reference string is unchanged.
func versionReason(oldImage, newImage string) string {
	if oldImage != newImage {
		return reasonImageChanged
	}
	return reasonResourcesChanged
}

// versionRetired reports whether the (image, config) version is currently
// retired in this pool: either the image (by exact content identity or by its
// whole reference) was invalidated/superseded, or the resource config was
// superseded. It must be called with p.mu held.
func (p *appPool) versionRetired(image, config string) bool {
	return imageRetired(p.retiredImages, image) || p.retiredConfigs[config]
}

// retiredReasonLocked picks the discard reason for a stale (image, config)
// request: an image retirement reports reasonImageChanged, otherwise the config
// was superseded and it reports reasonResourcesChanged. It must be called with
// p.mu held, after versionRetired has confirmed the version is retired.
func (p *appPool) retiredReasonLocked(image, config string) string {
	if imageRetired(p.retiredImages, image) {
		return reasonImageChanged
	}
	if p.retiredConfigs[config] {
		return reasonResourcesChanged
	}
	return reasonImageChanged
}

// usedLocked returns the number of regular capacity slots currently consumed:
// the active generation's containers (idle and busy), every draining
// generation's busy containers (they are real containers still completing, so
// they count), and in-flight start reservations. Transients are deliberately
// excluded. It must be called with p.mu held.
func (p *appPool) usedLocked() int {
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
func (p *appPool) rollbackStartLocked(transient bool) {
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
// app: idle ones are discarded immediately, busy ones are marked retired
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
	pools := make([]*appPool, 0, len(cc.pools))
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
func (p *appPool) invalidateImage(image string) {
	p.mu.Lock()
	// Remember the retirement by REFERENCE so any start already in flight (or
	// any later stale acquire resolving this tag to any content) produces a
	// throwaway container that is discarded on release rather than pooled.
	p.retiredImages[image] = true
	var discard []*pooledContainer
	if p.active != nil && identityRef(p.active.image) == image {
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
		if identityRef(g.image) != image {
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

// removeApp requests the removal of fnName's warm container state. The
// request is linearized under the cache lock: it sets the removed mark and, in
// the SAME critical section (cache.mu -> pool.mu), marks the pool removing, so
// a concurrent activateApp can never be undone by this removal's later
// teardown (activation observes removing and detaches the pool), and no
// concurrent acquire can create warm state after this point (poolFor refuses
// while the mark is set). Idle containers are then discarded outside the locks,
// busy ones were retired so their release discards them, and once the pool is
// empty its state is deleted from the cache.
//
// The app's runtime-pool metric series are deleted in the SAME critical
// section that installs the removal tombstone (p.removing), under BOTH cc.mu and
// p.mu. This is what makes metric cleanup atomic with the lifecycle transition:
// every pool metric writer checks the tombstone under p.mu, so a writer runs
// entirely before the delete (its series are then deleted) or entirely after
// (it observes the tombstone and writes nothing) — it can never recreate a
// series after the delete. Holding cc.mu additionally excludes a concurrent
// reactivation: activateApp needs cc.mu, so it cannot clear the removal
// mark and let a fresh pool publish between the tombstone and the delete (which
// would delete the reactivated pool's brand-new series). A genuinely reactivated
// app gets a fresh pool after this critical section and publishes normally.
func (cc *containerCache) removeApp(fnName string) {
	cc.mu.Lock()
	cc.lazyInit()
	cc.removedApps[fnName] = true
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
// is attached. It is called from removeApp's critical section (cc.mu held,
// and p.mu held when a pool exists) so the delete is atomic with the removal
// tombstone; it must not be hoisted outside that section.
func (cc *containerCache) deleteRuntimePoolMetrics(fnName string) {
	if cc.metrics != nil {
		cc.metrics.DeleteRuntimePool(fnName)
	}
}

// activateApp clears a previous removal mark for fnName so a
// removed-then-recreated app warms again, and un-retires exactly the
// app's own image so a same-image recreation is not permanently treated as
// retired. Clearing the mark, un-retiring the image, and detaching a
// removal-draining pool all happen in one critical section, so:
//   - a concurrent acquire after this point sees the app active;
//   - a removal that already began cannot leave a stale removing pool in the
//     map to permanently serve errPoolClosed (the pool is detached; the next
//     acquire builds a fresh one with the current capacity);
//   - only fnName's own image is touched. A foreign image reference is never
//     cleared, so a stale request for another app's image cannot be
//     un-retired.
//
// image is the image Prepare resolved for this app ("" means "no image to
// un-retire", e.g. a caller that only wants the removal mark cleared).
func (cc *containerCache) activateApp(fnName, image string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.lazyInit()
	delete(cc.removedApps, fnName)
	if image == "" {
		// No image to un-retire: only the removal mark is cleared.
	} else if name, ok := appNameFromImage(image); ok && name == fnName {
		// The image is fnName's own: its same-image recreation must warm.
		// Retirements of OTHER versions of this app stay in place so a
		// stale old-version request can never supersede the active version.
		// Every content identity sharing this reference is cleared too, so a
		// reverted content address warms again.
		clearImageRetirement(cc.retiredImages, image)
	} else {
		// A foreign or unparseable reference is never un-retired, so a caller
		// cannot clear another app's (or an arbitrary) retirement.
		image = ""
	}
	if p, ok := cc.pools[fnName]; ok {
		if p.activate(image) {
			// Detach the draining pool: a later acquire must build a fresh pool
			// (with the app's current capacity) rather than be served the
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
func (p *appPool) activate(image string) (detach bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.removing {
		return true
	}
	if image != "" {
		clearImageRetirement(p.retiredImages, image)
	}
	return false
}

// beginRemoveLocked marks the pool removed and collects every container that
// must be torn down immediately: active and draining idle containers, while
// active and draining busy containers are marked retired (discarded on release)
// and transient throwaways are marked retired. It is idempotent and returns the
// discards to perform outside the pool lock. It must be called with p.mu held
// (and, for the removeApp path, with cc.mu held so the mark is linearized
// against activation).
func (p *appPool) beginRemoveLocked() []*pooledContainer {
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
					pc.retireReason = reasonAppRemove
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
			pc.retireReason = reasonAppRemove
		}
	}
	p.pruneDrainingLocked()
	// No gauge publish here: p.removing is now set, so the pool is a metric
	// tombstone (publishPoolGaugesLocked is a no-op). The caller
	// (containerCache.removeApp) deletes the app's runtime-pool series
	// in this same critical section, which is the removal-visible transition.
	return discard
}

// teardownDiscards discards the containers beginRemoveLocked removed from the
// pool's ownership. It runs outside the pool lock; dead containers are still
// recorded (their own path already tore them down) but not discarded again.
func (p *appPool) teardownDiscards(discard []*pooledContainer) {
	for _, pc := range discard {
		p.discardContainer(pc, reasonAppRemove)
	}
}

// maybeDeletePool deletes p from the cache map when it has been removed and is
// now empty, so a removed app's state does not linger and a later
// reactivation starts from a clean pool. It is a no-op for a pool that is not
// removing, has work in flight, or has already been replaced in the map. Lock
// order is cache.mu -> pool.mu (the pool lock must not be held by the caller).
func (cc *containerCache) maybeDeletePool(fnName string, p *appPool) {
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
func (p *appPool) pruneDrainingLocked() {
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
func (p *appPool) isEmptyLocked() bool {
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

// evictIdle runs one maintenance pass over every pool with a detached bound. It
// is the unbounded convenience form of evictIdleContext used by tests and direct
// callers; the Manager's maintenance loop uses evictIdleContext(manager
// lifecycle) so shutdown cancellation makes an in-flight pass return promptly.
func (cc *containerCache) evictIdle() {
	cc.evictIdleContext(context.Background())
}

// evictIdleContext is evictIdle under a caller-supplied bound. It always reaps
// idle containers that are already dead (a container tore itself down while idle,
// so its slot must be dropped and its gauges republished), plus retired idle
// containers; when timeout is positive it additionally evicts healthy idle
// containers that have been idle at least the timeout. It is the only
// age-eviction path and is driven by Manager's single ticker (never a ticker or
// goroutine per container). Ownership is removed from the idle list under the
// pool lock and the actual teardown runs outside it through
// discardContainerContext, so a context-aware container (the production
// executionContainer) observes ctx — the manager lifecycle on the maintenance
// path — and a cancelled lifecycle makes the pass return promptly (each
// teardown is still capped by containerOpTimeout). A failed teardown is never
// reinserted (the container has already lost its idle slot), so a cleanup
// failure can only leak the container, never resurrect it.
func (cc *containerCache) evictIdleContext(ctx context.Context) {
	now := time.Now()
	if cc.now != nil {
		now = cc.now()
	}
	cc.mu.Lock()
	pools := make([]*appPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()
	for _, p := range pools {
		p.evictIdle(ctx, now, cc.idleTimeout)
	}
}

// evictIdle reaps dead/retired idle containers from this pool and, when timeout
// is positive, evicts healthy idle containers older than timeout. Dead idle
// containers are reaped even when age eviction is disabled (a non-positive
// timeout), so a self-terminated container can never leave a stale idle gauge;
// the pool's authoritative counts are republished whenever anything is dropped.
// Teardown runs on ctx so a shutdown-cancelled maintenance pass returns
// promptly.
func (p *appPool) evictIdle(ctx context.Context, now time.Time, timeout time.Duration) {
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
		p.discardContainerContext(ctx, pc, reason)
	}
}

// close discards EVERY pooled container with reason "shutdown", wakes all
// waiters, and prevents further acquires. Idle containers are discarded
// immediately; busy (active or draining) and transient containers are marked
// retired AND discarded, so a later release is a no-op (discard is idempotent)
// and no container survives shutdown even if a lease is never returned. It is
// the graceful-shutdown hook the worker's defer Manager.Close() flows into.
// It is the unbounded convenience form of closeContext; production shutdown
// uses closeContext so the teardown observes the shutdown step's bound.
func (cc *containerCache) close() {
	cc.closeContext(context.Background())
}

// closeContext is close with a caller-supplied bound. It marks the cache and
// every pool closed under their locks (so no new acquire can start a container),
// then tears down every pooled container THROUGH A BOUNDED WORKER POOL: a large
// warm pool is discarded in parallel batches of containerShutdownConcurrency
// rather than O(N) serial 10s delays. Each teardown is context-aware (see
// discardOnContext), so when ctx expires the Docker calls observe it; the method
// returns once every container is torn down or ctx is done, whichever first.
// The Docker client is deliberately closed by Manager only AFTER this returns.
func (cc *containerCache) closeContext(ctx context.Context) {
	cc.mu.Lock()
	cc.closed = true
	cc.lazyInit()
	pools := make([]*appPool, 0, len(cc.pools))
	for _, p := range cc.pools {
		pools = append(pools, p)
	}
	cc.mu.Unlock()

	var all []pooledDiscard
	for _, p := range pools {
		for _, pc := range p.beginClose() {
			all = append(all, pooledDiscard{pool: p, pc: pc})
		}
	}
	discardAllContext(ctx, all, reasonShutdown)
}

// pooledDiscard pairs a container with its owning pool, so the bounded parallel
// teardown can still record each discard metric against the right app.
type pooledDiscard struct {
	pool *appPool
	pc   *pooledContainer
}

// containerShutdownConcurrency bounds how many container teardowns run at once
// during closeContext. It is an internal constant (no user knob): high enough to
// keep a large warm pool's shutdown from taking O(N) serial kill/remove calls,
// low enough to bound the burst of Docker requests at shutdown.
const containerShutdownConcurrency = 8

// discardAllContext tears down every item through a BOUNDED worker pool of at
// most containerShutdownConcurrency workers (never a goroutine per container),
// and returns only after every worker has exited. Each teardown runs
// discardContainerContext, which uses discardOnContext (so a context-aware
// container observes ctx) and records exactly one discard metric. When ctx is
// done the feeder stops queuing and the workers drain what they are already
// running (bounded by that one context-aware operation each), so the call
// returns promptly AND no worker outlives it — which is what lets Manager close
// the Docker client only after every teardown has concluded. A caller that truly
// cannot wait relies on the shutdown registry's outer per-step timeout (the step
// runs in its own goroutine), not on this app abandoning its workers.
func discardAllContext(ctx context.Context, items []pooledDiscard, reason string) {
	if len(items) == 0 {
		return
	}
	workers := containerShutdownConcurrency
	if len(items) < workers {
		workers = len(items)
	}
	work := make(chan pooledDiscard)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for it := range work {
				it.pool.discardContainerContext(ctx, it.pc, reason)
			}
		}()
	}
	// Feed every item, stopping early if ctx is done. Closing work when the
	// feeder returns is what lets the workers exit.
	go func() {
		defer close(work)
		for _, it := range items {
			select {
			case work <- it:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
}

// beginClose marks the pool closed, wakes all waiters, and returns every
// container it owns (idle and busy of the active and draining generations, plus
// transients) for the caller to tear down outside the pool lock. It is
// idempotent: a second call returns nil. The caller owns the teardown, so
// closeContext can run it in a bounded parallel worker pool while the serial
// close convenience form tears down in place.
func (p *appPool) beginClose() []*pooledContainer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
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
	return all
}

// close closes the pool and tears down every container it owns with reason
// "shutdown", serially with detached per-container bounds. It is the convenience
// form used by tests and callers without a shutdown bound; production shutdown
// uses closeContext.
func (p *appPool) close() {
	for _, pc := range p.beginClose() {
		p.discardContainer(pc, reasonShutdown)
	}
}

// poolDiscardReason picks the teardown reason for an unregistered container
// whose pool was removed mid-start: a removed app is reported as
// app_removed, otherwise the pool is shutting down. It is a pure helper
// for the acquire unwind path.
func poolDiscardReason(removing bool) string {
	if removing {
		return reasonAppRemove
	}
	return reasonShutdown
}

// signalLocked wakes every waiter. It must be called with p.mu held. A pool
// that is closed has nobody left to wake (its notify was closed by close and
// never replaced), so signalling is a no-op then.
func (p *appPool) signalLocked() {
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
func (p *appPool) release(pc *pooledContainer) {
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
			reason = reasonAppRemove
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
		drop, reason = true, reasonAppRemove
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
	pool *appPool
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

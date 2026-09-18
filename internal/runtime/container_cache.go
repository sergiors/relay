package runtime

import (
	"context"
	"sync"
	"sync/atomic"
)

// reusableContainer is the seam the per-function container cache programs: the
// production implementation is *executionContainer; tests inject fake
// containers to exercise the cache's get-or-create, serialization, and
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

// containerCache owns execution containers, one per FUNCTION NAME (a function
// never touches another function's container). It serializes get-or-create and
// Invoke per entry (phase-1 serialization: two concurrent invocations for the
// same function queue up — explicitly acceptable at this phase, and the
// per-function serialization is what makes the sequential request/response
// frames well-defined on a shared stdin/stdout pair) and exposes image-based
// invalidation for image retirement.
//
// Locking: a manager-wide mu guards the map itself; each entry carries its own
// mutex held across Start and Invoke. The manager-wide mu is never held during
// create/Invoke.
type containerCache struct {
	// mu guards entries.
	mu      sync.Mutex
	entries map[string]*cacheEntry
}

// cacheEntry is one function's current container plus the image it was created
// from. entry.mu serializes get-or-create/Invoke/TryLock invalidation for that
// function.
type cacheEntry struct {
	mu sync.Mutex
	// image is the image the current container was created from. Written only
	// under mu (in execute), but READ by invalidateImage without mu (TryLock
	// must not block), so it is an atomic pointer.
	image atomic.Pointer[string]
	c     reusableContainer
	// pendingInvalidate records an invalidation that hit a busy entry (an
	// in-flight Invoke); it is drained by the next Execute. It is written by
	// invalidateImage without the entry lock, so it is atomic.
	pendingInvalidate atomicPtrString
}

// atomicPtrString is a nilable atomic string (nil == ""): methods because the
// field is a defined type, so writes never race the lockless readers.
type atomicPtrString struct{ v atomic.Pointer[string] }

func (p *atomicPtrString) set(s string) {
	if s == "" {
		p.v.Store(nil)
		return
	}
	p.v.Store(&s)
}

func (p *atomicPtrString) get() string {
	if s := p.v.Load(); s != nil {
		return *s
	}
	return ""
}

// matchesImage atomically reports whether the current container's image is
// img (readable without the entry lock).
func (e *cacheEntry) matchesImage(img string) bool {
	cur := e.image.Load()
	return cur != nil && *cur == img
}

func newContainerCache() *containerCache {
	return &containerCache{entries: map[string]*cacheEntry{}}
}

// lazyInit ensures the entries map exists (a zero-valued Manager from tests
// must still be Close-able). It must be called with cc.mu held.
func (cc *containerCache) lazyInit() {
	if cc.entries == nil {
		cc.entries = map[string]*cacheEntry{}
	}
}

// execute runs one invocation through the cached (or freshly created)
// container for fnName. start creates a fresh container when the cache has
// none (or the old one was discarded/replaced); it returns the caller's error
// verbatim on failure. Errors that poisoned the container clear the entry so
// the next call starts fresh; handler errors keep the container.
func (cc *containerCache) execute(ctx context.Context, fnName, image string, start func() (reusableContainer, error), handler string, eventJSON []byte, env map[string]string) error {
	entry := cc.entryFor(fnName)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// A container discarded by its own paths (timeout, process exit, protocol
	// error) is dead by the time we get here: drop it and start fresh.
	if entry.c != nil && entry.c.dead() {
		entry.c = nil
	}

	// Drained pending invalidation and image-change check: either way the old
	// container is no longer the right one for this image.
	if entry.pendingInvalidate.get() != "" {
		entry.pendingInvalidate.set("")
		if entry.c != nil {
			entry.c.discard("image_changed")
			entry.c = nil
		}
	}
	if entry.c != nil && !entry.matchesImage(image) {
		// The image moved (new version prepared for this function). Kick even
		// a healthy container: it must not keep serving a retired version.
		entry.c.discard("image_changed")
		entry.c = nil
	}

	if entry.c == nil {
		c, err := start()
		if err != nil {
			return err
		}
		entry.c = c
		img := image
		entry.image.Store(&img)
	}

	err := entry.c.Invoke(ctx, handler, eventJSON, env)
	if err != nil && entry.c.dead() {
		entry.c = nil
	}
	// Drain a pending invalidation that queued while the Invoke was holding
	// the lock ("deferred to the post-Invoke path").
	if entry.pendingInvalidate.get() != "" {
		if entry.c != nil {
			entry.c.discard("image_changed")
			entry.c = nil
		}
		entry.pendingInvalidate.set("")
	}
	return err
}

// entryFor returns (creating) the cache entry for fnName. The manager-wide mu
// is held for the map access (including lazy init).
func (cc *containerCache) entryFor(fnName string) *cacheEntry {
	cc.mu.Lock()
	cc.lazyInit()
	defer cc.mu.Unlock()
	if e, ok := cc.entries[fnName]; ok {
		return e
	}
	e := &cacheEntry{}
	cc.entries[fnName] = e
	return e
}

// peek returns the entry for fnName without creating one. It is nil-safe reads
// for InvalidateImage/close. The manager-wide mu is held for the map access
// (including lazy init).
func (cc *containerCache) peek(fnName string) *cacheEntry {
	cc.mu.Lock()
	cc.lazyInit()
	defer cc.mu.Unlock()
	return cc.entries[fnName]
}

// invalidateImage discards any cached container whose image matches, without
// blocking: an entry mid-Invoke is SKIPPED (the discard is recorded as pending
// and drained right after the in-flight Invoke completes in execute). It must
// never block image retirement.
func (cc *containerCache) invalidateImage(image string) {
	cc.lazyInit()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, entry := range cc.entries {
		if !entry.matchesImage(image) {
			continue
		}
		if !entry.mu.TryLock() {
			// Busy: an invocation is in flight; defer the discard to the
			// post-Invoke path.
			entry.pendingInvalidate.set(image)
			continue
		}
		if entry.c == nil || entry.c.dead() {
			entry.mu.Unlock()
			continue
		}
		entry.c.discard("image_changed")
		entry.c = nil
		entry.pendingInvalidate.set("")
		entry.mu.Unlock()
	}
}

// close discards EVERY cached container with reason "shutdown". It takes each
// entry lock (blocking is correct during shutdown) and is the graceful
// shutdown hook the worker's defer Manager.Close() flows into.
func (cc *containerCache) close() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, entry := range cc.entries {
		entry.mu.Lock()
		if entry.c != nil && !entry.c.dead() {
			entry.c.discard("shutdown")
		}
		entry.c = nil
		entry.mu.Unlock()
	}
}

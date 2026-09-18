package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

// fakeClock is the injectable clock seam used by eviction tests. It is safe for
// concurrent use so a maintenance loop and a test can read it under -race.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newManagedCache returns a cache with the given idle timeout and clock, so
// evictIdle's decisions are fully deterministic.
func newManagedCache(clk *fakeClock, timeout time.Duration) (*containerCache, *fakeFactory) {
	cc := newContainerCache()
	cc.idleTimeout = timeout
	if clk != nil {
		cc.now = clk.Now
	}
	return cc, &fakeFactory{}
}

// newTestManager builds a Manager without Docker (direct struct construction),
// wires a cache with the given clock/timeout, and starts the real maintenance
// loop. Cleanup closes it. Only the cache/maintenance parts of Manager are
// exercised; container execution goes through the cache directly.
func newTestManager(t *testing.T, clk *fakeClock, timeout, interval time.Duration) *Manager {
	t.Helper()
	m := &Manager{
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		done:      make(chan struct{}),
		maintDone: make(chan struct{}),
	}
	m.containers = newContainerCache()
	m.containers.idleTimeout = timeout
	if clk != nil {
		m.containers.now = clk.Now
	}
	m.startMaintenance(interval)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestIdleEvictionAfterTimeout(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, 5*time.Minute)
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()

	// Just under the timeout: still kept.
	clk.Advance(5*time.Minute - time.Second)
	cc.evictIdle()
	if c1.dead() {
		t.Fatal("idle container evicted before the timeout")
	}

	// At the timeout: evicted with the idle_timeout reason.
	clk.Advance(time.Second)
	cc.evictIdle()
	if got := c1.reasons(); len(got) != 1 || got[0] != reasonIdleTimeout {
		t.Fatalf("idle discard reasons = %v, want [%s]", got, reasonIdleTimeout)
	}

	// The next acquire starts a fresh container (the evicted one is not reused).
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after eviction: %v", err)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 (evicted container must not be reused)", ff.count())
	}
}

// TestIdleEvictionOnlyHealthyIdle proves a BUSY container is never evicted by
// the sweep, no matter how long the invocation runs, and a DEAD idle container
// is dropped from the idle list without an eviction discard (its own path
// already tore it down).
func TestIdleEvictionOnlyHealthyIdle(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	busy := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	idle := &fakeContainer{}
	built := 0
	ff.build = func() *fakeContainer {
		built++
		if built == 1 {
			return busy
		}
		return idle
	}
	busyDone := make(chan error, 1)
	go func() {
		busyDone <- cc.execute(context.Background(), "fn-a", "img-1", 2, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered
	if err := run(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("seed idle: %v", err)
	}

	clk.Advance(time.Hour)
	cc.evictIdle()
	if idle.dead() != true {
		t.Fatal("idle container should have been evicted")
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy container evicted mid-invocation: %v", got)
	}

	close(busy.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("healthy busy container must be kept after release, discards = %v", got)
	}
}

// TestIdleEvictionCleanupFailureNeverReinserts proves that when a container's
// cleanup fails, the eviction still removes it from the idle list and never
// reinserts it: a subsequent acquire starts a fresh container rather than
// leasing the failed one, and cleanup is not retried by the sweep.
func TestIdleEvictionCleanupFailureNeverReinserts(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()
	c1.mu.Lock()
	c1.discardFails = true
	c1.mu.Unlock()

	clk.Advance(2 * time.Minute)
	cc.evictIdle()
	if c1.dead() {
		t.Fatal("failed cleanup must not mark the container dead")
	}
	if got := c1.attempts(); got != 1 {
		t.Fatalf("discard attempts = %d, want 1 (cleanup is not retried)", got)
	}

	// The failed container lost its idle slot: a new acquire must NOT lease it.
	c1.mu.Lock()
	c1.discardFails = false
	c1.mu.Unlock()
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after failed eviction: %v", err)
	}
	if got := c1.calls(); got != 1 {
		t.Fatalf("failed-evicted container invocations = %d, want 1 (never re-leased)", got)
	}
	if ff.count() != 2 {
		t.Fatalf("creations = %d, want 2 (fresh container after eviction)", ff.count())
	}

	// A second sweep must not touch the failed container again.
	clk.Advance(2 * time.Minute)
	cc.evictIdle()
	if got := c1.attempts(); got != 1 {
		t.Fatalf("discard attempts after second sweep = %d, want still 1", got)
	}
}

// TestIdleEvictionDisabledForNonPositiveTimeout pins that a non-positive
// configured timeout disables eviction entirely (safety net; config.Load
// rejects non-positive values before they reach a Manager).
func TestIdleEvictionDisabledForNonPositiveTimeout(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, 0)
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()
	clk.Advance(24 * time.Hour)
	cc.evictIdle()
	if c1.dead() {
		t.Fatal("eviction must be disabled for a non-positive timeout")
	}
}

// TestManagerMaintenanceLoopEvicts proves the Manager's single loop is what
// drives eviction end to end (not a test-only direct evictIdle call): an idle
// container is evicted once the injected clock passes the timeout.
func TestManagerMaintenanceLoopEvicts(t *testing.T) {
	clk := newFakeClock()
	m := newTestManager(t, clk, 50*time.Millisecond, 5*time.Millisecond)
	ff := &fakeFactory{}
	if err := run(t, m.containers, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()

	clk.Advance(100 * time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for !c1.dead() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !c1.dead() {
		t.Fatal("maintenance loop did not evict the idle container")
	}
	if got := c1.reasons(); len(got) != 1 || got[0] != reasonIdleTimeout {
		t.Fatalf("discard reasons = %v, want [%s]", got, reasonIdleTimeout)
	}
}

// TestManagerCloseStopsMaintenance proves Close joins the maintenance loop
// (maintDone closes) and is idempotent.
func TestManagerCloseStopsMaintenance(t *testing.T) {
	clk := newFakeClock()
	m := newTestManager(t, clk, time.Minute, time.Hour)
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-m.maintDone:
	default:
		t.Fatal("maintDone not closed after Close")
	}
	// Second Close must not panic or block.
	if err := m.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestMaintenanceIntervalBounds(t *testing.T) {
	cases := []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{5 * time.Minute, time.Minute}, // half capped at 1m
		{10 * time.Second, 5 * time.Second},
		{2 * time.Millisecond, 10 * time.Millisecond}, // floored
	}
	for _, tc := range cases {
		if got := maintenanceInterval(tc.timeout); got != tc.want {
			t.Errorf("maintenanceInterval(%v) = %v, want %v", tc.timeout, got, tc.want)
		}
	}
}

// TestResolveManagerOptions pins the option contract NewManager relies on: the
// three-argument caller gets the package default; an explicit positive timeout
// is honored; a non-positive timeout falls back to the default rather than
// disabling eviction; and the unexported test clock passthrough survives.
func TestResolveManagerOptions(t *testing.T) {
	if got := resolveManagerOptions(nil).idleTimeout; got != DefaultWarmContainerIdleTimeout {
		t.Fatalf("default idleTimeout = %v, want %v", got, DefaultWarmContainerIdleTimeout)
	}
	if got := resolveManagerOptions([]ManagerOption{WithWarmContainerIdleTimeout(90 * time.Second)}).idleTimeout; got != 90*time.Second {
		t.Fatalf("explicit idleTimeout = %v, want 90s", got)
	}
	for _, bad := range []time.Duration{0, -time.Second} {
		if got := resolveManagerOptions([]ManagerOption{WithWarmContainerIdleTimeout(bad)}).idleTimeout; got != DefaultWarmContainerIdleTimeout {
			t.Errorf("non-positive timeout %v resolved to %v, want default %v", bad, got, DefaultWarmContainerIdleTimeout)
		}
	}
	clk := newFakeClock()
	resolved := resolveManagerOptions([]ManagerOption{withClock(clk.Now), nil})
	if resolved.now == nil || !resolved.now().Equal(clk.Now()) {
		t.Fatal("clock option was not applied")
	}
}

// TestBackwardCompatibleNilOption pins that a nil option in the variadic list is
// ignored (a nil option function must not panic NewManager's resolution).
func TestBackwardCompatibleNilOption(t *testing.T) {
	resolved := resolveManagerOptions([]ManagerOption{nil, WithWarmContainerIdleTimeout(time.Minute), nil})
	if resolved.idleTimeout != time.Minute {
		t.Fatalf("idleTimeout = %v, want 1m with nil options ignored", resolved.idleTimeout)
	}
}

// TestGenerationDrainingRemovedWhenEmpty proves the internal generation model:
// a superseded busy container moves the old generation to draining, and the
// draining generation is removed once its last busy container releases.
func TestGenerationDrainingRemovedWhenEmpty(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	oldC := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ff.build = func() *fakeContainer { return oldC }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 2, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-oldC.entered

	// Transition to img-2 while oldC is busy: old generation drains.
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := run(t, cc, ff, "fn-a", "img-2", 2, "h"); err != nil {
		t.Fatalf("execute img-2: %v", err)
	}
	p := cc.poolFor("fn-a", 2)
	p.mu.Lock()
	if p.active == nil || p.active.image != "img-2" {
		p.mu.Unlock()
		t.Fatalf("active generation = %+v, want img-2", p.active)
	}
	if len(p.draining) != 1 || p.draining[0].image != "img-1" {
		p.mu.Unlock()
		t.Fatalf("draining = %+v, want one img-1 generation", p.draining)
	}
	p.mu.Unlock()

	close(oldC.release)
	if err := <-done; err != nil {
		t.Fatalf("old execute: %v", err)
	}
	if got := oldC.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("old container discards = %v, want [%s]", got, reasonImageChanged)
	}

	p.mu.Lock()
	nd := len(p.draining)
	p.mu.Unlock()
	if nd != 0 {
		t.Fatalf("draining generations = %d, want 0 after the last busy container released", nd)
	}
}

// TestGenerationNoNewOldGeneration proves a stale acquire for a superseded
// image is served as a transient and never creates a new generation for it.
func TestGenerationNoNewOldGeneration(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if err := run(t, cc, ff, "fn-a", "img-2", 1, "h"); err != nil {
		t.Fatalf("v2: %v", err)
	}
	// Stale img-1 acquire: transient, never pooled.
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("stale v1: %v", err)
	}
	c := ff.lastContainer()
	if got := c.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("stale container discards = %v, want [%s]", got, reasonImageChanged)
	}
	p := cc.poolFor("fn-a", 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active == nil || p.active.image != "img-2" {
		t.Fatalf("active generation = %+v, want img-2", p.active)
	}
	for _, g := range p.draining {
		if g.image == "img-1" {
			t.Fatal("a stale acquire created an img-1 generation")
		}
	}
}

// TestFunctionRemovalDiscardsIdleAndBusy proves RemoveFunction discards idle
// containers immediately, retires busy ones until release, refuses new
// acquires, and removes the pool state once empty.
func TestFunctionRemovalDiscardsIdleAndBusy(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	// fn-a: one busy container (created first, so the pool is empty and the
	// acquire lazily starts it), then one idle container.
	busy := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 2, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	idle := &fakeContainer{}
	ff.build = func() *fakeContainer { return idle }
	if err := run(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("idle seed: %v", err)
	}

	// fn-b must be untouched by fn-a's removal. Reset the factory so fn-b gets
	// its own container rather than the shared idle pointer above.
	ff.build = nil
	if err := run(t, cc, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("fn-b seed: %v", err)
	}

	cc.removeFunction("fn-a")
	if got := idle.reasons(); len(got) != 1 || got[0] != reasonFunctionRemove {
		t.Fatalf("idle removal discards = %v, want [%s]", got, reasonFunctionRemove)
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy container discarded mid-invocation: %v", got)
	}

	// A new acquire for the removed function fails immediately.
	if err := run(t, cc, ff, "fn-a", "img-1", 2, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}

	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonFunctionRemove {
		t.Fatalf("busy removal discards = %v, want [%s]", got, reasonFunctionRemove)
	}

	// fn-a's state is gone; fn-b's is intact and reusable.
	cc.mu.Lock()
	_, aExists := cc.pools["fn-a"]
	cc.mu.Unlock()
	if aExists {
		t.Fatal("removed function's empty pool must be deleted from the cache")
	}
	if err := run(t, cc, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("fn-b execute after fn-a removal: %v", err)
	}
}

// TestFunctionRemovalLateReleaseCannotRecreate proves a late release after
// removal cannot recreate pool state, and a subsequent acquire still fails.
func TestFunctionRemovalLateReleaseCannotRecreate(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	busy := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	cc.removeFunction("fn-a")
	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}

	cc.mu.Lock()
	_, exists := cc.pools["fn-a"]
	removed := cc.removedFunctions["fn-a"]
	cc.mu.Unlock()
	if exists {
		t.Fatal("removed function's empty pool must be deleted from the cache")
	}
	if !removed {
		t.Fatal("removed function must stay marked removed after its pool drains")
	}
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after late release = %v, want errPoolClosed", err)
	}
}

// TestFunctionReactivateAfterRemoval proves Prepare's activation clears the
// removal mark so a removed-then-recreated function warms again.
func TestFunctionReactivateAfterRemoval(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.removeFunction("fn-a")
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}

	cc.activateFunction("fn-a", "img-1")
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("acquire after reactivation: %v", err)
	}
}

// TestFunctionSameImageRecreationWarms is the deterministic regression for the
// reported bug: a function removed and then recreated with the SAME image must
// WARM, not be permanently treated as retired. It models the real sequence:
// RemoveFunction discards the pool, the runner retires every function image
// (InvalidateImage), and a later Prepare/activate for the exact same image must
// un-retire it so the recreated function reuses a warm container instead of
// starting a throwaway per invocation.
func TestFunctionSameImageRecreationWarms(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	// Use a real Relay image reference: activation's scoping guard only accepts
	// the function's own relay-fn-<name>:<tag> reference.
	const image = "relay-fn-fn-a:abc123"
	// Seed a warm container for fn-a, then remove the function and retire its
	// image exactly as the worker's removal hook does (manager then runner).
	if err := run(t, cc, ff, "fn-a", image, 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.removeFunction("fn-a")
	cc.invalidateImage(image)

	// A stale acquire during removal must fail, never warm.
	if err := run(t, cc, ff, "fn-a", image, 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire while removed = %v, want errPoolClosed", err)
	}

	// Function is recreated with the same content/image. Activation must clear
	// both the removal mark and the image retirement for this function's image.
	cc.activateFunction("fn-a", image)

	before := ff.count()
	if err := run(t, cc, ff, "fn-a", image, 1, "h"); err != nil {
		t.Fatalf("first acquire after recreation: %v", err)
	}
	c := ff.lastContainer()
	if got := c.reasons(); len(got) != 0 {
		t.Fatalf("recreated same-image container discarded as retired: %v", got)
	}

	// The recreated function must be WARM: a second invocation reuses the
	// container rather than starting a fresh throwaway.
	if err := run(t, cc, ff, "fn-a", image, 1, "h"); err != nil {
		t.Fatalf("second acquire after recreation: %v", err)
	}
	if ff.count() != before+1 {
		t.Fatalf("creations = %d, want %d (same-image recreation must warm)", ff.count(), before+1)
	}
	if got := c.calls(); got != 2 {
		t.Fatalf("container invocations = %d, want 2 (warm reuse)", got)
	}
}

// TestFunctionRecreationOnlyUnretiresOwnImage proves activation is scoped: a
// foreign (or untagged) image reference can never be un-retired, so a stale
// request cannot use reactivation to tear down another function's current
// version.
func TestFunctionRecreationOnlyUnretiresOwnImage(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	const foreignImage = "relay-fn-fn-b:abc123"
	// fn-b is warm on its image, which is then invalidated (retired).
	if err := run(t, cc, ff, "fn-b", foreignImage, 1, "h"); err != nil {
		t.Fatalf("fn-b seed: %v", err)
	}
	cc.invalidateImage(foreignImage)

	// Activating fn-a with fn-b's image must NOT clear fn-b's retirement: the
	// name encoded in the reference is fn-b, not fn-a.
	cc.activateFunction("fn-a", foreignImage)

	if err := run(t, cc, ff, "fn-b", foreignImage, 1, "h"); err != nil {
		t.Fatalf("fn-b acquire: %v", err)
	}
	if got := ff.lastContainer().reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("fn-b container discards = %v, want [%s] (foreign activation must not un-retire)", got, reasonImageChanged)
	}
}

// TestFunctionRemovalDuringInFlightStart proves the removal linearization at
// the create boundary: a removal that lands while an acquire is blocked in the
// start factory must prevent that container from being leased or pooled. The
// in-flight start completes into the removal path (discarded, errPoolClosed),
// and the pool is then deleted once the reservation is gone.
func TestFunctionRemovalDuringInFlightStart(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	started := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	ff.build = func() *fakeContainer {
		once.Do(func() {
			close(started)
			<-unblock
		})
		return &fakeContainer{}
	}

	type result struct {
		lease *containerLease
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		l, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, ff.start())
		resCh <- result{l, err}
	}()
	<-started

	// Removal runs while the lazy start is in flight.
	cc.removeFunction("fn-a")
	close(unblock)

	res := <-resCh
	if res.lease != nil {
		res.lease.release()
		t.Fatal("acquire leased a container created after the removal request")
	}
	if !errors.Is(res.err, errPoolClosed) {
		t.Fatalf("in-flight acquire after removal = %v, want errPoolClosed", res.err)
	}
	if got := ff.lastContainer().reasons(); len(got) != 1 || got[0] != reasonFunctionRemove {
		t.Fatalf("in-flight-start container discards = %v, want [%s]", got, reasonFunctionRemove)
	}

	cc.mu.Lock()
	_, exists := cc.pools["fn-a"]
	cc.mu.Unlock()
	if exists {
		t.Fatal("removed function's empty pool must be deleted after the in-flight start unwinds")
	}

	// A later acquire still fails (removal not lifted).
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}
}

// poolExists reports whether the cache still holds state for fnName.
func poolExists(cc *containerCache, fnName string) bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	_, ok := cc.pools[fnName]
	return ok
}

// TestFunctionRemovalDuringInFlightStartFailureDeletesPool is the regression
// for the review finding: a removal that races a REGULAR lazy start which then
// FAILS must still delete the now-empty removing pool. The reservation is the
// only thing that kept the pool non-empty when removeFunction ran, so rolling
// it back is exactly the point at which the pool becomes empty and must leave
// the cache. Before the fix this path decremented `creating` and signalled but
// never called maybeDeletePool, leaving a stale removing pool in the map.
func TestFunctionRemovalDuringInFlightStartFailureDeletesPool(t *testing.T) {
	clk := newFakeClock()
	cc, _ := newManagedCache(clk, time.Minute)

	started := make(chan struct{})
	unblock := make(chan struct{})
	start := func() (reusableContainer, error) {
		close(started)
		<-unblock
		return nil, errors.New("start boom")
	}

	resCh := make(chan error, 1)
	go func() {
		_, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, start)
		resCh <- err
	}()
	<-started

	// Removal runs while the lazy start is in flight, holding the reservation.
	cc.removeFunction("fn-a")
	if !poolExists(cc, "fn-a") {
		t.Fatal("pool must still exist while the in-flight start holds a reservation")
	}
	close(unblock)

	if err := <-resCh; err == nil || errors.Is(err, errPoolClosed) {
		t.Fatalf("failed in-flight start = %v, want the start error (not errPoolClosed)", err)
	}
	if poolExists(cc, "fn-a") {
		t.Fatal("removed function's empty pool must be deleted after a failed in-flight start")
	}

	// A later acquire still fails (removal not lifted).
	if err := run(t, cc, &fakeFactory{}, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}
}

// TestFunctionRemovalDuringInFlightStartPanicDeletesPool proves the panic-unwind
// variant of the finding for a REGULAR lazy start: a start factory that panics
// after a removal raced it must roll back the reservation AND delete the empty
// removing pool, while still propagating the panic.
func TestFunctionRemovalDuringInFlightStartPanicDeletesPool(t *testing.T) {
	clk := newFakeClock()
	cc, _ := newManagedCache(clk, time.Minute)

	started := make(chan struct{})
	unblock := make(chan struct{})
	start := func() (reusableContainer, error) {
		close(started)
		<-unblock
		panic("factory boom")
	}

	resCh := make(chan any, 1)
	go func() {
		defer func() { resCh <- recover() }()
		_, _ = cc.acquire(context.Background(), "fn-a", "img-1", 1, start)
	}()
	<-started

	cc.removeFunction("fn-a")
	close(unblock)

	if r := <-resCh; r == nil {
		t.Fatal("expected the factory panic to propagate")
	}
	if poolExists(cc, "fn-a") {
		t.Fatal("removed function's empty pool must be deleted after a panicking in-flight start")
	}

	if err := run(t, cc, &fakeFactory{}, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}
}

// TestFunctionRemovalDuringInFlightTransientStartFailureDeletesPool is the
// TRANSIENT variant of the finding: a stale request for a retired image starts a
// throwaway container; a removal races that start and the start then fails. The
// transientCreating reservation is rolled back and the empty removing pool must
// be deleted from the cache.
func TestFunctionRemovalDuringInFlightTransientStartFailureDeletesPool(t *testing.T) {
	clk := newFakeClock()
	cc, _ := newManagedCache(clk, time.Minute)

	// Retire the image so the acquire takes the transient (throwaway) path.
	cc.invalidateImage("img-1")

	started := make(chan struct{})
	unblock := make(chan struct{})
	start := func() (reusableContainer, error) {
		close(started)
		<-unblock
		return nil, errors.New("transient start boom")
	}

	resCh := make(chan error, 1)
	go func() {
		_, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, start)
		resCh <- err
	}()
	<-started

	cc.removeFunction("fn-a")
	if !poolExists(cc, "fn-a") {
		t.Fatal("pool must still exist while the in-flight transient holds a reservation")
	}
	close(unblock)

	if err := <-resCh; err == nil || errors.Is(err, errPoolClosed) {
		t.Fatalf("failed transient in-flight start = %v, want the start error (not errPoolClosed)", err)
	}
	if poolExists(cc, "fn-a") {
		t.Fatal("removed function's empty pool must be deleted after a failed in-flight transient start")
	}

	if err := run(t, cc, &fakeFactory{}, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}
}

// TestFunctionRemovalDuringInFlightTransientStartPanicDeletesPool proves the
// panic-unwind variant for the TRANSIENT path: the transientCreating
// reservation is rolled back, the empty removing pool is deleted, and the panic
// still propagates.
func TestFunctionRemovalDuringInFlightTransientStartPanicDeletesPool(t *testing.T) {
	clk := newFakeClock()
	cc, _ := newManagedCache(clk, time.Minute)

	cc.invalidateImage("img-1")

	started := make(chan struct{})
	unblock := make(chan struct{})
	start := func() (reusableContainer, error) {
		close(started)
		<-unblock
		panic("transient factory boom")
	}

	resCh := make(chan any, 1)
	go func() {
		defer func() { resCh <- recover() }()
		_, _ = cc.acquire(context.Background(), "fn-a", "img-1", 1, start)
	}()
	<-started

	cc.removeFunction("fn-a")
	close(unblock)

	if r := <-resCh; r == nil {
		t.Fatal("expected the transient factory panic to propagate")
	}
	if poolExists(cc, "fn-a") {
		t.Fatal("removed function's empty pool must be deleted after a panicking in-flight transient start")
	}

	if err := run(t, cc, &fakeFactory{}, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal = %v, want errPoolClosed", err)
	}
}

// TestFunctionRemovalRequestAfterReactivationWins proves ordering: a removal
// that runs AFTER a reactivation is a later event and must take effect, while a
// removal that ran before it is undone by reactivation. This pins the
// linearization at the cache lock.
func TestFunctionRemovalRequestAfterReactivationWins(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)

	cc.removeFunction("fn-a")
	cc.activateFunction("fn-a", "")
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("acquire after remove-then-activate: %v", err)
	}

	// A subsequent removal is later and must win.
	cc.removeFunction("fn-a")
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after later removal = %v, want errPoolClosed", err)
	}
}

// TestFunctionLateReleaseCannotUndoReactivation proves the removal/reactivation
// linearization: a removal that began before reactivation must not undo it. The
// busy container is retired by the removal, then activation detaches the
// draining pool; the later release can only discard the container and must not
// re-remove or otherwise disable the reactivated function, whose next acquire
// warms normally.
func TestFunctionLateReleaseCannotUndoReactivation(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	busy := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	// Removal begins while busy is in flight; activation follows immediately.
	cc.removeFunction("fn-a")
	cc.activateFunction("fn-a", "img-1")

	// The late release of the removal-retired container must not undo the
	// activation: fn-a must warm again.
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonFunctionRemove {
		t.Fatalf("removal-retired busy discards = %v, want [%s]", got, reasonFunctionRemove)
	}
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("acquire after late release: %v", err)
	}
	// Warm again: a second acquire reuses the fresh container.
	before := ff.count()
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("second acquire after late release: %v", err)
	}
	if ff.count() != before {
		t.Fatalf("creations = %d, want %d (reactivation must survive the late release)", ff.count(), before)
	}
}

// TestPrepareFailureDoesNotReactivateRemovedFunction is the finding (3)
// regression: Manager.Prepare must only lift a function's removal after a
// SUCCESSFUL prepare. A fingerprint failure (here, a missing function dir) must
// leave the removal in place so a stale acquire cannot warm a function the
// reconciler has not actually reconciled. Prepare fails before touching Docker,
// so this runs without a daemon.
func TestPrepareFailureDoesNotReactivateRemovedFunction(t *testing.T) {
	m := &Manager{
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		containers: newContainerCache(),
	}
	m.containers.removeFunction("fn-a")

	fn := function.Function{
		Name: "fn-a",
		Dir:  filepath.Join(t.TempDir(), "does-not-exist"),
	}
	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("expected Prepare to fail for a missing function dir")
	}

	m.containers.mu.Lock()
	_, removed := m.containers.removedFunctions["fn-a"]
	if p := m.containers.pools["fn-a"]; p != nil {
		p.mu.Lock()
		removing := p.removing
		p.mu.Unlock()
		if !removing {
			m.containers.mu.Unlock()
			t.Fatal("failed Prepare must not create a live (non-removing) pool")
		}
	}
	m.containers.mu.Unlock()
	if !removed {
		t.Fatal("failed Prepare must not clear the removal mark")
	}

	ff := &fakeFactory{}
	if err := run(t, m.containers, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after failed Prepare = %v, want errPoolClosed", err)
	}
}

// TestFunctionRemovalWakesWaiter proves a blocked acquire is woken by removal
// and fails with errPoolClosed (rather than hanging).
func TestFunctionRemovalWakesWaiter(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	busy := &fakeContainer{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	waiter := make(chan error, 1)
	go func() {
		waiter <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()

	cc.removeFunction("fn-a")
	select {
	case err := <-waiter:
		if !errors.Is(err, errPoolClosed) {
			t.Fatalf("waiter error = %v, want errPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("removal did not wake the blocked acquire")
	}
	close(busy.release)
	<-done
}

// TestIndependentPoolsEvictionAndRemoval proves pools are isolated: eviction
// and removal in one function never touches another function's containers.
func TestIndependentPoolsEvictionAndRemoval(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Minute)
	for _, name := range []string{"fn-a", "fn-b"} {
		if err := run(t, cc, ff, name, "img-1", 1, "h"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	containers := ff.allContainers()
	if len(containers) != 2 {
		t.Fatalf("containers = %d, want 2", len(containers))
	}

	// Evict everything: both pools lose their idle container.
	clk.Advance(2 * time.Minute)
	cc.evictIdle()
	for i, c := range containers {
		if !c.dead() {
			t.Fatalf("container %d not evicted", i)
		}
	}

	// Re-seed, then remove only fn-a.
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("reseed fn-a: %v", err)
	}
	if err := run(t, cc, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("reseed fn-b: %v", err)
	}
	last := ff.lastContainer()
	cc.removeFunction("fn-a")
	if last.dead() {
		t.Fatal("removing fn-a must not discard fn-b's container")
	}
}

// TestEvictionRaceSafety hammers eviction against concurrent acquire/release.
// Its value is under -race: it proves the cache's lock discipline (pool lock
// released before cleanup, cache -> pool ordering) holds under contention.
func TestEvictionRaceSafety(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Millisecond)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Eviction + clock-advance loop. It hammers the sweep concurrently with
	// acquire/release so -race can observe any unsynchronized access.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			clk.Advance(time.Millisecond)
			cc.evictIdle()
		}
	}()

	// Concurrent acquires/releases against two functions. Containers never
	// block, so each acquire/release completes immediately.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "fn-race-a"
			if i%2 == 1 {
				name = "fn-race-b"
			}
			for j := 0; j < 50; j++ {
				lease, err := cc.acquire(context.Background(), name, "img-1", 2, ff.start())
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				lease.release()
			}
		}(i)
	}

	// Concurrent remove/reactivate on an unrelated function exercises the
	// cache's removal path under contention too.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			cc.removeFunction("fn-race-removed")
			cc.activateFunction("fn-race-removed", "")
		}
	}()

	// Serial executes while the sweep runs: they may race eviction but must
	// never fail or hang.
	for i := 0; i < 4; i++ {
		if err := run(t, cc, ff, "fn-race-a", "img-1", 2, "h"); err != nil {
			t.Fatalf("serial execute: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	<-done
}

// TestAcquireAfterCloseStillFails pins that the generation refactor preserves
// errPoolClosed behavior on a closed pool.
func TestAcquireAfterCloseStillFails(t *testing.T) {
	cc, ff := mkCache()
	cc.close()
	if err := run(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after close = %v, want errPoolClosed", err)
	}
}

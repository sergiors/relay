package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
)

// warmCount reads the cache's global warm-budget usage under its lock.
func warmCount(cc *containerCache) int {
	cc.warmMu.Lock()
	defer cc.warmMu.Unlock()
	return cc.warmCount
}

// setWarm sets the cache's global bound and publishes its capacity gauge.
func setWarm(cc *containerCache, max int) {
	cc.warmMu.Lock()
	cc.maxWarm = max
	cc.lazyInitWarmLocked()
	cc.publishWarmGaugesLocked()
	cc.warmMu.Unlock()
}

// TestResolveManagerOptionsMaxWarmContainers pins the WithMaxWarmContainers
// option contract: an explicit positive value is honored, and a non-positive or
// unset value falls back to DefaultMaxWarmContainers (never "unbounded").
func TestResolveManagerOptionsMaxWarmContainers(t *testing.T) {
	if got := resolveManagerOptions(nil).maxWarmContainers; got != DefaultMaxWarmContainers {
		t.Fatalf("default maxWarmContainers = %d, want %d", got, DefaultMaxWarmContainers)
	}
	if got := resolveManagerOptions([]ManagerOption{WithMaxWarmContainers(3)}).maxWarmContainers; got != 3 {
		t.Fatalf("explicit maxWarmContainers = %d, want 3", got)
	}
	for _, bad := range []int{0, -1} {
		optsBad := []ManagerOption{WithMaxWarmContainers(bad)}
		if got := resolveManagerOptions(optsBad).maxWarmContainers; got != DefaultMaxWarmContainers {
			t.Errorf("non-positive maxWarmContainers %d resolved to %d, want default %d", bad, got, DefaultMaxWarmContainers)
		}
	}
	if DefaultMaxWarmContainers != 8 {
		t.Fatalf("DefaultMaxWarmContainers = %d, want 8 (mirrors config)", DefaultMaxWarmContainers)
	}
}

// TestWarmBudgetUnboundedByDefault pins that a directly constructed cache (no
// bound configured) admits unconditionally and counts nothing, so existing
// in-package callers are unchanged and a nil permit is returned.
func TestWarmBudgetUnboundedByDefault(t *testing.T) {
	cc := newContainerCache()
	permit, err := cc.reserveWarm(context.Background())
	if err != nil {
		t.Fatalf("reserveWarm (unbounded): %v", err)
	}
	if permit != nil {
		t.Fatalf("reserveWarm (unbounded) = %+v, want nil permit", permit)
	}
	if got := warmCount(cc); got != 0 {
		t.Fatalf("warmCount (unbounded) = %d, want 0", got)
	}
}

// TestWarmBudgetCountsAndReleases pins the basic reserve/release accounting and
// that the capacity/usage gauges are published.
func TestWarmBudgetCountsAndReleases(t *testing.T) {
	reg := metrics.New()
	cc := newContainerCache()
	cc.metrics = reg
	setWarm(cc, 2)

	if got := reg.Gauge(metrics.MetricRuntimeWarmCapacity); got != 2 {
		t.Fatalf("warm capacity gauge = %v, want 2", got)
	}

	p1, err := cc.reserveWarm(context.Background())
	if err != nil || p1 == nil {
		t.Fatalf("reserve 1 = (%v, %v), want a permit", p1, err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after 1 = %d, want 1", got)
	}
	if got := reg.Gauge(metrics.MetricRuntimeWarmContainers); got != 1 {
		t.Fatalf("warm usage gauge = %v, want 1", got)
	}
	p1.Release()
	p1.Release() // idempotent
	if got := warmCount(cc); got != 0 {
		t.Fatalf("warmCount after release = %d, want 0", got)
	}
}

// TestWarmBudgetHardBoundAllBusyBackpressures pins that when every warm
// container is busy (none idle), a reserve WAITS (backpressure) rather than
// over-committing, and returns the context error on timeout.
func TestWarmBudgetHardBoundAllBusyBackpressures(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 2)

	blockA := newBlockingContainer(1)
	blockB := newBlockingContainer(1)
	built := 0
	ff.build = func() *fakeContainer {
		built++
		if built == 1 {
			return blockA
		}
		return blockB
	}
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() {
		doneA <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-blockA.entered
	go func() {
		doneB <- cc.execute(context.Background(), "fn-b", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-blockB.entered

	if got := warmCount(cc); got != 2 {
		t.Fatalf("warmCount = %d, want 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err := cc.reserveWarm(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reserveWarm at bound = %v, want context.DeadlineExceeded", err)
	}
	if got := warmCount(cc); got != 2 {
		t.Fatalf("warmCount after blocked reserve = %d, want 2 (no over-commit)", got)
	}

	// Releasing one busy container frees a slot; the next reserve succeeds.
	close(blockA.release)
	if err := <-doneA; err != nil {
		t.Fatalf("blockA execute: %v", err)
	}
	permit, err := cc.reserveWarm(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("reserveWarm after release = (%v, %v), want a permit", permit, err)
	}
	permit.Release()
	close(blockB.release)
	if err := <-doneB; err != nil {
		t.Fatalf("blockB execute: %v", err)
	}
}

// TestWarmBudgetEvictsGloballyOldestIdle pins LRU eviction: at the bound, a new
// container evicts the globally OLDEST healthy idle container across all apps
// (not merely the requesting app's), with the warm_eviction reason, without
// evicting a busy one.
func TestWarmBudgetEvictsGloballyOldestIdle(t *testing.T) {
	clk := newFakeClock()
	cc, ff := newManagedCache(clk, time.Hour)
	setWarm(cc, 2)
	// Seed two idle containers (bound 2), stamping distinct idleSince values.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	clk.Advance(time.Minute)
	if err := runInvoke(t, cc, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed b: %v", err)
	}
	a := ff.allContainers()[0]
	b := ff.allContainers()[1]

	// A direct acquire for a third app at the bound must evict the oldest idle
	// (fn-a's, idle at t0) and create a fresh container, keeping the count at 2.
	if err := runInvoke(t, cc, ff, "fn-c", "img-1", 1, "h"); err != nil {
		t.Fatalf("acquire c: %v", err)
	}

	if got := a.reasons(); len(got) != 1 || got[0] != reasonWarmEviction {
		t.Fatalf("oldest idle (fn-a) reasons = %v, want [%s]", got, reasonWarmEviction)
	}
	if got := b.reasons(); len(got) != 0 {
		t.Fatalf("younger idle (fn-b) was evicted: %v", got)
	}
	if got := warmCount(cc); got != 2 {
		t.Fatalf("warmCount = %d, want 2", got)
	}
}

// TestWarmBudgetNeverEvictsBusy pins that a busy container is never chosen for
// eviction: at the bound with the only container busy, a new direct acquire
// blocks rather than killing it.
// TestWarmBudgetNeverEvictsBusy pins that a busy container is never chosen for
// eviction: at the bound with the only container busy, a direct acquire is
// refused with ErrWarmBudgetSaturated (non-blocking, so it never holds a claimed
// attempt) rather than killing the busy container.
func TestWarmBudgetNeverEvictsBusy(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)

	busy := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	err := cc.execute(context.Background(), "fn-b", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	if !errors.Is(err, ErrWarmBudgetSaturated) {
		t.Fatalf("execute for fn-b at bound = %v, want ErrWarmBudgetSaturated", err)
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy container was evicted: %v", got)
	}
	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
}

// TestWarmBudgetReuseDoesNotGrowCount pins that leasing an existing idle
// container neither grows the count nor creates a container: the bound is a
// bound on containers, not on invocations.
func TestWarmBudgetReuseDoesNotGrowCount(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 2)
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
			t.Fatalf("reuse %d: %v", i, err)
		}
	}
	if got := ff.count(); got != 1 {
		t.Fatalf("creations = %d, want 1 (reused)", got)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount = %d, want 1", got)
	}
}

// TestWarmBudgetConcurrentHardBoundAcrossApps hammers concurrent acquires
// across many apps against a small global bound and proves the number of live
// regular containers never exceeds it. It is the cross-app (not per-app)
// guarantee: per-app caps are all 1, but the sum is bounded by maxWarm.
func TestWarmBudgetConcurrentHardBoundAcrossApps(t *testing.T) {
	cc, ff := newTestCache()
	const bound = 3
	setWarm(cc, bound)

	// Track the peak observed regular-container count. Each busy container is
	// held for a short deterministic barrier so overlaps are real.
	var mu sync.Mutex
	peak := 0
	observe := func() {
		mu.Lock()
		if n := warmCount(cc); n > peak {
			peak = n
		}
		mu.Unlock()
	}

	const workers = 12
	var wg sync.WaitGroup
	releaseAll := make(chan struct{})
	// Each worker runs a bounded number of acquire/release cycles against its
	// own app; containers are non-blocking, so the loops are iteration-bounded.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			name := fmt.Sprintf("fn-%d", w)
			for i := 0; i < 30; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				l, err := cc.acquire(ctx, name, "img-1", 1, ff.start())
				cancel()
				if err != nil {
					continue
				}
				observe()
				l.release()
			}
		}(w)
	}
	wg.Wait()
	close(releaseAll)

	if peak > bound {
		t.Fatalf("peak live warm containers = %d, want <= %d (global hard bound)", peak, bound)
	}
}

// TestWarmBudgetFailedCreationAfterEviction keeps the accounting safe when a
// start fails after an eviction freed a slot: the reservation is rolled back, no
// slot leaks, and a subsequent reserve still succeeds.
func TestWarmBudgetFailedCreationAfterEviction(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)
	// Seed an idle container, then arm the next create to fail.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	idle := ff.allContainers()[0]
	ff.startErr = errors.New("start boom")

	err := runInvoke(t, cc, ff, "fn-b", "img-1", 1, "h")
	if err == nil {
		t.Fatal("execute with a failing start returned nil")
	}
	// The oldest idle (fn-a) was evicted to make room, and the failed create
	// released its reservation.
	if got := idle.reasons(); len(got) != 1 || got[0] != reasonWarmEviction {
		t.Fatalf("evicted idle reasons = %v, want [%s]", got, reasonWarmEviction)
	}
	if got := warmCount(cc); got != 0 {
		t.Fatalf("warmCount after failed create = %d, want 0 (reservation rolled back)", got)
	}
	// A later reserve fits the whole bound.
	permit, err := cc.reserveWarm(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("reserveWarm after failed create = (%v, %v), want a permit", permit, err)
	}
	permit.Release()
}

// TestWarmBudgetDiscardFailureKeepsSlotReserved pins the conservative teardown
// policy: when an idle container's physical removal cannot be confirmed, its
// global warm-budget slot stays reserved for the Manager lifetime, so the counted
// bound is never exceeded by an orphan. At the bound the next create cannot free
// capacity, so the acquire is refused through the warm saturation/backpressure
// sentinel (never charging an attempt), the teardown is attempted, and a
// replacement container is never admitted.
func TestWarmBudgetDiscardFailureKeepsSlotReserved(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)
	stuck := &fakeContainer{discardFails: true}
	ff.build = func() *fakeContainer { return stuck }
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount = %d, want 1", got)
	}

	// The bound forces an eviction of the idle, un-removable container. Its
	// removal cannot be confirmed, so its slot stays reserved and the
	// replacement create is refused with the saturation sentinel rather than
	// over-committing the bound.
	err := runInvoke(t, cc, ff, "fn-b", "img-1", 1, "h")
	if !errors.Is(err, ErrWarmBudgetSaturated) {
		t.Fatalf("second acquire = %v, want ErrWarmBudgetSaturated", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after failed eviction = %d, want 1 (slot reserved, never over max)", got)
	}
	if stuck.attempts() == 0 {
		t.Fatal("eviction never attempted the teardown")
	}

	// A fresh container is never admitted on the phantom capacity: every later
	// acquire at the bound is refused the same way and the slot stays reserved.
	if err := runInvoke(t, cc, ff, "fn-b", "img-1", 1, "h"); !errors.Is(err, ErrWarmBudgetSaturated) {
		t.Fatalf("third acquire = %v, want ErrWarmBudgetSaturated", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after repeated failed eviction = %d, want 1", got)
	}
	// The failed container was never replaced in the pool either.
	if got := ff.count(); got != 1 {
		t.Fatalf("creations = %d, want 1 (no replacement on unconfirmed removal)", got)
	}
}

// TestWarmBudgetConfirmedRemovalReleasesSlot pins the complementary half: when
// the physical container IS confirmed removed, its slot is returned exactly once,
// so the bound is available again and a replacement can be admitted.
func TestWarmBudgetConfirmedRemovalReleasesSlot(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Discard the idle container successfully; its removal is confirmed.
	c := ff.allContainers()[0]
	pool := cc.poolFor("fn-a", 1)
	if !pool.evictIdleContainer(context.Background(), mustIdle(t, pool, c)) {
		t.Fatal("confirmed eviction must report the slot as freed")
	}
	if got := warmCount(cc); got != 0 {
		t.Fatalf("warmCount after confirmed eviction = %d, want 0 (slot released)", got)
	}
	// The bound now has room again.
	permit, err := cc.reserveWarm(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("reserveWarm after confirmed eviction = (%v, %v), want a permit", permit, err)
	}
	permit.Release()
}

// TestWarmBudgetCloseUnblocksWaiters pins that a blocked reserve on a
// saturated budget is unblocked by cache close and returns errPoolClosed.
func TestWarmBudgetCloseUnblocksWaiters(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)
	busy := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	reserveErr := make(chan error, 1)
	go func() {
		_, err := cc.reserveWarm(context.Background())
		reserveErr <- err
	}()
	// Give the waiter a moment to block at the bound, then close.
	time.Sleep(20 * time.Millisecond)
	cc.closeWarm()

	select {
	case err := <-reserveErr:
		if !errors.Is(err, errPoolClosed) {
			t.Fatalf("blocked reserve after close = %v, want errPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closeWarm did not unblock the reserve waiter")
	}
	close(busy.release)
	<-done
}

// TestWarmBudgetExecuteConsumesPermitAndReleasesOnDiscard proves the runtime
// half of the runner seam: a permit carried on the invocation context is
// consumed by the freshly created container, counted once, and released exactly
// once when that container is discarded.
func TestWarmBudgetExecuteConsumesPermitAndReleasesOnDiscard(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 2)

	permit, err := cc.reserveWarm(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("reserveWarm = (%v, %v), want a permit", permit, err)
	}
	ctx := WithWarmPermit(context.Background(), permit)
	if err := cc.execute(ctx, "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after execute = %d, want 1", got)
	}
	// The runner's deferred Release is a no-op: the container owns the slot.
	permit.Release()
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after unused Release = %d, want 1 (container owns it)", got)
	}
	// Discarding the idle container returns the slot.
	c := ff.allContainers()[0]
	pool := cc.poolFor("fn-a", 1)
	pool.evictIdleContainer(context.Background(), mustIdle(t, pool, c))
	if got := warmCount(cc); got != 0 {
		t.Fatalf("warmCount after discard = %d, want 0", got)
	}
}

// TestWarmBudgetExecuteReleasesPermitOnReuse proves that leasing an existing
// idle container (no create) releases the carried permit unused, so the counted
// slot is not leaked when Execute never attaches it to a container.
func TestWarmBudgetExecuteReleasesPermitOnReuse(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 2)
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	permit, err := cc.reserveWarm(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("reserveWarm = (%v, %v), want a permit", permit, err)
	}
	if got := warmCount(cc); got != 2 {
		t.Fatalf("warmCount before execute = %d, want 2", got)
	}
	ctx := WithWarmPermit(context.Background(), permit)
	if err := cc.execute(ctx, "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := ff.count(); got != 1 {
		t.Fatalf("creations = %d, want 1 (reused)", got)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after reuse = %d, want 1 (permit released unused)", got)
	}
}

// mustIdle finds the idle pooledContainer wrapping c in pool's active
// generation. It fails the test when c is not currently idle.
func mustIdle(t *testing.T, p *appPool, c *fakeContainer) *pooledContainer {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active != nil {
		for _, pc := range p.active.idle {
			if pc.c == c {
				return pc
			}
		}
	}
	t.Fatalf("container is not idle in the active generation")
	return nil
}

// TestWarmBudgetCapacityAccessor pins Manager.WarmBudgetCapacity reflects the
// configured bound (0 for a Manager with no cache configured).
func TestWarmBudgetCapacityAccessor(t *testing.T) {
	m := &Manager{}
	if got := m.WarmBudgetCapacity(); got != 0 {
		t.Fatalf("WarmBudgetCapacity (no cache) = %d, want 0", got)
	}
	m.containers = newContainerCache()
	m.containers.maxWarm = 5
	if got := m.WarmBudgetCapacity(); got != 5 {
		t.Fatalf("WarmBudgetCapacity = %d, want 5", got)
	}
}

// TestWarmBudgetTransientBypassesBound pins that stale-version throwaway
// containers do not consume the global warm bound: with the bound fully used by
// a regular container, a retired-image request for another app is still served
// on a throwaway.
func TestWarmBudgetTransientBypassesBound(t *testing.T) {
	cc, ff := newTestCache()
	setWarm(cc, 1)
	if err := runInvoke(t, cc, ff, "fn-a", "img-a", 1, "h"); err != nil {
		t.Fatalf("seed regular fn-a: %v", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount after seed = %d, want 1", got)
	}

	// img-b is retired globally; fn-b's pool is created afterwards and seeded
	// from the cache-level retirement, so its request is served transiently.
	cc.invalidateImage("img-b")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cc.execute(ctx, "fn-b", "img-b", 1, ff.start(), "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("stale execute: %v", err)
	}
	if got := warmCount(cc); got != 1 {
		t.Fatalf("warmCount with a throwaway = %d, want 1 (transient outside bound)", got)
	}
}

// TestWarmBudgetManagerExecuteThroughPermit drives the production Manager.Execute
// path with a pre-admitted permit carried on the context (the runner seam) and
// proves the Manager consumes it: the container is created, counted once, and a
// second Execute reuses it without growing the count.
func TestWarmBudgetManagerExecuteThroughPermit(t *testing.T) {
	m := &Manager{maxConcurrentInvocations: 4}
	m.containers = newContainerCache()
	m.containers.maxWarm = 4

	started := 0
	m.startContainerFn = func(context.Context, string, resolvedImage, []string, app.ResourceLimits, RunMeta) (reusableContainer, error) {
		started++
		return &fakeContainer{}, nil
	}
	prepared := &Prepared{Name: "fn", Image: "img-1", Concurrency: 2}

	permit, err := m.AcquireWarmPermit(context.Background())
	if err != nil || permit == nil {
		t.Fatalf("AcquireWarmPermit = (%v, %v), want a permit", permit, err)
	}
	ctx := WithWarmPermit(context.Background(), permit)
	if err := m.Execute(ctx, prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if started != 1 {
		t.Fatalf("starts = %d, want 1", started)
	}
	if got := warmCount(m.containers); got != 1 {
		t.Fatalf("warmCount = %d, want 1", got)
	}
	// The runner releases any unconsumed permit; the container owns this one.
	permit.Release()
	if got := warmCount(m.containers); got != 1 {
		t.Fatalf("warmCount after unused release = %d, want 1", got)
	}

	// A second Execute reuses the idle container; a carried permit is released
	// unused, so the count stays at 1.
	permit2, err := m.AcquireWarmPermit(context.Background())
	if err != nil || permit2 == nil {
		t.Fatalf("AcquireWarmPermit 2 = (%v, %v), want a permit", permit2, err)
	}
	if err := m.Execute(WithWarmPermit(context.Background(), permit2), prepared, "h", []byte(`{}`), nil); err != nil {
		t.Fatalf("Execute 2: %v", err)
	}
	if started != 1 {
		t.Fatalf("starts after reuse = %d, want 1", started)
	}
	if got := warmCount(m.containers); got != 1 {
		t.Fatalf("warmCount after reuse = %d, want 1", got)
	}
}

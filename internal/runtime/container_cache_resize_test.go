package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/observability/metrics"
)

// acquireN leases n containers for fnName/image at max and returns the leases.
// It fails the test if any acquire errors. The acquire is non-blocking when the
// pool has capacity; callers that expect blocking use context timeouts.
func acquireN(t *testing.T, cc *containerCache, ff *fakeFactory, fnName, image string, max, n int) []*containerLease {
	t.Helper()
	leases := make([]*containerLease, 0, n)
	for i := 0; i < n; i++ {
		l, err := cc.acquire(context.Background(), fnName, image, max, ff.start())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		leases = append(leases, l)
	}
	return leases
}

// discardReasons aggregates every container's recorded discard reasons by value.
func discardReasons(ff *fakeFactory) map[string]int {
	counts := map[string]int{}
	for _, c := range ff.allContainers() {
		for _, r := range c.reasons() {
			counts[r]++
		}
	}
	return counts
}

// poolMax reads a pool's authoritative bound (the value acquisition,
// PoolSnapshot, and the capacity gauge all derive from).
func poolMax(cc *containerCache, fnName string) int {
	p := cc.poolFor(fnName, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max
}

// TestPoolResizeInitialBound pins the baseline: a pool created at max is bounded
// by it, and its snapshot and capacity gauge agree.
func TestPoolResizeInitialBound(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 3, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if got := poolMax(cc, "fn-a"); got != 3 {
		t.Fatalf("pool max = %d, want 3", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 3 {
		t.Fatalf("capacity gauge = %d, want 3", got)
	}
	if s, ok := cc.snapshot("fn-a", reg); !ok || s.Capacity != 3 {
		t.Fatalf("snapshot = %+v, ok=%v; want capacity 3", s, ok)
	}
}

// TestPoolResizeIncreaseUpdatesAdmissionLazily proves a capacity increase takes
// effect without a restart, starts no container eagerly, opens admission
// immediately, and updates the snapshot and capacity gauge.
func TestPoolResizeIncreaseUpdatesAdmissionLazily(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	// Seed one idle container at max 1.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := ff.count()
	if before != 1 {
		t.Fatalf("seed creations = %d, want 1", before)
	}

	cc.setFunctionConcurrency("fn-a", 3)

	// Lazy: no container is created by the resize itself.
	if got := ff.count(); got != before {
		t.Fatalf("resize eagerly created containers: %d, want %d", got, before)
	}
	if got := poolMax(cc, "fn-a"); got != 3 {
		t.Fatalf("pool max after increase = %d, want 3", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 3 {
		t.Fatalf("capacity gauge after increase = %d, want 3", got)
	}
	if s, ok := cc.snapshot("fn-a", reg); !ok || s.Capacity != 3 {
		t.Fatalf("snapshot after increase = %+v, ok=%v; want capacity 3", s, ok)
	}

	// Admission opened: three concurrent leases now fit (one idle + two starts).
	leases := acquireN(t, cc, ff, "fn-a", "img-1", 3, 3)
	if got := ff.count(); got != 3 {
		t.Fatalf("creations after 3 leases = %d, want 3", got)
	}
	for _, l := range leases {
		l.release()
	}
}

// TestPoolResizeIncreaseWakesWaiter proves a blocked acquire at the old bound is
// woken by an increase and then acquires a container.
func TestPoolResizeIncreaseWakesWaiter(t *testing.T) {
	cc, ff := newTestCache()
	blocking := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return blocking }

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-blocking.entered

	waiter := make(chan *containerLease, 1)
	errCh := make(chan error, 1)
	go func() {
		l, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, ff.start())
		if err != nil {
			errCh <- err
			return
		}
		waiter <- l
	}()

	// The waiter blocks at max 1 until the increase opens admission.
	select {
	case l := <-waiter:
		l.release()
		t.Fatal("waiter acquired before the increase")
	case err := <-errCh:
		t.Fatalf("waiter: %v", err)
	case <-time.After(80 * time.Millisecond):
	}

	cc.setFunctionConcurrency("fn-a", 2)

	select {
	case l := <-waiter:
		l.release()
	case err := <-errCh:
		t.Fatalf("waiter after increase: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("increase did not wake the blocked acquire")
	}
	close(blocking.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first execute: %v", err)
	}
}

// TestPoolResizeDecreaseRetiresExcessIdle proves a decrease immediately retires
// only the excess idle containers, keeps the rest, and updates the gauge.
func TestPoolResizeDecreaseRetiresExcessIdle(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	// Three concurrent leases, released to idle.
	leases := acquireN(t, cc, ff, "fn-a", "img-1", 3, 3)
	for _, l := range leases {
		l.release()
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 3 {
		t.Fatalf("idle before shrink = %d, want 3", got)
	}

	cc.setFunctionConcurrency("fn-a", 1)

	counts := discardReasons(ff)
	if counts[reasonConcurrencyShrink] != 2 {
		t.Fatalf("concurrency_shrink discards = %d, want 2 (reasons=%v)", counts[reasonConcurrencyShrink], counts)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 1 {
		t.Fatalf("idle after shrink = %d, want 1", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 1 {
		t.Fatalf("capacity gauge after shrink = %d, want 1", got)
	}
	if s, ok := cc.snapshot("fn-a", reg); !ok || s.Capacity != 1 || s.Idle != 1 {
		t.Fatalf("snapshot after shrink = %+v, ok=%v; want capacity 1 idle 1", s, ok)
	}
}

// TestPoolResizeDecreaseNeverKillsBusyAndConvergesOnRelease pins the shrink
// invariant: a decrease while every container is busy discards nothing; each
// release over the new bound retires instead of returning to idle, so the pool
// converges to max as leases drain.
func TestPoolResizeDecreaseNeverKillsBusyAndConvergesOnRelease(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	leases := acquireN(t, cc, ff, "fn-a", "img-1", 3, 3)

	cc.setFunctionConcurrency("fn-a", 1)

	if got := discardReasons(ff)[reasonConcurrencyShrink]; got != 0 {
		t.Fatalf("busy containers were discarded on shrink: %d", got)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateBusy)); got != 3 {
		t.Fatalf("busy after shrink = %d, want 3 (never killed)", got)
	}

	// First two releases are over the new bound: retired, not pooled.
	leases[0].release()
	leases[1].release()
	if got := discardReasons(ff)[reasonConcurrencyShrink]; got != 2 {
		t.Fatalf("over-bound releases = %d shrink discards, want 2", got)
	}
	// Last release brings the pool to exactly the new bound: pooled.
	leases[2].release()
	if got := discardReasons(ff)[reasonConcurrencyShrink]; got != 2 {
		t.Fatalf("final release shrink discards = %d, want still 2", got)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 1 {
		t.Fatalf("idle after drain = %d, want 1 (converged to max)", got)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateBusy)); got != 0 {
		t.Fatalf("busy after drain = %d, want 0", got)
	}
	if s, ok := cc.snapshot("fn-a", reg); !ok || s.Capacity != 1 || s.Containers != 1 || s.Idle != 1 {
		t.Fatalf("snapshot after drain = %+v, ok=%v; want capacity 1 idle 1", s, ok)
	}
}

// TestPoolResizeDecreaseBoundsAcquisition proves the new (smaller) bound is what
// acquisition enforces: after a shrink, an acquire beyond the bound blocks until
// context cancellation.
func TestPoolResizeDecreaseBoundsAcquisition(t *testing.T) {
	cc, ff := newTestCache()
	lease := acquireN(t, cc, ff, "fn-a", "img-1", 1, 1)
	lease[0].release() // one idle
	cc.setFunctionConcurrency("fn-a", 1)

	busy := acquireN(t, cc, ff, "fn-a", "img-1", 1, 1)
	_ = busy

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := cc.acquire(ctx, "fn-a", "img-1", 1, ff.start())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire beyond shrunk bound = %v, want context.DeadlineExceeded", err)
	}
	busy[0].release()
}

// TestPoolResizeNoOpKeepsPoolUnchanged proves resizing to the same bound
// discards nothing and leaves the pool unchanged.
func TestPoolResizeNoOpKeepsPoolUnchanged(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.setFunctionConcurrency("fn-a", 2)
	if got := discardReasons(ff)[reasonConcurrencyShrink]; got != 0 {
		t.Fatalf("no-op resize discarded %d containers", got)
	}
	if got := poolMax(cc, "fn-a"); got != 2 {
		t.Fatalf("pool max after no-op = %d, want 2", got)
	}
}

// TestPoolResizeImageAndConcurrencyTogether proves a simultaneous image and
// concurrency update works: the shrink retires excess idle containers, the
// forward image transition discards the remaining old-image idle container and
// retires the busy one (drained on release), and the new image serves at the new
// bound.
func TestPoolResizeImageAndConcurrencyTogether(t *testing.T) {
	cc, ff := newTestCache()

	// busy is a blocking img-1 container; two more img-1 containers are returned
	// to idle. max 3 so all three exist (busy + 2 idle = 3).
	busy := newBlockingContainer(1)
	idle1 := &fakeContainer{}
	idle2 := &fakeContainer{}
	built := 0
	ff.build = func() *fakeContainer {
		built++
		switch built {
		case 1:
			return busy
		case 2:
			return idle1
		default:
			return idle2
		}
	}
	busyDone := make(chan error, 1)
	go func() {
		busyDone <- cc.execute(context.Background(), "fn-a", "img-1", 3, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered
	// Lease the other two slots concurrently so two distinct idle containers are
	// created, then return both to idle (sequential seeds would reuse one).
	idleLeases := acquireN(t, cc, ff, "fn-a", "img-1", 3, 2)
	for _, l := range idleLeases {
		l.release()
	}

	// Concurrency shrinks 3 -> 2: used is busy(1)+idle(2)=3, so exactly one excess
	// idle container is retired now (the busy one is never killed).
	cc.setFunctionConcurrency("fn-a", 2)
	if got := poolMax(cc, "fn-a"); got != 2 {
		t.Fatalf("pool max = %d, want 2", got)
	}
	if got := idle1.reasons(); len(got)+len(idle2.reasons()) != 1 {
		t.Fatalf("excess idle discards = %v/%v, want exactly one %s",
			idle1.reasons(), idle2.reasons(), reasonConcurrencyShrink)
	}

	// A new image arrives at the new bound: the remaining img-1 idle container is
	// discarded by the transition, and the busy old-generation container is
	// retired (not killed) and drains on release. With one draining busy
	// container (used=1 < max=2) the new image is served immediately.
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", 2, "h"); err != nil {
		t.Fatalf("img-2 execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 0 {
		t.Fatalf("busy old-image container discarded mid-invocation: %v", got)
	}
	close(busy.release)
	if err := <-busyDone; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonImageChanged {
		t.Fatalf("drained old-generation discards = %v, want [%s]", got, reasonImageChanged)
	}
	if got := poolMax(cc, "fn-a"); got != 2 {
		t.Fatalf("pool max after image+concurrency update = %d, want 2", got)
	}
	// The new image's idle container is reused at the new bound.
	before := ff.count()
	if err := runInvoke(t, cc, ff, "fn-a", "img-2", 2, "h"); err != nil {
		t.Fatalf("reuse img-2: %v", err)
	}
	if ff.count() != before {
		t.Fatalf("creations = %d, want %d (img-2 reused)", ff.count(), before)
	}
}

// TestPoolResizeFreshPoolUsesEffectiveCapacity proves a pool created AFTER a
// concurrency reconcile is seeded with the recorded effective bound, not with a
// stale caller's max. This is the race where a stale in-flight acquire (still
// holding an old Prepared.Concurrency) creates the pool after Prepare ran.
func TestPoolResizeFreshPoolUsesEffectiveCapacity(t *testing.T) {
	cc, _ := newTestCache()
	// Prepare reconciled the function to 5; no pool exists yet.
	cc.setFunctionConcurrency("fn-a", 5)

	// A stale acquire passes the old max 2; the fresh pool must use 5.
	p := cc.poolFor("fn-a", 2)
	p.mu.Lock()
	got := p.max
	p.mu.Unlock()
	if got != 5 {
		t.Fatalf("fresh pool max = %d, want 5 (recorded effective capacity)", got)
	}

	// A direct caller (integration test) with no recorded capacity still uses its
	// own max for a function never prepared.
	p2 := cc.poolFor("fn-raw", 3)
	p2.mu.Lock()
	got2 := p2.max
	p2.mu.Unlock()
	if got2 != 3 {
		t.Fatalf("direct-caller pool max = %d, want 3 (caller fallback)", got2)
	}
}

// TestPoolResizeManagerSnapshotReflectsNewBound proves a Manager's LIVE pool
// snapshot (the source for the socket/CLI) reports the reconciled bound after a
// resize, matching what acquisition enforces.
func TestPoolResizeManagerSnapshotReflectsNewBound(t *testing.T) {
	reg := metrics.New()
	m := &Manager{metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	m.containers.setFunctionConcurrency("fn-a", 4)
	s, ok := m.PoolSnapshot("fn-a")
	if !ok {
		t.Fatal("PoolSnapshot(fn-a) not found")
	}
	if s.Capacity != 4 {
		t.Fatalf("snapshot capacity = %d, want 4", s.Capacity)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 4 {
		t.Fatalf("capacity gauge = %d, want 4", got)
	}
}

// TestPoolResizeRemovedFunctionIgnores proves a resize of a removed/detached
// pool is a safe no-op and does not resurrect it.
func TestPoolResizeRemovedFunctionIgnores(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.removeFunction("fn-a")
	cc.setFunctionConcurrency("fn-a", 5)
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("acquire after removal+resize = %v, want errPoolClosed", err)
	}
}

// TestPoolResizeRaceSafety hammers concurrent resizes against acquire/release
// and snapshot. Its value is under -race: it proves the resized bound and the
// gauges are consistently lock-protected.
func TestPoolResizeRaceSafety(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	ff.build = func() *fakeContainer { return &fakeContainer{} }

	var wg sync.WaitGroup

	// All loops run a fixed, generous iteration count; the whole test is
	// iteration-bounded (no wall-clock soak), so it cannot flake slow or fast.
	const resizeIters = 400
	const acquireIters = 80
	const snapshotIters = 400

	// Resize loop: alternate small and large bounds.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < resizeIters; i++ {
			cc.setFunctionConcurrency("fn-race", 1+(i%4))
		}
	}()

	// Acquire/release loop.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < acquireIters; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				l, err := cc.acquire(ctx, "fn-race", "img-1", 2, ff.start())
				cancel()
				if err != nil {
					// Capacity churn may time out an acquire; that is expected.
					continue
				}
				l.release()
			}
		}()
	}

	// Snapshot loop reads the bound while it changes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < snapshotIters; i++ {
			_, _ = cc.snapshot("fn-race", reg)
		}
	}()

	wg.Wait()
}

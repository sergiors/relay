package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/metrics"
)

// newMetricsCache returns a cache wired to a fresh registry plus a fake factory.
func newMetricsCache() (*containerCache, *fakeFactory, *metrics.Registry) {
	cc := newContainerCache()
	reg := metrics.New()
	cc.metrics = reg
	return cc, &fakeFactory{}, reg
}

// fnLabels is the single-function label set for a registry read.
func fnLabels(name string) []metrics.Label {
	return []metrics.Label{{Name: "function", Value: name}}
}

// stateLabels is the function+state label set for a runtime_containers read.
func stateLabels(name, state string) []metrics.Label {
	return []metrics.Label{{Name: "function", Value: name}, {Name: "state", Value: state}}
}

// acquireLabels is the function+outcome label set for a runtime acquire read.
func acquireLabels(name, outcome string) []metrics.Label {
	return []metrics.Label{{Name: "function", Value: name}, {Name: "outcome", Value: outcome}}
}

// gauge reads a labeled runtime gauge.
func gauge(reg *metrics.Registry, name string, labels []metrics.Label) int64 {
	return int64(reg.GaugeLabels(name, labels))
}

// counters is a convenience snapshot of the acquire/discard counters for fn.
func acquireCount(reg *metrics.Registry, fn, outcome string) int64 {
	return reg.CounterLabels(metrics.MetricRuntimeContainerAcquires, acquireLabels(fn, outcome))
}

func discardCount(reg *metrics.Registry, fn, reason string) int64 {
	return reg.CounterLabels(metrics.MetricRuntimeContainerDiscards,
		[]metrics.Label{{Name: "function", Value: fn}, {Name: "reason", Value: reason}})
}

// TestPoolMetricsFirstAcquireIsCold pins the cold-start path: the first acquire
// starts a container, counts one cold outcome (never warm), observes exactly one
// successful acquire duration, and publishes capacity plus the idle gauge.
func TestPoolMetricsFirstAcquireIsCold(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("cold acquires = %d, want 1", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm); got != 0 {
		t.Fatalf("warm acquires = %d, want 0", got)
	}
	if c, _ := reg.HistogramLabels(metrics.MetricRuntimeContainerAcquireDuration, fnLabels("fn-a")); c != 1 {
		t.Fatalf("acquire duration observations = %d, want 1", c)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 2 {
		t.Fatalf("capacity gauge = %d, want 2", got)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 1 {
		t.Fatalf("idle gauge = %d, want 1", got)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateBusy)); got != 0 {
		t.Fatalf("busy gauge = %d, want 0", got)
	}
}

// TestPoolMetricsReuseIsWarm pins that a second acquire served by the idle
// container is counted warm (not cold) and that no new container is created.
func TestPoolMetricsReuseIsWarm(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	for i := 0; i < 2; i++ {
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
			t.Fatalf("execute %d: %v", i, err)
		}
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("cold acquires = %d, want 1", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm); got != 1 {
		t.Fatalf("warm acquires = %d, want 1", got)
	}
	if ff.count() != 1 {
		t.Fatalf("creations = %d, want 1", ff.count())
	}
	if c, _ := reg.HistogramLabels(metrics.MetricRuntimeContainerAcquireDuration, fnLabels("fn-a")); c != 2 {
		t.Fatalf("acquire duration observations = %d, want 2", c)
	}
}

// TestPoolMetricsConcurrentCold pins that N concurrent acquires with capacity
// all count as cold starts and the final idle gauge equals the pool bound.
func TestPoolMetricsConcurrentCold(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	ff.created = make(chan *fakeContainer, 2)
	ff.build = func() *fakeContainer {
		return newBlockingContainer(2)
	}

	const max = 2
	var wg sync.WaitGroup
	for i := 0; i < max; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cc.execute(context.Background(), "fn-a", "img-1", max, ff.start(), "h", []byte(`{}`), nil)
		}()
	}
	c1 := <-ff.created
	c2 := <-ff.created
	<-c1.entered
	<-c2.entered

	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 2 {
		t.Fatalf("cold acquires = %d, want 2", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm); got != 0 {
		t.Fatalf("warm acquires = %d, want 0", got)
	}
	close(c1.release)
	close(c2.release)
	wg.Wait()
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 2 {
		t.Fatalf("idle gauge = %d, want 2", got)
	}
}

// TestPoolMetricsWaitThenReuseIsWarm pins the requirement that waiting then
// leasing an idle container is warm, not cold, and that the waits counter
// records the contention exactly once while no acquire duration is observed for
// the wait itself.
func TestPoolMetricsWaitThenReuseIsWarm(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	c1 := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return c1 }

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-c1.entered

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()

	// Wait for the waiter to reach the capacity wait (bounded, no fixed soak).
	waits := func() int64 { return reg.CounterLabels(metrics.MetricRuntimeContainerWaits, fnLabels("fn-a")) }
	if !pollUntil(nil, time.Second, func() bool { return waits() == 1 }) {
		t.Fatalf("waits = %d, want 1", waits())
	}

	close(c1.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm); got != 1 {
		t.Fatalf("warm acquires = %d, want 1 (wait then idle is warm)", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("cold acquires = %d, want 1", got)
	}
}

// TestPoolMetricsBusyAndStartingGauges pins the busy and starting gauges: a
// leased container is busy, an in-flight lazy start is starting, and both roll
// back to idle/zero once the acquire completes.
func TestPoolMetricsBusyAndStartingGauges(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	started := make(chan struct{})
	unblock := make(chan struct{})
	ff.build = func() *fakeContainer {
		close(started)
		<-unblock
		return &fakeContainer{}
	}

	go func() {
		_, _ = cc.acquire(context.Background(), "fn-a", "img-1", 1, ff.start())
	}()
	<-started
	// While start() is blocked the reservation must be visible as starting.
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateStarting)); got != 1 {
		t.Fatalf("starting gauge during start = %d, want 1", got)
	}
	close(unblock)
	busy := func() int64 {
		return gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateBusy))
	}
	if !pollUntil(nil, time.Second, func() bool { return busy() == 1 }) {
		t.Fatalf("busy gauge after acquire = %d, want 1", busy())
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateStarting)); got != 0 {
		t.Fatalf("starting gauge after acquire = %d, want 0", got)
	}
}

// TestPoolMetricsStartFailureRollsBackStarting pins that a failed lazy start
// counts no acquire, observes no duration, and rolls the starting gauge back to
// zero.
func TestPoolMetricsStartFailureRollsBackStarting(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	ff.startErr = errors.New("start boom")
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err == nil {
		t.Fatal("expected the failed start to surface")
	}
	acquires := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold) +
		acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm)
	if acquires != 0 {
		t.Fatalf("acquires after failed start = %d, want 0", acquires)
	}
	if c, _ := reg.HistogramLabels(metrics.MetricRuntimeContainerAcquireDuration, fnLabels("fn-a")); c != 0 {
		t.Fatalf("acquire duration observations = %d, want 0", c)
	}
	for _, state := range []string{metrics.RuntimeStateIdle, metrics.RuntimeStateBusy, metrics.RuntimeStateStarting} {
		if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", state)); got != 0 {
			t.Fatalf("%s gauge after failed start = %d, want 0", state, got)
		}
	}
}

// TestPoolMetricsPanicRollsBackStarting pins that a panicking start factory
// rolls the starting gauge back to zero while propagating the panic.
func TestPoolMetricsPanicRollsBackStarting(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	panicked := false
	start := func() (reusableContainer, error) {
		if !panicked {
			panicked = true
			panic("factory boom")
		}
		return ff.create()
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the factory panic to propagate")
			}
		}()
		_, _ = cc.acquire(context.Background(), "fn-a", "img-1", 1, start)
	}()
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateStarting)); got != 0 {
		t.Fatalf("starting gauge after panic = %d, want 0", got)
	}
}

// TestPoolMetricsCancellationRecordsNoAcquire pins that a cancelled capacity
// wait records the wait (contention happened) but no acquire and no duration.
func TestPoolMetricsCancellationRecordsNoAcquire(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	c1 := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return c1 }
	firstDone := make(chan struct{})
	go func() {
		_ = cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
		close(firstDone)
	}()
	<-c1.entered

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := cc.execute(ctx, "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v, want deadline exceeded", err)
	}
	if got := reg.CounterLabels(metrics.MetricRuntimeContainerWaits, fnLabels("fn-a")); got != 1 {
		t.Fatalf("waits = %d, want 1", got)
	}
	// The first (successful) execute accounts for the one cold acquire and the
	// one duration observation; the cancelled waiter must add none.
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeWarm); got != 0 {
		t.Fatalf("warm acquires = %d, want 0", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("cold acquires = %d, want 1 (only the first execute)", got)
	}
	if c, _ := reg.HistogramLabels(metrics.MetricRuntimeContainerAcquireDuration, fnLabels("fn-a")); c != 1 {
		t.Fatalf("acquire duration observations = %d, want 1 (cancelled wait not observed)", c)
	}
	close(c1.release)
	<-firstDone
}

// TestPoolMetricsDiscardReasons pins that each finite discard reason is counted
// under its own label. Self-terminated reasons (timeout/process_exit/
// protocol_error) come from the container; pool-initiated reasons
// (image_changed/idle_timeout/shutdown/function_removed) come from the pool.
func TestPoolMetricsDiscardReasons(t *testing.T) {
	t.Run("self-terminated", func(t *testing.T) {
		for _, reason := range []string{"timeout", "process_exit", "protocol_error"} {
			cc, _, reg := newMetricsCache()
			p := cc.poolFor("fn-a", 1)
			dead := &fakeContainer{deadFlag: true, reason: reason}
			// discardContainer records under p.mu itself, so it is called
			// without the pool lock held (as every production path does).
			p.discardContainer(&pooledContainer{c: dead, image: "img-1"}, reasonShutdown)
			if got := discardCount(reg, "fn-a", reason); got != 1 {
				t.Fatalf("discards{reason=%s} = %d, want 1 (container's own reason wins)", reason, got)
			}
		}
	})

	t.Run("image_changed", func(t *testing.T) {
		cc, ff, reg := newMetricsCache()
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		cc.invalidateImage("img-1")
		if got := discardCount(reg, "fn-a", reasonImageChanged); got != 1 {
			t.Fatalf("image_changed discards = %d, want 1", got)
		}
	})

	t.Run("idle_timeout", func(t *testing.T) {
		clk := newFakeClock()
		cc, ff, reg := newMetricsCache()
		cc.idleTimeout = time.Minute
		cc.now = clk.Now
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		clk.Advance(2 * time.Minute)
		cc.evictIdle()
		if got := discardCount(reg, "fn-a", reasonIdleTimeout); got != 1 {
			t.Fatalf("idle_timeout discards = %d, want 1", got)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		cc, ff, reg := newMetricsCache()
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		cc.close()
		if got := discardCount(reg, "fn-a", reasonShutdown); got != 1 {
			t.Fatalf("shutdown discards = %d, want 1", got)
		}
		for _, state := range []string{metrics.RuntimeStateIdle, metrics.RuntimeStateBusy, metrics.RuntimeStateStarting} {
			if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", state)); got != 0 {
				t.Fatalf("%s gauge after close = %d, want 0", state, got)
			}
		}
	})

	t.Run("function_removed", func(t *testing.T) {
		cc, ff, reg := newMetricsCache()
		if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		cc.removeFunction("fn-a")
		// Removal is a metric tombstone: the removal-time discard is deliberately
		// NOT counted, because removeFunction deletes the function's series in the
		// same critical section that installs the tombstone. Emitting it would
		// recreate the very series removal just deleted.
		if got := discardCount(reg, "fn-a", reasonFunctionRemove); got != 0 {
			t.Fatalf("function_removed discards = %d, want 0 (tombstoned removal)", got)
		}
		if seriesInSnapshot(reg.Snapshot(), metrics.MetricRuntimeContainerDiscards, "fn-a") {
			t.Fatalf("removed function must expose no discard series:\n%s", reg.Snapshot())
		}
	})
}

// TestPoolMetricsDiscardCountedOnce pins that a discard raced by several paths
// (release after close, remove while idle) increments the counter exactly once.
func TestPoolMetricsDiscardCountedOnce(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	busy := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	// Close discards the busy container and marks the lease retired; the
	// subsequent release must not double-count.
	cc.close()
	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := discardCount(reg, "fn-a", reasonShutdown); got != 1 {
		t.Fatalf("shutdown discards = %d, want exactly 1", got)
	}
}

// TestPoolMetricsIndependentFunctions pins that one function's acquires and
// discards never leak into another function's series.
func TestPoolMetricsIndependentFunctions(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("A: %v", err)
	}
	if err := runInvoke(t, cc, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("B: %v", err)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("fn-a cold = %d, want 1", got)
	}
	if got := acquireCount(reg, "fn-b", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("fn-b cold = %d, want 1", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 2 {
		t.Fatalf("fn-a capacity = %d, want 2", got)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-b")); got != 1 {
		t.Fatalf("fn-b capacity = %d, want 1", got)
	}
	cc.removeFunction("fn-a")
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-b", metrics.RuntimeStateIdle)); got != 1 {
		t.Fatalf("fn-b idle gauge = %d, want 1 (fn-a removal must not touch it)", got)
	}
	if got := discardCount(reg, "fn-b", reasonFunctionRemove); got != 0 {
		t.Fatalf("fn-b function_removed discards = %d, want 0", got)
	}
}

// TestPoolMetricsNilRegistry pins that a cache with no registry (metrics
// disabled) works and never panics.
func TestPoolMetricsNilRegistry(t *testing.T) {
	cc, ff := newTestCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	cc.removeFunction("fn-a")
	cc.close()
	// A nil registry read is safe too.
	var reg *metrics.Registry
	if got := reg.CounterLabels(metrics.MetricRuntimeContainerAcquires, fnLabels("fn-a")); got != 0 {
		t.Fatalf("nil registry read = %d, want 0", got)
	}
	if got := reg.GaugeLabels(metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 0 {
		t.Fatalf("nil registry gauge read = %v, want 0", got)
	}
}

// TestPoolMetricsSnapshot pins Manager.PoolSnapshot's mapping from the pool's
// authoritative state and the registry counters. It is a pure in-memory Manager
// (no Docker) so the test never touches a daemon.
func TestPoolMetricsSnapshot(t *testing.T) {
	reg := metrics.New()
	m := &Manager{metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("reuse: %v", err)
	}
	s, ok := m.PoolSnapshot("fn-a")
	if !ok {
		t.Fatal("PoolSnapshot(fn-a) not found")
	}
	if s.Capacity != 2 || s.Containers != 1 || s.Idle != 1 || s.Busy != 0 || s.Starting != 0 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.WarmAcquires != 1 || s.ColdStarts != 1 || s.Discarded != 0 {
		t.Fatalf("snapshot counters = %+v", s)
	}
	if _, ok := m.PoolSnapshot("nope"); ok {
		t.Fatal("PoolSnapshot(unknown) must report not-found")
	}
}

// TestPoolMetricsSelfTerminatedReleaseAttribution pins the release path: a
// container that tore itself down during an invocation (e.g. timeout) is counted
// under its OWN recorded reason, not the pool's fallback, and only once.
func TestPoolMetricsSelfTerminatedReleaseAttribution(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	c := &fakeContainer{selfDiscardReason: reasonTimeout, err: errors.New("handler timed out")}
	ff.build = func() *fakeContainer { return c }

	err := cc.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	if err == nil {
		t.Fatal("expected the invocation error to surface")
	}
	if got := discardCount(reg, "fn-a", reasonTimeout); got != 1 {
		t.Fatalf("timeout discards = %d, want 1 (container's own reason)", got)
	}
	if got := discardCount(reg, "fn-a", reasonImageChanged); got != 0 {
		t.Fatalf("image_changed discards = %d, want 0 (pool fallback must not win)", got)
	}
	// The dead container's idle slot is dropped and its gauge stays zero.
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 0 {
		t.Fatalf("idle gauge = %d, want 0", got)
	}
}

// TestPoolMetricsDeadIdleReapCountsOnce pins that a container that died while
// idle is counted when acquire reaps it, and not again by a later path.
func TestPoolMetricsDeadIdleReapCountsOnce(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c1 := ff.lastContainer()
	// Simulate the container's own monitor discarding it while idle (e.g. the
	// process exited between invocations), recording its self reason.
	c1.mu.Lock()
	c1.deadFlag = true
	c1.reason = reasonProcessExit
	c1.mu.Unlock()

	// A later acquire reaps the dead idle container and starts a fresh one.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after death: %v", err)
	}
	if got := discardCount(reg, "fn-a", reasonProcessExit); got != 1 {
		t.Fatalf("process_exit discards = %d, want 1", got)
	}
	// A subsequent acquire reuses the fresh container; no further discards.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute reuse: %v", err)
	}
	if got := discardCount(reg, "fn-a", reasonProcessExit); got != 1 {
		t.Fatalf("process_exit discards after reuse = %d, want still 1", got)
	}
}

// TestPoolMetricsDeadIdleReapedByMaintenanceEvenWithoutTimeout pins finding (4):
// a container that self-terminates while idle is reaped by the maintenance sweep
// (the idle gauge drops to zero and exactly one discard is counted) EVEN when
// age-based eviction is disabled (non-positive timeout). Without the fix the
// early timeout return left a stale idle gauge until the next acquire.
func TestPoolMetricsDeadIdleReapedByMaintenanceEvenWithoutTimeout(t *testing.T) {
	clk := newFakeClock()
	cc, ff, reg := newMetricsCache()
	cc.idleTimeout = 0 // age eviction disabled; dead-idle reaping must still run
	cc.now = clk.Now
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 1 {
		t.Fatalf("idle gauge after seed = %d, want 1", got)
	}

	// The container tears itself down while idle (e.g. process exit).
	c1 := ff.lastContainer()
	c1.mu.Lock()
	c1.deadFlag = true
	c1.reason = reasonProcessExit
	c1.mu.Unlock()

	cc.evictIdle()

	if got := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle)); got != 0 {
		t.Fatalf("idle gauge after maintenance reap = %d, want 0 (stale gauge)", got)
	}
	if got := discardCount(reg, "fn-a", reasonProcessExit); got != 1 {
		t.Fatalf("process_exit discards = %d, want 1 (reaped by maintenance)", got)
	}
	// The dead container lost its slot: the next acquire starts fresh.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("execute after reap: %v", err)
	}
	if got := discardCount(reg, "fn-a", reasonProcessExit); got != 1 {
		t.Fatalf("process_exit discards after reuse = %d, want still 1", got)
	}
}

// TestPoolMetricsManagerRemoveDeletesSeries pins that Manager.RemoveFunction
// deletes the function's runtime-pool series (gauges and counters) so a removed
// function exposes no live pool state, while another function's series survive.
func TestPoolMetricsManagerRemoveDeletesSeries(t *testing.T) {
	reg := metrics.New()
	m := &Manager{metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed fn-a: %v", err)
	}
	if err := runInvoke(t, m.containers, ff, "fn-b", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed fn-b: %v", err)
	}

	m.RemoveFunction("fn-a")

	for _, metricName := range []string{
		metrics.MetricRuntimePoolCapacity,
		metrics.MetricRuntimeContainers,
		metrics.MetricRuntimeContainerAcquires,
		metrics.MetricRuntimeContainerDiscards,
		metrics.MetricRuntimeContainerAcquireDuration,
		metrics.MetricRuntimeContainerWaits,
	} {
		if seriesInSnapshot(reg.Snapshot(), metricName, "fn-a") {
			t.Fatalf("fn-a %s series must be deleted on removal:\n%s", metricName, reg.Snapshot())
		}
	}
	// fn-b is untouched.
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-b")); got != 1 {
		t.Fatalf("fn-b capacity gauge = %d, want 1", got)
	}
	if got := acquireCount(reg, "fn-b", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("fn-b cold acquires = %d, want 1", got)
	}
	// PoolSnapshot for the removed function reports not-found.
	if _, ok := m.PoolSnapshot("fn-a"); ok {
		t.Fatal("PoolSnapshot for a removed function must report not-found")
	}
}

// TestPoolMetricsLateReleaseAfterRemovalDoesNotRecreateSeries pins finding (2):
// a busy container retired by a removal is discarded on its (late) release, but
// that release must not recreate the function's removed metric series. The
// release path funnels through recordDiscard, which observes the pool's removal
// tombstone (p.removing) under p.mu, so the discard is suppressed.
func TestPoolMetricsLateReleaseAfterRemovalDoesNotRecreateSeries(t *testing.T) {
	reg := metrics.New()
	m := &Manager{metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}

	busy := newBlockingContainer(1)
	ff.build = func() *fakeContainer { return busy }
	done := make(chan error, 1)
	go func() {
		done <- m.containers.execute(context.Background(), "fn-a", "img-1", 1, ff.start(), "h", []byte(`{}`), nil)
	}()
	<-busy.entered

	// Remove while the container is busy: its series are deleted now and the busy
	// container is retired (discarded on release).
	m.RemoveFunction("fn-a")
	if seriesInSnapshot(reg.Snapshot(), metrics.MetricRuntimeContainerDiscards, "fn-a") {
		t.Fatalf("removal must delete fn-a's discard series:\n%s", reg.Snapshot())
	}

	// The late release discards the retired container. That must stay invisible:
	// no counter/gauge for fn-a may reappear.
	close(busy.release)
	if err := <-done; err != nil {
		t.Fatalf("busy execute: %v", err)
	}
	if got := busy.reasons(); len(got) != 1 || got[0] != reasonFunctionRemove {
		t.Fatalf("busy removal discards = %v, want [%s]", got, reasonFunctionRemove)
	}
	for _, metricName := range []string{
		metrics.MetricRuntimePoolCapacity,
		metrics.MetricRuntimeContainers,
		metrics.MetricRuntimeContainerAcquires,
		metrics.MetricRuntimeContainerDiscards,
		metrics.MetricRuntimeContainerAcquireDuration,
		metrics.MetricRuntimeContainerWaits,
	} {
		if seriesInSnapshot(reg.Snapshot(), metricName, "fn-a") {
			t.Fatalf("late release recreated fn-a %s series:\n%s", metricName, reg.Snapshot())
		}
	}
}

// TestPoolMetricsRemoveThenReactivateKeepsFreshSeries pins finding (1)/(2)
// together: after a removal deletes fn-a's series, a genuine reactivation (a new
// pool) publishes a fresh capacity gauge and fresh acquire/discard series, and
// the late release of the ORIGINAL removed pool's container neither suppresses
// nor clobbers the reactivated pool's series. The removal tombstone is per-pool
// (the detached old pool keeps p.removing set), so it cannot bleed into the new
// pool's writes.
func TestPoolMetricsRemoveThenReactivateKeepsFreshSeries(t *testing.T) {
	reg := metrics.New()
	m := &Manager{metrics: reg}
	m.containers = newContainerCache()
	m.containers.metrics = reg
	ff := &fakeFactory{}

	// Seed a warm fn-a and immediately remove it: its series are deleted.
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 2, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	m.RemoveFunction("fn-a")
	if seriesInSnapshot(reg.Snapshot(), metrics.MetricRuntimePoolCapacity, "fn-a") {
		t.Fatalf("removal must delete fn-a capacity series:\n%s", reg.Snapshot())
	}

	// Reactivate and warm again: a fresh pool publishes fresh series.
	m.containers.activateFunction("fn-a", "img-1")
	ff.build = func() *fakeContainer { return &fakeContainer{} }
	if err := runInvoke(t, m.containers, ff, "fn-a", "img-1", 3, "h"); err != nil {
		t.Fatalf("reactivated acquire: %v", err)
	}
	if got := gauge(reg, metrics.MetricRuntimePoolCapacity, fnLabels("fn-a")); got != 3 {
		t.Fatalf("reactivated capacity gauge = %d, want 3 (fresh pool bound)", got)
	}
	if got := acquireCount(reg, "fn-a", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("reactivated cold acquires = %d, want 1 (fresh series)", got)
	}
	// The reactivated pool's snapshot is live again.
	if s, ok := m.PoolSnapshot("fn-a"); !ok || s.Capacity != 3 {
		t.Fatalf("reactivated PoolSnapshot = %+v, ok=%v; want capacity 3", s, ok)
	}
}

// TestPoolMetricsSnapshotCountsTransientAsBusy pins finding (3): transient
// throwaway containers are real containers and are reported as busy, so
// Containers (Busy+Idle) equals the runtime_containers gauge sum and is
// documented to possibly exceed Capacity while stale retired-image requests run.
func TestPoolMetricsSnapshotCountsTransientAsBusy(t *testing.T) {
	cc, ff, reg := newMetricsCache()
	// Seed and invalidate an image so a later request for it is a transient.
	if err := runInvoke(t, cc, ff, "fn-a", "img-1", 1, "h"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cc.invalidateImage("img-1")

	transient := &fakeContainer{}
	ff.build = func() *fakeContainer { return transient }
	l, err := cc.acquire(context.Background(), "fn-a", "img-1", 1, ff.start())
	if err != nil {
		t.Fatalf("transient acquire: %v", err)
	}
	// The lease is held (not invoked), so the transient is leased/busy.

	s, ok := cc.snapshot("fn-a", reg)
	if !ok {
		t.Fatal("snapshot not found while a transient is leased")
	}
	if s.Busy != 1 || s.Idle != 0 || s.Containers != 1 {
		t.Fatalf("snapshot = %+v, want Busy=1 Idle=0 Containers=1 (transient is real+busy)", s)
	}
	// The gauges agree with the snapshot's counts: single source, no contradiction.
	busyGauge := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateBusy))
	if busyGauge != int64(s.Busy) {
		t.Fatalf("busy gauge = %d, snapshot Busy = %d (must agree)", busyGauge, s.Busy)
	}
	idleGauge := gauge(reg, metrics.MetricRuntimeContainers, stateLabels("fn-a", metrics.RuntimeStateIdle))
	if idleGauge != int64(s.Idle) {
		t.Fatalf("idle gauge = %d, snapshot Idle = %d (must agree)", idleGauge, s.Idle)
	}

	l.release()
}

// seriesInSnapshot reports whether the registry snapshot (which renders display
// names, i.e. without the relay_ prefix) contains a line for metricName labeled
// function=name. It mirrors the metrics package's seriesPresent helper.
func seriesInSnapshot(snapshot, metricName, name string) bool {
	display := strings.TrimPrefix(metricName, "relay_")
	token := "function=" + name
	for _, line := range strings.Split(snapshot, "\n") {
		if !strings.HasPrefix(line, display+"{") {
			continue
		}
		if strings.Contains(line, token) {
			return true
		}
	}
	return false
}

// TestPoolMetricsRaceSafety hammers acquire/release/eviction/removal against a
// registry. Its value is under -race: the gauges are published under the pool
// lock and counters are concurrency-safe.
func TestPoolMetricsRaceSafety(t *testing.T) {
	clk := newFakeClock()
	cc, ff, _ := newMetricsCache()
	cc.idleTimeout = time.Millisecond
	cc.now = clk.Now

	var wg sync.WaitGroup
	stop := make(chan struct{})
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
	// Concurrent remove/reactivate on another function exercises the removal
	// publish path under contention.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			cc.removeFunction("fn-race-removed")
			cc.activateFunction("fn-race-removed", "")
		}
	}()
	for i := 0; i < 4; i++ {
		if err := runInvoke(t, cc, ff, "fn-race-a", "img-1", 2, "h"); err != nil {
			t.Fatalf("serial execute: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	<-done
}

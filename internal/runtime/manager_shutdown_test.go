package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"

	"relay/internal/testutil"
)

// TestNewManagerBoundedPingOnLifecycle is the deterministic ping-bound test: a
// daemon that never answers must make NewManager return within the lifecycle's
// bound rather than hanging forever. The ping is injected (no daemon needed) and
// blocks until its context is done, and the manager lifecycle carries a short
// deadline, so NewManager must surface the context error promptly.
func TestNewManagerBoundedPingOnLifecycle(t *testing.T) {
	lifecycle, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := NewManager(
		testutil.DiscardLogger(),
		nil,
		"test-host",
		WithLifecycleContext(lifecycle),
		withPing(func(ctx context.Context, _ *client.Client) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("NewManager returned nil for a daemon that never answers; want a bounded ping error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("NewManager ping error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("NewManager ping took %v; want it bounded by the short lifecycle", elapsed)
	}
}

// TestNewManagerPingCancelledLifecycleIsNotAHang pins that a cancelled lifecycle
// also releases the ping promptly (shutdown during startup), returning the
// context error rather than waiting out the ping cap.
func TestNewManagerPingCancelledLifecycleIsNotAHang(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := NewManager(
		testutil.DiscardLogger(),
		nil,
		"test-host",
		WithLifecycleContext(lifecycle),
		withPing(func(ctx context.Context, _ *client.Client) error {
			<-ctx.Done()
			return ctx.Err()
		}),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NewManager error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("NewManager took %v on a cancelled lifecycle; want prompt return", elapsed)
	}
}

// ctxFakeContainer is a reusableContainer whose context-aware teardown records
// the context it observed and can block until the context is done. It models the
// production executionContainer's ctx-aware kill/remove, so the manager
// cleanup's context propagation and bounded parallelism are testable without
// Docker.
type ctxFakeContainer struct {
	mu         sync.Mutex
	deadFlag   bool
	reason     string
	attempts   int
	observed   context.Context
	blockOnCTX bool

	// onDiscard, when non-nil, is invoked once when this container's teardown
	// enters (before it blocks), so a test can deterministically observe that a
	// teardown — e.g. an in-flight maintenance eviction — is underway.
	onDiscard func()

	// active/peak track concurrent teardown to prove the bounded worker pool.
	active int
	peak   int
	// delay, when > 0, makes each teardown take that long (to expose serial
	// delay).
	delay time.Duration
}

func (f *ctxFakeContainer) Invoke(context.Context, string, []byte, map[string]string) error {
	return nil
}

func (f *ctxFakeContainer) discard(reason string) bool {
	return f.discardContext(context.Background(), reason)
}

func (f *ctxFakeContainer) discardContext(ctx context.Context, reason string) bool {
	f.mu.Lock()
	f.attempts++
	if f.deadFlag {
		f.mu.Unlock()
		return false
	}
	f.deadFlag = true
	f.reason = reason
	f.observed = ctx
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	block := f.blockOnCTX
	delay := f.delay
	onDiscard := f.onDiscard
	f.mu.Unlock()

	if onDiscard != nil {
		onDiscard()
	}

	if block {
		<-ctx.Done()
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	f.active--
	f.mu.Unlock()
	return true
}

func (f *ctxFakeContainer) discardReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reason
}

func (f *ctxFakeContainer) dead() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deadFlag
}

// seedIdleCtxContainer places one idle ctx-aware container into the cache for
// fnName/image. It acquires a lease through the cache and releases it, so the
// pool's bookkeeping (generation, notify) is realistic.
func seedIdleCtxContainer(t *testing.T, cc *containerCache, fnName, image string, c *ctxFakeContainer) {
	t.Helper()
	start := func() (reusableContainer, error) { return c, nil }
	l, err := cc.acquire(context.Background(), fnName, image, 1, start)
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	l.release()
}

// TestManagerCloseContextIsContextAware proves Manager.CloseContext propagates
// its context to every container teardown: a container whose teardown blocks
// until ctx is done observes the manager's cancellation and CloseContext returns
// promptly (rather than waiting out the container's own detached bound).
func TestManagerCloseContextIsContextAware(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	c := &ctxFakeContainer{blockOnCTX: true}
	seedIdleCtxContainer(t, m.containers, "fn-a", "img-1", c)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := m.CloseContext(ctx); err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v; its context must bound the teardown", elapsed)
	}
	c.mu.Lock()
	observed := c.observed
	dead := c.deadFlag
	c.mu.Unlock()
	if !dead {
		t.Fatal("container was not discarded by CloseContext")
	}
	if observed == nil || !errors.Is(observed.Err(), context.DeadlineExceeded) {
		t.Fatalf("container teardown ctx.Err()=%v; want the manager's deadline", observed.Err())
	}
}

// TestManagerCloseContextManyWarmContainersBounded proves a large warm pool is
// torn down with bounded parallelism, not O(N) serial delays: with N containers
// each taking a fixed delay, the wall time must be far below the serial total,
// and the observed concurrency must never exceed containerShutdownConcurrency.
func TestManagerCloseContextManyWarmContainersBounded(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	const n = 40
	const perDiscard = 20 * time.Millisecond

	containers := make([]*ctxFakeContainer, 0, n)
	for i := 0; i < n; i++ {
		c := &ctxFakeContainer{delay: perDiscard}
		containers = append(containers, c)
		seedIdleCtxContainer(t, m.containers, fmt.Sprintf("fn-%d", i), "img", c)
	}

	start := time.Now()
	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	elapsed := time.Since(start)

	// Serial would be n*perDiscard = 800ms. Bounded parallel must be far less.
	if elapsed >= 400*time.Millisecond {
		t.Fatalf("CloseContext took %v for %d containers; want bounded parallel (serial=%v)",
			elapsed, n, n*perDiscard)
	}
	peak := 0
	for _, c := range containers {
		c.mu.Lock()
		if c.peak > peak {
			peak = c.peak
		}
		if !c.deadFlag {
			c.mu.Unlock()
			t.Fatal("a warm container survived CloseContext")
		}
		c.mu.Unlock()
	}
	if peak > containerShutdownConcurrency {
		t.Fatalf("peak concurrent teardown = %d, want <= %d", peak, containerShutdownConcurrency)
	}
	if peak < 1 {
		t.Fatal("no container teardown observed")
	}
}

// TestManagerCloseContextRepeatSafe proves CloseContext is idempotent: a second
// call returns the stored result without re-running teardown, and the containers
// are discarded exactly once. It also proves the serial Close convenience form
// is safe after CloseContext.
func TestManagerCloseContextRepeatSafe(t *testing.T) {
	m := &Manager{containers: newContainerCache()}
	c := &ctxFakeContainer{}
	seedIdleCtxContainer(t, m.containers, "fn-a", "img-1", c)

	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("first CloseContext: %v", err)
	}
	if err := m.CloseContext(context.Background()); err != nil {
		t.Fatalf("second CloseContext: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close after CloseContext: %v", err)
	}
	c.mu.Lock()
	attempts := c.attempts
	dead := c.deadFlag
	c.mu.Unlock()
	if !dead {
		t.Fatal("container not discarded")
	}
	if attempts != 1 {
		t.Fatalf("container teardown attempts = %d, want exactly 1 (idempotent)", attempts)
	}
}

// TestManagerCloseContextBoundedDuringMaintenanceEviction is the lifecycle edge
// regression: CloseContext first cancels the manager lifecycle and then joins
// the maintenance loop, but that loop may already be INSIDE an idle-eviction
// pass whose container teardown blocks. The maintenance pass drives its teardown
// on the manager lifecycle, so CloseContext's lifecycle cancellation reaches the
// in-flight pass and makes it (and therefore the maintDone join) return promptly,
// rather than waiting out each blocked container's per-operation cap serially.
//
// The containers' teardown blocks until its context is done, and their context
// is the manager lifecycle (NOT CloseContext's own supplied bound), so the test
// also pins that lifecycle cancellation — not the caller's deadline — is what
// releases an in-flight eviction.
func TestManagerCloseContextBoundedDuringMaintenanceEviction(t *testing.T) {
	clk := newFakeClock()
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	m := &Manager{
		log:             testutil.DiscardLogger(),
		done:            make(chan struct{}),
		maintDone:       make(chan struct{}),
		lifecycle:       lifecycle,
		lifecycleCancel: cancelLifecycle,
	}
	m.containers = newContainerCache()
	m.containers.idleTimeout = time.Minute
	m.containers.now = clk.Now

	// Several idle containers across distinct functions. The eviction pass walks
	// pools serially, so only the first teardown blocks; the rest block only
	// until the shared lifecycle is cancelled. entered closes on the first
	// teardown regardless of map order, making the "eviction is in flight"
	// observation deterministic.
	const n = 4
	entered := make(chan struct{})
	var enteredOnce sync.Once
	containers := make([]*ctxFakeContainer, 0, n)
	for i := 0; i < n; i++ {
		c := &ctxFakeContainer{blockOnCTX: true}
		c.onDiscard = func() { enteredOnce.Do(func() { close(entered) }) }
		containers = append(containers, c)
		seedIdleCtxContainer(t, m.containers, fmt.Sprintf("fn-%d", i), "img", c)
	}

	m.startMaintenance(5 * time.Millisecond)
	clk.Advance(2 * time.Minute)

	// Wait until the maintenance loop is demonstrably INSIDE a teardown.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance eviction never entered a container teardown")
	}

	// CloseContext carries a deliberately generous deadline: the prompt return
	// must come from the lifecycle cancellation reaching the in-flight eviction,
	// not from the caller's bound. It runs on its own goroutine so a regression
	// (the unconditional maintDone join never unblocking) fails the test cleanly
	// instead of hanging to the go-test timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closeDone := make(chan error, 1)
	start := time.Now()
	go func() { closeDone <- m.CloseContext(ctx) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseContext: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseContext blocked on the maintenance join while an eviction was in flight")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CloseContext took %v while maintenance was evicting; want prompt return", elapsed)
	}
	for i, c := range containers {
		c.mu.Lock()
		dead := c.deadFlag
		c.mu.Unlock()
		if !dead {
			t.Fatalf("container %d was not discarded by the cancelled eviction", i)
		}
	}
}

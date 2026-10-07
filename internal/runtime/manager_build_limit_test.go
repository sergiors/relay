package runtime

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
)

// permitPark is a deterministic barrier around the afterBuildPermit seam. It
// records how many runtime-backed preparations are inside the bounded section at
// once (active/peak) and blocks each arrival on its own release channel, so a
// test can hold preparations inside the section and observe the per-worker build
// cap without sleeps. Every wait is a channel signal at an exact production
// boundary; time.After is a deadlock failure bound only.
type permitPark struct {
	mu       sync.Mutex
	active   int
	peak     int
	slots    []chan struct{}
	arrival  chan struct{}
	released bool
	release  chan struct{}
}

func newPermitPark(buffer int) *permitPark {
	return &permitPark{
		arrival: make(chan struct{}, buffer),
		release: make(chan struct{}),
	}
}

// enter is the seam function. It increments the active count, signals one
// arrival, then blocks until this arrival's own slot or the whole park is
// released. On return it decrements the active count, so peak is the observable
// maximum concurrency inside the bounded section.
func (p *permitPark) enter() {
	p.mu.Lock()
	p.active++
	if p.active > p.peak {
		p.peak = p.active
	}
	slot := make(chan struct{})
	p.slots = append(p.slots, slot)
	p.mu.Unlock()

	p.arrival <- struct{}{}

	select {
	case <-slot:
	case <-p.release:
	}

	p.mu.Lock()
	p.active--
	p.mu.Unlock()
}

// waitArrival blocks until one preparation has entered the bounded section.
func (p *permitPark) waitArrival(t *testing.T) {
	t.Helper()
	select {
	case <-p.arrival:
	case <-time.After(2 * time.Second):
		t.Fatal("a preparation did not enter the bounded section in time")
	}
}

// assertNoArrival proves, without sleeping, that no further preparation has
// entered the bounded section: the arrival channel is drained by waitArrival, so
// a queued token means an unexpected arrival and an empty buffer means none.
func (p *permitPark) assertNoArrival(t *testing.T) {
	t.Helper()
	select {
	case <-p.arrival:
		t.Fatal("a preparation entered the bounded section while no permit was free")
	default:
	}
}

// peakActive returns the highest concurrent occupancy observed.
func (p *permitPark) peakActive() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// slot returns the release channel of the idx-th arrival.
func (p *permitPark) slot(idx int) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.slots[idx]
}

// releaseAll unblocks every held arrival (and any later one) exactly once.
func (p *permitPark) releaseAll() {
	p.mu.Lock()
	if !p.released {
		p.released = true
		close(p.release)
	}
	p.mu.Unlock()
}

// writeBuildApp writes a minimal Node app that requires a runtime-backed
// preparation (so it enters the bounded section) with a unique name.
func writeBuildApp(t *testing.T, name string) app.App {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function h(){ return 'ok'; }\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	return app.App{Name: name, Dir: dir, Template: &app.Template{Runtime: "node24"}}
}

// buildCapManager builds a Manager directly (no daemon) whose preparation
// limiter has the given capacity and whose scripted daemon always misses the
// image probe and reports the supplied build body. builds, when non-nil, is
// incremented atomically for every build request observed. It is the minimal
// wiring the runtime-backed prepare path needs.
func buildCapManager(t *testing.T, capacity int, buildBody string, builds *atomic.Int64) *Manager {
	t.Helper()
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodPost, path: "/build", body: buildBody, onMatch: func() {
			if builds != nil {
				builds.Add(1)
			}
		}},
	)
	m := newLifecycleManager(t, cli, context.Background())
	m.buildLimit.Store(newBuildLimiter(capacity))
	return m
}

// prepareAsync starts a Prepare in a goroutine and returns its error channel.
func prepareAsync(m *Manager, fn app.App, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := m.Prepare(ctx, fn)
		done <- err
	}()
	return done
}

// TestPrepareBuildLimitBoundsConcurrentPipelines is the deterministic proof of
// the per-worker MAX_CONCURRENT_BUILDS cap: with capacity 2 and three direct
// Prepare calls, only two enter the bounded section at once, the third waits for
// a free permit, and releasing one lets the third proceed. No sleeps: the
// afterBuildPermit seam parks each preparation inside the section.
func TestPrepareBuildLimitBoundsConcurrentPipelines(t *testing.T) {
	park := newPermitPark(8)
	defer park.releaseAll()

	var builds atomic.Int64
	m := buildCapManager(t, 2, `{"stream":"ok"}`, &builds)
	m.afterBuildPermit = park.enter

	apps := []app.App{writeBuildApp(t, "cap-a"), writeBuildApp(t, "cap-b"), writeBuildApp(t, "cap-c")}
	dones := make([]<-chan error, len(apps))
	for i, fn := range apps {
		dones[i] = prepareAsync(m, fn, context.Background())
	}

	// Exactly two enter the bounded section; the third is blocked on the permit.
	park.waitArrival(t)
	park.waitArrival(t)
	park.assertNoArrival(t)
	if got := park.peakActive(); got != 2 {
		t.Fatalf("peak in-section preparations = %d, want 2 (the cap)", got)
	}

	// Release the first; its permit frees and the third may now enter.
	close(park.slot(0))
	park.waitArrival(t)
	if got := park.peakActive(); got > 2 {
		t.Fatalf("peak in-section preparations = %d, want <= 2", got)
	}

	// Release the remaining held preparations and collect every result.
	park.releaseAll()
	for i, done := range dones {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("prepare %d failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("prepare %d did not finish after release", i)
		}
	}
	if got := park.peakActive(); got > 2 {
		t.Fatalf("observed peak %d, want <= 2", got)
	}
}

// TestPrepareBuildLimitCancelledWaiterReturns proves a third Prepare cancelled
// while waiting for a permit returns context.Canceled, never enters the bounded
// section, and acquires no slot. The two held preparations keep both permits
// busy; the cancelled third must not run (no third arrival, no build issued).
func TestPrepareBuildLimitCancelledWaiterReturns(t *testing.T) {
	park := newPermitPark(8)
	defer park.releaseAll()

	var builds atomic.Int64
	m := buildCapManager(t, 2, `{"stream":"ok"}`, &builds)
	m.afterBuildPermit = park.enter

	apps := []app.App{writeBuildApp(t, "cancel-a"), writeBuildApp(t, "cancel-b"), writeBuildApp(t, "cancel-c")}
	held := make([]<-chan error, 2)
	for i := 0; i < 2; i++ {
		held[i] = prepareAsync(m, apps[i], context.Background())
	}
	park.waitArrival(t)
	park.waitArrival(t)

	// Both permits are busy and neither held preparation has built yet.
	if got := builds.Load(); got != 0 {
		t.Fatalf("builds issued = %d while both permits are held in the bounded section, want 0", got)
	}

	// The third Prepare is cancelled while waiting for a permit.
	waitCtx, cancelWait := context.WithCancel(context.Background())
	third := prepareAsync(m, apps[2], waitCtx)
	cancelWait()
	select {
	case err := <-third:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return promptly")
	}
	// The cancelled preparation never entered the bounded section or built.
	park.assertNoArrival(t)
	if got := builds.Load(); got != 0 {
		t.Fatalf("builds issued = %d after the cancelled waiter returned, want 0", got)
	}

	// The two held permits are still live; release them.
	park.releaseAll()
	for i, done := range held {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("held prepare %d failed: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("held prepare %d did not finish", i)
		}
	}
}

// TestPrepareBuildLimitCancelledReadyBothStartsNoBuild is the deterministic
// proof of cancellation preference when a permit is ALREADY free and the caller's
// context is ALREADY cancelled: acquire must lose deterministically rather than
// let select pick pseudo-randomly between the ready permit and ctx.Done. A
// capacity-1 limiter is deliberately free, so the permit-send arm is ready; the
// cancelled prepare must still return context.Canceled, leave the slot untouched,
// never enter the bounded section (afterBuildPermit not fired), and issue no
// build. A follow-up preparation then proves the solo slot is intact.
func TestPrepareBuildLimitCancelledReadyBothStartsNoBuild(t *testing.T) {
	var builds atomic.Int64
	m := buildCapManager(t, 1, `{"stream":"ok"}`, &builds)
	entered := make(chan struct{}, 1)
	m.afterBuildPermit = func() { entered <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := m.Prepare(ctx, writeBuildApp(t, "cancel-ready")); !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare error = %v, want context.Canceled", err)
	}
	select {
	case <-entered:
		t.Fatal("a cancelled preparation entered the bounded section")
	default:
	}
	if got := builds.Load(); got != 0 {
		t.Fatalf("builds issued = %d after a cancelled preparation, want 0", got)
	}
	if got := len(m.buildLimiterFor().permits); got != 0 {
		t.Fatalf("permit capacity in use = %d after a cancelled preparation, want 0 (a slot leaked)", got)
	}

	// The solo permit is intact: a follow-up preparation acquires it, enters the
	// bounded section, builds, and releases it.
	m.afterBuildPermit = nil
	if _, err := m.Prepare(context.Background(), writeBuildApp(t, "cancel-ready-followup")); err != nil {
		t.Fatalf("follow-up preparation after a cancelled one: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("builds issued = %d after the follow-up, want 1", got)
	}
	if got := len(m.buildLimiterFor().permits); got != 0 {
		t.Fatalf("permit capacity in use = %d after the follow-up, want 0", got)
	}
}

// TestBuildLimiterAcquirePrefersCancellationOverFreePermit drives acquire
// directly, repeated so the ready-both select is exercised under -race: with a
// free permit and an already-cancelled context it must always return
// context.Canceled and never consume the permit. A successful acquire followed by
// release proves the limiter still functions and capacity is whole.
func TestBuildLimiterAcquirePrefersCancellationOverFreePermit(t *testing.T) {
	l := newBuildLimiter(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 1000; i++ {
		if err := l.acquire(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("acquire #%d error = %v, want context.Canceled", i, err)
		}
		if got := len(l.permits); got != 0 {
			t.Fatalf("acquire #%d left %d permit(s) in use, want 0", i, got)
		}
	}

	// The permit is still obtainable by a live caller.
	if err := l.acquire(context.Background()); err != nil {
		t.Fatalf("acquire with a live context: %v", err)
	}
	if got := len(l.permits); got != 1 {
		t.Fatalf("permit in use after a successful acquire = %d, want 1", got)
	}
	l.release()
	if got := len(l.permits); got != 0 {
		t.Fatalf("permit in use after release = %d, want 0", got)
	}
}

// TestPrepareBuildLimitReleasesPermit proves the permit is released on every
// exit path: success, build failure, a panic inside the bounded section, and
// manager-lifecycle cancellation. Each subtest fills a capacity-1 limiter with a
// first preparation and then proves a second Preparation can acquire the permit
// (it would block forever otherwise), so no path leaks a slot.
func TestPrepareBuildLimitReleasesPermit(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		m := buildCapManager(t, 1, `{"stream":"ok"}`, nil)
		first := writeBuildApp(t, "release-success-1")
		second := writeBuildApp(t, "release-success-2")
		assertPermitReleased(t, m, func() error {
			_, err := m.Prepare(context.Background(), first)
			return err
		}, second)
	})

	t.Run("build failure", func(t *testing.T) {
		// The first build fails; subsequent builds succeed, so the follow-up
		// preparation proves the permit was released by actually completing.
		var buildCalls atomic.Int64
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			dockerRoute{
				method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
				fail: func(req *http.Request) error {
					if buildCalls.Add(1) == 1 {
						return errors.New("boom")
					}
					return nil
				},
			},
		)
		m := newLifecycleManager(t, cli, context.Background())
		m.buildLimit.Store(newBuildLimiter(1))
		first := writeBuildApp(t, "release-fail-1")
		second := writeBuildApp(t, "release-fail-2")
		assertPermitReleased(t, m, func() error {
			_, err := m.Prepare(context.Background(), first)
			if err == nil {
				t.Fatal("expected the scripted build failure to surface")
			}
			return nil
		}, second)
	})

	t.Run("panic", func(t *testing.T) {
		m := buildCapManager(t, 1, `{"stream":"ok"}`, nil)
		m.afterBuildPermit = func() { panic("boom in the bounded section") }
		first := writeBuildApp(t, "release-panic-1")
		second := writeBuildApp(t, "release-panic-2")
		assertPermitReleased(t, m, func() error {
			defer func() {
				if recover() == nil {
					t.Fatal("expected the seam panic to propagate")
				}
				// Clear the panicking seam so the follow-up preparation reaches
				// the build path.
				m.afterBuildPermit = nil
			}()
			_, _ = m.Prepare(context.Background(), first)
			return nil
		}, second)
	})

	t.Run("lifecycle cancellation", func(t *testing.T) {
		lifecycle, cancelLifecycle := context.WithCancel(context.Background())
		defer cancelLifecycle()

		entered := make(chan struct{})
		var enteredOnce sync.Once
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodGet, path: "/images/", status: http.StatusNotFound, body: `{"message":"no such image"}`},
			dockerRoute{
				method: http.MethodPost, path: "/build",
				onRequest: func(req *http.Request) { enteredOnce.Do(func() { close(entered) }) },
				fail: func(req *http.Request) error {
					<-req.Context().Done()
					return req.Context().Err()
				},
			},
		)
		m := newLifecycleManager(t, cli, lifecycle)
		m.buildLimit.Store(newBuildLimiter(1))
		first := writeBuildApp(t, "release-cancel-1")
		second := writeBuildApp(t, "release-cancel-2")

		firstDone := prepareAsync(m, first, context.Background())
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("the first build was not entered")
		}
		cancelLifecycle()
		select {
		case err := <-firstDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled prepare error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("lifecycle cancellation did not cancel the active build")
		}

		// The permit must have been released: a second Prepare acquires it and
		// proceeds (it still fails because the lifecycle is cancelled, but it
		// must not block on the permit).
		done := prepareAsync(m, second, context.Background())
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("permit was not released after lifecycle cancellation")
		}
	})
}

// assertPermitReleased runs first (which occupies the sole permit and then
// exits for any reason) and then proves a second Prepare can acquire the permit
// and reach the build path. The second run is bounded so a leaked permit fails
// the test instead of hanging.
func assertPermitReleased(t *testing.T, m *Manager, first func() error, second app.App) {
	t.Helper()
	if err := first(); err != nil {
		t.Fatalf("first preparation: %v", err)
	}
	done := prepareAsync(m, second, context.Background())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second preparation after a returned first: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second preparation blocked on a leaked build permit")
	}
}

// TestBuildLimiterDirectConstructionIsBounded proves a Manager built directly
// (bypassing NewManager) still gets a bounded limiter via the lazy fallback, and
// that the fallback is race-safe: concurrent preparations share ONE limiter
// instead of each creating its own.
func TestBuildLimiterDirectConstructionIsBounded(t *testing.T) {
	var m Manager
	const goroutines = 16
	limiters := make([]*buildLimiter, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			limiters[idx] = m.buildLimiterFor()
		}(i)
	}
	wg.Wait()
	for i := 1; i < len(limiters); i++ {
		if limiters[i] != limiters[0] {
			t.Fatalf("buildLimiterFor returned distinct limiters (%p vs %p); the fallback must be race-safe and shared",
				limiters[i], limiters[0])
		}
	}
	if got := cap(limiters[0].permits); got != DefaultMaxConcurrentBuilds {
		t.Fatalf("direct-construction limiter capacity = %d, want default %d", got, DefaultMaxConcurrentBuilds)
	}
}

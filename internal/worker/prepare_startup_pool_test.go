package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
)

// poolPark is one deterministic per-app gate around a blockingPreparer call. It
// closes arrived exactly when the app's preparation starts, blocks that call on
// its own release (or the caller's ctx), and closes returned exactly when the
// call completes — so a test admits apps one at a time by channel signal, never
// by sleep, and can prove prepareApps joins the work it already admitted.
type poolPark struct {
	arrived     chan struct{}
	arrivedOnce sync.Once
	release     chan struct{}
	releaseOnce sync.Once
	returned    chan struct{}
	returnOnce  sync.Once
}

func newPoolPark() *poolPark {
	return &poolPark{
		arrived:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

// await blocks this preparation until its gate is released or ctx ends.
func (p *poolPark) await(ctx context.Context) error {
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *poolPark) markArrived()  { p.arrivedOnce.Do(func() { close(p.arrived) }) }
func (p *poolPark) free()         { p.releaseOnce.Do(func() { close(p.release) }) }
func (p *poolPark) markReturned() { p.returnOnce.Do(func() { close(p.returned) }) }

// blockingPreparer stands in for the runtime Manager behind the appPreparer
// seam while behaving like a real blocking build: it parks inside
// PrepareWithFingerprintAndSelection at the caller's gates, counts live
// concurrency, and reports the configured effective MAX_CONCURRENT_BUILDS —
// the value prepareApps sizes its worker pool with. An app with no
// pre-created park runs immediately (it never blocks); an app listed in fail
// returns that error after unblocking. Parks are fixed before the goroutines
// start, so only the guarded counters change concurrently.
type blockingPreparer struct {
	maxConcurrentBuilds int

	// parks is built once before any Prepare and read-only afterwards.
	parks map[string]*poolPark

	failMu sync.Mutex
	fail   map[string]error

	mu     sync.Mutex
	calls  int
	active int
	peak   int
}

func newBlockingPreparer(max int, parks map[string]*poolPark) *blockingPreparer {
	return &blockingPreparer{maxConcurrentBuilds: max, parks: parks}
}

func (b *blockingPreparer) setFail(name string, err error) {
	b.failMu.Lock()
	defer b.failMu.Unlock()
	if b.fail == nil {
		b.fail = make(map[string]error)
	}
	b.fail[name] = err
}

func (b *blockingPreparer) failFor(name string) error {
	b.failMu.Lock()
	defer b.failMu.Unlock()
	return b.fail[name]
}

func (b *blockingPreparer) PrepareWithFingerprintAndSelection(
	ctx context.Context,
	fn app.App,
	_ string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	b.mu.Lock()
	b.calls++
	b.active++
	if b.active > b.peak {
		b.peak = b.active
	}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.active--
		b.mu.Unlock()
	}()

	// A scripted failure exits immediately, before any park: it models a build
	// that fails without blocking a pool worker behind it.
	if err := b.failFor(fn.Name); err != nil {
		return nil, err
	}

	if park := b.parks[fn.Name]; park != nil {
		park.markArrived()
		if err := park.await(ctx); err != nil {
			park.markReturned()
			return nil, err
		}
		park.markReturned()
	}

	return &runtime.Prepared{
		Name:        fn.Name,
		Image:       "img-" + fn.Name,
		Fingerprint: "built-" + fn.Name,
	}, nil
}

func (b *blockingPreparer) MaxConcurrentBuilds() int {
	if b.maxConcurrentBuilds > 0 {
		return b.maxConcurrentBuilds
	}
	return runtime.DefaultMaxConcurrentBuilds
}

func (b *blockingPreparer) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

func (b *blockingPreparer) peakActive() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

func (b *blockingPreparer) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// startupAppsFor returns hand-built startup records in the given names' order;
// prepareApps receives them verbatim, and the spy never touches the filesystem.
func startupAppsFor(names ...string) []startupApp {
	startup := make([]startupApp, 0, len(names))
	for _, name := range names {
		startup = append(startup, startupApp{
			App:         app.App{Name: name, Template: &app.Template{Runtime: "python3.14"}},
			Fingerprint: "fp-" + name,
		})
	}
	return startup
}

// waitArrived blocks until the named app's preparation started; the timeout is
// a deadlock failure bound only.
func waitArrivedIn(t *testing.T, parks map[string]*poolPark, names ...string) {
	t.Helper()
	for _, name := range names {
		select {
		case <-parks[name].arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s did not arrive in time", name)
		}
	}
}

// assertNotArrived proves the named apps' preparations have not started: a
// closed arrived channel is only produced by an actual Prepare call.
func assertNotArrived(t *testing.T, parks map[string]*poolPark, names ...string) {
	t.Helper()
	for _, name := range names {
		select {
		case <-parks[name].arrived:
			t.Fatalf("%s arrived unexpectedly; the pool over-admitted", name)
		default:
		}
	}
}

// assertNotReturned proves prepareApps has NOT finished: the only reader of
// this channel is the test, so an available result means it returned while
// gated preparations were still parked inside the workers.
func assertNotReturned(t *testing.T, done <-chan []*runner.PreparedApp) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("prepareApps returned while parked preparations were unfinished: %+v", got)
	default:
	}
}

// collectPrepared waits for prepareApps' result and asserts startup-order
// preservation plus the expected availability of each entry by name.
func collectPrepared(t *testing.T, done <-chan []*runner.PreparedApp, names []string, unavailable map[string]bool) []*runner.PreparedApp {
	t.Helper()
	select {
	case prepared := <-done:
		if len(prepared) != len(names) {
			t.Fatalf("prepared = %d entries, want %d", len(prepared), len(names))
		}
		for i, pf := range prepared {
			if pf.Name() != names[i] {
				t.Fatalf("prepared[%d].Name() = %q, want %q (startup order preserved)", i, pf.Name(), names[i])
			}
			if unavailable[pf.Name()] {
				if pf.Prepared() != nil {
					t.Fatalf("%s should be unavailable", pf.Name())
				}
				continue
			}
			if pf.Prepared() == nil {
				t.Fatalf("%s should be available", pf.Name())
			}
		}
		return prepared
	case <-time.After(2 * time.Second):
		t.Fatal("prepareApps did not finish")
		return nil
	}
}

// TestPrepareAppsStartupPoolHonorsBuildCapTwo is the deterministic proof of the
// bounded startup pool: with an effective MAX_CONCURRENT_BUILDS of 2 at most
// two preparations run at once, the third and fourth wait for a free worker,
// and completing one immediately admits exactly one more — the next job is
// taken while the other worker's app is still parked, so there is no batch
// barrier that waits for a whole group to finish first.
func TestPrepareAppsStartupPoolHonorsBuildCapTwo(t *testing.T) {
	names := []string{"cap-a", "cap-b", "cap-c", "cap-d"}
	parks := make(map[string]*poolPark, len(names))
	for _, n := range names {
		parks[n] = newPoolPark()
	}
	spy := newBlockingPreparer(2, parks)

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	waitArrivedIn(t, parks, "cap-a", "cap-b")
	assertNotArrived(t, parks, "cap-c", "cap-d")
	if got := spy.peakActive(); got != 2 {
		t.Fatalf("peak concurrent preparations = %d, want the cap 2", got)
	}

	// cap-a completes; the freed pool worker takes the NEXT job (cap-c)
	// immediately, while cap-b is still parked in its own worker.
	parks["cap-a"].free()
	waitArrivedIn(t, parks, "cap-c")
	assertNotArrived(t, parks, "cap-d")

	// cap-b completes; the last job (cap-d) is admitted by the worker that
	// freed, not deferred to a batch barrier.
	parks["cap-b"].free()
	waitArrivedIn(t, parks, "cap-d")

	parks["cap-c"].free()
	parks["cap-d"].free()
	unavailable := map[string]bool{}
	collectPrepared(t, done, names, unavailable)
	if got := spy.peakActive(); got > 2 {
		t.Fatalf("observed peak %d concurrent preparations, want <= 2", got)
	}
}

// TestPrepareAppsStartupPoolSequentialWithCapOne pins the single-slot startup
// shape: MAX_CONCURRENT_BUILDS=1 means exactly one preparation at a time — the
// second app waits until the first completes and is admitted the instant the
// first returns, never in parallel.
func TestPrepareAppsStartupPoolSequentialWithCapOne(t *testing.T) {
	names := []string{"seq-a", "seq-b"}
	parks := map[string]*poolPark{
		"seq-a": newPoolPark(),
		"seq-b": newPoolPark(),
	}
	spy := newBlockingPreparer(1, parks)

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	// The pool runs the FIRST preparation; the second must wait: its arrival
	// channel is never closed while seq-a is parked.
	waitArrivedIn(t, parks, "seq-a")
	assertNotArrived(t, parks, "seq-b")
	if got := spy.peakActive(); got != 1 {
		t.Fatalf("peak concurrent preparations = %d, want 1", got)
	}

	parks["seq-a"].free()
	waitArrivedIn(t, parks, "seq-b")
	if got := spy.peakActive(); got != 1 {
		t.Fatalf("peak concurrent preparations with cap one = %d, want exactly 1", got)
	}
	parks["seq-b"].free()
	collectPrepared(t, done, names, nil)
	if got := spy.peakActive(); got != 1 {
		t.Fatalf("observed peak %d concurrent preparations, want exactly 1", got)
	}
}

// TestPrepareAppsStartupPoolFailureReleasesSlot proves the slot semantics for
// failure: a preparation that fails frees its worker to take the NEXT job
// immediately (cap-c is admitted while cap-a still parks in its own worker),
// so one bad app never blocks the pipeline behind it.
func TestPrepareAppsStartupPoolFailureReleasesSlot(t *testing.T) {
	names := []string{"rel-a", "rel-b", "rel-c", "rel-d"}
	parks := make(map[string]*poolPark, len(names))
	for _, n := range names {
		parks[n] = newPoolPark()
	}
	spy := newBlockingPreparer(2, parks)
	spy.setFail("rel-b", errors.New("injected build failure"))
	spy.setFail("rel-d", errors.New("injected build failure"))

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	// The pool schedules rel-a and rel-b: rel-a parks; rel-b fails instantly,
	// frees its worker, and that worker immediately takes rel-c.
	waitArrivedIn(t, parks, "rel-a", "rel-c")
	assertNotArrived(t, parks, "rel-d")

	// Free the parked preparations so the pool can finish the remaining work.
	parks["rel-a"].free()
	parks["rel-c"].free()
	unavailable := map[string]bool{"rel-b": true, "rel-d": true}
	collectPrepared(t, done, names, unavailable)
	if got := spy.peakActive(); got != 2 {
		t.Fatalf("observed peak %d concurrent preparations, want 2 (both workers were busy)", got)
	}
}

// TestPrepareAppsStartupPoolIndependentFailuresIsolated pins that per-app
// failures only affect their own result: the successful apps' preparations
// complete and return usable handles in startup order, and the failed app is
// marked unavailable without stalling or poisoning the neighboring pool work.
func TestPrepareAppsStartupPoolIndependentFailuresIsolated(t *testing.T) {
	names := []string{"mix-good1", "mix-bad", "mix-good2", "mix-good3"}
	parks := map[string]*poolPark{
		"mix-good1": newPoolPark(),
		"mix-good2": newPoolPark(),
		"mix-good3": newPoolPark(),
	}
	spy := newBlockingPreparer(2, parks)
	spy.setFail("mix-bad", errors.New("injected isolated failure"))

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	waitArrivedIn(t, parks, "mix-good1", "mix-good2")
	// mix-bad already failed without a park; its worker took mix-good3? No:
	// both workers are still busy parking, so the third good app cannot be
	// admitted yet.
	assertNotArrived(t, parks, "mix-good3")

	parks["mix-good1"].free()
	waitArrivedIn(t, parks, "mix-good3")
	parks["mix-good2"].free()
	parks["mix-good3"].free()
	unavailable := map[string]bool{"mix-bad": true}
	collectPrepared(t, done, names, unavailable)
}

// TestPrepareAppsStartupPoolCancellationStopsSchedulingAndJoinsActiveWork pins
// shutdown semantics for the bounded pool: a cancelled lifecycle stops
// SCHEDULING new jobs immediately (later apps receive no Prepare call and end
// up unavailable), while workers that were ALREADY admitted are joined —
// prepareApps cannot return while an admitted preparation is still in flight,
// and it finishes only that admitted work.
func TestPrepareAppsStartupPoolCancellationStopsSchedulingAndJoinsActiveWork(t *testing.T) {
	names := []string{"cxl-a", "cxl-b", "cxl-c", "cxl-d"}
	parks := make(map[string]*poolPark, len(names))
	for _, n := range names {
		parks[n] = newPoolPark()
	}
	spy := newBlockingPreparer(2, parks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan []*runner.PreparedApp, 1)
	go func() { done <- prepareApps(ctx, spy, startupAppsFor(names...), nil, nil, discardLogger()) }()

	// The pool admits the first two apps and parks them inside the spy.
	waitArrivedIn(t, parks, "cxl-a", "cxl-b")

	// Cancel while both workers are inside their admitted preparations: the
	// scheduler must stop admitting new jobs (a send of the next job can only
	// complete when a worker is free, and workers are parked; the select arm's
	// ctx.Done arm is ready, so the scheduler exits deterministically without
	// admitting cxl-c or cxl-d).
	cancel()
	assertNotArrived(t, parks, "cxl-c", "cxl-d")

	// The in-flight preparations are still joined: prepareApps cannot return
	// while a worker is parked inside a preparation.
	assertNotReturned(t, done)
	if got := spy.peakActive(); got > 2 {
		t.Fatalf("peak concurrent preparations = %d, want <= 2", got)
	}

	// Let the admitted work finish: only then may prepareApps return, and only
	// with the two admitted apps — pending ones never start.
	parks["cxl-a"].free()
	parks["cxl-b"].free()
	unavailable := map[string]bool{"cxl-a": true, "cxl-b": true, "cxl-c": true, "cxl-d": true}
	collectPrepared(t, done, names, unavailable)

	if got := spy.callCount(); got != 2 {
		t.Fatalf("Prepare calls = %d, want exactly 2 (the two admitted apps)", got)
	}
	for _, admitted := range []string{"cxl-a", "cxl-b"} {
		select {
		case <-parks[admitted].returned:
		default:
			t.Fatalf("%s's admitted preparation did not return before prepareApps finished", admitted)
		}
	}
	assertNotArrived(t, parks, "cxl-c", "cxl-d")
}

// TestPrepareAppsStartupPoolNoStateSinkRunsWithoutGates keeps the standalone
// shape (nil state sink, nil tokens) covered and pins the pool's steady-state
// concurrency: two apps park simultaneously (the pool runs at most two), and
// releasing them lets every remaining app finish promptly in startup order.
func TestPrepareAppsStartupPoolNoStateSinkRunsWithoutGates(t *testing.T) {
	names := []string{"free-a", "free-b", "free-c", "free-d"}
	parks := map[string]*poolPark{
		"free-a": newPoolPark(),
		"free-b": newPoolPark(),
	}
	spy := newBlockingPreparer(2, parks)

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	waitArrivedIn(t, parks, "free-a", "free-b")
	if got := spy.peakActive(); got != 2 {
		t.Fatalf("peak concurrent preparations = %d, want 2", got)
	}
	parks["free-a"].free()
	parks["free-b"].free()
	collectPrepared(t, done, names, nil)
	if got := spy.callCount(); got != len(names) {
		t.Fatalf("Prepare calls = %d, want %d", got, len(names))
	}
	if got := spy.peakActive(); got != 2 {
		t.Fatalf("observed peak %d concurrent preparations, want 2", got)
	}
}

// TestPrepareAppsStartupPoolMixedFailureDoesNotBlockIndependentApps pins the
// no-park fast-failure shape: one failing app in the middle of the startup
// list ends the pool promptly with every successful neighbor available in
// startup order, and no worker is left waiting on the failure.
func TestPrepareAppsStartupPoolMixedFailureDoesNotBlockIndependentApps(t *testing.T) {
	names := []string{"iso-x", "iso-fail", "iso-y"}
	spy := newBlockingPreparer(2, nil)
	spy.setFail("iso-fail", errors.New("injected failure"))

	done := make(chan []*runner.PreparedApp, 1)
	go func() {
		done <- prepareApps(context.Background(), spy, startupAppsFor(names...), nil, nil, discardLogger())
	}()

	unavailable := map[string]bool{"iso-fail": true}
	collectPrepared(t, done, names, unavailable)
	if got := spy.callCount(); got != len(names) {
		t.Fatalf("Prepare calls = %d, want %d", got, len(names))
	}
	if got := spy.peakActive(); got > 2 {
		t.Fatalf("observed peak %d concurrent preparations, want <= 2", got)
	}
}

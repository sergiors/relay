package runtime

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// errTestDepBusy is the test coordinator's retryable-busy error (the
// in-memory counterpart of state.ErrDependencyImageBusy, which the test
// coordinator must not import — it is a State-package error).
var errTestDepBusy = errors.New("test dependency image coordinator: busy")

// inProcessDepCoordinator is the explicit TEST-ONLY implementation of
// DependencyImageCoordinator used by unit tests with scripted Docker: its
// authority is process-local deliberately, because those tests exercise one
// process and no other process shares their fake daemon. It mirrors the same
// contract semantics as the production State-backed coordinator: counted
// shared use leases, one exclusive GC hold, non-blocking GC acquisition
// (refused with acquired=false), idempotent Release, and context-aware
// (already-cancelled context grants nothing).
//
// Every dep-bearing unit test obtains coordination explicitly — through
// newLifecycleManager (which installs one per test unless a shared one is
// passed) or by direct assignment to Manager.depCoord. A unit test must never
// rely on managers being silently uncoordinated: dep-bearing Prepare and
// dependency GC fail closed without a coordinator.
type inProcessDepCoordinator struct {
	mu   sync.Mutex
	uses map[string]int
	gc   map[string]bool

	// useAcquired and gcAttempts observe coordination traffic for
	// fail-closed/busy assertions without touching production seams.
	useAcquired int
	gcAttempts  int
	gcAcquired  int
}

func newInProcessDepCoordinator() *inProcessDepCoordinator {
	return &inProcessDepCoordinator{
		uses: map[string]int{},
		gc:   map[string]bool{},
	}
}

func (c *inProcessDepCoordinator) AcquireUse(
	ctx context.Context, depRef string,
) (DependencyImageLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gc[depRef] {
		return nil, errTestDepBusy
	}
	c.uses[depRef]++
	c.useAcquired++
	return &inProcessDepLease{c: c, ref: depRef}, nil
}

func (c *inProcessDepCoordinator) TryAcquireGC(
	ctx context.Context, depRef string,
) (DependencyImageLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gcAttempts++
	if c.gc[depRef] || c.uses[depRef] > 0 {
		return nil, false, nil
	}
	c.gc[depRef] = true
	c.gcAcquired++
	return &inProcessDepLease{c: c, ref: depRef, gc: true}, true, nil
}

// gcHeld reports whether an exclusive GC lease is currently admitted (used by
// tests to pin skip/sweep decisions without reaching into map internals).
func (c *inProcessDepCoordinator) gcHeld(depRef string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gc[depRef]
}

// useHeld reports whether any shared use lease is currently admitted.
func (c *inProcessDepCoordinator) useHeld(depRef string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uses[depRef] > 0
}

// gc/gui helpers wrap types below; see inProcessDepLease.
type inProcessDepLease struct {
	c   *inProcessDepCoordinator
	ref string
	gc  bool

	mu       sync.Mutex
	released bool
}

func (l *inProcessDepLease) Verify(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return errors.New("test dependency image lease lost: released")
	}
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	if l.gc {
		if !l.c.gc[l.ref] {
			return errors.New("test dependency image lease lost: GC hold gone")
		}
		return nil
	}
	if l.c.uses[l.ref] <= 0 {
		return errors.New("test dependency image lease lost: use gone")
	}
	return nil
}

func (l *inProcessDepLease) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	c := l.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if l.gc {
		delete(c.gc, l.ref)
	} else {
		if uses := c.uses[l.ref]; uses > 1 {
			c.uses[l.ref] = uses - 1
		} else {
			delete(c.uses, l.ref)
		}
	}
	return nil
}

// coordOf returns the in-process coordinator installed on a manager, so a test
// can SHARE one coordinator instance across several managers (the multi-manager
// unit scenario).
func coordOf(m *Manager) *inProcessDepCoordinator {
	if coord, ok := m.depCoord.(*inProcessDepCoordinator); ok {
		return coord
	}
	return nil
}

// TestInProcessDependencyCoordinatorSharedExclusive pins the test
// coordinator's own contract (the semantics the unit suite then relies on):
// shared uses coexist, an exclusive GC altogether refuses both directions, and
// releasing the last reference reopens the exclusive gate.
func TestInProcessDependencyCoordinatorSharedExclusive(t *testing.T) {
	c := newInProcessDepCoordinator()
	ctx := context.Background()

	l1, err := c.AcquireUse(ctx, "relay-dep-inmem")
	if err != nil {
		t.Fatalf("AcquireUse: %v", err)
	}
	l2, err := c.AcquireUse(ctx, "relay-dep-inmem")
	if err != nil {
		t.Fatalf("second shared AcquireUse: %v", err)
	}
	if _, ok, err := c.TryAcquireGC(ctx, "relay-dep-inmem"); err != nil || ok {
		t.Fatalf("TryAcquireGC with uses held = (acquired=%v, err=%v), want refusal", ok, err)
	}
	if err := l1.Verify(ctx); err != nil {
		t.Fatalf("Verify while shared-held: %v", err)
	}
	if err := l1.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := l1.Verify(ctx); err == nil {
		t.Fatal("Verify must fail after Release")
	}
	if err := l2.Verify(ctx); err != nil {
		t.Fatalf("second lease must still verify: %v", err)
	}
	if _, ok, err := c.TryAcquireGC(ctx, "relay-dep-inmem"); err != nil || ok {
		t.Fatalf("TryAcquireGC with one use held = (acquired=%v, err=%v), want refusal", ok, err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
	gc, ok, err := c.TryAcquireGC(ctx, "relay-dep-inmem")
	if err != nil || !ok {
		t.Fatalf("TryAcquireGC after every release = (acquired=%v, err=%v), want acquired", ok, err)
	}
	if err := gc.Release(); err != nil {
		t.Fatalf("GC Release: %v", err)
	}
	// A cancelled context grants nothing and leaves no residue.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.AcquireUse(canceled, "relay-dep-inmem"); err == nil {
		t.Fatal("canceled AcquireUse must fail")
	}
	if _, ok, _ := c.TryAcquireGC(ctx, "relay-dep-inmem"); !ok {
		t.Fatal("canceled attempt must leave the gate open for the next full-context try")
	}
}

// TestDependencyGCFailsClosedWithoutCoordinator pins the fail-closed GC
// contract: a Manager without an injected coordinator must report
// ErrDependencyImageCoordination and remove NOTHING (error surfaces before any
// Docker DELETE could be considered).
func TestDependencyGCFailsClosedWithoutCoordinator(t *testing.T) {
	dels := 0
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/json", body: depGCImageListJSON("relay-dep-uncoordinated0")},
		dockerRoute{method: http.MethodDelete, path: "/images/", body: "[]", onMatch: func() { dels++ }},
	)
	m := &Manager{cli: cli, log: testutil.DiscardLogger()}

	removed, err := m.CleanupUnusedDependencies(context.Background())
	if !errors.Is(err, ErrDependencyImageCoordination) {
		t.Fatalf("GC without a coordinator = (%d, %v), want ErrDependencyImageCoordination", removed, err)
	}
	if removed != 0 || dels != 0 {
		t.Fatalf("GC without a coordinator removed %d (DELETE %d), want 0 and 0", removed, dels)
	}
}

// TestPrepareFailsClosedWithoutDependencyCoordinator pins the fail-closed
// preparation contract: a dependency-bearing Prepare without an injected
// coordinator must fail retryably with ErrDependencyImageCoordination BEFORE
// the dependency flow starts — no dependency probe, no dependency build, no
// build POST. (Prepare does probe the APP image and acquire its local app /
// dependency leases first, so Docker queries do happen earlier in the flow;
// the boundary under test is that the dependency consumption never starts.)
func TestPrepareFailsClosedWithoutDependencyCoordinator(t *testing.T) {
	// The expected app image is named here only to set up the reuse-probe
	// route (404) that precedes coordination in Prepare's flow.
	fn, image, depRef := dependencyLifecycleFixture(t, "dep-no-coord")

	var builds int
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/" + image + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build", body: `{"stream":"ok"}`,
			onRequest: func(req *http.Request) { builds++ },
		},
	)
	m := newLifecycleManager(t, cli, context.Background())
	// Explicitly strip the helper's test coordination: this test pins the
	// fail-closed behavior of an UNCOORDINATED manager.
	m.depCoord = nil

	if _, err := m.Prepare(context.Background(), fn); err == nil {
		t.Fatal("dependency-bearing Prepare without a coordinator must fail")
	} else if !errors.Is(err, ErrDependencyImageCoordination) {
		t.Fatalf("Prepare error = %v, want ErrDependencyImageCoordination", err)
	}
	if builds != 0 {
		t.Fatalf("the fail-closed preparation must not enter any dependency or app build (no build POST), saw %d", builds)
	}
	// The precise boundary the scripted routes pin: a dependency probe would
	// have hit the depRef inspect route (drawing out a build POST, counted
	// above); coordination failing means the flow errors before it.
	if !strings.HasPrefix(depRef, "relay-dep-") {
		t.Fatalf("fixture dep reference %q must be within the managed namespace", depRef)
	}
}

// TestPrepareCancellationReleasesCrossProcessProtection pins that a cancelled
// preparation releases the cross-process shared protection on the cancellation
// path: the deferred release exists exactly for this, and a protection stranded
// across retries of the SAME manager would wedge every later preparation of
// the layer behind a stale reference.
func TestPrepareCancellationReleasesCrossProcessProtection(t *testing.T) {
	fn, image, depRef := dependencyLifecycleFixture(t, "dep-cancel-protection")

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()

	entered := make(chan struct{})
	var enteredOnce sync.Once
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodGet, path: "/images/" + depRef + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{method: http.MethodGet, path: "/images/" + image + "/json", status: http.StatusNotFound, body: `{"message":"no such image"}`},
		dockerRoute{
			method: http.MethodPost, path: "/build",
			onRequest: func(req *http.Request) {
				if req.URL.Query().Get("t") == depRef {
					enteredOnce.Do(func() { close(entered) })
				}
			},
			fail: func(req *http.Request) error {
				<-req.Context().Done()
				return req.Context().Err()
			},
		},
	)
	m := newLifecycleManager(t, cli, lifecycle)
	coord := coordOf(m)

	done := make(chan error, 1)
	go func() {
		_, err := m.Prepare(context.Background(), fn)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the dependency build was not entered")
	}
	if !coord.useHeld(depRef) {
		t.Fatal("the preparation must hold the cross-process protection during the build")
	}
	cancelLifecycle()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Prepare = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle cancellation did not cancel the preparation promptly")
	}
	if coord.useHeld(depRef) {
		t.Fatal("the cross-process protection must be released on the cancellation path")
	}
	// And the gate is fully open again: a fresh acquisition succeeds.
	use, err := coord.AcquireUse(context.Background(), depRef)
	if err != nil {
		t.Fatalf("fresh acquisition after cancellation: %v", err)
	}
	if err := use.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

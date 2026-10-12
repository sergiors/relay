package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/testutil"
)

// blockUntilCancelRoute scripts a Docker route whose RoundTrip blocks until the
// request's OWN context is cancelled, then returns that context's error. It is
// the deterministic stand-in for a Docker call that is still in flight when its
// operation context is cancelled: entered closes once the request is blocked (so
// a test synchronizes on the real call, never on a sleep), and cancelled closes
// when the request observes the cancellation. Both are closed exactly once even
// if the route is matched more than once.
type blockUntilCancelRoute struct {
	entered    chan struct{}
	cancelled  chan struct{}
	enteredDo  sync.Once
	cancelOnce sync.Once
}

func newBlockUntilCancelRoute() *blockUntilCancelRoute {
	return &blockUntilCancelRoute{
		entered:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (b *blockUntilCancelRoute) route(method, path string) dockerRoute {
	return dockerRoute{
		method: method, path: path,
		onRequest: func(*http.Request) { b.enteredDo.Do(func() { close(b.entered) }) },
		fail: func(req *http.Request) error {
			<-req.Context().Done()
			b.cancelOnce.Do(func() { close(b.cancelled) })
			return req.Context().Err()
		},
	}
}

func (b *blockUntilCancelRoute) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the scripted Docker call was never entered")
	}
}

func (b *blockUntilCancelRoute) waitCancelled(t *testing.T) {
	t.Helper()
	select {
	case <-b.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight scripted Docker call never observed its context cancellation")
	}
}

// stubbornRoute scripts a Docker route that IGNORES the request context and
// blocks until the test releases it. It models a best-effort cleanup whose
// Docker call does not cooperate with cancellation, so the only thing that can
// keep the client from closing beneath it is a real join.
type stubbornRoute struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newStubbornRoute() *stubbornRoute {
	return &stubbornRoute{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *stubbornRoute) route(method, path string) dockerRoute {
	return dockerRoute{
		method: method, path: path,
		onRequest: func(*http.Request) { s.once.Do(func() { close(s.entered) }) },
		fail:      func(*http.Request) error { <-s.release; return errors.New("scripted stop failure") },
	}
}

func (s *stubbornRoute) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-s.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the scripted Docker call was never entered")
	}
}

// opsTestManager is shutdownRecordingManager plus the two extra observations the
// operation-lifecycle tests need: the number of operations still admitted at the
// instant the Docker client closes, and (via the stamped-route guard the helper
// already installs) whether any Docker request ran after that close.
type opsTestManager struct {
	manager    *Manager
	closed     atomic.Bool
	callsAfter atomic.Int32
	opsAtClose atomic.Int32
}

func newOpsTestManager(t *testing.T, routes ...dockerRoute) *opsTestManager {
	t.Helper()
	ot := &opsTestManager{}
	ot.manager = shutdownRecordingManager(t, &ot.closed, &ot.callsAfter, routes...)
	inner := ot.manager.closeClient
	ot.manager.closeClient = func(c *client.Client) error {
		ot.opsAtClose.Store(int32(ot.manager.opsCoord().count()))
		return inner(c)
	}
	return ot
}

// expiringCloseContext returns a CloseContext caller context that is already
// cancelled: the operation drains must still join in-flight work rather than
// return early on the expired caller bound.
func expiringCloseContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestManagerOpsPhasesAdmission pins the two-phase admission contract: normal
// work is refused once normal admission closes; teardown admission is refused
// until the teardown phase opens, then admitted, then refused once teardown
// admission closes; and both drains observe zero.
func TestManagerOpsPhasesAdmission(t *testing.T) {
	o := newManagerOps(nil)

	ctx1, release1, err := o.begin(context.Background())
	if err != nil || ctx1 == nil {
		t.Fatalf("normal begin = (%v, %v), want admitted", ctx1, err)
	}
	if n := o.count(); n != 1 {
		t.Fatalf("count after normal begin = %d, want 1", n)
	}
	release1()
	if n := o.count(); n != 0 {
		t.Fatalf("count after normal release = %d, want 0", n)
	}

	// A pre-cancelled caller ctx is rolled back: no registration is held.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := o.begin(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("begin with cancelled caller ctx = %v, want context.Canceled", err)
	}
	if n := o.count(); n != 0 {
		t.Fatalf("count after cancelled begin = %d, want 0 (rollback)", n)
	}

	o.closeNormal()
	if _, _, err := o.begin(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("normal begin after closeNormal = %v, want ErrManagerShuttingDown", err)
	}
	// Teardown admission is refused BEFORE the teardown phase opens.
	if _, _, err := o.beginTeardown(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("teardown begin before openTeardown = %v, want ErrManagerShuttingDown", err)
	}

	o.openTeardown()
	tctx, trelease, err := o.beginTeardown(context.Background())
	if err != nil || tctx == nil {
		t.Fatalf("teardown begin after openTeardown = (%v, %v), want admitted", tctx, err)
	}
	if n := o.count(); n != 1 {
		t.Fatalf("count after teardown begin = %d, want 1", n)
	}

	// closeTeardown is atomic: no teardown admission afterwards.
	o.closeTeardown()
	if _, _, err := o.beginTeardown(context.Background()); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("teardown begin after closeTeardown = %v, want ErrManagerShuttingDown", err)
	}
	trelease()
	o.drainTeardown()
	if n := o.count(); n != 0 {
		t.Fatalf("count after final drain = %d, want 0", n)
	}
}

// TestManagerOpsGateRefusesNewWorkAfterClose pins the admission half of the
// operation lifecycle: once CloseContext has closed normal admission, EVERY
// ordinary Manager-owned Docker entry point is refused terminally with
// ErrManagerShuttingDown and issues no Docker call at all (the scripted daemon
// would fail the test on any unhandled request).
func TestManagerOpsGateRefusesNewWorkAfterClose(t *testing.T) {
	ot := newOpsTestManager(t) // no routes: any Docker call is a test failure
	if err := ot.manager.CloseContext(context.Background()); err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if !ot.manager.opsCoord().isClosing() {
		t.Fatal("the operation gate did not report closing after CloseContext")
	}
	if n := ot.manager.opsCoord().count(); n != 0 {
		t.Fatalf("admitted operations after close = %d, want 0", n)
	}

	ctx := context.Background()
	if _, err := ot.manager.NetworkExists(ctx, "net"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("NetworkExists after close = %v, want ErrManagerShuttingDown", err)
	}
	if _, err := ot.manager.ServiceContainerList(ctx); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("ServiceContainerList after close = %v, want ErrManagerShuttingDown", err)
	}
	if err := ot.manager.StopServiceContainers(ctx, []ServiceContainer{
		{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
	}); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("StopServiceContainers after close = %v, want ErrManagerShuttingDown", err)
	}
	if _, err := ot.manager.StartService(ctx, ServiceSpec{App: "a", Name: "web", Port: 80, Image: "alpine:3"}, 0); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("StartService after close = %v, want ErrManagerShuttingDown", err)
	}
	if err := ot.manager.RestartServiceContainer(ctx, "svc-1"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("RestartServiceContainer after close = %v, want ErrManagerShuttingDown", err)
	}
	if _, err := ot.manager.SweepOrphanContainers(ctx, "host"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("SweepOrphanContainers after close = %v, want ErrManagerShuttingDown", err)
	}
	if err := ot.manager.RemoveImage(ctx, "relay-app-a:v1"); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("RemoveImage after close = %v, want ErrManagerShuttingDown", err)
	}
	if _, release, err := ot.manager.beginDockerOp(ctx); !errors.Is(err, ErrManagerShuttingDown) || release != nil {
		t.Fatalf("beginDockerOp after close = (release=%v, err=%v), want (nil, ErrManagerShuttingDown)", release != nil, err)
	}
	if _, _, err := ot.manager.opsCoord().beginTeardown(ctx); !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("beginTeardown after final drain = %v, want ErrManagerShuttingDown", err)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestBatchDiscardStaysTrackedUntilAllItemsComplete pins that a batch
// reservation's operation is discharged EXACTLY ONCE, after the whole batch: with
// two blocked Docker removes, the operation count stays non-zero between item
// calls, Manager close waits for the entire batch, and the client is never
// closed between item Docker calls.
func TestBatchDiscardStaysTrackedUntilAllItemsComplete(t *testing.T) {
	ot := newOpsTestManager(t)
	m := ot.manager
	m.containers.ops = m.opsCoord()

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	blockCli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/kill",
			onMatch: func() {
				if ot.closed.Load() {
					ot.callsAfter.Add(1)
				}
			}},
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}",
			onMatch: func() {
				if ot.closed.Load() {
					ot.callsAfter.Add(1)
				}
			},
			fail: func(*http.Request) error {
				entered <- struct{}{}
				<-release
				return nil
			}},
	)

	seed := func(id string) *containerLease {
		clientEnd, peerEnd := net.Pipe()
		t.Cleanup(func() { _ = peerEnd.Close() })
		c := &executionContainer{
			cli:    blockCli,
			log:    testutil.DiscardLogger(),
			fn:     "fn",
			image:  "img",
			id:     id,
			ops:    m.containerOps(),
			attach: &client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(clientEnd, "application/vnd.docker.multiplexed-stream")},
			closed: make(chan struct{}),
		}
		lease, err := m.containers.acquire(context.Background(), "fn", "img", 2, func() (reusableContainer, error) { return c, nil })
		if err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
		return lease
	}
	// Hold BOTH leases so two distinct containers are created, then release both
	// so both are idle and a single batch discard tears both down.
	lease1 := seed("c1")
	lease2 := seed("c2")
	lease1.release()
	lease2.release()

	batchDone := make(chan struct{})
	go func() { defer close(batchDone); m.containers.invalidateImage("img") }()

	// First item's remove is blocked; the batch operation is still admitted.
	<-entered
	if n := m.opsCoord().count(); n < 1 {
		t.Fatalf("operations at first batch teardown = %d, want >= 1", n)
	}

	// Manager close begins: it must drain the WHOLE batch before closing.
	closeDone := make(chan error, 1)
	go func() { closeDone <- m.CloseContext(expiringCloseContext()) }()
	select {
	case <-closeDone:
		t.Fatal("CloseContext returned while a batch teardown was still in flight")
	case <-time.After(150 * time.Millisecond):
	}

	// Release item 1; item 2's remove blocks. The operation count must STILL be
	// non-zero: the batch token is released once after the whole batch, not per
	// item.
	release <- struct{}{}
	<-entered
	if n := m.opsCoord().count(); n < 1 {
		t.Fatalf("operations between batch items = %d, want >= 1 (batch token released early)", n)
	}
	release <- struct{}{}
	<-batchDone

	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
}

// TestSweepOrphanContainersClassifiesShutdownRace pins that a sweep error which
// races Manager shutdown is reported as ErrManagerShuttingDown, and that the
// client is only closed after the sweep operation is joined.
func TestSweepOrphanContainersClassifiesShutdownRace(t *testing.T) {
	block := newBlockUntilCancelRoute()
	ot := newOpsTestManager(t, block.route(http.MethodGet, "/containers/json"))

	sweepDone := make(chan error, 1)
	go func() {
		_, err := ot.manager.SweepOrphanContainers(context.Background(), "host")
		sweepDone <- err
	}()
	block.waitEntered(t)

	closeDone := make(chan error, 1)
	go func() { closeDone <- ot.manager.CloseContext(expiringCloseContext()) }()

	block.waitCancelled(t)
	if err := <-sweepDone; !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("shutdown-raced sweep = %v, want ErrManagerShuttingDown", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestManagerCloseJoinsBlockedServiceCleanupBeforeClientClose is the F-028
// regression: a service cleanup blocked inside a Docker call is strictly joined
// before the Docker client closes, even though the CloseContext caller context is
// already expired. The client closes with zero operations admitted and no
// request issued after the close.
func TestManagerCloseJoinsBlockedServiceCleanupBeforeClientClose(t *testing.T) {
	block := newBlockUntilCancelRoute()
	ot := newOpsTestManager(t, block.route(http.MethodPost, "/stop"))

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- ot.manager.StopServiceContainers(context.Background(), []ServiceContainer{
			{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
		})
	}()
	block.waitEntered(t)
	if n := ot.manager.opsCoord().count(); n < 1 {
		t.Fatalf("admitted operations at service-stop entry = %d, want >= 1", n)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- ot.manager.CloseContext(expiringCloseContext()) }()

	block.waitCancelled(t)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if err := <-stopDone; !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("blocked service cleanup = %v, want ErrManagerShuttingDown", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0 (Close must join every operation first)", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestManagerCloseJoinsNonCooperativeServiceCleanup is the F-028 core: a
// best-effort service cleanup whose Docker call ignores its bound and its
// registry step context (both cancelled) is still strictly joined by
// CloseContext before the Docker client closes.
func TestManagerCloseJoinsNonCooperativeServiceCleanup(t *testing.T) {
	stub := newStubbornRoute()
	ot := newOpsTestManager(t, stub.route(http.MethodPost, "/stop"))

	stepCtx, cancelStep := context.WithCancel(context.Background())
	defer cancelStep()
	stopDone := make(chan error, 1)
	go func() {
		stopDone <- ot.manager.StopServiceContainers(stepCtx, []ServiceContainer{
			{ID: "svc-1", App: "a", Name: "web", State: container.StateRunning},
		})
	}()
	stub.waitEntered(t)

	cancelStep()
	closeDone := make(chan error, 1)
	go func() { closeDone <- ot.manager.CloseContext(expiringCloseContext()) }()

	select {
	case <-closeDone:
		t.Fatal("CloseContext returned while a non-cooperative cleanup was still in flight")
	case <-time.After(150 * time.Millisecond):
	}

	close(stub.release)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if err := <-stopDone; err == nil {
		t.Fatal("the non-cooperative cleanup should have returned its scripted error")
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0 (Close must join the cleanup first)", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestManagerCloseJoinsBlockedImageRemovalDespiteExpiredContext pins the same
// guarantee for image retirement: a removal blocked inside ImageRemove is joined
// unconditionally, so an expired CloseContext caller context can never let the
// client close under a live removal.
func TestManagerCloseJoinsBlockedImageRemovalDespiteExpiredContext(t *testing.T) {
	block := newBlockUntilCancelRoute()
	ot := newOpsTestManager(t,
		dockerRoute{method: http.MethodGet, path: "/containers/json", body: `[]`},
		block.route(http.MethodDelete, "/images/"),
	)

	rmDone := make(chan error, 1)
	go func() { rmDone <- ot.manager.RemoveImage(context.Background(), "relay-app-a:v1") }()
	block.waitEntered(t)

	closeDone := make(chan error, 1)
	go func() { closeDone <- ot.manager.CloseContext(expiringCloseContext()) }()

	block.waitCancelled(t)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if err := <-rmDone; !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("blocked image removal = %v, want ErrManagerShuttingDown", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestManagerCloseCancelsInFlightServiceCreate covers the in-flight create path:
// a service create blocked in ContainerCreate is a normal operation, so Manager
// close cancels it, joins it, and only then closes the client.
func TestManagerCloseCancelsInFlightServiceCreate(t *testing.T) {
	block := newBlockUntilCancelRoute()
	ot := newOpsTestManager(t, block.route(http.MethodPost, "/containers/create"))

	createDone := make(chan error, 1)
	go func() {
		_, err := ot.manager.StartService(context.Background(),
			ServiceSpec{App: "a", Name: "web", Port: 80, Image: "alpine:3"}, 0)
		createDone <- err
	}()
	block.waitEntered(t)

	closeDone := make(chan error, 1)
	go func() { closeDone <- ot.manager.CloseContext(expiringCloseContext()) }()

	block.waitCancelled(t)
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if err := <-createDone; !errors.Is(err, ErrManagerShuttingDown) {
		t.Fatalf("in-flight service create = %v, want ErrManagerShuttingDown", err)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
}

// TestManagerStartContainerCleansCreateAfterShutdown proves an admitted
// execution create that completes only after Manager shutdown began is cleaned
// up inside the tracked create normal operation (its kill/remove observe a
// non-zero operation count), and the count reaches zero before the Docker client
// closes.
func TestManagerStartContainerCleansCreateAfterShutdown(t *testing.T) {
	ot := newOpsTestManager(t)
	m := ot.manager

	var opsAtKill atomic.Int32
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() { opsAtKill.Store(int32(m.opsCoord().count())) }},
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}"},
	)

	created := make(chan struct{})
	m.startExecutionContainerFn = func(
		ctx context.Context, ops containerOps, _ *client.Client, _ *slog.Logger,
		_, _ string, _ []string, _ []string, _ app.ResourceLimits, _ RunMeta,
		_ string, _ ...mount.Mount,
	) (reusableContainer, error) {
		close(created)
		<-ctx.Done() // complete only once Manager close cancels the create operation
		clientEnd, peerEnd := net.Pipe()
		t.Cleanup(func() { _ = peerEnd.Close() })
		return &executionContainer{
			cli:    cli,
			log:    testutil.DiscardLogger(),
			fn:     "fn",
			image:  "img",
			id:     "cid",
			ops:    ops,
			attach: &client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(clientEnd, "application/vnd.docker.multiplexed-stream")},
			closed: make(chan struct{}),
		}, nil
	}

	startDone := make(chan error, 1)
	go func() {
		_, err := m.startContainer(context.Background(), "fn", resolvedImage{ref: "img"}, nil, app.DefaultResourceLimits(), RunMeta{})
		startDone <- err
	}()
	select {
	case <-created:
	case <-time.After(2 * time.Second):
		t.Fatal("the execution create was never entered")
	}
	if n := m.opsCoord().count(); n < 1 {
		t.Fatalf("admitted operations at create entry = %d, want >= 1", n)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- m.CloseContext(expiringCloseContext()) }()

	if err := <-startDone; !errors.Is(err, errPoolClosed) {
		t.Fatalf("startContainer completing during shutdown = %v, want errPoolClosed", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext: %v", err)
	}
	if n := opsAtKill.Load(); n < 1 {
		t.Fatalf("operations observed at create cleanup kill = %d, want >= 1 (cleaned inside the create operation)", n)
	}
	if n := ot.opsAtClose.Load(); n != 0 {
		t.Fatalf("operations at client close = %d, want 0", n)
	}
	if n := ot.callsAfter.Load(); n != 0 {
		t.Fatalf("Docker calls after client close = %d, want 0", n)
	}
	if !ot.closed.Load() {
		t.Fatal("the Docker client was never closed")
	}
}

// TestExecutionContainerWaitIsTrackedByManagerOps proves the long-lived exit
// wait watcher owns a NORMAL Manager operation for its whole lifetime: the
// counter is non-zero while the wait is outstanding, Manager close cancels it,
// and drainNormal cannot return until the watcher (and its drained SDK reader)
// has concluded.
func TestExecutionContainerWaitIsTrackedByManagerOps(t *testing.T) {
	o := newManagerOps(nil)
	block := newBlockUntilCancelRoute()
	cli := newScriptedDockerClient(t, block.route(http.MethodPost, "/wait"))
	c := &executionContainer{
		cli:      cli,
		log:      testutil.DiscardLogger(),
		fn:       "fn",
		image:    "img",
		id:       "cid",
		ops:      containerOps{lifecycle: o.context(), gate: o},
		exitInfo: make(chan exitInfo, 1),
		closed:   make(chan struct{}),
	}
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); c.watchExit() }()

	block.waitEntered(t)
	if n := o.count(); n < 1 {
		t.Fatalf("operations while the wait request is outstanding = %d, want >= 1", n)
	}

	o.closeNormal()
	drained := make(chan struct{})
	go func() { o.drainNormal(); close(drained) }()

	block.waitCancelled(t)
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the wait watcher did not conclude after Manager close")
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drainNormal returned before the wait watcher released its operation")
	}
	if n := o.count(); n != 0 {
		t.Fatalf("operations after the watcher concluded = %d, want 0", n)
	}
}

// TestExecutionContainerWaitRefusedAfterNormalClose proves a watcher started
// after normal admission closed is NOT admitted late and issues no wait request:
// the container's teardown is owned by cache teardown.
func TestExecutionContainerWaitRefusedAfterNormalClose(t *testing.T) {
	o := newManagerOps(nil)
	var waits atomic.Int32
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/wait", onMatch: func() { waits.Add(1) }},
	)
	c := &executionContainer{
		cli:      cli,
		log:      testutil.DiscardLogger(),
		fn:       "fn",
		image:    "img",
		id:       "cid",
		ops:      containerOps{lifecycle: o.context(), gate: o},
		exitInfo: make(chan exitInfo, 1),
		closed:   make(chan struct{}),
	}
	o.closeNormal()

	done := make(chan struct{})
	go func() { defer close(done); c.watchExit() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchExit did not return when normal admission was closed")
	}
	if n := waits.Load(); n != 0 {
		t.Fatalf("wait requests issued after normal close = %d, want 0", n)
	}
	if n := o.count(); n != 0 {
		t.Fatalf("operations after refused watcher = %d, want 0", n)
	}
}

// TestExecutionContainerDiscardRunsUnderManagerOps proves a container teardown is
// registered as a NORMAL Manager operation while its kill/remove run, and
// released afterwards, so no execution-container Docker call escapes the
// lifecycle. It also pins the monitor/discard refusal path: once normal
// admission has closed, a discard issues no Docker call and leaves the container
// to cache teardown.
func TestExecutionContainerDiscardRunsUnderManagerOps(t *testing.T) {
	newContainer := func(o *managerOps, cli *client.Client) *executionContainer {
		clientEnd, peerEnd := net.Pipe()
		t.Cleanup(func() { _ = peerEnd.Close() })
		return &executionContainer{
			cli:    cli,
			log:    testutil.DiscardLogger(),
			fn:     "fn",
			image:  "img",
			id:     "cid",
			ops:    containerOps{lifecycle: o.context(), gate: o},
			attach: &client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(clientEnd, "application/vnd.docker.multiplexed-stream")},
			closed: make(chan struct{}),
		}
	}

	t.Run("admitted", func(t *testing.T) {
		o := newManagerOps(nil)
		var opsAtKill, kills atomic.Int32
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() { kills.Add(1); opsAtKill.Store(int32(o.count())) }},
			dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}"},
		)
		c := newContainer(o, cli)
		if !c.discard(reasonProcessExit) {
			t.Fatal("the first discard must perform the teardown")
		}
		if n := opsAtKill.Load(); n < 1 {
			t.Fatalf("operations at kill = %d, want >= 1 (the discard must be registered)", n)
		}
		if n := o.count(); n != 0 {
			t.Fatalf("operations after discard = %d, want 0", n)
		}
	})

	t.Run("refused", func(t *testing.T) {
		o := newManagerOps(nil)
		var kills, removes atomic.Int32
		cli := newScriptedDockerClient(t,
			dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() { kills.Add(1) }},
			dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}", onMatch: func() { removes.Add(1) }},
		)
		c := newContainer(o, cli)
		o.closeNormal()

		if c.discard(reasonProcessExit) {
			t.Fatal("a discard after normal close must not perform a teardown")
		}
		if kills.Load() != 0 || removes.Load() != 0 {
			t.Fatalf("discard after normal close issued Docker calls: kills=%d removes=%d", kills.Load(), removes.Load())
		}
		if n := o.count(); n != 0 {
			t.Fatalf("operations after refused discard = %d, want 0", n)
		}
	})
}

// TestPoolDiscardRefusedLeavesContainerForTeardown proves a pool-initiated
// discard that races normal close is refused and leaves its container in the
// pool, so the shutdown-owned cache teardown owns it: no detached Docker call
// runs, and no container is leaked out of the pool's ownership.
func TestPoolDiscardRefusedLeavesContainerForTeardown(t *testing.T) {
	o := newManagerOps(nil)
	cc := newContainerCache()
	cc.ops = o

	var kills atomic.Int32
	cli := newScriptedDockerClient(t,
		dockerRoute{method: http.MethodPost, path: "/kill", onMatch: func() { kills.Add(1) }},
		dockerRoute{method: http.MethodDelete, path: "/containers/", body: "{}"},
	)
	clientEnd, peerEnd := net.Pipe()
	defer peerEnd.Close()
	c := &executionContainer{
		cli:    cli,
		log:    testutil.DiscardLogger(),
		fn:     "fn",
		image:  "img",
		id:     "cid",
		ops:    containerOps{lifecycle: o.context(), gate: o},
		attach: &client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(clientEnd, "application/vnd.docker.multiplexed-stream")},
		closed: make(chan struct{}),
	}

	lease, err := cc.acquire(context.Background(), "fn", "img", 1, func() (reusableContainer, error) { return c, nil })
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	p := cc.poolFor("fn", 1)
	lease.pc.retired = true // force release to drop

	o.closeNormal()
	lease.release() // must be refused and leave the container in the pool

	if kills.Load() != 0 {
		t.Fatalf("refused pool discard issued %d kill(s), want 0", kills.Load())
	}
	p.mu.Lock()
	_, stillBusy := p.active.busy[lease.pc]
	p.mu.Unlock()
	if !stillBusy {
		t.Fatal("refused pool discard removed the container from the pool; cache teardown could not own it")
	}
	if n := o.count(); n != 0 {
		t.Fatalf("operations after refused pool discard = %d, want 0", n)
	}
}

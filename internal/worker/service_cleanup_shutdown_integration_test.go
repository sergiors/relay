//go:build integration

// This file proves the graceful-shutdown service-cleanup path against REAL
// services: a real Docker daemon (a live, persistent service container that
// traps SIGTERM) and real PostgreSQL (the pinned WORKER_ID ownership lock). It
// is excluded from the default suite by the integration build tag, so running it
// (`go test -tags=integration ./internal/worker/`) REQUIRES a reachable Docker
// daemon (client.FromEnv) and PostgreSQL at TEST_DATABASE_URL; a missing
// dependency fails the affected test rather than skipping it.
//
// Redis is deliberately NOT required: this scenario never touches the event
// stream, the PEL, or invocation state, so pulling Redis in would be noise
// rather than coverage.
//
// It pins the F-028 service-cleanup regression end to end. The worker cancels
// its lifecycle (its signal context) BEFORE it runs the ordered shutdown steps,
// so by the time the service-cleanup step runs every ORDINARY runtime admission
// is already refused. The step therefore runs through the manager's narrow
// shutdown-only authorization (reconciler.ShutdownDocker -> the Manager's
// ShutdownServiceContainerList/ShutdownStopServiceContainers), which is tracked
// by the SAME normal-operation count CloseContext drains. The tests below prove
// that cleanup removes this worker's container, that a real in-flight cleanup is
// joined by the Docker client close, and that a FAILED real ownership proof
// (a terminated pinned lock backend) keeps the container alive.
package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// trappingServiceImage is the image the test's live service container runs. It
// is a base image already present on the documented dev/CI daemon; the pull in
// createTrappingServiceContainer makes that explicit rather than assumed.
const trappingServiceImage = "alpine:3"

// trappingServiceEntry installs a SIGTERM trap that lingers, so the daemon's
// stop is a real, observable in-flight Docker call rather than an instantaneous
// one: the first echo synchronizes the test on the trap being installed, and the
// second fires when Docker's stop delivers the signal. busybox ash (alpine's
// /bin/sh) runs the trap once the current `sleep 1` returns, so the delay is at
// most about one second longer than the in-trap sleep, and the container exits
// before the daemon's 10s SIGKILL fallback.
const trappingServiceEntry = "trap 'echo RELAY_SIGTERM; sleep 5; exit 0' TERM; " +
	"echo RELAY_READY; while :; do sleep 1; done"

// createTrappingServiceContainer creates and starts a live container carrying
// the given Relay labels and the SIGTERM-trap entrypoint, waits until it
// reports ready (so the trap is installed before the test cancels anything),
// and returns its ID. The container is force-removed on cleanup, so a failing
// test never leaks it.
func createTrappingServiceContainer(
	t *testing.T, cli *client.Client, labels map[string]string,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pull, err := cli.ImagePull(ctx, trappingServiceImage, client.ImagePullOptions{})
	if err != nil {
		t.Fatalf("pull %s: %v", trappingServiceImage, err)
	}
	if err := pull.Wait(ctx); err != nil {
		t.Fatalf("wait for %s pull: %v", trappingServiceImage, err)
	}

	created, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:      trappingServiceImage,
			Entrypoint: []string{"/bin/sh", "-c", trappingServiceEntry},
			Labels:     labels,
		},
	})
	if err != nil {
		t.Fatalf("create trapping service container: %v", err)
	}
	t.Cleanup(func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rmCancel()
		if _, err := cli.ContainerRemove(
			rmCtx, created.ID, client.ContainerRemoveOptions{Force: true},
		); err != nil {
			t.Logf("cleanup: remove trapping container %s: %v", created.ID, err)
		}
	})

	if _, err := cli.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start trapping service container: %v", err)
	}
	waitForContainerMarker(t, cli, created.ID, "RELAY_READY")
	return created.ID
}

// waitForContainerMarker polls the container's combined output until the marker
// appears. It is the synchronization point for "the trap is installed" and "the
// stop's SIGTERM was delivered", so the test never sleeps on a guess.
func waitForContainerMarker(t *testing.T, cli *client.Client, id, marker string) {
	t.Helper()
	testutil.WaitFor(t, 30*time.Second, "container marker "+marker, func() bool {
		return containerOutputContains(cli, id, marker)
	})
}

// containerOutputContains reads the container's stdout/stderr and reports
// whether the marker appears. Without a TTY, ContainerLogs returns the
// multiplexed stream, but a marker written by a single echo stays contiguous
// inside one frame payload, so a raw substring search is exact enough here.
func containerOutputContains(cli *client.Client, id, marker string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := cli.ContainerLogs(ctx, id, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return false
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return bytes.Contains(b, []byte(marker))
}

// cancelledBound is an already-cancelled caller bound for CloseContext: the
// operation drains must still join in-flight work rather than return early on
// the expired caller context.
func cancelledBound() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// serviceLabels builds the Relay label set for THIS worker's persistent service
// container. relay.hostname is the worker identity (WORKER_ID), which is the
// only label the shutdown cleanup selects on; relay.app/relay.service are
// stamped for realism and readable failures.
func serviceLabels(workerID, appName, serviceName string) map[string]string {
	return map[string]string{
		"relay.type":     "service",
		"relay.hostname": workerID,
		"relay.app":      appName,
		"relay.service":  serviceName,
	}
}

// TestIntegrationShutdownServiceCleanupRemovesServiceAndCloseJoinsInFlight is
// the end-to-end F-028 regression over real Docker and PostgreSQL:
//
//	Given a live persistent service container owned by this worker, and a worker
//	      lifecycle that has been cancelled (the production shutdown ordering),
//	When  ordinary Docker operations are attempted, then the shutdown service
//	      cleanup runs on a fresh shutdown step context, and the Docker client
//	      close is started while the cleanup's stop is still in flight,
//	Then  ordinary operations are refused, the container is removed, the close
//	      waits for the in-flight cleanup, and the container is gone.
func TestIntegrationShutdownServiceCleanupRemovesServiceAndCloseJoinsInFlight(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)

	workerID := testutil.UniqueName(t, "svc-cleanup")
	appName := workerID + "-app"
	const serviceName = "web"
	labels := serviceLabels(workerID, appName, serviceName)

	// The real pinned WORKER_ID lock: the shutdown step's ownership proof reads
	// it, and the manager is rooted at the lifecycle the worker cancels on
	// shutdown.
	st := testutil.OpenState(t, dsn, state.Options{Owner: workerID, Instance: "inst-1"})
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	logger := testutil.DiscardLogger()

	manager, err := runtime.NewManager(
		logger, nil, workerID, runtime.WithLifecycleContext(lifecycle),
	)
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	closeReturned := make(chan struct{})
	t.Cleanup(func() {
		// A test that fails before it drives CloseContext itself must still tear
		// the manager down.
		select {
		case <-closeReturned:
		default:
			_ = manager.Close()
		}
	})

	svcCtrl := reconciler.NewServiceReconciler(
		manager, nil, routing.TraefikConfig{}, logger, reconcileTimeout,
	)

	containerID := createTrappingServiceContainer(t, dcli, labels)

	// Given: the worker cancels its lifecycle BEFORE it runs its ordered
	// shutdown steps (Run's shutdown defer).
	cancelLifecycle()

	// Then: every ordinary runtime admission is terminally refused, so the
	// cleanup cannot run through the ordinary methods.
	if _, err := manager.ServiceContainerList(context.Background()); !errors.Is(err, runtime.ErrManagerShuttingDown) {
		t.Fatalf("ordinary ServiceContainerList after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}
	if err := manager.StopServiceContainers(context.Background(), []runtime.ServiceContainer{
		{ID: containerID, State: container.StateRunning},
	}); !errors.Is(err, runtime.ErrManagerShuttingDown) {
		t.Fatalf("ordinary StopServiceContainers after lifecycle cancel = %v, want ErrManagerShuttingDown", err)
	}

	// When: the real shutdown service cleanup runs on its own fresh shutdown
	// step context and its own fresh ownership proof.
	proof := ownershipProof{loss: &ownershipLoss{}, verify: st.VerifyOwnerLock}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	defer cancelCleanup()
	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- shutdownServices(svcCtrl, workerID, proof, logger)(cleanupCtx)
	}()

	// The stop is genuinely in flight once the container handles the SIGTERM the
	// daemon delivered: the cleanup's Docker call has not unwound.
	waitForContainerMarker(t, dcli, containerID, "RELAY_SIGTERM")

	closeDone := make(chan error, 1)
	go func() {
		err := manager.CloseContext(cancelledBound())
		close(closeReturned)
		closeDone <- err
	}()

	// Then: the close cannot return while the shutdown cleanup is still holding
	// its operation, because the client must not close beneath an in-flight
	// call. The container is checked to still exist, so the window is pinned to
	// a cleanup that is genuinely still in flight rather than one that quietly
	// finished.
	select {
	case err := <-closeDone:
		t.Fatalf("CloseContext returned (err=%v) while the shutdown cleanup was in flight", err)
	case <-time.After(1500 * time.Millisecond):
	}
	if id := findContainerByLabels(context.Background(), dcli, labels); id == "" {
		t.Fatal("the cleanup finished during the close window; the close did not observe an in-flight cleanup")
	}

	// When the container finally exits, the cleanup completes, the container is
	// removed, and only then does the close return.
	if err := <-cleanupDone; err != nil {
		t.Fatalf("shutdown service cleanup = %v, want nil", err)
	}
	if !waitForContainerGone(t, dcli, labels) {
		t.Fatalf("service container %s was not removed by the shutdown cleanup", containerID)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("CloseContext = %v, want nil", err)
	}
	if id := findContainerByLabels(context.Background(), dcli, labels); id != "" {
		t.Fatalf("container %s still present after shutdown cleanup and close", id)
	}
}

// TestIntegrationShutdownServiceCleanupSkipsAfterFailedOwnershipProof proves the
// fail-closed side over real Docker and PostgreSQL: when the worker's pinned
// ownership lock is genuinely lost (its backend terminated from a separate
// connection), the shutdown service cleanup still discovers this worker's
// container, but the failed proof latches the loss and the removal is skipped,
// so a container a replacement owner might have adopted is never touched.
//
//	Given a live service container owned by this worker and a pinned WORKER_ID
//	      lock whose backend has been terminated,
//	When  the shutdown service cleanup runs with the lifecycle cancelled,
//	Then  the proof runs and fails, the loss is latched, and the running
//	      container survives.
func TestIntegrationShutdownServiceCleanupSkipsAfterFailedOwnershipProof(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	testutil.RequireStateDatabase(t)
	dsn := testutil.NewStateDSN(t)

	workerID := testutil.UniqueName(t, "svc-lost")
	appName := workerID + "-app"
	const serviceName = "web"
	labels := serviceLabels(workerID, appName, serviceName)

	st := testutil.OpenState(t, dsn, state.Options{Owner: workerID, Instance: "inst-1"})
	ownerPID := st.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner backend pid = %d, want > 0", ownerPID)
	}

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	logger := testutil.DiscardLogger()

	manager, err := runtime.NewManager(
		logger, nil, workerID, runtime.WithLifecycleContext(lifecycle),
	)
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	svcCtrl := reconciler.NewServiceReconciler(
		manager, nil, routing.TraefikConfig{}, logger, reconcileTimeout,
	)

	containerID := createTrappingServiceContainer(t, dcli, labels)

	// Given: the lock backend is terminated from a SEPARATE connection — the
	// residual "lock lost while the process runs" case the fail-closed proof
	// exists for. Discovery still runs and captures the container; the proof is
	// what must stop the removal.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(
		context.Background(), `SELECT pg_terminate_backend($1)`, ownerPID,
	); err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	// The worker lifecycle is already cancelled, exactly as the shutdown
	// ordering leaves it.
	cancelLifecycle()

	var verifyCalls atomic.Int32
	loss := &ownershipLoss{}
	proof := ownershipProof{
		loss: loss,
		verify: func(ctx context.Context) error {
			verifyCalls.Add(1)
			return st.VerifyOwnerLock(ctx)
		},
		onLost: ownershipLossAction(loss, nil, logger, workerID, "inst-1", func() {}),
	}

	// When: the shutdown service cleanup runs.
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), shutdownServiceTimeout)
	defer cancelCleanup()
	if err := shutdownServices(svcCtrl, workerID, proof, logger)(cleanupCtx); err != nil {
		t.Fatalf("shutdown service cleanup = %v, want nil (removal suppressed, not fatal)", err)
	}

	// Then: the proof ran (so discovery captured the container first), the loss
	// is latched, and the still-running container survives untouched.
	if got := verifyCalls.Load(); got == 0 {
		t.Fatal("the ownership proof never ran; the container must have been captured for it to be skipped")
	}
	if !loss.isLost() {
		t.Fatal("a failed ownership proof must latch the loss")
	}
	if id := findContainerByLabels(context.Background(), dcli, labels); id == "" {
		t.Fatalf("service container %s was removed despite a failed ownership proof", containerID)
	}
	insp, err := dcli.ContainerInspect(context.Background(), containerID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("inspect surviving container: %v", err)
	}
	if insp.Container.State == nil || !insp.Container.State.Running {
		t.Fatalf("container state = %+v, want still running (removal skipped)", insp.Container.State)
	}
}

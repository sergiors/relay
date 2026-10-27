//go:build integration

// This file proves the startup service recovery path against REAL Docker and
// PostgreSQL: a previous Relay incarnation exits abruptly (its pinned WORKER_ID
// lock backend is terminated server-side while its containers keep running) and
// a replacement incarnation with the SAME WORKER_ID recovers.
//
// It is excluded from the default suite by the integration build tag, so
// running it (`go test -tags=integration ./internal/worker/`) REQUIRES a
// reachable Docker daemon (client.FromEnv) and PostgreSQL at TEST_DATABASE_URL;
// a missing dependency fails the affected test rather than skipping it.
//
// Redis is deliberately NOT required: these scenarios never touch the event
// stream, the PEL, or invocation state, so pulling Redis in would be noise
// rather than coverage. They drive the same startup helpers Run wires in the
// same order (execution orphan sweep first, service convergence, then the
// service-orphan sweep) instead of Run itself, which would require the
// filesystem-root and Redis wiring those helpers deliberately do not.
package worker

import (
	"context"
	"database/sql"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// recoveryImage is a base image already present on the documented dev/CI
// daemon. The helpers below pull it only when it is genuinely missing.
const recoveryImage = "alpine:3"

// recoveryServiceEntry is a long-lived but stop-responsive process: the shell
// traps SIGTERM and exits immediately, so a sweep's stop does not pay the
// daemon's 10s SIGKILL grace. The reconciler ignores the entry command when it
// compares an existing container for convergence, so it is safe for both the
// adopted (already running) and freshly started containers.
func recoveryServiceEntry() []string {
	return []string{"/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"}
}

// recoveryServiceTemplate is the desired template shared by the previous and
// replacement incarnations. It resolves through the `entrypoint` source (the
// app image is recoveryImage) so adoption can be exercised without a registry.
func recoveryServiceTemplate() *app.Template {
	return &app.Template{
		Runtime: "node24",
		Services: []app.Service{{
			Name:       "web",
			Entrypoint: "web.js",
			Port:       80,
			Replicas:   1,
		}},
	}
}

// ensureRecoveryImage makes the shared base image available, pulling only when
// the daemon does not already have it.
func ensureRecoveryImage(t *testing.T, cli *client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := cli.ImageInspect(ctx, recoveryImage); err == nil {
		return
	}
	pull, err := cli.ImagePull(ctx, recoveryImage, client.ImagePullOptions{})
	if err != nil {
		t.Fatalf("pull %s: %v", recoveryImage, err)
	}
	if err := pull.Wait(ctx); err != nil {
		t.Fatalf("wait for %s pull: %v", recoveryImage, err)
	}
}

// startRecoveryService starts one persistent service container through the real
// runtime Manager, so its label set is exactly what the reconciler later
// discovers and compares.
func startRecoveryService(
	t *testing.T, manager *runtime.Manager, appName string, tmpl *app.Template, replica int,
) string {
	t.Helper()
	svc := tmpl.Services[0]
	env, err := reconciler.BuildEnv(context.Background(), tmpl, svc.Port, nil, nil)
	if err != nil {
		t.Fatalf("build service env for %q: %v", appName, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	id, err := manager.StartService(ctx, runtime.ServiceSpec{
		App:       appName,
		Name:      svc.Name,
		SourceRef: svc.SourceRef(),
		Port:      svc.Port,
		Image:     recoveryImage,
		Entry:     recoveryServiceEntry(),
		Env:       env,
		Resources: tmpl.ResourceLimits(),
	}, replica)
	if err != nil {
		t.Fatalf("start service %s/%s replica %d: %v", appName, svc.Name, replica, err)
	}
	return id
}

// createRecoveryExecutionContainer starts a raw managed execution container
// (the strict relay.type=event/schedule form the startup execution sweep
// selects) for one worker identity.
func createRecoveryExecutionContainer(
	t *testing.T, cli *client.Client, workerID, appName, containerType string,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	created, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image:      recoveryImage,
			Entrypoint: recoveryServiceEntry(),
			Labels: map[string]string{
				"relay.type":     containerType,
				"relay.hostname": workerID,
				"relay.app":      appName,
				"relay.handler":  "index.handler",
			},
		},
		// Real Relay execution containers are AutoRemove; a RUNNING abandoned
		// warm container stays on the daemon (AutoRemove only fires on exit), so
		// this keeps the orphan-sweep scenario faithful.
		HostConfig: &container.HostConfig{AutoRemove: true},
	})
	if err != nil {
		t.Fatalf("create %s execution container for %q: %v", containerType, workerID, err)
	}
	if _, err := cli.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start %s execution container %s: %v", containerType, created.ID, err)
	}
	return created.ID
}

// recoveryContainerIDs returns every container (running or exited) carrying all
// of the given labels.
func recoveryContainerIDs(t *testing.T, cli *client.Client, match map[string]string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	list, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		t.Fatalf("list containers: %v", err)
	}
	var ids []string
	for _, c := range list.Items {
		matches := true
		for k, v := range match {
			if c.Labels[k] != v {
				matches = false
				break
			}
		}
		if matches {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// recoveryContainerRunning reports whether the container exists and is running.
func recoveryContainerRunning(t *testing.T, cli *client.Client, id string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return false
		}
		t.Fatalf("inspect container %s: %v", id, err)
	}
	return insp.Container.State != nil && insp.Container.State.Running
}

// recoveryWaitContainerGone waits until the container no longer exists.
func recoveryWaitContainerGone(t *testing.T, cli *client.Client, id string) {
	t.Helper()
	testutil.WaitFor(t, 20*time.Second, "container "+id+" removed", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
		return cerrdefs.IsNotFound(err)
	})
}

// openRecoveryReplacementState opens the replacement incarnation's state handle,
// polling while PostgreSQL finishes releasing the terminated owner's session
// advisory lock. It mirrors testutil.OpenState, but a lock that is still being
// torn down is a transient startup race rather than a test failure.
func openRecoveryReplacementState(t *testing.T, dsn, workerID, instanceID string) *state.State {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var lastErr error
	for {
		st, err := state.Open(context.Background(), state.Options{
			DSN:      dsn,
			Owner:    workerID,
			Instance: instanceID,
		})
		if err == nil {
			t.Cleanup(func() { _ = st.Close() })
			return st
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("open replacement state for %q: %v", workerID, lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestIntegrationStartupRecoveryAdoptsServiceAfterAbruptExit is the end-to-end
// crash-recovery scenario over real Docker and PostgreSQL:
//
//	Given a previous worker incarnation holds WORKER_ID and leaves behind a
//	      running converged service, an obsolete same-identity service, managed
//	      execution containers, and services owned by ANOTHER worker identity,
//	When  its pinned PostgreSQL lock backend is terminated (an abrupt exit) and
//	      a replacement with the SAME WORKER_ID runs the startup sequence,
//	Then  the old execution containers are removed, the converged service is
//	      adopted by the SAME Docker id, the obsolete same-identity service is
//	      removed, every other identity's container survives, and repeating the
//	      recovery is an idempotent no-op.
func TestIntegrationStartupRecoveryAdoptsServiceAfterAbruptExit(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	dsn := testutil.NewStateDSN(t)
	logger := testutil.DiscardLogger()
	ensureRecoveryImage(t, dcli)

	workerID := testutil.UniqueName(t, "recov")
	otherWorkerID := testutil.UniqueName(t, "recov-other")
	desiredApp := testutil.UniqueName(t, "recov-desired")
	removalApp := testutil.UniqueName(t, "recov-remove")
	obsoleteApp := testutil.UniqueName(t, "recov-obsolete")
	executionApp := testutil.UniqueName(t, "recov-exec")
	scheduledApp := testutil.UniqueName(t, "recov-sched")

	// Cleanup every container either identity owns, registered before the first
	// container exists so an early failure cannot leak containers onto the
	// shared daemon.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Logf("cleanup: list containers: %v", err)
			return
		}
		for _, c := range list.Items {
			host := c.Labels["relay.hostname"]
			if host == workerID || host == otherWorkerID {
				if _, err := dcli.ContainerRemove(
					ctx, c.ID, client.ContainerRemoveOptions{Force: true},
				); err != nil {
					t.Logf("cleanup: remove container %s: %v", c.ID, err)
				}
			}
		}
	})

	// Given: the previous incarnation owns the WORKER_ID lock and has left real
	// Docker state behind, as a crash mid-run would.
	previous := testutil.OpenState(t, dsn, state.Options{Owner: workerID, Instance: "previous"})
	// The other identity is genuinely ACTIVE: it holds its own PG lock, so the
	// foreign containers below model a live peer sharing the daemon rather than
	// a stale label set.
	otherState := testutil.OpenState(t, dsn, state.Options{Owner: otherWorkerID, Instance: "other"})
	tmpl := recoveryServiceTemplate()

	previousManager, err := runtime.NewManager(logger, nil, workerID)
	if err != nil {
		t.Fatalf("previous runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = previousManager.Close() })

	desiredID := startRecoveryService(t, previousManager, desiredApp, tmpl, 0)
	obsoleteID := startRecoveryService(t, previousManager, obsoleteApp, tmpl, 0)

	otherManager, err := runtime.NewManager(logger, nil, otherWorkerID)
	if err != nil {
		t.Fatalf("other worker runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = otherManager.Close() })

	// Same app as the desired service, but owned by the OTHER worker identity:
	// convergence must never adopt or disturb it.
	foreignSameAppID := startRecoveryService(t, otherManager, desiredApp, tmpl, 1)
	// Same ABSENT app as the obsolete service, but owned by the OTHER worker
	// identity: the orphan sweep must never sweep it.
	foreignObsoleteID := startRecoveryService(t, otherManager, obsoleteApp, tmpl, 1)

	executionID := createRecoveryExecutionContainer(t, dcli, workerID, executionApp, "event")
	scheduledID := createRecoveryExecutionContainer(t, dcli, workerID, scheduledApp, "schedule")
	foreignExecutionID := createRecoveryExecutionContainer(t, dcli, otherWorkerID, executionApp, "event")

	// Given: the previous incarnation's pinned lock backend is terminated from a
	// SEPARATE connection. The process is gone without releasing anything; the
	// containers it started keep running.
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()

	ownerPID := previous.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("previous owner backend pid = %d, want > 0", ownerPID)
	}
	if _, err := admin.ExecContext(
		context.Background(), `SELECT pg_terminate_backend($1)`, ownerPID,
	); err != nil {
		t.Fatalf("terminate previous owner backend: %v", err)
	}

	// When: a replacement incarnation of the SAME WORKER_ID takes the identity.
	current := openRecoveryReplacementState(t, dsn, workerID, "current")
	if err := current.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("replacement VerifyOwnerLock = %v, want nil", err)
	}
	proof := ownershipProof{loss: &ownershipLoss{}, verify: current.VerifyOwnerLock}

	currentManager, err := runtime.NewManager(logger, nil, workerID)
	if err != nil {
		t.Fatalf("replacement runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = currentManager.Close() })

	svcCtrl := reconciler.NewServiceReconciler(
		currentManager, nil, routing.TraefikConfig{}, logger, reconcileTimeout,
		reconciler.WithWorkerID(workerID),
	)

	// Startup service housekeeping is asynchronous in production: Run publishes
	// the desired services, then an exclusive window waits for them to settle and
	// runs the service-orphan sweep. Drive that real path instead of calling the
	// sweep helper directly, so ordering and the absent-app filter are covered.
	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	services := reconciler.NewServiceCoordinator(svcCtrl)
	services.Start(lifecycle)
	var housekeepingDone <-chan struct{}
	t.Cleanup(func() {
		cancelLifecycle()
		// The exclusive callback runs in the housekeeping goroutine, not a
		// coordinator worker, so Join alone does not join it; wait for it before
		// the manager/state handles close beneath an in-flight sweep.
		if housekeepingDone != nil {
			select {
			case <-housekeepingDone:
			case <-time.After(30 * time.Second):
				t.Logf("startup housekeeping did not stop after cancellation")
			}
		}
		joinCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := services.Join(joinCtx); err != nil {
			t.Logf("coordinator join: %v", err)
		}
	})

	recoverStartup := func() {
		t.Helper()

		// Execution orphan sweep runs first, exactly as Run wires it.
		sweepCtx, cancel := context.WithTimeout(lifecycle, reconcileTimeout)
		_, sweepErr := retryIdentitySnapshotCleanup(
			sweepCtx, proof, logger, "orphan-sweep",
			func(stepCtx context.Context) ([]runtime.OrphanContainer, error) {
				return currentManager.DiscoverOrphanContainers(stepCtx, workerID)
			},
			func(stepCtx context.Context, orphans []runtime.OrphanContainer) (int, error) {
				return currentManager.RemoveOrphanContainers(stepCtx, orphans)
			},
		)
		cancel()
		if sweepErr != nil {
			t.Fatalf("startup execution orphan sweep: %v", sweepErr)
		}

		// Publish the desired state like Run's startup enqueue, then run the
		// startup housekeeping exactly as Run does. Completion is observed on the
		// ABSENT-app canary's Docker id disappearing plus the housekeeping
		// goroutine returning — never on readiness or on Enqueue returning.
		services.Enqueue(desiredApp, tmpl, recoveryImage, nil)
		done := startStartupHousekeeping(lifecycle, logger, startupHousekeeper{
			exclusive: services.RunExclusive,
			sweep: func(hctx context.Context) {
				sweepStartupServiceOrphans(
					hctx, svcCtrl, map[string]bool{desiredApp: true}, proof, logger,
				)
			},
			images: func(context.Context) {},
			deps:   func(context.Context) {},
		})
		recoveryWaitContainerGone(t, dcli, obsoleteID)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("startup housekeeping did not finish after sweeping the absent-app canary")
		}
	}

	// The absent-app canary is running before recovery; its later disappearance
	// must be the exclusive startup sweep, not a pre-existing absence.
	if !recoveryContainerRunning(t, dcli, obsoleteID) {
		t.Fatalf("absent-app canary %s is not running before recovery", obsoleteID)
	}

	recoverStartup()

	// Then: this identity's old execution containers and its obsolete service
	// are gone.
	recoveryWaitContainerGone(t, dcli, executionID)
	recoveryWaitContainerGone(t, dcli, scheduledID)
	recoveryWaitContainerGone(t, dcli, obsoleteID)

	// The converged service was adopted, not recreated: exactly one container for
	// the app and this identity, and it is the SAME immutable Docker id.
	desiredIDs := recoveryContainerIDs(t, dcli, map[string]string{
		"relay.type":     "service",
		"relay.hostname": workerID,
		"relay.app":      desiredApp,
	})
	if len(desiredIDs) != 1 || desiredIDs[0] != desiredID {
		t.Fatalf("desired service containers = %v, want exactly [%s] (same-id adoption)", desiredIDs, desiredID)
	}
	if !recoveryContainerRunning(t, dcli, desiredID) {
		t.Fatalf("adopted service container %s is not running", desiredID)
	}

	// Every other identity's container survives unchanged.
	if !recoveryContainerRunning(t, dcli, foreignSameAppID) {
		t.Fatalf("another worker's service container %s (same app) was disturbed", foreignSameAppID)
	}
	if !recoveryContainerRunning(t, dcli, foreignObsoleteID) {
		t.Fatalf("another worker's service container %s (absent app) was swept", foreignObsoleteID)
	}
	if !recoveryContainerRunning(t, dcli, foreignExecutionID) {
		t.Fatalf("another worker's execution container %s was swept", foreignExecutionID)
	}
	// The other worker identity remains genuinely active: recovering one
	// identity must not disturb a live peer's own lock.
	if err := otherState.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("recovering %q disturbed the other active worker's identity: %v", workerID, err)
	}

	// When: the same recovery runs again, it is an idempotent no-op.
	recoverStartup()
	recoveryWaitContainerGone(t, dcli, executionID)
	recoveryWaitContainerGone(t, dcli, scheduledID)
	recoveryWaitContainerGone(t, dcli, obsoleteID)
	if ids := recoveryContainerIDs(t, dcli, map[string]string{
		"relay.type":     "service",
		"relay.hostname": workerID,
		"relay.app":      desiredApp,
	}); len(ids) != 1 || ids[0] != desiredID {
		t.Fatalf("after repeated recovery desired service containers = %v, want exactly [%s]", ids, desiredID)
	}
	for name, id := range map[string]string{
		"other worker's same-app service":   foreignSameAppID,
		"other worker's absent-app service": foreignObsoleteID,
		"other worker's execution":          foreignExecutionID,
	} {
		if !recoveryContainerRunning(t, dcli, id) {
			t.Fatalf("%s %s was disturbed by repeated recovery", name, id)
		}
	}

	// Worker-scoped app removal must remove only this worker's service even
	// when another active worker has a service for the same app.
	removalServiceID := startRecoveryService(t, currentManager, removalApp, tmpl, 0)
	foreignRemovalServiceID := startRecoveryService(t, otherManager, removalApp, tmpl, 0)
	removeCtx, cancelRemove := context.WithTimeout(context.Background(), 30*time.Second)
	if err := services.Wait(removeCtx); err != nil {
		cancelRemove()
		t.Fatalf("wait for coordinator work before app removal: %v", err)
	}
	removeCalled := false
	if err := services.RunExclusive(removeCtx, func(ctx context.Context) {
		removeCalled = true
		svcCtrl.Remove(ctx, removalApp)
	}); err != nil {
		cancelRemove()
		t.Fatalf("run worker-scoped app removal exclusively: %v", err)
	}
	if err := services.Wait(removeCtx); err != nil {
		cancelRemove()
		t.Fatalf("wait for coordinator work after app removal: %v", err)
	}
	cancelRemove()
	if !removeCalled {
		t.Fatal("RunExclusive did not invoke the app removal")
	}
	recoveryWaitContainerGone(t, dcli, removalServiceID)
	if !recoveryContainerRunning(t, dcli, foreignRemovalServiceID) {
		t.Fatalf("another worker's same-app service %s was removed by worker-scoped app removal", foreignRemovalServiceID)
	}

	// Ordinary live convergence still runs after recovery: publishing the
	// removal through the same coordinator stops this identity's adopted
	// container, while the other worker's same-app container is untouched.
	emptyTemplate := &app.Template{Runtime: "node24"}
	services.Enqueue(desiredApp, emptyTemplate, recoveryImage, nil)
	recoveryWaitContainerGone(t, dcli, desiredID)
	if !recoveryContainerRunning(t, dcli, foreignSameAppID) {
		t.Fatalf("another worker's same-app service %s was removed by ordinary convergence", foreignSameAppID)
	}
}

// TestIntegrationStartupRecoveryFinishesInterruptedOrphanSweep proves that a
// startup execution-orphan sweep interrupted by a crash is completed by the
// next replacement of the SAME WORKER_ID:
//
//	Given a worker identity owns TWO abandoned managed execution containers,
//	When  one replacement removes only ONE of them and then loses its pinned
//	      PostgreSQL backend (an abrupt exit) before touching the other,
//	Then  a subsequent replacement of the SAME identity discovers and removes
//	      the remaining container, leaving no execution orphan behind.
//
// The first replacement removes one candidate through the real removal path and
// then stops, modelling a process killed mid-sweep; this is not a claim about
// retry counts, which the unit tests pin instead.
func TestIntegrationStartupRecoveryFinishesInterruptedOrphanSweep(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	dsn := testutil.NewStateDSN(t)
	logger := testutil.DiscardLogger()
	ensureRecoveryImage(t, dcli)

	workerID := testutil.UniqueName(t, "recov-partial")
	firstApp := testutil.UniqueName(t, "recov-partial-a")
	secondApp := testutil.UniqueName(t, "recov-partial-b")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Logf("cleanup: list containers: %v", err)
			return
		}
		for _, c := range list.Items {
			if c.Labels["relay.hostname"] != workerID {
				continue
			}
			if _, err := dcli.ContainerRemove(
				ctx, c.ID, client.ContainerRemoveOptions{Force: true},
			); err != nil {
				t.Logf("cleanup: remove container %s: %v", c.ID, err)
			}
		}
	})

	// Given: the previous incarnation owns the identity and leaves two managed
	// execution containers behind.
	previous := testutil.OpenState(t, dsn, state.Options{Owner: workerID, Instance: "previous"})
	firstID := createRecoveryExecutionContainer(t, dcli, workerID, firstApp, "event")
	secondID := createRecoveryExecutionContainer(t, dcli, workerID, secondApp, "schedule")

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()

	ownerPID := previous.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("previous owner backend pid = %d, want > 0", ownerPID)
	}
	if _, err := admin.ExecContext(
		context.Background(), `SELECT pg_terminate_backend($1)`, ownerPID,
	); err != nil {
		t.Fatalf("terminate previous owner backend: %v", err)
	}

	// When: the first replacement removes only ONE of the two candidates and
	// then exits abruptly before it can sweep the other.
	partial := openRecoveryReplacementState(t, dsn, workerID, "partial")
	partialManager, err := runtime.NewManager(logger, nil, workerID)
	if err != nil {
		t.Fatalf("partial replacement runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = partialManager.Close() })

	discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), reconcileTimeout)
	orphans, err := partialManager.DiscoverOrphanContainers(discoverCtx, workerID)
	cancelDiscover()
	if err != nil {
		t.Fatalf("partial replacement discover orphans: %v", err)
	}
	byID := make(map[string]runtime.OrphanContainer, len(orphans))
	for _, orphan := range orphans {
		byID[orphan.ID] = orphan
	}
	first, ok := byID[firstID]
	if !ok {
		t.Fatalf("discovered orphans %v do not include first execution container %s", orphans, firstID)
	}
	if _, ok := byID[secondID]; !ok {
		t.Fatalf("discovered orphans %v do not include second execution container %s", orphans, secondID)
	}

	removeCtx, cancelRemove := context.WithTimeout(context.Background(), reconcileTimeout)
	if _, err := partialManager.RemoveOrphanContainers(
		removeCtx, []runtime.OrphanContainer{first},
	); err != nil {
		cancelRemove()
		t.Fatalf("partial replacement remove %s: %v", firstID, err)
	}
	cancelRemove()
	recoveryWaitContainerGone(t, dcli, firstID)
	if !recoveryContainerRunning(t, dcli, secondID) {
		t.Fatalf("second execution container %s must survive the interrupted sweep", secondID)
	}

	// The partial replacement dies mid-sweep: its pinned backend is terminated
	// and it performs no graceful cleanup.
	partialPID := partial.OwnerBackendPID()
	if partialPID <= 0 {
		t.Fatalf("partial replacement backend pid = %d, want > 0", partialPID)
	}
	if _, err := admin.ExecContext(
		context.Background(), `SELECT pg_terminate_backend($1)`, partialPID,
	); err != nil {
		t.Fatalf("terminate partial replacement backend: %v", err)
	}

	// Then: the next replacement of the SAME identity completes the sweep.
	final := openRecoveryReplacementState(t, dsn, workerID, "final")
	finalManager, err := runtime.NewManager(logger, nil, workerID)
	if err != nil {
		t.Fatalf("final replacement runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = finalManager.Close() })

	finalProof := ownershipProof{loss: &ownershipLoss{}, verify: final.VerifyOwnerLock}
	sweepCtx, cancelSweep := context.WithTimeout(context.Background(), reconcileTimeout)
	_, sweepErr := retryIdentitySnapshotCleanup(
		sweepCtx, finalProof, logger, "orphan-sweep",
		func(stepCtx context.Context) ([]runtime.OrphanContainer, error) {
			return finalManager.DiscoverOrphanContainers(stepCtx, workerID)
		},
		func(stepCtx context.Context, orphans []runtime.OrphanContainer) (int, error) {
			return finalManager.RemoveOrphanContainers(stepCtx, orphans)
		},
	)
	cancelSweep()
	if sweepErr != nil {
		t.Fatalf("final replacement execution sweep: %v", sweepErr)
	}
	recoveryWaitContainerGone(t, dcli, secondID)

	for _, containerType := range []string{"event", "schedule"} {
		if ids := recoveryContainerIDs(t, dcli, map[string]string{
			"relay.hostname": workerID,
			"relay.type":     containerType,
		}); len(ids) != 0 {
			t.Fatalf("%s execution orphans remain after the completing restart: %v", containerType, ids)
		}
	}
}

// TestIntegrationStartupSweepsSkipRemovalAfterOwnershipLoss proves the
// startup-path fail-closed contract over real Docker and PostgreSQL: after the
// pinned WORKER_ID lock is genuinely lost, both startup sweeps still discover
// this identity's candidates but the failed ownership proof suppresses every
// removal, so a replacement owner's containers are never disturbed.
func TestIntegrationStartupSweepsSkipRemovalAfterOwnershipLoss(t *testing.T) {
	dcli := testutil.RequireDocker(t)
	dsn := testutil.NewStateDSN(t)
	logger := testutil.DiscardLogger()
	ensureRecoveryImage(t, dcli)

	workerID := testutil.UniqueName(t, "recov-lost")
	serviceApp := testutil.UniqueName(t, "recov-lost-svc")
	executionApp := testutil.UniqueName(t, "recov-lost-exec")

	// Cleanup every container this identity owns, registered before the first
	// container exists so an early failure cannot leak one.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		list, err := dcli.ContainerList(ctx, client.ContainerListOptions{All: true})
		if err != nil {
			t.Logf("cleanup: list containers: %v", err)
			return
		}
		for _, c := range list.Items {
			if c.Labels["relay.hostname"] != workerID {
				continue
			}
			if _, err := dcli.ContainerRemove(
				ctx, c.ID, client.ContainerRemoveOptions{Force: true},
			); err != nil {
				t.Logf("cleanup: remove container %s: %v", c.ID, err)
			}
		}
	})

	st := testutil.OpenState(t, dsn, state.Options{Owner: workerID, Instance: "inst-1"})
	manager, err := runtime.NewManager(logger, nil, workerID)
	if err != nil {
		t.Fatalf("runtime manager: %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	svcCtrl := reconciler.NewServiceReconciler(
		manager, nil, routing.TraefikConfig{}, logger, reconcileTimeout,
		reconciler.WithWorkerID(workerID),
	)

	// The service's app is not in liveNames, so it is a sweep candidate; the
	// execution container is a managed orphan for this identity.
	serviceID := startRecoveryService(t, manager, serviceApp, recoveryServiceTemplate(), 0)
	executionID := createRecoveryExecutionContainer(t, dcli, workerID, executionApp, "event")

	// Given: the pinned lock backend is terminated from a SEPARATE connection.
	ownerPID := st.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner backend pid = %d, want > 0", ownerPID)
	}
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

	// When: the startup service-orphan sweep runs with the real failed proof.
	serviceLoss := &ownershipLoss{}
	serviceProof := ownershipProof{
		loss:   serviceLoss,
		verify: st.VerifyOwnerLock,
		onLost: ownershipLossAction(serviceLoss, nil, logger, workerID, "inst-1", func() {}),
	}
	sweepStartupServiceOrphans(context.Background(), svcCtrl, nil, serviceProof, logger)

	// Then: the proof ran (so discovery captured the container first), the loss
	// is latched, and the running container survives.
	if !serviceLoss.isLost() {
		t.Fatal("the startup service sweep must latch a failed ownership proof")
	}
	if !recoveryContainerRunning(t, dcli, serviceID) {
		t.Fatalf("service container %s was removed despite a failed ownership proof", serviceID)
	}

	// When: the startup execution sweep runs with its own fresh failed proof.
	executionLoss := &ownershipLoss{}
	executionProof := ownershipProof{
		loss:   executionLoss,
		verify: st.VerifyOwnerLock,
		onLost: ownershipLossAction(executionLoss, nil, logger, workerID, "inst-1", func() {}),
	}
	sweepCtx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
	_, sweepErr := retryIdentitySnapshotCleanup(
		sweepCtx, executionProof, logger, "orphan-sweep",
		func(stepCtx context.Context) ([]runtime.OrphanContainer, error) {
			return manager.DiscoverOrphanContainers(stepCtx, workerID)
		},
		func(stepCtx context.Context, orphans []runtime.OrphanContainer) (int, error) {
			return manager.RemoveOrphanContainers(stepCtx, orphans)
		},
	)
	cancel()
	if sweepErr != nil {
		t.Fatalf("startup execution sweep = %v, want nil (removal suppressed, not fatal)", sweepErr)
	}

	// Then: the execution orphan also survives.
	if !executionLoss.isLost() {
		t.Fatal("the startup execution sweep must latch a failed ownership proof")
	}
	if !recoveryContainerRunning(t, dcli, executionID) {
		t.Fatalf("execution container %s was removed despite a failed ownership proof", executionID)
	}
}

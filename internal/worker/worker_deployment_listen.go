package worker

import (
	"context"
	"log/slog"
	"time"

	"relay/internal/deployment"
	"relay/internal/state"
)

// deploymentListenRetryInitial is the first reconnect delay after a failed
// subscription or a dropped LISTEN session. deploymentListenRetryMax bounds the
// exponential backoff.
const (
	deploymentListenRetryInitial = time.Second
	deploymentListenRetryMax     = 30 * time.Second
)

// startDeploymentListener starts the optional PostgreSQL LISTEN supervisor for
// coordinated deployment hints and registers its shutdown barrier.
//
// The listener is an optimization, never a dependency: if it cannot subscribe,
// the coordinator's fallback poll still converges every published revision. A
// listener failure therefore never cancels the worker. The supervisor runs in
// its own goroutine because a blocking wait must not compete with worker
// orchestration; the registered barrier joins it (and releases its dedicated
// connection) before the state handle closes in a later shutdown step.
//
// It must be started after the coordinator loop is running: the subscription
// itself is followed by a normal wake, so a revision published between the
// startup read and the LISTEN statement is still picked up promptly.
func startDeploymentListener(
	ctx context.Context,
	logger *slog.Logger,
	st *state.State,
	coordinator *deployment.Coordinator,
	shutdown *shutdownRegistry,
) {
	if st == nil || coordinator == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseDeploymentListener(ctx, logger, st, coordinator)
	}()
	shutdown.register(deploymentListenerBarrierStep(done))
}

// superviseDeploymentListener keeps one LISTEN session subscribed for as long
// as it is healthy and reconnects with bounded exponential backoff otherwise.
// Every successful subscription (initial or reconnect) issues one normal wake,
// which is the catch-up read for anything published while no session existed.
func superviseDeploymentListener(
	ctx context.Context,
	logger *slog.Logger,
	st *state.State,
	coordinator *deployment.Coordinator,
) {
	backoff := deploymentListenRetryInitial
	for ctx.Err() == nil {
		listener, err := st.ListenDeploymentDesired(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.WarnContext(ctx,
				"Deployment: desired revision subscription failed; polling fallback remains active",
				"error", err,
				"retry_in", backoff)
			if !sleepDeploymentListener(ctx, backoff) {
				return
			}
			backoff = nextDeploymentListenBackoff(backoff)
			continue
		}
		backoff = deploymentListenRetryInitial
		logger.DebugContext(ctx, "Deployment: subscribed to desired revision notifications")
		coordinator.Wake()
		err = awaitDeploymentNotifications(ctx, listener, coordinator)
		_ = listener.Close()
		if ctx.Err() != nil {
			return
		}
		logger.WarnContext(ctx,
			"Deployment: desired revision subscription ended; reconnecting",
			"error", err,
			"retry_in", backoff)
		if !sleepDeploymentListener(ctx, backoff) {
			return
		}
		backoff = nextDeploymentListenBackoff(backoff)
	}
}

// awaitDeploymentNotifications turns every hint into a normal (non-forcing)
// coordinator wake. The hint never supplies state: the coordinator re-reads the
// authoritative desired row.
func awaitDeploymentNotifications(
	ctx context.Context,
	listener *state.DeploymentDesiredListener,
	coordinator *deployment.Coordinator,
) error {
	for {
		notified, err := listener.Wait(ctx)
		if err != nil {
			return err
		}
		if notified {
			coordinator.Wake()
		}
	}
}

// sleepDeploymentListener waits for the reconnect delay or reports whether the
// lifecycle ended first.
func sleepDeploymentListener(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextDeploymentListenBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > deploymentListenRetryMax {
		return deploymentListenRetryMax
	}
	return d
}

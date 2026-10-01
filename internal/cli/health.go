package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/urfave/cli/v3"

	"relay/internal/worker"
)

// healthCommand builds the `relay health` subcommand. It reports whether the
// RUNNING worker is healthy by dialing the worker's control socket
// (worker.CheckReady) — the same socket `app inspect` and `stats reset`
// use — and exits 0 only when the worker reports ready with its live
// dependencies healthy, 1 otherwise. It is the single user-facing
// health/readiness query.
//
// It deliberately loads no configuration and instantiates no Redis or Docker
// client in the CLI process: the answer comes entirely from the worker over its
// socket. There is no fallback — with no running worker (an unreachable socket)
// health fails even if Redis and Docker happen to be healthy, because
// dependency health is only meaningful as observed by the worker that owns
// them.
func healthCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "health",
		Usage: "Check whether the running worker is healthy",
		Description: "Ask the running worker (over its control socket) whether it is ready " +
			"to consume. Exits 0 when ready, 1 when not: no worker is running, the worker " +
			"is still starting or shutting down, or its live dependencies (Redis consumer, " +
			"Docker, configured NETWORKS) are currently failing.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("health: too many arguments", 2)
			}
			return runHealthCommand(ctx, cmd.Writer, deps.SocketPath)
		},
	}
}

// runHealthCommand implements `relay health` against the worker socket at path.
// It writes "healthy" and returns nil only for a genuinely ready worker with
// healthy live dependencies. Every other outcome is an error (the root
// ExitErrHandler prints the message once and exits 1), with a concise, useful
// reason:
//
//   - an unreachable socket (no worker, a stale socket, or a failed exchange)
//     is "worker not running";
//   - a worker that answered not-ready surfaces its own short reason, which
//     names the phase or failed dependency (e.g. "worker starting",
//     "worker shutting down", "redis consumer unhealthy", "docker unavailable",
//     a missing NETWORKS network).
func runHealthCommand(ctx context.Context, w io.Writer, path string) error {
	ready, reason, err := worker.CheckReady(ctx, path)
	if err == nil {
		if !ready {
			// Defensive: CheckReady reports not-ready as an error, so this is
			// unreachable in practice.
			return fmt.Errorf("health: worker not ready")
		}
		fmt.Fprintln(w, "healthy")
		return nil
	}
	switch {
	case errors.Is(err, worker.ErrReadyUnavailable):
		// No worker answered: no socket, a stale socket, or a failed exchange.
		// Worker health is only meaningful as observed by the worker that owns
		// the dependencies, so this is a failure even if Redis/Docker are up.
		return fmt.Errorf("health: worker not running")
	case errors.Is(err, worker.ErrNotReady):
		// The worker answered not-ready. Surface its own short reason, which
		// names the phase or failed dependency ("worker starting", "worker
		// shutting down", "redis consumer unhealthy", "docker unavailable", a
		// missing network, ...).
		if reason == "" {
			reason = "not ready"
		}
		return fmt.Errorf("health: %s", reason)
	default:
		return fmt.Errorf("health: %w", err)
	}
}

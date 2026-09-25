package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v3"

	"relay/internal/processlock"
	"relay/internal/worker"
)

// errStartAlreadyRunning is the concise operator-facing error returned when
// another process already holds the start lock. It carries no stack trace; the
// process prints it exactly once via cmd/main.go.
var errStartAlreadyRunning = errors.New("relay start is already running")

// startRun is the hook the "start" command delegates to. It is a package-level
// variable so tests can observe dispatch without launching the runtime. The
// worker's Run returns any startup/runtime failure (after converging cleanup)
// and the hook propagates it unchanged, so cmd/main.go prints it and owns the
// process exit.
var startRun = func(l *slog.Logger) error {
	return worker.Run(l)
}

// startCommand builds the `relay start` subcommand. It is the only command
// that starts the long-running Relay runtime, delegating to internal/worker in
// the foreground. The process never daemonizes or writes a PID file; Docker,
// systemd, Kubernetes, or a terminal owns process supervision.
//
// Before entering the runtime it creates deps.LockPath's parent directory and
// takes a process-level flock on deps.LockPath and holds it for the whole run
// (released by the deferred Close, or by the kernel if the process dies), so a
// second `relay start` against the same state directory fails fast instead of
// racing the first. No other subcommand acquires this lock.
func startCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start the runtime",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("start: too many arguments", 2)
			}

			// Create the ephemeral runtime directory (/run/relay in production)
			// BEFORE taking the lock, so both the lock file and the worker's later
			// socket bind have their parent. It is deliberately outside the
			// persistent /var/lib/relay state volume; the injected lock's
			// directory is the single source for it.
			if err := os.MkdirAll(filepath.Dir(deps.LockPath), 0o755); err != nil {
				return fmt.Errorf("relay start: cannot create runtime dir: %w", err)
			}

			// Process boundary: acquire the single-instance lock before the
			// worker touches Redis, Docker, or /functions. The lock is held for
			// the whole runtime lifetime via the deferred Close.
			lock, err := processlock.Acquire(deps.LockPath)
			if err != nil {
				if errors.Is(err, processlock.ErrAlreadyLocked) {
					return errStartAlreadyRunning
				}
				return fmt.Errorf("relay start: cannot initialize process lock: %w", err)
			}
			defer lock.Close()

			// Delegation, not implementation: the CLI only parses commands; the
			// long-running Relay lifecycle lives in internal/worker.
			return startRun(logger)
		},
	}
}

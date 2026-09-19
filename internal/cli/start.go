package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/urfave/cli/v3"

	"relay/internal/processlock"
	"relay/internal/worker"
)

// startLockPath is the process-level lock file `relay start` holds for its
// lifetime. It defaults to the fixed internal path (/var/lib/relay/relay.lock);
// tests replace it with a temp path so they never touch /var/lib/relay. It is a
// package-level variable purely for dependency injection, mirroring statePath,
// gitConfigPath, and the other CLI path seams.
var startLockPath = processlock.DefaultPath

// errStartAlreadyRunning is the concise operator-facing error returned when
// another process already holds the start lock. It carries no stack trace; the
// process prints it exactly once via cmd/main.go.
var errStartAlreadyRunning = errors.New("relay start is already running")

// startRun is the hook the "start" command delegates to. It is a package-level
// variable so tests can observe dispatch without launching the runtime. The
// worker's Run signature is fixed and must not change.
var startRun = func(l *slog.Logger) error {
	worker.Run(l)
	return nil
}

// startCommand builds the `relay start` subcommand. It is the only command
// that starts the long-running Relay runtime, delegating to internal/worker in
// the foreground. The process never daemonizes or writes a PID file; Docker,
// systemd, Kubernetes, or a terminal owns process supervision.
//
// Before entering the runtime it takes a process-level flock on startLockPath
// and holds it for the whole run (released by the deferred Close, or by the
// kernel if the process dies), so a second `relay start` against the same state
// directory fails fast instead of racing the first. No other subcommand
// acquires this lock.
func startCommand(logger *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start the runtime",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("start: too many arguments", 2)
			}

			// Process boundary: acquire the single-instance lock before the
			// worker touches Redis, Docker, or /functions. The lock is held for
			// the whole runtime lifetime via the deferred Close.
			lock, err := processlock.Acquire(startLockPath)
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

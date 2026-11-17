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
)

// errStartAlreadyRunning is the concise operator-facing error returned when
// another process already holds the start lock. It carries no stack trace; the
// process reports it exactly once via cmd/main.go.
var errStartAlreadyRunning = errors.New("relay start is already running")

// startFailure marks a failed `relay start` lifecycle for the process boundary
// (cmd/main.go). Administrative commands return plain errors that are printed to
// stderr; start failures are reported through the structured process logger
// instead, with the full wrapped cause retained. The marker implements Unwrap,
// so errors.Is/As still classify the underlying error — notably urfave/cli
// ExitCoders, whose exit code and plain usage message must be preserved.
type startFailure struct{ err error }

func (f *startFailure) Error() string { return f.err.Error() }
func (f *startFailure) Unwrap() error { return f.err }

// IsStartFailure reports whether err is a failure of the `relay start`
// lifecycle. cmd/main.go uses it to choose structured reporting without
// inspecting argv, so aliases, flags, and future commands cannot be
// misclassified.
func IsStartFailure(err error) bool {
	var failure *startFailure
	return errors.As(err, &failure)
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
//
// Delegation goes through deps.Start, the injected long-running lifecycle, so
// the command observes dispatch without a package-level hook and tests supply a
// fake runner directly.
func startCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "start",
		Usage:       "Start the runtime",
		Description: "Run in the foreground until stopped.",
		Action: func(ctx context.Context, cmd *cli.Command) (err error) {
			// Every post-parse failure from this command is marked for the
			// process boundary, which reports it as one structured error event.
			// The marker preserves the cause, so ExitCoder usage errors keep
			// their native code and plain message.
			defer func() {
				if err != nil {
					err = &startFailure{err: err}
				}
			}()

			if cmd.Args().Present() {
				return cli.Exit("start: too many arguments", 2)
			}

			// Create the ephemeral runtime directory (/run/relay in production)
			// BEFORE taking the lock, so both the lock file and the worker's later
			// socket bind have their parent. It is deliberately outside the
			// persistent /var/lib/relay state volume; the injected lock's
			// directory is the single source for it. The mode is owner-only and
			// matches processlock's directory policy; Acquire re-enforces it
			// (MkdirAll's mode is creation-only and umask-masked).
			runtimeDir := filepath.Dir(deps.LockPath)
			if err := os.MkdirAll(runtimeDir, processlock.DirPerm); err != nil {
				return fmt.Errorf("relay start: cannot create runtime dir: %w", err)
			}
			if err := os.Chmod(runtimeDir, processlock.DirPerm); err != nil {
				return fmt.Errorf("relay start: cannot secure runtime dir: %w", err)
			}

			// Process boundary: acquire the single-instance lock before the
			// worker touches Redis, Docker, or /apps. The lock is held for
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
			// long-running Relay lifecycle lives in internal/worker, injected as
			// deps.Start.
			return deps.Start(logger)
		},
	}
}

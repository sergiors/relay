// Package cli builds the relay command tree on github.com/urfave/cli/v3. It
// owns parsing and native help only; the long-running runtime lifecycle lives
// in internal/worker (see `relay start`). Errors are RETURNED, never printed or
// turned into os.Exit here: cmd/main.go prints the returned error exactly once
// and owns the process exit. Run receives the context from main, so signal
// cancellation propagates into ctx-aware paths like health probes.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/urfave/cli/v3"

	"relay/internal/processlock"
	"relay/internal/state"
	"relay/internal/worker"
)

// Dependencies carries the CLI's injectable filesystem locations. The three
// paths are the only process-wide filesystem seams the command tree needs;
// production wires the immutable defaults via DefaultDependencies, while tests
// (and any embedding host) construct their own pointing at temp locations. It
// deliberately replaces the former package-level mutable path variables
// (statePath, runtimeSocketPath, startLockPath), so a command's paths come from
// its construction rather than from global state.
type Dependencies struct {
	// StatePath is the local state database the read-only administrative
	// commands (function, stats) open. Production: state.DBPath.
	StatePath string
	// SocketPath is the live worker query socket `relay function inspect`
	// dials for the live runtime-pool gauges. Production: worker.SocketPath.
	SocketPath string
	// LockPath is the process-level lock file `relay start` holds for its
	// lifetime; its parent directory is created before the lock is acquired.
	// Production: processlock.DefaultPath.
	LockPath string
}

// DefaultDependencies returns the fixed production filesystem locations. The
// referenced constants stay immutable application conventions; there are no
// environment overrides or setters.
func DefaultDependencies() Dependencies {
	return Dependencies{
		StatePath:  state.DBPath,
		SocketPath: worker.SocketPath,
		LockPath:   processlock.DefaultPath,
	}
}

// New constructs the fully configured root relay command. logger is the process
// logger relay start hands to the worker; writer is set on the root and
// propagated to subcommands (urfave inherits nil Reader/Writer/ErrWriter from
// the parent); deps supplies the injected filesystem locations. New does not
// execute the CLI, create a context, or exit.
//
// The silent ExitErrHandler MUST stay set: urfave's default HandleExitCoder
// would print the error and call os.Exit itself, but printing belongs exactly
// once to cmd/main.go.
func New(logger *slog.Logger, writer io.Writer, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:            "relay",
		Usage:           "Relay event-driven function runner",
		HideHelpCommand: true,
		HideVersion:     true,
		Writer:          writer,
		ExitErrHandler:  func(context.Context, *cli.Command, error) {},
		Commands: []*cli.Command{
			startCommand(logger, deps),
			functionCommand(deps),
			secretCommand(),
			gitCommand(logger),
			statsCommand(deps),
			healthCommand(logger),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if !cmd.Args().Present() {
				return cli.ShowAppHelp(cmd)
			}

			return fmt.Errorf(
				"relay: unknown command: relay %s\n\nRun 'relay --help' for more information",
				cmd.Args().First(),
			)
		},
	}
}

// Run executes the root command tree against args. args includes the program
// name; urfave's parser consumes it. Run neither prints the error nor exits:
// it returns the tree's error to cmd/main.go, which prints and exits once. It
// wires the production filesystem defaults; tests construct New with temp paths
// instead of calling Run.
func Run(ctx context.Context, args []string, logger *slog.Logger) error {
	return New(logger, os.Stdout, DefaultDependencies()).Run(ctx, args)
}

// Package cli builds the relay command tree on github.com/urfave/cli/v3. It
// owns parsing and native help only; the long-running runtime lifecycle lives
// in internal/worker (see `relay start`). Errors are RETURNED, never printed or
// turned into os.Exit here: cmd/main.go prints the returned error exactly once
// and owns the process exit. Run receives the context from main, so signal
// cancellation propagates into ctx-aware paths like health probes.
package cli

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

// New constructs the fully configured root relay command. logger is the process
// logger relay start hands to the worker; writer is set on the root and
// propagated to subcommands (urfave inherits nil Reader/Writer/ErrWriter from
// the parent). New does not execute the CLI, create a context, or exit.
//
// The silent ExitErrHandler MUST stay set: urfave's default HandleExitCoder
// would print the error and call os.Exit itself, but printing belongs exactly
// once to cmd/main.go.
func New(logger *log.Logger, writer io.Writer) *cli.Command {
	return &cli.Command{
		Name:            "relay",
		Usage:           "Relay is a Redis Streams \u2192 function event relay",
		HideHelpCommand: true,
		HideVersion:     true,
		Writer:          writer,
		ExitErrHandler:  func(context.Context, *cli.Command, error) {},
		Commands: []*cli.Command{
			startCommand(logger),
			functionCommand(),
			secretCommand(),
			statsCommand(),
			healthCommand(),
		},
	}
}

// Run executes the root command tree against args. args includes the program
// name; urfave's parser consumes it. Run neither prints the error nor exits:
// it returns the tree's error to cmd/main.go, which prints and exits once.
func Run(ctx context.Context, args []string, logger *log.Logger) error {
	return New(logger, os.Stdout).Run(ctx, args)
}

// Command relay is the single Relay executable. `relay start` runs the
// long-running Relay runtime in the foreground; every other subcommand is an
// administrative interface around the same process (functions, secrets,
// stats, health). It stays minimal on purpose: command construction and
// parsing live in internal/cli and the runtime lifecycle in internal/worker.
// main owns the process logger, a signal-aware context, error printing
// (exactly once), and the process exit.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lmittmann/tint"
	urfavecli "github.com/urfave/cli/v3"

	"relay/internal/cli"
	"relay/internal/config"
)

func main() {
	// Bootstrap the process logger from LOG_LEVEL before any command runs, so
	// all process-level logging follows the configured level. LOG_LEVEL is
	// parsed here (fail-fast on an invalid value) and parsed again inside
	// config.Load as the commands dispatch; both agree, so the value is
	// validated exactly once at the executable boundary.
	level, err := config.ParseLogLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger := slog.New(tint.NewTextHandler(os.Stdout, &tint.Options{
		Level:      level,
		TimeFormat: "2006-01-02 15:04:05",
	}))

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// runCommand owns the process-boundary error policy: print the returned
	// error exactly once to stderr and map it to an exit code. main only acts
	// on a nonzero code, so the success path exits 0 naturally by returning.
	if code := runCommand(func() error {
		return cli.Run(ctx, os.Args, logger)
	}, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

// runCommand executes fn and maps its error to a process exit code, writing the
// error to stderr exactly once. It is the single place that translates a
// returned error into an exit code: nil yields 0 and writes nothing; an error
// wrapping a urfave/cli ExitCoder (the cli.Exit(msg, 2) usage errors the command
// tree returns) yields that explicit code; any other error yields 1. Isolating
// this as a pure function of (func() error, io.Writer) lets tests assert the
// boundary directly instead of spawning the process. os.Exit is deliberately
// not called here — only main, the caller that receives code, owns the process.
func runCommand(fn func() error, stderr io.Writer) int {
	err := fn()
	if err == nil {
		return 0
	}

	fmt.Fprintln(stderr, err)

	var exitErr urfavecli.ExitCoder
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 1
}

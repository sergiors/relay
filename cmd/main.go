// Command relay is the single Relay executable. `relay start` runs the
// long-running Relay runtime in the foreground; every other subcommand is an
// administrative interface around the same process (apps, secrets,
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
	"relay/internal/worker"
)

func main() {
	// Bootstrap the process logger from LOG_LEVEL before any command runs, so
	// all process-level logging follows the configured level. LOG_LEVEL is
	// parsed here (fail-fast on an invalid value) so logger construction can
	// never use an invalid level, and config.Load validates it again when the
	// worker starts. Both checks are deliberate: an invalid value is rejected at
	// the earliest process boundary, not only after command dispatch.
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

	// runCommand owns the process-boundary error policy: report the returned
	// error exactly once and map it to an exit code. main only acts on a nonzero
	// code, so the success path exits 0 naturally by returning.
	if code := runCommand(func() error {
		return cli.Run(ctx, os.Args, logger)
	}, os.Stderr, logger); code != 0 {
		os.Exit(code)
	}
}

// runCommand executes fn and maps its error to a process exit code, reporting
// the error exactly once. A urfave/cli ExitCoder (native usage/help errors and
// the command tree's explicit cli.Exit values) keeps its plain stderr message
// and explicit code. A `relay start` failure is reported as one structured
// error log event through logger: "Worker startup failed" for startup
// failures (including CLI lock/setup errors) and "Worker stopped" for a worker
// failure after it became ready, classified by the typed phase on worker.Run's
// error. The full wrapped cause is retained (for example the PostgreSQL startup
// context). Administrative commands keep their plain stderr line because their
// output is human-facing. Any other error yields 1. Isolating this as a pure
// function of (func() error, io.Writer, *slog.Logger) lets tests assert the
// boundary directly instead of spawning the process. os.Exit is deliberately
// not called here — only main, the caller that receives code, owns the process.
func runCommand(fn func() error, stderr io.Writer, logger *slog.Logger) int {
	err := fn()
	if err == nil {
		return 0
	}

	var exitErr urfavecli.ExitCoder
	if errors.As(err, &exitErr) {
		fmt.Fprintln(stderr, err)
		return exitErr.ExitCode()
	}

	if cli.IsStartFailure(err) {
		if logger != nil {
			message := "Worker startup failed"
			if phase, ok := worker.RunPhaseOf(err); ok && phase == worker.RunPhaseRuntime {
				message = "Worker stopped"
			}
			logger.Error(message, "error", err)
			return 1
		}
		// No process logger wired: keep the full cause visible rather than
		// dropping the report.
		fmt.Fprintln(stderr, err)
		return 1
	}

	fmt.Fprintln(stderr, err)
	return 1
}

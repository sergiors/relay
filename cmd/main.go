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
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lmittmann/tint"

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

	if err := cli.Run(ctx, os.Args, logger); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

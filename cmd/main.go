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
	"log"
	"os"
	"os/signal"
	"syscall"

	"relay/internal/cli"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := cli.Run(ctx, os.Args, logger); err != nil {
		logger.Print(err)
		os.Exit(1)
	}
}

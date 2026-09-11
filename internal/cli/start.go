package cli

import (
	"context"
	"log"

	"github.com/urfave/cli/v3"

	"relay/internal/worker"
)

// startRun is the hook the "start" command delegates to. It is a package-level
// variable so tests can observe dispatch without launching the runtime. The
// worker's Run signature is fixed and must not change.
var startRun = func(l *log.Logger) error {
	worker.Run(l)
	return nil
}

// startCommand builds the `relay start` subcommand. It is the only command
// that starts the long-running Relay runtime, delegating to internal/worker in
// the foreground. The process never daemonizes or writes a PID file; Docker,
// systemd, Kubernetes, or a terminal owns process supervision.
func startCommand(logger *log.Logger) *cli.Command {
	return &cli.Command{
		Name:  "start",
		Usage: "Start Relay",
		Description: "Start Relay in the foreground and block until it is interrupted " +
			"(SIGINT/SIGTERM). Relay does not background itself or write a PID file; " +
			"the container manager, init system, or terminal owns process supervision.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("start: too many arguments", 2)
			}
			// Delegation, not implementation: the CLI only parses commands; the
			// long-running Relay lifecycle lives in internal/worker.
			return startRun(logger)
		},
	}
}

package cli

import (
	"log"
	"os"

	"relay/internal/worker"
)

// startRun is the hook the "start" command delegates to. It is a package-level
// variable so tests can observe dispatch without launching the runtime.
var startRun = func(l *log.Logger) int {
	worker.Run(l)
	return 0
}

func startUsage() string {
	return "Usage:\n  relay start\n\nStart Relay in the foreground.\n\nRelay blocks until it is interrupted (SIGINT/SIGTERM). It does not\nbackground itself or write a PID file; Docker, systemd, Kubernetes, or a\nterminal owns process supervision:\n\n  docker compose up -d\n  docker run -d ...\n  systemctl start relay\n"
}

// runStartCommand delegates to the worker's long-running lifecycle. The CLI
// stays a thin dispatcher: it owns argument parsing and exit codes, while
// internal/worker owns configuration, dependencies, recovery, and graceful
// shutdown. The process runs in the foreground by design — supervision belongs
// to the container manager, init system, or terminal, not to Relay itself.
func runStartCommand() int {
	return startRun(log.New(os.Stdout, "", log.LstdFlags))
}

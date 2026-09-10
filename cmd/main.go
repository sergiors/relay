// Command relay is the single Relay executable. `relay start` runs the
// long-running Relay runtime in the foreground; every other subcommand is an
// administrative interface around the same process (functions, secrets,
// stats, health). It stays minimal on purpose: command parsing lives in
// internal/cli and the runtime lifecycle in internal/worker.
package main

import (
	"os"

	"relay/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}

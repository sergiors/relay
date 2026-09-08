package main

import (
	"fmt"
	"os"
)

// usage text printed to stderr for the CLI entrypoint. The CLI is read-only:
// it never starts the worker. The long-running process is a separate binary,
// relay-worker.
const usageText = `Relay command-line interface

Usage:
  relay function ls                     List functions from the local state DB
  relay function inspect <name>         Show detail for one function
  relay health                          Check Redis and Docker reachability

The long-running worker is the 'relay-worker' binary.
`

func main() {
	os.Exit(runCLI(os.Args[1:]))
}

// runCLI dispatches the CLI's subcommands. It is separated from main so tests
// can exercise the exit codes directly. The CLI is read-only and never starts
// the worker (a separate relay-worker binary). Exit codes:
//
//	0  success
//	1  runtime error (from a subcommand)
//	2  usage error
func runCLI(args []string) int {
	if len(args) < 1 {
		fmt.Fprint(os.Stderr, "relay: missing command\n\n"+usageText)
		return 2
	}

	switch args[0] {
	case "function":
		return runFunctionCommand(args[1:])
	case "health":
		return runHealthCommand()
	default:
		fmt.Fprintf(os.Stderr, "relay: unknown command %q\n\n%s", args[0], usageText)
		return 2
	}
}

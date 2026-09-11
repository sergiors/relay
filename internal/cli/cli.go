package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

func isHelp(arg string) bool {
	return arg == "--help" || arg == "-h"
}

// helpWriter renders a two-column command list aligned with tabwriter, using
// the same options as printList (minwidth 0, tab width 4, padding 2, space).
func helpWriter(rows [][2]string) string {
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	for _, r := range rows {
		fmt.Fprintf(w, "  %s\t%s\n", r[0], r[1])
	}
	_ = w.Flush()
	return b.String()
}

func rootHelp() string {
	return "Usage:\n  relay COMMAND\n\nCommands:\n" +
		helpWriter([][2]string{
			{"start", "Start Relay"},
			{"function", "Manage functions"},
			{"secret", "Manage local secrets"},
			{"stats", "Show current operational statistics"},
			{"health", "Check Relay dependencies"},
		}) +
		"\nRun 'relay COMMAND --help' for more information on a command.\n"
}

func functionHelp() string {
	return "Usage:\n  relay function COMMAND\n\nCommands:\n" +
		helpWriter([][2]string{
			{"ls", "List functions"},
			{"inspect", "Show detailed information about a function"},
		}) +
		"\nRun 'relay function COMMAND --help' for more information on a command.\n"
}

func functionLsUsage() string {
	return "Usage:\n  relay function ls\n\nList functions.\n"
}

func functionInspectUsage() string {
	return "Usage:\n  relay function inspect NAME\n\nShow detailed information about a function.\n"
}

func healthUsage() string {
	return "Usage:\n  relay health\n\nCheck Relay dependencies.\n"
}

// printUsageError writes an Error line followed by a usage block to stderr and
// returns the usage exit code.
func printUsageError(msg, usage string) int {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	fmt.Fprint(os.Stderr, usage)
	return 2
}

// Run dispatches the CLI's subcommands. It is separated from main so tests
// can exercise the exit codes directly. The CLI parses commands and owns exit
// codes; `relay start` is the one command that starts the long-running Relay
// runtime (delegating to internal/worker), while every other command is a
// read-only administrative/inspection command that never starts the runtime
// and never touches Redis or Docker. It does manage local secret files under
// the secrets directory (see `relay secret`). Exit codes:
//
//	0  success
//	1  runtime error (from a subcommand)
//	2  usage error
func Run(args []string) int {
	if len(args) < 1 {
		return printUsageError("missing command", rootHelp())
	}

	switch args[0] {
	case "start":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, startUsage())
			return 0
		}
		if len(args) != 1 {
			return printUsageError("start: too many arguments", startUsage())
		}
		// Delegation, not implementation: the CLI only parses commands; the
		// long-running Relay lifecycle lives in internal/worker.
		return runStartCommand()
	case "function":
		return runFunctionCommand(args[1:])
	case "health":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, healthUsage())
			return 0
		}
		if len(args) != 1 {
			return printUsageError("health: too many arguments", healthUsage())
		}
		return runHealthCommand()
	case "secret":
		return runSecretCommand(args[1:])
	case "stats":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, statsUsage())
			return 0
		}
		if len(args) != 1 {
			return printUsageError("stats: too many arguments", statsUsage())
		}
		return runStatsCommand()
	case "--help", "-h":
		if len(args) == 1 {
			fmt.Fprint(os.Stdout, rootHelp())
			return 0
		}
		return printUsageError(fmt.Sprintf("unknown command %q", args[0]), rootHelp())
	default:
		return printUsageError(fmt.Sprintf("unknown command %q", args[0]), rootHelp())
	}
}

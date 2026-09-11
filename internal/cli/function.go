package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/state"
)

// statePath is the local state database location the CLI reads. It defaults to
// the fixed internal path; tests replace it with a temp file so they never
// touch /var/lib/relay.
var statePath = state.DBPath

// functionCommand builds the read-only `relay function ...` subcommand family.
// It touches the local state database only — never Redis, Docker, or the
// /functions loader — so it works with no REDIS_ADDR and no worker reachable.
func functionCommand() *cli.Command {
	return &cli.Command{
		Name:  "function",
		Usage: "Manage functions",
		Description: "List and inspect the functions Relay has discovered and " +
			"reconciled, reading the local state database.",
		// Unknown or missing subcommands are usage errors; a non-nil Action here
		// keeps an unknown token from falling through to the built-in help.
		Action: func(ctx context.Context, cmd *cli.Command) error {
			switch {
			case !cmd.Args().Present():
				return cli.Exit("function: missing subcommand", 2)
			default:
				return cli.Exit(fmt.Sprintf("function: unknown subcommand %q", cmd.Args().First()), 2)
			}
		},
		Commands: []*cli.Command{
			{
				Name:        "ls",
				Usage:       "List functions",
				Description: "List all functions Relay has discovered, sorted by name.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("function ls: too many arguments", 2)
					}
					return functionList(ctx, cmd.Writer)
				},
			},
			{
				Name:      "inspect",
				Usage:     "Show detailed information about a function",
				UsageText: "relay function inspect NAME",
				Description: "Show the full detail record for a single function, including " +
					"its runtime, status, handlers, and env/secret mappings.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("function inspect: too many arguments", 2)
					}
					return functionInspect(ctx, cmd.Writer, cmd.StringArgs("name")[0])
				},
			},
		},
	}
}

// openState opens the local state DB (creating it if absent) and returns it
// with a cleanup func. Any failure is returned as an error for the root
// ExitErrHandler to print once.
func openState() (*state.State, func(), error) {
	st, err := state.Open(statePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open state: %w", err)
	}
	st.SetLogger(log.New(os.Stderr, "", 0))
	return st, func() { _ = st.Close() }, nil
}

// functionList prints a Docker-like table of functions sorted by name.
func functionList(ctx context.Context, w io.Writer) error {
	st, cleanup, err := openState()
	if err != nil {
		return err
	}
	defer cleanup()
	return printList(w, st)
}

// functionInspect prints the full detail for one function. An unknown name
// returns a runtime error (exit 1 via main).
func functionInspect(ctx context.Context, w io.Writer, name string) error {
	st, cleanup, err := openState()
	if err != nil {
		return err
	}
	defer cleanup()

	d, ok := st.GetFunction(name)
	if !ok {
		return fmt.Errorf("unknown function %q", name)
	}
	printInspect(w, st, d)
	return nil
}

// printList renders the ls table to w. It is separated from the command
// plumbing so tests can invoke it against a temp state DB directly.
func printList(w io.Writer, st *state.State) error {
	rows := st.ListFunctions()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tRUNTIME\tSTATUS\tHANDLERS\tUPDATED")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
			r.Name, r.Runtime, r.Status, r.HandlerCount, displayTime(r))
	}
	return tw.Flush()
}

// printInspect renders the full detail record to w. Labels are tab-aligned
// through a tabwriter so padding matches the longest label without hand-
// maintained spaces. The Handlers section is rendered with the same alignment,
// using a wider padding for visual grouping.
func printInspect(w io.Writer, st *state.State, d state.Detail) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Name:\t%s\n", d.Name)
	fmt.Fprintf(tw, "Runtime:\t%s\n", d.Runtime)
	fmt.Fprintf(tw, "Status:\t%s\n", d.Status)
	if d.Image != "" {
		fmt.Fprintf(tw, "Image:\t%s\n", d.Image)
	}
	if d.Fingerprint != "" {
		fmt.Fprintf(tw, "Fingerprint:\t%s\n", d.Fingerprint)
	}
	if d.PreparedAt != "" {
		fmt.Fprintf(tw, "Prepared:\t%s (%s)\n", d.PreparedAt, state.RelativeAgo(d.PreparedAt))
	}
	if d.LastReconcileAt != "" {
		fmt.Fprintf(tw, "Last reconcile:\t%s (%s)\n", d.LastReconcileStatus, state.RelativeAgo(d.LastReconcileAt))
	}
	if d.LastError != "" {
		fmt.Fprintf(tw, "Last error:\t%s\n", d.LastError)
	}
	tw.Flush()

	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Stats:")
	sw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
	fs, _ := st.FunctionStats(d.Name)
	fmt.Fprintf(sw, "  Events processed:\t%d\n", fs.EventsProcessedTotal)
	fmt.Fprintf(sw, "  Handler successes:\t%d\n", fs.HandlerSuccessTotal)
	fmt.Fprintf(sw, "  Handler failures:\t%d\n", fs.HandlerFailureTotal)
	fmt.Fprintf(sw, "  Retries:\t%d\n", fs.RetryTotal)
	fmt.Fprintf(sw, "  DLQ entries:\t%d\n", fs.DLQTotal)
	sw.Flush()

	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Handlers:")
	hw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
	for _, h := range d.Handlers {
		fmt.Fprintf(hw, "  %s\ttimeout=%s\n", h.Name, h.Timeout)
	}
	hw.Flush()

	// Env and secrets sections render the template's MAPPINGS only: literal env
	// values (not secret) and secret references (never values). Both are
	// omitted when the template defines none. Keys are sorted.
	if len(d.Env) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Environment:")
		ew := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, k := range sortedKeys(d.Env) {
			fmt.Fprintf(ew, "  %s=%s\n", k, d.Env[k])
		}
		ew.Flush()
	}
	if len(d.Secrets) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Secrets:")
		sw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, k := range sortedKeys(d.Secrets) {
			fmt.Fprintf(sw, "  %s=%s\n", k, d.Secrets[k])
		}
		sw.Flush()
	}
}

// sortedKeys returns the map's keys sorted, for deterministic inspect output.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// displayTime returns the UPDATED column value: prepared_at (if set) else
// updated_at, rendered as a short relative age.
func displayTime(r state.Row) string {
	raw := r.PreparedAt
	if raw == "" {
		raw = r.UpdatedAt
	}
	return state.RelativeAgo(raw)
}

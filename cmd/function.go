package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"text/tabwriter"

	"relay/internal/state"
)

// statePath is the local state database location the CLI reads. It defaults to
// the fixed internal path; tests replace it with a temp file so they never
// touch /var/lib/relay.
var statePath = state.DBPath

// runFunctionCommand implements the read-only `relay function ...` subcommand
// family. It touches the local state database only — never Redis, Docker, or
// the /functions loader — so it works with no REDIS_ADDR and no daemon
// reachable. Exit codes:
//
//	0  success
//	1  runtime error (e.g. unknown function)
//	2  usage error
func runFunctionCommand(args []string) int {
	if len(args) == 0 {
		return functionUsage("function: missing subcommand")
	}

	switch args[0] {
	case "ls":
		if len(args) != 1 {
			return functionUsage("function ls: too many arguments")
		}
		return functionList()
	case "inspect":
		if len(args) != 2 {
			return functionUsage("function inspect: expected a function name")
		}
		return functionInspect(args[1])
	default:
		return functionUsage(fmt.Sprintf("function: unknown subcommand %q", args[0]))
	}
}

// functionUsage prints a usage line to stderr and returns the usage exit code.
func functionUsage(msg string) int {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	fmt.Fprintln(os.Stderr, "Usage: relay function <ls|inspect <name>>")
	return 2
}

// openState opens the local state DB (creating it if absent) and returns it
// with a cleanup func. Any failure is reported on stderr with exit 1.
func openState() (*state.State, func(), int) {
	st, err := state.Open(statePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: open state: %v\n", err)
		return nil, nil, 1
	}
	st.SetLogger(log.New(os.Stderr, "", 0))
	return st, func() { _ = st.Close() }, 0
}

// functionList prints a Docker-like table of functions sorted by name.
func functionList() int {
	st, cleanup, code := openState()
	if code != 0 {
		return code
	}
	defer cleanup()

	if err := printList(st); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

// functionInspect prints the full detail for one function. An unknown name
// reports to stderr and returns 1.
func functionInspect(name string) int {
	st, cleanup, code := openState()
	if code != 0 {
		return code
	}
	defer cleanup()

	d, ok := st.GetFunction(name)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: unknown function %q\n", name)
		return 1
	}
	printInspect(st, d)
	return 0
}

// printList renders the ls table to stdout. It is separated from the command
// plumbing so tests can invoke it against a temp state DB directly.
func printList(st *state.State) error {
	rows := st.ListFunctions()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tRUNTIME\tSTATUS\tHANDLERS\tUPDATED")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n",
			r.Name, r.Runtime, r.Status, r.HandlerCount, displayTime(r))
	}
	return w.Flush()
}

// printInspect renders the full detail record to stdout. Labels are
// tab-aligned through a tabwriter so padding matches the longest label
// without hand-maintained spaces. The Handlers section is rendered with
// the same alignment, using a wider padding for visual grouping.
func printInspect(st *state.State, d state.Detail) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Name:\t%s\n", d.Name)
	fmt.Fprintf(w, "Runtime:\t%s\n", d.Runtime)
	fmt.Fprintf(w, "Status:\t%s\n", d.Status)
	if d.Image != "" {
		fmt.Fprintf(w, "Image:\t%s\n", d.Image)
	}
	if d.Fingerprint != "" {
		fmt.Fprintf(w, "Fingerprint:\t%s\n", d.Fingerprint)
	}
	if d.PreparedAt != "" {
		fmt.Fprintf(w, "Prepared:\t%s (%s)\n", d.PreparedAt, state.RelativeAgo(d.PreparedAt))
	}
	if d.LastReconcileAt != "" {
		fmt.Fprintf(w, "Last reconcile:\t%s (%s)\n", d.LastReconcileStatus, state.RelativeAgo(d.LastReconcileAt))
	}
	if d.LastError != "" {
		fmt.Fprintf(w, "Last error:\t%s\n", d.LastError)
	}
	w.Flush()

	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, "Handlers:")
	hw := tabwriter.NewWriter(os.Stdout, 0, 4, 3, ' ', 0)
	for _, h := range d.Handlers {
		fmt.Fprintf(hw, "  %s\ttimeout=%s\n", h.Name, h.Timeout)
	}
	hw.Flush()
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

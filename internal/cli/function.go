package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/worker"
)

// poolSnapshotProvider is the optional IN-PROCESS seam for the live warm-
// container pool view rendered by `relay function inspect`. It defaults to nil.
// An in-process host (embedding the worker, an admin path, or a test) wires the
// provider via SetPoolSnapshotProvider and avoids a socket round trip. The
// standalone inspect process leaves it nil and falls back to the worker query
// socket (Dependencies.SocketPath); when neither resolves a live pool, the live
// gauges render as explicitly unavailable rather than from stale persisted
// state. The provider supplies ONLY the live gauges; the cumulative
// acquire/discard counters always come from the persisted per-function stats
// row, so the section is useful without the worker.
var poolSnapshotProvider func(name string) (runtime.PoolSnapshot, bool)

// SetPoolSnapshotProvider installs the live warm-container pool provider used by
// `relay function inspect`. Passing nil (the default) leaves inspect to consult
// the worker query socket, then to render persisted cumulative counters with the
// live gauges marked unavailable. It is safe to call before any command runs;
// the worker does not call it today (inspect is a distinct process), but an
// embedding host may.
func SetPoolSnapshotProvider(fn func(name string) (runtime.PoolSnapshot, bool)) {
	poolSnapshotProvider = fn
}

// functionCommand builds the `relay function ...` subcommand family. `ls` and
// `inspect` touch the local state database only — never Redis, Docker, or the
// /functions loader — so they work with no REDIS_URI and no worker reachable;
// `invoke` instead dials the RUNNING worker's query socket to execute handlers
// on its live runtime pool (it has no offline fallback). deps supplies the state
// database those commands open and the worker socket inspect/invoke dial. It is
// a pure grouping command, so it uses the shared namespaceAction: a bare
// `relay function` shows the subcommand help, and an unknown first token is a
// friendly Docker-style usage error naming the full path.
func functionCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "function",
		Usage: "Manage functions",
		Description: "List, inspect, and invoke the functions Relay has discovered " +
			"and reconciled. list/inspect read the local state database; invoke runs " +
			"a function's matching handlers on the running worker.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			{
				Name:        "ls",
				Usage:       "List functions",
				Description: "List all functions Relay has discovered, sorted by name.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("function ls: too many arguments", 2)
					}
					return functionList(ctx, cmd.Writer, deps.StatePath)
				},
			},
			{
				Name:      "inspect",
				Usage:     "Show detailed information about a function",
				UsageText: "relay function inspect NAME",
				Description: "Show the full detail record for a single function, including " +
					"its runtime, status, events and schedules, env/secret mappings, and " +
					"its cumulative container pool counters.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("function inspect: too many arguments", 2)
					}
					return functionInspect(ctx, cmd.Writer, cmd.StringArgs("name")[0], deps)
				},
			},
			functionInvokeCommand(deps),
		},
	}
}

// openState opens the local state DB at path (creating it if absent) and
// returns it with a cleanup func. Any failure is returned as an error for the
// root ExitErrHandler to print once.
func openState(path string) (*state.State, func(), error) {
	st, err := state.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open state: %w", err)
	}
	st.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return st, func() { _ = st.Close() }, nil
}

// functionList prints a Docker-like table of functions sorted by name from the
// state database at statePath.
func functionList(ctx context.Context, w io.Writer, statePath string) error {
	st, cleanup, err := openState(statePath)
	if err != nil {
		return err
	}
	defer cleanup()
	return printList(w, st)
}

// functionInspect prints the full detail for one function, reading the state
// database at deps.StatePath and resolving the live pool gauges (when the
// in-process provider is absent) from the worker socket at deps.SocketPath. An
// unknown name returns a runtime error (exit 1 via main).
func functionInspect(ctx context.Context, w io.Writer, name string, deps Dependencies) error {
	st, cleanup, err := openState(deps.StatePath)
	if err != nil {
		return err
	}
	defer cleanup()

	detail, ok := st.GetFunction(name)
	if !ok {
		return fmt.Errorf("unknown function %q", name)
	}
	fs := printInspect(w, st, detail)
	appendRuntimePool(w, name, fs, deps.SocketPath)
	return nil
}

// printList renders the ls table to w. It is separated from the command
// plumbing so tests can invoke it against a temp state DB directly.
func printList(w io.Writer, st *state.State) error {
	rows := st.ListFunctions()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tRUNTIME\tSTATUS\tUPDATED")

	for _, r := range rows {
		runtime := r.Runtime
		if runtime == "" {
			runtime = "-"
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			r.Name, runtime, r.Status, displayTime(r))
	}

	return tw.Flush()
}

// printInspect renders the full detail record to w and returns the per-function
// stats it read, so the caller can render the Runtime pool section from the same
// persisted snapshot without a second read. Labels are tab-aligned through a
// tabwriter so padding matches the longest label without hand-maintained spaces.
// The Events, Schedules, and Services sections are rendered with the same
// alignment, using a wider padding for visual grouping.
func printInspect(w io.Writer, st *state.State, detail state.Detail) state.FunctionStats {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "Name:\t%s\n", detail.Name)

	runtime := detail.Runtime
	if runtime == "" {
		runtime = "-"
	}
	fmt.Fprintf(tw, "Runtime:\t%s\n", runtime)

	fmt.Fprintf(tw, "Status:\t%s\n", detail.Status)
	if detail.Image != "" {
		fmt.Fprintf(tw, "Image:\t%s\n", detail.Image)
	}
	if detail.Fingerprint != "" {
		fmt.Fprintf(tw, "Fingerprint:\t%s\n", detail.Fingerprint)
	}
	if detail.PreparedAt != "" {
		fmt.Fprintf(tw, "Prepared:\t%s (%s)\n", detail.PreparedAt, state.RelativeAgo(detail.PreparedAt))
	}
	if detail.LastReconcileAt != "" {
		fmt.Fprintf(tw, "Last reconcile:\t%s (%s)\n", detail.LastReconcileStatus, state.RelativeAgo(detail.LastReconcileAt))
	}
	if detail.LastError != "" {
		fmt.Fprintf(tw, "Last error:\t%s\n", detail.LastError)
	}
	tw.Flush()

	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Stats:")
	sw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
	fs, _ := st.FunctionStats(detail.Name)
	fmt.Fprintf(sw, "  Events matched:\t%d\n", fs.EventsMatchedTotal)
	fmt.Fprintf(sw, "  Handler successes:\t%d\n", fs.HandlerSuccessTotal)
	fmt.Fprintf(sw, "  Handler failures:\t%d\n", fs.HandlerFailureTotal)
	fmt.Fprintf(sw, "  Retries:\t%d\n", fs.RetryTotal)
	fmt.Fprintf(sw, "  DLQ entries:\t%d\n", fs.DLQTotal)
	fmt.Fprintf(sw, "  Last execution:\t%s\n", lastAgo(fs.LastExecutionAt))
	fmt.Fprintf(sw, "  Last success:\t%s\n", lastAgo(fs.LastSuccessAt))
	fmt.Fprintf(sw, "  Last failure:\t%s\n", lastAgo(fs.LastFailureAt))
	fmt.Fprintf(sw, "  Last DLQ:\t%s\n", lastAgo(fs.LastDLQAt))
	sw.Flush()

	if len(detail.Handlers) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Events:")
		hw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, h := range detail.Handlers {
			fmt.Fprintf(hw, "  %s\ttimeout=%s\n", h.Name, h.Timeout)
		}
		hw.Flush()
	}

	if len(detail.Schedules) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Schedules:")
		sw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, s := range detail.Schedules {
			// Cron descriptions are best-effort; the raw expression remains authoritative.
			if desc, ok := describeCronFunc(s.Cron); ok {
				fmt.Fprintf(
					sw,
					"  %s\tcron=%q (%s) timezone=%s timeout=%s\n",
					s.Handler,
					s.Cron,
					desc,
					s.Timezone,
					s.Timeout,
				)
				continue
			}
			fmt.Fprintf(
				sw,
				"  %s\tcron=%q timezone=%s timeout=%s\n",
				s.Handler,
				s.Cron,
				s.Timezone,
				s.Timeout,
			)
		}
		sw.Flush()
	}

	if len(detail.Services) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Services:")
		srw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, svc := range detail.Services {
			// Prefix non-entrypoint sources so their kind remains visible in CLI output.
			source := svc.Entrypoint
			switch {
			case svc.Build != "":
				source = "build:" + svc.Build
			case svc.Image != "":
				source = "image:" + svc.Image
			}

			if svc.Path != "" {
				fmt.Fprintf(
					srw,
					"  %s\tport=%d replicas=%d path=%s\n",
					source,
					svc.Port,
					svc.Replicas,
					svc.Path,
				)
				continue
			}
			fmt.Fprintf(srw, "  %s\tport=%d replicas=%d\n", source, svc.Port, svc.Replicas)
		}
		srw.Flush()
	}

	if len(detail.Env) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Environment:")
		ew := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, k := range sortedKeys(detail.Env) {
			fmt.Fprintf(ew, "  %s=%s\n", k, detail.Env[k])
		}
		ew.Flush()
	}

	if len(detail.Secrets) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Secrets:")
		sw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		for _, k := range sortedKeys(detail.Secrets) {
			// Show secret references only; resolved values must never be exposed.
			fmt.Fprintf(sw, "  %s=%s\n", k, detail.Secrets[k])
		}
		sw.Flush()
	}

	if len(detail.Networks) > 0 {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Networks:")
		nw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
		// The list is normalized (trimmed, deduped, sorted) at parse time, so it
		// renders deterministically.
		for _, n := range detail.Networks {
			fmt.Fprintf(nw, "  %s\n", n)
		}
		nw.Flush()
	}

	return fs
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

// poolGauges is the live runtime-pool gauge view rendered by inspect, resolved
// from either the in-process provider (runtime.PoolSnapshot) or the worker query
// socket (worker.RuntimeState). Keeping a CLI-local shape lets both sources
// render identically without importing the worker's wire type into the renderer.
// It carries ONLY the live gauges: the cumulative counters are not a gauge and
// always come from the persisted stats row.
type poolGauges struct {
	Capacity   int
	Containers int
	Busy       int
	Idle       int
	Starting   int
}

// appendRuntimePool appends the Runtime pool section to an inspect output for
// name. The cumulative acquire/discard counters (warm acquires, cold starts,
// discarded) ALWAYS come from the persisted function_stats row (fs) — never from
// a live provider or the socket, so the operator-facing history is the same in
// every process and survives a worker restart. Only the LIVE gauges (capacity,
// container counts by lease state) are resolved from a live source, in
// preference order:
//
//  1. an in-process provider, when wired (an embedding host or test);
//  2. the worker query socket at socketPath (the standalone CLI);
//  3. otherwise explicitly unavailable ("unknown").
//
// The live gauges are NEVER rendered from persisted state: a persisted live
// gauge would go stale between flushes, so "unknown" is always the honest
// fallback. name is passed separately because fs is the zero value when no stats
// row exists yet.
func appendRuntimePool(w io.Writer, name string, fs state.FunctionStats, socketPath string) {
	gauges, ok := livePoolGauges(name, socketPath)
	if !ok {
		printRuntimePool(w, nil, fs.WarmAcquiresTotal, fs.ColdStartsTotal, fs.DiscardedTotal)
		return
	}
	printRuntimePool(w, &gauges, fs.WarmAcquiresTotal, fs.ColdStartsTotal, fs.DiscardedTotal)
}

// livePoolGauges resolves name's live pool gauges from the in-process provider
// first, then the worker query socket at socketPath. It reports (zero, false)
// when neither source resolves a live pool, so the caller renders the gauges
// unknown. The cumulative counters are deliberately NOT read here: they always
// come from the persisted stats row.
func livePoolGauges(name, socketPath string) (poolGauges, bool) {
	if snap, ok := livePoolSnapshot(name); ok {
		return poolGauges{
			Capacity:   snap.Capacity,
			Containers: snap.Containers,
			Busy:       snap.Busy,
			Idle:       snap.Idle,
			Starting:   snap.Starting,
		}, true
	}
	return socketPoolGauges(name, socketPath)
}

// livePoolSnapshot consults the optional in-process provider. It returns
// (zero, false) when no provider is wired (the standalone inspect process) or
// the provider does not report a live pool for the function.
func livePoolSnapshot(name string) (runtime.PoolSnapshot, bool) {
	if poolSnapshotProvider == nil {
		return runtime.PoolSnapshot{}, false
	}
	return poolSnapshotProvider(name)
}

// socketPoolGauges queries the live worker socket at socketPath for name's pool
// gauges. Any failure — no worker running, a stale/unreachable socket, or the
// worker having no pool for the function — reports (zero, false) so the caller
// renders the live gauges unknown. It never fails the command: live
// observability is best-effort, and the persisted counters remain authoritative
// cumulative history.
func socketPoolGauges(name, socketPath string) (poolGauges, bool) {
	st, err := worker.QueryRuntimeState(socketPath, name)
	if err != nil {
		return poolGauges{}, false
	}
	return poolGauges{
		Capacity:   st.Capacity,
		Containers: st.Containers,
		Busy:       st.Busy,
		Idle:       st.Idle,
		Starting:   st.Starting,
	}, true
}

// printRuntimePool renders the compact Runtime pool section for one function.
// warm/cold/discarded are the cumulative counters to render; live carries the
// live gauges when they are known. It has two shapes:
//
//   - live != nil: the authoritative gauges (capacity, container counts by
//     lease state, and the transient starting reservation) are shown. Known
//     zero values print as 0 (a valid live reading), and Starting is shown only
//     when non-zero (it is transient and usually zero).
//   - live == nil: no live pool is reachable from this process, so the live
//     gauges render as explicitly unavailable ("unknown") — never a guessed or
//     stale number.
//
// The layout matches the wider padding of the Services section.
func printRuntimePool(w io.Writer, live *poolGauges, warm, cold, discarded int64) {
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Runtime pool:")
	tw := tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)

	totalAcquires := warm + cold
	warmAcquires := fmt.Sprintf("%d (n/a)", warm)
	if totalAcquires > 0 {
		reuseRate := float64(warm) / float64(totalAcquires) * 100
		warmAcquires = fmt.Sprintf("%d (%.1f%%)", warm, reuseRate)
	}

	if live != nil {
		fmt.Fprintf(tw, "  Capacity:\t%d\n", live.Capacity)
		fmt.Fprintf(tw, "  Containers:\t%d\n", live.Containers)
		fmt.Fprintf(tw, "  Busy:\t%d\n", live.Busy)
		fmt.Fprintf(tw, "  Idle:\t%d\n", live.Idle)
		if live.Starting > 0 {
			fmt.Fprintf(tw, "  Starting:\t%d\n", live.Starting)
		}
		fmt.Fprintf(tw, "  Warm acquires:\t%s\n", warmAcquires)
		fmt.Fprintf(tw, "  Cold starts:\t%d\n", cold)
		fmt.Fprintf(tw, "  Discarded:\t%d\n", discarded)
		tw.Flush()
		return
	}
	// No live pool: the gauges are unavailable (the worker's in-memory pool is
	// not reachable from this process). "unknown" is explicit rather than a
	// stale number so an operator never mistakes a persisted value for a live
	// reading.
	fmt.Fprintf(tw, "  Capacity:\tunknown\n")
	fmt.Fprintf(tw, "  Containers:\tunknown\n")
	fmt.Fprintf(tw, "  Busy:\tunknown\n")
	fmt.Fprintf(tw, "  Idle:\tunknown\n")
	fmt.Fprintf(tw, "  Warm acquires:\t%s\n", warmAcquires)
	fmt.Fprintf(tw, "  Cold starts:\t%d\n", cold)
	fmt.Fprintf(tw, "  Discarded:\t%d\n", discarded)
	tw.Flush()
}

// lastAgo renders a per-function timestamp for inspect: the empty string means
// the event was never observed ("never"); any other value renders through
// state.RelativeAgo, which falls back to the raw string when parsing fails — an
// acceptable cosmetic fallback that keeps inspect rendering non-fatal.
func lastAgo(rfc3339 string) string {
	if rfc3339 == "" {
		return "never"
	}
	return state.RelativeAgo(rfc3339)
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

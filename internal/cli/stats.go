package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v3"

	"relay/internal/state"
	"relay/internal/worker"
)

// statsCommand builds the `relay stats ...` subcommand family. It reads and
// resets the operational snapshot in the local state database at
// deps.StatePath; a reset with a running worker goes through the worker's Unix
// socket so its in-memory totals are reset too, but the read path never needs
// Redis, Docker, or the worker, so `relay stats` works with no REDIS_URI set. A
// missing or unreadable stats row renders a zero snapshot rather than failing,
// so an empty state database always produces sensible output with exit 0.
//
// It is a hybrid grouping command: bare `relay stats` keeps rendering the table
// through the parent Action (urfave runs it when no subcommand token resolves),
// and an unknown first token still fails through that Action's too-many-
// arguments guard, matching the previous leaf command. `reset` is the only
// subcommand.
func statsCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "stats",
		Usage: "Show current operational statistics",
		Description: "Show or reset the persisted operational statistics in the local " +
			"state database.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats: too many arguments", 2)
			}
			st, cleanup, err := openState(deps.StatePath)
			if err != nil {
				return err
			}
			defer cleanup()
			printStats(cmd.Writer, st)
			return nil
		},
		Commands: []*cli.Command{statsResetCommand(deps)},
	}
}

// statsResetCommand builds `relay stats reset`: with a running worker it asks
// the worker over its Unix socket (worker.ResetRuntimeStats) to reset Relay's
// accumulated statistics, so the worker's in-memory snapshot source and
// persisted stats both continue from zero and a captured pre-reset snapshot
// cannot be written after the reset (the worker performs both under its flush
// mutex). When no worker answers, it falls back to state.ResetStats, which
// rewrites the persisted global counters and every per-function stats row to
// zero in one transaction. It deliberately does not touch pending
// events/backlog, Redis, containers/runtime pools/schedules/services, or the
// worker's Prometheus counters (monotonic for the process lifetime).
func statsResetCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "reset",
		Usage:       "Reset persisted cumulative statistics",
		UsageText:   "relay stats reset",
		Description: "Reset cumulative global and per-function statistics.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats reset: too many arguments", 2)
			}
			// A running worker owns the in-memory source of the persisted
			// totals, so reset through its socket first; only when no worker
			// answers do we reset the state database directly (the stopped-worker
			// path).
			if err := worker.ResetRuntimeStats(deps.SocketPath); err == nil {
				fmt.Fprintln(cmd.Writer, "Stats reset")
				return nil
			} else if !errors.Is(err, worker.ErrRuntimeStatsUnavailable) {
				return fmt.Errorf("stats reset: %w", err)
			}
			st, cleanup, err := openState(deps.StatePath)
			if err != nil {
				return err
			}
			defer cleanup()
			if err := st.ResetStats(); err != nil {
				return fmt.Errorf("stats reset: %w", err)
			}
			fmt.Fprintln(cmd.Writer, "Stats reset")
			return nil
		},
	}
}

// humanAge renders an integer number of seconds as a Go duration string
// ("2m14s"). Zero seconds renders as "0s", which reads naturally for a fresh
// or empty snapshot.
func humanAge(seconds int64) string {
	return (time.Duration(seconds) * time.Second).String()
}

// printStats renders the operational snapshot to w. The zero value of the
// Stats struct is used when no row exists yet (absent or read error), which
// keeps the output predictable on a fresh state database.
func printStats(w io.Writer, st *state.State) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	s, _ := st.Stats()
	updated := "never"
	if s.UpdatedAt != "" {
		updated = state.RelativeAgo(s.UpdatedAt)
	}

	fmt.Fprintf(tw, "Events processed:\t%d\n", s.EventsProcessedTotal)
	fmt.Fprintf(tw, "Handler successes:\t%d\n", s.HandlerSuccessTotal)
	fmt.Fprintf(tw, "Handler failures:\t%d\n", s.HandlerFailureTotal)
	fmt.Fprintf(tw, "Retries:\t%d\n", s.RetryTotal)
	fmt.Fprintf(tw, "DLQ entries:\t%d\n", s.DLQTotal)
	fmt.Fprintf(tw, "Pending entries:\t%d\n", s.PendingEntries)
	fmt.Fprintf(tw, "Oldest pending age:\t%s\n", humanAge(s.OldestPendingAgeSeconds))
	fmt.Fprintf(tw, "Updated:\t%s\n", updated)
	_ = tw.Flush()
}

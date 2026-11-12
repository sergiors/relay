package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/urfave/cli/v3"

	"relay/internal/state"
	"relay/internal/worker"
)

// statsCommand builds the `relay stats ...` subcommand family. It reads and
// resets the operational snapshot in the local state database at
// deps.OpenState; a reset with a running worker goes through the worker's Unix
// socket so its in-memory totals are reset too, but the read path never needs
// Redis, Docker, or the worker, so `relay stats` works with no REDIS_URI set. An
// absent stats row renders a zero snapshot; an unreadable one is an error, never
// a fabricated zero, so an empty state database still exits 0 but a real
// failure surfaces.
//
// It is a hybrid grouping command: bare `relay stats` keeps rendering the table
// through the parent Action (urfave runs it when no subcommand token resolves),
// and an unknown first token still fails through that Action's too-many-
// arguments guard, matching the previous leaf command. `reset` is the only
// subcommand.
func statsCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "stats",
		Usage: "Show operational statistics",
		Description: "Show the latest persisted counters and backlog gauges; the snapshot may " +
			"lag live metrics by a few seconds. Requires PostgreSQL and works " +
			"without a running worker.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats: too many arguments", 2)
			}
			st, cleanup, err := deps.OpenState(ctx)
			if err != nil {
				return err
			}
			defer cleanup()

			// An absent stats row renders zeros; only a failed read is an
			// error, so a fresh database still exits 0.
			s, _, err := st.ReadStats(ctx)
			if err != nil {
				return fmt.Errorf("stats: %w", err)
			}
			return renderStats(cmd.Writer, s)
		},
		Commands: []*cli.Command{statsResetCommand(deps)},
	}
}

// statsResetCommand builds `relay stats reset`: with a running worker it asks
// the worker over its Unix socket (worker.ResetRuntimeStats) to reset Relay's
// accumulated statistics, so the worker's in-memory snapshot source and
// persisted stats both continue from zero and a captured pre-reset snapshot
// cannot be written after the reset (the worker performs both under its flush
// mutex). Only when the socket POSITIVELY reports no worker (the socket file is
// gone or refusing connections) does it fall back to state.ResetStats, which
// rewrites the persisted global counters and every per-app stats row to
// zero in one transaction. An ambiguous outcome — a timeout, a failed exchange,
// or a worker that answered but could not reset — is surfaced, never silently
// masked by a direct write: a live worker may have received the reset (or reset
// concurrently), and the live worker stays the authority. It deliberately does
// not touch pending events/backlog, Redis, containers/runtime
// pools/schedules/services, or the worker's Prometheus counters (monotonic for
// the process lifetime).
func statsResetCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "reset",
		Usage:     "Reset cumulative statistics",
		UsageText: "relay stats reset",
		Description: "Reset cumulative global and per-app counters. Backlog gauges and " +
			"Prometheus metrics are not reset. Resets through the running worker when " +
			"reachable; with no worker running it resets the persisted counters directly. A " +
			"reachable worker is authoritative, so its errors are reported instead of falling " +
			"back.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats reset: too many arguments", 2)
			}
			// A running worker owns the in-memory source of the persisted
			// totals, so reset through its socket first. Only a positively
			// absent worker (no socket / connection refused) permits the direct
			// state-DB write; every ambiguous outcome is surfaced so a live
			// worker is never second-guessed.
			err := worker.ResetRuntimeStats(deps.SocketPath)
			switch {
			case err == nil:
				out := newOutput(cmd.Writer)
				out.line("Stats reset")
				return out.err
			case !errors.Is(err, worker.ErrRuntimeStatsNoWorker):
				return fmt.Errorf("stats reset: %w", err)
			}

			st, cleanup, err := deps.OpenState(ctx)
			if err != nil {
				return err
			}
			defer cleanup()
			if _, err := st.ResetStats(); err != nil {
				return fmt.Errorf("stats reset: %w", err)
			}
			out := newOutput(cmd.Writer)
			out.line("Stats reset")
			return out.err
		},
	}
}

// humanAge renders an integer number of seconds as a Go duration string
// ("2m14s"). Zero seconds renders as "0s", which reads naturally for a fresh
// or empty snapshot.
func humanAge(seconds int64) string {
	return (time.Duration(seconds) * time.Second).String()
}

// renderStats renders the operational snapshot. The zero Stats value is a
// valid rendering for a state database with no snapshot yet.
func renderStats(w io.Writer, stats state.Stats) error {
	out := newOutput(w)
	tw := out.table(2)

	updated := "never"
	if stats.UpdatedAt != "" {
		updated = relativeAgo(stats.UpdatedAt)
	}

	fmt.Fprintf(tw, "Events received:\t%d\n", stats.EventsReceivedTotal)
	fmt.Fprintf(tw, "Events matched:\t%d\n", stats.EventsMatchedTotal)
	fmt.Fprintf(tw, "Events unmatched:\t%d\n", stats.EventsUnmatchedTotal)
	fmt.Fprintf(tw, "Handler successes:\t%d\n", stats.HandlerSuccessTotal)
	fmt.Fprintf(tw, "Handler failures:\t%d\n", stats.HandlerFailureTotal)
	fmt.Fprintf(tw, "Message reclaims:\t%d\n", stats.RetryTotal)
	fmt.Fprintf(tw, "DLQ entries:\t%d\n", stats.DLQTotal)
	fmt.Fprintf(tw, "Pending entries:\t%d\n", stats.PendingEntries)
	fmt.Fprintf(tw, "Oldest pending age:\t%s\n", humanAge(stats.OldestPendingAgeSeconds))
	fmt.Fprintf(tw, "Updated:\t%s\n", updated)
	out.flush(tw)
	return out.err
}

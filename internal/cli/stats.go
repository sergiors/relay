package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/urfave/cli/v3"

	"relay/internal/config"
	"relay/internal/state"
	"relay/internal/worker"
)

// statsCommand builds the `relay stats ...` subcommand family. It reads and
// resets the operational snapshot in the local state database at
// deps.OpenState. Bare `relay stats` reads NODE_ID's own row; --node reads one
// named row and --all aggregates every node. A reset with a running worker goes
// through the worker's Unix socket so its in-memory totals are reset too, but
// the read path never needs Redis, Docker, or the worker, so `relay stats` works
// with no REDIS_URI set. An absent stats row renders a zero snapshot; an
// unreadable one is an error, never a fabricated zero, so an empty state
// database still exits 0 but a real failure surfaces.
//
// It is a hybrid grouping command: bare `relay stats` keeps rendering the table
// through the parent Action (urfave runs it when no subcommand token resolves),
// and an unknown first token still fails through that Action's too-many-
// arguments guard, matching the previous leaf command. `reset` is the only
// subcommand.
func statsCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "stats",
		Usage:     "Show operational statistics",
		UsageText: "relay stats [--node <id>|--all]",
		Description: "Show persisted counters and backlog gauges for NODE_ID by default, " +
			"or select a node with --node or aggregate every node with --all; the snapshot " +
			"may lag live metrics by a few seconds.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "node",
				Usage: "Read only the named node's statistics",
			},
			&cli.BoolFlag{
				Name:  "all",
				Usage: "Aggregate statistics across all nodes",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats: too many arguments", 2)
			}
			nodeID, all, err := statsSelector(cmd)
			if err != nil {
				return err
			}
			st, cleanup, err := deps.OpenState(ctx)
			if err != nil {
				return err
			}
			defer cleanup()

			// An absent stats row renders zeros; only a failed read is an
			// error, so a fresh database still exits 0.
			var s state.Stats
			if all {
				s, _, err = st.ReadStats(ctx)
			} else {
				s, _, err = st.ReadNodeStats(ctx, nodeID)
			}
			if err != nil {
				return fmt.Errorf("stats: %w", err)
			}
			return renderStats(cmd.Writer, s)
		},
		Commands: []*cli.Command{statsResetCommand(deps)},
	}
}

// statsSelector resolves the mutually exclusive node selectors for the read
// command. With neither flag it returns NODE_ID, validated exactly as the worker
// validates it; --node validates the named identity; --all aggregates. Combining
// --node and --all is a usage error rather than silently preferring one.
func statsSelector(cmd *cli.Command) (nodeID string, all bool, err error) {
	all = cmd.Bool("all")
	named := cmd.String("node")
	if all && named != "" {
		return "", false, cli.Exit("stats: --node and --all are mutually exclusive", 2)
	}
	if all {
		return "", true, nil
	}
	raw := named
	if raw == "" {
		raw = os.Getenv("NODE_ID")
	}
	nodeID, parseErr := config.ParseNodeID(raw)
	if parseErr != nil {
		if named != "" {
			return "", false, fmt.Errorf("stats: --node: %w", parseErr)
		}
		return "", false, fmt.Errorf("stats: %w", parseErr)
	}
	return nodeID, false, nil
}

// statsResetCommand builds `relay stats reset`: with a running worker it asks
// the worker over its Unix socket to reset Relay's accumulated statistics, so
// the worker's in-memory snapshot source and persisted stats both continue from
// zero and a captured pre-reset snapshot cannot be written after the reset (the
// worker performs both under its flush mutex). A local reset targets NODE_ID;
// --all uses the global reset command so every node's rows are zeroed. Only when
// the socket POSITIVELY reports no worker (the socket file is gone or refusing
// connections) does it fall back to state.ResetNodeStats or state.ResetAllStats,
// which rewrite the target rows to zero in one transaction. An ambiguous
// outcome — a timeout, a failed exchange, or a worker that answered but could
// not reset — is surfaced, never silently masked by a direct write: a live
// worker may have received the reset (or reset concurrently), and the live
// worker stays the authority. It deliberately does not touch pending
// events/backlog, Redis, containers/runtime pools/schedules/services, or the
// worker's Prometheus counters (monotonic for the process lifetime).
func statsResetCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "reset",
		Usage:     "Reset cumulative statistics",
		UsageText: "relay stats reset [--all]",
		Description: "Reset NODE_ID's cumulative global and per-app counters, or every " +
			"node's with --all. Backlog gauges and Prometheus metrics are not reset. " +
			"Resets through the running worker when reachable; with no worker running it " +
			"resets the persisted counters directly. A reachable worker is authoritative, so " +
			"its errors are reported instead of falling back.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "all",
				Usage: "Reset every node's cumulative statistics",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats reset: too many arguments", 2)
			}
			all := cmd.Bool("all")
			nodeID := ""
			if !all {
				var err error
				nodeID, err = localStatsNodeID()
				if err != nil {
					return fmt.Errorf("stats reset: %w", err)
				}
			}
			// A running worker owns the in-memory source of the persisted
			// totals, so reset through its socket first. The local command
			// carries the CLI-selected NODE_ID as the expected target; the
			// worker rejects a mismatch before resetting. Only a positively
			// absent worker (no socket / connection refused) permits the direct
			// state-DB write; every ambiguous or worker-reported outcome is
			// surfaced so a live worker is never second-guessed.
			var err error
			if all {
				err = worker.ResetAllRuntimeStats(deps.SocketPath)
			} else {
				err = worker.ResetRuntimeStatsForNode(deps.SocketPath, nodeID)
			}
			switch {
			case err == nil:
				return writeStatsResetConfirmation(cmd.Writer, all, nodeID)
			case !errors.Is(err, worker.ErrRuntimeStatsNoWorker):
				return fmt.Errorf("stats reset: %w", err)
			}

			st, cleanup, err := deps.OpenState(ctx)
			if err != nil {
				return err
			}
			defer cleanup()
			if all {
				_, err = st.ResetAllStats(ctx)
			} else {
				_, err = st.ResetNodeStats(ctx, nodeID)
			}
			if err != nil {
				return fmt.Errorf("stats reset: %w", err)
			}
			return writeStatsResetConfirmation(cmd.Writer, all, nodeID)
		},
	}
}

// localStatsNodeID resolves and validates this process's NODE_ID for the
// node-scoped read and reset commands. There is no hostname or generated
// fallback, matching the worker's own identity requirement.
func localStatsNodeID() (string, error) {
	return config.ParseNodeID(os.Getenv("NODE_ID"))
}

// writeStatsResetConfirmation reports which scope the successful reset covered,
// naming the local node so an operator can tell a node-scoped reset from the
// cluster-wide one.
func writeStatsResetConfirmation(w io.Writer, all bool, nodeID string) error {
	out := newOutput(w)
	if all {
		out.line("Stats reset for all nodes")
	} else {
		out.line("Stats reset for node " + nodeID)
	}
	return out.err
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

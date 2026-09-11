package cli

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/urfave/cli/v3"

	"relay/internal/state"
)

// statsCommand builds the read-only `relay stats` subcommand. It reads the
// operational snapshot from the local state database and renders it to stdout.
// The command touches only the state database — never Redis, Docker, or the
// worker — so it works with no REDIS_ADDR set. A missing or unreadable stats
// row renders a zero snapshot rather than failing, so an empty state database
// always produces sensible output with exit 0.
func statsCommand() *cli.Command {
	return &cli.Command{
		Name:        "stats",
		Usage:       "Show current operational statistics",
		Description: "Show the current operational snapshot from the local state database.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("stats: too many arguments", 2)
			}
			st, cleanup, err := openState()
			if err != nil {
				return err
			}
			defer cleanup()
			printStats(cmd.Writer, st)
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

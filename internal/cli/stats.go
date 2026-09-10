package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"relay/internal/state"
)

func statsUsage() string {
	return "Usage:\n  relay stats\n\nShow current operational statistics.\n"
}

// humanAge renders an integer number of seconds as a Go duration string
// ("2m14s"). Zero seconds renders as "0s", which reads naturally for a fresh
// or empty snapshot.
func humanAge(seconds int64) string {
	return (time.Duration(seconds) * time.Second).String()
}

// runStatsCommand implements the read-only `relay stats` subcommand. It reads
// the operational snapshot from the local state database and renders it to
// stdout. The command touches only the state database — never Redis, Docker, or
// the worker — so it works with no REDIS_ADDR set. A missing or unreadable
// stats row renders a zero snapshot rather than failing, so an empty state
// database always produces sensible output with exit 0. Exit codes:
//
//	0  success
//	1  state database could not be opened
func runStatsCommand() int {
	st, cleanup, code := openState()
	if code != 0 {
		return code
	}
	defer cleanup()

	printStats(st)
	return 0
}

// printStats renders the operational snapshot to stdout. The zero value of the
// Stats struct is used when no row exists yet (absent or read error), which
// keeps the output predictable on a fresh state database.
func printStats(st *state.State) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)

	s, _ := st.Stats()
	updated := "never"
	if s.UpdatedAt != "" {
		updated = state.RelativeAgo(s.UpdatedAt)
	}

	fmt.Fprintf(w, "Events processed:\t%d\n", s.EventsProcessedTotal)
	fmt.Fprintf(w, "Handler successes:\t%d\n", s.HandlerSuccessTotal)
	fmt.Fprintf(w, "Handler failures:\t%d\n", s.HandlerFailureTotal)
	fmt.Fprintf(w, "Retries:\t%d\n", s.RetryTotal)
	fmt.Fprintf(w, "DLQ entries:\t%d\n", s.DLQTotal)
	fmt.Fprintf(w, "Pending entries:\t%d\n", s.PendingEntries)
	fmt.Fprintf(w, "Oldest pending age:\t%s\n", humanAge(s.OldestPendingAgeSeconds))
	fmt.Fprintf(w, "Updated:\t%s\n", updated)
	_ = w.Flush()
}

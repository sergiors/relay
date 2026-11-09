package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"relay/internal/dlq"
)

// renderDLQList renders the ls table in the store's write order, showing the
// full entry ID so it can be copied directly into inspect, replay, or rm.
func renderDLQList(w io.Writer, entries []dlq.Entry) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "ID\tAPP\tHANDLER\tATTEMPTS\tAGE")
	for _, e := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
			e.ID,
			e.App,
			e.Handler,
			e.HandlerAttempts,
			relativeAgo(e.Timestamp),
		)
	}
	out.flush(tw)
	return out.err
}

// renderDLQEntry renders one entry's operator-facing metadata plus the original
// diagnostic transport `value`, pretty-printed. The stored logical ReplayPayload
// is intentionally not displayed. The value is printed verbatim when it is not
// valid JSON (the malformed placeholder "-", for instance) rather than hiding
// it, so an operator sees exactly what was dead-lettered.
func renderDLQEntry(w io.Writer, entry dlq.Entry) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintf(tw, "ID:\t%s\n", entry.ID)
	fmt.Fprintf(tw, "Original stream:\t%s\n", entry.Source)
	fmt.Fprintf(tw, "Original ID:\t%s\n", entry.SourceMessageID)
	fmt.Fprintf(tw, "Group:\t%s\n", entry.SourceGroup)
	fmt.Fprintf(tw, "Consumer:\t%s\n", entry.Consumer)
	fmt.Fprintf(tw, "App:\t%s\n", entry.App)
	fmt.Fprintf(tw, "Handler:\t%s\n", entry.Handler)
	fmt.Fprintf(tw, "Handler attempts:\t%d\n", entry.HandlerAttempts)
	fmt.Fprintf(tw, "Deliveries:\t%d\n", entry.Deliveries)
	if entry.Trace != "" {
		fmt.Fprintf(tw, "Trace:\t%s\n", entry.Trace)
	}
	fmt.Fprintf(tw, "Timestamp:\t%s (%s)\n", entry.Timestamp, relativeAgo(entry.Timestamp))
	fmt.Fprintf(tw, "Reason:\t%s\n", entry.Reason)
	out.flush(tw)

	out.blank()
	out.line("Event:")
	out.line("%s", prettyEvent(entry.Event))
	return out.err
}

// prettyEvent renders the original transport value as indented JSON. A value
// that is not valid JSON (including the "-" placeholder for a malformed
// message) is rendered verbatim, so inspect never hides or mangles what was
// dead-lettered.
func prettyEvent(event string) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(event), "", "  "); err != nil {
		return event
	}
	return buf.String()
}

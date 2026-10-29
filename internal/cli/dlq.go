package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/dlq"
	"relay/internal/schedule"
	"relay/internal/worker"
)

// openPostgresDLQStore is the production OpenDLQStore: it opens the same
// PostgreSQL state handle the other administrative commands use. The DLQ is a
// durable table there, so `relay dlq` needs DATABASE_URL but never starts or
// contacts the worker: listing, inspection, removal, and replay read only the
// recorded app/handler/event, and replay uses the running worker's socket when
// one is available.
func openPostgresDLQStore(ctx context.Context, _ *slog.Logger) (dlq.Store, func(), error) {
	st, cleanup, err := openPostgresState(ctx)
	if err != nil {
		return nil, nil, err
	}
	return st, cleanup, nil
}

// dlqCommand builds the `relay dlq ...` subcommand family: `ls`, `inspect`,
// `replay`, and `rm` against the PostgreSQL DLQ. Entry IDs are stable logical
// identities (source delivery + invocation), so they remain valid across worker
// restarts and redeliveries. It is a pure grouping command, so it uses the shared
// namespaceAction: a bare `relay dlq` shows the subcommand help, and an unknown
// first token is a friendly Docker-style usage error naming the full path.
//
// PostgreSQL access stays behind the broker-neutral dlq.Store seam and the
// injected OpenDLQStore opener; the presentation helpers
// (dlqList/dlqInspect/dlqReplay/dlqRm) never construct clients or read
// configuration themselves, so they are testable in isolation.
func dlqCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "dlq",
		Usage: "Inspect and replay dead-lettered events",
		Description: "List, inspect, replay, and remove durable dead-letter entries. " +
			"Replay re-executes one entry's exact app and handler once on the " +
			"running worker's live runtime pool.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			dlqLsCommand(logger, deps),
			dlqInspectCommand(logger, deps),
			dlqReplayCommand(logger, deps),
			dlqRmCommand(logger, deps),
		},
	}
}

// dlqLsCommand builds `relay dlq ls`: a concise table of the DLQ entries in
// write order, one row per entry. The row shows the entry ID, the original
// message identity, the exact failed app/handler and its handler attempt count,
// and the entry's age.
func dlqLsCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "ls",
		Usage:       "List DLQ entries",
		Description: "List every DLQ entry in write order: entry ID, source message, failed app/handler, handler attempts, and age.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq ls: too many arguments", 2)
			}
			return withDLQStore(ctx, logger, deps, func(store dlq.Store) error {
				return dlqList(ctx, cmd.Writer, store)
			})
		},
	}
}

// dlqInspectCommand builds `relay dlq inspect ID`: the exact current fields of
// one entry, followed by the original event JSON pretty-printed. An unknown ID
// is a clear error.
func dlqInspectCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "inspect",
		Usage:       "Show one DLQ entry",
		UsageText:   "relay dlq inspect ID",
		Description: "Show a single DLQ entry's metadata and its original event JSON, pretty-printed.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "id", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq inspect: too many arguments", 2)
			}
			id := cmd.StringArgs("id")[0]
			return withDLQStore(ctx, logger, deps, func(store dlq.Store) error {
				return dlqInspect(ctx, cmd.Writer, store, id)
			})
		},
	}
}

// dlqReplayCommand builds `relay dlq replay ID`: it reads the entry's exact
// stored app, handler, and event, asks the running worker over its query
// socket to execute exactly that one handler once against the live runtime, and
// deletes only that DLQ entry after the execution succeeds. On any failure — a
// removed app/handler, a failed handler, or an unavailable worker — the
// entry is retained and a concise error is returned.
func dlqReplayCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "replay",
		Usage:     "Replay one DLQ entry",
		UsageText: "relay dlq replay ID",
		Description: "Re-execute one DLQ entry's exact app and handler once on the " +
			"running worker, then delete the entry on success. The entry is kept on failure.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "id", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq replay: too many arguments", 2)
			}
			id := cmd.StringArgs("id")[0]
			return withDLQStore(ctx, logger, deps, func(store dlq.Store) error {
				return dlqReplay(ctx, cmd.Writer, store, deps.SocketPath, id)
			})
		},
	}
}

// dlqRmCommand builds `relay dlq rm ID`: it deletes exactly one durable entry by
// its stable ID and leaves every other entry in place. It never touches broker
// invocation state and never causes execution; if the source delivery is still
// pending, a redelivery may recreate the row. An unknown ID is a clear error.
func dlqRmCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "rm",
		Usage:       "Remove one DLQ entry",
		UsageText:   "relay dlq rm ID",
		Description: "Delete exactly one DLQ entry by its stable ID.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "id", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq rm: too many arguments", 2)
			}
			id := cmd.StringArgs("id")[0]
			return withDLQStore(ctx, logger, deps, func(store dlq.Store) error {
				return dlqRm(ctx, cmd.Writer, store, id)
			})
		},
	}
}

// withDLQStore opens the DLQ store for one command, runs fn, and always closes
// the store. It keeps the client-open plumbing out of every command Action and
// out of the presentation helpers.
func withDLQStore(ctx context.Context, logger *slog.Logger, deps Dependencies, fn func(dlq.Store) error) error {
	if deps.OpenDLQ == nil {
		// No opener wired (an embedding host / test): fail clearly instead of
		// panicking.
		return fmt.Errorf("dlq: PostgreSQL is not configured")
	}
	store, cleanup, err := deps.OpenDLQ(ctx, logger)
	if err != nil {
		return fmt.Errorf("dlq: %w", err)
	}
	defer cleanup()
	if err := fn(store); err != nil {
		return fmt.Errorf("dlq: %w", err)
	}
	return nil
}

// dlqList renders the ls table. It is separated from the command plumbing so
// tests can invoke it against a fake store directly. Rows are rendered in the
// order the store returns them (write order), which is the natural DLQ order.
func dlqList(ctx context.Context, w io.Writer, store dlq.Store) error {
	entries, err := store.ListDLQEntries(ctx)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tORIGINAL\tAPP\tHANDLER\tATTEMPTS\tAGE")
	for _, e := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
			e.ID,
			originalRef(e),
			e.App,
			e.Handler,
			e.HandlerAttempts,
			relativeAgo(e.Timestamp),
		)
	}
	return tw.Flush()
}

// dlqInspect renders one entry's exact current fields, followed by the original
// event JSON pretty-printed. It is separated from the command plumbing so tests
// can invoke it against a fake store directly. An unknown ID returns a clear
// error. The event is printed verbatim when it is not valid JSON (the malformed
// placeholder "-", for instance) rather than hiding it, so an operator sees
// exactly what was dead-lettered.
func dlqInspect(ctx context.Context, w io.Writer, store dlq.Store, id string) error {
	entry, ok, err := store.GetDLQEntry(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown DLQ entry %q", id)
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
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
	_ = tw.Flush()

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Event:")
	fmt.Fprintln(w, prettyEvent(entry.Event))
	return nil
}

// dlqReplay admits, executes, and settles one fenced replay.
//
// Admission is a PostgreSQL claim committed BEFORE the worker is contacted: only
// a settled entry is eligible, a monotonic fencing token is incremented, and a
// lease in database time is stamped (dlq.ReplayLease). A concurrent replay sees
// the unexpired lease, gets an explicit replay-in-progress error, and never
// dials, so two CLIs cannot both execute the same entry. The claim never spans
// execution: no database transaction is held while the handler runs.
//
// The claim is released early only when the outcome is known to have left no
// running handler: a pre-dial/model error, a worker-reported failure (including
// handler failure, unknown app, or removed handler), or a request that never
// reached the worker. An ambiguous outcome — a lost/malformed response, a read
// or write failure after send, or client cancellation — retains the lease until
// it expires; this deliberately favors not starting a second execution over
// immediate retry. Expiry is the recovery path for a crashed claimant.
//
// On success the delete is one fenced conditional statement requiring the entry
// to still be settled, to still carry this claim's token, and to have an
// unexpired lease at database time. A stale claimant can therefore never delete a
// row that was re-claimed; it reports that the entry was retained.
//
// Replay is at-least-once, not exactly-once: a handler may finish after the CLI
// disconnects (the worker's replay context is not cancelled by the disconnect) or
// after the lease expires, so the same entry can be executed again. Handlers must
// stay idempotent.
//
// A non-replayable entry is rejected after admission and its claim released: a
// malformed-message or oversized-event placeholder has no app/handler to
// re-execute and an event that is not JSON, so replay would otherwise attempt to
// encode invalid JSON and report a misleading socket-unavailability error. The
// entry is kept for inspection or removal.
//
// The stored event is the source body, which for a schedule occurrence is its
// envelope rather than the payload the handler received. schedule.ReplayPayload
// maps it back to Occurrence.Payload() before dialing and leaves a valid
// ordinary event byte-for-byte unchanged; an invalid or non-object payload, or
// a malformed schedule claim, is an explicit pre-dial error and releases the
// claim.
func dlqReplay(ctx context.Context, w io.Writer, store dlq.Store, socketPath, id string) error {
	claim, err := store.ClaimDLQReplay(ctx, id, dlq.ReplayLease)
	if err != nil {
		switch {
		case errors.Is(err, dlq.ErrEntryNotFound):
			return fmt.Errorf("unknown DLQ entry %q", id)
		case errors.Is(err, dlq.ErrEntryUnsettled):
			// An unsettled entry's source delivery may still be recoverable.
			// Replaying it would execute a handler whose dead letter cannot yet
			// be removed, and the later delete would be refused after the side
			// effect had already happened.
			return fmt.Errorf("DLQ entry %q is not settled: source delivery may still be recoverable", id)
		case errors.Is(err, dlq.ErrReplayInProgress):
			return fmt.Errorf("DLQ entry %q replay is already in progress; retry after the claim lease expires", id)
		default:
			return fmt.Errorf("replay %s: %w", id, err)
		}
	}
	entry := claim.Entry
	if !entry.Replayable() {
		return releaseReplayClaim(ctx, store, id, claim.Token,
			fmt.Errorf("DLQ entry %q is not replayable: no app/handler to re-execute (malformed-message placeholder or oversized-event summary)", id))
	}

	// Replay the payload the handler actually received, not necessarily the
	// stored body. The stored envelope itself stays untouched for inspection.
	payload, err := schedule.ReplayPayload([]byte(entry.Event))
	if err != nil {
		return releaseReplayClaim(ctx, store, id, claim.Token,
			fmt.Errorf("replay %s: %w", id, err))
	}

	if err := worker.ReplayDLQ(ctx, socketPath, entry.App, entry.Handler, payload, entry.Trace); err != nil {
		replayErr := fmt.Errorf("replay %s: %w", id, err)
		// A definitive worker answer means no handler is still running. A
		// request that never started is equally safe to release. Everything
		// else (lost response, I/O failure after send, cancellation) may have
		// executed and retains the lease until expiry.
		if errors.Is(err, worker.ErrInvokeFailed) || errors.Is(err, worker.ErrInvokeNotStarted) {
			return releaseReplayClaim(ctx, store, id, claim.Token, replayErr)
		}
		return replayErr
	}

	// The handler succeeded: delete exactly this entry with the fenced claim.
	// A failure here means the replay DID run, so the operator must know the
	// entry lingers; re-running would repeat the execution.
	if err := store.DeleteDLQReplay(ctx, id, claim.Token); err != nil {
		return fmt.Errorf("replay %s: handler succeeded but the DLQ entry was retained: %w", id, err)
	}
	fmt.Fprintln(w, "Replayed handler successfully")
	return nil
}

// releaseReplayClaim releases a claim for an outcome known not to have left a
// running handler, preserving the replay error as the primary cause. A release
// failure is joined rather than hidden: the claim then stays until its lease
// expires, which is safe but delays the next attempt.
func releaseReplayClaim(ctx context.Context, store dlq.Store, id string, token int64, cause error) error {
	if err := store.ReleaseDLQReplay(ctx, id, token); err != nil {
		return errors.Join(cause, fmt.Errorf("release DLQ replay claim: %w", err))
	}
	return cause
}

// dlqRm deletes exactly one settled entry by its stable ID. An unknown ID is a
// clear error; an entry with an active replay claim is refused, because the
// replay may still be executing. A successful removal prints a concise
// confirmation.
func dlqRm(ctx context.Context, w io.Writer, store dlq.Store, id string) error {
	deleted, err := store.DeleteDLQEntry(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, dlq.ErrEntryUnsettled):
			return fmt.Errorf("DLQ entry %q is not settled: source delivery may still be recoverable", id)
		case errors.Is(err, dlq.ErrReplayInProgress):
			return fmt.Errorf("DLQ entry %q has an active replay claim; retry after it expires", id)
		default:
			return err
		}
	}
	if !deleted {
		return fmt.Errorf("unknown DLQ entry %q", id)
	}
	fmt.Fprintln(w, "DLQ entry removed")
	return nil
}

// originalRef renders the entry's source-message identity for the concise ls
// row: "source/id".
func originalRef(e dlq.Entry) string {
	return e.Source + "/" + e.SourceMessageID
}

// prettyEvent renders the original event as indented JSON. A payload that is not
// valid JSON (including the "-" placeholder for a malformed message) is rendered
// verbatim, so inspect never hides or mangles what was dead-lettered.
func prettyEvent(event string) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(event), "", "  "); err != nil {
		return event
	}
	return buf.String()
}

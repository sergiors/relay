package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/dlq"
	"relay/internal/worker"
)

const (
	// dlqIDLength is the full stable entry ID length: SHA-256 rendered as
	// lowercase hexadecimal by dlq.NewID.
	dlqIDLength = 64
	// dlqIDShortLength is how many leading ID characters `dlq ls` shows and
	// the minimum prefix length accepted by inspect, replay, and rm, so an
	// operator can copy the visible ID directly.
	dlqIDShortLength = 10
)

// dlqIDPrefixFinder is the narrow administrative capability of a DLQ store to
// resolve a short entry-ID prefix. It is deliberately separate from dlq.Store:
// the stream processor never needs prefix lookup, so keeping it narrow avoids
// forcing every store implementation and fake to grow an admin method.
type dlqIDPrefixFinder interface {
	// FindDLQEntryIDsByPrefix returns up to two candidate entry IDs from an
	// index range starting at prefix, in ascending ID order. The range is
	// evaluated under the database collation, so callers must verify that each
	// candidate byte-starts with prefix before acting on it.
	FindDLQEntryIDsByPrefix(ctx context.Context, prefix string) ([]string, error)
}

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
// ls prints only the first dlqIDShortLength characters of each ID; the admin
// commands accept that visible prefix (or any longer unique prefix) as well as
// the full ID.
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
// write order, one row per entry. The row shows the first dlqIDShortLength
// characters of the entry ID, the exact failed app/handler and its handler
// attempt count, and the entry's age; the short ID can be passed directly to
// inspect, replay, or rm as a prefix.
func dlqLsCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "ls",
		Usage: "List DLQ entries",
		Description: "List every DLQ entry in write order: short entry ID, " +
			"failed app/handler, handler attempts, and age.",
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

// dlqInspectCommand builds `relay dlq inspect ID|PREFIX`: the exact current
// fields of one entry, followed by the original transport `value` pretty-printed.
// A unique prefix of at least dlqIDShortLength characters is accepted. An
// unknown or ambiguous ID is a clear error.
func dlqInspectCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "inspect",
		Usage:     "Show one DLQ entry",
		UsageText: "relay dlq inspect ID|PREFIX",
		Description: "Show a single DLQ entry's metadata and its original " +
			"transport value, pretty-printed. Accepts the full ID or a unique " +
			"prefix of at least 10 characters.",
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

// dlqReplayCommand builds `relay dlq replay ID|PREFIX`: it reads the entry's
// exact stored app, handler, and replay payload, asks the running worker over
// its query socket to execute exactly that one handler once against the live
// runtime, and deletes only that DLQ entry after the execution succeeds. A
// unique prefix of at least dlqIDShortLength characters is accepted. On any
// failure — a removed app/handler, a failed handler, or an unavailable worker —
// the entry is retained and a concise error is returned.
func dlqReplayCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "replay",
		Usage:     "Replay one DLQ entry",
		UsageText: "relay dlq replay ID|PREFIX",
		Description: "Re-execute one DLQ entry's exact app and handler once on " +
			"the running worker, then delete the entry on success. The entry is " +
			"kept on failure. Accepts the full ID or a unique prefix of at " +
			"least 10 characters.",
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

// dlqRmCommand builds `relay dlq rm ID|PREFIX`: it deletes exactly one durable
// entry by its stable ID and leaves every other entry in place. A unique prefix
// of at least dlqIDShortLength characters is accepted. It never touches broker
// invocation state and never causes execution; if the source delivery is still
// pending, a redelivery may recreate the row. An unknown or ambiguous ID is a
// clear error.
func dlqRmCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "rm",
		Usage:     "Remove one DLQ entry",
		UsageText: "relay dlq rm ID|PREFIX",
		Description: "Delete exactly one DLQ entry by its stable ID. " +
			"Accepts the full ID or a unique prefix of at least 10 characters.",
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
// Only the short ID prefix is shown; inspect still renders the full ID.
func dlqList(ctx context.Context, w io.Writer, store dlq.Store) error {
	entries, err := store.ListDLQEntries(ctx)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tAPP\tHANDLER\tATTEMPTS\tAGE")
	for _, e := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n",
			shortDLQID(e.ID),
			e.App,
			e.Handler,
			e.HandlerAttempts,
			relativeAgo(e.Timestamp),
		)
	}
	return tw.Flush()
}

// dlqInspect renders one entry's operator-facing metadata plus the original
// diagnostic transport `value`, pretty-printed. The stored logical
// ReplayPayload is intentionally not displayed. It is separated from the command
// plumbing so tests can invoke it against a fake store directly. The supplied ID
// may be a unique prefix; it is resolved before the exact-ID lookup. An unknown
// ID returns a clear error. The value is printed verbatim when it is not valid
// JSON (the malformed placeholder "-", for instance) rather than hiding it, so
// an operator sees exactly what was dead-lettered.
func dlqInspect(ctx context.Context, w io.Writer, store dlq.Store, id string) error {
	id, err := resolveDLQID(ctx, store, id)
	if err != nil {
		return err
	}
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
// re-execute. The entry is kept for inspection or removal.
//
// The handler is handed the entry's stored ReplayPayload, the exact logical
// payload produced by the same envelope decoder as live delivery (raw JSON,
// normalized Debezium event, or Occurrence.Payload for a schedule). There is no
// fallback to the diagnostic Event body: an entry with no stored payload is an
// explicit pre-dial error and releases the claim.
func dlqReplay(ctx context.Context, w io.Writer, store dlq.Store, socketPath, id string) error {
	id, err := resolveDLQID(ctx, store, id)
	if err != nil {
		return err
	}
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
	// Replay exactly the logical handler payload stored when the entry was
	// dead-lettered. It already reflects the envelope decoder's schedule
	// payload and Debezium normalization, so the diagnostic Event body is never
	// re-derived here. An entry without a stored payload must be refused before
	// dialing the worker.
	if len(entry.ReplayPayload) == 0 {
		return releaseReplayClaim(ctx, store, id, claim.Token,
			fmt.Errorf("DLQ entry %q is not replayable: no stored handler payload", id))
	}
	payload := entry.ReplayPayload

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

// dlqRm deletes exactly one settled entry by its stable ID. The supplied ID may
// be a unique prefix; it is resolved before the exact-ID delete. An unknown ID
// is a clear error; an entry with an active replay claim is refused, because
// the replay may still be executing. A successful removal prints a concise
// confirmation.
func dlqRm(ctx context.Context, w io.Writer, store dlq.Store, id string) error {
	id, err := resolveDLQID(ctx, store, id)
	if err != nil {
		return err
	}
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

// resolveDLQID validates a user-supplied entry identifier and returns the full
// stable ID used by the exact-ID store operations. A full-length ID is used
// as-is; a shorter prefix must match exactly one stored entry. Candidates are
// byte-verified before use, so a collation-widened range can never resolve to a
// non-matching entry. No match, ambiguity, and malformed input are reported
// before any replay or removal is attempted.
func resolveDLQID(ctx context.Context, store dlq.Store, input string) (string, error) {
	if err := validateDLQID(input); err != nil {
		return "", err
	}
	if len(input) == dlqIDLength {
		return input, nil
	}

	finder, ok := store.(dlqIDPrefixFinder)
	if !ok {
		return "", fmt.Errorf("DLQ entry prefix lookup is not supported by this store")
	}
	ids, err := finder.FindDLQEntryIDsByPrefix(ctx, input)
	if err != nil {
		return "", err
	}

	// The store's range is evaluated under the database collation. Verify every
	// candidate byte-starts with the requested prefix before it can reach an
	// exact-ID operation. If any candidate fails, the two-row limit may also
	// have hidden a real match behind it, so refuse the whole result rather than
	// guess which entry the operator meant. No-match is safer than a wrong
	// deletion or replay.
	matches := make([]string, 0, len(ids))
	for _, id := range ids {
		if strings.HasPrefix(id, input) {
			matches = append(matches, id)
		}
	}
	if len(matches) != len(ids) {
		return "", fmt.Errorf(
			"no DLQ entry safely matches ID prefix %q; use the full 64-character ID",
			input)
	}

	switch len(matches) {
	case 0:
		return "", fmt.Errorf("no DLQ entry matches ID prefix %q", input)
	case 1:
		return matches[0], nil
	default:
		return "", fmt.Errorf(
			"ambiguous DLQ entry ID prefix %q: matches %s and %s; use more characters",
			input, matches[0], matches[1])
	}
}

// validateDLQID enforces the entry-identifier grammar: 10 to 64 lowercase
// hexadecimal characters. The minimum is the prefix length shown by `dlq ls`,
// and only the canonical lowercase form is accepted so the ID is literal and
// unambiguous.
func validateDLQID(id string) error {
	if len(id) < dlqIDShortLength || len(id) > dlqIDLength {
		return fmt.Errorf(
			"invalid DLQ entry ID %q: use %d to %d lowercase hexadecimal "+
				"characters or a unique prefix",
			id, dlqIDShortLength, dlqIDLength)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf(
				"invalid DLQ entry ID %q: use only lowercase hexadecimal characters (0-9, a-f)",
				id)
		}
	}
	return nil
}

// shortDLQID returns the visible ls prefix. Stored IDs are full-length, but a
// guard keeps a malformed short fixture from panicking the listing.
func shortDLQID(id string) string {
	if len(id) <= dlqIDShortLength {
		return id
	}
	return id[:dlqIDShortLength]
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

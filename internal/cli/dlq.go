package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/urfave/cli/v3"

	"relay/internal/dlq"
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
// ls prints only the first dlqIDShortLength characters of each ID; the admin
// commands accept that visible prefix (or any longer unique prefix) as well as
// the full ID.
//
// PostgreSQL access stays behind the broker-neutral dlq.Store seam and the
// injected OpenDLQStore opener; the command helpers retrieve entries and pass
// plain data to the renderers, so presentation never sees a store handle.
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
				entries, err := store.ListDLQEntries(ctx)
				if err != nil {
					return err
				}
				return renderDLQList(cmd.Writer, entries)
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
				entry, err := findDLQEntry(ctx, store, id)
				if err != nil {
					return err
				}
				return renderDLQEntry(cmd.Writer, entry)
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
				return replayDLQEntry(ctx, cmd.Writer, store, deps.SocketPath, id)
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
				return removeDLQEntry(ctx, cmd.Writer, store, id)
			})
		},
	}
}

// withDLQStore opens the DLQ store for one command, runs fn, and always closes
// the store. It keeps the client-open plumbing out of every command Action and
// out of the entry/render helpers.
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

// removeDLQEntry deletes exactly one settled entry by its stable ID. The
// supplied ID may be a unique prefix; it is resolved before the exact-ID
// delete. An unknown ID is a clear error; an entry with an active replay claim
// is refused, because the replay may still be executing.
func removeDLQEntry(ctx context.Context, w io.Writer, store dlq.Store, id string) error {
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
	out := newOutput(w)
	out.line("DLQ entry removed")
	return out.err
}

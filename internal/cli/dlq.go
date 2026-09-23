package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/config"
	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/worker"
)

// openRedisDLQStore is the production OpenDLQStore: it loads the process
// configuration through the same config.Load + config.RedisOptions conventions
// `relay health` and `relay start` use, then builds a Redis-backed store over
// the DLQ stream derived from the configured source stream. config.Load is the
// fail-fast executable boundary (a missing required REDIS_* variable exits),
// matching the worker's startup behavior.
func openRedisDLQStore(logger *slog.Logger) (DLQStore, func(), error) {
	cfg := config.Load(logger)
	opts, err := config.RedisOptions(cfg.RedisURI)
	if err != nil {
		return nil, nil, fmt.Errorf("redis config: %w", err)
	}
	store := stream.NewRedisDLQStore(opts, cfg.RedisStream)
	return store, func() { _ = store.Close() }, nil
}

// dlqCommand builds the `relay dlq ...` subcommand family: `ls`, `inspect`,
// `replay`, and `rm` against the Relay-owned DLQ stream. The DLQ stream name is
// derived from the configured source stream (stream.DLQStreamFor), the same
// convention the consumer uses when it dead-letters. It is a pure grouping
// command, so it uses the shared namespaceAction: a bare `relay dlq` shows the
// subcommand help, and an unknown first token is a friendly Docker-style usage
// error naming the full path.
//
// Redis access stays behind the DLQStore seam and the injected OpenDLQStore
// opener; the presentation helpers (dlqList/dlqInspect/dlqReplay/dlqRm) never
// construct clients or read configuration themselves, so they are testable in
// isolation.
func dlqCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "dlq",
		Usage: "Inspect and replay dead-lettered events",
		Description: "List, inspect, replay, and remove entries from the Relay DLQ " +
			"stream (relay:<stream>:dlq). Replay re-executes one entry's exact " +
			"function and handler once on the running worker's live runtime pool.",
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
// Redis stream order (ascending entry ID), one row per entry. The row shows the
// entry ID, the original message identity, the exact failed function/handler and
// its handler attempt count, and the entry's age.
func dlqLsCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "ls",
		Usage:       "List DLQ entries",
		Description: "List every DLQ entry in stream order: entry ID, source message, failed function/handler, handler attempts, and age.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq ls: too many arguments", 2)
			}
			return withDLQStore(logger, deps, func(store DLQStore) error {
				return dlqList(ctx, cmd.Writer, store)
			})
		},
	}
}

// dlqInspectCommand builds `relay dlq inspect ID`: the exact current fields of
// one entry, followed by the original event JSON pretty-printed. ID is the DLQ
// stream entry ID. An unknown ID is a clear error.
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
			return withDLQStore(logger, deps, func(store DLQStore) error {
				return dlqInspect(ctx, cmd.Writer, store, id)
			})
		},
	}
}

// dlqReplayCommand builds `relay dlq replay ID`: it reads the entry's exact
// stored function, handler, and event, asks the running worker over its query
// socket to execute exactly that one handler once against the live runtime, and
// deletes only that DLQ entry after the execution succeeds. On any failure — a
// removed function/handler, a failed handler, or an unavailable worker — the
// entry is retained and a concise error is returned.
func dlqReplayCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "replay",
		Usage:     "Replay one DLQ entry",
		UsageText: "relay dlq replay ID",
		Description: "Re-execute one DLQ entry's exact function and handler once on the " +
			"running worker, then delete the entry on success. The entry is kept on failure.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "id", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq replay: too many arguments", 2)
			}
			id := cmd.StringArgs("id")[0]
			return withDLQStore(logger, deps, func(store DLQStore) error {
				return dlqReplay(ctx, cmd.Writer, store, deps.SocketPath, id)
			})
		},
	}
}

// dlqRmCommand builds `relay dlq rm ID`: it deletes exactly one DLQ entry by its
// stream entry ID (XDEL with a single ID) and leaves every other entry in place.
// An unknown ID is a clear error.
func dlqRmCommand(logger *slog.Logger, deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "rm",
		Usage:       "Remove one DLQ entry",
		UsageText:   "relay dlq rm ID",
		Description: "Delete exactly one DLQ entry by its stream entry ID.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "id", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("dlq rm: too many arguments", 2)
			}
			id := cmd.StringArgs("id")[0]
			return withDLQStore(logger, deps, func(store DLQStore) error {
				return dlqRm(ctx, cmd.Writer, store, id)
			})
		},
	}
}

// withDLQStore opens the DLQ store for one command, runs fn, and always closes
// the store. It keeps the client-open plumbing out of every command Action and
// out of the presentation helpers.
func withDLQStore(logger *slog.Logger, deps Dependencies, fn func(DLQStore) error) error {
	if deps.OpenDLQ == nil {
		// No opener wired (an embedding host / test with no Redis): fail
		// clearly instead of panicking.
		return fmt.Errorf("dlq: redis is not configured")
	}
	store, cleanup, err := deps.OpenDLQ(logger)
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
// order the store returns them (Redis stream order), which is the natural DLQ
// order.
func dlqList(ctx context.Context, w io.Writer, store DLQStore) error {
	entries, err := store.List(ctx)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tORIGINAL\tFUNCTION\tHANDLER\tATTEMPTS\tAGE")
	for _, e := range entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n",
			e.ID,
			originalRef(e),
			e.Function,
			e.Handler,
			e.HandlerAttempts,
			state.RelativeAgo(e.Timestamp),
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
func dlqInspect(ctx context.Context, w io.Writer, store DLQStore, id string) error {
	entry, ok, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown DLQ entry %q", id)
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "ID:\t%s\n", entry.ID)
	fmt.Fprintf(tw, "Original stream:\t%s\n", entry.OriginalStream)
	fmt.Fprintf(tw, "Original ID:\t%s\n", entry.OriginalID)
	fmt.Fprintf(tw, "Group:\t%s\n", entry.Group)
	fmt.Fprintf(tw, "Consumer:\t%s\n", entry.Consumer)
	fmt.Fprintf(tw, "Function:\t%s\n", entry.Function)
	fmt.Fprintf(tw, "Handler:\t%s\n", entry.Handler)
	fmt.Fprintf(tw, "Handler attempts:\t%d\n", entry.HandlerAttempts)
	fmt.Fprintf(tw, "Deliveries:\t%d\n", entry.Deliveries)
	fmt.Fprintf(tw, "Timestamp:\t%s (%s)\n", entry.Timestamp, state.RelativeAgo(entry.Timestamp))
	fmt.Fprintf(tw, "Reason:\t%s\n", entry.Reason)
	_ = tw.Flush()

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Event:")
	fmt.Fprintln(w, prettyEvent(entry.Event))
	return nil
}

// dlqReplay reads one entry, replays its exact stored function/handler/event
// through the running worker socket, and — only when the execution succeeds —
// deletes exactly that entry. Any failure (unknown ID, a non-replayable entry,
// an unavailable worker, a removed function/handler, or a failed handler)
// returns a concise error and leaves the entry in place. It never consults event
// matching or writes broker state: the worker's ReplayDLQ executes exactly the
// recorded handler once.
//
// A non-replayable entry is rejected before dialing: a malformed-message
// placeholder has no function/handler to re-execute and an event that is not
// JSON, so replay would otherwise attempt to encode invalid JSON and report a
// misleading socket-unavailability error. The entry is kept for inspection or
// removal.
func dlqReplay(ctx context.Context, w io.Writer, store DLQStore, socketPath, id string) error {
	entry, ok, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unknown DLQ entry %q", id)
	}
	if !entry.Replayable() {
		return fmt.Errorf("DLQ entry %q is not replayable: no function/handler to re-execute (malformed-message placeholder)", id)
	}

	if err := worker.ReplayDLQ(ctx, socketPath, entry.Function, entry.Handler, []byte(entry.Event)); err != nil {
		return fmt.Errorf("replay %s: %w", id, err)
	}

	// The handler succeeded: delete exactly this entry. A delete failure is
	// surfaced (the replay DID run, so the operator must know the entry lingers;
	// re-running would repeat the execution).
	if _, err := store.Delete(ctx, id); err != nil {
		return err
	}
	fmt.Fprintln(w, "Replayed handler successfully")
	return nil
}

// dlqRm deletes exactly one entry by its stream entry ID. An unknown ID is a
// clear error; a successful removal prints a concise confirmation.
func dlqRm(ctx context.Context, w io.Writer, store DLQStore, id string) error {
	deleted, err := store.Delete(ctx, id)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("unknown DLQ entry %q", id)
	}
	fmt.Fprintln(w, "DLQ entry removed")
	return nil
}

// originalRef renders the entry's source-message identity for the concise ls
// row: "stream/id".
func originalRef(e stream.DLQEntry) string {
	return e.OriginalStream + "/" + e.OriginalID
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

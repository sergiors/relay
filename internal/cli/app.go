package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/urfave/cli/v3"
)

// appCommand builds the `relay app ...` subcommand family. `ls` touches only
// the local state database; `inspect` reads the same state and additionally
// performs one best-effort, read-only Docker image inspect when the app has an
// active image, to show that image's provenance. Neither needs Redis or a
// reachable worker; `invoke` instead dials the RUNNING worker's query socket to
// execute handlers on its live runtime pool (it has no offline fallback). deps
// supplies the state database those commands open and the worker socket
// inspect/invoke dial. It is a pure grouping command, so it uses the shared
// namespaceAction: a bare `relay app` shows the subcommand help, and an unknown
// first token is a friendly Docker-style usage error naming the full path.
func appCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "app",
		Usage: "Manage apps",
		Description: "List, inspect, and invoke the apps Relay has discovered " +
			"and reconciled. ls/inspect read the local state database; inspect " +
			"also reads the active image's provenance from Docker when available; " +
			"invoke runs an app's matching handlers on the running worker.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			{
				Name:        "ls",
				Usage:       "List apps",
				Description: "List all apps Relay has discovered, sorted by name.",
				Action:      appListAction(deps),
			},
			{
				Name:      "inspect",
				Usage:     "Show detailed information about an app",
				UsageText: "relay app inspect NAME",
				Description: "Show the full detail record for a single app, including " +
					"its runtime, base-image provenance read from the active image, " +
					"status, events and schedules, env-var names (values " +
					"redacted) and secret reference names, and " +
					"its cumulative container pool counters.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("app inspect: too many arguments", 2)
					}
					return appInspect(ctx, cmd.Writer, cmd.StringArgs("name")[0], deps)
				},
			},
			appInvokeCommand(deps),
		},
	}
}

// appsCommand builds the top-level `relay apps` short form of `relay app ls`.
// It is a thin alias with NO subcommands: it shares appListAction with the
// canonical `relay app ls`, so listing, sorting, and argument rejection are
// identical. Every positional token — including `inspect` or `invoke`, which are
// subcommands of the `relay app` group — is a usage error here rather than a
// namespace dispatch, so the app grouping surface stays under `relay app`.
// `relay app ls` remains the canonical form.
func appsCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "apps",
		Usage: "List apps (short form of 'relay app ls')",
		Description: "Short form of `relay app ls`: list all apps Relay has " +
			"discovered, sorted by name, from the local state database. It takes no " +
			"arguments; use `relay app inspect` or `relay app invoke` for the other " +
			"app operations.",
		Action: appListAction(deps),
	}
}

// appListAction is the Action shared verbatim by the canonical `relay app ls`
// and the top-level `relay apps` short form, so the two entry points can never
// drift in listing, sorting, or argument handling. It rejects ANY positional
// argument as a usage error — including a stray `inspect` or `invoke`, which
// must not resolve as a namespace at the alias — and otherwise renders the table
// through deps.OpenState. The error text names the canonical
// command, because `app ls` is the form the alias mirrors.
func appListAction(deps Dependencies) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		if cmd.Args().Present() {
			return cli.Exit("app ls: too many arguments", 2)
		}
		return appList(ctx, cmd.Writer, deps)
	}
}

// appList reads the app summaries and renders the ls table. The read is kept
// separate from renderAppList so a failed query is never presented as an empty
// listing.
func appList(ctx context.Context, w io.Writer, deps Dependencies) error {
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	rows, err := st.ReadAppSummaries(ctx)
	if err != nil {
		return err
	}
	return renderAppList(w, rows)
}

// appInspect reads one app's detail and persisted stats, resolves the active
// image's diagnostic provenance, and renders the full record. An unknown name
// is a runtime error (exit 1 via main); an unreadable row is a separate error,
// never reported as an unknown app. The image provenance is best-effort and
// read-only: Docker is consulted only for an active image, and a failed lookup
// renders as unavailable without failing the command. The live gauges are
// likewise best-effort telemetry: an unavailable pool renders as "unknown"
// without failing the command.
func appInspect(ctx context.Context, w io.Writer, name string, deps Dependencies) error {
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	detail, found, err := st.ReadApp(ctx, name)
	if err != nil {
		return fmt.Errorf("app inspect: %w", err)
	}
	if !found {
		return fmt.Errorf("unknown app %q", name)
	}
	stats, _, err := st.ReadAppStats(ctx, name)
	if err != nil {
		return fmt.Errorf("app inspect: %w", err)
	}

	var provenance appProvenance
	if detail.Image != "" {
		provenance = lookupAppProvenance(ctx, detail.Image, deps)
	}
	if err := renderAppDetailWithProvenance(
		w, detail, stats, provenance,
	); err != nil {
		return err
	}

	gauges, live := resolvePoolGauges(name, deps.PoolSnapshotProvider, deps.SocketPath)
	var liveGauges *poolGauges
	if live {
		liveGauges = &gauges
	}
	return renderRuntimePool(w, stats, liveGauges)
}

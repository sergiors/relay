package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"relay/internal/git"
	"relay/internal/state"
)

// deployCommand builds the `relay deploy` command family. Rollback selects a
// commit from the configured Git source; pin and unpin control whether future
// Git updates can change the revision.
func deployCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:        "deploy",
		Usage:       "Manage coordinated deployments",
		Description: "Manage Git deployments across nodes.",
		Action:      namespaceAction(),
		Commands: []*cli.Command{
			deployRollbackCommand(deps),
			deployPinCommand(deps),
			deployUnpinCommand(deps),
			deployStatusCommand(deps),
		},
	}
}

func deployRollbackCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "rollback",
		Usage:     "Select a commit from the configured repository and pin it",
		UsageText: "relay deploy rollback <sha>",
		Description: "Select a full commit SHA from the configured repository and pin it. " +
			"Run 'relay deploy unpin' to allow future Git updates.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "commit", Min: 1, Max: 1},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("deploy rollback: too many arguments", 2)
			}
			return deployRollback(ctx, cmd.Writer, deps, cmd.StringArgs("commit")[0])
		},
	}
}

func deployPinCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "pin",
		Usage:     "Block Git updates to the current revision",
		UsageText: "relay deploy pin",
		Description: "Block 'relay git sync' and webhook updates from changing the " +
			"current revision.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("deploy pin: too many arguments", 2)
			}
			return deploySetPinned(ctx, cmd.Writer, deps, true)
		},
	}
}

func deployUnpinCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "unpin",
		Usage:     "Allow future Git updates",
		UsageText: "relay deploy unpin",
		Description: "Allow a future 'relay git sync' or webhook update to change the " +
			"revision. This does not immediately change the current revision.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("deploy unpin: too many arguments", 2)
			}
			return deploySetPinned(ctx, cmd.Writer, deps, false)
		},
	}
}

func deployStatusCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show the desired revision and node/app status",
		Description: "Show the desired generation and commit, pin state, and each node's " +
			"current revision and app status.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("deploy status: too many arguments", 2)
			}
			return deployStatus(ctx, cmd.Writer, deps)
		},
	}
}

// deployRollback publishes one explicit configured-source commit as a new pinned
// generation. It never uses Git ordering metadata; rollback is an operator action
// that works even while the current revision is pinned.
func deployRollback(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	rawCommit string,
) error {
	commit, err := state.NormalizeDeploymentCommit(rawCommit)
	if err != nil {
		return err
	}
	local, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		return fmt.Errorf("deploy rollback: a configured git source is required: %w", err)
	}
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	// Verify the exact commit exists in the configured repository without
	// materializing /apps. ResolveRef fetches and resolves only; SyncCommit is not
	// invoked. A missing object fails closed before publication.
	o := newGitSyncOptions(deps)
	o.Ref = commit
	verified, err := git.ResolveRef(ctx, o, local)
	if err != nil {
		return fmt.Errorf(
			"deploy rollback: commit %s is not available in the configured repository: %w", commit, err,
		)
	}
	if !strings.EqualFold(verified, commit) {
		return fmt.Errorf(
			"deploy rollback: configured repository did not resolve the requested commit %s", commit)
	}

	result, err := st.PublishDeploymentDesired(ctx, state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     commit,
			Repository: local.Repository,
			Ref:        local.Ref,
			Path:       local.Path,
			Publisher:  "cli:rollback",
			Pinned:     true,
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	})
	if err != nil {
		return err
	}
	if !result.Published {
		fmt.Fprintf(w, "Already using %s; deployment remains pinned\n",
			result.Desired.Commit)
		return nil
	}
	fmt.Fprintf(w, "Rolled back to %s; deployment is pinned\n",
		result.Desired.Commit)
	return nil
}

func deploySetPinned(ctx context.Context, w io.Writer, deps Dependencies, pinned bool) error {
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	desired, found, err := st.SetDeploymentPinned(ctx, pinned)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("deploy: no Git deployment exists; run 'relay git set' and 'relay git sync' first")
	}
	if pinned {
		fmt.Fprintf(w, "Deployment pinned at %s; sync and webhook updates are blocked\n",
			desired.Commit)
		return nil
	}
	fmt.Fprintf(w, "Deployment unpinned at %s; current revision remains until the next sync or webhook update\n",
		desired.Commit)
	return nil
}

func deployStatus(ctx context.Context, w io.Writer, deps Dependencies) error {
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	desired, found, err := st.ReadDeploymentDesired(ctx)
	if err != nil {
		return err
	}
	if !found {
		fmt.Fprintln(w, "No deployment is available yet.")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	fmt.Fprintf(tw, "Desired generation:\t%d\n", desired.Generation)
	fmt.Fprintf(tw, "Commit:\t%s\n", shortCommit(desired.Commit))
	fmt.Fprintf(tw, "Source:\t%s ref=%s", desired.Repository, desired.Ref)
	if desired.Path != "" {
		fmt.Fprintf(tw, " path=%s", desired.Path)
	}
	fmt.Fprintln(tw)

	if desired.Pinned {
		fmt.Fprintln(tw, "Pinned:\tyes")
	}

	if err := tw.Flush(); err != nil {
		return err
	}

	nodes, err := st.ReadDeploymentNodes(ctx)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		fmt.Fprintln(w, "\nNo node status available yet.")
		return nil
	}

	fmt.Fprintln(w)

	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tDESIRED\tACTIVE\tSTATUS\tCOMMIT\tUPDATED\tERROR")

	for _, node := range nodes {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
			node.NodeID,
			node.Generation,
			node.AppliedGeneration,
			nodeStatus(node.Phase),
			shortCommit(node.AppliedCommit),
			node.UpdatedAt,
			oneLine(node.Error),
		)
	}

	if err := tw.Flush(); err != nil {
		return err
	}

	for _, node := range nodes {
		if node.AppliedGeneration <= 0 {
			continue
		}

		apps, err := st.ReadDeploymentAppProgress(ctx, node.NodeID, node.AppliedGeneration)
		if err != nil {
			return err
		}
		if len(apps) == 0 {
			continue
		}

		fmt.Fprintf(w, "\nApps on %s (active generation %d)\n",
			node.NodeID, node.AppliedGeneration)

		atw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(atw, "APP\tSTATUS\tCOMMIT\tUPDATED\tERROR")

		for _, app := range apps {
			fmt.Fprintf(atw, "%s\t%s\t%s\t%s\t%s\n",
				app.App,
				app.Phase,
				shortCommit(app.Commit),
				app.UpdatedAt,
				oneLine(app.Error),
			)
		}

		if err := atw.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func shortCommit(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}

func nodeStatus(phase string) string {
	switch phase {
	case state.DeploymentNodeMaterializing:
		return "updating"
	case state.DeploymentNodeMaterialized:
		return "ready"
	default:
		return phase
	}
}

func oneLine(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "\n", " ")
}

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	git "relay/internal/git"
	"relay/internal/state"
)

// gitCommand builds the `relay git ...` manual command family. It never runs
// automatically or touches Redis, Docker, or the worker. It manages local Git
// config, keys, and checkout; `sync` also publishes through PostgreSQL before
// materializing /apps. It is a pure grouping command, so it uses the shared
// namespaceAction:
// a bare `relay git` shows the subcommand help (there is nothing else to do with
// just the command name), and an unknown first token is a friendly Docker-style
// usage error naming the full path.
//
// These are short-lived CLI commands, so they take no process logger: every
// outcome — keygen guidance, the sync step summary, the status table, the
// removal report — is presentation and goes to the command writer. Operational
// diagnostics for background sync (the worker/webhook path) are emitted by the
// git package through SyncOptions.Log, which this command leaves nil.
func gitCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "git",
		Usage: "Configure Git and deploy app updates",
		Description: "`git set` saves the Git source; `git sync` deploys its latest revision. " +
			"No running worker is required.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			{
				Name:      "keygen",
				Usage:     "Generate an SSH deploy key",
				UsageText: "relay git keygen",
				Description: "Generate an SSH deploy key under " + deps.GitSSHDir +
					" and print its public key. Add it to the repository as a read-only deploy key. " +
					"Fails if a key already exists.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git keygen: too many arguments", 2)
					}
					return gitKeygen(cmd.Writer, deps)
				},
			},
			{
				Name:      "set",
				Usage:     "Set the git source repository",
				UsageText: "relay git set <repository> [--ref REF] [--path PATH] [--webhook-secret NAME]",
				Description: "Save the SSH repository and optional ref, app path, and webhook " +
					"secret. This only configures the source; run 'relay git sync' to deploy it.",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "ref", Usage: "branch, tag, or commit (default " + git.DefaultRef + ")"},
					&cli.StringFlag{Name: "path", Usage: "app directory within the repository"},
					&cli.StringFlag{
						Name:  "webhook-secret",
						Usage: "stored secret name for GitHub webhook verification; requires a configured webhook listener",
					},
				},
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "repository", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git set: too many arguments", 2)
					}
					return gitSet(
						ctx,
						cmd.Writer,
						deps,
						cmd.StringArgs("repository")[0],
						cmd.String("ref"),
						cmd.String("path"),
						cmd.String("webhook-secret"),
					)
				},
			},
			{
				Name:  "sync",
				Usage: "Deploy the latest revision from the configured Git source",
				Description: "Deploy the latest revision from the configured Git source. Other " +
					"Relay nodes using the same source update independently. A pinned deployment " +
					"blocks sync; run 'relay deploy unpin' first. If another update wins during " +
					"sync, retry.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git sync: too many arguments", 2)
					}
					return gitSync(ctx, cmd.Writer, deps)
				},
			},
			{
				Name:        "status",
				Usage:       "Show the Git source and last sync",
				Description: "Show the configured source and last sync time. Does not print key material.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git status: too many arguments", 2)
					}
					return gitStatus(cmd.Writer, deps)
				},
			},
			{
				Name:      "remove",
				Usage:     "Remove the git source config and checkout",
				UsageText: "relay git remove [-y]",
				Description: "Remove the saved Git source and checkout. Leaves /apps and the " +
					"deploy key untouched. Prompts for confirmation (default No); use -y to skip.",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "yes", Aliases: []string{"y"}, Usage: "skip confirmation (for automation)"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git remove: too many arguments", 2)
					}
					return gitRemoveAction(ctx, cmd, deps)
				},
			},
		},
	}
}

// gitKeygen generates the SSH key and prints the public key plus deployment
// guidance. An existing key is an error (the message says so).
func gitKeygen(w io.Writer, deps Dependencies) error {
	pub, err := git.GenerateKey(deps.GitSSHDir)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, pub)
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Add the public key above as a read-only Deploy Key for your repository,")
	fmt.Fprintln(w, "then run 'relay git set <repository>'.")
	return nil
}

// gitSet validates and persists the source config (upsert semantics; calling
// again overwrites) by delegating to git.SetSource, which funnels through the
// package's one canonical validation path (SSH-URL, ref, monorepo-path, and,
// when provided, webhook-secret-name rules). It takes no logger: the
// confirmation line on w IS the complete record of this single-step command. The
// confirmation line may mention the webhook-secret REFERENCE name (never its
// value).
func gitSet(ctx context.Context, w io.Writer, deps Dependencies, repository, ref, path, webhookSecretRef string) error {
	// Default the ref to DefaultRef when the flag was omitted, so the
	// confirmation line shows the effective ref. SetSource applies the same
	// default and the full canonical validation.
	if ref == "" {
		ref = git.DefaultRef
	}
	if err := git.SetSource(deps.GitConfigPath, repository, ref, path, webhookSecretRef); err != nil {
		return err
	}
	fmt.Fprintf(w, "Set git source repository=%s ref=%s", repository, ref)
	if path != "" {
		fmt.Fprintf(w, " path=%s", path)
	}
	if webhookSecretRef != "" {
		fmt.Fprintf(w, " webhook-secret=%s", webhookSecretRef)
	}
	fmt.Fprintln(w)
	return nil
}

// gitSync runs the manual sync; the process context propagates Ctrl-C. It loads
// the persisted source, publishes its resolved SHA through PostgreSQL, then
// materializes exactly that commit. Its options carry the injected filesystem
// locations and CloneURL transport override (empty in production and never
// persisted, so the configured repository remains the validated SSH URL).
// SyncOptions.Log is left nil: this short-lived command's step summary on w is
// its complete user-facing record, and background operational diagnostics belong
// to the worker/webhook path.
func gitSync(ctx context.Context, w io.Writer, deps Dependencies) error {
	o := newGitSyncOptions(deps)
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		return err
	}
	if deps.OpenState == nil {
		return fmt.Errorf("git sync: deployment state is unavailable")
	}
	// A configured Git source always participates in coordinated publication.
	// Open PostgreSQL BEFORE resolving the moving ref or touching /apps so a
	// missing/unreachable DATABASE_URL fails clearly without side effects.
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	desired, found, err := st.ReadDeploymentDesired(ctx)
	if err != nil {
		return err
	}
	if found && desired.Pinned {
		return fmt.Errorf(
			"git sync: deployment is pinned at %s; run 'relay deploy unpin' before syncing",
			shortSHA(desired.Commit))
	}
	// Capture the generation this sync observed BEFORE resolving the moving ref.
	// Publication is a compare-and-swap against it: two concurrent `git sync`
	// processes that resolve B then C cannot let the slower B overwrite C as a new
	// generation. Zero means "no deployment row yet", so the first publication is
	// still a normal absent-row CAS.
	expectedGeneration := desired.Generation
	if !found {
		expectedGeneration = 0
	}

	// Resolve the configured branch/tag tip to an immutable SHA before publishing.
	// No materialization happens while the desired revision is merely being
	// published; SyncCommit below checks out exactly the published object.
	o.Out = nil
	sha, err := git.ResolveRef(ctx, o, cfg)
	if err != nil {
		return err
	}

	result, err := st.PublishDeploymentDesired(ctx, state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     sha,
			Repository: cfg.Repository,
			Ref:        cfg.Ref,
			Path:       cfg.Path,
			Publisher:  "cli:git-sync",
		},
		ExpectedGeneration: expectedGeneration,
		AllowPinned:        false,
	})
	if err != nil {
		if errors.Is(err, state.ErrDeploymentConflict) {
			return fmt.Errorf(
				"git sync: deployment changed during sync; run 'relay git sync' again")
		}
		return err
	}
	if !result.Published && result.Reason == state.DeploymentPublishReasonPinned {
		return fmt.Errorf(
			"git sync: deployment is pinned at %s; run 'relay deploy unpin' before syncing",
			shortSHA(result.Desired.Commit))
	}

	o.Out = w
	return git.SyncCommit(ctx, o, cfg, sha)
}

func newGitSyncOptions(deps Dependencies) git.SyncOptions {
	o := git.NewSyncOptions()
	o.ConfigPath = deps.GitConfigPath
	o.CheckoutDir = deps.GitCheckoutDir
	o.AppsDir = deps.GitAppsDir
	o.SSHDir = deps.GitSSHDir
	o.CloneURL = deps.GitCloneURL
	return o
}

func shortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// gitStatus renders the sync state with tab-aligned labels. A missing config is
// NOT an error: it prints "No git source configured." and returns nil (exit 0).
// It takes no logger: the rendered table on w IS the status report, and there is
// no multi-step progress to trail.
func gitStatus(w io.Writer, deps Dependencies) error {
	s, err := git.Status(deps.GitConfigPath, deps.GitSSHDir, deps.GitCheckoutDir)
	if err != nil {
		// A corrupt config is a genuine error; nothing-configured is not.
		return err
	}
	git.PrintStatus(w, s)
	return nil
}

// gitRemoveAction runs the confirmation gate for `git remove` and, when
// confirmed (or -y/--yes is set), performs the removal. It reads one answer
// line from cmd.Reader (the injected stdin — tests drive it through runCLI's
// Reader; never os.Stdin), prompts on os.Stderr (a prompt is UI, not a command
// result, mirroring secret.go's terminal value reader), and routes the removal report
// to cmd.Writer. The safe default is No: anything but an explicit y/yes on a
// trimmed, lowercased line cancels silently with a nil error (exit 0, nothing
// removed, no output on the command writer). That default makes the command
// safe for automation: piped stdin with EOF reads "" and therefore cancels, so
// a bare non-interactive `git remove` can never remove anything.
func gitRemoveAction(ctx context.Context, cmd *cli.Command, deps Dependencies) error {
	if cmd.Bool("yes") {
		return gitRemove(cmd.Writer, deps)
	}
	yes, err := confirmRemove(cmd.Reader, cmd.Writer, os.Stderr)
	if err != nil {
		return err
	}
	if !yes {
		// Safe default of No: nothing is removed and nothing is printed on the
		// command writer — cancellation is silent (exit 0), so the absence of
		// the removal report is the only record.
		return nil
	}
	return gitRemove(cmd.Writer, deps)
}

// confirmRemove asks the operator on stderr for a remove confirmation and
// reports the verdict. It is the testable core of the confirmation gate: the
// prompt goes to stderr, one line is read from in, and only an explicit
// y/yes (case-insensitive, whitespace-trimmed) yields true. The safe default
// is No: an empty or unknown answer returns (false, nil) so the caller cancels
// silently rather than erroring. Reading the line uses a fresh bufio.Reader
// per call, which is fine because remove reads exactly one line.
func confirmRemove(in io.Reader, w io.Writer, stderr io.Writer) (bool, error) {
	fmt.Fprint(stderr, "Remove git source config and checkout? [y/N] ")
	answer, err := readLine(in)
	if err != nil {
		return false, fmt.Errorf("read confirmation: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

// readLine reads one line from in, returning the text without its trailing
// newline. The newline is stripped from whatever ReadString returns; a final
// unterminated line (io.EOF without a newline) is still a valid answer, and
// empty/EOF-only input yields "". This keeps the confirmation gate's default-No
// behavior identical for a piped empty stdin and a typed empty line.
func readLine(in io.Reader) (string, error) {
	if in == nil {
		return "", nil
	}
	r := bufio.NewReader(in)
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}

// gitRemove removes the config and checkout, leaving /apps and the key.
// It takes no logger: the removal report on w IS the complete record of this
// single-step command.
func gitRemove(w io.Writer, deps Dependencies) error {
	return git.Remove(deps.GitConfigPath, deps.GitCheckoutDir, w)
}

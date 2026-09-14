package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/urfave/cli/v3"

	"relay/internal/function"
	git "relay/internal/git"
)

// gitConfigPath, gitCheckoutDir, gitSSHDir, and gitFunctionsDir are the derived
// paths the CLI git commands operate on. They default to the fixed internal
// paths (/var/lib/relay/... and function.Dir); tests replace them with temp dirs
// so the commands never touch /var/lib/relay or /functions.
var (
	gitConfigPath   = git.ConfigPath
	gitCheckoutDir  = git.CheckoutDir
	gitSSHDir       = git.SSHDir
	gitFunctionsDir = function.Dir
)

// gitCommand builds the `relay git ...` manual sync subcommand family. It never
// runs automatically and never touches Redis, Docker, or the worker; it only
// writes the local git config/key/checkout and materializes /functions (on
// sync). Mirroring secret.go/function.go, unknown or missing subcommands are
// usage errors. It receives the process logger (like startCommand and
// healthCommand) but threads it ONLY into the sync path: sync is the one
// multi-step operation whose progress is worth a Debug-level trail. set, status,
// keygen, and remove are single-step commands whose whole outcome already lands
// on the command writer, so they take no logger at all.
func gitCommand(logger *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "git",
		Usage: "Manage manual Git synchronization",
		Description: "Generate an SSH deploy key, configure a repository source, and " +
			"manually sync it into /functions. Sync is always a manual, explicit " +
			"operation: Relay never polls or syncs automatically.",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			switch {
			case !cmd.Args().Present():
				return cli.Exit("git: missing subcommand", 2)
			default:
				return cli.Exit(fmt.Sprintf("git: unknown subcommand %q", cmd.Args().First()), 2)
			}
		},
		Commands: []*cli.Command{
			{
				Name:      "keygen",
				Usage:     "Generate an SSH deploy key",
				UsageText: "relay git keygen",
				Description: "Generate a new ed25519 pair under " + gitSSHDir + " (0700/0600) and " +
					"print the public key in authorized_keys form. Add it as a read-only Deploy " +
					"Key with your provider; Relay itself never calls a provider API. Fails if a " +
					"key already exists (remove the file to regenerate).",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git keygen: too many arguments", 2)
					}
					return gitKeygen(cmd.Writer)
				},
			},
			{
				Name:      "set",
				Usage:     "Set the git source repository",
				UsageText: "relay git set <repository> [--ref REF] [--path PATH]",
				Description: "Remember an SSH git repository to sync from. The value must be an SSH " +
					"URL (scp-like or ssh://). --ref selects the branch/tag/commit (default " + git.DefaultRef + "); " +
					"--path selects an optional monorepo subdirectory. Calling set again overwrites the source.",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "ref", Usage: "branch, tag, or commit to sync (default " + git.DefaultRef + ")"},
					&cli.StringFlag{Name: "path", Usage: "optional monorepo subdirectory within the repo"},
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
						cmd.StringArgs("repository")[0],
						cmd.String("ref"),
						cmd.String("path"),
					)
				},
			},
			{
				Name:  "sync",
				Usage: "Sync the configured source into /functions",
				Description: "Clone/fetch/checkout the configured repository and materialize its function " +
					"directories into /functions deterministically. Uses the process context so Ctrl-C " +
					"propagates.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git sync: too many arguments", 2)
					}
					return gitSync(ctx, cmd.Writer, logger)
				},
			},
			{
				Name:  "status",
				Usage: "Show git sync status",
				Description: "Show the configured source, whether the SSH key and checkout exist, and the last " +
					"sync time. Prints no key material.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git status: too many arguments", 2)
					}
					return gitStatus(cmd.Writer)
				},
			},
			{
				Name:  "remove",
				Usage: "Remove the git source config and checkout",
				Description: "Remove the persisted git source config and the managed checkout. Leaves /functions " +
					"and the SSH key untouched (the key survives because the operator registers it as a Deploy Key).",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git remove: too many arguments", 2)
					}
					return gitRemove(cmd.Writer)
				},
			},
		},
	}
}

// gitKeygen generates the SSH key and prints the public key plus deployment
// guidance. An existing key is an error (the message says so).
func gitKeygen(w io.Writer) error {
	pub, err := git.GenerateKey(gitSSHDir)
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
// again overwrites). It enforces the SSH-URL and monorepo-path rules. It takes
// no logger: the confirmation line on w IS the complete record of this
// single-step command.
func gitSet(ctx context.Context, w io.Writer, repository, ref, path string) error {
	// Default the ref to DefaultRef when the flag was omitted.
	if ref == "" {
		ref = git.DefaultRef
	}
	if err := git.ValidateRepositoryURL(repository); err != nil {
		return err
	}
	if err := git.SetSource(gitConfigPath, repository, ref, path); err != nil {
		return err
	}
	fmt.Fprintf(w, "Set git source repository=%s ref=%s", repository, ref)
	if path != "" {
		fmt.Fprintf(w, " path=%s", path)
	}
	fmt.Fprintln(w)
	return nil
}

// gitSync runs the manual sync; the process context propagates Ctrl-C. The sync
// reads the persisted config itself (via opts.ConfigPath), so production simply
// points at the CLI-level dir defaults. logger is the injected process logger
// (DI), threaded through to SyncOptions.Log so the sync core can emit its
// Debug-level step trail on it; user-facing progress still lands on w.
func gitSync(ctx context.Context, w io.Writer, logger *slog.Logger) error {
	o := git.NewSyncOptions()
	o.ConfigPath = gitConfigPath
	o.CheckoutDir = gitCheckoutDir
	o.FunctionsDir = gitFunctionsDir
	o.SSHDir = gitSSHDir
	o.Log = logger
	o.Out = w
	return git.Sync(ctx, o)
}

// gitStatus renders the sync state with tab-aligned labels. A missing config is
// NOT an error: it prints "No git source configured." and returns nil (exit 0).
// It takes no logger: the rendered table on w IS the status report, and there is
// no multi-step progress to trail.
func gitStatus(w io.Writer) error {
	s, err := git.Status(gitConfigPath, gitSSHDir, gitCheckoutDir)
	if err != nil {
		// A corrupt config is a genuine error; nothing-configured is not.
		return err
	}
	git.PrintStatus(w, s)
	return nil
}

// gitRemove removes the config and checkout, leaving /functions and the key.
// It takes no logger: the removal report on w IS the complete record of this
// single-step command.
func gitRemove(w io.Writer) error {
	return git.Remove(gitConfigPath, gitCheckoutDir, w)
}

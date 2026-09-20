package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

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
// sync). It is a pure grouping command, so it uses the shared namespaceAction:
// a bare `relay git` shows the subcommand help (there is nothing else to do with
// just the command name), and an unknown first token is a friendly Docker-style
// usage error naming the full path.
//
// These are short-lived CLI commands, so they take no process logger: every
// outcome — keygen guidance, the sync step summary, the status table, the
// removal report — is presentation and goes to the command writer. Operational
// diagnostics for background sync (the worker/webhook path) are emitted by the
// git package through SyncOptions.Log, which this command leaves nil.
func gitCommand() *cli.Command {
	return &cli.Command{
		Name:  "git",
		Usage: "Manage manual Git synchronization",
		Description: "Generate an SSH deploy key, configure a repository source, and " +
			"manually sync it into /functions.",
		Action: namespaceAction(),
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
				UsageText: "relay git set <repository> [--ref REF] [--path PATH] [--webhook-secret NAME]",
				Description: "Remember an SSH git repository to sync from. The value must be an SSH " +
					"URL (scp-like or ssh://). --ref selects the branch/tag/commit (default " + git.DefaultRef + "); " +
					"--path selects an optional monorepo subdirectory. --webhook-secret names the secret " +
					"(in Relay's secret store) holding the GitHub webhook secret used to verify deliveries; " +
					"calling set again overwrites the source.",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "ref", Usage: "branch, tag, or commit to sync (default " + git.DefaultRef + ")"},
					&cli.StringFlag{Name: "path", Usage: "optional monorepo subdirectory within the repo"},
					&cli.StringFlag{
						Name:  "webhook-secret",
						Usage: "name of the secret holding the GitHub webhook secret (optional; when set, webhook deliveries must be signed with this secret)",
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
						cmd.StringArgs("repository")[0],
						cmd.String("ref"),
						cmd.String("path"),
						cmd.String("webhook-secret"),
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
					return gitSync(ctx, cmd.Writer)
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
				Name:      "remove",
				Usage:     "Remove the git source config and checkout",
				UsageText: "relay git remove [-y]",
				Description: "Remove the persisted git source config and the managed checkout. Leaves /functions " +
					"and the SSH key untouched (the key survives because the operator registers it as a Deploy Key). " +
					"Prompts for confirmation on the terminal with a default of No unless -y/--yes is given (for automation).",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "yes", Aliases: []string{"y"}, Usage: "skip confirmation (for automation)"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("git remove: too many arguments", 2)
					}
					return gitRemoveAction(ctx, cmd)
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
// again overwrites). It enforces the SSH-URL, monorepo-path, and (when
// provided) webhook-secret-name rules. It takes no logger: the confirmation
// line on w IS the complete record of this single-step command. The confirmation
// line may mention the webhook-secret REFERENCE name (never its value).
func gitSet(ctx context.Context, w io.Writer, repository, ref, path, webhookSecretRef string) error {
	// Default the ref to DefaultRef when the flag was omitted.
	if ref == "" {
		ref = git.DefaultRef
	}
	if err := git.ValidateRepositoryURL(repository); err != nil {
		return err
	}
	if err := git.SetSource(gitConfigPath, repository, ref, path, webhookSecretRef); err != nil {
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

// gitSync runs the manual sync; the process context propagates Ctrl-C. The sync
// reads the persisted config itself (via opts.ConfigPath), so production simply
// points at the CLI-level dir defaults. SyncOptions.Log is left nil: this
// short-lived command's step summary on w is its complete user-facing record,
// and background operational diagnostics belong to the worker/webhook path.
func gitSync(ctx context.Context, w io.Writer) error {
	o := git.NewSyncOptions()
	o.ConfigPath = gitConfigPath
	o.CheckoutDir = gitCheckoutDir
	o.FunctionsDir = gitFunctionsDir
	o.SSHDir = gitSSHDir
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

// gitRemoveAction runs the confirmation gate for `git remove` and, when
// confirmed (or -y/--yes is set), performs the removal. It reads one answer
// line from cmd.Reader (the injected stdin — tests drive it through runCLI's
// Reader; never os.Stdin), prompts on os.Stderr (a prompt is UI, not a command
// result, mirroring secret.go's readSecretValue), and routes the removal report
// to cmd.Writer. The safe default is No: anything but an explicit y/yes on a
// trimmed, lowercased line cancels silently with a nil error (exit 0, nothing
// removed, no output on the command writer). That default makes the command
// safe for automation: piped stdin with EOF reads "" and therefore cancels, so
// a bare non-interactive `git remove` can never remove anything.
func gitRemoveAction(ctx context.Context, cmd *cli.Command) error {
	if cmd.Bool("yes") {
		return gitRemove(cmd.Writer)
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
	return gitRemove(cmd.Writer)
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

// gitRemove removes the config and checkout, leaving /functions and the key.
// It takes no logger: the removal report on w IS the complete record of this
// single-step command.
func gitRemove(w io.Writer) error {
	return git.Remove(gitConfigPath, gitCheckoutDir, w)
}

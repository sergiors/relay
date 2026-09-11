package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"
	"golang.org/x/term"

	"relay/internal/secrets"
)

// secretsPath is the local secrets directory the CLI reads and writes. It
// defaults to the fixed internal path; tests replace it with a temp dir so they
// never touch /var/lib/relay.
var secretsPath = secrets.SecretsDir

// secretCommand builds the `relay secret ...` subcommand family. It manages
// local secret files under the secrets directory only — it never touches
// Redis, Docker, or the worker.
func secretCommand() *cli.Command {
	return &cli.Command{
		Name:  "secret",
		Usage: "Manage local secrets",
		Description: "List, set, and remove local secrets. Secret values are stored locally " +
			"and are never accepted as positional arguments or printed.",
		// Unknown or missing subcommands are usage errors; a non-nil Action here
		// keeps an unknown token from falling through to the built-in help.
		Action: func(ctx context.Context, cmd *cli.Command) error {
			switch {
			case !cmd.Args().Present():
				return cli.Exit("secret: missing subcommand", 2)
			default:
				return cli.Exit(fmt.Sprintf("secret: unknown subcommand %q", cmd.Args().First()), 2)
			}
		},
		Commands: []*cli.Command{
			{
				Name:        "ls",
				Usage:       "List secrets",
				Description: "List the names of all local secrets, sorted.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("secret ls: too many arguments", 2)
					}
					return secretList(ctx, cmd.Writer)
				},
			},
			{
				Name:      "set",
				Usage:     "Create or update a secret",
				UsageText: "relay secret set NAME",
				Description: "Create or update a secret. The value is read from the terminal " +
					"(hidden) or, when stdin is not a terminal, from all of stdin. It is never " +
					"accepted as a positional argument and never printed.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						// A value must never be passed as a positional argument: the
						// single name argument plus this guard makes
						// `relay secret set foo bar` invalid.
						return cli.Exit("secret set: too many arguments", 2)
					}
					return secretSet(ctx, cmd.Reader, cmd.Writer, cmd.StringArgs("name")[0])
				},
			},
			{
				Name:        "rm",
				Usage:       "Remove a secret",
				Description: "Remove a local secret. A missing secret is an error.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("secret rm: too many arguments", 2)
					}
					return secretRm(ctx, cmd.Writer, cmd.StringArgs("name")[0])
				},
			},
		},
	}
}

// openSecretStore opens the local secrets store at the package path. It is
// infallible to construct (the directory is created lazily on Set).
func openSecretStore() (*secrets.LocalStore, error) {
	return secrets.NewLocal(secretsPath)
}

// secretList prints the sorted secret names as a single NAME column.
func secretList(ctx context.Context, w io.Writer) error {
	store, err := openSecretStore()
	if err != nil {
		return err
	}
	names, err := store.List(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME")
	for _, n := range names {
		fmt.Fprintln(tw, n)
	}
	_ = tw.Flush()
	return nil
}

// secretSet reads a value (hidden on a terminal, or all of stdin when piped)
// and writes it atomically. The value is never echoed and never printed.
func secretSet(ctx context.Context, in io.Reader, out io.Writer, name string) error {
	store, err := openSecretStore()
	if err != nil {
		return err
	}
	return secretSetValue(ctx, store, name, in, out)
}

// secretSetValue is the testable core of `secret set`: it validates the name,
// reads the value from in (hidden when in is a terminal, else all of stdin),
// writes it via store, and reports success on out. It returns an error.
func secretSetValue(ctx context.Context, store *secrets.LocalStore, name string, in io.Reader, out io.Writer) error {
	if err := secrets.ValidateName(name); err != nil {
		return err
	}
	value, err := readSecretValue(in)
	if err != nil {
		return err
	}
	if err := store.Set(ctx, name, value); err != nil {
		return err
	}
	fmt.Fprintf(out, "Successfully set secret %q\n", name)
	return nil
}

// readSecretValue reads a secret value from in. When in is a terminal, it
// prompts and reads a single line with echo disabled (so the value is never
// shown); otherwise it reads ALL of stdin (the safe non-interactive path, e.g.
// `printf 'value' | relay secret set foo`). In both cases a single trailing
// newline is stripped; the stored bytes are otherwise authoritative.
func readSecretValue(in io.Reader) (string, error) {
	if f, ok := in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		fmt.Fprint(os.Stderr, "Enter value: ")
		line, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", fmt.Errorf("read secret: %w", err)
		}
		return strings.TrimSuffix(string(line), "\n"), nil
	}
	data, err := io.ReadAll(in)
	if err != nil {
		return "", fmt.Errorf("read secret from stdin: %w", err)
	}
	// Strip a single trailing newline (the terminal-input convention), so a
	// piped `printf 'value\n'` stores "value" exactly like terminal input.
	s := string(data)
	s = strings.TrimSuffix(s, "\n")
	s = strings.TrimSuffix(s, "\r")
	return s, nil
}

// secretRm removes a secret. A missing secret is an error (exit 1 via main).
func secretRm(ctx context.Context, w io.Writer, name string) error {
	store, err := openSecretStore()
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, name); err != nil {
		return err
	}
	fmt.Fprintf(w, "Removed secret %q\n", name)
	return nil
}

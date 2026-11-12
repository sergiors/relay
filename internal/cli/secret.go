package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/urfave/cli/v3"
	"golang.org/x/term"

	"relay/internal/config"
	"relay/internal/secrets"
)

// secretStdinArg is the only accepted second positional argument of
// `secret set`: it explicitly selects stdin as the value source. Secret values
// are never accepted as positional arguments.
const secretStdinArg = "-"

// secretCommand builds the `relay secret ...` subcommand family. It never
// touches Redis, Docker, or the worker. `ls` reads the provider selected by
// SECRETS_PROVIDER (local files by default, or Infisical); `set` and `rm` write
// only local files and reject every other provider before reading a value or
// touching the filesystem. It is a pure grouping command, so it uses the
// shared namespaceAction: a bare `relay secret` shows the subcommand help, and
// an unknown first token is a friendly Docker-style usage error naming the full
// path.
func secretCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "secret",
		Usage: "Manage secrets (list active provider; set/rm local only)",
		Description: "List secret names from the provider selected by SECRETS_PROVIDER, and create or " +
			"remove local secret files. `secret ls` reads the active provider (local files by default, " +
			"or Infisical); `secret set` and `secret rm` write only the local secrets directory and " +
			"fail without reading a value when an external provider is selected. Secret values are " +
			"never accepted as positional arguments or printed.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			{
				Name:  "ls",
				Usage: "List secrets from the active provider",
				Description: "List the names of secrets in the provider selected by SECRETS_PROVIDER " +
					"(local files by default, or Infisical), sorted. Only names are read: values are " +
					"never requested from Infisical or printed.",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("secret ls: too many arguments", 2)
					}
					return secretList(ctx, cmd.Writer, deps)
				},
			},
			{
				Name:      "set",
				Usage:     "Create or update a local secret (local provider only)",
				UsageText: "relay secret set NAME [-]",
				Description: "Create or update a local secret. This command is available only when " +
					"SECRETS_PROVIDER=local (the default); with an external provider it fails before " +
					"reading any value and never writes a local file. Without a second argument the value " +
					"is read from an interactive terminal with echo disabled; use - to read all of " +
					"stdin instead. A secret value is never accepted as a positional argument " +
					"and is never printed.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1, UsageText: "NAME"},
					&cli.StringArgs{Name: "source", Min: 0, Max: 1, UsageText: "[-]"},
				},
				// Stop flag parsing after NAME so a trailing "-" and any extra
				// argument are preserved as positionals: urfave otherwise swallows
				// everything after a lone "-", which would silently accept extra
				// arguments instead of rejecting them.
				StopOnNthArg: stopOnFirstPositional(),
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("secret set: too many arguments", 2)
					}
					source := cmd.StringArgs("source")
					if len(source) > 0 && source[0] != secretStdinArg {
						// A value must never be passed as a positional argument.
						// Do not echo the rejected literal: it is secret material.
						return cli.Exit(
							"secret set: secret values must not be passed as arguments (use - to read the value from stdin)",
							2,
						)
					}
					return secretSet(ctx, cmd.Reader, cmd.Writer, deps, cmd.StringArgs("name")[0], len(source) > 0)
				},
			},
			{
				Name:  "rm",
				Usage: "Remove a local secret (local provider only)",
				Description: "Remove a local secret. This command is available only when " +
					"SECRETS_PROVIDER=local (the default); with an external provider it fails before " +
					"touching the local secrets directory. A missing secret is an error.",
				Arguments: []cli.Argument{
					&cli.StringArgs{Name: "name", Min: 1, Max: 1},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Args().Present() {
						return cli.Exit("secret rm: too many arguments", 2)
					}
					return secretRm(ctx, cmd.Writer, deps, cmd.StringArgs("name")[0])
				},
			},
		},
	}
}

// openSecretStore opens the local secrets store at dir. It is infallible to
// construct (the directory is created lazily on Set).
func openSecretStore(dir string) (*secrets.LocalStore, error) {
	return secrets.NewLocal(dir)
}

// openConfiguredSecretProvider resolves only the secret-provider configuration
// and constructs the selected provider. It deliberately does not load the full
// worker configuration, so `relay secret ls` works without Redis, Docker, or
// PostgreSQL. Provider construction validates configuration only; the remote
// login happens on the first List call.
func openConfiguredSecretProvider(localDir string) (secrets.Provider, error) {
	cfg, err := config.LoadSecretsConfig()
	if err != nil {
		return nil, fmt.Errorf("secret provider: %w", err)
	}
	return secrets.NewProvider(secrets.ProviderOptions{
		Provider: cfg.Provider,
		LocalDir: localDir,
		Infisical: secrets.InfisicalOptions{
			SiteURL:      cfg.Infisical.SiteURL,
			ProjectID:    cfg.Infisical.ProjectID,
			Environment:  cfg.Infisical.Environment,
			SecretPath:   cfg.Infisical.SecretPath,
			ClientID:     cfg.Infisical.ClientID,
			ClientSecret: cfg.Infisical.ClientSecret,
		},
	})
}

// requireLocalSecretProvider resolves and validates the selected provider, then
// rejects every non-local provider. It runs before any secret value is read or
// any local file is touched, so `set` and `rm` can never partially mutate local
// state while an external provider is active. It never constructs a provider or
// opens a network client.
func requireLocalSecretProvider(command string) error {
	cfg, err := config.LoadSecretsConfig()
	if err != nil {
		return fmt.Errorf("%s: %w", command, err)
	}
	if cfg.Provider != config.SecretsProviderLocal {
		return fmt.Errorf(
			"%s: not supported with SECRETS_PROVIDER=%q: Relay writes only local secret files; manage Infisical secrets in the Infisical console or API",
			command, cfg.Provider,
		)
	}
	return nil
}

// secretList prints the sorted secret names from the active provider as a
// single NAME column. Local behavior and output are unchanged.
func secretList(ctx context.Context, w io.Writer, deps Dependencies) error {
	open := deps.OpenSecretProvider
	if open == nil {
		open = openConfiguredSecretProvider
	}
	provider, err := open(deps.SecretsDir)
	if err != nil {
		return err
	}
	lister, ok := provider.(secrets.Lister)
	if !ok {
		return errors.New("secret ls: the selected secret provider does not support listing")
	}
	names, err := lister.List(ctx)
	if err != nil {
		return err
	}
	sort.Strings(names)
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "NAME")
	for _, n := range names {
		fmt.Fprintln(tw, n)
	}
	out.flush(tw)
	return out.err
}

// secretSet reads a value (hidden from an interactive terminal, or all of stdin
// when `-` was given) and writes it atomically. The value is never echoed and
// never printed.
func secretSet(
	ctx context.Context,
	in io.Reader,
	out io.Writer,
	deps Dependencies,
	name string,
	fromStdin bool,
) error {
	if err := requireLocalSecretProvider("secret set"); err != nil {
		return err
	}
	store, err := openSecretStore(deps.SecretsDir)
	if err != nil {
		return err
	}
	return secretSetValue(ctx, store, name, in, out, fromStdin)
}

// secretSetValue is the testable core of `secret set`: it validates the name,
// reads the value from the explicitly selected source (the terminal when
// fromStdin is false, otherwise all of stdin), writes it via store, and reports
// success on out. It returns an error.
func secretSetValue(
	ctx context.Context,
	store *secrets.LocalStore,
	name string,
	in io.Reader,
	out io.Writer,
	fromStdin bool,
) error {
	if err := secrets.ValidateName(name); err != nil {
		return err
	}
	read := readSecretFromTerminal
	if fromStdin {
		read = readSecretFromStdin
	}
	value, err := read(in)
	if err != nil {
		return err
	}
	if value == "" {
		return errors.New("secret set: empty value")
	}
	if err := store.Set(ctx, name, value); err != nil {
		return err
	}
	fmt.Fprintf(out, "Successfully set secret %q\n", name)
	return nil
}

// readSecretFromTerminal prompts on stderr and reads one line with echo disabled,
// so the value is never shown. It requires an actual terminal: when in is not an
// interactive terminal it fails with guidance instead of blocking on a read.
func readSecretFromTerminal(in io.Reader) (string, error) {
	f, ok := in.(*os.File)
	if !ok || !term.IsTerminal(int(f.Fd())) {
		return "", errors.New(
			"secret set: an interactive terminal is required to prompt for the value (use - to read it from stdin)",
		)
	}
	fmt.Fprint(os.Stderr, "Enter value: ")
	line, err := term.ReadPassword(int(f.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("secret set: read terminal: %w", err)
	}
	return strings.TrimSuffix(string(line), "\n"), nil
}

// readSecretFromStdin reads ALL of stdin (the safe non-interactive path, e.g.
// `printf 'value' | relay secret set foo -`). The stored bytes are authoritative
// apart from exactly one trailing line ending (see trimSecretLineEnding), and an
// empty value is rejected rather than stored.
func readSecretFromStdin(in io.Reader) (string, error) {
	if in == nil {
		return "", errors.New("secret set: no stdin available to read (use - only when piping the value)")
	}
	data, err := io.ReadAll(in)
	if err != nil {
		return "", fmt.Errorf("secret set: read stdin: %w", err)
	}
	value := trimSecretLineEnding(string(data))
	if value == "" {
		return "", errors.New("secret set: empty value on stdin")
	}
	return value, nil
}

// trimSecretLineEnding removes exactly one trailing line ending from a piped
// secret value: one CRLF, or else one LF. Every other byte is preserved —
// trailing CR or spaces, leading whitespace, and internal newlines included —
// so a value that legitimately ends in CR is not silently rewritten.
func trimSecretLineEnding(s string) string {
	if trimmed, ok := strings.CutSuffix(s, "\r\n"); ok {
		return trimmed
	}
	return strings.TrimSuffix(s, "\n")
}

// secretRm removes a secret. A missing secret is an error (exit 1 via main).
func secretRm(ctx context.Context, w io.Writer, deps Dependencies, name string) error {
	if err := requireLocalSecretProvider("secret rm"); err != nil {
		return err
	}
	store, err := openSecretStore(deps.SecretsDir)
	if err != nil {
		return err
	}
	if err := store.Delete(ctx, name); err != nil {
		return err
	}
	fmt.Fprintf(w, "Removed secret %q\n", name)
	return nil
}

package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"golang.org/x/term"

	"relay/internal/secrets"
)

// secretsPath is the local secrets directory the CLI reads and writes. It
// defaults to the fixed internal path; tests replace it with a temp dir so they
// never touch /var/lib/relay.
var secretsPath = secrets.SecretsDir

func secretHelp() string {
	return "Usage:\n  relay secret COMMAND\n\nManage local secrets.\n\nCommands:\n" +
		helpWriter([][2]string{
			{"ls", "List secrets"},
			{"set", "Create or update a secret"},
			{"rm", "Remove a secret"},
		}) +
		"\nRun 'relay secret COMMAND --help' for more information on a command.\n"
}

func secretLsUsage() string {
	return "Usage:\n  relay secret ls\n\nList secrets.\n"
}

func secretSetUsage() string {
	return "Usage:\n  relay secret set NAME\n\nCreate or update a secret. The value is read from the terminal (hidden) or, when stdin is not a terminal, from all of stdin.\n"
}

func secretRmUsage() string {
	return "Usage:\n  relay secret rm NAME\n\nRemove a secret.\n"
}

// openSecretStore opens the local secrets store at the package path. It is
// infallible to construct (the directory is created lazily on Set).
func openSecretStore() (*secrets.LocalStore, error) {
	return secrets.NewLocal(secretsPath)
}

// runSecretCommand implements the `relay secret ...` subcommand family. It
// manages local secret files under the secrets directory only — it never
// touches Redis, Docker, or the worker. Exit codes:
//
//	0  success
//	1  runtime error (e.g. unknown secret)
//	2  usage error
func runSecretCommand(args []string) int {
	if len(args) == 0 {
		return printUsageError("secret: missing subcommand", secretHelp())
	}

	switch args[0] {
	case "ls":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, secretLsUsage())
			return 0
		}
		if len(args) != 1 {
			return printUsageError("secret ls: too many arguments", secretLsUsage())
		}
		return secretList()
	case "set":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, secretSetUsage())
			return 0
		}
		if len(args) != 2 {
			return printUsageError("secret set: expected a secret name", secretSetUsage())
		}
		return secretSet(args[1])
	case "rm":
		if len(args) == 2 && isHelp(args[1]) {
			fmt.Fprint(os.Stdout, secretRmUsage())
			return 0
		}
		if len(args) != 2 {
			return printUsageError("secret rm: expected a secret name", secretRmUsage())
		}
		return secretRm(args[1])
	case "--help", "-h":
		if len(args) == 1 {
			fmt.Fprint(os.Stdout, secretHelp())
			return 0
		}
		return printUsageError(fmt.Sprintf("secret: unknown subcommand %q", args[0]), secretHelp())
	default:
		return printUsageError(fmt.Sprintf("secret: unknown subcommand %q", args[0]), secretHelp())
	}
}

// secretList prints the sorted secret names as a single NAME column.
func secretList() int {
	store, err := openSecretStore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	names, err := store.List(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME")
	for _, n := range names {
		fmt.Fprintln(w, n)
	}
	_ = w.Flush()
	return 0
}

// secretSet reads a value (hidden on a terminal, or all of stdin when piped)
// and writes it atomically. The value is never echoed and never printed.
func secretSet(name string) int {
	store, err := openSecretStore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return secretSetValue(store, name, os.Stdin, os.Stdout)
}

// secretSetValue is the testable core of `secret set`: it reads the value from
// in (hidden when in is a terminal, else all of stdin), writes it via store,
// and reports success on out. It returns the process exit code.
func secretSetValue(store *secrets.LocalStore, name string, in io.Reader, out io.Writer) int {
	if err := secrets.ValidateName(name); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	value, err := readSecretValue(in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if err := store.Set(context.Background(), name, value); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Fprintf(out, "Successfully set secret %q\n", name)
	return 0
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

// secretRm removes a secret. A missing secret is an error (exit 1).
func secretRm(name string) int {
	store, err := openSecretStore()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if err := store.Delete(context.Background(), name); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stdout, "Removed secret %q\n", name)
	return 0
}

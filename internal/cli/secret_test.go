package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/secrets"
)

// runSecretCLI runs the command tree with dependencies that the secret family
// never touches, so the focused secret tests run without PostgreSQL or Docker.
// The injected Reader is always a strings.Reader, which is not an interactive
// terminal, making the no-terminal path deterministic.
func runSecretCLI(t *testing.T, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runCLIWithDeps(t, Dependencies{}, stdin, args...)
}

// seedSecretStore redirects the package secrets path to a temp dir, restoring
// the previous value on cleanup, and returns a store over it.
func seedSecretStore(t *testing.T) *secrets.LocalStore {
	t.Helper()
	prev := secretsPath
	secretsPath = filepath.Join(t.TempDir(), "secrets")
	t.Cleanup(func() { secretsPath = prev })
	store, err := secrets.NewLocal(secretsPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store
}

// requireSecretMissing asserts name was not stored.
func requireSecretMissing(t *testing.T, store *secrets.LocalStore, name string) {
	t.Helper()
	if got, err := store.Resolve(context.Background(), name); err == nil {
		t.Fatalf("secret %q was stored as %q despite the error", name, got)
	}
}

// TestSecretSetFromStdin verifies `secret set NAME -` reads the value from all
// of stdin (never echoing it), writes it atomically with 0600 perms, and
// reports success without printing the value.
func TestSecretSetFromStdin(t *testing.T) {
	store := seedSecretStore(t)
	var out bytes.Buffer
	err := secretSetValue(context.Background(), store, "db", strings.NewReader("s3cr3t\n"), &out, true)
	if err != nil {
		t.Fatalf("secretSetValue: %v", err)
	}
	if strings.Contains(out.String(), "s3cr3t") {
		t.Fatalf("output leaked the secret value: %q", out.String())
	}
	if !strings.Contains(out.String(), `Successfully set secret "db"`) {
		t.Fatalf("output missing success message: %q", out.String())
	}
	got, err := store.Resolve(context.Background(), "db")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != "s3cr3t" {
		t.Fatalf("resolved = %q, want s3cr3t (trailing newline stripped)", got)
	}
	info, err := os.Stat(filepath.Join(secretsPath, "db"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %o, want 0600", info.Mode().Perm())
	}
}

// TestSecretSetAtomicUpdate verifies setting the same secret twice overwrites
// atomically and leaves no temp files.
func TestSecretSetAtomicUpdate(t *testing.T) {
	store := seedSecretStore(t)
	var out bytes.Buffer
	if err := secretSetValue(context.Background(), store, "tok",
		strings.NewReader("v1\n"), &out, true); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if err := secretSetValue(context.Background(), store, "tok",
		strings.NewReader("v2\n"), &out, true); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	got, _ := store.Resolve(context.Background(), "tok")
	if got != "v2" {
		t.Fatalf("resolved = %q, want v2", got)
	}
	entries, err := os.ReadDir(secretsPath)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("dir entries = %d, want 1 (no temp files)", len(entries))
	}
}

// TestSecretRmMissingError verifies removing a missing secret is a runtime
// error (exit class 1). The message flows into the returned error (the
// ExitErrHandler is a silent no-op); cmd/main.go prints it and exits 1.
func TestSecretRmMissingError(t *testing.T) {
	_ = seedSecretStore(t)
	_, _, err := runSecretCLI(t, "", "secret", "rm", "ghost")
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("returned error missing the missing-secret name: %v", err)
	}
}

// TestSecretLsNamesOnly verifies `secret ls` prints a NAME header and the
// sorted names, and nothing else.
func TestSecretLsNamesOnly(t *testing.T) {
	store := seedSecretStore(t)
	for _, n := range []string{"zeta", "alpha"} {
		if err := store.Set(context.Background(), n, "v"); err != nil {
			t.Fatalf("set %s: %v", n, err)
		}
	}
	out, _, err := runSecretCLI(t, "", "secret", "ls")
	if err != nil {
		t.Fatalf("secret ls: err = %v, want nil", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if lines[0] != "NAME" {
		t.Fatalf("header = %q, want NAME", lines[0])
	}
	if len(lines) != 3 || lines[1] != "alpha" || lines[2] != "zeta" {
		t.Fatalf("ls rows = %v, want [NAME alpha zeta]", lines)
	}
}

// TestSecretSetCommandReadsExplicitStdin drives the full command path:
// `secret set db -` reads the value from the injected Reader and never echoes
// it.
func TestSecretSetCommandReadsExplicitStdin(t *testing.T) {
	store := seedSecretStore(t)
	out, _, err := runSecretCLI(t, "v4lue\n", "secret", "set", "db", "-")
	if err != nil {
		t.Fatalf("secret set: err = %v, want nil", err)
	}
	if strings.Contains(out, "v4lue") {
		t.Fatalf("output leaked the secret value: %q", out)
	}
	if !strings.Contains(out, `Successfully set secret "db"`) {
		t.Fatalf("output missing success message: %q", out)
	}
	if got, err := store.Resolve(context.Background(), "db"); err != nil || got != "v4lue" {
		t.Fatalf("resolved = %q, err = %v, want v4lue", got, err)
	}
}

// TestSecretSetCommandRequiresTerminalWithoutMarker verifies a bare
// `secret set NAME` does not consume piped stdin implicitly: without a `-` it
// requires an interactive terminal and stores nothing.
func TestSecretSetCommandRequiresTerminalWithoutMarker(t *testing.T) {
	store := seedSecretStore(t)
	const value = "implicit-stdin-super-secret"
	out, stderr, err := runSecretCLI(t, value+"\n", "secret", "set", "db")
	if err == nil || !strings.Contains(err.Error(), "interactive terminal is required") {
		t.Fatalf("err = %v, want an interactive-terminal error", err)
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty (prompt is never reached)", stderr)
	}
	if strings.Contains(err.Error(), value) {
		t.Fatal("error leaked the piped value")
	}
	requireSecretMissing(t, store, "db")
}

// TestSecretCommandErrors verifies arg handling: an unknown subcommand, a
// missing name, and an extra argument are all returned as errors carrying the
// expected message. A positional value is rejected by the dedicated safety
// error. `secret` alone no longer errors — it shows help (see
// TestSecretBareShowsHelp).
func TestSecretCommandErrors(t *testing.T) {
	_ = seedSecretStore(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"secret", "bogus"}, "unknown command: relay secret bogus"},
		{[]string{"secret", "set"}, "not provided"},
		{[]string{"secret", "rm"}, "not provided"},
		{[]string{"secret", "set", "db", "extra"}, "secret values must not be passed as arguments"},
		{[]string{"secret", "set", "db", "-", "extra"}, "secret set: too many arguments"},
	} {
		_, _, err := runSecretCLI(t, "", tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: err = %v, want message containing %q", tc.args, err, tc.want)
		}
	}
}

// A bare `relay secret` shows the subcommand help on stdout and exits 0,
// listing the secret subcommands.
func TestSecretBareShowsHelp(t *testing.T) {
	out, _, err := runSecretCLI(t, "", "secret")
	if err != nil {
		t.Fatalf("secret alone: err = %v, want nil", err)
	}
	if !strings.Contains(out, "COMMANDS:") {
		t.Fatalf("bare secret help missing COMMANDS section:\n%s", out)
	}
	for _, want := range []string{"ls", "set", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("secret help missing %q:\n%s", want, out)
		}
	}
}

// An unknown secret subcommand returns the friendly Docker-style usage error
// naming the full command path.
func TestSecretUnknownCommandFriendly(t *testing.T) {
	_, _, err := runSecretCLI(t, "", "secret", "bogus")
	if err == nil {
		t.Fatal("secret bogus: err = nil, want usage error")
	}
	for _, want := range []string{
		"relay: unknown command: relay secret bogus",
		"Run 'relay secret --help' for more information",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("secret bogus err missing %q: %v", want, err)
		}
	}
}

// TestSecretHelp verifies the secret help and subcommand help render.
func TestSecretHelp(t *testing.T) {
	out, _, err := runSecretCLI(t, "", "secret", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "set", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

// TestSecretSetHelp verifies `secret set --help` documents the stdin marker and
// the interactive-terminal requirement.
func TestSecretSetHelp(t *testing.T) {
	out, _, err := runSecretCLI(t, "", "secret", "set", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"relay secret set NAME [-]", "interactive terminal", "stdin"} {
		if !strings.Contains(out, want) {
			t.Fatalf("help missing %q:\n%s", want, out)
		}
	}
}

// TestSecretSetInvalidName verifies an invalid secret name is rejected early as
// a runtime error (exit class 1) before any value is read. The message flows
// into the returned error; cmd/main.go prints it and exits 1.
func TestSecretSetInvalidName(t *testing.T) {
	_ = seedSecretStore(t)
	_, _, err := runSecretCLI(t, "value\n", "secret", "set", "../etc", "-")
	if err == nil || !strings.Contains(err.Error(), "invalid secret name") {
		t.Fatalf("returned error missing invalid-name message: %v", err)
	}
}

// pemValue is a canonical PEM-shaped private-key fixture (header/footer,
// multiple internal newlines, a blank line, and an indented value with double
// spaces). It is deliberately inert — not a real RSA key — so printing it in a
// test failure is harmless. It has no trailing newline. Every multiline test in
// this package uses exactly this fixture so a single corruption at any hop is
// caught.
const pemValue = "-----BEGIN PRIVATE KEY-----\nMIIB\nline2\n\nindented:  value\n-----END PRIVATE KEY-----"

// TestSecretSetMultilineStdin pins that explicit stdin reads a whole multiline
// (PEM-shaped) value, removes exactly one trailing line ending — one CRLF, or
// else one LF — and preserves every other byte, including leading whitespace,
// internal newlines, lone CRs, and trailing spaces. The stored result is
// written atomically with 0600 perms and is never echoed.
func TestSecretSetMultilineStdin(t *testing.T) {
	cases := []struct {
		name  string
		stdin string
		want  string
	}{
		{"pem fixture no trailing newline", pemValue, pemValue},
		{"pem fixture one trailing newline", pemValue + "\n", pemValue},
		{"no trailing newline verbatim", "v1\nv2", "v1\nv2"},
		// Internal \r\n line endings are preserved verbatim; only a trailing
		// CRLF is adjusted.
		{
			"internal CRLF preserved",
			"-----BEGIN-----\r\na\r\n-----END-----",
			"-----BEGIN-----\r\na\r\n-----END-----",
		},
		{"trailing LF stripped", "a\n", "a"},
		{"trailing CRLF stripped", "a\r\n", "a"},
		{"lone trailing CR preserved", "a\r", "a\r"},
		{"only one trailing LF stripped", "a\n\n", "a\n"},
		{"only one trailing CRLF stripped", "a\r\n\r\n", "a\r\n"},
		{"leading whitespace preserved", "  abc\n", "  abc"},
		{"intentional trailing spaces preserved", "abc  \n", "abc  "},
		{"trailing spaces without newline preserved", "abc  ", "abc  "},
		{"internal newlines preserved", "v1\nv2\nv3\n", "v1\nv2\nv3"},
		{"only CR preserved", "\r", "\r"},
		{"single space preserved", " \n", " "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := seedSecretStore(t)
			var out bytes.Buffer
			if err := secretSetValue(context.Background(), store, "db",
				strings.NewReader(tc.stdin), &out, true); err != nil {
				t.Fatalf("secretSetValue: %v", err)
			}
			// The output must be exactly the success line: any echoed value
			// fragment would change it, including whitespace-only values.
			if got, want := out.String(), `Successfully set secret "db"`+"\n"; got != want {
				t.Fatalf("output = %q, want exactly %q", got, want)
			}
			got, err := store.Resolve(context.Background(), "db")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tc.want {
				t.Fatalf("stored length = %d, want %d (byte-for-byte equality)", len(got), len(tc.want))
			}
			info, err := os.Stat(filepath.Join(secretsPath, "db"))
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("file mode = %o, want 0600", info.Mode().Perm())
			}
		})
	}
}

// TestSecretSetRejectsEmptyStdin verifies explicit stdin rejects values that are
// empty after removing at most one trailing line ending, without storing a
// secret or printing a success line.
func TestSecretSetRejectsEmptyStdin(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stdin string
	}{
		{"empty stream", ""},
		{"one LF", "\n"},
		{"one CRLF", "\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := seedSecretStore(t)
			var out bytes.Buffer
			err := secretSetValue(context.Background(), store, "db", strings.NewReader(tc.stdin), &out, true)
			if err == nil || !strings.Contains(err.Error(), "empty value on stdin") {
				t.Fatalf("err = %v, want an empty-value error", err)
			}
			if out.Len() != 0 {
				t.Fatalf("output = %q, want empty", out.String())
			}
			requireSecretMissing(t, store, "db")
		})
	}
}

// TestSecretSetCommandRejectsEmptyStdin verifies the full command path rejects
// an empty explicit stdin instead of storing an empty secret.
func TestSecretSetCommandRejectsEmptyStdin(t *testing.T) {
	store := seedSecretStore(t)
	out, _, err := runSecretCLI(t, "", "secret", "set", "db", "-")
	if err == nil || !strings.Contains(err.Error(), "empty value on stdin") {
		t.Fatalf("err = %v, want an empty-value error", err)
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
	requireSecretMissing(t, store, "db")
}

// errReader returns data together with a read error, so the read-failure path
// can be checked for secret leakage.
type errReader struct{ data string }

func (r errReader) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	return n, errors.New("boom")
}

// TestSecretSetFromStdinReadError verifies an explicit-stdin read failure is
// reported without storing a secret or echoing any bytes that were read.
func TestSecretSetFromStdinReadError(t *testing.T) {
	store := seedSecretStore(t)
	const value = "read-failure-super-secret"
	var out bytes.Buffer
	err := secretSetValue(context.Background(), store, "db", errReader{data: value}, &out, true)
	if err == nil || !strings.Contains(err.Error(), "secret set: read stdin") {
		t.Fatalf("err = %v, want a stdin read error", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the underlying read error", err)
	}
	if strings.Contains(err.Error(), value) {
		t.Fatal("error leaked the secret value")
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
	requireSecretMissing(t, store, "db")
}

// TestSecretSetPromptRequiresTerminal verifies the terminal prompt path never
// reads a non-terminal reader: it fails with guidance instead of consuming
// stdin, and stores nothing.
func TestSecretSetPromptRequiresTerminal(t *testing.T) {
	store := seedSecretStore(t)
	const value = "nonterminal-super-secret"
	var out bytes.Buffer
	err := secretSetValue(context.Background(), store, "db",
		strings.NewReader(value+"\n"), &out, false)
	if err == nil || !strings.Contains(err.Error(), "interactive terminal is required") {
		t.Fatalf("err = %v, want an interactive-terminal error", err)
	}
	if strings.Contains(err.Error(), value) {
		t.Fatal("error leaked the reader contents")
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q, want empty", out.String())
	}
	requireSecretMissing(t, store, "db")
}

// TestReadSecretFromTerminalRejectsNonTerminal verifies the terminal guard for
// a nil reader, a plain reader, and a non-terminal *os.File. An os.Pipe stands
// in for a redirected stdin: a real file descriptor that is not a terminal.
func TestReadSecretFromTerminalRejectsNonTerminal(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() { _ = pr.Close(); _ = pw.Close() })

	readers := []struct {
		name string
		in   io.Reader
	}{
		{"nil", nil},
		{"strings reader", strings.NewReader("value\n")},
		{"pipe file", pr},
	}
	for _, tc := range readers {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readSecretFromTerminal(tc.in)
			if err == nil || !strings.Contains(err.Error(), "interactive terminal is required") {
				t.Fatalf("err = %v, want an interactive-terminal error", err)
			}
		})
	}
}

// TestSecretSetNeverLeaksValueOnError verifies neither the success output nor a
// returned error ever contains the secret value, on invalid-name, read-failure,
// store-write-failure, and no-terminal paths.
func TestSecretSetNeverLeaksValueOnError(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid name", func(t *testing.T) {
		store := seedSecretStore(t)
		const value = "invalid-name-super-secret"
		var out bytes.Buffer
		err := secretSetValue(ctx, store, "../etc", strings.NewReader(value+"\n"), &out, true)
		if err == nil || !strings.Contains(err.Error(), "invalid secret name") {
			t.Fatalf("err = %v, want invalid-name error", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want empty", out.String())
		}
		if strings.Contains(err.Error(), value) {
			t.Fatal("error leaked the secret value")
		}
	})

	t.Run("read failure", func(t *testing.T) {
		store := seedSecretStore(t)
		const value = "read-failure-super-secret"
		var out bytes.Buffer
		err := secretSetValue(ctx, store, "db", errReader{data: value}, &out, true)
		if err == nil || !strings.Contains(err.Error(), "read stdin") {
			t.Fatalf("err = %v, want read error", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want empty", out.String())
		}
		if strings.Contains(err.Error(), value) {
			t.Fatal("error leaked the secret value")
		}
	})

	t.Run("no terminal", func(t *testing.T) {
		store := seedSecretStore(t)
		const value = "no-terminal-super-secret"
		var out bytes.Buffer
		err := secretSetValue(ctx, store, "db", strings.NewReader(value), &out, false)
		if err == nil || !strings.Contains(err.Error(), "interactive terminal is required") {
			t.Fatalf("err = %v, want an interactive-terminal error", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want empty", out.String())
		}
		if strings.Contains(err.Error(), value) {
			t.Fatal("error leaked the reader contents")
		}
	})

	t.Run("store write failure", func(t *testing.T) {
		// Make the secrets path a regular file, so creating the directory for
		// the write fails after the value has been read.
		prev := secretsPath
		secretsPath = filepath.Join(t.TempDir(), "secrets")
		t.Cleanup(func() { secretsPath = prev })
		if err := os.WriteFile(secretsPath, []byte("blocker"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		store, err := secrets.NewLocal(secretsPath)
		if err != nil {
			t.Fatalf("new store: %v", err)
		}

		const value = "store-failure-super-secret"
		var out bytes.Buffer
		err = secretSetValue(ctx, store, "db", strings.NewReader(value+"\n"), &out, true)
		if err == nil || !strings.Contains(err.Error(), "create dir") {
			t.Fatalf("err = %v, want store write error", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want empty", out.String())
		}
		if strings.Contains(err.Error(), value) {
			t.Fatal("error leaked the secret value")
		}
	})
}

// TestSecretSetRejectsPositionalValue pins argument safety: the only accepted
// second argument is `-`, so any literal value is rejected without being read
// from stdin, stored, or echoed in the output/error.
func TestSecretSetRejectsPositionalValue(t *testing.T) {
	cases := []struct {
		name       string
		positional string
	}{
		{"plain value", "positional-super-secret"},
		{"flag-shaped value", "--positional-super-secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := seedSecretStore(t)
			const piped = "piped-super-secret"
			out, _, err := runSecretCLI(t, piped+"\n", "secret", "set", "db", tc.positional)
			if err == nil ||
				!strings.Contains(err.Error(), "secret values must not be passed as arguments") {
				t.Fatalf("err = %v, want the positional-value rejection", err)
			}
			if out != "" {
				t.Fatalf("stdout = %q, want empty", out)
			}
			for _, value := range []string{tc.positional, piped} {
				if strings.Contains(err.Error(), value) {
					t.Fatalf("error leaked %q: %v", value, err)
				}
			}
			requireSecretMissing(t, store, "db")
		})
	}
}

// TestSecretSetMultilineViaCommandPipe drives the FULL command path
// (`cat private_key.pem | relay secret set rsa-private-key -` equivalently): a
// PEM-shaped value with a single trailing newline piped through the CLI must be
// stored byte-for-byte equal to the fixture (single trailing newline stripped)
// and resolvable through a fresh store, with no fragment of the value echoed.
func TestSecretSetMultilineViaCommandPipe(t *testing.T) {
	_ = seedSecretStore(t)
	out, _, err := runSecretCLI(t, pemValue+"\n", "secret", "set", "db", "-")
	if err != nil {
		t.Fatalf("secret set: err = %v, want nil", err)
	}
	for _, frag := range []string{"BEGIN PRIVATE KEY", "MIIB"} {
		if strings.Contains(out, frag) {
			t.Fatalf("output leaked secret fragment %q", frag)
		}
	}
	if !strings.Contains(out, `Successfully set secret "db"`) {
		t.Fatalf("output missing success message: %q", out)
	}
	// Resolve through a fresh store (not the CLI's) and require byte-for-byte
	// equality with the fixture — proving the stored bytes are authoritative.
	store, err := secrets.NewLocal(secretsPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	got, err := store.Resolve(context.Background(), "db")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != pemValue {
		t.Fatalf("stored length = %d, want %d (byte-for-byte equality)", len(got), len(pemValue))
	}
}

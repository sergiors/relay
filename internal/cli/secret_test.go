package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/secrets"
)

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

// TestSecretSetViaStdinPipe verifies `secret set` reads the value from a piped
// stdin (never echoing it), writes it atomically with 0600 perms, and reports
// success without printing the value.
func TestSecretSetViaStdinPipe(t *testing.T) {
	store := seedSecretStore(t)
	var out bytes.Buffer
	err := secretSetValue(context.Background(), store, "db", strings.NewReader("s3cr3t\n"), &out)
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
	if err := secretSetValue(context.Background(), store, "tok", strings.NewReader("v1\n"), &out); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if err := secretSetValue(context.Background(), store, "tok", strings.NewReader("v2\n"), &out); err != nil {
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
	_, _, err := runCLI(t, "", "secret", "rm", "ghost")
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
	out, _, err := runCLI(t, "", "secret", "ls")
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

// TestSecretSetViaCommandPipe drives the full command path: the value always
// comes from the injected Reader, never from a positional argument.
func TestSecretSetViaCommandPipe(t *testing.T) {
	_ = seedSecretStore(t)
	out, _, err := runCLI(t, "v4lue\n", "secret", "set", "db")
	if err != nil {
		t.Fatalf("secret set: err = %v, want nil", err)
	}
	if strings.Contains(out, "v4lue") {
		t.Fatalf("output leaked the secret value: %q", out)
	}
	if !strings.Contains(out, `Successfully set secret "db"`) {
		t.Fatalf("output missing success message: %q", out)
	}
}

// TestSecretCommandErrors verifies arg handling: an unknown subcommand, a
// missing name, and an extra value are all returned as errors carrying the
// expected message. `secret` alone no longer errors — it shows help (see
// TestSecretBareShowsHelp). The extra value case is the critical safety
// regression (a value passed as a positional argument is never accepted).
func TestSecretCommandErrors(t *testing.T) {
	_ = seedSecretStore(t)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"secret", "bogus"}, "unknown command: relay secret bogus"},
		{[]string{"secret", "set"}, "not provided"},
		{[]string{"secret", "rm"}, "not provided"},
	} {
		_, _, err := runCLI(t, "", tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: err = %v, want message containing %q", tc.args, err, tc.want)
		}
	}

	// The critical safety regression: a value passed as a positional argument
	// is never accepted.
	_, _, err := runCLI(t, "", "secret", "set", "a", "extra")
	if err == nil || !strings.Contains(err.Error(), "secret set: too many arguments") {
		t.Fatalf("set a extra: missing rejection error: %v", err)
	}
}

// A bare `relay secret` shows the subcommand help on stdout and exits 0,
// listing the secret subcommands.
func TestSecretBareShowsHelp(t *testing.T) {
	out, _, err := runCLI(t, "", "secret")
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
	_, _, err := runCLI(t, "", "secret", "bogus")
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
	out, _, err := runCLI(t, "", "secret", "--help")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	for _, want := range []string{"ls", "set", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

// TestSecretSetInvalidName verifies an invalid secret name is rejected early as
// a runtime error (exit class 1). The message flows into the returned error;
// cmd/main.go prints it and exits 1.
func TestSecretSetInvalidName(t *testing.T) {
	_ = seedSecretStore(t)
	_, _, err := runCLI(t, "", "secret", "set", "../etc")
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

// TestSecretSetMultilineStdin pins that `secret set` reads a whole multiline
// (PEM-shaped) stdin value, strips exactly one trailing newline and one
// trailing \r (the terminal-input convention), preserves every internal
// newline, writes the result atomically with 0600 perms, and never echoes it.
// readSecretValue's trailing trimming only ever removes ONE trailing \n and ONE
// trailing \r after it; nothing else is normalized.
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
		// "\r\n" is adjusted (strip one "\n" then one "\r").
		{"internal CRLF preserved", "-----BEGIN-----\r\na\r\n-----END-----", "-----BEGIN-----\r\na\r\n-----END-----"},
		{"trailing CRLF stripped", "a\r\n", "a"},
		{"empty value accepted", "", ""},
		{"only newline becomes empty", "\n", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := seedSecretStore(t)
			var out bytes.Buffer
			if err := secretSetValue(context.Background(), store, "db", strings.NewReader(tc.stdin), &out); err != nil {
				t.Fatalf("secretSetValue: %v", err)
			}
			// The value must never be echoed. Empty values are exempt: an empty
			// substring matches any output trivially.
			if tc.want != "" && strings.Contains(out.String(), tc.want) {
				t.Fatalf("output leaked the secret value (len %d)", len(tc.want))
			}
			if !strings.Contains(out.String(), `Successfully set secret "db"`) {
				t.Fatalf("output missing success message: %q", out.String())
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

// TestSecretSetMultilineViaCommandPipe drives the FULL command path
// (`cat private_key.pem | relay secret set rsa-private-key` equivalently): a
// PEM-shaped value with a single trailing newline piped through runCLI must be
// stored byte-for-byte equal to the fixture (single trailing newline stripped)
// and resolvable through a fresh store, with no fragment of the value echoed.
func TestSecretSetMultilineViaCommandPipe(t *testing.T) {
	_ = seedSecretStore(t)
	out, _, err := runCLI(t, pemValue+"\n", "secret", "set", "db")
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

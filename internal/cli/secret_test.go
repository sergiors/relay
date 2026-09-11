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

// seedSecretStore redirects the package secrets path to a temp dir and returns
// a store over it.
func seedSecretStore(t *testing.T) *secrets.LocalStore {
	t.Helper()
	secretsPath = filepath.Join(t.TempDir(), "secrets")
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
	if err == nil || err.Error() == "" {
		t.Fatalf("returned error missing message: %v", err)
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

// TestSecretCommandErrors verifies arg handling: no subcommand, unknown
// subcommand, missing name, and an extra value are all returned as errors —
// the extra value case is the critical safety regression (a value passed as a
// positional argument is never accepted).
func TestSecretCommandErrors(t *testing.T) {
	_ = seedSecretStore(t)
	for _, args := range [][]string{
		{"secret"},
		{"secret", "bogus"},
		{"secret", "set"},
		{"secret", "rm"},
	} {
		_, _, err := runCLI(t, "", args...)
		if err == nil || err.Error() == "" {
			t.Fatalf("args %v: missing returned error message", args)
		}
	}

	// The critical safety regression: a value passed as a positional argument
	// is never accepted.
	_, _, err := runCLI(t, "", "secret", "set", "a", "extra")
	if err == nil || !strings.Contains(err.Error(), "secret set: too many arguments") {
		t.Fatalf("set a extra: missing rejection error: %v", err)
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
	if err == nil || err.Error() == "" {
		t.Fatalf("returned error missing message: %v", err)
	}
}

package main

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
	code := secretSetValue(store, "db", strings.NewReader("s3cr3t\n"), &out)
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
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
	if code := secretSetValue(store, "tok", strings.NewReader("v1\n"), &out); code != 0 {
		t.Fatalf("set v1 exit = %d", code)
	}
	if code := secretSetValue(store, "tok", strings.NewReader("v2\n"), &out); code != 0 {
		t.Fatalf("set v2 exit = %d", code)
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

// TestSecretRmMissingError verifies removing a missing secret is an error.
func TestSecretRmMissingError(t *testing.T) {
	_ = seedSecretStore(t)
	errOut := captureErr(t, func() {
		_ = runSecretCommand([]string{"rm", "ghost"})
	})
	if !strings.Contains(errOut, "Error:") {
		t.Fatalf("stderr missing error: %q", errOut)
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
	out := capture(t, func() {
		if code := runSecretCommand([]string{"ls"}); code != 0 {
			t.Fatalf("ls exit = %d", code)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if lines[0] != "NAME" {
		t.Fatalf("header = %q, want NAME", lines[0])
	}
	if len(lines) != 3 || lines[1] != "alpha" || lines[2] != "zeta" {
		t.Fatalf("ls rows = %v, want [NAME alpha zeta]", lines)
	}
}

// TestSecretCommandExitCodes verifies arg handling: no subcommand, unknown
// subcommand, and missing name are usage errors (exit 2).
func TestSecretCommandExitCodes(t *testing.T) {
	_ = seedSecretStore(t)
	if code := runSecretCommand(nil); code != 2 {
		t.Fatalf("no args: exit = %d, want 2", code)
	}
	if code := runSecretCommand([]string{"bogus"}); code != 2 {
		t.Fatalf("unknown subcommand: exit = %d, want 2", code)
	}
	if code := runSecretCommand([]string{"set"}); code != 2 {
		t.Fatalf("set no name: exit = %d, want 2", code)
	}
	if code := runSecretCommand([]string{"set", "a", "extra"}); code != 2 {
		t.Fatalf("set extra arg: exit = %d, want 2", code)
	}
	if code := runSecretCommand([]string{"rm"}); code != 2 {
		t.Fatalf("rm no name: exit = %d, want 2", code)
	}
}

// TestSecretHelp verifies the secret help and subcommand help render.
func TestSecretHelp(t *testing.T) {
	var code int
	out := capture(t, func() {
		code = runSecretCommand([]string{"--help"})
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	for _, want := range []string{"relay secret COMMAND", "ls", "set", "rm"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
}

// TestSecretSetInvalidName verifies an invalid secret name is rejected early.
func TestSecretSetInvalidName(t *testing.T) {
	_ = seedSecretStore(t)
	errOut := captureErr(t, func() {
		_ = runSecretCommand([]string{"set", "../etc"})
	})
	if !strings.Contains(errOut, "Error:") {
		t.Fatalf("stderr missing error: %q", errOut)
	}
}

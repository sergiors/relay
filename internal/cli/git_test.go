package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	gitpkg "relay/internal/git"
)

// cliGitPaths carries the redirected dirs the CLI git tests use for assertions.
type cliGitPaths struct {
	configPath, checkoutDir, sshDir, functionsDir string
}

// redirectGitDirs points the CLI git paths at temp dirs so tests never touch
// /var/lib/relay or /functions, and returns the redirected paths.
func redirectGitDirs(t *testing.T) cliGitPaths {
	t.Helper()
	oldCfg, oldCo, oldSSH, oldFn := gitConfigPath, gitCheckoutDir, gitSSHDir, gitFunctionsDir
	p := cliGitPaths{
		configPath:   filepath.Join(t.TempDir(), "git", "source.json"),
		checkoutDir:  filepath.Join(t.TempDir(), "git", "checkout"),
		sshDir:       filepath.Join(t.TempDir(), "ssh"),
		functionsDir: filepath.Join(t.TempDir(), "functions"),
	}
	gitConfigPath, gitCheckoutDir, gitSSHDir, gitFunctionsDir = p.configPath, p.checkoutDir, p.sshDir, p.functionsDir
	t.Cleanup(func() {
		gitConfigPath, gitCheckoutDir, gitSSHDir, gitFunctionsDir = oldCfg, oldCo, oldSSH, oldFn
	})
	return p
}

// TestGitKeygenPrintsPublicKey drives the CLI keygen command and verifies the
// public key line prints with the authorized_keys prefix and the key file is
// written under the redirected SSH dir.
func TestGitKeygenPrintsPublicKey(t *testing.T) {
	p := redirectGitDirs(t)
	out, _, err := runCLI(t, "", "git", "keygen")
	if err != nil {
		t.Fatalf("git keygen: err = %v, want nil", err)
	}
	if !strings.Contains(out, "ssh-ed25519 ") {
		t.Fatalf("keygen output missing public key:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(p.sshDir, gitpkg.PrivateKeyFile)); err != nil {
		t.Fatalf("key file not written: %v", err)
	}
}

// TestGitKeygenRefusesExistingKey drives a second keygen through the CLI and
// expects an error mentioning the key already exists.
func TestGitKeygenRefusesExistingKey(t *testing.T) {
	_ = redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "keygen"); err != nil {
		t.Fatalf("first keygen: %v", err)
	}
	_, _, err := runCLI(t, "", "git", "keygen")
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second keygen err = %v, want 'already exists'", err)
	}
}

// TestGitSetPersistsAndDefaultsRef drives `git set` through the CLI and verifies
// the config file persists with the repository and the default ref.
func TestGitSetPersistsAndDefaultsRef(t *testing.T) {
	p := redirectGitDirs(t)
	out, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git")
	if err != nil {
		t.Fatalf("git set: %v", err)
	}
	if !strings.Contains(out, "Set git source") {
		t.Fatalf("set output missing confirmation:\n%s", out)
	}
	cfg, err := gitpkg.LoadConfig(p.configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Repository != "git@github.com:acme/repo.git" || cfg.Ref != gitpkg.DefaultRef {
		t.Fatalf("persisted = %+v, want repo + default ref %s", cfg, gitpkg.DefaultRef)
	}
}

// TestGitSetExplicitRefAndPath verifies --ref and --path are persisted.
func TestGitSetExplicitRefAndPath(t *testing.T) {
	p := redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "--ref", "v1.2.3", "--path", "pkg/fn", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("git set: %v", err)
	}
	cfg, _ := gitpkg.LoadConfig(p.configPath)
	if cfg.Ref != "v1.2.3" || cfg.Path != "pkg/fn" {
		t.Fatalf("persisted = %+v, want ref v1.2.3 path pkg/fn", cfg)
	}
}

// TestGitSetRejectsInvalidURL ensures an http/https/non-SSH URL is rejected.
func TestGitSetRejectsInvalidURL(t *testing.T) {
	p := redirectGitDirs(t)
	for _, u := range []string{"https://github.com/a/r", "http://x/y", "file:///tmp/r"} {
		_, _, err := runCLI(t, "", "git", "set", u)
		if err == nil || !strings.Contains(err.Error(), "SSH") {
			t.Fatalf("git set %q err = %v, want SSH-only error", u, err)
		}
	}
	if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
		t.Fatal("config persisted despite invalid URL")
	}
}

// TestGitSetRejectsTraversal ensures a monorepo --path that escapes is rejected.
func TestGitSetRejectsTraversal(t *testing.T) {
	p := redirectGitDirs(t)
	for _, path := range []string{"../x", "/abs", "a/../../b"} {
		_, _, err := runCLI(t, "", "git", "set", "--path", path, "git@github.com:acme/repo.git")
		if err == nil {
			t.Fatalf("git set --path %q: nil, want traversal error", path)
		}
	}
	if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
		t.Fatal("config persisted despite traversal path")
	}
}

// TestGitStatusNoConfig drives `git status` with nothing configured: it prints
// "No git source configured." and exits 0 (not an error).
func TestGitStatusNoConfig(t *testing.T) {
	_ = redirectGitDirs(t)
	out, _, err := runCLI(t, "", "git", "status")
	if err != nil {
		t.Fatalf("git status (no config): err = %v, want nil", err)
	}
	if !strings.Contains(out, "No git source configured.") {
		t.Fatalf("status output = %q, want 'No git source configured'", out)
	}
}

// TestGitStatusShowsConfigured drives `git status` after a set: it prints the
// configured repository/ref and the key/checkout booleans, never key material.
func TestGitStatusShowsConfigured(t *testing.T) {
	_ = redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, _, err := runCLI(t, "", "git", "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(out, "git@github.com:acme/repo.git") || !strings.Contains(out, "SSH key") {
		t.Fatalf("status output missing fields:\n%s", out)
	}
	if strings.Contains(out, "BEGIN OPENSSH PRIVATE") {
		t.Fatalf("status leaked key material:\n%s", out)
	}
}

// TestGitUnknownSubcommandFriendlyError verifies an unknown subcommand (single
// and multi-token) returns a friendly Docker-style usage error naming the full
// command path and pointing at the exact help command.
func TestGitUnknownSubcommandFriendlyError(t *testing.T) {
	_ = redirectGitDirs(t)
	for _, c := range []struct {
		args []string
		full string
	}{
		{[]string{"git", "asdsa"}, "relay git asdsa"},
		{[]string{"git", "foo", "bar"}, "relay git foo bar"},
	} {
		_, _, err := runCLI(t, "", c.args...)
		if err == nil {
			t.Fatalf("%v: err = nil, want usage error", c.args)
		}
		for _, want := range []string{
			"relay: unknown command: " + c.full,
			"Usage: relay git",
			"Run 'relay git --help' for more information",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%v err missing %q: %v", c.args, want, err)
			}
		}
	}
}

// TestGitBareShowsHelp verifies `git` alone shows the subcommand help on stdout
// and exits 0 (nil error), listing every subcommand.
func TestGitBareShowsHelp(t *testing.T) {
	_ = redirectGitDirs(t)
	out, _, err := runCLI(t, "", "git")
	if err != nil {
		t.Fatalf("git alone: err = %v, want nil", err)
	}
	if !strings.Contains(out, "COMMANDS:") {
		t.Fatalf("bare git help missing COMMANDS section:\n%s", out)
	}
	for _, want := range []string{"keygen", "set", "sync", "status", "remove"} {
		if !strings.Contains(out, want) {
			t.Fatalf("bare git help missing %q:\n%s", want, out)
		}
	}
}

// TestGitRemoveConfirmed drives `git remove` with an affirmative `y\n` answer:
// it removes the config and reports success.
func TestGitRemoveConfirmed(t *testing.T) {
	p := redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, _, err := runCLI(t, "y\n", "git", "remove")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !strings.Contains(out, "Removed git source config") {
		t.Fatalf("remove output missing confirmation:\n%s", out)
	}
	if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
		t.Fatal("config not removed after confirming")
	}
}

// TestGitRemoveConfirmedYes drives `git remove` with the long affirmative form
// `yes\n`, verifying both accepted spellings.
func TestGitRemoveConfirmedYes(t *testing.T) {
	p := redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, _, err := runCLI(t, "yes\n", "git", "remove")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !strings.Contains(out, "Removed git source config") {
		t.Fatalf("remove output missing confirmation:\n%s", out)
	}
	if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
		t.Fatal("config not removed after confirming")
	}
}

// TestGitRemoveCancelled verifies non-affirmative answers (an explicit no and
// a junk answer) cancel silently: nil error, NO output on the command writer,
// the config STILL present, and the checkout untouched.
func TestGitRemoveCancelled(t *testing.T) {
	p := redirectGitDirs(t)
	for _, answer := range []string{"n\n", "maybe\n"} {
		if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
			t.Fatalf("set: %v", err)
		}
		out, _, err := runCLI(t, answer, "git", "remove")
		if err != nil {
			t.Fatalf("remove with %q: err = %v, want nil", answer, err)
		}
		if out != "" {
			t.Fatalf("remove with %q should stay silent on cancel, got:\n%s", answer, out)
		}
		if _, err := os.Stat(p.configPath); err != nil {
			t.Fatalf("remove with %q: config should still exist, got %v", answer, err)
		}
		if _, err := os.Stat(p.checkoutDir); !os.IsNotExist(err) {
			t.Fatalf("remove with %q: checkout should be untouched", answer)
		}
	}
}

// TestGitRemoveSkipsPromptWithYesFlag verifies `git remove -y` (short form) and
// `git remove --yes` (long form) skip the prompt entirely: removal happens
// with empty stdin and the -y flag is accepted.
func TestGitRemoveSkipsPromptWithYesFlag(t *testing.T) {
	for _, flag := range []string{"-y", "--yes"} {
		p := redirectGitDirs(t)
		if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
			t.Fatalf("set: %v", err)
		}
		// Empty stdin: with the flag the prompt is skipped, so removal proceeds.
		out, _, err := runCLI(t, "", "git", "remove", flag)
		if err != nil {
			t.Fatalf("remove %s: err = %v, want nil", flag, err)
		}
		if !strings.Contains(out, "Removed git source config") {
			t.Fatalf("remove %s output missing confirmation:\n%s", flag, out)
		}
		if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
			t.Fatalf("remove %s: config not removed", flag)
		}
	}
}

// TestGitRemoveCancelledEmptyStdin verifies the safe default of No for
// non-interactive stdin: a bare `git remove` reading EOF (empty piped stdin)
// cancels silently (nil error, no output) and leaves the config intact.
func TestGitRemoveCancelledEmptyStdin(t *testing.T) {
	p := redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("set: %v", err)
	}
	out, _, err := runCLI(t, "", "git", "remove")
	if err != nil {
		t.Fatalf("remove: err = %v, want nil", err)
	}
	if out != "" {
		t.Fatalf("cancelled remove should stay silent, got:\n%s", out)
	}
	if _, err := os.Stat(p.configPath); err != nil {
		t.Fatal("config should still exist after cancelled remove")
	}
}

// TestGitRemoveIdempotent drives `git remove` twice and verifies the config is
// gone and that the second remove reports "not configured" rather than failing.
// The first remove passes -y so it skips the confirmation gate; the second
// passes -y too (there is nothing configured, so there is no risk either way).
func TestGitRemoveIdempotent(t *testing.T) {
	p := redirectGitDirs(t)
	if _, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, _, err := runCLI(t, "", "git", "remove", "-y"); err != nil {
		t.Fatalf("first remove: %v", err)
	}
	if _, err := os.Stat(p.configPath); !os.IsNotExist(err) {
		t.Fatal("config not removed")
	}
	out, _, err := runCLI(t, "", "git", "remove", "-y")
	if err != nil {
		t.Fatalf("second remove: %v", err)
	}
	if !strings.Contains(out, "Not configured") {
		t.Fatalf("second remove output = %q, want 'Not configured'", out)
	}
}

// TestGitHelpRendersSubcommands verifies `git --help` lists the subcommands.
func TestGitHelpRendersSubcommands(t *testing.T) {
	_ = redirectGitDirs(t)
	out, _, err := runCLI(t, "", "git", "--help")
	if err != nil {
		t.Fatalf("git --help: %v", err)
	}
	for _, want := range []string{"keygen", "set", "sync", "status", "remove"} {
		if !strings.Contains(out, want) {
			t.Fatalf("git --help missing %q:\n%s", want, out)
		}
	}
}

// TestGitSetTooManyArgs verifies extra positional args after the repository are
// rejected.
func TestGitSetTooManyArgs(t *testing.T) {
	_ = redirectGitDirs(t)
	_, _, err := runCLI(t, "", "git", "set", "git@github.com:acme/repo.git", "extra")
	if err == nil || !strings.Contains(err.Error(), "too many arguments") {
		t.Fatalf("git set extra err = %v, want too many arguments", err)
	}
}

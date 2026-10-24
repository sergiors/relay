package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
	"relay/internal/git"
	"relay/internal/processlock"
	"relay/internal/secrets"
	"relay/internal/worker"
)

// TestDefaultDependenciesMatchProductionPaths pins that DefaultDependencies
// resolves every filesystem seam to the documented production constants, so
// the CLI reads and writes exactly where the worker and the runtime do.
func TestDefaultDependenciesMatchProductionPaths(t *testing.T) {
	deps := DefaultDependencies()
	if deps.GitConfigPath != git.ConfigPath {
		t.Fatalf("GitConfigPath = %q, want %q", deps.GitConfigPath, git.ConfigPath)
	}
	if deps.GitCheckoutDir != git.CheckoutDir {
		t.Fatalf("GitCheckoutDir = %q, want %q", deps.GitCheckoutDir, git.CheckoutDir)
	}
	if deps.GitSSHDir != git.SSHDir {
		t.Fatalf("GitSSHDir = %q, want %q", deps.GitSSHDir, git.SSHDir)
	}
	if deps.GitAppsDir != app.Dir {
		t.Fatalf("GitAppsDir = %q, want %q", deps.GitAppsDir, app.Dir)
	}
	if deps.SecretsDir != secrets.SecretsDir {
		t.Fatalf("SecretsDir = %q, want %q", deps.SecretsDir, secrets.SecretsDir)
	}
	if deps.SocketPath != worker.SocketPath {
		t.Fatalf("SocketPath = %q, want %q", deps.SocketPath, worker.SocketPath)
	}
	if deps.LockPath != processlock.DefaultPath {
		t.Fatalf("LockPath = %q, want %q", deps.LockPath, processlock.DefaultPath)
	}
}

// TestSeparateTreesUseDistinctPathsIndependently pins the F-04 injection
// boundary: two CLI trees built from separately constructed Dependencies run
// against their own Git and secrets directories, and neither tree observes the
// other's state. `git set` and `secret set` in tree A must leave tree B with
// no configured source and no stored secret.
func TestSeparateTreesUseDistinctPathsIndependently(t *testing.T) {
	depsA := testDeps(t)
	depsB := testDeps(t)
	if depsA.GitConfigPath == depsB.GitConfigPath ||
		depsA.SecretsDir == depsB.SecretsDir {
		t.Fatal("testDeps must give separately constructed trees distinct injected paths")
	}

	// Tree A: configure a git source and a secret.
	if _, _, err := runCLIWithDeps(t, depsA, "", "git", "set", "git@github.com:acme/repo.git"); err != nil {
		t.Fatalf("tree A git set: %v", err)
	}
	if _, _, err := runCLIWithDeps(t, depsA, "value-a\n", "secret", "set", "shared-name", "-"); err != nil {
		t.Fatalf("tree A secret set: %v", err)
	}

	// Tree B: the same names resolve to nothing under B's own dirs.
	out, _, err := runCLIWithDeps(t, depsB, "", "git", "status")
	if err != nil {
		t.Fatalf("tree B git status: %v", err)
	}
	if !strings.Contains(out, "No git source configured.") {
		t.Fatalf("tree B saw tree A's git config:\n%s", out)
	}
	out, _, err = runCLIWithDeps(t, depsB, "", "secret", "ls")
	if err != nil {
		t.Fatalf("tree B secret ls: %v", err)
	}
	if strings.Contains(out, "shared-name") {
		t.Fatalf("tree B saw tree A's secret:\n%s", out)
	}

	// B's filesystem state is real isolation, not just presentation.
	if _, err := os.Stat(filepath.Dir(depsB.GitConfigPath)); !os.IsNotExist(err) {
		t.Fatalf("tree B wrote/observed a git config dir: %v", err)
	}
	if _, err := os.Stat(depsB.SecretsDir); !os.IsNotExist(err) {
		t.Fatalf("tree B wrote/observed a secrets dir: %v", err)
	}

	// And the reverse: configuring B does not change A.
	if _, _, err := runCLIWithDeps(t, depsB, "", "git", "set", "git@github.com:acme/other.git"); err != nil {
		t.Fatalf("tree B git set: %v", err)
	}
	cfg, err := git.LoadConfig(depsA.GitConfigPath)
	if err != nil || cfg.Repository != "git@github.com:acme/repo.git" {
		t.Fatalf("tree A config changed by tree B: cfg=%+v err=%v", cfg, err)
	}
}

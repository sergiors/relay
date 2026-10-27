package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"
)

// appListExitCode extracts the process exit code urfave's cli.Exit carries, so
// alias tests can assert the usage-error exit code (2) matches `app ls`, not
// only the message. A non-ExitCoder error reports -1.
func appListExitCode(err error) int {
	var ec cli.ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return -1
}

// TestAppsAliasErrorsMatchAppLs compares the alias's argument error against the
// canonical command's error verbatim, so a change to one is caught by the other.
func TestAppsAliasErrorsMatchAppLs(t *testing.T) {
	deps := testDeps(t)

	_, _, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls", "extra")
	_, _, aliasErr := runCLIWithDeps(t, deps, "", "apps", "extra")
	if canonicalErr == nil || aliasErr == nil {
		t.Fatalf("want both errors, got app ls=%v apps=%v", canonicalErr, aliasErr)
	}
	if aliasErr.Error() != canonicalErr.Error() {
		t.Fatalf("error differs:\n apps:    %q\n app ls: %q", aliasErr, canonicalErr)
	}
	if want := 2; appListExitCode(aliasErr) != want {
		t.Fatalf("apps exit code = %d, want %d", appListExitCode(aliasErr), want)
	}
}

// TestAppsAliasRejectsArguments pins that the alias is not a namespace: every
// positional token is a usage error with the same message and exit code as
// `app ls extra`, and in particular `apps inspect` / `apps invoke` do NOT
// resolve to `relay app` subcommands. The shared argument guard rejects before
// the state lookup, so the default database-free fixture suffices.
func TestAppsAliasRejectsArguments(t *testing.T) {
	deps := testDeps(t)

	canonicalOut, _, canonicalErr := runCLIWithDeps(t, deps, "", "app", "ls", "extra")
	if canonicalErr == nil {
		t.Fatal("app ls extra: err = nil, want usage error")
	}

	for _, args := range [][]string{
		{"apps", "extra"},
		{"apps", "inspect"},
		{"apps", "invoke"},
		{"apps", "inspect", "alpha-app"},
		{"apps", "invoke", "alpha-app"},
	} {
		out, _, err := runCLIWithDeps(t, deps, "", args...)
		if err == nil {
			t.Fatalf("%v: err = nil, want usage error", args)
		}
		if err.Error() != canonicalErr.Error() {
			t.Fatalf("%v: err = %q, want %q (same as app ls)", args, err, canonicalErr)
		}
		if got := appListExitCode(err); got != appListExitCode(canonicalErr) {
			t.Fatalf("%v: exit code = %d, want %d", args, got, appListExitCode(canonicalErr))
		}
		if out != canonicalOut {
			t.Fatalf("%v: stdout = %q, want %q", args, out, canonicalOut)
		}
	}
}

// TestAppsHelpDescribesList pins that the alias's own top-level help makes its
// purpose clear: it lists apps and is the short form of `relay app ls`.
func TestAppsHelpDescribesList(t *testing.T) {
	out, _, err := runCLI(t, "", "apps", "--help")
	if err != nil {
		t.Fatalf("apps --help: err = %v, want nil", err)
	}
	for _, want := range []string{"List apps", "relay app ls"} {
		if !strings.Contains(out, want) {
			t.Fatalf("apps help missing %q:\n%s", want, out)
		}
	}
}

// TestRootHelpListsApps pins that the top-level help advertises the short form
// alongside the canonical app group.
func TestRootHelpListsApps(t *testing.T) {
	out, _, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatalf("--help: err = %v, want nil", err)
	}
	cmds := out
	if i := strings.Index(cmds, "COMMANDS:"); i >= 0 {
		cmds = cmds[i:]
	}
	if !strings.Contains(cmds, "apps") {
		t.Fatalf("COMMANDS block must list the apps alias:\n%s", out)
	}
}

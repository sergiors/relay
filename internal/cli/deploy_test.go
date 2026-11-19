package cli

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const deployTestSHA = "0123456789abcdef0123456789abcdef01234567"

// deployHelpSubcommands extracts the subcommand names listed in the COMMANDS
// block of deploy help output, so tests can pin the exact public set rather
// than spot-checking a few names. The block ends at the next unindented
// heading (OPTIONS).
func deployHelpSubcommands(t *testing.T, help string) []string {
	t.Helper()
	idx := strings.Index(help, "COMMANDS:")
	if idx < 0 {
		t.Fatalf("deploy help missing a COMMANDS section:\n%s", help)
	}
	var names []string
	for _, line := range strings.Split(help[idx+len("COMMANDS:"):], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			break
		}
		names = append(names, strings.Fields(line)[0])
	}
	return names
}

// TestDeployHelpListsOnlyPublicSubcommands pins the simplified command surface
// and its operator-facing help: the COMMANDS block lists exactly status,
// rollback, pin, and unpin — no publish entry and no alias wording (bare
// `relay deploy` is a namespace, not a status alias) — each subcommand
// explains itself in user terms — and no deploy help text leaks implementation
// details (PostgreSQL/DATABASE_URL, internal CAS, branch-tip, publication, or
// materialization wording).
func TestDeployHelpListsOnlyPublicSubcommands(t *testing.T) {
	help := make(map[string]string)
	for _, args := range [][]string{
		{"deploy", "--help"},
		{"deploy", "rollback", "--help"},
		{"deploy", "pin", "--help"},
		{"deploy", "unpin", "--help"},
		{"deploy", "status", "--help"},
	} {
		out, _, err := runCLI(t, "", args...)
		if err != nil {
			t.Fatalf("%v: err = %v, want nil", args, err)
		}
		label := strings.Join(args, " ")
		help[label] = out
		assertNoImplementationDetails(t, label, out)
	}

	// The COMMANDS block is exactly the four public subcommands: no publish,
	// nothing extra, nothing missing.
	wantCmds := []string{"rollback", "pin", "unpin", "status"}
	gotCmds := deployHelpSubcommands(t, help["deploy --help"])
	if len(gotCmds) != len(wantCmds) {
		t.Fatalf("deploy help subcommands = %v, want exactly %v", gotCmds, wantCmds)
	}
	for _, name := range wantCmds {
		if !slices.Contains(gotCmds, name) {
			t.Fatalf("deploy help missing subcommand %q (got %v)", name, gotCmds)
		}
	}

	// Each public subcommand is described with a concise, user-facing
	// one-liner in the command list.
	for _, want := range []string{
		"Select a commit from the configured repository and pin it",
		"Block Git updates to the current revision",
		"Allow future Git updates",
		"Show the desired revision and node/app status",
	} {
		if !strings.Contains(help["deploy --help"], want) {
			t.Fatalf("deploy help missing %q:\n%s", want, help["deploy --help"])
		}
	}

	// Each subcommand's own help spells out the operator-facing contract.
	for cmd, wants := range map[string][]string{
		"deploy rollback --help": {
			"Select a full commit SHA from the configured repository and pin it",
			"Run 'relay deploy unpin' to allow future Git updates",
		},
		"deploy pin --help": {
			"Block 'relay git sync' and webhook updates from changing the current revision",
		},
		"deploy unpin --help": {
			"Allow a future 'relay git sync' or webhook update to change the revision",
			"does not immediately change the current revision",
		},
		"deploy status --help": {
			"desired generation and commit, pin state",
			"each node's current revision and app status",
		},
	} {
		for _, want := range wants {
			if !strings.Contains(help[cmd], want) {
				t.Fatalf("%s missing %q:\n%s", cmd, want, help[cmd])
			}
		}
	}

	// No indented COMMANDS entry lists "publish" as a subcommand, and no help
	// presents `relay deploy` itself as an alias of another command: it is a
	// namespace, not a shorthand.
	for label, out := range help {
		if regexp.MustCompile(`(?m)^\s+publish\s`).MatchString(out) {
			t.Fatalf("%s still lists a publish subcommand:\n%s", label, out)
		}
		if regexp.MustCompile(`(?i)alias`).MatchString(out) {
			t.Fatalf("%s still documents an alias:\n%s", label, out)
		}
	}
}

// TestDeployBareShowsNamespaceHelp pins that bare `relay deploy` is a plain
// namespace invocation: it succeeds with the same help surface as
// `relay deploy --help` and never reaches OpenState — the database-free
// fixture fails loudly on any state access — while the explicit
// `relay deploy status` still hits the injected state-access sentinel.
func TestDeployBareShowsNamespaceHelp(t *testing.T) {
	bareOut, _, bareErr := runCLI(t, "", "deploy")
	if bareErr != nil {
		t.Fatalf("bare deploy err = %v, want nil (subcommand help)", bareErr)
	}
	helpOut, _, helpErr := runCLI(t, "", "deploy", "--help")
	if helpErr != nil {
		t.Fatalf("deploy --help err = %v, want nil", helpErr)
	}
	if bareOut != helpOut {
		t.Fatalf("bare deploy output differs from deploy --help:\nbare:\n%s\nhelp:\n%s",
			bareOut, helpOut)
	}
	if !strings.Contains(bareOut, "COMMANDS:") {
		t.Fatalf("bare deploy output missing the COMMANDS section:\n%s", bareOut)
	}

	_, _, statusErr := runCLI(t, "", "deploy", "status")
	if !errors.Is(statusErr, errUnexpectedStateAccess) {
		t.Fatalf("deploy status err = %v, want the state-access path", statusErr)
	}
}

// TestDeployRemovedOperandsAreUnknownCommands pins that the removed surface is
// gone: the `<sha>` shorthand and `publish` (bare or with a SHA) are rejected
// by the shared namespace action as Docker-style unknown-command usage errors
// with exit code 2 — never a publication and never the old arity message — and
// none of them reaches OpenState.
func TestDeployRemovedOperandsAreUnknownCommands(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"shorthand", []string{"deploy", deployTestSHA}},
		{"publish bare", []string{"deploy", "publish"}},
		{"publish with sha", []string{"deploy", "publish", deployTestSHA}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runCLI(t, "", tc.args...)
			want := "relay: unknown command: relay deploy " + strings.Join(tc.args[1:], " ") +
				"\n\nUsage: relay deploy\n\nRun 'relay deploy --help' for more information"
			requireExitCode2(t, err, want)
			if errors.Is(err, errUnexpectedStateAccess) {
				t.Fatalf("%v reached the state database: %v", tc.args, err)
			}
		})
	}
}

// TestDeployRollbackRejectsInvalidArgumentsAndFlags pins rollback's strict
// contract: it takes exactly one exact SHA and carries NO configuration
// override flags — repository, ref, and path come only from `relay git set`.
// Every violation fails before any state access.
func TestDeployRollbackRejectsInvalidArgumentsAndFlags(t *testing.T) {
	t.Run("missing commit", func(t *testing.T) {
		_, _, err := runCLI(t, "", "deploy", "rollback")
		if err == nil || errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("err = %v, want an arity error without state access", err)
		}
	})
	t.Run("extra argument", func(t *testing.T) {
		_, _, err := runCLI(t, "", "deploy", "rollback", deployTestSHA, "extra")
		if err == nil || errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("err = %v, want an arity error without state access", err)
		}
	})
	t.Run("non-exact SHA", func(t *testing.T) {
		_, _, err := runCLI(t, "", "deploy", "rollback", "deadbeef")
		if err == nil || !strings.Contains(err.Error(), "full 40-character hexadecimal SHA") {
			t.Fatalf("err = %v, want the exact-SHA validation error", err)
		}
		if errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("rollback reached the state database before validating: %v", err)
		}
	})
	t.Run("no configured git source", func(t *testing.T) {
		_, _, err := runCLI(t, "", "deploy", "rollback", deployTestSHA)
		if err == nil || !strings.Contains(err.Error(), "a configured git source is required") {
			t.Fatalf("err = %v, want the missing-git-source error", err)
		}
		if errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("rollback reached the state database before the git source check: %v", err)
		}
	})
	for _, flag := range []string{"--repository", "--ref", "--path", "--pinned"} {
		t.Run("rejected flag "+flag, func(t *testing.T) {
			_, _, err := runCLI(t, "", "deploy", "rollback", deployTestSHA, flag, "value")
			if err == nil {
				t.Fatal("err = nil, want a rejected unknown flag")
			}
			if errors.Is(err, errUnexpectedStateAccess) {
				t.Fatalf("flag %s reached the state database: %v", flag, err)
			}
		})
	}
	t.Run("rejected flag on bare deploy", func(t *testing.T) {
		_, _, err := runCLI(t, "", "deploy", "--pinned")
		if err == nil {
			t.Fatal("err = nil, want a rejected unknown flag")
		}
		if errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("flag --pinned reached the state database: %v", err)
		}
	})
}

// TestDeployPinUnpinStatusRequireState pins that the remaining state-backed
// commands go through the injected OpenState seam exactly as the database-free
// fixture documents.
func TestDeployPinUnpinStatusRequireState(t *testing.T) {
	for _, args := range [][]string{
		{"deploy", "pin"},
		{"deploy", "unpin"},
		{"deploy", "status"},
	} {
		_, _, err := runCLI(t, "", args...)
		if !errors.Is(err, errUnexpectedStateAccess) {
			t.Fatalf("%v: err = %v, want the fixture's state-access error", args, err)
		}
	}
}

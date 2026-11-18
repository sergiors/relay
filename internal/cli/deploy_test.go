package cli

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

const deployTestSHA = "0123456789abcdef0123456789abcdef01234567"

// TestDeployHelpListsOnlyPublicCommands pins the simplified command surface and
// its operator-facing help: bare `relay deploy` documents itself as the status
// alias, the only public subcommands are status, rollback, pin, and unpin (no
// publish entry remains in the command list), each subcommand explains itself
// in user terms — and no deploy help text leaks implementation details
// (PostgreSQL/DATABASE_URL, internal CAS, branch-tip, publication, or
// materialization wording).
func TestDeployHelpListsOnlyPublicCommands(t *testing.T) {
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

	// The bare command presents itself as the status alias and describes each
	// public subcommand with a concise, user-facing one-liner.
	for _, want := range []string{
		"Alias for 'relay deploy status'",
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

	// No indented COMMANDS entry lists "publish" as a subcommand.
	if regexp.MustCompile(`(?m)^\s+publish\s`).MatchString(help["deploy --help"]) {
		t.Fatalf("deploy help still lists a publish subcommand:\n%s", help["deploy --help"])
	}
}

// TestDeployBareIsStatusAlias pins that bare `relay deploy` reaches exactly the
// status path: with the database-free fixture both forms fail on the same
// state-access sentinel, proving the alias performs no other work.
func TestDeployBareIsStatusAlias(t *testing.T) {
	_, _, bareErr := runCLI(t, "", "deploy")
	_, _, statusErr := runCLI(t, "", "deploy", "status")
	if !errors.Is(bareErr, errUnexpectedStateAccess) {
		t.Fatalf("bare deploy err = %v, want the status state-access path", bareErr)
	}
	if !errors.Is(statusErr, errUnexpectedStateAccess) {
		t.Fatalf("deploy status err = %v, want the state-access path", statusErr)
	}
}

// TestDeployObsoleteShorthandAndPublishRejected pins that the removed surface
// is gone: the `<sha>` shorthand is a usage error, and `publish` (subcommand
// or bare) is an unknown command — never a publication.
func TestDeployObsoleteShorthandAndPublishRejected(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"shorthand", []string{"deploy", deployTestSHA},
			"deploy: too many arguments (use 'relay deploy status')"},
		// With no publish subcommand, the operands fall through to the bare
		// deploy action, which refuses any argument.
		{"publish subcommand", []string{"deploy", "publish", deployTestSHA},
			"deploy: too many arguments (use 'relay deploy status')"},
		{"publish bare", []string{"deploy", "publish"},
			"deploy: too many arguments (use 'relay deploy status')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := runCLI(t, "", tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%v: err = %v, want %q", tc.args, err, tc.want)
			}
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

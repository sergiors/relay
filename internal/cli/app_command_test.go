// This file holds the state-free `relay app ...` argument/usage guards in the
// default (untagged) suite: each path returns its error before deps.OpenState
// runs, so testDeps suffices and unexpected state access fails loudly. The
// persisted-state error paths live in the integration-tagged files.

package cli

import (
	"strings"
	"testing"
)

// TestOldFunctionCommandIsUnknown pins that `relay function` is NOT a command
// (no backward-compatible alias): it must fail as an unknown command rather than
// dispatch to the renamed app group. The rejection happens at the root dispatch,
// before any command action, so no database is needed.
func TestOldFunctionCommandIsUnknown(t *testing.T) {
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "function", "ls")
	if err == nil {
		t.Fatal("relay function ls: err = nil, want an unknown-command error")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("relay function ls: err = %v, want an unknown-command error", err)
	}
}

// Arg handling: usage errors are returned with their messages (cmd/main.go
// prints them and exits 1). `app` alone no longer errors — it shows help (see
// TestAppBareShowsHelp). An unknown token and missing/extra arguments still
// return errors carrying the expected message. The unknown-app persisted-state
// lookup stays in the integration suite (TestAppInspectUnknownMessage).
func TestAppCommandErrors(t *testing.T) {
	deps := testDeps(t)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"app", "bogus"}, "unknown command: relay app bogus"},
		{[]string{"app", "ls", "extra"}, "app ls: too many arguments"},
		{[]string{"app", "inspect"}, "not provided"},
	} {
		_, _, err := runCLIWithDeps(t, deps, "", tc.args...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args %v: err = %v, want message containing %q", tc.args, err, tc.want)
		}
	}
}

// `relay app inspect name extra` is a usage error (exit 2). The arity guard
// fires before the state lookup, so the database-free fixture suffices.
func TestAppInspectTooManyArgs(t *testing.T) {
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "app", "inspect", "user-events-python", "extra")
	if err == nil || !strings.Contains(err.Error(), "app inspect: too many arguments") {
		t.Fatalf("returned error missing usage error: %v", err)
	}
}

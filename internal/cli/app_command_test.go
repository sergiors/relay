package cli

import (
	"strings"
	"testing"
)

// TestAppCommandIsNamedApp pins the breaking CLI rename: the application-scoped
// management group is `relay app`, and the old `relay function` name is gone
// with no alias. A stale alias would silently keep the pre-refactor surface.
func TestAppCommandIsNamedApp(t *testing.T) {
	out, _, err := runCLI(t, "", "--help")
	if err != nil {
		t.Fatalf("root help: err = %v, want nil", err)
	}
	cmds := out
	if i := strings.Index(cmds, "COMMANDS:"); i >= 0 {
		cmds = cmds[i:]
	}
	if !strings.Contains(cmds, "app") {
		t.Fatalf("COMMANDS block must list the app command:\n%s", out)
	}
	if strings.Contains(cmds, "function") {
		t.Fatalf("COMMANDS block must not list a function command:\n%s", out)
	}
}

// TestOldFunctionCommandIsUnknown pins that `relay function` is NOT a command
// (no backward-compatible alias): it must fail as an unknown command rather than
// dispatch to the renamed app group.
func TestOldFunctionCommandIsUnknown(t *testing.T) {
	_, deps := seedTestState(t)
	_, _, err := runCLIWithDeps(t, deps, "", "function", "ls")
	if err == nil {
		t.Fatal("relay function ls: err = nil, want an unknown-command error")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("relay function ls: err = %v, want an unknown-command error", err)
	}
}

package cli

import (
	"strings"
	"testing"
)

// TestHumanAge pins Go-duration formatting, including zero rendered as "0s".
func TestHumanAge(t *testing.T) {
	for _, tc := range []struct {
		in   int64
		want string
	}{
		{0, "0s"},
		{134, "2m14s"},
		{3723, "1h2m3s"},
	} {
		if got := humanAge(tc.in); got != tc.want {
			t.Errorf("humanAge(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestStatsUnknownTokenFallsThroughToParentGuard pins that a junk token under
// `stats` is NOT swallowed by the subcommand list: with no matching subcommand,
// urfave runs the parent Action, whose too-many-arguments guard preserves the
// previous `stats: too many arguments` semantics. The guard fires before the
// state lookup, so the default database-free fixture suffices.
func TestStatsUnknownTokenFallsThroughToParentGuard(t *testing.T) {
	_, _, err := runCLIWithDeps(t, testDeps(t), "", "stats", "asdsa")
	if err == nil || !strings.Contains(err.Error(), "stats: too many arguments") {
		t.Fatalf("stats asdsa: err = %v, want parent guard", err)
	}
}

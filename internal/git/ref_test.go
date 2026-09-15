package git

import (
	"strings"
	"testing"
)

// TestMatchPushedRef pins the webhook pushed-ref/configured-ref matching
// semantics now centralized in the git package (moved from the webhook package's
// refMatches). It is the single source of truth for how a GitHub push payload
// (fully-qualified refs like refs/heads/main) lines up with a bare
// branch/tag/commit-hash configured ref.
func TestMatchPushedRef(t *testing.T) {
	const sha = "abcdef0123456789abcdef0123456789abcdef01"
	cases := []struct {
		name, pushed, cfg string
		want              bool
	}{
		// Branch and tag refs.
		{"branch", "refs/heads/main", "main", true},
		{"tag", "refs/tags/v1", "v1", true},
		{"bare equality", "main", "main", true},
		{"non-matching branch", "refs/heads/develop", "main", false},
		// A branch named like a tag matches the configured tag (pinned as the
		// current behavior: the pushed ref is classified by namespace, and its
		// short name equals the configured name either way).
		{"branch/tag cross", "refs/heads/v1", "v1", true},

		// Full commit SHAs (40 hex).
		{"full commit SHA", sha, sha, true},
		{"full SHA case-insensitive", sha, strings.ToUpper(sha), true},
		// Abbreviated (4-63 hex) commit SHAs.
		{"abbreviated commit SHA", "dead", "DEAD", true},
		{"abbreviated 12-char prefix", sha[:12], sha[:12], true},
		{"abbreviated SHA non-match", "deadbeee", "DEADBEEF", false},
		// A branch ref must NOT match a hash configured ref: a full branch ref
		// like refs/heads/1234abcd does not equal the hash 1234ABCD.
		{"pushed branch ref vs hash cfg", "refs/heads/1234abcd", "1234ABCD", false},

		// Invalid / non-matching inputs.
		{"empty cfg", "refs/heads/main", "", false},
		{"empty pushed", "", "main", false},
		{"remote-tracking ref", "refs/remotes/origin/main", "main", false},
		{"whitespace pushed", "  main", "main", false},
		{"whitespace cfg", "refs/heads/main", "  main", false},

		// A full SHA spelled like a branch name: the hash cfg wins the EqualFold
		// comparison only when the pushed value is exactly the (case-insensitive)
		// hash — not when it is a fully-qualified branch ref sharing that spelling.
		{"SHA-branch spelling", "refs/heads/" + sha, sha, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := MatchPushedRef(c.pushed, c.cfg); got != c.want {
				t.Fatalf("MatchPushedRef(%q, %q) = %v, want %v", c.pushed, c.cfg, got, c.want)
			}
		})
	}
}

// TestIsHashLike pins the hash-likeness heuristic gate. It accepts 4-64 hex
// chars (full 40-hex and abbreviated 4-63 prefixes). The floor of 4 matches
// go-git's hash-prefix resolution floor, so a hex-looking value within SHA
// length resolves as a hash when the object exists and otherwise falls through
// to branch/tag resolution at sync time (see resolveRef) — this is a gate, not
// a verdict.
func TestIsHashLike(t *testing.T) {
	cases := []struct {
		name string
		ref  string
		want bool
	}{
		{"full 40-hex", "abcdef0123456789abcdef0123456789abcdef01", true},
		{"12-char prefix", "abcdef012345", true},
		{"4-char dead", "dead", true},
		{"uppercase hex", "DEADBEEF", true},
		{"64-hex", strings.Repeat("a", 64), true},

		{"3-char", "dea", false},
		{"65-hex", strings.Repeat("a", 65), false},
		{"main", "main", false},
		{"empty", "", false},
		{"invalid char", "abc1!", false},
		{"space", "dead beef", false},
		{"mixed hex", "abcg", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsHashLike(c.ref); got != c.want {
				t.Fatalf("IsHashLike(%q) = %v, want %v", c.ref, got, c.want)
			}
		})
	}
}

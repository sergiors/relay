package state

import "testing"

// TestNormalizeDeploymentCommit pins the exact immutable-SHA contract: a
// coordinated revision must be a full 40-character object name, is trimmed of
// surrounding whitespace, and is canonicalized to lowercase so every node and
// every comparison sees one spelling. Mutable refs and ambiguous abbreviations
// are rejected.
func TestNormalizeDeploymentCommit(t *testing.T) {
	const sha = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{"lowercase sha", sha, sha},
		{"uppercase sha lowercased", "0123456789ABCDEF0123456789ABCDEF01234567", sha},
		{"surrounded by whitespace", "  " + sha + "\n", sha},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeDeploymentCommit(tc.raw)
			if err != nil {
				t.Fatalf("NormalizeDeploymentCommit(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeDeploymentCommit(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}

	for _, raw := range []string{
		"",
		"main",
		"deadbeef", // abbreviated
		sha[:39],   // one short
		sha + "a",  // one long
		"0123456789abcdef0123456789abcdef012345g", // non-hex
	} {
		if got, err := NormalizeDeploymentCommit(raw); err == nil {
			t.Fatalf("NormalizeDeploymentCommit(%q) = %q, want an error", raw, got)
		}
	}
}

// TestDeploymentAutomaticOrderingAllowsBranchAnchorPolicy pins the safe
// automatic-publication policy itself. Acceptance is decided ONLY by the
// persisted branch-tip anchor:
//   - a before SHA equal to the anchor (case-insensitively) is the proven
//     chain and is accepted;
//   - a missing anchor or a missing before SHA is ambiguous (rejected);
//   - any other before SHA is stale (rejected) REGARDLESS of the candidate's
//     commit timestamp — a future timestamp can never override the mismatch;
//   - the candidate revision's own fields carry no ordering authority.
func TestDeploymentAutomaticOrderingAllowsBranchAnchorPolicy(t *testing.T) {
	const (
		anchor    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		candidate = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		future    = "2026-06-01T10:00:00Z"
	)
	ordering := func(sourceTime string) DeploymentDesired {
		return DeploymentDesired{Commit: candidate, SourceTime: sourceTime}
	}

	tests := []struct {
		name           string
		anchor         string
		desired        DeploymentDesired
		previousCommit string
		wantAllowed    bool
		wantAmbiguous  bool
	}{
		{
			name:           "matching before chains from the anchor",
			anchor:         anchor,
			desired:        ordering(future),
			previousCommit: anchor,
			wantAllowed:    true,
		},
		{
			name:           "anchor chain matches case-insensitively",
			anchor:         anchor,
			desired:        ordering(future),
			previousCommit: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			wantAllowed:    true,
		},
		{
			name:           "a future timestamp does not rescue a mismatched before",
			anchor:         anchor,
			desired:        ordering(future),
			previousCommit: candidate,
			wantAllowed:    false,
			wantAmbiguous:  false,
		},
		{
			name:           "an empty before SHA is ambiguous even with a future timestamp",
			anchor:         anchor,
			desired:        ordering(future),
			previousCommit: "",
			wantAllowed:    false,
			wantAmbiguous:  true,
		},
		{
			name:           "a missing anchor is ambiguous regardless of the chain offered",
			anchor:         "",
			desired:        ordering(future),
			previousCommit: candidate,
			wantAllowed:    false,
			wantAmbiguous:  true,
		},
		{
			name:           "an empty before SHA against a missing anchor is ambiguous",
			anchor:         "",
			desired:        ordering(""),
			previousCommit: "",
			wantAllowed:    false,
			wantAmbiguous:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			current := DeploymentDesired{Commit: anchor, BranchTip: tc.anchor}
			allowed, ambiguous := deploymentAutomaticOrderingAllows(
				current, tc.desired, tc.previousCommit)
			if allowed != tc.wantAllowed || ambiguous != tc.wantAmbiguous {
				t.Fatalf("deploymentAutomaticOrderingAllows(...) = (%v, %v), want (%v, %v)",
					allowed, ambiguous, tc.wantAllowed, tc.wantAmbiguous)
			}
		})
	}
}

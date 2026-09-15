package git

import (
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
)

// IsHashLike reports whether ref could be a git commit SHA (full or
// abbreviated, 4–64 hex chars). It is the single shared classification used
// both when resolving a ref against an opened repository (resolveRef) and when
// matching a webhook pushed ref against a configured ref (MatchPushedRef).
//
// It exists alongside go-git's own capabilities for two reasons. First,
// plumbing.IsHash only accepts a FULL 40-hex SHA, whereas Relay must also
// classify abbreviated prefixes (4–63 hex) so a short hash resolves or matches
// exactly like a full one. Second, Repository.ResolveRevision resolves hash
// prefixes correctly but requires an OPEN repository; callers like the webhook
// ref matcher classify bare strings with no repository open, so the
// classification must be a pure string predicate.
//
// IsHashLike is deliberately a HEURISTIC GATE, not a verdict: a hex-looking
// value within SHA length is treated as a hash, it resolves to the object when
// that object exists, and otherwise falls through to branch/tag resolution
// (see resolveRef). The floor of 4 hex chars matches go-git's hash-prefix
// resolution range while rejecting the common case of a short all-hex branch or
// tag name (which is far more often a word).
func IsHashLike(ref string) bool {
	n := len(ref)
	if n < 4 || n > 64 {
		return false
	}
	for i := 0; i < n; i++ {
		switch {
		case ref[i] >= '0' && ref[i] <= '9':
		case ref[i] >= 'a' && ref[i] <= 'f':
		case ref[i] >= 'A' && ref[i] <= 'F':
		default:
			return false
		}
	}
	return true
}

// MatchPushedRef reports whether a pushed ref (GitHub reports fully-qualified
// refs like "refs/heads/main") corresponds to the configured ref. The
// configured ref comes from git.Config and is a bare branch/tag name (e.g.
// "main", "v1") or a commit hash (full or abbreviated).
//
// Semantics:
//   - An empty configuredRef never matches.
//   - When the configuredRef is a (possibly abbreviated) hash, the pushed ref
//     must match it case-insensitively (strings.EqualFold): hashes are
//     case-insensitive identifiers and GitHub reports SHAs lowercase. A full
//     branch ref like "refs/heads/1234abcd" does NOT equal the hash and so must
//     not match.
//   - Otherwise the pushed ref is classified with plumbing.ReferenceName: if it
//     is a branch (refs/heads/...) or a tag (refs/tags/...), it matches when its
//     short name equals the configured ref; a bare ref matches only by exact
//     string equality.
//
// Classification uses plumbing.ReferenceName rather than hand-concatenated
// prefixes: ReferenceName.Short strips exactly one refs/heads/ or refs/tags/
// prefix and leaves bare names unchanged, and IsBranch/IsTag decide which
// namespace a pushed ref lives in. This is behaviorally equivalent to the old
// string rule (pushed == "refs/heads/"+cfg || pushed == cfg || pushed ==
// "refs/tags/"+cfg) for every input, including edge cases like "refs/heads/v1"
// vs a configured tag "v1" (a match in both) and "refs/remotes/origin/main" vs
// "main" (a non-match in both, because a remote-tracking ref is neither a
// branch nor a tag and so falls to exact equality).
func MatchPushedRef(pushedRef, configuredRef string) bool {
	if configuredRef == "" {
		return false
	}
	if IsHashLike(configuredRef) {
		return strings.EqualFold(pushedRef, configuredRef)
	}
	rn := plumbing.ReferenceName(pushedRef)
	if rn.IsBranch() || rn.IsTag() {
		return rn.Short() == configuredRef
	}
	return pushedRef == configuredRef
}

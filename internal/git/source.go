package git

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
)

// validateRef reports whether ref is a plausible git revision string for the
// config. It enforces the minimal safety/production rules: non-empty and free of
// whitespace. No leading '-' (would be misread as a flag) is enforced here too.
// We deliberately do not fully validate the ref's resolvability — that only
// happens against a real checkout at sync time — so branch names, tags, and
// commit hashes are all accepted as long as they are clean strings.
func validateRef(ref string) error {
	if ref == "" {
		return fmt.Errorf("git: ref must not be empty")
	}
	if strings.ContainsAny(ref, " \t\n\r") {
		return fmt.Errorf("git: ref %q must not contain whitespace", ref)
	}
	if strings.HasPrefix(ref, "-") {
		return fmt.Errorf("git: ref %q must not start with '-'", ref)
	}
	return nil
}

// ValidateRepositoryURL reports whether repository is a plausible git SSH URL
// (either scp-like "git@host:org/repo.git" or "ssh://git@host/org/repo.git").
// It rejects empty strings and http/https/file URLs: this iteration authenticates
// over SSH only. Parsing goes through go-git's NewEndpoint, which correctly maps
// scp-like URLs to protocol "ssh"; anything other than "ssh" is rejected.
func ValidateRepositoryURL(repository string) error {
	if strings.TrimSpace(repository) == "" {
		return fmt.Errorf("git: repository URL must not be empty")
	}
	ep, err := parseEndpoint(repository)
	if err != nil {
		return fmt.Errorf("git: invalid repository URL: %w", err)
	}
	if ep.Protocol != "ssh" {
		return fmt.Errorf(
			"git: repository must use SSH (url %q resolves to protocol %q); only SSH URLs are supported",
			repository, ep.Protocol,
		)
	}
	if ep.Host == "" {
		return fmt.Errorf("git: repository %q has no host", repository)
	}
	return nil
}

// parseEndpoint is a small indirection over transport.NewEndpoint so tests can
// pin the URL classifier without re-testing go-git's parser. It only fails on a
// syntactically invalid endpoint.
func parseEndpoint(repository string) (*transport.Endpoint, error) {
	return transport.NewEndpoint(repository)
}

// validatePath rejects a monorepo Path that could escape the checkout root. The
// rules are strict and simple: it must be empty (repo root) or a clean relative
// path that stays inside the root — no absolute path, no ".." traversal, no
// backslashes on posix, no path that resolves outside, and it must not reduce to
// "." (an empty path is the explicit repo-root spelling; "." adds nothing). The
// cleaned form is not returned; the caller stores the original string and the
// sync code re-resolves it against the checkout via a join+clean+prefix check.
func validatePath(path string) error {
	if path == "" {
		return nil
	}
	if strings.ContainsRune(path, '\\') {
		return fmt.Errorf("git: path %q must use forward slashes, not backslashes", path)
	}
	if filepath.IsAbs(path) {
		return fmt.Errorf("git: path %q must be relative to the repository root, not absolute", path)
	}
	clean := filepath.Clean(path)
	if clean == "." {
		return fmt.Errorf("git: path %q resolves to the repository root; leave path empty to use the root", path)
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("git: path %q must not escape the repository", path)
	}
	// Also reject a path that would normalize into a traversal from deeper
	// nesting, e.g. "a/../../b".
	if containsParentTraversal(clean) {
		return fmt.Errorf("git: path %q must not escape the repository", path)
	}
	return nil
}

// containsParentTraversal reports whether clean already contains a ".." element.
// filepath.Clean collapses ".." but only when it can; a residual ".." (e.g. from
// "a/../../b" collapsing to "../b") is caught here so it cannot resolve outside.
func containsParentTraversal(clean string) bool {
	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		if part == ".." {
			return true
		}
	}
	return false
}

// resolveSourceDir joins the checkout root and the validated monorepo path and
// reports whether the result stays inside root. It is the runtime counterpart of
// validatePath: even though validation runs at `git set` time, the file is
// operator-editable and sync re-verifies before reading anything. A path that
// escapes (or reduces to the root) returns an error so the invariant "source is
// strictly the repo/path, nothing else" is never violated.
func resolveSourceDir(root, path string) (string, error) {
	if path == "" {
		return root, nil
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	joined := filepath.Join(root, filepath.Clean(path))
	rel, err := filepath.Rel(root, joined)
	if err != nil {
		return "", fmt.Errorf("git: resolve source dir: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("git: source path %q escapes the checkout", path)
	}
	return joined, nil
}

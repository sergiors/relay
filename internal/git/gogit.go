package git

import (
	"context"
	"strings"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
)

// goGitOps is the production gitOps implementation wrapping go-git.
type goGitOps struct{}

// newGoGitOps returns the go-git-backed transport implementation.
func newGoGitOps() gitOps { return goGitOps{} }

// clone implements gitOps by delegating to go-git's PlainCloneContext. A default
// (ReferenceName empty) clone checks out the default branch; SingleBranch=false
// keeps every remote branch and tag so any ref can be resolved later. isBare is
// always false (we want a working tree).
func (goGitOps) clone(ctx context.Context, url, path string, auth gitssh.AuthMethod) error {
	_, err := git.PlainCloneContext(ctx, path, false, &git.CloneOptions{
		URL:          url,
		Auth:         auth,
		SingleBranch: false,
	})
	return err
}

// open implements gitOps by opening an existing repository at path.
func (goGitOps) open(path string) (gitRepo, error) {
	r, err := git.PlainOpen(path)
	if err != nil {
		return nil, err
	}
	return &goGitRepo{r: r}, nil
}

// goGitRepo adapts a *git.Repository to the gitRepo seam.
type goGitRepo struct {
	r *git.Repository
}

// fetch implements gitRepo by fetching remote "origin". Force + Prune drop refs
// that no longer exist remotely; AllTags pulls tags so tag refs resolve.
func (g *goGitRepo) fetch(ctx context.Context, url string, auth gitssh.AuthMethod) error {
	remote, err := g.r.Remote("origin")
	if err != nil {
		return err
	}
	return remote.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin",
		RemoteURL:  url,
		Auth:       auth,
		Force:      true,
		Prune:      true,
		Tags:       git.AllTags,
	})
}

// resolveRef resolves ref to a commit hash, making the freshly-fetched remote
// authoritative for branch names. This matters because go-git's RefRevParseRules
// (plumbing/reference.go) expand a bare `main` in this order:
//
//	%s, refs/%s, refs/tags/%s, refs/heads/%s, refs/remotes/%s, refs/remotes/%s/HEAD
//
// A clone ALWAYS creates a local refs/heads/main when main is the remote's default
// branch (the normal GitHub/GitLab case). After a fetch that advanced origin/main
// to a new commit, refs/heads/main still points at the OLD commit — so resolving
// the bare `main` first would silently materialize stale content. The remote
// tracking ref refs/remotes/origin/main is the fresh one, so it must win.
//
// Resolution order:
//
//  1. Exact/abbreviated hash: if ref looks like a commit SHA (4-64 hex chars),
//     resolve it directly via go-git's hash-prefix resolution and return
//     regardless of remote state. A 40-hex ref is a full SHA and must resolve to
//     itself even if a branch happens to share its spelling; an abbreviated
//     prefix resolves through the same hash-prefix lookup.
//  2. Fully-qualified ref: a refs/... prefixed value (branch, tag, remote
//     tracking) resolves directly, never against origin/.
//  3. Bare branch name (the default case, e.g. main): resolve
//     refs/remotes/origin/<ref> FIRST — the freshly fetched remote-tracking ref.
//     If that fails, fall back to refs/tags/<ref> (a bare string like `v1.2.0`
//     is a tag name; go-git would find it via refs/tags/%s, but we must NOT
//     resolve the bare name before the origin-tracking ref for branches). As a
//     last resort resolve the bare name itself — the ONLY case where local state
//     wins, and it only happens when no remote-tracking ref exists at all (e.g.
//     odd checkouts where a local branch exists but origin tracking does not).
func (g *goGitRepo) resolveRef(ref string) (plumbing.Hash, error) {
	if isHashLike(ref) {
		if h, err := g.resolveHash(ref); err == nil {
			return h, nil
		}
	}

	// Fully-qualified refs are exact: resolve them directly.
	if strings.HasPrefix(ref, "refs/") {
		if h, err := g.resolveRevision(plumbing.Revision(ref)); err == nil {
			return h, nil
		}
		return plumbing.ZeroHash, plumbing.ErrReferenceNotFound
	}

	// Bare branch name: the freshly fetched remote-tracking ref is authoritative.
	// Only after it fails do we fall back to a tag, then (last resort) the bare
	// name itself. See the function doc comment for the stale-local-trap WHY.
	if h, err := g.resolveRevision(plumbing.Revision("refs/remotes/origin/" + ref)); err == nil {
		return h, nil
	}
	if h, err := g.resolveRevision(plumbing.Revision("refs/tags/" + ref)); err == nil {
		return h, nil
	}
	if h, err := g.resolveRevision(plumbing.Revision(ref)); err == nil {
		return h, nil
	}

	return plumbing.ZeroHash, plumbing.ErrReferenceNotFound
}

// resolveRevision resolves a single, fully-expanded revision to a commit hash.
// ResolveRevision peels annotated tags to their commit (it calls TagObject.Commit
// when the hash is a tag object), so both lightweight and annotated tags land on
// the commit.
func (g *goGitRepo) resolveRevision(rev plumbing.Revision) (plumbing.Hash, error) {
	h, err := g.r.ResolveRevision(rev)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return *h, nil
}

// isHashLike reports whether ref could be a commit SHA (full or abbreviated).
// go-git resolves hash prefixes via resolveHashPrefix, which requires hex and a
// prefix of at least two hex chars; we accept 4-64 hex chars so a bare branch or
// tag name (which may be all-hex but is far more commonly a word) is not
// misclassified. A value that is all-hex and within SHA length is treated as a
// hash; it resolves to the SHA if that object exists, and otherwise falls through
// to the branch/tag/bare chain below.
func isHashLike(ref string) bool {
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

// resolveHash resolves a full or abbreviated hex SHA. For a full 40-hex SHA this
// resolves to itself; for an abbreviated prefix go-git's ResolveRevision runs
// resolveHashPrefix over the object store. It returns ErrReferenceNotFound when
// the hash does not exist so resolveRef can fall through to the bare-chain.
func (g *goGitRepo) resolveHash(ref string) (plumbing.Hash, error) {
	h, err := g.r.ResolveRevision(plumbing.Revision(ref))
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return *h, nil
}

// checkoutForce performs a hard-reset checkout to hash (detached HEAD). go-git's
// Worktree.Checkout sets Reset mode to HardReset when Force is true (verified
// against go-git source), which throws away local changes and puts the worktree
// exactly at hash. We never Pull: the remote is the source of truth and a merge
// state must never be produced.
func (g *goGitRepo) checkoutForce(hash plumbing.Hash) error {
	wt, err := g.r.Worktree()
	if err != nil {
		return err
	}
	return wt.Checkout(&git.CheckoutOptions{Hash: hash, Force: true})
}

// head returns the current HEAD commit hash. After our force checkout HEAD is
// detached, so Repository.Head returns the checked-out hash directly.
func (g *goGitRepo) head() (plumbing.Hash, error) {
	h, err := g.r.Head()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return h.Hash(), nil
}

// RemoteURL reads the origin remote's URL from the repository config. It returns
// ("", false) when there is no origin remote so ensureCheckout treats it as "no
// fixed source yet".
func (g *goGitRepo) RemoteURL() (string, bool) {
	cfg, err := g.r.Config()
	if err != nil {
		return "", false
	}
	rem, ok := cfg.Remotes["origin"]
	if !ok {
		return "", false
	}
	if len(rem.URLs) == 0 {
		return "", false
	}
	return rem.URLs[0], true
}

// ensure goGitRepo satisfies gitRepo.
var _ gitRepo = (*goGitRepo)(nil)

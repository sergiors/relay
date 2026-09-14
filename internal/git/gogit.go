package git

import (
	"context"

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

// resolveRef resolves ref to a commit hash. go-git's ResolveRevision expands
// bare branch names against refs/remotes/<ref> — which becomes refs/remotes/main,
// never refs/remotes/origin/main — so a configured bare branch name like "main"
// would fail after a clone even though origin/main exists. We therefore try the
// bare revision first (covers tags, hashes, and names that happen to fit go-git's
// rules) and fall back to the explicit origin-tracking form refs/remotes/origin/<ref>,
// which reliably resolves short branch names.
func (g *goGitRepo) resolveRef(ref string) (plumbing.Hash, error) {
	if h, err := g.r.ResolveRevision(plumbing.Revision(ref)); err == nil {
		return *h, nil
	}
	h, err := g.r.ResolveRevision(plumbing.Revision("refs/remotes/origin/" + ref))
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

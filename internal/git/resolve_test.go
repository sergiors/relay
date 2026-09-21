package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// fixtureDefaultMain returns the same fixture as fixture() but additionally
// points the bare remote's symbolic HEAD at refs/heads/main instead of the
// default refs/heads/master. This mirrors the common GitHub/GitLab default-branch
// checkout: a clone of this remote makes main the local checked-out branch and
// ALWAYS creates a local refs/heads/main. That local branch is exactly the trap
// that can make a bare `main` resolution prefer a stale local refs/heads/main
// over the freshly fetched refs/remotes/origin/main (see the resolveRef doc
// comment).
func fixtureDefaultMain(t *testing.T, withTag bool) testEnv {
	t.Helper()
	e := fixture(t, withTag)
	b, err := git.PlainOpen(e.bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	if err := b.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, "refs/heads/main")); err != nil {
		t.Fatalf("set bare HEAD -> refs/heads/main: %v", err)
	}
	return e
}

// resolveErr is a tiny wrapper so tests can assert a resolved hash equals a
// wanted value with a label.
func assertResolve(t *testing.T, g *goGitRepo, ref string, want plumbing.Hash, label string) plumbing.Hash {
	t.Helper()
	got, err := g.resolveRef(ref)
	if err != nil {
		t.Fatalf("resolveRef(%q) [%s]: %v", ref, label, err)
	}
	if got != want {
		t.Fatalf("resolveRef(%q) [%s] = %s, want %s", ref, label, got, want)
	}
	return got
}

// TestResolveRefPrefersRemoteBranchOverStaleLocal pins resolveRef's precedence
// directly at the unit level: in one checkout the local refs/heads/main
// deliberately points at an OLD commit while the freshly fetched
// refs/remotes/origin/main points at a NEW commit. resolveRef("main") must
// return the NEW hash — proving the remote-tracking ref beats a stale local
// branch (go-git's RefRevParseRules would otherwise let refs/heads/%s win for
// the bare name). It also pins that full hashes, abbreviated hash prefixes, and
// a tag name all resolve to their commit.
func TestResolveRefPrefersRemoteBranchOverStaleLocal(t *testing.T) {
	// Work repo with an initial commit on main (the old state), then a NEW one.
	e := fixtureDefaultMain(t, true)
	r, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	if err := wt.Checkout(&git.CheckoutOptions{
		Branch: plumbing.ReferenceName("refs/heads/main"), Force: true,
	}); err != nil {
		t.Fatalf("checkout main: %v", err)
	}
	oldHead, _ := r.Head()
	oldHash := oldHead.Hash()

	// A NEW commit with distinct content, pushed to the bare remote.
	writeFile(t, filepath.Join(e.work, "fn", "template.yaml"), "# NEW\n")
	if _, err := wt.Add("fn/template.yaml"); err != nil {
		t.Fatalf("add: %v", err)
	}
	commit(t, wt, "new commit on main")
	newHead, _ := r.Head()
	newHash := newHead.Hash()
	if newHash == oldHash {
		t.Fatalf("test setup: commit hash did not advance")
	}
	// Move the fixture's tag v1 forward to a distinct NEW commit too, so the tag
	// resolves to a knowable fresh target rather than the initial commit.
	mustSetRef(t, r, "refs/tags/v1", newHash)
	pushBranchs(t, e, "main")

	// Checkout a workspace copy of the remote's main, as sync does.
	co := filepath.Join(t.TempDir(), "co")
	if _, err := git.PlainClone(co, false, &git.CloneOptions{URL: e.bare, SingleBranch: false}); err != nil {
		t.Fatalf("clone checkout: %v", err)
	}
	gr, err := git.PlainOpen(co)
	if err != nil {
		t.Fatalf("open checkout: %v", err)
	}
	grepo := &goGitRepo{r: gr}

	// Deliberately rewind the LOCAL refs/heads/main to the OLD commit while the
	// remote-tracking refs/remotes/origin/main stays at NEW. This is the stale
	// default-branch trap: the clone created refs/heads/main at OLD and it never
	// advanced, while origin/main was freshly fetched to NEW.
	mustSetRef(t, gr, "refs/heads/main", oldHash)
	originRef, err := gr.Reference("refs/remotes/origin/main", false)
	if err != nil {
		t.Fatalf("read origin/main: %v", err)
	}
	if originRef.Hash() != newHash {
		t.Fatalf("test setup: origin/main = %s, want NEW %s", originRef.Hash(), newHash)
	}

	// Bare branch name must resolve to the NEW remote commit, not the OLD local.
	assertResolve(t, grepo, "main", newHash, "bare branch beats stale local")

	// A full 40-hex SHA resolves to itself regardless of remote state.
	assertResolve(t, grepo, newHash.String(), newHash, "full hash")

	// An abbreviated SHA prefix resolves to the same commit.
	assertResolve(t, grepo, newHash.String()[:12], newHash, "abbreviated hash prefix")

	// A tag name resolves to its tagged commit (here the NEW commit we moved it to).
	assertResolve(t, grepo, "v1", newHash, "tag name")
}

// TestSyncResolvesFreshRemoteBranch reproduces the exact user-reported scenario:
// a default-branch checkout (bare HEAD -> refs/heads/main, so the clone creates a
// local refs/heads/main). After a post-clone operator push advances the remote's
// main, a re-sync must materialize the NEW content — resolving main against the
// freshly fetched refs/remotes/origin/main, never the now-stale local
// refs/heads/main.
func TestSyncResolvesFreshRemoteBranch(t *testing.T) {
	e := fixtureDefaultMain(t, true)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}

	// First sync: clone + materialize the initial content.
	mustSync(t, e, cfg)
	b, err := os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}
	if strings.Contains(string(b), "# NEW") {
		t.Fatalf("initial sync already materialized NEW content: %q", string(b))
	}

	// Advance the work repo's main (changing template content) and push to the
	// bare remote, simulating an operator push. The checkout's local
	// refs/heads/main stays at the OLD commit.
	r, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	if err := wt.Checkout(&git.CheckoutOptions{
		Branch: plumbing.ReferenceName("refs/heads/main"), Force: true,
	}); err != nil {
		t.Fatalf("checkout main: %v", err)
	}
	writeFile(t, filepath.Join(e.work, "fn", "template.yaml"), "# NEW\nruntime: node24\n")
	if _, err := wt.Add("fn/template.yaml"); err != nil {
		t.Fatalf("add: %v", err)
	}
	commit(t, wt, "advance main")
	rHead, _ := r.Head()
	newHash := rHead.Hash()
	pushBranchs(t, e, "main")

	// Re-sync reusing the existing checkout. The materialized content must be the
	// NEW content and the config must record the NEW full commit hash.
	out := mustSync(t, e, cfg)
	// The summary's short hash is derived from HEAD; assert it matches NEW's prefix.
	if !strings.Contains(out.String(), newHash.String()[:7]) {
		t.Fatalf("summary %q does not pin the NEW short hash %s", out.String(), newHash.String()[:7])
	}
	b, err = os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template after re-sync: %v", err)
	}
	if !strings.Contains(string(b), "# NEW") {
		t.Fatalf("re-sync materialized stale content %q; want NEW content", string(b))
	}

	saved, err := LoadConfig(filepath.Join(e.gitDir, "source.json"))
	if err != nil {
		t.Fatalf("load persisted config: %v", err)
	}
	if saved.LastSyncedCommit != newHash.String() {
		t.Fatalf("LastSyncedCommit = %s, want NEW %s", saved.LastSyncedCommit, newHash)
	}
}

// TestResolveRefAnnotatedTagPeelsCommit pins that an annotated tag resolves to
// its underlying commit, not the tag object's own hash. ResolveRevision peels
// tag objects via TagObject.Commit, which is what makes refs/tags/<name> land on
// a commit sync can check out.
func TestResolveRefAnnotatedTagPeelsCommit(t *testing.T) {
	e := fixture(t, false)
	r, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work: %v", err)
	}
	head, _ := r.Head()
	commitHash := head.Hash()
	// Create an annotated tag (opts != nil) pointing at the commit.
	if _, err := r.CreateTag("anna", commitHash, &git.CreateTagOptions{
		Tagger:  &object.Signature{Name: "t", Email: "t@e", When: time.Now()},
		Message: "annotated",
	}); err != nil {
		t.Fatalf("create annotated tag: %v", err)
	}
	// Re-seed the bare remote so a fresh checkout fetch pulls the tag down.
	seedBare(t, e)

	co := filepath.Join(t.TempDir(), "co")
	if _, err := git.PlainClone(co, false, &git.CloneOptions{URL: e.bare, SingleBranch: false}); err != nil {
		t.Fatalf("clone checkout: %v", err)
	}
	gr, err := git.PlainOpen(co)
	if err != nil {
		t.Fatalf("open checkout: %v", err)
	}
	grepo := &goGitRepo{r: gr}
	assertResolve(t, grepo, "anna", commitHash, "annotated tag peels to commit")

	// Confirm the raw tag ref in the checkout points at a tag object, not the
	// commit directly — proving the peel (not a coincidental hash match) did work.
	tagRef, err := gr.Reference("refs/tags/anna", false)
	if err != nil {
		t.Fatalf("read refs/tags/anna: %v", err)
	}
	if tagRef.Hash() == commitHash {
		t.Fatalf("test setup: annotated tag ref hashes equal the commit; peel not exercised")
	}
}

// TestResolveRefBareNameFallback covers the last-resort path: when neither a
// remote-tracking ref nor a tag exists for a bare name, resolveRef falls back to
// resolving the bare name itself (the only case local state wins — here a lone
// local branch with no origin tracking ref at all).
func TestResolveRefBareNameFallback(t *testing.T) {
	r, err := git.PlainInit(filepath.Join(t.TempDir(), "repo"), false)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	wt, _ := r.Worktree()
	writeFile(t, filepath.Join(wt.Filesystem.Root(), "f", "x.txt"), "x")
	if _, err := wt.Add("f/x.txt"); err != nil {
		t.Fatalf("add: %v", err)
	}
	commit(t, wt, "init")
	h, _ := r.Head()
	// A lone local branch with NO remote-tracking ref (no origin remote at all).
	mustSetRef(t, r, "refs/heads/solo", h.Hash())

	grepo := &goGitRepo{r: r}
	got, err := grepo.resolveRef("solo")
	if err != nil {
		t.Fatalf("resolveRef(solo) = %v, want fallback to local branch", err)
	}
	if got != h.Hash() {
		t.Fatalf("resolveRef(solo) = %s, want %s", got, h.Hash())
	}
}

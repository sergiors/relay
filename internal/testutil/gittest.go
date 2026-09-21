// Package testutil provides shared test fixtures for Relay packages. It is
// compiled into test binaries only and must never be imported by production
// code.
//
// The local-git fixture (New and friends) builds a non-bare work repo with one
// commit plus a filesystem bare remote seeded from it, so the git, webhook, and
// CLI tests perform the same init/commit/set-ref/fetch dance once instead of
// each carrying a private copy. All operations use a plain filesystem path as
// the "remote" — go-git clones and fetches from a local path with no SSH and no
// network — mirroring the local transport seam SyncOptions{CloneURL, Auth} the
// production SSH-only path uses.
//
// Alongside the git fixtures it holds the small helpers that packages used to
// duplicate: EnvOr, RequireRedis, RequireDocker, DiscardLogger, SyncBuffer,
// FreeAddr, FreePort, and WaitFor.
package testutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// DefaultTemplate is the minimal function template committed at fn/template.yaml
// when Options.Template is empty.
const DefaultTemplate = "runtime: node24\n"

// Repo is a local git fixture: a non-bare work repo with one commit on branch
// main plus a bare remote seeded from it. Only the paths are exposed; the tests
// that consume it drive further git operations through go-git directly.
type Repo struct {
	Work string
	Bare string
}

// Options controls the fixture shape. Zero values get fresh temp dirs, the
// DefaultTemplate, and a single main branch.
type Options struct {
	// Work and Bare are the fixture directories; empty values get fresh temp
	// dirs ("work" and "remote.git").
	Work, Bare string
	// Template is the content written to <work>/fn/template.yaml.
	Template string
	// ExtraBranches are additional branches (beyond main) pointing at the
	// initial commit.
	ExtraBranches []string
	// Tag, when non-empty, is a lightweight tag at the initial commit.
	Tag string
}

// New builds the fixture and returns it. Any git failure fails the test.
func New(t *testing.T, opts Options) Repo {
	t.Helper()
	r := Repo{Work: opts.Work, Bare: opts.Bare}
	if r.Work == "" {
		r.Work = filepath.Join(t.TempDir(), "work")
	}
	if r.Bare == "" {
		r.Bare = filepath.Join(t.TempDir(), "remote.git")
	}
	tmpl := opts.Template
	if tmpl == "" {
		tmpl = DefaultTemplate
	}

	repo, err := git.PlainInit(r.Work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	WriteFile(t, filepath.Join(r.Work, "fn", "template.yaml"), tmpl)
	if _, err := wt.Add("fn/template.yaml"); err != nil {
		t.Fatalf("add: %v", err)
	}
	h := Commit(t, wt, "initial")
	SetRef(t, repo, "refs/heads/main", h)
	for _, br := range opts.ExtraBranches {
		SetRef(t, repo, "refs/heads/"+br, h)
	}
	if opts.Tag != "" {
		if _, err := repo.CreateTag(opts.Tag, h, nil); err != nil {
			t.Fatalf("tag %s: %v", opts.Tag, err)
		}
	}
	SeedBare(t, r.Bare, r.Work)
	return r
}

// NewBareRepo is the convenience form for tests that only need the bare remote
// path: it builds a work repo with the DefaultTemplate on main and returns the
// bare path.
func NewBareRepo(t *testing.T) string {
	t.Helper()
	return New(t, Options{}).Bare
}

// SeedBare creates (or re-seeds) the bare remote at bare from the work repo,
// fetching every branch and tag.
func SeedBare(t *testing.T, bare, work string) {
	t.Helper()
	if _, err := os.Stat(bare); os.IsNotExist(err) {
		if _, err := git.PlainInit(bare, true); err != nil {
			t.Fatalf("init bare: %v", err)
		}
	}
	b, err := git.PlainOpen(bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	orig, err := b.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{work}})
	if err != nil {
		orig, err = b.Remote("origin")
		if err != nil {
			t.Fatalf("get origin: %v", err)
		}
	}
	if err := orig.Fetch(&git.FetchOptions{
		RefSpecs: []config.RefSpec{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"},
	}); err != nil {
		t.Fatalf("seed bare: %v", err)
	}
}

// WriteFile writes content at path with mode 0644, creating parent dirs.
func WriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// Author returns the fixed commit author identity tests use.
func Author() object.Signature {
	return object.Signature{Name: "t", Email: "t@e", When: time.Now()}
}

// Commit commits the staged worktree changes with the test author.
func Commit(t *testing.T, wt *git.Worktree, msg string) plumbing.Hash {
	t.Helper()
	author := Author()
	h, err := wt.Commit(msg, &git.CommitOptions{Author: &author})
	if err != nil {
		t.Fatalf("commit %q: %v", msg, err)
	}
	return h
}

// SetRef points name at h directly in the store.
func SetRef(t *testing.T, r *git.Repository, name string, h plumbing.Hash) {
	t.Helper()
	if err := r.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), h)); err != nil {
		t.Fatalf("set ref %s: %v", name, err)
	}
}

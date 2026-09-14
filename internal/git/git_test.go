// git_test.go holds the shared local-transport fixture helpers for the
// internal/git package tests, plus small unit tests for the seam and validation.
//
// All tests use a local filesystem bare repo as the "remote" and a filesystem
// work repo as the "source" — go-git clones/fetches from a plain filesystem path
// with no SSH and no network. Production sync is SSH-only; the seam (SyncOptions{
// CloneURL, Auth}) is exactly what lets tests drive the very same core with a
// local path and a nil auth without changing the production SSH-only path.
package git

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// testEnv bundles the temp dirs and a local fixture repo for a sync test.
type testEnv struct {
	work, bare, checkout, functions, gitDir, sshDir string
}

// fixture builds a local working repo and a bare "remote" origin. The working
// repo starts with one function "fn" committed to branch "main" and a second
// branch "other" at the same commit, and (optionally) an annotated tag. It
// returns the paths; e.bare is the CloneURL for sync tests.
func fixture(t *testing.T, withTag bool) testEnv {
	t.Helper()
	e := testEnv{
		work:      filepath.Join(t.TempDir(), "work"),
		bare:      filepath.Join(t.TempDir(), "remote.git"),
		checkout:  filepath.Join(t.TempDir(), "checkout"),
		functions: filepath.Join(t.TempDir(), "functions"),
		gitDir:    filepath.Join(t.TempDir(), "git"),
		sshDir:    filepath.Join(t.TempDir(), "ssh"),
	}

	r, err := git.PlainInit(e.work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	writeFile(t, filepath.Join(e.work, "fn", "template.yaml"), "runtime: node24\nevents:\n  - handler: foo.handler\n    pattern:\n      event_name: [INSERT]\n")
	if _, err := wt.Add("fn/template.yaml"); err != nil {
		t.Fatalf("add: %v", err)
	}
	commit(t, wt, "initial")
	h, _ := r.Head()
	mustSetRef(t, r, "refs/heads/main", h.Hash())
	mustSetRef(t, r, "refs/heads/other", h.Hash())
	if withTag {
		if _, err := r.CreateTag("v1", h.Hash(), nil); err != nil {
			t.Fatalf("tag: %v", err)
		}
	}
	seedBare(t, e)
	return e
}

func mustSetRef(t *testing.T, r *git.Repository, name string, h plumbing.Hash) {
	t.Helper()
	if err := r.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), h)); err != nil {
		t.Fatalf("set ref %s: %v", name, err)
	}
}

// seedBare creates (or re-seeds) the bare remote from the work repo, fetching
// every branch and tag.
func seedBare(t *testing.T, e testEnv) {
	t.Helper()
	if _, err := os.Stat(e.bare); os.IsNotExist(err) {
		if _, err := git.PlainInit(e.bare, true); err != nil {
			t.Fatalf("init bare: %v", err)
		}
	}
	b, err := git.PlainOpen(e.bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	orig, err := b.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{e.work}})
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

func commit(t *testing.T, wt *git.Worktree, msg string) plumbing.Hash {
	t.Helper()
	h, err := wt.Commit(msg, &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Now()},
	})
	if err != nil {
		t.Fatalf("commit %q: %v", msg, err)
	}
	return h
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// syncOpts returns a SyncOptions bound to the test env, with the bare repo as
// the local-transport CloneURL and a nil auth.
func syncOpts(t *testing.T, e testEnv, out io.Writer) SyncOptions {
	t.Helper()
	o := NewSyncOptions()
	o.ConfigPath = filepath.Join(e.gitDir, "source.json")
	o.CheckoutDir = e.checkout
	o.FunctionsDir = e.functions
	o.CloneURL = e.bare
	o.Out = out
	return o
}

// testLog is a discard writer sink for tests that need an io.Writer.
func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestNewSyncOptionsDefaults verifies the production default paths.
func TestNewSyncOptionsDefaults(t *testing.T) {
	o := NewSyncOptions()
	if o.ConfigPath != ConfigPath || o.CheckoutDir != CheckoutDir {
		t.Fatalf("defaults = %+v, want ConfigPath=%s CheckoutDir=%s", o, ConfigPath, CheckoutDir)
	}
}

func TestValidateRepositoryURL(t *testing.T) {
	for _, u := range []string{
		"git@github.com:acme/repo.git",
		"ssh://git@github.com/acme/repo.git",
		"git@gitlab.com:group/sub/repo.git",
	} {
		if err := ValidateRepositoryURL(u); err != nil {
			t.Errorf("ValidateRepositoryURL(%q): %v, want nil", u, err)
		}
	}
	for _, u := range []string{"", "https://github.com/a/r", "http://x/y", "file:///tmp/r", "plain-path"} {
		if err := ValidateRepositoryURL(u); err == nil {
			t.Errorf("ValidateRepositoryURL(%q): nil, want error", u)
		}
	}
}

func TestValidatePath(t *testing.T) {
	if err := validatePath("pkg/functions"); err != nil {
		t.Errorf("validatePath(pkg/functions): %v, want nil", err)
	}
	for _, p := range []string{"../x", "/abs", "a/../../b", "..", ".", "a/../..", `back\slash`} {
		if err := validatePath(p); err == nil {
			t.Errorf("validatePath(%q): nil, want error", p)
		}
	}
}

// assertErrorIs is a tiny helper for sentinel checks.
func assertErrorIs(t *testing.T, err error, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want errors.Is(want=%v)", err, want)
	}
}

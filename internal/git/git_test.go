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
	"path/filepath"
	"testing"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"relay/internal/testutil"
)

// fixtureTemplate is the function committed by fixture: one handler with an
// INSERT pattern, distinct from testutil.DefaultTemplate so git package tests can
// assert on the handler.
const fixtureTemplate = "runtime: node24\nevents:\n  - handler: foo.handler\n    pattern:\n      event_name: [INSERT]\n"

// testEnv bundles the temp dirs and a local fixture repo for a sync test.
type testEnv struct {
	work, bare, checkout, functions, gitDir, sshDir string
}

// fixture builds a local working repo and a bare "remote" origin. The working
// repo starts with one function "fn" committed to branch "main" and a second
// branch "other" at the same commit, and (optionally) a tag. It returns the
// paths; e.bare is the CloneURL for sync tests.
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
	opts := testutil.Options{
		Work:          e.work,
		Bare:          e.bare,
		Template:      fixtureTemplate,
		ExtraBranches: []string{"other"},
	}
	if withTag {
		opts.Tag = "v1"
	}
	testutil.New(t, opts)
	return e
}

// mustSetRef points ref at h directly in the store.
func mustSetRef(t *testing.T, r *git.Repository, name string, h plumbing.Hash) {
	t.Helper()
	testutil.SetRef(t, r, name, h)
}

// seedBare re-seeds the test env's bare remote from its work repo.
func seedBare(t *testing.T, e testEnv) {
	t.Helper()
	testutil.SeedBare(t, e.bare, e.work)
}

func commit(t *testing.T, wt *git.Worktree, msg string) plumbing.Hash {
	t.Helper()
	return testutil.Commit(t, wt, msg)
}

// writeFile writes content at path, creating parent dirs.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	testutil.WriteFile(t, path, content)
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

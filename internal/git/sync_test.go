package git

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"relay/internal/function"
)

// mustSync runs a sync against the test env's bare remote and fails the test on
// error, returning the captured summary.
func mustSync(t *testing.T, e testEnv, cfg Config) bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	o := syncOpts(t, e, &out)
	if err := SyncFromConfig(context.Background(), o, cfg); err != nil {
		t.Fatalf("sync: %v", err)
	}
	return out
}

// TestInitialSync verifies the first sync clones the repo, checks out the ref,
// materializes function dirs into the target, and records Synced/commit in the
// config file.
func TestInitialSync(t *testing.T) {
	e := fixture(t, true)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	out := mustSync(t, e, cfg)

	b, err := os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}
	if !strings.Contains(string(b), "foo.handler") {
		t.Fatalf("materialized template = %q, want foo.handler", string(b))
	}
	if _, err := os.Stat(filepath.Join(e.checkout, ".git")); err != nil {
		t.Fatalf("checkout missing: %v", err)
	}
	// Config bookkeeping recorded.
	cfgPath := filepath.Join(e.gitDir, "source.json")
	saved, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load persisted config: %v", err)
	}
	if !saved.Synced || saved.LastSyncedCommit == "" || saved.LastSyncedAt == "" {
		t.Fatalf("persisted config not marked synced: %+v", saved)
	}
	// The summary mentions the resolved ref and materialized count.
	if !strings.Contains(out.String(), "Resolved") {
		t.Fatalf("summary missing 'Resolved':\n%s", out.String())
	}
}

// TestContentChangeAndRemoval pins incremental behavior: a changed file is
// refreshed and a removed function directory disappears from /functions.
func TestContentChangeAndRemoval(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)

	// Update fn's content, add fn2.
	updateFnAndAdd(t, e, "main", "fn", "runtime: python3.14\n# changed\n", "fn2")
	mustSync(t, e, cfg)
	b, _ := os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if !strings.Contains(string(b), "# changed") {
		t.Fatalf("fn content not refreshed: %q", string(b))
	}
	if _, err := os.Stat(filepath.Join(e.functions, "fn2")); err != nil {
		t.Fatalf("fn2 missing: %v", err)
	}

	// Remove fn from the repo, sync, fn must vanish from /functions.
	removeFnFromRepo(t, e, "main", "fn")
	mustSync(t, e, cfg)
	if _, err := os.Stat(filepath.Join(e.functions, "fn")); !os.IsNotExist(err) {
		t.Fatal("fn still in /functions after removal from repo")
	}
}

// updateFnAndAdd updates fn's template content and adds a new function on the
// given branch, then pushes both to the bare remote.
func updateFnAndAdd(t *testing.T, e testEnv, branch, fnName, content, extraName string) {
	t.Helper()
	r, _ := git.PlainOpen(e.work)
	wt, _ := r.Worktree()
	if err := wt.Checkout(&git.CheckoutOptions{Branch: plumbing.ReferenceName("refs/heads/" + branch), Force: true}); err != nil {
		t.Fatalf("checkout %s: %v", branch, err)
	}
	writeFile(t, filepath.Join(e.work, fnName, "template.yaml"), content)
	writeFile(t, filepath.Join(e.work, extraName, "template.yaml"), "runtime: python3.14\nhandler: bar\n")
	if _, err := wt.Add(filepath.Join(fnName, "template.yaml")); err != nil {
		t.Fatalf("add fn: %v", err)
	}
	if _, err := wt.Add(filepath.Join(extraName, "template.yaml")); err != nil {
		t.Fatalf("add extra: %v", err)
	}
	commit(t, wt, "update add")
	pushBranchs(t, e, branch)
}

// removeFnFromRepo deletes fnName from branch and pushes the deletion. It stages
// the removal through the worktree (git rm), which is what actually records the
// deletion in the index for the next commit.
func removeFnFromRepo(t *testing.T, e testEnv, branch, fnName string) {
	t.Helper()
	r, _ := git.PlainOpen(e.work)
	wt, _ := r.Worktree()
	if err := wt.Checkout(&git.CheckoutOptions{Branch: plumbing.ReferenceName("refs/heads/" + branch), Force: true}); err != nil {
		t.Fatalf("checkout %s: %v", branch, err)
	}
	// Remove every tracked file under the function dir first (git rm), then the
	// directory on disk. Removing the dir via wt.Remove("fn") fails because the
	// index holds individual files, so we remove each tracked file and drop the
	// empty directory.
	if _, err := wt.Remove(fnName + "/template.yaml"); err != nil {
		t.Fatalf("git rm %s: %v", fnName, err)
	}
	if err := os.RemoveAll(filepath.Join(e.work, fnName)); err != nil {
		t.Fatalf("remove from fs: %v", err)
	}
	commit(t, wt, "remove "+fnName)
	pushBranchs(t, e, branch)
}

// pushBranchs fetches the given branches (and all tags) from work into the bare
// remote, simulating an operator push.
func pushBranchs(t *testing.T, e testEnv, branches ...string) {
	t.Helper()
	b, err := git.PlainOpen(e.bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	orig, err := b.Remote("origin")
	if err != nil {
		t.Fatalf("remote: %v", err)
	}
	var refs []config.RefSpec
	for _, br := range branches {
		refs = append(refs, config.RefSpec("+refs/heads/"+br+":refs/heads/"+br))
	}
	refs = append(refs, config.RefSpec("+refs/tags/*:refs/tags/*"))
	if err := orig.Fetch(&git.FetchOptions{RefSpecs: refs}); err != nil {
		t.Fatalf("push to bare: %v", err)
	}
}

// TestRefChange verifies switching the configured ref to another branch makes
// /functions reflect the other branch's content.
func TestRefChange(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "other"}
	// Distinct content only on 'other'.
	updateFnAndAdd(t, e, "other", "fn", "runtime: node24\n#other\n", "otherfn")
	mustSync(t, e, cfg)
	b, _ := os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if !strings.Contains(string(b), "#other") {
		t.Fatalf("fn content = %q, want other-branch content", string(b))
	}
	if _, err := os.Stat(filepath.Join(e.functions, "otherfn")); err != nil {
		t.Fatalf("otherfn missing: %v", err)
	}
}

// TestTagRefResolves verifies a configured tag ref resolves and materializes the
// commit it points at. Bare branch names need the origin-tracking fallback in
// resolveRef; tags resolve through go-git's own rules, and both must work for a
// tag-only repo.
func TestTagRefResolves(t *testing.T) {
	e := fixture(t, true) // creates tag v1 at the initial commit
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "v1"}
	mustSync(t, e, cfg)
	b, err := os.ReadFile(filepath.Join(e.functions, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("materialized function missing via tag: %v", err)
	}
	if !strings.Contains(string(b), "foo.handler") {
		t.Fatalf("tag materialized content = %q, want foo.handler", string(b))
	}
}

// TestDeterministicReplace pins the core rule: a directory planted in the
// target /functions that is NOT in the repo is removed once a git source is
// configured and synced, so /functions reflects the repo exactly.
func TestDeterministicReplace(t *testing.T) {
	e := fixture(t, false)
	// Plant an extra operator dir before the first sync.
	writeFile(t, filepath.Join(e.functions, "stray", "template.yaml"), "runtime: node\n")
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)
	if _, err := os.Stat(filepath.Join(e.functions, "stray")); !os.IsNotExist(err) {
		t.Fatal("stray dir survived a deterministic sync; /functions must reflect the repo exactly")
	}
	if _, err := os.Stat(filepath.Join(e.functions, "fn")); err != nil {
		t.Fatalf("fn missing: %v", err)
	}
}

// TestMonorepoSync pins end-to-end materialization of a monorepo source: with
// Path set to a checkout subtree that contains function dirs, sync materializes
// exactly those functions and only those (non-function children of the subtree
// are ignored, and the subtree dir itself is never materialized). It also pins
// that deterministic removal still applies (a stray dir planted in /functions
// before the sync is removed), and — after that — that a second sync with a
// Path that does not exist in the checked-out ref fails with the missing-source
// error while leaving the already-materialized functions intact.
func TestMonorepoSync(t *testing.T) {
	e := fixture(t, false)
	_ = Config{Repository: "git@github.com:acme/r.git", Ref: "main"}

	// Build a monorepo layout on main: services/funa, services/funb, a
	// non-function file, and a non-function subdir. Both function names are
	// lowercase because ValidName rejects uppercase (the same rule the
	// reconciler load applies); using valid names keeps the test about the
	// monorepo path, not name validation. The work repo already has "fn" from
	// the fixture; we replace the whole tree with the monorepo layout committed
	// on main, then re-seed the bare so the checkout reflects it.
	r, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work: %v", err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	writeFile(t, filepath.Join(e.work, "services", "funa", "template.yaml"), "runtime: node24\nevents:\n  - handler: a.handler\n")
	writeFile(t, filepath.Join(e.work, "services", "funb", "template.yaml"), "runtime: node24\nevents:\n  - handler: b.handler\n")
	writeFile(t, filepath.Join(e.work, "services", "README.md"), "# services\n")
	writeFile(t, filepath.Join(e.work, "services", "vendor", "lib.go"), "package vendor\n")
	for _, p := range []string{
		"services/funa/template.yaml",
		"services/funb/template.yaml",
		"services/README.md",
		"services/vendor/lib.go",
	} {
		if _, err := wt.Add(p); err != nil {
			t.Fatalf("add %s: %v", p, err)
		}
	}
	if _, err := wt.Commit("monorepo layout", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Now()}}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	h, _ := r.Head()
	if err := r.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", h.Hash())); err != nil {
		t.Fatalf("set main: %v", err)
	}
	seedBare(t, e)

	// Plant a stray dir in the target before the first sync; deterministic
	// removal must clear it exactly like the repo-root case.
	writeFile(t, filepath.Join(e.functions, "stray", "template.yaml"), "runtime: node\n")

	monoCfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main", Path: "services"}
	out := mustSync(t, e, monoCfg)

	// Both functions materialized with the correct content.
	wantHandler := map[string]string{"funa": "a.handler", "funb": "b.handler"}
	for _, name := range []string{"funa", "funb"} {
		b, err := os.ReadFile(filepath.Join(e.functions, name, "template.yaml"))
		if err != nil {
			t.Fatalf("read materialized %s: %v", name, err)
		}
		if !strings.Contains(string(b), wantHandler[name]) {
			t.Fatalf("%s template = %q, want handler %s", name, string(b), wantHandler[name])
		}
	}
	// The services subtree dir and non-function children are NOT materialized.
	for _, absent := range []string{"services", "README.md", "vendor"} {
		if _, err := os.Stat(filepath.Join(e.functions, absent)); err == nil {
			t.Fatalf("non-function path %q materialized as a directory; want it ignored", absent)
		}
	}
	// Deterministic removal applied to the planted stray.
	if _, err := os.Stat(filepath.Join(e.functions, "stray")); !os.IsNotExist(err) {
		t.Fatal("stray dir survived a deterministic monorepo sync; /functions must reflect the source exactly")
	}
	// The summary reports exactly the two discovered functions.
	if !strings.Contains(out.String(), "Materialized 2 function(s): funa, funb") {
		t.Fatalf("summary = %q, want 'Materialized 2 function(s): funa, funb'", out.String())
	}

	// A second sync pointing at a path absent from the checked-out ref must fail
	// with the clear missing-source error BEFORE touching /functions or the
	// config bookkeeping.
	badCfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main", Path: "does-not-exist"}
	err = SyncFromConfig(context.Background(), syncOpts(t, e, io.Discard), badCfg)
	if err == nil {
		t.Fatal("sync with a nonexistent monorepo path: nil error, want missing-source error")
	}
	if !strings.Contains(err.Error(), "does not exist in the checkout") {
		t.Fatalf("sync err = %v, want clear missing-source message", err)
	}
	// The previously materialized functions are still intact.
	for name := range map[string]bool{"funa": true, "funb": true} {
		if _, serr := os.Stat(filepath.Join(e.functions, name, "template.yaml")); serr != nil {
			t.Fatalf("function %s was removed by the failed missing-source sync: %v", name, serr)
		}
	}
}

// TestSyncLoadsFunctionLoader verifies the materialized output loads cleanly
// with the real function loader, proving format compatibility without
// duplicating loader logic.
func TestSyncLoadsFunctionLoader(t *testing.T) {
	e := fixture(t, false)
	mustSync(t, e, Config{Repository: "git@github.com:acme/r.git", Ref: "main"})
	fns, err := function.NewLoader(e.functions, testLog()).Load()
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(fns) != 1 || fns[0].Name != "fn" {
		t.Fatalf("loaded functions = %+v, want [fn]", fns)
	}
}

// TestSyncNoConfigSource verifies sync errors clearly (ErrConfigNotFound) when
// no source is configured yet.
func TestSyncNoConfigSource(t *testing.T) {
	e := fixture(t, false)
	o := syncOpts(t, e, io.Discard)
	err := Sync(context.Background(), o)
	assertErrorIs(t, err, ErrConfigNotFound())
}

// TestSyncMissingKey verifies a sync that needs SSH auth fails with a clear
// "run relay git keygen" error when no key is present, and does NOT proceed
// with host verification disabled.
func TestSyncMissingKey(t *testing.T) {
	e := fixture(t, false)
	o := syncOpts(t, e, io.Discard)
	o.CloneURL = ""
	o.RepositoryURL = "git@github.com:acme/r.git"
	o.SSHDir = e.sshDir
	err := SyncFromConfig(context.Background(), o, Config{Repository: "git@github.com:acme/r.git", Ref: "main"})
	if err == nil || !strings.Contains(err.Error(), "keygen") {
		t.Fatalf("sync err = %v, want missing-key/keygen guidance", err)
	}
}

// TestSyncKnownHostsRequired verifies that even with a key present, a sync to an
// SSH host fails when no known_hosts file can be found — host-key verification
// is NEVER skipped. This exercises the guard: the auth builder must reject,
// never fall back to InsecureIgnoreHostKey.
func TestSyncKnownHostsRequired(t *testing.T) {
	e := fixture(t, false)
	if _, err := GenerateKey(e.sshDir); err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	// Deterministically hide any host system known_hosts by pointing
	// SSH_KNOWN_HOSTS at a path that does not exist, so go-git's callback errors
	// the same way regardless of this machine's ~/.ssh contents.
	t.Setenv("SSH_KNOWN_HOSTS", filepath.Join(t.TempDir(), "does-not-exist"))
	o := syncOpts(t, e, io.Discard)
	o.CloneURL = ""
	o.RepositoryURL = "git@github.com:acme/r.git"
	o.SSHDir = e.sshDir
	err := SyncFromConfig(context.Background(), o, Config{Repository: "git@github.com:acme/r.git", Ref: "main"})
	if err == nil {
		t.Fatal("sync with no known_hosts: nil error, want host-key verification error")
	}
	// The error must carry the known_hosts guidance and must NOT say the key is
	// missing (the key exists here).
	if strings.Contains(err.Error(), "keygen") || !strings.Contains(err.Error(), "known_hosts") {
		t.Fatalf("sync err = %v, want known_hosts guidance (not keygen)", err)
	}
}

// TestSyncLogsThroughInjectedLogger pins the DI wiring AND the two-channel
// report split. A local fixture sync runs with o.Log set to a Debug-level slog
// logger over one buffer and o.Out set to a separate buffer; the step lines
// (e.g. "Syncing..." and "Sync complete") must appear on BOTH. It also documents
// the nil-contract: every other sync test in this file runs with o.Log unset
// (nil) — which pins nil-logger safety (no panic, Out is still written) now that
// the fallback-constructor helper is gone and nil-tolerance lives at this single
// call site.
func TestSyncLogsThroughInjectedLogger(t *testing.T) {
	e := fixture(t, true)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "v1"}

	var logBuf bytes.Buffer
	var outBuf bytes.Buffer
	o := syncOpts(t, e, &outBuf)
	o.Log = slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := SyncFromConfig(context.Background(), o, cfg); err != nil {
		t.Fatalf("sync: %v", err)
	}

	for _, step := range []string{"Syncing...", "Sync complete"} {
		if !strings.Contains(outBuf.String(), step) {
			t.Fatalf("out missing %q:\n%s", step, outBuf.String())
		}
		if !strings.Contains(logBuf.String(), step) {
			t.Fatalf("log missing %q:\n%s", step, logBuf.String())
		}
	}
}

// TestSetSourceUpsertPreservesBookkeeping verifies SetSource on an existing
// config keeps the last-synced metadata (so changing ref/path retains status).
func TestSetSourceUpsertPreservesBookkeeping(t *testing.T) {
	p := filepath.Join(t.TempDir(), "source.json")
	if err := SetSource(p, "git@github.com:acme/r.git", "main", ""); err != nil {
		t.Fatalf("SetSource initial: %v", err)
	}
	// Simulate a completed sync by writing the metadata into the config.
	cfg, _ := LoadConfig(p)
	cfg.Synced = true
	cfg.LastSyncedCommit = "abc"
	cfg.LastSyncedAt = "2026-01-01T00:00:00Z"
	if err := writeConfig(cfg, p); err != nil {
		t.Fatalf("writeConfig: %v", err)
	}
	// Re-set with a different ref/path: must keep the bookkeeping.
	if err := SetSource(p, "git@github.com:acme/r.git", "dev", "pkg/fn"); err != nil {
		t.Fatalf("SetSource update: %v", err)
	}
	got, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got.Ref != "dev" || got.Path != "pkg/fn" || !got.Synced || got.LastSyncedCommit != "abc" {
		t.Fatalf("updated = %+v, want ref dev path pkg/fn synced with preserved commit", got)
	}
}

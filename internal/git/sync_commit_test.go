package git

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

// mustSyncCommit runs an exact-SHA sync against the test env's bare remote and
// fails the test on error, returning the captured summary.
func mustSyncCommit(t *testing.T, e testEnv, cfg Config, commit string) bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	o := syncOpts(t, e, &out)
	if err := SyncCommitFromConfig(context.Background(), o, cfg, commit); err != nil {
		t.Fatalf("sync commit %s: %v", commit, err)
	}
	return out
}

// headOf returns the current HEAD commit of the checkout at dir.
func headOf(t *testing.T, dir string) plumbing.Hash {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatalf("open checkout: %v", err)
	}
	h, err := r.Head()
	if err != nil {
		t.Fatalf("read HEAD: %v", err)
	}
	return h.Hash()
}

// TestSyncCommitPinsExactSHAWhileRefMoves pins the coordinated-deployment
// materialization contract: an exact-SHA sync checks out precisely that commit
// and materializes its tree even after the configured branch has moved on, and
// a later exact-SHA sync of the same commit restores it idempotently. The
// plain ref sync in between keeps its historical behavior: it follows the
// freshly fetched remote tip.
func TestSyncCommitPinsExactSHAWhileRefMoves(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}

	work, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work repo: %v", err)
	}
	head, err := work.Head()
	if err != nil {
		t.Fatalf("work HEAD: %v", err)
	}
	original := head.Hash().String()

	// Move the branch: fn's content changes on main and is pushed.
	updateFnAndAdd(t, e, "main", "fn", "runtime: python3.14\n# changed\n", "fn2")

	out := mustSyncCommit(t, e, cfg, original)
	// The exact-SHA sync presents the same concise summary as any sync — no
	// ref-resolution or checkout narration; the exact checkout itself is proven
	// by the HEAD and bookkeeping assertions below.
	for _, want := range []string{
		"Updating apps on this node...", "Updated 1 app(s): fn",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("summary missing %q:\n%s", want, out.String())
		}
	}

	content, err := os.ReadFile(filepath.Join(e.apps, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}
	if !strings.Contains(string(content), "foo.handler") {
		t.Fatalf("exact-SHA sync materialized the moved tip: %q", string(content))
	}
	if strings.Contains(string(content), "# changed") {
		t.Fatalf("exact-SHA sync leaked branch-tip content: %q", string(content))
	}
	if got := headOf(t, e.checkout).String(); got != original {
		t.Fatalf("checkout HEAD = %s, want the exact commit %s", got, original)
	}
	saved, err := LoadConfig(filepath.Join(e.gitDir, "source.json"))
	if err != nil {
		t.Fatalf("load persisted config: %v", err)
	}
	if saved.LastSyncedCommit != original {
		t.Fatalf("LastSyncedCommit = %q, want %q", saved.LastSyncedCommit, original)
	}

	// The plain ref sync keeps its moving-ref behavior: it follows the tip.
	mustSync(t, e, cfg)
	content, err = os.ReadFile(filepath.Join(e.apps, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}
	if !strings.Contains(string(content), "# changed") {
		t.Fatalf("ref sync did not follow the moved tip: %q", string(content))
	}

	// Re-materializing the pinned commit restores its tree exactly.
	mustSyncCommit(t, e, cfg, original)
	content, err = os.ReadFile(filepath.Join(e.apps, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}
	if !strings.Contains(string(content), "foo.handler") || strings.Contains(string(content), "# changed") {
		t.Fatalf("re-sync of the exact commit = %q, want the original tree", string(content))
	}
	if got := headOf(t, e.checkout).String(); got != original {
		t.Fatalf("checkout HEAD after re-sync = %s, want %s", got, original)
	}
}

// TestSyncCommitUppercaseSHANormalizes pins that the exact-SHA entry accepts an
// uppercase object name (case-insensitive input) and still checks out the
// canonical lowercase commit.
func TestSyncCommitUppercaseSHANormalizes(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}

	work, err := git.PlainOpen(e.work)
	if err != nil {
		t.Fatalf("open work repo: %v", err)
	}
	head, err := work.Head()
	if err != nil {
		t.Fatalf("work HEAD: %v", err)
	}
	original := head.Hash().String()

	mustSyncCommit(t, e, cfg, strings.ToUpper(original))
	if got := headOf(t, e.checkout).String(); got != original {
		t.Fatalf("checkout HEAD = %s, want %s", got, original)
	}
}

// TestSyncCommitMissingObjectFailsHard pins that a syntactically valid commit
// that is absent from the remote after the fetch is a hard error — never a
// fallback to the configured ref — and the already-materialized tree stays
// untouched.
func TestSyncCommitMissingObjectFailsHard(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg) // materialize the real tip first

	const missing = "0123456789abcdef0123456789abcdef01234567"
	o := syncOpts(t, e, io.Discard)
	err := SyncCommitFromConfig(context.Background(), o, cfg, missing)
	if err == nil || !strings.Contains(err.Error(), "not available in the checkout") {
		t.Fatalf("sync of a missing commit = %v, want the missing-object error", err)
	}

	// The previously materialized tree is intact — no ref fallback ran.
	content, rerr := os.ReadFile(filepath.Join(e.apps, "fn", "template.yaml"))
	if rerr != nil {
		t.Fatalf("read materialized template: %v", rerr)
	}
	if !strings.Contains(string(content), "foo.handler") {
		t.Fatalf("failed exact-SHA sync mutated /apps: %q", string(content))
	}
}

// TestSyncCommitRejectsNonExactCommits pins the validation gate: abbreviated
// hashes, over-long values, and non-hex input are rejected before any
// transport, checkout, or materialization work runs.
func TestSyncCommitRejectsNonExactCommits(t *testing.T) {
	e := fixture(t, false)
	cfg := Config{Repository: "git@github.com:acme/r.git", Ref: "main"}
	mustSync(t, e, cfg)

	for _, bad := range []string{
		"deadbeef",
		"0123456789abcdef0123456789abcdef01234567a",
		"0123456789abcdef0123456789abcdef012345g",
		"main",
	} {
		o := syncOpts(t, e, io.Discard)
		err := SyncCommitFromConfig(context.Background(), o, cfg, bad)
		if err == nil || !strings.Contains(err.Error(), "full 40-character hexadecimal SHA") {
			t.Fatalf("sync with commit %q = %v, want the exact-SHA validation error", bad, err)
		}
	}
}

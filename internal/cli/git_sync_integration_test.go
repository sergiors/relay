//go:build integration

package cli

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"relay/internal/app"
	"relay/internal/git"
	"relay/internal/state"
	"relay/internal/testutil"
)

const (
	gitSyncCommitX = "9999999999999999999999999999999999999999"
	deployCommitA  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	deployCommitB  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// coordinatedGitDeps builds the participating-node CLI fixture: an isolated
// state schema behind the injected OpenState seam (unchanged architecture), a
// local bare remote as the transport seam, and a configured local Git source.
// It returns the dependencies, a direct state handle, and the bare path.
func coordinatedGitDeps(t *testing.T) (Dependencies, *state.State, string) {
	t.Helper()
	deps, dsn := stateBackedDeps(t)
	bare := testutil.NewBareRepo(t)
	deps.GitCloneURL = bare
	if err := git.SetSource(deps.GitConfigPath, "git@github.com:acme/r.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	st := testutil.OpenState(t, dsn)
	return deps, st, bare
}

// bareHeadSHA returns the tip commit of the fixture's main branch.
func bareHeadSHA(t *testing.T, bare string) string {
	t.Helper()
	r, err := gogit.PlainOpen(bare)
	if err != nil {
		t.Fatalf("open bare: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("bare HEAD: %v", err)
	}
	return head.Hash().String()
}

// TestGitSyncPublishesResolvedSHAAndMaterializes pins the single-node
// publication path: `relay git sync` resolves the configured branch to an
// immutable SHA, publishes it as the shared desired revision, then
// materializes exactly that object into the local tree — through the injected
// OpenState seam.
func TestGitSyncPublishesResolvedSHAAndMaterializes(t *testing.T) {
	deps, st, bare := coordinatedGitDeps(t)
	wantSHA := bareHeadSHA(t, bare)

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err != nil {
		t.Fatalf("git sync: %v (out=%s)", err, out)
	}
	// The user-facing summary is the concise sync report: start line, the
	// materialized apps, and the completion line — no clone, resolve, or
	// publication narration.
	for _, want := range []string{
		"Updating apps on this node...",
		"Updated 1 app(s): fn",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("sync output missing %q:\n%s", want, out)
		}
	}

	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v), want the published row",
			desired, found, err)
	}
	if desired.Generation != 1 || desired.Commit != wantSHA {
		t.Fatalf("desired = generation %d commit %s, want generation 1 commit %s",
			desired.Generation, desired.Commit, wantSHA)
	}
	if desired.Publisher != "cli:git-sync" {
		t.Fatalf("publisher = %q, want cli:git-sync", desired.Publisher)
	}

	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Synced || cfg.LastSyncedCommit != wantSHA {
		t.Fatalf("config = %+v, want synced at %s", cfg, wantSHA)
	}
	if _, err := os.Stat(filepath.Join(deps.GitAppsDir, "fn", "template.yaml")); err != nil {
		t.Fatalf("materialized app missing: %v", err)
	}
}

// TestGitSyncRequiresStateWithoutLocalFallback pins the simplified database
// contract: with a configured Git source, `git sync` opens PostgreSQL BEFORE
// resolving the moving ref. A CLI environment without a reachable state
// database gets a clear error from the injected OpenState seam and NO local
// branch-sync fallback — nothing is resolved and nothing is materialized.
func TestGitSyncRequiresStateWithoutLocalFallback(t *testing.T) {
	deps := testDeps(t) // OpenState fails loudly with the fixture sentinel
	bare := testutil.NewBareRepo(t)
	deps.GitCloneURL = bare
	if err := git.SetSource(deps.GitConfigPath, "git@github.com:acme/r.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if !errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("err = %v, want the state-access failure (no local fallback)", err)
	}
	if strings.Contains(out, "Updating apps on this node...") {
		t.Fatalf("sync produced output %q; the state database opens BEFORE any "+
			"resolve or materialization", out)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced {
		t.Fatal("a stateless sync must not fall back to a local branch sync")
	}
	if _, err := os.Stat(filepath.Join(deps.GitAppsDir, "fn")); !os.IsNotExist(err) {
		t.Fatalf("apps dir stat = %v, want no materialization", err)
	}
}

// TestGitSyncUnavailableStateFailsWithoutSideEffects pins the unreachable-
// database case: the real OpenState error surfaces and, again, no ref is
// resolved and no tree is touched.
func TestGitSyncUnavailableStateFailsWithoutSideEffects(t *testing.T) {
	deps := testDeps(t)
	bare := testutil.NewBareRepo(t)
	deps.GitCloneURL = bare
	if err := git.SetSource(deps.GitConfigPath, "git@github.com:acme/r.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	deps.OpenState = func(ctx context.Context) (*state.State, func(), error) {
		st, err := state.Open(ctx, state.Options{
			DSN: "postgres://relay:relay@127.0.0.1:1/relay?sslmode=disable",
		})
		if err != nil {
			return nil, nil, err
		}
		return st, func() { _ = st.Close() }, nil
	}

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err == nil {
		t.Fatal("git sync against an unreachable database = nil, want a clear error")
	}
	if strings.Contains(out, "Updating apps on this node...") {
		t.Fatalf("sync started before the state database opened: %q", out)
	}
	cfg, cfgErr := git.LoadConfig(deps.GitConfigPath)
	if cfgErr != nil {
		t.Fatalf("load config: %v", cfgErr)
	}
	if cfg.Synced {
		t.Fatal("an unreachable state database must not fall back to a local sync")
	}
	if _, statErr := os.Stat(filepath.Join(deps.GitAppsDir, "fn")); !os.IsNotExist(statErr) {
		t.Fatalf("apps dir stat = %v, want no materialization", statErr)
	}
}

// TestGitSyncWithoutGitSourceFailsBeforeState pins the ordering: without a
// configured Git source the sync fails with the source error and never
// reaches the state database (the database-free fixture would fail loudly).
// A worker without a Git source keeps the existing local /apps behavior —
// covered by the worker startup suite and the source-classification contract
// in internal/git.
func TestGitSyncWithoutGitSourceFailsBeforeState(t *testing.T) {
	deps := testDeps(t) // OpenState fails loudly: it must not be consulted
	deps.GitCloneURL = testutil.NewBareRepo(t)

	_, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err == nil || !strings.Contains(err.Error(), "no git source configured") {
		t.Fatalf("err = %v, want the missing-git-source error", err)
	}
	if errors.Is(err, errUnexpectedStateAccess) {
		t.Fatalf("git sync consulted the state database before the git source: %v", err)
	}
}

// TestGitSyncRefusesPinnedDesiredAndUnpinPreservesRevision pins the pin
// contract on the sync path: a pinned shared revision refuses `git sync`
// BEFORE any resolve or materialization, `relay deploy unpin` changes only the
// policy (commit and generation untouched), and after unpinning the sync
// publishes the resolved tip as the next generation.
func TestGitSyncRefusesPinnedDesiredAndUnpinPreservesRevision(t *testing.T) {
	deps, st, bare := coordinatedGitDeps(t)
	wantSHA := bareHeadSHA(t, bare)

	// An operator rollback pinned the shared revision.
	pinned, err := st.PublishDeploymentDesired(context.Background(), state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     gitSyncCommitX,
			Repository: "git@github.com:acme/r.git",
			Ref:        "main",
			Publisher:  "cli",
			Pinned:     true,
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	})
	if err != nil || !pinned.Published || pinned.Desired.Generation != 1 {
		t.Fatalf("seed pinned revision = (%+v, %v)", pinned, err)
	}

	// git sync refuses: no sync summary, no publication, no local sync.
	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err == nil || !strings.Contains(err.Error(), "is pinned at") ||
		!strings.Contains(err.Error(), "relay deploy unpin") {
		t.Fatalf("pinned git sync err = %v (out=%s), want the pinned-refusal guidance", err, out)
	}
	if strings.Contains(out, "Updating apps on this node...") {
		t.Fatalf("pinned git sync produced output %q; want no sync progress", out)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced {
		t.Fatal("a refused sync must not materialize the local tree")
	}

	// Unpin flips policy only: commit and generation must not move.
	unpinOut, _, err := runCLIWithDeps(t, deps, "", "deploy", "unpin")
	if err != nil {
		t.Fatalf("deploy unpin: %v (out=%s)", err, unpinOut)
	}
	if !strings.Contains(unpinOut, "Deployment unpinned at "+gitSyncCommitX+
		"; current revision remains until the next sync or webhook update") {
		t.Fatalf("unpin output = %q, want the unchanged commit with policy flipped", unpinOut)
	}
	after, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", after, found, err)
	}
	if after.Generation != 1 || after.Commit != gitSyncCommitX || after.Pinned {
		t.Fatalf("after unpin = generation %d commit %s pinned %v, "+
			"want generation 1 commit %s with policy flipped only",
			after.Generation, after.Commit, after.Pinned, gitSyncCommitX)
	}

	// With the pin released, sync publishes the resolved tip and materializes.
	out, _, err = runCLIWithDeps(t, deps, "", "git", "sync")
	if err != nil {
		t.Fatalf("git sync after unpin: %v (out=%s)", err, out)
	}
	for _, want := range []string{
		"Updating apps on this node...",
		"Updated 1 app(s): fn",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("post-unpin sync output missing %q:\n%s", want, out)
		}
	}
	final, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", final, found, err)
	}
	if final.Generation != 2 || final.Commit != wantSHA {
		t.Fatalf("final desired = generation %d commit %s, want generation 2 commit %s",
			final.Generation, final.Commit, wantSHA)
	}
}

// gatedTransport wraps the embedded local-file transport and parks the FIRST
// upload-pack session (the resolve's fetch/clone) until release fires — the
// exact window between `git sync`'s shared-generation snapshot and its
// publication CAS.
type gatedTransport struct {
	inner   transport.Transport
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedTransport) gate() {
	g.once.Do(func() { g.started <- struct{}{} })
	<-g.release
}

func (g *gatedTransport) NewUploadPackSession(
	ep *transport.Endpoint, auth transport.AuthMethod,
) (transport.UploadPackSession, error) {
	g.gate()
	return g.inner.NewUploadPackSession(ep, auth)
}

func (g *gatedTransport) NewReceivePackSession(
	ep *transport.Endpoint, auth transport.AuthMethod,
) (transport.ReceivePackSession, error) {
	return g.inner.NewReceivePackSession(ep, auth)
}

// installGatedFileTransport swaps the "file" protocol client for a gated wrapper
// and returns the gate plus a release func. Cleanup restores the standard
// embedded transport so later tests are unaffected.
func installGatedFileTransport(t *testing.T) (gated *gatedTransport, release func()) {
	t.Helper()
	gated = &gatedTransport{
		inner:   server.NewClient(server.DefaultLoader),
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	var once sync.Once
	unlock := func() { once.Do(func() { close(gated.release) }) }
	client.InstallProtocol("file", gated)
	t.Cleanup(func() {
		unlock()
		client.InstallProtocol("file", server.NewClient(server.DefaultLoader))
	})
	return gated, unlock
}

// TestGitSyncConcurrentPublicationConflictsWithoutMaterializing pins the
// compare-and-swap contract of the source-driven sync: git sync snapshots the
// shared generation BEFORE resolving the moving branch, a concurrent
// first-ever publication advancing the row lands inside the resolve window, and
// the stale sync then fails its CAS with the "deployment changed during sync"
// conflict — its resolved SHA is never published and never materialized.
func TestGitSyncConcurrentPublicationConflictsWithoutMaterializing(t *testing.T) {
	deps, st, _ := coordinatedGitDeps(t)
	gated, release := installGatedFileTransport(t)

	concurrentDone := make(chan error, 1)
	go func() {
		select {
		case <-gated.started:
			// The snapshot is taken; advance the shared row before the CAS.
			_, err := st.PublishDeploymentDesired(context.Background(),
				state.DeploymentPublishRequest{
					Desired: state.DeploymentDesired{
						Commit:     deployCommitA,
						Repository: "git@github.com:acme/r.git",
						Ref:        "main",
						Publisher:  "concurrent",
					},
					ExpectedGeneration: -1,
					AllowPinned:        true,
				})
			release()
			concurrentDone <- err
		case <-time.After(10 * time.Second):
			concurrentDone <- errors.New("git sync never reached the branch resolve")
		}
	}()

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err == nil || !strings.Contains(err.Error(), "deployment changed during sync") ||
		!strings.Contains(err.Error(), "run 'relay git sync' again") {
		t.Fatalf("stale git sync err = %v (out=%s), want the concurrent-update conflict", err, out)
	}
	if cerr := <-concurrentDone; cerr != nil {
		t.Fatalf("concurrent publication: %v", cerr)
	}
	// The gate proves the resolve ran before the CAS: the concurrent publication
	// only lands (and release only fires) after the stale sync's fetch started.
	// What must NOT follow is any materialization progress.
	if strings.Contains(out, "Updating apps on this node...") {
		t.Fatalf("stale sync materialized anyway: %q", out)
	}

	// The concurrent publication is the ONLY write that landed, and the stale
	// sync never materialized its resolved SHA.
	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", desired, found, err)
	}
	if desired.Generation != 1 || desired.Commit != deployCommitA ||
		desired.Publisher != "concurrent" {
		t.Fatalf("desired = generation %d commit %s publisher %q, want only the concurrent write",
			desired.Generation, desired.Commit, desired.Publisher)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced {
		t.Fatal("a conflicted sync must not record or materialize its resolved SHA")
	}
	if _, err := os.Stat(filepath.Join(deps.GitAppsDir, "fn")); !os.IsNotExist(err) {
		t.Fatalf("apps dir stat = %v, want no local materialization", err)
	}
}

// TestGitSyncSameRevisionConcurrentPublicationKeepsMaterializing pins the
// same-revision semantics of the resolve window: when the shared row already
// holds the exact SHA this sync resolved and a concurrent publication of that
// SAME revision lands mid-resolve, the row is unchanged — the CAS still
// matches, no new generation is published, no refusal occurs, and
// materialization proceeds normally.
func TestGitSyncSameRevisionConcurrentPublicationKeepsMaterializing(t *testing.T) {
	deps, st, bare := coordinatedGitDeps(t)
	wantSHA := bareHeadSHA(t, bare)

	// The shared row already holds this sync's resolved revision.
	seeded, err := st.PublishDeploymentDesired(context.Background(), state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     wantSHA,
			Repository: "git@github.com:acme/r.git",
			Ref:        "main",
			Publisher:  "cli",
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	})
	if err != nil || !seeded.Published || seeded.Desired.Generation != 1 {
		t.Fatalf("seed publication = (%+v, %v)", seeded, err)
	}

	gated, release := installGatedFileTransport(t)
	concurrentDone := make(chan error, 1)
	go func() {
		select {
		case <-gated.started:
			// A concurrent publisher offers the SAME revision mid-resolve: an
			// unchanged no-op that must not invalidate the snapshot CAS.
			_, err := st.PublishDeploymentDesired(context.Background(),
				state.DeploymentPublishRequest{
					Desired: state.DeploymentDesired{
						Commit:     wantSHA,
						Repository: "git@github.com:acme/r.git",
						Ref:        "main",
						Publisher:  "concurrent",
					},
					ExpectedGeneration: -1,
					AllowPinned:        true,
				})
			release()
			concurrentDone <- err
		case <-time.After(10 * time.Second):
			concurrentDone <- errors.New("git sync never reached the branch resolve")
		}
	}()

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err != nil {
		t.Fatalf("git sync: %v (out=%s)", err, out)
	}
	if cerr := <-concurrentDone; cerr != nil {
		t.Fatalf("concurrent same-revision publish: %v", cerr)
	}
	// No refusal and no new generation: the sync runs its normal summary and
	// materializes the resolved tip.
	for _, want := range []string{
		"Updating apps on this node...",
		"Updated 1 app(s): fn",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("same-revision sync output missing %q:\n%s", want, out)
		}
	}

	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", desired, found, err)
	}
	if desired.Generation != 1 || desired.Commit != wantSHA {
		t.Fatalf("desired = generation %d commit %s, want the unchanged generation 1 "+
			"commit %s", desired.Generation, desired.Commit, wantSHA)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Synced || cfg.LastSyncedCommit != wantSHA {
		t.Fatalf("config = %+v, want the sync materialized at %s", cfg, wantSHA)
	}
}

// TestGitSyncConcurrentPinRefusesWithoutMaterializing pins the other
// interleaving: the snapshot observes an UNPINNED revision, a concurrent pin
// lands inside the resolve window without moving the generation, and the
// sync's CAS passes but the pin policy refuses the publication — nothing is
// published, the pinner's state stands, and no local materialization runs.
func TestGitSyncConcurrentPinRefusesWithoutMaterializing(t *testing.T) {
	deps, st, _ := coordinatedGitDeps(t)

	// An existing, unpinned shared revision: the pre-resolve pin check passes.
	seeded, err := st.PublishDeploymentDesired(context.Background(), state.DeploymentPublishRequest{
		Desired: state.DeploymentDesired{
			Commit:     deployCommitB,
			Repository: "git@github.com:acme/r.git",
			Ref:        "main",
			Publisher:  "cli",
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	})
	if err != nil || !seeded.Published || seeded.Desired.Generation != 1 {
		t.Fatalf("seed publication = (%+v, %v)", seeded, err)
	}

	gated, release := installGatedFileTransport(t)
	concurrentDone := make(chan error, 1)
	go func() {
		select {
		case <-gated.started:
			_, _, err := st.SetDeploymentPinned(context.Background(), true)
			release()
			concurrentDone <- err
		case <-time.After(10 * time.Second):
			concurrentDone <- errors.New("git sync never reached the branch resolve")
		}
	}()

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err == nil || !strings.Contains(err.Error(), "is pinned at") ||
		!strings.Contains(err.Error(), "relay deploy unpin") {
		t.Fatalf("pinned-mid-resolve sync err = %v (out=%s), want the pin refusal", err, out)
	}
	if perr := <-concurrentDone; perr != nil {
		t.Fatalf("concurrent pin: %v", perr)
	}
	if strings.Contains(out, "Updating apps on this node...") {
		t.Fatalf("refused sync materialized anyway: %q", out)
	}

	// The pinner's state stands: generation and commit unmoved, pin applied.
	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", desired, found, err)
	}
	if desired.Generation != 1 || desired.Commit != deployCommitB || !desired.Pinned {
		t.Fatalf("desired = generation %d commit %s pinned %v, want the pinned seed unchanged",
			desired.Generation, desired.Commit, desired.Pinned)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced {
		t.Fatal("a refused sync must not record or materialize its resolved SHA")
	}
	if _, err := os.Stat(filepath.Join(deps.GitAppsDir, "fn")); !os.IsNotExist(err) {
		t.Fatalf("apps dir stat = %v, want no local materialization", err)
	}
}

// TestGitSyncAncestorIgnoreChangeDoesNotTouchApp pins app-local source scoping
// through the whole CLI path: a revision whose ONLY change is the
// repository-root .gitignore still advances the shared desired generation (it is
// a new commit), but leaves the materialized app tree, inode, and fingerprint
// untouched and never copies the ancestor policy into the app.
func TestGitSyncAncestorIgnoreChangeDoesNotTouchApp(t *testing.T) {
	deps, dsn := stateBackedDeps(t)
	repo := testutil.New(t, testutil.Options{})
	deps.GitCloneURL = repo.Bare
	if err := git.SetSource(deps.GitConfigPath, "git@github.com:acme/r.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	st := testutil.OpenState(t, dsn)

	if out, _, err := runCLIWithDeps(t, deps, "", "git", "sync"); err != nil {
		t.Fatalf("initial git sync: %v (out=%s)", err, out)
	}

	appDir := filepath.Join(deps.GitAppsDir, "fn")
	beforeInfo, err := os.Stat(appDir)
	if err != nil {
		t.Fatalf("stat materialized app: %v", err)
	}
	beforeFingerprint, err := app.Fingerprint(appDir)
	if err != nil {
		t.Fatalf("fingerprint materialized app: %v", err)
	}
	beforeTemplate, err := os.ReadFile(filepath.Join(appDir, "template.yaml"))
	if err != nil {
		t.Fatalf("read materialized template: %v", err)
	}

	// Commit only the repository-root .gitignore, an ancestor of the app.
	work, err := gogit.PlainOpen(repo.Work)
	if err != nil {
		t.Fatalf("open work repo: %v", err)
	}
	wt, err := work.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	testutil.WriteFile(t, filepath.Join(repo.Work, ".gitignore"), "*.log\nfn/\n")
	if _, err := wt.Add(".gitignore"); err != nil {
		t.Fatalf("add root .gitignore: %v", err)
	}
	commitHash := testutil.Commit(t, wt, "ignore app files at the root")
	testutil.SetRef(t, work, "refs/heads/main", commitHash)
	testutil.SeedBare(t, repo.Bare, repo.Work)

	out, _, err := runCLIWithDeps(t, deps, "", "git", "sync")
	if err != nil {
		t.Fatalf("ancestor-ignore git sync: %v (out=%s)", err, out)
	}
	for _, want := range []string{
		"Updating apps on this node...",
		"No app changes: 1 app(s) already up to date",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("sync output missing %q:\n%s", want, out)
		}
	}

	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", desired, found, err)
	}
	if desired.Generation != 2 || desired.Commit != commitHash.String() {
		t.Fatalf("desired = generation %d commit %s, want generation 2 commit %s",
			desired.Generation, desired.Commit, commitHash.String())
	}

	afterInfo, err := os.Stat(appDir)
	if err != nil {
		t.Fatalf("stat materialized app after ancestor-only revision: %v", err)
	}
	if !os.SameFile(beforeInfo, afterInfo) {
		t.Fatal("the app directory was replaced by an ancestor-only ignore change")
	}
	afterFingerprint, err := app.Fingerprint(appDir)
	if err != nil {
		t.Fatalf("fingerprint app after ancestor-only revision: %v", err)
	}
	if afterFingerprint != beforeFingerprint {
		t.Fatalf("app fingerprint changed from %s to %s", beforeFingerprint, afterFingerprint)
	}
	afterTemplate, err := os.ReadFile(filepath.Join(appDir, "template.yaml"))
	if err != nil {
		t.Fatalf("read template after ancestor-only revision: %v", err)
	}
	if !bytes.Equal(afterTemplate, beforeTemplate) {
		t.Fatalf("app template changed from %q to %q", beforeTemplate, afterTemplate)
	}
	if _, err := os.Stat(filepath.Join(appDir, ".gitignore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor .gitignore was copied into the app; stat err = %v", err)
	}
}

// stateBackedDeps returns test dependencies whose OpenState opens an isolated
// PostgreSQL schema — the production injection architecture, unchanged. The
// returned DSN is available for tests that also need a direct handle.
func stateBackedDeps(t *testing.T) (Dependencies, string) {
	t.Helper()
	deps := testDeps(t)
	dsn := testutil.NewStateDSN(t)
	deps.OpenState = func(ctx context.Context) (*state.State, func(), error) {
		st, err := state.Open(ctx, state.Options{DSN: dsn})
		if err != nil {
			return nil, nil, err
		}
		return st, func() { _ = st.Close() }, nil
	}
	return deps, dsn
}

// TestGitSyncCLIStepsAreWriterOnlyAtDebug pins that a state-backed
// `relay git sync` writes its full user-facing step summary to the command
// writer and emits NO process-log output, even with a DEBUG-level process
// logger: the CLI sync never sets SyncOptions.Log, so structured diagnostics
// belong exclusively to the background worker/webhook path.
func TestGitSyncCLIStepsAreWriterOnlyAtDebug(t *testing.T) {
	deps, _ := stateBackedDeps(t)
	deps.GitCloneURL = testutil.NewBareRepo(t)
	if err := git.SetSource(deps.GitConfigPath, "git@github.com:acme/repo.git", "main", "", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	out, _, err := runCLIWithLoggerAndDeps(t, logger, deps, "", "git", "sync")
	if err != nil {
		t.Fatalf("git sync: %v (out=%s)", err, out)
	}
	for _, step := range []string{
		"Updating apps on this node...", "Updated 1 app(s): fn",
		"This node's apps are up to date",
	} {
		if !strings.Contains(out, step) {
			t.Fatalf("git sync writer output missing %q:\n%s", step, out)
		}
	}
	// The summary above is the complete presentation: no clone, reuse,
	// ref-resolution, or checkout narration reaches the operator.
	for _, narration := range []string{"Cloned", "Reusing existing checkout", "Resolved", "Checked out"} {
		if strings.Contains(out, narration) {
			t.Fatalf("git sync writer output narrates internals %q:\n%s", narration, out)
		}
	}
	if logs.Len() != 0 {
		t.Fatalf("CLI git sync emitted process-log output:\n%s", logs.String())
	}
	if _, err := os.Stat(filepath.Join(deps.GitAppsDir, "fn", "template.yaml")); err != nil {
		t.Fatalf("function not materialized: %v", err)
	}
}

// TestSeparateTreesIndependentGitCloneOverrides drives two independently
// constructed command trees, each with its own isolated state schema and its
// own local filesystem repo through its own deps.GitCloneURL, and pins the
// dependency-injection architecture: each tree captures its clone override at
// construction (mutating the source Dependencies afterwards cannot retarget a
// built tree), materializes only into its own apps dir from its own repo, and
// never persists the local bare path — both configs keep their validated SSH
// URLs.
func TestSeparateTreesIndependentGitCloneOverrides(t *testing.T) {
	depsA, _ := stateBackedDeps(t)
	depsB, _ := stateBackedDeps(t)
	bareA := testutil.New(t, testutil.Options{Template: "runtime: nodejs24.x\n"}).Bare
	bareB := testutil.New(t, testutil.Options{Template: "runtime: python3.14\n"}).Bare

	if _, _, err := runCLIWithDeps(t, depsA, "", "git", "set", "git@github.com:acme/repo-a.git"); err != nil {
		t.Fatalf("tree A git set: %v", err)
	}
	if _, _, err := runCLIWithDeps(t, depsB, "", "git", "set", "git@github.com:acme/repo-b.git"); err != nil {
		t.Fatalf("tree B git set: %v", err)
	}
	depsA.GitCloneURL = bareA
	depsB.GitCloneURL = bareB

	var outA, outB, errA, errB bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	cmdA := New(logger, &outA, depsA)
	cmdA.Reader = strings.NewReader("")
	cmdA.ErrWriter = &errA
	cmdB := New(logger, &outB, depsB)
	cmdB.Reader = strings.NewReader("")
	cmdB.ErrWriter = &errB

	// Changing the source Dependencies after construction cannot retarget
	// either command tree: each captured its own value at New time.
	depsA.GitCloneURL = bareB
	depsB.GitCloneURL = bareA

	if err := cmdA.Run(context.Background(), []string{"relay", "git", "sync"}); err != nil {
		t.Fatalf("tree A git sync: %v (out=%s)", err, outA.String())
	}
	if _, err := os.Stat(filepath.Join(depsB.GitAppsDir, "fn")); !os.IsNotExist(err) {
		t.Fatalf("tree B apps dir saw tree A's materialization: %v", err)
	}
	tmplA, err := os.ReadFile(filepath.Join(depsA.GitAppsDir, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read tree A materialized template: %v", err)
	}

	if err := cmdB.Run(context.Background(), []string{"relay", "git", "sync"}); err != nil {
		t.Fatalf("tree B git sync: %v (out=%s)", err, outB.String())
	}
	if errA.Len() != 0 || errB.Len() != 0 {
		t.Fatalf("CLI command errors: tree A %q; tree B %q", errA.String(), errB.String())
	}
	tmplB, err := os.ReadFile(filepath.Join(depsB.GitAppsDir, "fn", "template.yaml"))
	if err != nil {
		t.Fatalf("read tree B materialized template: %v", err)
	}
	if !strings.Contains(string(tmplA), "nodejs24.x") || strings.Contains(string(tmplA), "python3.14") {
		t.Fatalf("tree A materialized from the wrong source: %s", tmplA)
	}
	if !strings.Contains(string(tmplB), "python3.14") || strings.Contains(string(tmplB), "nodejs24.x") {
		t.Fatalf("tree B did not sync from its own local repo: %s", tmplB)
	}

	// The overrides were never persisted: each config keeps exactly the SSH
	// URL `git set` wrote, and no local bare path leaks into either file.
	cfgA, err := git.LoadConfig(depsA.GitConfigPath)
	if err != nil || cfgA.Repository != "git@github.com:acme/repo-a.git" {
		t.Fatalf("tree A config after sync = %+v (%v), want the persisted SSH URL", cfgA, err)
	}
	cfgB, err := git.LoadConfig(depsB.GitConfigPath)
	if err != nil || cfgB.Repository != "git@github.com:acme/repo-b.git" {
		t.Fatalf("tree B config after sync = %+v (%v), want the persisted SSH URL", cfgB, err)
	}
	for _, entry := range []struct{ config, bare string }{
		{config: depsA.GitConfigPath, bare: bareA},
		{config: depsB.GitConfigPath, bare: bareB},
	} {
		raw, err := os.ReadFile(entry.config)
		if err != nil {
			t.Fatalf("read config: %v", err)
		}
		if strings.Contains(string(raw), entry.bare) {
			t.Fatalf("config leaked the local transport override: %s", raw)
		}
	}
}

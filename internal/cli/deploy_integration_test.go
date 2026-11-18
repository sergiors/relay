//go:build integration

package cli

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	gogit "github.com/go-git/go-git/v5"

	"relay/internal/git"
	"relay/internal/state"
	"relay/internal/testutil"
)

// rollbackDeps builds the operator fixture for the simplified deploy surface:
// an isolated state schema behind the injected OpenState seam (the production
// architecture is unchanged), a configured git source WITH a monorepo path —
// the only source of repository/ref/path — and a local two-commit bare
// repository as the transport seam for rollback's commit verification.
// It returns the dependencies, a direct state handle, and the two commit SHAs.
func rollbackDeps(t *testing.T) (Dependencies, *state.State, string, string) {
	t.Helper()
	deps, dsn := stateBackedDeps(t)
	first, second := bareRepoWithTwoCommits(t, &deps)
	if err := git.SetSource(
		deps.GitConfigPath, "git@github.com:acme/r.git", "main", "services", ""); err != nil {
		t.Fatalf("set source: %v", err)
	}
	st := testutil.OpenState(t, dsn)
	return deps, st, first, second
}

// bareRepoWithTwoCommits builds a work+bare fixture whose main branch has two
// commits and points deps.GitCloneURL at the bare remote. It returns the first
// (original) and second commit SHAs.
func bareRepoWithTwoCommits(t *testing.T, deps *Dependencies) (first, second string) {
	t.Helper()
	repo := testutil.New(t, testutil.Options{})
	r, err := gogit.PlainOpen(repo.Work)
	if err != nil {
		t.Fatalf("open work repo: %v", err)
	}
	head, err := r.Head()
	if err != nil {
		t.Fatalf("work HEAD: %v", err)
	}
	first = head.Hash().String()

	wt, err := r.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	testutil.WriteFile(t, filepath.Join(repo.Work, "second.txt"), "second\n")
	if _, err := wt.Add("second.txt"); err != nil {
		t.Fatalf("add second.txt: %v", err)
	}
	second = testutil.Commit(t, wt, "second commit").String()
	testutil.SeedBare(t, repo.Bare, repo.Work)

	deps.GitCloneURL = repo.Bare
	return first, second
}

// TestDeployRollbackPublishesPinnedGenerationFromConfiguredSource pins the
// simplified rollback: one exact SHA, verified against the configured
// repository (fetch/resolve only), published as a NEW pinned generation whose
// repository/ref/path come exclusively from `relay git set` — and no
// materialization of /apps.
func TestDeployRollbackPublishesPinnedGenerationFromConfiguredSource(t *testing.T) {
	deps, st, first, _ := rollbackDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "deploy", "status")
	if err != nil {
		t.Fatalf("deploy status: %v", err)
	}
	if !strings.Contains(out, "No deployment is available yet.") {
		t.Fatalf("empty status output:\n%s", out)
	}

	out, _, err = runCLIWithDeps(t, deps, "", "deploy", "rollback", first)
	if err != nil {
		t.Fatalf("deploy rollback: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "Rolled back to "+first+"; deployment is pinned") {
		t.Fatalf("rollback output:\n%s", out)
	}

	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v), want the published row",
			desired, found, err)
	}
	if desired.Generation != 1 || desired.Commit != first || !desired.Pinned {
		t.Fatalf("desired = generation %d commit %s pinned %v, want generation 1 "+
			"commit %s pinned", desired.Generation, desired.Commit, desired.Pinned, first)
	}
	if desired.Publisher != "cli:rollback" {
		t.Fatalf("publisher = %q, want cli:rollback", desired.Publisher)
	}
	// Source identity from `relay git set` only (including the monorepo path).
	if desired.Repository != "git@github.com:acme/r.git" ||
		desired.Ref != "main" || desired.Path != "services" {
		t.Fatalf("source = (%q, %q, %q), want the configured git set identity",
			desired.Repository, desired.Ref, desired.Path)
	}

	// Verification fetches only: /apps is never materialized.
	if entries, err := filepath.Glob(filepath.Join(deps.GitAppsDir, "*")); err != nil ||
		len(entries) != 0 {
		t.Fatalf("apps dir entries = %v (err=%v), want no materialization", entries, err)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced || cfg.LastSyncedCommit != "" {
		t.Fatalf("config = %+v, want no sync bookkeeping from rollback", cfg)
	}
}

// TestDeployRollbackWhileAlreadyPinnedPublishesNewGeneration pins that rollback
// works as an operator override even while the deployment is already pinned:
// re-selecting the same commit is a no-op that reports the revision is already
// in use without advancing the generation, while a different commit writes a
// NEW pinned generation carrying the exact verified commit.
func TestDeployRollbackWhileAlreadyPinnedPublishesNewGeneration(t *testing.T) {
	deps, st, first, second := rollbackDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", first)
	if err != nil {
		t.Fatalf("first rollback: %v (out=%s)", err, out)
	}

	sameOut, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", first)
	if err != nil {
		t.Fatalf("same-commit rollback: %v (out=%s)", err, sameOut)
	}
	if !strings.Contains(sameOut, "Already using "+first+"; deployment remains pinned") {
		t.Fatalf("same-commit rollback output:\n%s", sameOut)
	}
	if desired, found, rerr := st.ReadDeploymentDesired(context.Background()); rerr != nil ||
		!found || desired.Generation != 1 || desired.Commit != first || !desired.Pinned {
		t.Fatalf("after same-commit rollback = (%+v, %v, %v), want generation 1 "+
			"commit %s still pinned", desired, found, rerr, first)
	}

	out, _, err = runCLIWithDeps(t, deps, "", "deploy", "rollback", second)
	if err != nil {
		t.Fatalf("second rollback while pinned: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "Rolled back to "+second+"; deployment is pinned") {
		t.Fatalf("second rollback output:\n%s", out)
	}

	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", desired, found, err)
	}
	if desired.Generation != 2 || desired.Commit != second || !desired.Pinned {
		t.Fatalf("desired = generation %d commit %s pinned %v, want generation 2 "+
			"commit %s pinned", desired.Generation, desired.Commit, desired.Pinned, second)
	}
}

// TestDeployRollbackRejectsMissingCommitWithoutPublishing pins that a SHA
// absent from the configured repository fails closed BEFORE publication: no
// generation is written and nothing is materialized.
func TestDeployRollbackRejectsMissingCommitWithoutPublishing(t *testing.T) {
	deps, st, _, _ := rollbackDeps(t)
	missing := "0123456789abcdef0123456789abcdef01234567"

	_, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", missing)
	if err == nil || !strings.Contains(err.Error(),
		"is not available in the configured repository") {
		t.Fatalf("err = %v, want the missing-commit refusal", err)
	}
	if _, found, rerr := st.ReadDeploymentDesired(context.Background()); rerr != nil || found {
		t.Fatalf("ReadDeploymentDesired = (_, %v, %v), want no row after a refused rollback",
			found, rerr)
	}
	if entries, gerr := filepath.Glob(filepath.Join(deps.GitAppsDir, "*")); gerr != nil ||
		len(entries) != 0 {
		t.Fatalf("apps dir entries = %v (err=%v), want no materialization", entries, gerr)
	}
}

// TestDeployPinUnpinPreserveRevisionWithoutMaterializing pins that pin/unpin
// only flip the policy: commit and generation are byte-identical before and
// after, and neither command fetches, syncs, or materializes anything.
func TestDeployPinUnpinPreserveRevisionWithoutMaterializing(t *testing.T) {
	deps, st, first, _ := rollbackDeps(t)

	out, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", first)
	if err != nil {
		t.Fatalf("rollback: %v (out=%s)", err, out)
	}
	before, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", before, found, err)
	}

	out, _, err = runCLIWithDeps(t, deps, "", "deploy", "pin")
	if err != nil {
		t.Fatalf("deploy pin: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "Deployment pinned at "+first+
		"; sync and webhook updates are blocked") {
		t.Fatalf("pin output:\n%s", out)
	}
	out, _, err = runCLIWithDeps(t, deps, "", "deploy", "unpin")
	if err != nil {
		t.Fatalf("deploy unpin: %v (out=%s)", err, out)
	}
	if !strings.Contains(out, "Deployment unpinned at "+first+
		"; current revision remains until the next sync or webhook update") {
		t.Fatalf("unpin output:\n%s", out)
	}

	after, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v)", after, found, err)
	}
	if after.Generation != before.Generation || after.Commit != before.Commit {
		t.Fatalf("pin/unpin moved the revision: generation %d->%d commit %s->%s, "+
			"want both preserved", before.Generation, after.Generation,
			before.Commit, after.Commit)
	}
	if after.Pinned {
		t.Fatal("desired still pinned after unpin")
	}

	// No fetch/sync/materialize happened for the policy flips.
	if entries, gerr := filepath.Glob(filepath.Join(deps.GitAppsDir, "*")); gerr != nil ||
		len(entries) != 0 {
		t.Fatalf("apps dir entries = %v (err=%v), want no materialization", entries, gerr)
	}
	cfg, err := git.LoadConfig(deps.GitConfigPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Synced || cfg.LastSyncedCommit != "" {
		t.Fatalf("config = %+v, want no sync bookkeeping from pin/unpin", cfg)
	}
}

// TestDeployStatusShowsNodeProgressWhileBehind pins the status rendering for a
// node that has observed a newer generation than it has applied (and whose
// latest attempt failed): the node table shows observed/applied/phase, and the
// per-app activation progress for the applied generation renders beneath the
// desired revision block.
func TestDeployStatusShowsNodeProgressWhileBehind(t *testing.T) {
	deps, st, first, second := rollbackDeps(t)
	ctx := context.Background()

	if out, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", first); err != nil {
		t.Fatalf("first rollback: %v (out=%s)", err, out)
	}
	if out, _, err := runCLIWithDeps(t, deps, "", "deploy", "rollback", second); err != nil {
		t.Fatalf("second rollback: %v (out=%s)", err, out)
	}

	// A node that observed generation 2 but still applies generation 1, whose
	// latest materialization attempt failed.
	acquired, err := st.AcquireDeploymentNode(ctx, "node-1", "inst-1", 2,
		state.DeploymentNodeProgress{
			NodeID: "node-1", Generation: 2,
			Phase: state.DeploymentNodeMaterializing, Commit: second,
		})
	if err != nil {
		t.Fatalf("acquire node: %v", err)
	}
	if landed, err := st.RecordDeploymentNodeProgress(ctx, "node-1", 2,
		acquired.Incarnation, state.DeploymentNodeProgress{
			NodeID: "node-1", Generation: 2,
			AppliedGeneration: 1, AppliedIncarnation: acquired.Incarnation,
			Phase: state.DeploymentNodeFailed, Commit: second,
			AppliedCommit: first, Error: "fetch failed",
		}); err != nil || !landed {
		t.Fatalf("record node progress = (%v, %v)", landed, err)
	}
	if landed, err := st.RecordDeploymentAppProgress(ctx, "node-1", "gated", 1,
		acquired.Incarnation, state.DeploymentAppProgress{
			NodeID: "node-1", App: "gated", Generation: 1, Instance: "inst-1",
			Phase: state.DeploymentAppActive, Fingerprint: "fp-gated",
			TemplateIdentity: "tid-gated", Commit: first,
		}); err != nil || !landed {
		t.Fatalf("record app progress = (%v, %v)", landed, err)
	}

	out, _, err := runCLIWithDeps(t, deps, "", "deploy", "status")
	if err != nil {
		t.Fatalf("deploy status: %v (out=%s)", err, out)
	}
	for _, want := range []string{
		"Desired generation 2",
		"Commit     " + second,
		"Source     git@github.com:acme/r.git ref=main path=services",
		"Pin state  pinned",
		"Apps on node-1 (active generation 1)",
		"fetch failed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("status output missing %q:\n%s", want, out)
		}
	}
	// The node table and per-app progress render their documented columns in
	// order (spacing is the tabwriter's, so match it loosely).
	nodeHeader := regexp.MustCompile(`NODE\s+DESIRED\s+ACTIVE\s+STATUS\s+` +
		`ACTIVE COMMIT\s+UPDATED\s+ERROR`)
	if !nodeHeader.MatchString(out) {
		t.Fatalf("status output missing the node column header:\n%s", out)
	}
	appHeader := regexp.MustCompile(`APP\s+STATUS\s+COMMIT\s+UPDATED\s+ERROR`)
	if !appHeader.MatchString(out) {
		t.Fatalf("status output missing the app column header:\n%s", out)
	}
	// The node row pairs observed=2, applied=1 with the failed phase and the
	// applied commit while the node is behind.
	nodeRow := regexp.MustCompile(`node-1\s+2\s+1\s+` +
		regexp.QuoteMeta(state.DeploymentNodeFailed) + `\s+` +
		regexp.QuoteMeta(first) + `\s`)
	if !nodeRow.MatchString(out) {
		t.Fatalf("status output missing the observed/applied node row:\n%s", out)
	}
	// The app activation progress row for the applied generation.
	appRow := regexp.MustCompile(`gated\s+` +
		regexp.QuoteMeta(state.DeploymentAppActive) + `\s+` +
		regexp.QuoteMeta(first) + `\s`)
	if !appRow.MatchString(out) {
		t.Fatalf("status output missing the app activation row:\n%s", out)
	}
}

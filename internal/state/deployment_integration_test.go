//go:build integration

package state

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Coordinated deployment state contracts. Every test runs on its own isolated
// PostgreSQL schema (openTestState) and fails rather than skips when the
// database is unreachable.
//
// The safe automatic-publication policy these tests pin: a webhook delivery is
// accepted only when its before SHA equals the persisted branch-tip anchor
// (BranchTip); commit timestamps are diagnostic metadata and can never override
// an anchor mismatch, and only manual publication may change the shared
// repository/ref/path identity.

const (
	deployCommitA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	deployCommitB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	deployCommitC = "cccccccccccccccccccccccccccccccccccccccc"
	deployCommitD = "dddddddddddddddddddddddddddddddddddddddd"
	deployCommitE = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	deployCommitZ = "9999999999999999999999999999999999999999"
)

const (
	sourceOldest = "2026-01-01T10:00:00Z"
	sourceOld    = "2026-01-02T10:00:00Z"
	sourceMid    = "2026-01-03T10:00:00Z"
	sourceNew    = "2026-01-04T10:00:00Z"
	sourceFuture = "2026-06-01T10:00:00Z"
)

const (
	testRepository = "git@github.com:acme/r.git"
	testRef        = "main"
)

// manualDeployRequest builds an operator publication against the default
// source identity: explicit pin policy, no source timestamp of its own, no
// automatic ordering, and the generation checked under the publication lock.
func manualDeployRequest(commit, note string, pinned bool) DeploymentPublishRequest {
	return manualSourceRequest(testRepository, testRef, "", commit, note, pinned)
}

// manualSourceRequest is an operator publication with an explicit source
// identity, for publications that deliberately change repository/ref/path.
func manualSourceRequest(
	repository, ref, path, commit, note string, pinned bool,
) DeploymentPublishRequest {
	return DeploymentPublishRequest{
		Desired: DeploymentDesired{
			Commit:     commit,
			Repository: repository,
			Ref:        ref,
			Path:       path,
			Publisher:  "cli",
			Note:       note,
			Pinned:     pinned,
		},
		ExpectedGeneration: -1,
		AllowPinned:        true,
	}
}

// webhookDeployRequest builds an automatic webhook publication from one push
// delivery against the default source identity: the provider's before SHA must
// chain from the persisted branch-tip anchor, the after SHA is the candidate
// revision, and the commit timestamp is diagnostic metadata only.
func webhookDeployRequest(previousCommit, commit, sourceTime string) DeploymentPublishRequest {
	return autoDeployRequest(
		previousCommit, commit, sourceTime, testRepository, testRef, "")
}

// autoDeployRequest is one automatic publication attempt against an explicit
// source identity.
func autoDeployRequest(
	previousCommit, commit, sourceTime, repository, ref, path string,
) DeploymentPublishRequest {
	return DeploymentPublishRequest{
		Desired: DeploymentDesired{
			Commit:     commit,
			Repository: repository,
			Ref:        ref,
			Path:       path,
			SourceTime: sourceTime,
			Publisher:  "webhook",
		},
		ExpectedGeneration: -1,
		RejectStaleSource:  true,
		PreviousCommit:     previousCommit,
	}
}

func publishOrFatal(t *testing.T, st *State, req DeploymentPublishRequest) DeploymentPublishResult {
	t.Helper()
	result, err := st.PublishDeploymentDesired(context.Background(), req)
	if err != nil {
		t.Fatalf("PublishDeploymentDesired: %v", err)
	}
	return result
}

// requireDesired reads back the shared row and fails unless it describes the
// given revision identity. The literal "*" is a wildcard for commit and
// branchTip, for callers that assert only the source identity or generation.
func requireDesiredRow(
	t *testing.T,
	st *State,
	generation int64,
	commit, repository, ref, path, branchTip string,
) DeploymentDesired {
	t.Helper()
	desired, found, err := st.ReadDeploymentDesired(context.Background())
	if err != nil || !found {
		t.Fatalf("ReadDeploymentDesired = (%+v, %v, %v), want a row", desired, found, err)
	}
	if desired.Generation != generation ||
		commit != "*" && !strings.EqualFold(desired.Commit, commit) ||
		desired.Repository != repository || desired.Ref != ref || desired.Path != path ||
		branchTip != "*" && !strings.EqualFold(desired.BranchTip, branchTip) {
		t.Fatalf("desired = generation %d commit %s source (%s, %s, %q) tip %s, "+
			"want generation %d commit %s source (%s, %s, %q) tip %s",
			desired.Generation, desired.Commit,
			desired.Repository, desired.Ref, desired.Path, desired.BranchTip,
			generation, commit, repository, ref, path, branchTip)
	}
	return desired
}

// requireRejected fails unless the attempt did not advance the shared row: the
// generation, commit, and source identity must all be unchanged.
func requireRejected(
	t *testing.T,
	st *State,
	result DeploymentPublishResult,
	generation int64,
	commit, repository, ref, path string,
) {
	t.Helper()
	if result.Published {
		t.Fatalf("publication was accepted (%+v), want it rejected", result)
	}
	requireDesiredRow(t, st, generation, commit, repository, ref, path, "*")
}

// TestPublishDeploymentDesiredIsIdempotent pins that repeating the exact same
// revision is a no-op: no new generation, no error, and the committed row still
// describes the first publication. A genuinely different revision advances the
// monotonic generation by exactly one.
func TestPublishDeploymentDesiredIsIdempotent(t *testing.T) {
	st := openTestState(t)

	first := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "first", false))
	if !first.Published || first.Desired.Generation != 1 {
		t.Fatalf("first publish = %+v, want published generation 1", first)
	}
	if first.Desired.Commit != deployCommitA || first.Desired.Pinned {
		t.Fatalf("first publish = %+v, want commit %s unpinned", first, deployCommitA)
	}
	// A first forward manual publication anchors itself so the next branch push
	// can chain from it.
	if !strings.EqualFold(first.Desired.BranchTip, deployCommitA) {
		t.Fatalf("first publish BranchTip = %q, want the published commit %s",
			first.Desired.BranchTip, deployCommitA)
	}

	repeat := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "second", false))
	if repeat.Published || repeat.Reason != DeploymentPublishReasonUnchanged {
		t.Fatalf("repeat publish = %+v, want an unchanged no-op", repeat)
	}
	if repeat.Desired.Generation != 1 {
		t.Fatalf("repeat advanced the generation to %d, want 1", repeat.Desired.Generation)
	}

	desired := requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
	if desired.Note != "first" {
		t.Fatalf("stored note = %q, want %q (an unchanged repeat writes nothing)", desired.Note, "first")
	}

	changed := publishOrFatal(t, st, manualDeployRequest(deployCommitB, "next", false))
	if !changed.Published || changed.Desired.Generation != 2 {
		t.Fatalf("changed publish = %+v, want published generation 2", changed)
	}
}

// TestRollbackIsNewGenerationPinsOlderCommit pins the rollback contract: an
// older commit is published as a NEW, higher generation — the row is never
// edited back to an older generation — so every node still observes a monotonic
// advance and materializes the rolled-back tree.
func TestRollbackIsNewGenerationPinsOlderCommit(t *testing.T) {
	st := openTestState(t)

	if r := publishOrFatal(t, st, manualDeployRequest(deployCommitB, "v2", false)); r.Desired.Generation != 1 {
		t.Fatalf("first publish generation = %d, want 1", r.Desired.Generation)
	}
	rollback := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "rollback to v1", true))
	if !rollback.Published || rollback.Desired.Generation != 2 {
		t.Fatalf("rollback = %+v, want published generation 2", rollback)
	}
	if rollback.Desired.Commit != deployCommitA || !rollback.Desired.Pinned {
		t.Fatalf("rollback = %+v, want commit %s pinned", rollback, deployCommitA)
	}
	requireDesiredRow(t, st, 2, deployCommitA, testRepository, testRef, "", "*")
}

// TestSetDeploymentPinnedPolicy pins the pin/unpin contract: an automatic push
// while pinned is ignored with the pinned reason (checked before every ordering
// rule), pinning and unpinning preserve the branch-tip anchor and never advance
// the generation, an operator publication overrides the pin explicitly, and
// after unpinning the next anchored chain is accepted again.
func TestSetDeploymentPinnedPolicy(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	if _, found, err := st.SetDeploymentPinned(ctx, true); err != nil || found {
		t.Fatalf("SetDeploymentPinned without a row = (_, %v, %v), want found=false", found, err)
	}

	// Seed the anchor through one accepted automatic publication.
	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
		t.Fatalf("seeding publication = %+v, want published generation 1", r)
	}

	pinned, found, err := st.SetDeploymentPinned(ctx, true)
	if err != nil || !found {
		t.Fatalf("SetDeploymentPinned(true) = (%+v, %v, %v), want found", pinned, found, err)
	}
	if !pinned.Pinned || pinned.Generation != 1 || pinned.Commit != deployCommitA {
		t.Fatalf("pinned desired = %+v, want pinned generation 1 commit %s", pinned, deployCommitA)
	}
	// Pinning is a policy flip only: the branch-tip anchor must survive it.
	if !strings.EqualFold(pinned.BranchTip, deployCommitA) {
		t.Fatalf("BranchTip after pin = %q, want the preserved anchor %s",
			pinned.BranchTip, deployCommitA)
	}

	// While pinned, an automatic push is ignored — and the pinned reason wins
	// even when the delivery would otherwise chain.
	blocked := publishOrFatal(t, st, webhookDeployRequest(deployCommitA, deployCommitB, sourceOld))
	if blocked.Published || blocked.Reason != DeploymentPublishReasonPinned {
		t.Fatalf("automatic push while pinned = %+v, want reason %q",
			blocked, DeploymentPublishReasonPinned)
	}

	// An operator publication overrides the pin explicitly (AllowPinned).
	override := publishOrFatal(t, st, manualDeployRequest(deployCommitB, "", false))
	if !override.Published || override.Desired.Generation != 2 || override.Desired.Pinned {
		t.Fatalf("manual override = %+v, want unpinned generation 2", override)
	}

	// Unpinning flips only the policy; the revision and generation stay put.
	unpinned, found, err := st.SetDeploymentPinned(ctx, false)
	if err != nil || !found {
		t.Fatalf("SetDeploymentPinned(false) = (%+v, %v, %v)", unpinned, found, err)
	}
	if unpinned.Pinned || unpinned.Generation != 2 || unpinned.Commit != deployCommitB {
		t.Fatalf("unpinned desired = %+v, want unpinned generation 2 commit %s",
			unpinned, deployCommitB)
	}
}

// TestUnpinResumesBranchChain pins that after a pin/unpin round trip the next
// delivery whose before SHA chains from the persisted anchor is accepted and
// advances both the generation and the anchor.
func TestUnpinResumesBranchChain(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
		t.Fatalf("seeding publication = %+v, want published generation 1", r)
	}
	if _, found, err := st.SetDeploymentPinned(ctx, true); err != nil || !found {
		t.Fatalf("SetDeploymentPinned(true) = (_, %v, %v)", found, err)
	}
	if _, found, err := st.SetDeploymentPinned(ctx, false); err != nil || !found {
		t.Fatalf("SetDeploymentPinned(false) = (_, %v, %v)", found, err)
	}

	automatic := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitA, deployCommitB, sourceMid))
	if !automatic.Published || automatic.Desired.Generation != 2 {
		t.Fatalf("anchored chain after unpin = %+v, want published generation 2", automatic)
	}
	if !strings.EqualFold(automatic.Desired.BranchTip, deployCommitB) {
		t.Fatalf("BranchTip after the accepted push = %q, want the advanced tip %s",
			automatic.Desired.BranchTip, deployCommitB)
	}
	requireDesiredRow(t, st, 2, deployCommitB, testRepository, testRef, "", deployCommitB)
}

// TestAutomaticPublicationBranchChainAccepted pins that a delivery whose
// before SHA equals the persisted anchor is the proven chain: it is accepted
// and advances the anchor to its after SHA — whatever the commit timestamp says
// (older, equal, future, or missing). The first-ever webhook publication
// establishes the anchor.
func TestAutomaticPublicationBranchChainAccepted(t *testing.T) {
	st := openTestState(t)

	first := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceOld))
	if !first.Published || first.Desired.Generation != 1 {
		t.Fatalf("first automatic publish = %+v, want published generation 1", first)
	}
	if !strings.EqualFold(first.Desired.BranchTip, deployCommitA) {
		t.Fatalf("first automatic BranchTip = %q, want the established tip %s",
			first.Desired.BranchTip, deployCommitA)
	}
	requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", deployCommitA)

	chain := []struct {
		name           string
		previousCommit string
		commit         string
		sourceTime     string
	}{
		{"chain with an older timestamp", deployCommitA, deployCommitB, sourceOldest},
		{"chain with an equal timestamp", deployCommitB, deployCommitC, sourceOldest},
		{"chain with a future timestamp", deployCommitC, deployCommitD, sourceFuture},
		{"chain with a missing timestamp", deployCommitD, deployCommitE, ""},
	}
	generation := int64(1)
	for _, step := range chain {
		generation++
		result := publishOrFatal(t, st,
			webhookDeployRequest(step.previousCommit, step.commit, step.sourceTime))
		if !result.Published || result.Desired.Generation != generation {
			t.Fatalf("%s = %+v, want published generation %d", step.name, result, generation)
		}
		if !strings.EqualFold(result.Desired.BranchTip, step.commit) {
			t.Fatalf("%s advanced BranchTip to %q, want %s",
				step.name, result.Desired.BranchTip, step.commit)
		}
		requireDesiredRow(t, st, generation, step.commit, testRepository, testRef, "", step.commit)
	}
}

// TestBranchTipRoundTripsThroughJSONB pins that the branch-tip anchor is
// durable: after every accepted publication and policy change it is present in
// the persisted JSONB payload AND decodes back through ReadDeploymentDesired.
// A broken round trip would silently destroy the anchor — every later chain
// would read as unorderable — so this test checks BOTH the raw stored payload
// and the decoded row after each transition.
func TestBranchTipRoundTripsThroughJSONB(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	check := func(step, tip string) {
		t.Helper()
		var raw string
		if err := st.db.QueryRowContext(ctx,
			`SELECT data::text FROM deployment_desired WHERE id = 1`,
		).Scan(&raw); err != nil {
			t.Fatalf("%s: read raw payload: %v", step, err)
		}
		// jsonb::text renders "key": value with a space; normalize so the check
		// is independent of the textual spacing.
		compact := strings.ReplaceAll(raw, ": ", ":")
		if !strings.Contains(compact, `"branch_tip":"`+tip+`"`) {
			t.Fatalf("%s: persisted JSONB payload does not carry branch_tip %q: %s",
				step, tip, raw)
		}
		desired, found, err := st.ReadDeploymentDesired(ctx)
		if err != nil || !found {
			t.Fatalf("%s: ReadDeploymentDesired = (%+v, %v, %v)", step, desired, found, err)
		}
		if !strings.EqualFold(desired.BranchTip, tip) {
			t.Fatalf("%s: decoded BranchTip = %q, want %q (JSONB round trip)",
				step, desired.BranchTip, tip)
		}
	}

	if r := publishOrFatal(t, st,
		webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
		t.Fatalf("first webhook publish = %+v, want published generation 1", r)
	}
	check("first automatic publication establishes the anchor", deployCommitA)

	if r := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitA, deployCommitB, sourceNew)); !r.Published {
		t.Fatalf("chain publish = %+v, want published generation 2", r)
	}
	check("an accepted chain advances the anchor", deployCommitB)

	if r := publishOrFatal(t, st,
		manualDeployRequest(deployCommitA, "rollback", true)); !r.Published {
		t.Fatalf("pinned rollback = %+v, want published generation 3", r)
	}
	check("a pinned rollback preserves the anchor", deployCommitB)

	if _, found, err := st.SetDeploymentPinned(ctx, true); err != nil || !found {
		t.Fatalf("SetDeploymentPinned(true) = (_, %v, %v)", found, err)
	}
	check("pin preserves the anchor", deployCommitB)

	if _, found, err := st.SetDeploymentPinned(ctx, false); err != nil || !found {
		t.Fatalf("SetDeploymentPinned(false) = (_, %v, %v)", found, err)
	}
	check("unpin preserves the anchor", deployCommitB)

	if r := publishOrFatal(t, st,
		manualDeployRequest(deployCommitC, "re-anchor", false)); !r.Published {
		t.Fatalf("unpinned manual publish = %+v, want published generation 4", r)
	}
	check("an unpinned manual publication re-anchors to its commit", deployCommitC)

	if r := publishOrFatal(t, st, manualSourceRequest(
		testRepository, "release", "", deployCommitD, "retarget", false)); !r.Published {
		t.Fatalf("manual source change = %+v, want published generation 5", r)
	}
	check("an unpinned manual source change re-anchors to its commit", deployCommitD)
}

// TestAutomaticPublicationOutOfOrderRejected pins the conservative rejection
// policy for deliveries that do not chain from the persisted anchor: a missing
// before SHA (or missing anchor) is ambiguous, and a mismatched before SHA is
// stale — even when its commit timestamp is in the future. Every rejection
// leaves the shared row untouched.
func TestAutomaticPublicationOutOfOrderRejected(t *testing.T) {
	tests := []struct {
		name           string
		previousCommit string
		sourceTime     string
		wantReason     string
	}{
		{"missing before SHA is ambiguous", "", sourceFuture,
			DeploymentPublishReasonAmbiguous},
		{"missing before SHA with a missing timestamp is ambiguous", "", "",
			DeploymentPublishReasonAmbiguous},
		{"mismatched before with a future timestamp is stale", deployCommitZ, sourceFuture,
			DeploymentPublishReasonStale},
		{"mismatched before with an older timestamp is stale", deployCommitZ, sourceOldest,
			DeploymentPublishReasonStale},
		{"mismatched before with an equal timestamp is stale", deployCommitZ, sourceMid,
			DeploymentPublishReasonStale},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestState(t)
			if r := publishOrFatal(t, st,
				webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
				t.Fatalf("seeding publication = %+v, want published generation 1", r)
			}
			attempt := publishOrFatal(t, st,
				webhookDeployRequest(tc.previousCommit, deployCommitB, tc.sourceTime))
			if attempt.Published || attempt.Reason != tc.wantReason {
				t.Fatalf("attempt = %+v, want rejection with reason %q",
					attempt, tc.wantReason)
			}
			requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
		})
	}
}

// TestAutomaticPublicationCannotChangeSourceIdentity pins that an automatic
// (webhook) publication can never rewrite the shared repository/ref/path of an
// already-published revision — not with a newer timestamp, and not even with a
// before SHA that chains from the anchor. Only a manual publication may change
// the source identity; a first-ever webhook publication may establish it.
func TestAutomaticPublicationCannotChangeSourceIdentity(t *testing.T) {
	t.Run("first publication establishes the identity", func(t *testing.T) {
		st := openTestState(t)
		result := publishOrFatal(t, st,
			autoDeployRequest("", deployCommitA, sourceOld, testRepository, "release", "services"))
		if !result.Published || result.Desired.Generation != 1 {
			t.Fatalf("first webhook publish = %+v, want published generation 1", result)
		}
		requireDesiredRow(t, st, 1, deployCommitA, testRepository, "release", "services", "*")
	})

	// The shared identity is established once and frozen against automatic
	// publications, whatever ordering evidence the delivery carries.
	freeze := []struct {
		name       string
		repository string
		ref        string
		path       string
	}{
		{"ref change", testRepository, "release", ""},
		{"repository change", "git@github.com:acme/other.git", testRef, ""},
		{"path change", testRepository, testRef, "services"},
	}
	for _, tc := range freeze {
		t.Run("automatic publication cannot change the "+tc.name, func(t *testing.T) {
			st := openTestState(t)
			if r := publishOrFatal(t, st,
				webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
				t.Fatalf("seeding publication = %+v, want published generation 1", r)
			}
			// Strongest possible automatic request: a chain from the anchor AND
			// a future source timestamp.
			attempt := publishOrFatal(t, st, autoDeployRequest(
				deployCommitA, deployCommitB, sourceFuture, tc.repository, tc.ref, tc.path))
			if attempt.Published ||
				attempt.Reason != DeploymentPublishReasonSourceMismatch {
				t.Fatalf("attempt = %+v, want rejection with reason %q",
					attempt, DeploymentPublishReasonSourceMismatch)
			}
			requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
		})
	}

	t.Run("manual publication may change the identity", func(t *testing.T) {
		st := openTestState(t)
		if r := publishOrFatal(t, st,
			webhookDeployRequest("", deployCommitA, sourceMid)); !r.Published {
			t.Fatalf("seeding publication = %+v, want published generation 1", r)
		}
		manual := manualSourceRequest(
			testRepository, "release", "services", deployCommitB, "operator retarget", false)
		result := publishOrFatal(t, st, manual)
		if !result.Published || result.Desired.Generation != 2 {
			t.Fatalf("manual identity change = %+v, want published generation 2", result)
		}
		requireDesiredRow(t, st, 2, deployCommitB, testRepository, "release", "services", "*")
	})
}

// TestPinnedRollbackPreservesAnchorAndChainResumes pins the rollback/pin
// contract around the branch-tip anchor: a PINNED manual rollback preserves the
// anchor (the last accepted automatic push), so after unpinning the next
// current-branch chain is accepted — even with a stale-looking timestamp —
// while a replayed out-of-order branch event whose before SHA no longer matches
// the advanced anchor is rejected whatever its timestamp.
func TestPinnedRollbackPreservesAnchorAndChainResumes(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceOld)); !r.Published {
		t.Fatalf("first publication = %+v, want published generation 1", r)
	}
	if r := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitA, deployCommitD, sourceNew)); !r.Published {
		t.Fatalf("branch advance = %+v, want published generation 2", r)
	}
	rollback := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "rollback", true))
	if !rollback.Published || rollback.Desired.Generation != 3 {
		t.Fatalf("rollback = %+v, want published generation 3", rollback)
	}
	// The rollback rewrites desired state but must NOT move the branch anchor.
	if !strings.EqualFold(rollback.Desired.BranchTip, deployCommitD) {
		t.Fatalf("rollback BranchTip = %q, want the preserved anchor %s",
			rollback.Desired.BranchTip, deployCommitD)
	}
	requireDesiredRow(t, st, 3, deployCommitA, testRepository, testRef, "", deployCommitD)

	if _, found, err := st.SetDeploymentPinned(ctx, false); err != nil || !found {
		t.Fatalf("unpin = (_, %v, %v)", found, err)
	}
	requireDesiredRow(t, st, 3, deployCommitA, testRepository, testRef, "", deployCommitD)

	// The next push on the branch chains from the preserved anchor (commit D),
	// not from the rolled-back desired commit — even though its timestamp looks
	// stale relative to the stored metadata.
	result := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitD, deployCommitE, sourceOldest))
	if !result.Published || result.Desired.Generation != 4 ||
		!strings.EqualFold(result.Desired.BranchTip, deployCommitE) {
		t.Fatalf("chain after unpin = %+v, want published generation 4 anchored at %s",
			result, deployCommitE)
	}
	requireDesiredRow(t, st, 4, deployCommitE, testRepository, testRef, "", deployCommitE)

	// A replayed delivery from before the anchor advanced must not overwrite
	// the revision, even with a future timestamp.
	attempt := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitB, deployCommitD, sourceFuture))
	if attempt.Published || attempt.Reason != DeploymentPublishReasonStale {
		t.Fatalf("replayed out-of-order event = %+v, want reason %q",
			attempt, DeploymentPublishReasonStale)
	}
	requireDesiredRow(t, st, 4, deployCommitE, testRepository, testRef, "", deployCommitE)
}

// TestUnpinnedManualPublishReanchorsBranchTip pins the documented recovery for
// a missed automatic chain: an UNPINNED same-source manual publication
// re-anchors the branch tip to its own commit. The next push must chain from
// that re-anchored tip; a push chaining from the pre-manual tip is now stale.
func TestUnpinnedManualPublishReanchorsBranchTip(t *testing.T) {
	st := openTestState(t)

	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceOld)); !r.Published {
		t.Fatalf("first publication = %+v, want published generation 1", r)
	}
	if r := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitA, deployCommitD, sourceNew)); !r.Published {
		t.Fatalf("branch advance = %+v, want published generation 2", r)
	}

	manual := publishOrFatal(t, st,
		manualDeployRequest(deployCommitA, "missed-chain recovery", false))
	if !manual.Published || manual.Desired.Generation != 3 ||
		!strings.EqualFold(manual.Desired.BranchTip, deployCommitA) {
		t.Fatalf("manual publish = %+v, want generation 3 re-anchored to %s",
			manual, deployCommitA)
	}
	requireDesiredRow(t, st, 3, deployCommitA, testRepository, testRef, "", deployCommitA)

	// The branch continues from the re-anchored tip: this push chains from the
	// manual commit and carries no timestamp of its own.
	result := publishOrFatal(t, st, webhookDeployRequest(deployCommitA, deployCommitB, ""))
	if !result.Published || result.Desired.Generation != 4 ||
		!strings.EqualFold(result.Desired.BranchTip, deployCommitB) {
		t.Fatalf("chain from the re-anchored tip = %+v, want published generation 4", result)
	}
	requireDesiredRow(t, st, 4, deployCommitB, testRepository, testRef, "", deployCommitB)

	// The pre-manual tip no longer chains.
	attempt := publishOrFatal(t, st,
		webhookDeployRequest(deployCommitD, deployCommitE, sourceFuture))
	if attempt.Published || attempt.Reason != DeploymentPublishReasonStale {
		t.Fatalf("stale pre-manual chain = %+v, want reason %q",
			attempt, DeploymentPublishReasonStale)
	}
	requireDesiredRow(t, st, 4, deployCommitB, testRepository, testRef, "", deployCommitB)
}

// TestRepeatedRevisionSemantics pins redelivery of the CURRENT commit: it is
// always an unchanged no-op — whatever the delivery's before SHA, since the
// current revision cannot overwrite itself — while a strictly newer provider
// timestamp still advances the diagnostic watermark and an older one never
// regresses it.
func TestRepeatedRevisionSemantics(t *testing.T) {
	st := openTestState(t)

	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceOld)); !r.Published {
		t.Fatalf("first publication = %+v, want published generation 1", r)
	}

	repeat := publishOrFatal(t, st, webhookDeployRequest(deployCommitA, deployCommitA, sourceNew))
	if repeat.Published || repeat.Reason != DeploymentPublishReasonUnchanged {
		t.Fatalf("newer redelivery = %+v, want an unchanged no-op", repeat)
	}
	desired := requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
	if desired.SourceTime != sourceNew {
		t.Fatalf("watermark = %q, want it advanced to %q", desired.SourceTime, sourceNew)
	}

	// An older redelivery must not regress the watermark.
	regress := publishOrFatal(t, st, webhookDeployRequest(deployCommitA, deployCommitA, sourceOldest))
	if regress.Published || regress.Reason != DeploymentPublishReasonUnchanged {
		t.Fatalf("older redelivery = %+v, want an unchanged no-op", regress)
	}
	desired = requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
	if desired.SourceTime != sourceNew {
		t.Fatalf("watermark = %q, want it kept at %q (never regress)", desired.SourceTime, sourceNew)
	}

	// A redelivery carrying a mismatched before SHA is still the SAME commit:
	// there is nothing to overwrite, so it is unchanged, not a chain attempt.
	mismatched := publishOrFatal(t, st, webhookDeployRequest(deployCommitZ, deployCommitA, sourceFuture))
	if mismatched.Published || mismatched.Reason != DeploymentPublishReasonUnchanged {
		t.Fatalf("same commit with a mismatched before = %+v, want an unchanged no-op", mismatched)
	}
	requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
}

// TestManualPublicationPreservesSourceOrderingMetadata pins the manual-
// publication metadata policy: an ADVANCING manual publication on the SAME
// source preserves the provider-timestamp watermark (diagnostic continuity),
// while a manual publication that deliberately changes the source coordinates
// starts clean — the old source's metadata never leaks into the new identity.
func TestManualPublicationPreservesSourceOrderingMetadata(t *testing.T) {
	st := openTestState(t)

	if r := publishOrFatal(t, st, webhookDeployRequest("", deployCommitA, sourceNew)); !r.Published {
		t.Fatalf("first publication = %+v, want published generation 1", r)
	}

	manual := publishOrFatal(t, st, manualDeployRequest(deployCommitB, "operator", false))
	if !manual.Published || manual.Desired.Generation != 2 {
		t.Fatalf("manual publish = %+v, want published generation 2", manual)
	}
	if manual.Desired.SourceTime != sourceNew {
		t.Fatalf("manual SourceTime = %q, want the same-source watermark %q preserved",
			manual.Desired.SourceTime, sourceNew)
	}

	// A delayed webhook on the same source that does not chain is still
	// rejected after the manual publication.
	attempt := publishOrFatal(t, st, webhookDeployRequest(deployCommitZ, deployCommitC, sourceOldest))
	requireRejected(t, st, attempt, 2, deployCommitB, testRepository, testRef, "")

	// A manual publication onto DIFFERENT source coordinates does not carry the
	// old source's ordering metadata with it.
	retarget := publishOrFatal(t, st,
		manualSourceRequest(testRepository, "release", "", deployCommitC, "retarget", false))
	if !retarget.Published || retarget.Desired.Generation != 3 {
		t.Fatalf("retarget = %+v, want published generation 3", retarget)
	}
	if retarget.Desired.SourceTime != "" {
		t.Fatalf("retarget SourceTime = %q, want empty (the old watermark is not inherited)",
			retarget.Desired.SourceTime)
	}
}

// TestPublishDeploymentDesiredCompareAndSwap pins the generation expectations:
// zero means "the row must be absent", a positive value must match the current
// generation exactly, -1 is the blind under-the-lock publication, and anything
// below -1 is a validation error that never reaches the database.
func TestPublishDeploymentDesiredCompareAndSwap(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	absent := manualDeployRequest(deployCommitA, "", false)
	absent.ExpectedGeneration = 5
	if _, err := st.PublishDeploymentDesired(ctx, absent); !errors.Is(err, ErrDeploymentConflict) {
		t.Fatalf("CAS against a missing row with expectation 5 = %v, want ErrDeploymentConflict", err)
	}

	create := manualDeployRequest(deployCommitA, "", false)
	create.ExpectedGeneration = 0
	created, err := st.PublishDeploymentDesired(ctx, create)
	if err != nil || !created.Published || created.Desired.Generation != 1 {
		t.Fatalf("CAS create = (%+v, %v), want published generation 1", created, err)
	}

	again := manualDeployRequest(deployCommitB, "", false)
	again.ExpectedGeneration = 0
	if _, err := st.PublishDeploymentDesired(ctx, again); !errors.Is(err, ErrDeploymentConflict) {
		t.Fatalf("CAS expectation 0 against an existing row = %v, want ErrDeploymentConflict", err)
	}

	future := manualDeployRequest(deployCommitB, "", false)
	future.ExpectedGeneration = 2
	if _, err := st.PublishDeploymentDesired(ctx, future); !errors.Is(err, ErrDeploymentConflict) {
		t.Fatalf("CAS expectation 2 against generation 1 = %v, want ErrDeploymentConflict", err)
	}

	match := manualDeployRequest(deployCommitB, "", false)
	match.ExpectedGeneration = 1
	matched, err := st.PublishDeploymentDesired(ctx, match)
	if err != nil || !matched.Published || matched.Desired.Generation != 2 {
		t.Fatalf("matching CAS = (%+v, %v), want published generation 2", matched, err)
	}

	bad := manualDeployRequest(deployCommitC, "", false)
	bad.ExpectedGeneration = -2
	if _, err := st.PublishDeploymentDesired(ctx, bad); err == nil ||
		!strings.Contains(err.Error(), "expected generation") {
		t.Fatalf("ExpectedGeneration -2 = %v, want a validation error", err)
	}
	requireDesiredRow(t, st, 2, deployCommitB, testRepository, testRef, "", "*")
}

// TestPublishDeploymentDesiredValidationWritesNothing pins that a rejected
// publication (bad SHA, empty repository/ref) never creates or mutates the
// shared row.
func TestPublishDeploymentDesiredValidationWritesNothing(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		req  DeploymentPublishRequest
	}{
		{"abbreviated commit", manualDeployRequest("deadbeef", "", false)},
		{"empty repository", func() DeploymentPublishRequest {
			r := manualDeployRequest(deployCommitA, "", false)
			r.Desired.Repository = "  "
			return r
		}()},
		{"empty ref", func() DeploymentPublishRequest {
			r := manualDeployRequest(deployCommitA, "", false)
			r.Desired.Ref = ""
			return r
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := st.PublishDeploymentDesired(ctx, tc.req); err == nil {
				t.Fatal("invalid publication = nil, want an error")
			}
		})
	}

	if _, found, err := st.ReadDeploymentDesired(ctx); err != nil || found {
		t.Fatalf("ReadDeploymentDesired = (_, %v, %v), want no row after rejected writes", found, err)
	}
}

// TestConcurrentPublishersSerializeGenerationAdvances pins the compare-and-swap
// core under real concurrency: distinct revisions racing through the advisory
// lock each land on their own unique generation (1..N, none lost, none
// duplicated), while identical revisions collapse to exactly one publication.
func TestConcurrentPublishersSerializeGenerationAdvances(t *testing.T) {
	t.Run("distinct revisions", func(t *testing.T) {
		st := openTestState(t)
		const publishers = 8
		commits := []string{
			deployCommitA, deployCommitB, deployCommitC, deployCommitD,
			deployCommitE,
			"1111111111111111111111111111111111111111",
			"2222222222222222222222222222222222222222",
			"3333333333333333333333333333333333333333",
		}
		var (
			wg   sync.WaitGroup
			mu   sync.Mutex
			got  []int64
			errs []error
		)
		start := make(chan struct{})
		for _, commit := range commits {
			wg.Add(1)
			go func(commit string) {
				defer wg.Done()
				req := manualDeployRequest(commit, "", false)
				<-start
				result, err := st.PublishDeploymentDesired(context.Background(), req)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				if result.Published {
					got = append(got, result.Desired.Generation)
				}
			}(commit)
		}
		close(start)
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("concurrent publishes failed: %v", errs[0])
		}
		if len(got) != publishers {
			t.Fatalf("published generations = %v, want %d distinct results", got, publishers)
		}
		seen := map[int64]bool{}
		for _, generation := range got {
			if generation < 1 || generation > publishers {
				t.Fatalf("generation %d outside 1..%d", generation, publishers)
			}
			if seen[generation] {
				t.Fatalf("generation %d published twice", generation)
			}
			seen[generation] = true
		}
		requireDesiredRow(t, st, publishers, "*", testRepository, testRef, "", "*")
	})

	t.Run("identical revision collapses to one", func(t *testing.T) {
		st := openTestState(t)
		const publishers = 8
		var (
			wg        sync.WaitGroup
			mu        sync.Mutex
			published int
			errs      []error
		)
		start := make(chan struct{})
		for i := 0; i < publishers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := manualDeployRequest(deployCommitA, "", false)
				<-start
				result, err := st.PublishDeploymentDesired(context.Background(), req)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs = append(errs, err)
					return
				}
				if result.Published {
					published++
				}
			}()
		}
		close(start)
		wg.Wait()

		if len(errs) > 0 {
			t.Fatalf("concurrent publishes failed: %v", errs[0])
		}
		if published != 1 {
			t.Fatalf("published = %d, want exactly 1 (identical revisions are idempotent)", published)
		}
		requireDesiredRow(t, st, 1, deployCommitA, testRepository, testRef, "", "*")
	})
}

// TestWithDeploymentDesiredLock pins the read-only check-and-swap seam: the
// callback observes the current desired revision (or found=false) while the
// advisory lock is held, its error is propagated to the caller, a nil callback
// is rejected, and the callback's outcome (an in-process swap) stands after the
// transaction releases the lock by rollback.
func TestWithDeploymentDesiredLock(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()

	var observed DeploymentDesired
	observedFound := true
	if err := st.WithDeploymentDesiredLock(ctx,
		func(desired DeploymentDesired, found bool) error {
			observed, observedFound = desired, found
			return nil
		}); err != nil {
		t.Fatalf("WithDeploymentDesiredLock: %v", err)
	}
	if observedFound || observed.Generation != 0 {
		t.Fatalf("callback observed (%+v, %v), want found=false before any publication",
			observed, observedFound)
	}

	if r := publishOrFatal(t, st, manualDeployRequest(deployCommitA, "", false)); !r.Published {
		t.Fatalf("seeding publication = %+v", r)
	}
	if err := st.WithDeploymentDesiredLock(ctx,
		func(desired DeploymentDesired, found bool) error {
			observed, observedFound = desired, found
			return nil
		}); err != nil {
		t.Fatalf("WithDeploymentDesiredLock: %v", err)
	}
	if !observedFound || observed.Generation != 1 || observed.Commit != deployCommitA {
		t.Fatalf("callback observed (%+v, %v), want the published revision", observed, observedFound)
	}

	sentinel := errors.New("callback failed")
	if err := st.WithDeploymentDesiredLock(ctx,
		func(DeploymentDesired, bool) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("callback error = %v, want it propagated", err)
	}
	if err := st.WithDeploymentDesiredLock(ctx, nil); err == nil {
		t.Fatal("nil callback = nil, want an error")
	}
}

// TestDeploymentNodeProgressFencing pins the process fences around node
// progress: a fresh incarnation is allocated on every acquisition (a restart
// takes ownership through a successor handle), the applied generation survives
// re-acquisition (a failed newer target never erases the authorized one),
// stale-incarnation/generation writes are normal no-ops, an older observed
// generation is superseded, identity the handle does not own is rejected, and
// independent nodes never observe each other's progress.
func TestDeploymentNodeProgressFencing(t *testing.T) {
	dsn := newTestDSN(t)
	ctx := context.Background()
	// Advisory locks are cluster-global, so the owner identities are unique to
	// this test even though their rows live in an isolated schema.
	nodeAID := uniqueNodeID(t, "node-a")
	nodeBID := uniqueNodeID(t, "node-b")

	// Given: the node's first process acquires generation 1 through its
	// node-owned handle.
	a1 := openDeployNodeOwner(t, dsn, nodeAID, "inst-1")
	first, err := a1.AcquireDeploymentNode(ctx, nodeAID, "inst-1", 1, DeploymentNodeProgress{
		NodeID: nodeAID, Generation: 1, Phase: DeploymentNodeMaterializing,
		Commit: deployCommitA,
	})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if first.Incarnation <= 0 || first.AppliedGeneration != 0 {
		t.Fatalf("first acquire = %+v, want a fresh incarnation and applied generation 0", first)
	}
	if first.Phase != DeploymentNodeMaterializing || first.Commit != deployCommitA {
		t.Fatalf("first acquire = %+v, want the supplied phase/commit", first)
	}

	landed, err := a1.RecordDeploymentNodeProgress(ctx, nodeAID, 1, first.Incarnation,
		DeploymentNodeProgress{
			NodeID: nodeAID, Generation: 1,
			AppliedGeneration: 1, AppliedIncarnation: first.Incarnation,
			Phase: DeploymentNodeMaterialized, Commit: deployCommitA,
			AppliedCommit: deployCommitA,
		})
	if err != nil || !landed {
		t.Fatalf("record progress = (%v, %v), want landed", landed, err)
	}

	// When: the node restarts under a new process instance. The successor
	// handle opens only after the predecessor closed (the node lock is
	// exclusive), and the re-acquisition of the SAME observed generation takes
	// a fresh incarnation while the applied authorization is preserved.
	a2 := reopenDeployNodeOwner(t, dsn, a1, "inst-2")
	reacquired, err := a2.AcquireDeploymentNode(ctx, nodeAID, "inst-2", 1, DeploymentNodeProgress{
		NodeID: nodeAID, Generation: 1, Phase: DeploymentNodeMaterialized,
		Commit: deployCommitA, AppliedCommit: deployCommitA,
	})
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if reacquired.Incarnation <= first.Incarnation {
		t.Fatalf("re-acquired incarnation %d, want a fresh one after %d",
			reacquired.Incarnation, first.Incarnation)
	}
	if reacquired.AppliedGeneration != 1 || reacquired.AppliedIncarnation != first.Incarnation {
		t.Fatalf("re-acquire = %+v, want applied generation 1 preserved", reacquired)
	}

	// Then: the superseded incarnation can no longer write.
	landed, err = a2.RecordDeploymentNodeProgress(ctx, nodeAID, 1, first.Incarnation,
		DeploymentNodeProgress{
			NodeID: nodeAID, Generation: 1,
			AppliedGeneration: 99, AppliedIncarnation: first.Incarnation,
			Phase: DeploymentNodeFailed, Commit: deployCommitA,
		})
	if err != nil || landed {
		t.Fatalf("stale-incarnation write = (%v, %v), want a no-op", landed, err)
	}
	// A mismatched generation with the CURRENT incarnation is also a no-op.
	landed, err = a2.RecordDeploymentNodeProgress(ctx, nodeAID, 7, reacquired.Incarnation,
		DeploymentNodeProgress{
			NodeID: nodeAID, Generation: 7,
			AppliedGeneration: 7, AppliedIncarnation: reacquired.Incarnation,
			Phase: DeploymentNodeMaterialized,
		})
	if err != nil || landed {
		t.Fatalf("mismatched-generation write = (%v, %v), want a no-op", landed, err)
	}
	// The current incarnation still writes.
	landed, err = a2.RecordDeploymentNodeProgress(ctx, nodeAID, 1, reacquired.Incarnation,
		DeploymentNodeProgress{
			NodeID: nodeAID, Generation: 1,
			AppliedGeneration: 1, AppliedIncarnation: reacquired.Incarnation,
			Phase: DeploymentNodeMaterialized, Commit: deployCommitA,
			AppliedCommit: deployCommitA,
		})
	if err != nil || !landed {
		t.Fatalf("current-incarnation write = (%v, %v), want landed", landed, err)
	}

	// Invalid acquisitions are rejected before touching the row: a
	// non-positive generation on a matching identity, and identities the
	// handle does not own.
	if _, err := a2.AcquireDeploymentNode(ctx, nodeAID, "inst-2", 0,
		DeploymentNodeProgress{NodeID: nodeAID}); err == nil {
		t.Fatal("acquire generation 0 = nil, want an error")
	}
	if _, err := a2.AcquireDeploymentNode(ctx, "", "inst-2", 1,
		DeploymentNodeProgress{}); err == nil {
		t.Fatal("acquire with an empty node ID = nil, want an error")
	}
	if _, err := a2.AcquireDeploymentNode(ctx, nodeAID, "inst-9", 1,
		DeploymentNodeProgress{NodeID: nodeAID}); err == nil {
		t.Fatal("acquire with a foreign instance = nil, want an error")
	}

	// When: node-a observes a newer generation (a fresh incarnation again
	// under its next restart), then an older observed generation is superseded
	// outright.
	a3 := reopenDeployNodeOwner(t, dsn, a2, "inst-3")
	nodeALatest, err := a3.AcquireDeploymentNode(ctx, nodeAID, "inst-3", 3,
		DeploymentNodeProgress{
			NodeID: nodeAID, Generation: 3,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitC,
		})
	if err != nil {
		t.Fatalf("acquire node-a generation 3: %v", err)
	}
	if _, err := a3.AcquireDeploymentNode(ctx, nodeAID, "inst-3", 2,
		DeploymentNodeProgress{NodeID: nodeAID, Generation: 2,
			Phase: DeploymentNodeMaterializing}); !errors.Is(err, ErrDeploymentNodeSuperseded) {
		t.Fatalf("acquire an older generation = %v, want ErrDeploymentNodeSuperseded", err)
	}

	// Independent node progress: node-b acquires its own generation without
	// observing node-a, and neither row changes the other.
	b := openDeployNodeOwner(t, dsn, nodeBID, "inst-1")
	nodeB, err := b.AcquireDeploymentNode(ctx, nodeBID, "inst-1", 4, DeploymentNodeProgress{
		NodeID: nodeBID, Generation: 4, Phase: DeploymentNodeMaterializing,
		Commit: deployCommitB,
	})
	if err != nil {
		t.Fatalf("acquire node-b: %v", err)
	}

	nodeA, found, err := a3.ReadDeploymentNode(ctx, nodeAID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode(node-a) = (%+v, %v, %v)", nodeA, found, err)
	}
	if nodeA.Generation != 3 || nodeA.AppliedGeneration != 1 ||
		nodeA.Incarnation != nodeALatest.Incarnation {
		t.Fatalf("node-a = %+v, want generation 3, applied 1, and its latest incarnation", nodeA)
	}
	nodeBB, found, err := b.ReadDeploymentNode(ctx, nodeBID)
	if err != nil || !found {
		t.Fatalf("ReadDeploymentNode(node-b) = (%+v, %v, %v)", nodeBB, found, err)
	}
	if nodeBB.Generation != 4 || nodeBB.Incarnation != nodeB.Incarnation {
		t.Fatalf("node-b = %+v, want generation 4 with its own incarnation", nodeBB)
	}
	nodes, err := a3.ReadDeploymentNodes(ctx)
	if err != nil || len(nodes) != 2 {
		t.Fatalf("ReadDeploymentNodes = (%d rows, %v), want 2 nodes", len(nodes), err)
	}
	if nodes[0].NodeID != nodeAID || nodes[1].NodeID != nodeBID {
		t.Fatalf("node order = [%s %s], want [%s %s]",
			nodes[0].NodeID, nodes[1].NodeID, nodeAID, nodeBID)
	}
}

// TestDeploymentAppProgressFencing pins the per-app activation fence: writes
// land only for the node's CURRENT incarnation while the generation matches
// either the observed or the applied generation, a stale incarnation or a
// foreign generation is a no-op, and pruning removes only rows below the new
// applied generation.
func TestDeploymentAppProgressFencing(t *testing.T) {
	dsn := newTestDSN(t)
	ctx := context.Background()
	// Advisory locks are cluster-global, so this fixture's node identity is
	// unique to the test even though its rows live in an isolated schema.
	nodeID := uniqueNodeID(t, "node-app")

	st := openDeployNodeOwner(t, dsn, nodeID, "inst-1")
	firstIncarnation, err := st.AcquireDeploymentNode(ctx, nodeID, "inst-1", 2,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2, Phase: DeploymentNodeMaterializing,
			Commit: deployCommitB,
		})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// Restart under the successor process instance: the successor handle opens
	// only after the predecessor's closed (the node lock is exclusive).
	st = reopenDeployNodeOwner(t, dsn, st, "inst-2")
	node, err := st.AcquireDeploymentNode(ctx, nodeID, "inst-2", 2, DeploymentNodeProgress{
		NodeID: nodeID, Generation: 2, Phase: DeploymentNodeMaterializing,
		Commit: deployCommitB,
	})
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if node.Incarnation <= firstIncarnation.Incarnation {
		t.Fatalf("re-acquired incarnation %d, want a fresh one after %d",
			node.Incarnation, firstIncarnation.Incarnation)
	}
	// Applied generation 1 (previous tree) while observed generation is 2.
	if landed, err := st.RecordDeploymentNodeProgress(ctx, nodeID, 2, node.Incarnation,
		DeploymentNodeProgress{
			NodeID: nodeID, Generation: 2,
			AppliedGeneration: 1, AppliedIncarnation: node.Incarnation,
			Phase: DeploymentNodeMaterializing, Commit: deployCommitB,
			AppliedCommit: deployCommitA,
		}); err != nil || !landed {
		t.Fatalf("record applied progress = (%v, %v)", landed, err)
	}

	appReq := func(generation int64, phase string) DeploymentAppProgress {
		return DeploymentAppProgress{
			NodeID: nodeID, App: "fn", Generation: generation,
			Instance: "inst-2", Phase: phase,
			Fingerprint: "fp-" + phase, TemplateIdentity: "tid", Commit: deployCommitB,
		}
	}

	// Observed generation (2) and applied generation (1) both accept writes
	// from the current incarnation.
	for _, generation := range []int64{2, 1} {
		if landed, err := st.RecordDeploymentAppProgress(
			ctx, nodeID, "fn", generation, node.Incarnation,
			appReq(generation, DeploymentAppPending)); err != nil || !landed {
			t.Fatalf("app progress for generation %d = (%v, %v), want landed",
				generation, landed, err)
		}
	}
	// A generation the node has neither observed nor applied is rejected.
	if landed, err := st.RecordDeploymentAppProgress(
		ctx, nodeID, "fn", 7, node.Incarnation,
		appReq(7, DeploymentAppActive)); err != nil || landed {
		t.Fatalf("app progress for a foreign generation = (%v, %v), want a no-op", landed, err)
	}
	// The superseded incarnation is rejected even for a matching generation.
	if landed, err := st.RecordDeploymentAppProgress(
		ctx, nodeID, "fn", 2, firstIncarnation.Incarnation,
		appReq(2, DeploymentAppFailed)); err != nil || landed {
		t.Fatalf("app progress with a stale incarnation = (%v, %v), want a no-op", landed, err)
	}
	// Validation: missing identifiers are errors, not silent writes.
	if _, err := st.RecordDeploymentAppProgress(
		ctx, nodeID, "", 2, node.Incarnation, appReq(2, DeploymentAppActive)); err == nil {
		t.Fatal("app progress without an app name = nil, want an error")
	}

	rows, err := st.ReadDeploymentAppProgress(ctx, nodeID, 0)
	if err != nil || len(rows) != 2 {
		t.Fatalf("ReadDeploymentAppProgress(all) = (%d rows, %v), want 2 rows", len(rows), err)
	}
	rows, err = st.ReadDeploymentAppProgress(ctx, nodeID, 2)
	if err != nil || len(rows) != 1 || rows[0].Phase != DeploymentAppPending {
		t.Fatalf("ReadDeploymentAppProgress(2) = (%+v, %v), want the generation-2 row", rows, err)
	}
	if rows[0].Fingerprint != "fp-"+DeploymentAppPending || rows[0].Commit != deployCommitB {
		t.Fatalf("generation-2 row = %+v, want the recorded fingerprint and commit", rows[0])
	}

	// Pruning below the new applied generation removes only older rows.
	if err := st.PruneDeploymentAppProgress(ctx, nodeID, 2); err != nil {
		t.Fatalf("PruneDeploymentAppProgress: %v", err)
	}
	rows, err = st.ReadDeploymentAppProgress(ctx, nodeID, 0)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("rows after prune = (%+v, %v), want only the generation-2 row", rows, err)
	}
	if err := st.PruneDeploymentAppProgress(ctx, nodeID, 0); err != nil {
		t.Fatalf("PruneDeploymentAppProgress(0): %v", err)
	}
}

// TestLocalWritesPreserveSourceGenerationFence pins the uncoordinated-writer
// contract: a local (sourceGeneration=0) RecordDiscovered/RecordPreparing
// against an existing coordinated row must NOT clear the row's nonzero
// SourceGeneration fence — and the preserved fence must keep rejecting a
// success that would publish a NEW active fingerprint once the shared desired
// generation has advanced.
func TestLocalWritesPreserveSourceGenerationFence(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()
	fn := fnFor(t, "fence-local", mustTemplate(t, twoHandlerTmpl))
	preparedAt := time.Now()

	// Generation 1 is coordinated: a stamped preparation publishes normally.
	publishOrFatal(t, st, manualDeployRequest(deployCommitA, "", false))
	token, err := st.RecordPreparingWithSourceGeneration(ctx, fn.Name, fn, "fp-1", 1)
	if err != nil || token <= 0 {
		t.Fatalf("stamped preparation = (%d, %v)", token, err)
	}
	if landed, err := st.RecordReconcileSuccess(
		ctx, fn.Name, "img-1", "fp-1", preparedAt, fn, token); err != nil || !landed {
		t.Fatalf("first success = (%v, %v), want landed", landed, err)
	}
	if detail, ok := st.GetApp(fn.Name); !ok || detail.SourceGeneration != 1 {
		t.Fatalf("detail = (%+v, %v), want SourceGeneration 1", detail, ok)
	}

	// Uncoordinated (zero-generation) writes must preserve the fence. Their
	// fencing token becomes the row's current one, so the fence assertions
	// below exercise the SourceGeneration rule — not a stale-token miss.
	if localToken, err := st.RecordDiscoveredWithFingerprint(ctx, fn, "fp-2"); err != nil ||
		localToken <= 0 {
		t.Fatalf("local discovery = (%d, %v)", localToken, err)
	}
	if detail, _ := st.GetApp(fn.Name); detail.SourceGeneration != 1 {
		t.Fatalf("SourceGeneration after local discovery = %d, want the fence kept at 1",
			detail.SourceGeneration)
	}
	localToken, err := st.RecordPreparingWithFingerprint(ctx, fn.Name, fn, "fp-3")
	if err != nil || localToken <= 0 {
		t.Fatalf("local preparation = (%d, %v)", localToken, err)
	}
	if detail, _ := st.GetApp(fn.Name); detail.SourceGeneration != 1 {
		t.Fatalf("SourceGeneration after local preparation = %d, want the fence kept at 1",
			detail.SourceGeneration)
	}

	// The shared revision advances; the preserved fence must still reject a
	// success that would publish a NEW active fingerprint.
	publishOrFatal(t, st, manualDeployRequest(deployCommitB, "", false))
	landed, err := st.RecordReconcileSuccess(
		ctx, fn.Name, "img-2", "fp-2", preparedAt, fn, localToken)
	if err != nil {
		t.Fatalf("stale success errored: %v", err)
	}
	if landed {
		t.Fatal("a local zero-generation write cleared the fence: the stale " +
			"source generation must still refuse a new active fingerprint")
	}
	detail, ok := st.GetApp(fn.Name)
	if !ok {
		t.Fatal("app row disappeared after the refused publication")
	}
	if detail.Image != "img-1" || detail.Fingerprint != "fp-1" {
		t.Fatalf("active generation = %q/%q, want img-1/fp-1 retained",
			detail.Image, detail.Fingerprint)
	}
	if detail.SourceGeneration != 1 {
		t.Fatalf("SourceGeneration = %d, want the preserved fence at 1", detail.SourceGeneration)
	}

	// Re-stamping the already-active fingerprint stays allowed after the local
	// writes: service convergence of the serving generation is never blocked.
	landed, err = st.RecordReconcileSuccess(
		ctx, fn.Name, "img-1", "fp-1", preparedAt, fn, localToken)
	if err != nil || !landed {
		t.Fatalf("same-fingerprint re-stamp = (%v, %v), want landed", landed, err)
	}

	// A COORDINATED preparation stamped with a newer generation advances the
	// preserved fence, and that generation's success then publishes normally.
	advToken, err := st.RecordPreparingWithSourceGeneration(ctx, fn.Name, fn, "fp-2", 2)
	if err != nil || advToken <= 0 {
		t.Fatalf("coordinated preparation = (%d, %v)", advToken, err)
	}
	if detail, _ := st.GetApp(fn.Name); detail.SourceGeneration != 2 {
		t.Fatalf("SourceGeneration after a coordinated preparation = %d, "+
			"want the fence advanced to 2", detail.SourceGeneration)
	}
	landed, err = st.RecordReconcileSuccess(
		ctx, fn.Name, "img-2", "fp-2", preparedAt, fn, advToken)
	if err != nil || !landed {
		t.Fatalf("coordinated success = (%v, %v), want landed", landed, err)
	}
	if detail, ok := st.GetApp(fn.Name); !ok ||
		detail.SourceGeneration != 2 || detail.Fingerprint != "fp-2" {
		t.Fatalf("detail = (%+v, %v), want the generation-2 publication", detail, ok)
	}
}

// TestRecordReconcileSuccessSourceGenerationFence pins the state-level
// publication fence: a preparation stamped with deployment generation G cannot
// publish a NEW active fingerprint once the shared desired generation differs
// (including a rollback with identical bytes), while re-stamping the
// already-active fingerprint (service convergence of the still-serving
// generation) is always allowed, and a preparation stamped with the current
// generation publishes normally.
func TestRecordReconcileSuccessSourceGenerationFence(t *testing.T) {
	st := openTestState(t)
	ctx := context.Background()
	fn := fnFor(t, "fenced-app", mustTemplate(t, twoHandlerTmpl))
	preparedAt := time.Now()

	// Generation 1 is the shared desired revision.
	publishOrFatal(t, st, manualDeployRequest(deployCommitA, "", false))

	// A preparation stamped with generation 1 publishes normally.
	token, err := st.RecordPreparingWithSourceGeneration(ctx, fn.Name, fn, "fp-1", 1)
	if err != nil || token <= 0 {
		t.Fatalf("RecordPreparingWithSourceGeneration = (%d, %v), want a positive token", token, err)
	}
	landed, err := st.RecordReconcileSuccess(
		ctx, fn.Name, "img-1", "fp-1", preparedAt, fn, token)
	if err != nil || !landed {
		t.Fatalf("first success = (%v, %v), want landed", landed, err)
	}

	// The shared revision advances (for example a rollback to other bytes).
	publishOrFatal(t, st, manualDeployRequest(deployCommitB, "rollback", true))

	// A preparation still stamped with the STALE generation cannot publish a
	// new active fingerprint.
	staleToken, err := st.RecordPreparingWithSourceGeneration(ctx, fn.Name, fn, "fp-2", 1)
	if err != nil || staleToken <= 0 {
		t.Fatalf("stale preparation = (%d, %v)", staleToken, err)
	}
	landed, err = st.RecordReconcileSuccess(
		ctx, fn.Name, "img-2", "fp-2", preparedAt, fn, staleToken)
	if err != nil {
		t.Fatalf("stale success errored: %v", err)
	}
	if landed {
		t.Fatal("stale-generation success landed; the publication fence must refuse it")
	}
	detail, ok := st.GetApp(fn.Name)
	if !ok {
		t.Fatal("app row disappeared after the refused publication")
	}
	if detail.Image != "img-1" || detail.Fingerprint != "fp-1" {
		t.Fatalf("active generation = %q/%q, want the previous img-1/fp-1",
			detail.Image, detail.Fingerprint)
	}

	// Re-stamping the ALREADY-ACTIVE fingerprint is allowed at any generation:
	// service convergence of the serving generation is never blocked.
	landed, err = st.RecordReconcileSuccess(
		ctx, fn.Name, "img-1", "fp-1", preparedAt, fn, staleToken)
	if err != nil || !landed {
		t.Fatalf("same-fingerprint re-stamp = (%v, %v), want landed", landed, err)
	}

	// A preparation stamped with the CURRENT generation publishes normally.
	currentToken, err := st.RecordPreparingWithSourceGeneration(ctx, fn.Name, fn, "fp-2", 2)
	if err != nil || currentToken <= 0 {
		t.Fatalf("current preparation = (%d, %v)", currentToken, err)
	}
	landed, err = st.RecordReconcileSuccess(
		ctx, fn.Name, "img-2", "fp-2", preparedAt, fn, currentToken)
	if err != nil || !landed {
		t.Fatalf("current-generation success = (%v, %v), want landed", landed, err)
	}
	detail, ok = st.GetApp(fn.Name)
	if !ok || detail.Image != "img-2" || detail.Fingerprint != "fp-2" {
		t.Fatalf("active generation = (%+v, %v), want img-2/fp-2 published", detail, ok)
	}
	if detail.SourceGeneration != 2 {
		t.Fatalf("SourceGeneration = %d, want 2 (the stamped generation)", detail.SourceGeneration)
	}
}

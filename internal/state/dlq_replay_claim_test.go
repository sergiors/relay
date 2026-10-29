//go:build integration

// This file pins the PostgreSQL replay-admission contract: ClaimDLQReplay
// leases one settled entry at a time under a real row lock, the fencing token
// is monotonic, a stale claimant can neither release nor delete a newer
// claimant's row, admin removal and retention preserve an active claim, and
// lease expiry (test-time short leases, never the 10m production value) is the
// recovery path. Real PostgreSQL is required so the row-lock serialization and
// the database-time lease comparison are exercised, not simulated.
package state

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/dlq"
)

// shortLease is the test-time lease used where the tests must observe expiry
// without waiting out dlq.ReplayLease (10m).
const shortLease = 40 * time.Millisecond

// claimFixture persists one settled, replayable entry and returns it: exactly
// the fixture state `relay dlq replay`'s claim requires.
func claimFixture(t *testing.T, c *State, msgID, invocation string) dlq.Entry {
	t.Helper()
	ctx := context.Background()
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", msgID)
	entry := dlqEntryFor(deliveryID, invocation, `{"event_name":"INSERT"}`, "exhausted: retry attempts", time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC))
	if inserted, err := c.SaveDLQEntry(ctx, entry); err != nil || !inserted {
		t.Fatalf("seed %s = (%v,%v)", entry.ID, inserted, err)
	}
	if err := c.MarkDLQDeliverySettled(ctx, deliveryID, time.Date(2026, 10, 1, 8, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("settle %s: %v", entry.ID, err)
	}
	return entry
}

// claimRowOf reads one entry's raw replay-claim columns, so tests observe the
// row's fencing state independent of any API wrapping: the unclaimed lease is 0
// and the unclaimed token stays 0.
func claimRowOf(t *testing.T, c *State, id string) (token, leaseUntilMs int64) {
	t.Helper()
	err := c.db.QueryRowContext(context.Background(),
		`SELECT replay_claim_token, replay_lease_until_ms FROM dlq WHERE id = $1`, id).
		Scan(&token, &leaseUntilMs)
	if err != nil {
		t.Fatalf("read claim columns of %s: %v", id, err)
	}
	return token, leaseUntilMs
}

// dbNowMillisInt reads the database's current time in milliseconds, so tests
// compare lease stamps on the SAME clock the store claims on (never the test
// process's clock).
func dbNowMillisInt(t *testing.T, c *State) int64 {
	t.Helper()
	var ms int64
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT (EXTRACT(EPOCH FROM now()) * 1000)::bigint`).Scan(&ms); err != nil {
		t.Fatalf("read database now: %v", err)
	}
	return ms
}

// TestDLQReplayClaimAdmitsExactlyOneConcurrentClaimant races many concurrent
// claimants against real PostgreSQL: exactly one claim wins (with the entry in
// hand and token 1) and every other claimant is refused without mutating the
// row — two concurrent replays can never both be admitted.
func TestDLQReplayClaimAdmitsExactlyOneConcurrentClaimant(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-race-1", "fn/index.run")

	const claimants = 8
	type claimResult struct {
		claim dlq.ReplayClaim
		err   error
	}
	results := make([]claimResult, claimants)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < claimants; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i].claim, results[i].err = c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
		}(i)
	}
	close(start)
	wg.Wait()

	var winner *claimResult
	winnerAt := -1
	for i := range results {
		switch {
		case results[i].err == nil:
			if winner != nil {
				t.Fatalf("claimant %d admitted alongside %d; exactly one claim must win", i, winnerAt)
			}
			winner, winnerAt = &results[i], i
		case errors.Is(results[i].err, dlq.ErrReplayInProgress):
			// The expected concurrent refusal.
		default:
			t.Fatalf("claimant %d: unexpected error %v", i, results[i].err)
		}
	}
	if winner == nil {
		t.Fatalf("no claimant won the race: %+v", results)
	}

	// The winner holds the entry itself, a token from a fresh row (0 -> 1), and
	// an unexpired lease stamped in database time.
	if winner.claim.Token != 1 {
		t.Fatalf("winner token = %d, want 1", winner.claim.Token)
	}
	if winner.claim.Entry.ID != entry.ID || winner.claim.Entry.App != "fn" ||
		winner.claim.Entry.Handler != "index.run" || winner.claim.Entry.Event != `{"event_name":"INSERT"}` {
		t.Fatalf("winner entry = %+v, want exactly the stored entry", winner.claim.Entry)
	}
	if winner.claim.Entry.SettledAt == "" {
		t.Fatal("the claimed entry must be the settled row")
	}
	token, leaseUntilMs := claimRowOf(t, c, entry.ID)
	if token != 1 || leaseUntilMs <= dbNowMillisInt(t, c) {
		t.Fatalf("row after the race: token=%d lease=%d, want the winner's unexpired claim", token, leaseUntilMs)
	}
	if want := time.UnixMilli(leaseUntilMs).UTC(); !winner.claim.LeaseUntil.Equal(want) {
		t.Fatalf("LeaseUntil = %v, want the row's lease stamp %v", winner.claim.LeaseUntil, want)
	}
}

// TestDLQReplayClaimRefusesSecondClaimantWhileLeaseHeld proves the sequential
// boundary: with a claim held, a second claim is refused with
// ErrReplayInProgress and the refused attempt leaves the row (its fencing
// columns and its payload) exactly as the active claimant's claim left it.
func TestDLQReplayClaimRefusesSecondClaimantWhileLeaseHeld(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-second-1", "fn/index.run")

	claim, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	second, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("second claim err = %v / claim %+v, want ErrReplayInProgress", err, second)
	}
	// The refused attempt mutated nothing.
	token, leaseUntilMs := claimRowOf(t, c, entry.ID)
	if token != claim.Token || leaseUntilMs != claim.LeaseUntil.UnixMilli() {
		t.Fatalf("refused claim changed the row: token=%d lease=%d, want %d/%d",
			token, leaseUntilMs, claim.Token, claim.LeaseUntil.UnixMilli())
	}
	kept, ok, err := c.GetDLQEntry(ctx, entry.ID)
	if err != nil || !ok || kept.Event != entry.Event || kept.Reason != entry.Reason {
		t.Fatalf("refused claim must not touch the payload: (%+v, %v, %v)", kept, ok, err)
	}
}

// TestDLQReplayClaimClassifiesMissingUnsettledAndInvalidLease proves the claim
// refuses exactly the ineligible candidates with their explicit sentinels: an
// unknown ID, an unsettled entry, and a non-positive lease.
func TestDLQReplayClaimClassifiesMissingUnsettledAndInvalidLease(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	settled := claimFixture(t, c, "claim-classify-1", "fn/index.run")

	if _, err := c.ClaimDLQReplay(ctx, "no-such-entry", dlq.ReplayLease); !errors.Is(err, dlq.ErrEntryNotFound) {
		t.Fatalf("unknown claim err = %v, want ErrEntryNotFound", err)
	}

	// An unsettled entry (its delivery may still be recoverable) is refused
	// before anything like a worker dial could happen.
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "claim-classify-2")
	unsettled := dlqEntryFor(deliveryID, "fn/index.run", `{"event_name":"INSERT"}`, "exhausted", time.Now())
	if inserted, err := c.SaveDLQEntry(ctx, unsettled); err != nil || !inserted {
		t.Fatalf("seed unsettled = (%v,%v)", inserted, err)
	}
	if _, err := c.ClaimDLQReplay(ctx, unsettled.ID, dlq.ReplayLease); !errors.Is(err, dlq.ErrEntryUnsettled) {
		t.Fatalf("unsettled claim err = %v, want ErrEntryUnsettled", err)
	}

	// A non-positive lease is a caller error, not an admission.
	if _, err := c.ClaimDLQReplay(ctx, settled.ID, 0); err == nil {
		t.Fatal("a zero lease must be refused")
	}
	if _, err := c.ClaimDLQReplay(ctx, settled.ID, -time.Minute); err == nil {
		t.Fatal("a negative lease must be refused")
	}
	// The validation refusals left the settled row unclaimed and claimable.
	if _, err := c.ClaimDLQReplay(ctx, settled.ID, time.Minute); err != nil {
		t.Fatalf("claim after invalid requests: %v", err)
	}
}

// TestDLQReplayClaimExpiredLeaseAdmitsNewerFencedToken proves expiry is the
// recovery path: once the lease lapses, a newer claimant is admitted with the
// next monotonic token, and that newer claim in turn blocks until its own lease
// lapses. Short/medium leases keep the waits bounded in the test.
func TestDLQReplayClaimExpiredLeaseAdmitsNewerFencedToken(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-expiry-1", "fn/index.run")

	first, err := c.ClaimDLQReplay(ctx, entry.ID, shortLease)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// While the (short) lease is still live, a retry is refused.
	if _, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute); !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("retry while leased err = %v, want ErrReplayInProgress", err)
	}

	// When the lease lapses...
	waitForLeaseExpiry(t, first.LeaseUntil)
	// ...a newer claimant is admitted with the next fencing token.
	second, err := c.ClaimDLQReplay(ctx, entry.ID, 2*time.Second)
	if err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if second.Token != first.Token+1 {
		t.Fatalf("token = %d, want the monotonic next token %d", second.Token, first.Token+1)
	}
	if !second.LeaseUntil.After(first.LeaseUntil) {
		t.Fatalf("lease = %v, want it re-stamped forward past %v", second.LeaseUntil, first.LeaseUntil)
	}
	if _, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute); !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("claim while the new lease holds err = %v, want ErrReplayInProgress", err)
	}

	// Expiry recovers a crashed claimant even after a long silence: a fresh
	// claim with the next token is admitted again.
	waitForLeaseExpiry(t, second.LeaseUntil)
	third, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim after the second expiry: %v", err)
	}
	if third.Token != second.Token+1 {
		t.Fatalf("third token = %d, want %d", third.Token, second.Token+1)
	}
}

func waitForLeaseExpiry(t *testing.T, leaseUntil time.Time) {
	t.Helper()
	rest := time.Until(leaseUntil)
	if rest > 0 {
		time.Sleep(rest + 20*time.Millisecond)
	}
}

// TestDLQReplayClaimStampsTenMinuteProductionLeaseInDatabaseTime pins the
// production lease the CLI passes: storing dlq.ReplayLease leaves roughly ten
// minutes of lease measured on the DATABASE clock (milliseconds), and the
// claim's LeaseUntil equals that exact row value.
func TestDLQReplayClaimStampsTenMinuteProductionLeaseInDatabaseTime(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-ten-minutes-1", "fn/index.run")

	claim, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	token, leaseUntilMs := claimRowOf(t, c, entry.ID)
	if token != claim.Token {
		t.Fatalf("row token = %d, want the returned claim token %d", token, claim.Token)
	}
	if claim.LeaseUntil.UnixMilli() != leaseUntilMs {
		t.Fatalf("LeaseUntil = %v (%dms), want the row's %v (%dms)",
			claim.LeaseUntil, claim.LeaseUntil.UnixMilli(), time.UnixMilli(leaseUntilMs), leaseUntilMs)
	}
	remaining := time.Duration(leaseUntilMs-dbNowMillisInt(t, c)) * time.Millisecond
	if remaining < dlq.ReplayLease-15*time.Second || remaining > dlq.ReplayLease+15*time.Second {
		t.Fatalf("stamped lease = %v remaining, want roughly the production %v (±15s)", remaining, dlq.ReplayLease)
	}
}

// TestDLQReplayReleaseFreesEntryImmediatelyAndFencesStaleToken proves the
// token-fenced release: only the current claimant's token releases the claim
// (making the entry immediately re-claimable, without waiting for expiry), and
// a stale token's release is a no-op that leaves the active claim intact.
func TestDLQReplayReleaseFreesEntryImmediatelyAndFencesStaleToken(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-release-1", "fn/index.run")

	claim, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A stale/toy token cannot release someone else's claim.
	if err := c.ReleaseDLQReplay(ctx, entry.ID, 999); err != nil {
		t.Fatalf("stale release err = %v, want nil (a no-op)", err)
	}
	beforeToken, beforeLease := claimRowOf(t, c, entry.ID)
	if beforeToken != claim.Token || beforeLease != claim.LeaseUntil.UnixMilli() {
		t.Fatalf("stale release changed the row: %d/%d", beforeToken, beforeLease)
	}
	if _, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute); !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("claim after a stale release err = %v, want ErrReplayInProgress", err)
	}

	// The current token's release takes the claim down so a fresh claim is
	// admitted immediately, with the NEXT fencing token.
	if err := c.ReleaseDLQReplay(ctx, entry.ID, claim.Token); err != nil {
		t.Fatalf("current release: %v", err)
	}
	releaseToken, releaseLease := claimRowOf(t, c, entry.ID)
	if releaseToken != claim.Token || releaseLease != 0 {
		t.Fatalf("released row = token %d lease %d, want token %d lease 0", releaseToken, releaseLease, claim.Token)
	}
	if _, ok, err := c.GetDLQEntry(ctx, entry.ID); err != nil || !ok {
		t.Fatalf("release must not delete the entry: (%v,%v)", ok, err)
	}
	next, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v, want immediate admittance", err)
	}
	if next.Token != claim.Token+1 {
		t.Fatalf("post-release token = %d, want %d", next.Token, claim.Token+1)
	}
}

// TestDLQReplayStaleClaimantCannotReleaseOrDeleteNewerClaim proves the
// fencing contract for a claimant whose lease expired and was re-claimed: its
// token is stale, so neither its release nor its success delete can ever affect
// the row the NEWER claimant now owns (the entry is retained, the newer lease
// stays held), while the newer claimant's own delete removes the row.
func TestDLQReplayStaleClaimantCannotReleaseOrDeleteNewerClaim(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-stale-1", "fn/index.run")

	// Given: claimant A's lease expired while its handler was still running,
	// and claimant B immediately re-claimed the row.
	stale, err := c.ClaimDLQReplay(ctx, entry.ID, shortLease)
	if err != nil {
		t.Fatalf("stale claim: %v", err)
	}
	waitForLeaseExpiry(t, stale.LeaseUntil)
	fresh, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("fresh claim: %v", err)
	}

	// The stale claimant's release is a no-op: it must not clear the newer
	// claimant's lease.
	if err := c.ReleaseDLQReplay(ctx, entry.ID, stale.Token); err != nil {
		t.Fatalf("stale release: %v", err)
	}
	if _, err := c.ClaimDLQReplay(ctx, entry.ID, time.Minute); !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("claim after a stale release err = %v, want the newer lease still held", err)
	}

	// The stale claimant's success delete is refused and retains the entry for
	// the newer claimant's operator.
	if err := c.DeleteDLQReplay(ctx, entry.ID, stale.Token); !errors.Is(err, dlq.ErrReplayClaimLost) {
		t.Fatalf("stale delete err = %v, want ErrReplayClaimLost", err)
	}
	if _, ok, err := c.GetDLQEntry(ctx, entry.ID); err != nil || !ok {
		t.Fatalf("a stale delete must retain the entry: (ok=%v, err=%v)", ok, err)
	}
	keeperToken, keeperLease := claimRowOf(t, c, entry.ID)
	if keeperToken != fresh.Token || keeperLease != fresh.LeaseUntil.UnixMilli() {
		t.Fatalf("refused delete changed the row: %d/%d, want %d/%d",
			keeperToken, keeperLease, fresh.Token, fresh.LeaseUntil.UnixMilli())
	}

	// The CURRENT claimant's success delete removes the row.
	if err := c.DeleteDLQReplay(ctx, entry.ID, fresh.Token); err != nil {
		t.Fatalf("current-token delete: %v", err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, entry.ID); ok {
		t.Fatal("the current claimant's delete must remove the row")
	}
}

// TestDLQReplayReleasedAndNeverClaimedDeleteFailClosed proves the success
// delete fails closed for every non-claimant shape: a claim that was released
// (its lease is 0, no longer unexpired), an entry never claimed at all, and an
// unsettled entry never match — the row is left in place for retention or an
// operator decision.
func TestDLQReplayReleasedAndNeverClaimedDeleteFailClosed(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	releasedEntry := claimFixture(t, c, "claim-delete-1", "fn/index.run")
	neverClaimed := claimFixture(t, c, "claim-delete-2", "fn/other")

	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "claim-delete-3")
	unsettled := dlqEntryFor(deliveryID, "fn/index.run", `{"event_name":"INSERT"}`, "exhausted", time.Now())
	if inserted, err := c.SaveDLQEntry(ctx, unsettled); err != nil || !inserted {
		t.Fatalf("seed unsettled = (%v,%v)", inserted, err)
	}

	claim, err := c.ClaimDLQReplay(ctx, releasedEntry.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := c.ReleaseDLQReplay(ctx, releasedEntry.ID, claim.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := c.DeleteDLQReplay(ctx, releasedEntry.ID, claim.Token); !errors.Is(err, dlq.ErrReplayClaimLost) {
		t.Fatalf("delete after release err = %v, want ErrReplayClaimLost", err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, releasedEntry.ID); !ok {
		t.Fatal("a released claim's delete must retain the entry")
	}

	if err := c.DeleteDLQReplay(ctx, neverClaimed.ID, 1); !errors.Is(err, dlq.ErrReplayClaimLost) {
		t.Fatalf("never-claimed delete err = %v, want ErrReplayClaimLost", err)
	}
	if err := c.DeleteDLQReplay(ctx, unsettled.ID, 1); !errors.Is(err, dlq.ErrReplayClaimLost) {
		t.Fatalf("unsettled delete err = %v, want ErrReplayClaimLost", err)
	}
}

// TestDLQRemoveEntryRefusesActiveReplayClaimUntilFreed proves the admin-removal
// boundary: `relay dlq rm` (DeleteDLQEntry) refuses an entry with an active
// claim — the replay may still be executing — and becomes removable only when
// the claim is released or its lease has expired.
func TestDLQRemoveEntryRefusesActiveReplayClaimUntilFreed(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	releasedEntry := claimFixture(t, c, "claim-rm-1", "fn/first.run")
	expiredEntry := claimFixture(t, c, "claim-rm-2", "fn/second.run")

	// Given: active claims on both entries.
	claimA, err := c.ClaimDLQReplay(ctx, releasedEntry.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim A: %v", err)
	}
	claimB, err := c.ClaimDLQReplay(ctx, expiredEntry.ID, shortLease)
	if err != nil {
		t.Fatalf("claim B: %v", err)
	}

	// Then: removal of an actively claimed entry is refused and the row stays.
	if _, err := c.DeleteDLQEntry(ctx, releasedEntry.ID); !errors.Is(err, dlq.ErrReplayInProgress) {
		t.Fatalf("rm with an active claim err = %v, want ErrReplayInProgress", err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, releasedEntry.ID); !ok {
		t.Fatal("the actively claimed entry must survive the refused rm")
	}

	// When claim A is released, its entry becomes removable.
	if err := c.ReleaseDLQReplay(ctx, releasedEntry.ID, claimA.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if deleted, err := c.DeleteDLQEntry(ctx, releasedEntry.ID); err != nil || !deleted {
		t.Fatalf("rm after release = (%v,%v), want (true,nil)", deleted, err)
	}

	// Expiry frees the removal path too: claim B's expired lease no longer
	// protects the row.
	waitForLeaseExpiry(t, claimB.LeaseUntil)
	if deleted, err := c.DeleteDLQEntry(ctx, expiredEntry.ID); err != nil || !deleted {
		t.Fatalf("rm after expiry = (%v,%v), want (true,nil)", deleted, err)
	}
}

// TestDLQPruneSkipsEntriesWithActiveReplayClaim proves retention cannot remove
// a row under a replay: a claimed row is skipped so it can be deleted by its
// own fenced success delete afterwards, while other settled rows of the same
// age are pruned normally — including within the SAME bounded batch.
func TestDLQPruneSkipsEntriesWithActiveReplayClaim(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	// Two settled rows older than ANY cutoff, the OLDEST one claimed.
	claimed := claimFixture(t, c, "claim-prune-1", "fn/oldest.run")
	unclaimed := claimFixture(t, c, "claim-prune-2", "fn/newer.run")
	cutoff := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

	claim, err := c.ClaimDLQReplay(ctx, claimed.ID, dlq.ReplayLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Given the batch is bounded to one row and the oldest row is normally
	// picked first, the claimed row is skipped and the OTHER settled row is
	// removed instead.
	removed, err := c.PruneDLQEntries(ctx, cutoff, 1)
	if err != nil || removed != 1 {
		t.Fatalf("bounded prune = (%d,%v), want the unclaimed row removed", removed, err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, unclaimed.ID); ok {
		t.Fatal("the unclaimed settled row must be the one pruned")
	}
	if _, ok, _ := c.GetDLQEntry(ctx, claimed.ID); !ok {
		t.Fatal("the actively claimed row must survive retention")
	}
	if removed, err := c.PruneDLQEntries(ctx, cutoff, 10); err != nil || removed != 0 {
		t.Fatalf("prune with the only remaining row claimed = (%d,%v), want it skipped", removed, err)
	}

	// When the claim is released, the last prune cycle removes it.
	if err := c.ReleaseDLQReplay(ctx, claimed.ID, claim.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if removed, err := c.PruneDLQEntries(ctx, cutoff, 10); err != nil || removed != 1 {
		t.Fatalf("prune after release = (%d,%v), want 1", removed, err)
	}
	if _, ok, err := c.GetDLQEntry(ctx, claimed.ID); err != nil || ok {
		t.Fatalf("the released row must be gone: (ok=%v, err=%v)", ok, err)
	}
}

// TestDLQSchemaFreshDatabaseHasReplayColumns pins the whole schema migration
// the admission contract needs: a FRESH database gets the dlq table with both
// replay columns directly from the CREATE TABLE (the removed
// ALTER TABLE ... ADD COLUMN IF NOT EXISTS compatibility statements were the
// only other writers of them), each NOT NULL with the unclaimed default 0 —
// so an entry the stream just persisted starts unclaimed and is admitted as
// the row's first claim (token 1).
func TestDLQSchemaFreshDatabaseHasReplayColumns(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	rows, err := c.db.QueryContext(ctx, `
		SELECT column_name, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'dlq'
		ORDER BY column_name`)
	if err != nil {
		t.Fatalf("read dlq columns: %v", err)
	}
	defer rows.Close()
	replayColumns := map[string]*struct {
		nullable bool
		def      string
		seen     bool
	}{}
	for rows.Next() {
		var name, nullable string
		var def *string
		if err := rows.Scan(&name, &nullable, &def); err != nil {
			t.Fatalf("scan column metadata: %v", err)
		}
		info, ok := replayColumns[name]
		if !ok {
			info = &struct {
				nullable bool
				def      string
				seen     bool
			}{}
			replayColumns[name] = info
		}
		info.seen = true
		info.nullable = nullable == "YES"
		info.def = ""
		if def != nil {
			info.def = *def
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("scan column metadata: %v", err)
	}
	for name, want := range map[string]struct {
		nullable bool
		def      string
	}{
		"replay_claim_token":    {false, "0"},
		"replay_lease_until_ms": {false, "0"},
	} {
		got := replayColumns[name]
		if got == nil || !got.seen {
			t.Fatalf("dlq must have column %s on a fresh database (the CREATE TABLE is its only source)", name)
		}
		if got.nullable != want.nullable || got.def != want.def {
			t.Fatalf("dlq column %s = (nullable=%v, default=%q), want (nullable=%v, default=%q)",
				name, got.nullable, got.def, want.nullable, want.def)
		}
	}

	// The defaults apply through the API: a newly persisted entry is unclaimed,
	// so the very first admission claims it at token 1 with a stamped lease.
	entry := claimFixture(t, c, "claim-schema-fresh-1", "fn/index.run")
	token, leaseUntilMs := claimRowOf(t, c, entry.ID)
	if token != 0 || leaseUntilMs != 0 {
		t.Fatalf("fresh row claim state = %d/%d, want the unclaimed defaults 0/0", token, leaseUntilMs)
	}
	claim, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
	if err != nil || claim.Token != 1 {
		t.Fatalf("first claim on a fresh row = (%+v, %v), want token 1", claim, err)
	}
}

// TestDLQReplayClaimSerializedByRowLock proves the SQL-level fence underneath
// admission: an independent transaction holding the row's SELECT ... FOR UPDATE
// lock holds ClaimDLQReplay open, and only that transaction's commit lets the
// claim complete — so a racing claim can never read-or-write the fencing
// columns mid-transaction of another claimant.
func TestDLQReplayClaimSerializedByRowLock(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	entry := claimFixture(t, c, "claim-serialized-1", "fn/index.run")

	// Given: another transaction locks the row.
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin external transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	var lockedToken int64
	if err := tx.QueryRowContext(ctx,
		`SELECT replay_claim_token FROM dlq WHERE id = $1 FOR UPDATE`, entry.ID).Scan(&lockedToken); err != nil {
		t.Fatalf("lock row: %v", err)
	}

	// When: a claim races it, it must not complete while the lock is held.
	type claimResult struct {
		claim dlq.ReplayClaim
		err   error
	}
	done := make(chan claimResult, 1)
	go func() {
		claim, err := c.ClaimDLQReplay(ctx, entry.ID, dlq.ReplayLease)
		done <- claimResult{claim, err}
	}()
	select {
	case res := <-done:
		t.Fatalf("claim completed while another transaction held the row lock: (%+v, %v)", res.claim, res.err)
	case <-time.After(500 * time.Millisecond):
		// The expected serialization: still blocked.
	}

	// Then: releasing the lock (commit) lets the claim complete with the fresh
	// token for the unclaimed row.
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit external transaction: %v", err)
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("claim after the lock was freed: %v", res.err)
		}
		if res.claim.Token != 1 {
			t.Fatalf("token = %d, want the row's first claim 1", res.claim.Token)
		}
		if token, leaseUntilMs := claimRowOf(t, c, entry.ID); token != 1 || leaseUntilMs == 0 {
			t.Fatalf("row claim state = %d/%d, want token 1 with a stamped lease", token, leaseUntilMs)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("claim never completed after the blocking transaction committed")
	}
}

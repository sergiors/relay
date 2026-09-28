//go:build integration

// This file exercises the ATOMIC invocation-state transitions against a real
// Redis server: the Lua-scripted TryStart/complete/failure/exhaustion writes
// that make a claim unique across replicas, keep a stale claim from overwriting a
// newer marker or a success, keep terminal states monotonic, guarantee every
// mutation carries a TTL that covers the protected deadline, and use exact
// integer-millisecond eligibility with an opaque per-claim token. It is excluded
// from the default suite by the integration build tag and REQUIRES Redis at
// REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping when it
// is absent.
package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// atomicStateStore builds a real Redis-backed invocationStore bound to one
// (stream, group, msgID), plus a cleanup that deletes the hash key. It is the
// per-script test seam: no consumer loop is involved.
func atomicStateStore(t *testing.T, cli *redis.Client) (invocationStateStore, string, string, string) {
	t.Helper()
	prefix := fmt.Sprintf("atomic-%d", time.Now().UnixNano())
	stream, group, msgID := prefix+"-stream", prefix+"-group", "1-0"
	key := invocationStateKey(stream, group, msgID)
	t.Cleanup(func() {
		_ = cli.Del(context.Background(), key).Err()
	})
	return &invocationStore{client: cli}, stream, group, msgID
}

// seedMarker writes an active/exhausted marker directly, so a test can control
// the exact deadline and claim identity it exercises.
func seedMarker(t *testing.T, cli *redis.Client, stream, group, msgID, invocation, value string) {
	t.Helper()
	if err := cli.HSet(context.Background(), invocationStateKey(stream, group, msgID), invocation, value).Err(); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
}

// marker reads an invocation field's raw value.
func marker(t *testing.T, cli *redis.Client, stream, group, msgID, invocation string) string {
	t.Helper()
	v, err := cli.HGet(context.Background(), invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatalf("hget marker: %v", err)
	}
	return v
}

// TestIntegrationAtomicTryStartExactlyOneConcurrentWinner drives many
// simultaneous TryStart calls for the same invocation through separate Redis
// clients (simulating replicas) and asserts that exactly one observes the
// absent marker and starts: every other caller sees the winner's running marker
// and is protected (started=false, wait>0). A plain read-then-write store would
// permit a dual start; the Lua script does not. The winner's claim carries a
// unique opaque token, and the persisted marker matches it.
func TestIntegrationAtomicTryStartExactlyOneConcurrentWinner(t *testing.T) {
	for _, n := range []int{2, 16} {
		t.Run(fmt.Sprintf("concurrency=%d", n), func(t *testing.T) {
			cli := testutil.RequireRedis(t)
			_, stream, group, msgID := atomicStateStore(t, cli)

			// Use independent clients so the EVALs truly race on the server,
			// not through a single pooled connection.
			clients := make([]*redis.Client, n)
			for i := range clients {
				c := redis.NewClient(&redis.Options{Addr: cli.Options().Addr})
				clients[i] = c
				t.Cleanup(func() { _ = c.Close() })
			}
			stores := make([]invocationStateStore, n)
			for i := range stores {
				stores[i] = &invocationStore{client: clients[i]}
			}

			start := make(chan struct{})
			type result struct {
				started bool
				claim   InvocationClaim
				wait    time.Duration
				err     error
			}
			results := make([]result, n)
			var wg sync.WaitGroup
			now := time.Now()
			deadline := now.Add(time.Hour)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					s, c, w, err := stores[i].tryStart(context.Background(), stream, group, msgID, "fn/h", now, deadline)
					results[i] = result{s, c, w, err}
				}(i)
			}
			close(start)
			wg.Wait()

			winners := 0
			var winnerToken string
			for i, r := range results {
				if r.err != nil {
					t.Fatalf("caller %d: tryStart error: %v", i, r.err)
				}
				if r.started {
					winners++
					winnerToken = r.claim.Token
					if r.claim.Attempt != 1 {
						t.Errorf("winner attempt = %d, want 1", r.claim.Attempt)
					}
					if r.claim.Token == "" {
						t.Errorf("winner token is empty, want a fresh opaque token")
					}
					if r.wait != 0 {
						t.Errorf("winner wait = %s, want 0", r.wait)
					}
					continue
				}
				if r.claim.Attempt != 1 {
					t.Errorf("loser attempt = %d, want 1 (the winner's marker)", r.claim.Attempt)
				}
				if r.wait <= 0 {
					t.Errorf("loser wait = %s, want > 0 (protected by the winner)", r.wait)
				}
			}
			if winners != 1 {
				t.Fatalf("started callers = %d, want exactly 1", winners)
			}
			// The persisted marker is the winner's running marker at attempt 1
			// and carries the winner's token.
			v := marker(t, cli, stream, group, msgID, "fn/h")
			kind, _, attempts, ok := parseInvocationState(v)
			if !ok || kind != kindRunning || attempts != 1 {
				t.Fatalf("marker = %q, want running attempt 1", v)
			}
			if got := activeToken(v); got != winnerToken {
				t.Fatalf("persisted token = %q, want the winner's %q", got, winnerToken)
			}
		})
	}
}

// TestIntegrationAtomicTryStartAttemptOnce pins that a confirmed claim
// increments the attempt exactly once, and that the attempt count advances only
// when the deadline has genuinely expired (a protected claim never increments).
func TestIntegrationAtomicTryStartAttemptOnce(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()
	deadline := now.Add(time.Hour)

	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, deadline)
	if err != nil || !started || claim.Attempt != 1 || claim.Token == "" {
		t.Fatalf("first claim = (%v, %+v, %v), want (true, attempt 1, nil)", started, claim, err)
	}
	// A second claim while protected does not advance the attempt.
	started, loser, wait, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, deadline)
	if err != nil || started || loser.Attempt != 1 || wait <= 0 {
		t.Fatalf("protected claim = (%v, %+v, %s, %v), want (false, attempt 1, >0, nil)", started, loser, wait, err)
	}
	// After the deadline expires the reclaim advances to attempt 2 with a FRESH
	// token.
	later := deadline.Add(time.Minute)
	started, next, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", later, later.Add(time.Hour))
	if err != nil || !started || next.Attempt != 2 {
		t.Fatalf("reclaim = (%v, %+v, %v), want (true, attempt 2, nil)", started, next, err)
	}
	if next.Token == claim.Token {
		t.Fatalf("reclaim token = %q, must differ from the first claim's %q", next.Token, claim.Token)
	}
}

// TestIntegrationAtomicDeadlineBoundaryExactly pins the exact integer-millisecond
// eligibility boundary: a marker with deadline D is protected at D-1ms, eligible
// at exactly D, and eligible at D+1ms. The comparison is exact (no skew, no
// floating point): now < D protects, now >= D is eligible.
func TestIntegrationAtomicDeadlineBoundaryExactly(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	base := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	claim := InvocationClaim{Attempt: 2, Token: "aabbccdd"}

	cases := []struct {
		name        string
		offsetMs    int64
		wantStarted bool
	}{
		{"deadline-1ms is protected", -1, false},
		{"exactly at deadline is eligible", 0, true},
		{"deadline+1ms is eligible", 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seedMarker(t, cli, stream, group, msgID, "fn/h", runningValue(base, claim))
			now := base.Add(time.Duration(tc.offsetMs) * time.Millisecond)
			started, got, wait, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
			if err != nil {
				t.Fatalf("tryStart: %v", err)
			}
			if started != tc.wantStarted {
				t.Fatalf("started = %v at offset %dms, want %v", started, tc.offsetMs, tc.wantStarted)
			}
			if !tc.wantStarted {
				if got.Attempt != claim.Attempt {
					t.Fatalf("protected attempt = %d, want the existing %d", got.Attempt, claim.Attempt)
				}
				if wait != time.Millisecond {
					// Deadlines are millisecond-resolution, so the wait to the
					// -1ms boundary is exactly 1ms.
					t.Fatalf("protected wait = %s, want 1ms", wait)
				}
			}
		})
	}
}

// TestIntegrationAtomicNextAttemptAtDueBoundary pins the retry-backoff
// eligibility boundary exactly: before the persisted deadline every concurrent
// caller is protected (no winner); once the deadline is reached, exactly one
// caller wins the reclaim.
func TestIntegrationAtomicNextAttemptAtDueBoundary(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	// A next_attempt_at marker 1s in the future (millisecond resolution).
	due := time.Now().Add(time.Second).Truncate(time.Millisecond)
	seedMarker(t, cli, stream, group, msgID, "fn/h", nextAttemptValue(due, InvocationClaim{Attempt: 3, Token: "aabbccdd"}))

	// Before the due time all callers are protected: no winner.
	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]bool, n)
	before := due.Add(-time.Millisecond)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", before, before.Add(time.Hour))
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
			}
			results[i] = s
		}(i)
	}
	close(start)
	wg.Wait()
	for i, s := range results {
		if s {
			t.Fatalf("caller %d started before the backoff was due", i)
		}
	}

	// Exactly at the due time, exactly one caller wins and the attempt count
	// advances to 4.
	start2 := make(chan struct{})
	results2 := make([]bool, n)
	attempts := make([]int, n)
	var wg2 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			<-start2
			s, c, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", due, due.Add(time.Hour))
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
			}
			results2[i], attempts[i] = s, c.Attempt
		}(i)
	}
	close(start2)
	wg2.Wait()
	winners := 0
	for i := range results2 {
		if results2[i] {
			winners++
			if attempts[i] != 4 {
				t.Errorf("winner %d attempt = %d, want 4", i, attempts[i])
			}
		}
	}
	if winners != 1 {
		t.Fatalf("at-due winners = %d, want exactly 1", winners)
	}
}

// TestIntegrationAtomicNormalRetryClaim pins the positive retry path: a claim's
// finishFailure writes "next_attempt_at:<deadline_ms>:<attempt>:<token>"
// retaining the same attempt and token; the marker protects until that deadline
// and, once reached, a reclaim carries the attempt forward with a fresh token.
func TestIntegrationAtomicNormalRetryClaim(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started || claim.Attempt != 1 {
		t.Fatalf("claim = (%v,%+v,%v)", started, claim, err)
	}
	backoff := 2 * time.Minute
	ok, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claim, backoff, now)
	if err != nil || !ok {
		t.Fatalf("finishFailure = (%v,%v), want (true,nil)", ok, err)
	}
	// The retry marker retains the same attempt and token.
	want := nextAttemptValue(now.Add(backoff), claim)
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != want {
		t.Fatalf("marker = %q, want %q", got, want)
	}
	// Protected before the backoff deadline: no new claim.
	before := now.Add(backoff).Add(-time.Millisecond)
	if s, c, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", before, before.Add(time.Hour)); err != nil || s || c.Attempt != 1 || w <= 0 {
		t.Fatalf("pre-deadline claim = (%v,%+v,%s,%v), want protected", s, c, w, err)
	}
	// At the backoff deadline the reclaim advances to attempt 2 with a new token.
	at := now.Add(backoff)
	s, next, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", at, at.Add(time.Hour))
	if err != nil || !s || next.Attempt != 2 {
		t.Fatalf("post-deadline reclaim = (%v,%+v,%v), want (true, attempt 2, nil)", s, next, err)
	}
	if next.Token == claim.Token {
		t.Fatalf("reclaim token reused the retried claim's token")
	}
}

// TestIntegrationAtomicStaleClaimCannotTransition pins the token CAS for every
// active-claim-originated transition: once claim A's attempt-1 lease expires and
// claim B reclaims as attempt 2, A's failure, success, exhaustion, and retry are
// all refused (explicit false, no Redis error) and never disturb B's marker. A
// same-attempt claim with a DIFFERENT token is likewise refused, proving the
// token — not the attempt number — is the claim identity.
func TestIntegrationAtomicStaleClaimCannotTransition(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	// Claim A (attempt 1), then reclaim as claim B (attempt 2) by advancing past
	// A's deadline.
	started, claimA, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started || claimA.Attempt != 1 {
		t.Fatalf("claim A = (%v,%+v,%v)", started, claimA, err)
	}
	reclaimAt := now.Add(2 * time.Hour)
	started, claimB, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", reclaimAt, reclaimAt.Add(time.Hour))
	if err != nil || !started || claimB.Attempt != 2 {
		t.Fatalf("claim B = (%v,%+v,%v)", started, claimB, err)
	}
	wantB := runningValue(reclaimAt.Add(time.Hour), claimB)

	// Stale A failure, retry, success, and exhaustion are ALL refused.
	if ok, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claimA, time.Minute, reclaimAt); err != nil || ok {
		t.Fatalf("stale A finishFailure = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != wantB {
		t.Fatalf("stale A failure changed marker to %q, want B's %q", got, wantB)
	}
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claimA); err != nil || ok {
		t.Fatalf("stale A markComplete = (%v,%v), want (false,nil)", ok, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claimA); err != nil || ok {
		t.Fatalf("stale A markExhausted = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != wantB {
		t.Fatalf("stale A transitions changed marker to %q, want B's %q", got, wantB)
	}

	// A same-attempt claim with a different token is refused: the token, not the
	// attempt number, is the identity.
	impostor := InvocationClaim{Attempt: 2, Token: "deadbeef"}
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", impostor); err != nil || ok {
		t.Fatalf("impostor markComplete = (%v,%v), want (false,nil)", ok, err)
	}
	if ok, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", impostor, time.Minute, reclaimAt); err != nil || ok {
		t.Fatalf("impostor finishFailure = (%v,%v), want (false,nil)", ok, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", impostor); err != nil || ok {
		t.Fatalf("impostor markExhausted = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != wantB {
		t.Fatalf("impostor transitions changed marker to %q, want B's %q", got, wantB)
	}

	// Claim B's own transitions DO apply.
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claimB); err != nil || !ok {
		t.Fatalf("current B markComplete = (%v,%v), want (true,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != "ok" {
		t.Fatalf("marker = %q, want ok", got)
	}
}

// TestIntegrationAtomicStaleFailureDoesNotOverwriteComplete pins the stale-claim
// guard for success: a stale owner's failure after a newer claim completed must
// be a no-op, and a success written by a newer claim is terminal.
func TestIntegrationAtomicStaleFailureDoesNotOverwriteComplete(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	started, claimA, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started || claimA.Attempt != 1 {
		t.Fatalf("claim A = (%v,%+v,%v)", started, claimA, err)
	}
	reclaimAt := now.Add(2 * time.Hour)
	started, claimB, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", reclaimAt, reclaimAt.Add(time.Hour))
	if err != nil || !started || claimB.Attempt != 2 {
		t.Fatalf("claim B = (%v,%+v,%v)", started, claimB, err)
	}
	// Claim B completes.
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claimB); err != nil || !ok {
		t.Fatalf("claim B markComplete = (%v,%v)", ok, err)
	}
	// A stale claim-A failure arriving after completion must be a no-op.
	if ok, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claimA, time.Minute, reclaimAt); err != nil || ok {
		t.Fatalf("stale A finishFailure after complete = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != "ok" {
		t.Fatalf("marker = %q, want ok (stale failure must not overwrite success)", got)
	}
	// A stale claim-A exhaustion must also not re-open the terminal success.
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claimA); err != nil || ok {
		t.Fatalf("stale A markExhausted after complete = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != "ok" {
		t.Fatalf("marker = %q, want ok (stale exhaustion must not downgrade success)", got)
	}
}

// TestIntegrationAtomicTerminalExhaustionAndDLQ pins the terminal exhausted
// lifecycle: only the owning claim can write the exhausted marker, success never
// downgrades it, TryStart reports it terminal (with the retained attempt), and
// the DLQ upgrade is CASed on the exhausted identity and is monotonic.
func TestIntegrationAtomicTerminalExhaustionAndDLQ(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started || claim.Attempt != 1 {
		t.Fatalf("claim = (%v,%+v,%v)", started, claim, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markExhausted = (%v,%v)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != exhaustedValue(claim, false) {
		t.Fatalf("marker = %q, want the exhausted marker retaining the claim", got)
	}
	// Success must not downgrade the exhausted marker.
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim); err != nil || ok {
		t.Fatalf("markComplete on exhausted = (%v,%v), want (false,nil)", ok, err)
	}
	if got := marker(t, cli, stream, group, msgID, "fn/h"); got != exhaustedValue(claim, false) {
		t.Fatalf("marker = %q, want the exhausted marker preserved", got)
	}
	// TryStart reports it terminal (the retained attempt, no wait, no new claim).
	if s, c, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", time.Now(), time.Now().Add(time.Hour)); err != nil || s || c.Attempt != 1 || w != 0 {
		t.Fatalf("tryStart on exhausted = (%v,%+v,%s,%v), want (false, attempt 1, 0, nil)", s, c, w, err)
	}

	// The exhaustedState read exposes the retained identity and dlq=false.
	exClaim, dlq, ok, err := store.exhaustedState(ctx, stream, group, msgID, "fn/h")
	if err != nil || !ok || dlq || exClaim != claim {
		t.Fatalf("exhaustedState = (%+v,%v,%v,%v), want the retained claim, no dlq", exClaim, dlq, ok, err)
	}

	// A foreign identity cannot upgrade the marker.
	if ok, err := store.markExhaustedDLQ(ctx, stream, group, msgID, "fn/h", InvocationClaim{Attempt: 1, Token: "deadbeef"}); err != nil || ok {
		t.Fatalf("foreign markExhaustedDLQ = (%v,%v), want (false,nil)", ok, err)
	}
	// The owning identity upgrades it; a second upgrade is monotonic (still true).
	if ok, err := store.markExhaustedDLQ(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markExhaustedDLQ = (%v,%v), want (true,nil)", ok, err)
	}
	if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, true) {
		t.Fatalf("marker = %q, want the dlq-suffixed exhausted marker", v)
	}
	if ok, err := store.markExhaustedDLQ(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("re markExhaustedDLQ = (%v,%v), want (true,nil)", ok, err)
	}
	if _, dlq, ok, err := store.exhaustedState(ctx, stream, group, msgID, "fn/h"); err != nil || !ok || !dlq {
		t.Fatalf("exhaustedState after upgrade = (dlq=%v ok=%v err=%v), want dlq", dlq, ok, err)
	}
}

// TestIntegrationAtomicTTLCoversDeadlineAndRefreshes pins the TTL contract: an
// active marker's key TTL must be at least the protected window plus the safety
// margin (so the key can never expire before its deadline), and every mutation
// refreshes the TTL in the same atomic step. Terminal writes use the normal TTL.
func TestIntegrationAtomicTTLCoversDeadlineAndRefreshes(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	// A running marker whose protected window exceeds the normal TTL. The key
	// TTL must exceed the window+margin, never the terminal TTL.
	now := time.Now()
	longTimeout := invocationStateTTL + 48*time.Hour
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(longTimeout))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	ttl, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after tryStart: %v", err)
	}
	minTTL := longTimeout + invocationSafetyMargin - 5*time.Second // allow for elapsed time
	if ttl < minTTL {
		t.Fatalf("tryStart TTL = %s, want >= %s (window %s + margin %s)", ttl, minTTL, longTimeout, invocationSafetyMargin)
	}

	// A short-deadline marker is floored at the normal terminal TTL.
	shortNow := time.Now()
	started, shortClaim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/short", shortNow, shortNow.Add(time.Second))
	if err != nil || !started {
		t.Fatalf("short tryStart = (%v,%+v,%v)", started, shortClaim, err)
	}
	ttl, err = cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after short tryStart: %v", err)
	}
	if ttl < invocationStateTTL-5*time.Second {
		t.Fatalf("short-deadline TTL = %s, want the normal TTL %s", ttl, invocationStateTTL)
	}

	// Every later mutation refreshes the TTL (still positive, never -1).
	mutateAndCheck := func(label string, fn func() error) {
		t.Helper()
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		d, err := cli.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("%s: pttl: %v", label, err)
		}
		if d <= 0 {
			t.Fatalf("%s: key has no TTL (pttl=%s)", label, d)
		}
	}
	mutateAndCheck("finishFailure", func() error {
		_, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claim, time.Minute, time.Now())
		return err
	})
	mutateAndCheck("markComplete", func() error {
		_, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim)
		return err
	})
	mutateAndCheck("markExhausted", func() error {
		_, err := store.markExhausted(ctx, stream, group, msgID, "fn/ex", claim)
		return err
	})
	mutateAndCheck("markExhaustedDLQ", func() error {
		_, err := store.markExhaustedDLQ(ctx, stream, group, msgID, "fn/ex", claim)
		return err
	})
	mutateAndCheck("claimClassification", func() error {
		_, err := store.claimClassification(ctx, stream, group, msgID)
		return err
	})
	mutateAndCheck("recordTrace", func() error {
		return store.recordTrace(ctx, stream, group, msgID, "fn/h", "00-trace-span-01")
	})
}

// TestIntegrationAtomicCrashReclaimAfterDeadline pins crash recovery: a running
// marker that is never renewed simply expires at its deadline, after which a
// reclaim starts the next attempt with a fresh token. There is no permanent lock.
func TestIntegrationAtomicCrashReclaimAfterDeadline(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	// A crashed attempt 1 whose deadline is 300ms out.
	deadline := now.Add(300 * time.Millisecond)
	seedMarker(t, cli, stream, group, msgID, "fn/h", runningValue(deadline, InvocationClaim{Attempt: 1, Token: "aabbccdd"}))
	// Before expiry the marker protects.
	if s, c, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour)); err != nil || s || c.Attempt != 1 || w <= 0 {
		t.Fatalf("pre-expiry claim = (%v,%+v,%s,%v), want protected", s, c, w, err)
	}
	// At/after the deadline the reclaim starts attempt 2 with a fresh token.
	after := deadline.Add(time.Millisecond)
	s, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", after, after.Add(time.Hour))
	if err != nil || !s || claim.Attempt != 2 {
		t.Fatalf("post-expiry reclaim = (%v,%+v,%v), want (true, attempt 2, nil)", s, claim, err)
	}
	if claim.Token == "aabbccdd" {
		t.Fatalf("reclaim token reused the crashed claim's token %q", claim.Token)
	}
}

// TestIntegrationAtomicNonCanonicalDeadlineIsEligible pins that a marker with a
// non-canonical (leading-zero) deadline is treated as eligible rather than
// compared inexactly by the script's length-then-lexicographic comparison.
func TestIntegrationAtomicNonCanonicalDeadlineIsEligible(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	seedMarker(t, cli, stream, group, msgID, "fn/h", "running:01757000000000:2:ab12")
	now := time.Now()
	s, claim, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !s || claim.Attempt != 1 || w != 0 {
		t.Fatalf("non-canonical deadline claim = (%v,%+v,%s,%v), want (true, attempt 1, 0, nil)", s, claim, w, err)
	}
}

// TestIntegrationAtomicUnparseableMarkerIsEligible pins that a corrupt or
// legacy-grammar marker is treated as eligible (not a crash of the script) and
// the claim overwrites it at attempt 1 with a fresh token.
func TestIntegrationAtomicUnparseableMarkerIsEligible(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	for _, corrupt := range []string{
		"running:notanumber",
		"running:1757000000000:2",       // legacy/missing token
		"running:1757000000000000000#3", // old unix-nano grammar
	} {
		t.Run(corrupt, func(t *testing.T) {
			seedMarker(t, cli, stream, group, msgID, "fn/h", corrupt)
			now := time.Now()
			s, claim, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
			if err != nil || !s || claim.Attempt != 1 || w != 0 || claim.Token == "" {
				t.Fatalf("corrupt-marker claim = (%v,%+v,%s,%v), want (true, attempt 1, 0, nil)", s, claim, w, err)
			}
		})
	}
}

// TestIntegrationAtomicTransportErrorPropagates pins the ambiguous-claim
// contract at the store level: when Redis is unreachable, TryStart returns the
// transport error (NOT a fail-open started=true), so the runner can leave the
// message pending without executing.
func TestIntegrationAtomicTransportErrorPropagates(t *testing.T) {
	// A client pointed at a closed port: every command fails to connect.
	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = bad.Close() })
	store := &invocationStore{client: bad}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	now := time.Now()
	started, claim, _, err := store.tryStart(ctx, "s", "g", "1-0", "fn/h", now, now.Add(time.Hour))
	if err == nil {
		t.Fatal("tryStart against an unreachable Redis = nil error, want a transport error")
	}
	if started {
		t.Fatal("tryStart must NOT fail open (started=true) on a transport error")
	}
	if claim.Token != "" || claim.Attempt != 0 {
		t.Fatalf("claim = %+v on a transport error, want the zero claim", claim)
	}

	// The claim-CASing transitions likewise surface a transport error (never a
	// false "applied"), so the caller leaves the message pending.
	if ok, err := store.markComplete(ctx, "s", "g", "1-0", "fn/h", InvocationClaim{Attempt: 1, Token: "aa"}); err == nil || ok {
		t.Fatalf("markComplete against an unreachable Redis = (%v,%v), want (false, err)", ok, err)
	}
}

// TestIntegrationAtomicStoreFailureKeepsMessagePendingAndSkipsHandler wires a
// real Redis stream/group (so delivery works) to an invocation-state store whose
// Redis is unreachable, then runs processMessage with a handler that mirrors the
// runner's claim guard. The handler's TryStart fails, so it returns
// ErrInvocationNotEligible and must NOT execute: the message stays pending (no
// ACK, no DLQ) across the reclaim grace window. This is the end-to-end proof
// that an ambiguous claim never runs a handler.
func TestIntegrationAtomicStoreFailureKeepsMessagePendingAndSkipsHandler(t *testing.T) {
	cli := testutil.RequireRedis(t)
	prefix := fmt.Sprintf("claimfail-%d", time.Now().UnixNano())
	streamName, groupName := prefix+"-stream", prefix+"-group"

	// A consumer over the REAL Redis client for stream operations, but with an
	// invocation-state store pointed at a closed port (Redis "down" for the
	// lifecycle scripts only).
	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = bad.Close() })
	c := newConsumer(ConsumerConfig{
		Client:          cli,
		Stream:          streamName,
		Group:           groupName,
		Consumer:        prefix + "-consumer",
		Log:             testutil.DiscardLogger(),
		MinPendingIdle:  150 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
		Block:           200 * time.Millisecond,
	}, &invocationStore{client: bad})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Del(context.Background(), streamName, DLQStreamFor(streamName)).Err()
	})

	if _, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]any{"event": `{"a":1}`},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	// The handler mirrors the runner: it inspects the injected InvocationState,
	// calls TryStart, and on an ambiguous-claim error returns
	// ErrInvocationNotEligible without executing.
	var executed atomic.Int64
	handler := func(hctx context.Context, msgID string, _ map[string]any) error {
		p, ok := InvocationStateFrom(hctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, _, _, err := p.TryStart("fn/h", time.Hour)
		if err != nil {
			// Transport error: never execute; leave pending.
			return ErrInvocationNotEligible
		}
		if started {
			executed.Add(1)
		}
		return nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, handler)
	}()

	// Wait for the message to be delivered at least once (it may never ACK).
	testutil.WaitFor(t, 8*time.Second, "message delivered", func() bool {
		entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: streamName, Group: groupName, Start: "-", End: "+", Count: 10,
		}).Result()
		return err == nil && len(entries) > 0
	})
	// It must remain pending (not ACKed, not DLQ'd) and the handler must never
	// execute across a sustained window.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: streamName, Group: groupName, Start: "-", End: "+", Count: 10,
		}).Result()
		if err != nil || len(entries) == 0 {
			t.Fatalf("message left the PEL while the claim kept failing (entries=%d err=%v)", len(entries), err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if n := executed.Load(); n != 0 {
		t.Fatalf("handler executed %d times despite the claim error; want 0", n)
	}
}

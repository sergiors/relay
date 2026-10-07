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
	"strconv"
	"strings"
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
// when the deadline has genuinely elapsed (a protected claim never increments).
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
	// After the deadline elapses the reclaim advances to attempt 2 with a FRESH
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
// active-claim-originated transition: once claim A's attempt-1 deadline elapses
// and claim B reclaims as attempt 2, A's failure, success, exhaustion, and retry
// are all refused (explicit false, no Redis error) and never disturb B's marker. A
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

	// The atomic DLQ persistence appends the entry and upgrades the marker in one
	// script. A foreign identity is refused (an error, nothing appended).
	dlqStream := stream + "-dlq"
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlqStream).Err() })
	entry := map[string]any{"original_id": msgID, "app": "fn", "handler": "h"}
	if wrote, err := store.persistDLQ(ctx, dlqStream, stream, group, msgID, "fn/h", InvocationClaim{Attempt: 1, Token: "deadbeef"}, entry); err == nil || wrote {
		t.Fatalf("foreign persistDLQ = (%v,%v), want (false,error)", wrote, err)
	}
	if n, err := cli.XLen(ctx, dlqStream).Result(); err != nil || n != 0 {
		t.Fatalf("foreign persistDLQ appended %d entries (err %v), want 0", n, err)
	}
	// The owning identity appends exactly one entry and upgrades the marker.
	if wrote, err := store.persistDLQ(ctx, dlqStream, stream, group, msgID, "fn/h", claim, entry); err != nil || !wrote {
		t.Fatalf("persistDLQ = (%v,%v), want (true,nil)", wrote, err)
	}
	if v := marker(t, cli, stream, group, msgID, "fn/h"); v != exhaustedValue(claim, true) {
		t.Fatalf("marker = %q, want the dlq-suffixed exhausted marker", v)
	}
	if n, err := cli.XLen(ctx, dlqStream).Result(); err != nil || n != 1 {
		t.Fatalf("persistDLQ XLen = %d (err %v), want 1", n, err)
	}
	// A second call is monotonic: wrote=false and no second entry.
	if wrote, err := store.persistDLQ(ctx, dlqStream, stream, group, msgID, "fn/h", claim, entry); err != nil || wrote {
		t.Fatalf("re persistDLQ = (%v,%v), want (false,nil)", wrote, err)
	}
	if n, err := cli.XLen(ctx, dlqStream).Result(); err != nil || n != 1 {
		t.Fatalf("re persistDLQ XLen = %d (err %v), want 1 (no duplicate)", n, err)
	}
	if _, dlq, ok, err := store.exhaustedState(ctx, stream, group, msgID, "fn/h"); err != nil || !ok || !dlq {
		t.Fatalf("exhaustedState after upgrade = (dlq=%v ok=%v err=%v), want dlq", dlq, ok, err)
	}
}

// TestIntegrationAtomicRecoverableStateIsPersistent pins the persistence
// contract: every lifecycle mutation performed while the message is recoverable
// leaves the invocation-state hash with NO TTL (regardless of how far the
// deadline is), so a redelivery can never outlive its state. Go computes no TTL
// for these writes; the scripts PERSIST the key instead.
func TestIntegrationAtomicRecoverableStateIsPersistent(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	// A very long deadline must NOT introduce a TTL: the key is persistent.
	now := time.Now()
	longTimeout := DefaultInvocationRetention + 48*time.Hour
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(longTimeout))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("tryStart PTTL = %s (err %v), want -1 (persistent)", d, err)
	}

	// A second invocation's short-deadline marker also leaves the key persistent
	// (the hash is shared, and recoverable state is never TTL'd).
	shortNow := time.Now()
	started, shortClaim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/short", shortNow, shortNow.Add(time.Second))
	if err != nil || !started {
		t.Fatalf("short tryStart = (%v,%+v,%v)", started, shortClaim, err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("short tryStart PTTL = %s (err %v), want -1 (persistent)", d, err)
	}

	// Every later mutation ALSO leaves the key persistent (never -2/missing, and
	// never a positive TTL).
	mutateAndCheckPersistent := func(label string, fn func() error) {
		t.Helper()
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		d, err := cli.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("%s: pttl: %v", label, err)
		}
		if d != -1 {
			t.Fatalf("%s: PTTL = %s, want -1 (persistent while recoverable)", label, d)
		}
	}
	mutateAndCheckPersistent("finishFailure", func() error {
		_, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claim, time.Minute, time.Now())
		return err
	})
	mutateAndCheckPersistent("markComplete", func() error {
		_, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim)
		return err
	})
	mutateAndCheckPersistent("markExhausted", func() error {
		_, err := store.markExhausted(ctx, stream, group, msgID, "fn/ex", claim)
		return err
	})
	// Seed an exhausted marker so the atomic DLQ persistence has a claim to CAS.
	exClaim := InvocationClaim{Attempt: 1, Token: "ee01"}
	seedMarker(t, cli, stream, group, msgID, "fn/ex", exhaustedValue(exClaim, false))
	dlqStream := stream + "-dlq"
	t.Cleanup(func() { _ = cli.Del(context.Background(), dlqStream).Err() })
	mutateAndCheckPersistent("persistDLQ", func() error {
		_, err := store.persistDLQ(ctx, dlqStream, stream, group, msgID, "fn/ex", exClaim, map[string]any{"original_id": msgID})
		return err
	})
	mutateAndCheckPersistent("claimClassification", func() error {
		_, err := store.claimClassification(ctx, stream, group, msgID)
		return err
	})
	mutateAndCheckPersistent("recordTrace", func() error {
		return store.recordTrace(ctx, stream, group, msgID, "fn/h", "00-trace-span-01")
	})
}

// TestIntegrationAtomicRetainTerminalPinsTTLAndGuardsStaleWrites pins the
// terminal-retention contract: once the message left the PEL, retainTerminal
// writes the reserved terminal marker and applies the retention TTL in one step;
// it is monotonic (a second call does not re-extend the TTL); and once retained,
// a stale in-memory transition can neither mutate the retained state nor remove
// the TTL (which would resurrect an unrecoverable hash).
func TestIntegrationAtomicRetainTerminalPinsTTLAndGuardsStaleWrites(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	now := time.Now()
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markComplete = (%v,%v)", ok, err)
	}

	// Retain: the marker is present and the retention TTL is applied.
	if err := store.retainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal field = %q (err %v), want \"1\"", v, err)
	}
	ttl, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after retain: %v", err)
	}
	if ttl <= 0 || ttl > DefaultInvocationRetention {
		t.Fatalf("retention PTTL = %s, want (0, %s]", ttl, DefaultInvocationRetention)
	}

	// Monotonic: a second retain must NOT re-extend the TTL. Sleep enough to
	// distinguish the two, then confirm the deadline moved no later.
	before := time.Now().Add(ttl)
	time.Sleep(300 * time.Millisecond)
	if err := store.retainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err != nil {
		t.Fatalf("second retainTerminal: %v", err)
	}
	ttl2, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after second retain: %v", err)
	}
	after := time.Now().Add(ttl2)
	if after.After(before.Add(50 * time.Millisecond)) {
		t.Fatalf("second retain extended the deadline: %v -> %v", before, after)
	}

	// A stale in-memory delivery's transitions are inert: none may mutate the
	// retained state or remove the TTL.
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim); err != nil || ok {
		t.Fatalf("markComplete after retain = (%v,%v), want (false,nil)", ok, err)
	}
	if ok, err := store.finishFailure(ctx, stream, group, msgID, "fn/h", claim, time.Minute, time.Now()); err != nil || ok {
		t.Fatalf("finishFailure after retain = (%v,%v), want (false,nil)", ok, err)
	}
	if ok, err := store.markExhausted(ctx, stream, group, msgID, "fn/h", claim); err != nil || ok {
		t.Fatalf("markExhausted after retain = (%v,%v), want (false,nil)", ok, err)
	}
	if _, err := store.claimClassification(ctx, stream, group, msgID); err != nil {
		t.Fatalf("claimClassification after retain: %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d <= 0 {
		t.Fatalf("PTTL after stale writes = %s (err %v), want a positive retention TTL", d, err)
	}
	// tryStart must not re-open a terminal-retained hash.
	if s, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", time.Now(), time.Now().Add(time.Hour)); err != nil || s {
		t.Fatalf("tryStart after retain = (%v,%v), want (false,nil)", s, err)
	}
	if v, err := cli.HGet(ctx, key, "fn/h").Result(); err != nil || v != "ok" {
		t.Fatalf("marker after stale writes = %q (err %v), want ok", v, err)
	}
}

// TestIntegrationAtomicRetainTerminalDisabledWritesMarkerPersistent pins the
// explicit-disable contract (REDIS_INVOCATION_RETENTION empty/0/negative): the
// terminal marker is still written atomically, so terminal/stale-transition
// guards are unchanged, but NO TTL is applied — the hash is PERSISTed and stays
// terminal forever (until an operator deletes it). This is distinct from the
// positive-window path in the test above.
func TestIntegrationAtomicRetainTerminalDisabledWritesMarkerPersistent(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	now := time.Now()
	started, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour))
	if err != nil || !started {
		t.Fatalf("tryStart = (%v,%+v,%v)", started, claim, err)
	}
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim); err != nil || !ok {
		t.Fatalf("markComplete = (%v,%v)", ok, err)
	}

	// Retain with a disabled (zero) window: the marker is written, no TTL.
	if err := store.retainTerminal(ctx, stream, group, msgID, 0); err != nil {
		t.Fatalf("retainTerminal(disabled): %v", err)
	}
	if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal field = %q (err %v), want \"1\"", v, err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after disabled retain = %s (err %v), want -1 (persistent, no TTL)", d, err)
	}

	// Stale transitions are still inert: the marker guards them.
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/h", claim); err != nil || ok {
		t.Fatalf("markComplete after disabled retain = (%v,%v), want (false,nil)", ok, err)
	}
	if s, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", time.Now(), time.Now().Add(time.Hour)); err != nil || s {
		t.Fatalf("tryStart after disabled retain = (%v,%v), want (false,nil)", s, err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after stale writes = %s (err %v), want -1 (still persistent)", d, err)
	}

	// A repeated disabled retain must NOT introduce a TTL either (monotonic).
	if err := store.retainTerminal(ctx, stream, group, msgID, 0); err != nil {
		t.Fatalf("second retainTerminal(disabled): %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after repeated disabled retain = %s (err %v), want -1 (persistent)", d, err)
	}
}

// TestIntegrationAtomicRetainTerminalHonorsConfiguredWindow pins that
// store.retainTerminal applies exactly the window it is handed, not a
// package-level default: a short configured value yields that PTTL, so the
// resolved REDIS_INVOCATION_RETENTION (not a hard-coded constant) drives the
// terminal expiry.
func TestIntegrationAtomicRetainTerminalHonorsConfiguredWindow(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	now := time.Now()
	if started, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour)); err != nil || !started {
		t.Fatalf("tryStart = (%v,%v)", started, err)
	}

	const configured = 90 * time.Second
	if err := store.retainTerminal(ctx, stream, group, msgID, configured); err != nil {
		t.Fatalf("retainTerminal(configured): %v", err)
	}
	ttl, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after configured retain: %v", err)
	}
	if ttl <= 0 || ttl > configured {
		t.Fatalf("retention PTTL = %s, want (0, %s] (the configured window)", ttl, configured)
	}
	// It must be materially shorter than the default, proving the configured
	// value (not the default) was applied.
	if ttl >= DefaultInvocationRetention {
		t.Fatalf("retention PTTL = %s, want well under the default %s", ttl, DefaultInvocationRetention)
	}
}

// TestIntegrationAtomicRetentionTTLExpiresHash proves the terminal retention
// mechanism's expiry path, not just its setup: the retain script applies the TTL
// it is handed and Redis removes the hash once that TTL elapses. Production
// hands the script DefaultInvocationRetention, which is far too long to wait out in
// a test, so this runs the SAME script with a short test TTL on a unique key and
// polls until the key is gone. The configured production window is still pinned
// here (and by TestRetentionTTLMillis), so a shortened production TTL cannot hide
// behind the test TTL.
func TestIntegrationAtomicRetentionTTLExpiresHash(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	if got := retentionTTLMillis(DefaultInvocationRetention); got != int64(DefaultInvocationRetention/time.Millisecond) || DefaultInvocationRetention <= 0 {
		t.Fatalf("production retention TTL = %dms (const %s), want %dms positive",
			got, DefaultInvocationRetention, int64(DefaultInvocationRetention/time.Millisecond))
	}

	// Seed the hash as a real delivery would leave it, so expiry proves the TTL
	// removes genuine state rather than an empty key.
	now := time.Now()
	if started, _, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour)); err != nil || !started {
		t.Fatalf("tryStart = (%v,%v)", started, err)
	}

	// Run the same script production uses, with a short test TTL.
	const testTTL = 100 * time.Millisecond
	if _, err := retainTerminalScript.Run(ctx, cli, []string{key}, terminalField, testTTL.Milliseconds()).Int64(); err != nil {
		t.Fatalf("retainTerminalScript: %v", err)
	}
	if v, err := cli.HGet(ctx, key, terminalField).Result(); err != nil || v != "1" {
		t.Fatalf("terminal field = %q (err %v), want \"1\"", v, err)
	}
	ttl, err := cli.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("pttl after retain: %v", err)
	}
	if ttl <= 0 || ttl > testTTL {
		t.Fatalf("retention PTTL = %s, want (0, %s]", ttl, testTTL)
	}

	// Poll until Redis expires the hash. No fixed sleep: the assertion is the
	// expiry itself, bounded by a generous budget.
	testutil.WaitFor(t, 5*time.Second, "terminal-retained hash to expire via its TTL", func() bool {
		n, err := cli.Exists(ctx, key).Result()
		return err == nil && n == 0
	})
}

// TestIntegrationAtomicMakeRecoverableMigratesLegacyTTL pins the migration path:
// a hash written with a legacy TTL (as a pre-persistence Relay would) becomes
// persistent when makeRecoverable runs at the start of a delivery, but an
// already terminal-retained hash is left untouched (its retention TTL is never
// removed).
func TestIntegrationAtomicMakeRecoverableMigratesLegacyTTL(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	key := invocationStateKey(stream, group, msgID)

	// Seed a running marker with a legacy TTL, as an old Relay would.
	if err := cli.HSet(ctx, key, "fn/h", runningValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "aabbccdd"})).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := cli.PExpire(ctx, key, time.Hour).Err(); err != nil {
		t.Fatalf("pexpire: %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d <= 0 {
		t.Fatalf("seeded PTTL = %s (err %v), want a positive legacy TTL", d, err)
	}

	if err := store.makeRecoverable(ctx, stream, group, msgID); err != nil {
		t.Fatalf("makeRecoverable: %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d != -1 {
		t.Fatalf("PTTL after makeRecoverable = %s (err %v), want -1 (persistent)", d, err)
	}

	// Retain, then assert makeRecoverable never removes terminal retention.
	if err := store.retainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if err := store.makeRecoverable(ctx, stream, group, msgID); err != nil {
		t.Fatalf("makeRecoverable after retain: %v", err)
	}
	if d, err := cli.PTTL(ctx, key).Result(); err != nil || d <= 0 {
		t.Fatalf("PTTL after makeRecoverable on a terminal hash = %s (err %v), want retention TTL preserved", d, err)
	}

	// A missing key is a no-op.
	if err := cli.Del(ctx, key).Err(); err != nil {
		t.Fatalf("del: %v", err)
	}
	if err := store.makeRecoverable(ctx, stream, group, msgID); err != nil {
		t.Fatalf("makeRecoverable on a missing key: %v", err)
	}
}

// TestIntegrationAtomicCrashReclaimAfterDeadline pins crash recovery: a running
// marker that is never renewed does not expire out of Redis (its hash is
// persistent), but its deadline simply elapses, after which a reclaim starts the
// next attempt with a fresh token. There is no permanent lock.
func TestIntegrationAtomicCrashReclaimAfterDeadline(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	// A crashed attempt 1 whose deadline is 300ms out.
	deadline := now.Add(300 * time.Millisecond)
	seedMarker(t, cli, stream, group, msgID, "fn/h", runningValue(deadline, InvocationClaim{Attempt: 1, Token: "aabbccdd"}))
	// Before the deadline elapses the marker protects.
	if s, c, w, err := store.tryStart(ctx, stream, group, msgID, "fn/h", now, now.Add(time.Hour)); err != nil || s || c.Attempt != 1 || w <= 0 {
		t.Fatalf("pre-deadline claim = (%v,%+v,%s,%v), want protected", s, c, w, err)
	}
	// At/after the deadline the reclaim starts attempt 2 with a fresh token.
	after := deadline.Add(time.Millisecond)
	s, claim, _, err := store.tryStart(ctx, stream, group, msgID, "fn/h", after, after.Add(time.Hour))
	if err != nil || !s || claim.Attempt != 2 {
		t.Fatalf("post-deadline reclaim = (%v,%+v,%v), want (true, attempt 2, nil)", s, claim, err)
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

// pinnedDescriptor reads the reserved scheduleField value from a message's
// invocation-state hash ("" when absent).
func pinnedDescriptor(t *testing.T, cli *redis.Client, stream, group, msgID string) string {
	t.Helper()
	v, err := cli.HGet(context.Background(), invocationStateKey(stream, group, msgID), scheduleField).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatalf("hget schedule field: %v", err)
	}
	return v
}

// TestIntegrationAtomicScheduleAdmissionExactlyOneWinner drives many simultaneous
// schedule admissions for the same message through separate Redis clients
// (simulating replicas with potentially different templates), each proposing a
// DIFFERENT descriptor. Exactly one proposal is pinned, every other caller adopts
// it, exactly one handler's invocation field is ever claimed, and a subsequent
// admission with the winning descriptor is protected (not a second start).
func TestIntegrationAtomicScheduleAdmissionExactlyOneWinner(t *testing.T) {
	cli := testutil.RequireRedis(t)
	_, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	// Each caller proposes a handler that identifies it, so we can assert exactly
	// one handler field is claimed.
	const n = 12
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

	now := time.Now()
	start := make(chan struct{})
	type result struct {
		res scheduleStartResult
		err error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			desc := ScheduleDescriptor{
				Schedule: "cleanup",
				Handler:  fmt.Sprintf("jobs.h%d", i),
				Timeout:  30 * time.Second,
				Retries:  4,
			}
			res, err := stores[i].tryStartScheduled(ctx, stream, group, msgID, "fn/"+desc.Handler, now, now.Add(desc.Timeout), desc, true)
			results[i] = result{res, err}
		}(i)
	}
	close(start)
	wg.Wait()

	started := 0
	var winner ScheduleDescriptor
	winnerField := ""
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("caller %d: %v", i, r.err)
		}
		if !r.res.hasPinned {
			t.Fatalf("caller %d: no pinned descriptor in the reply", i)
		}
		if r.res.pinned.Schedule != "cleanup" {
			t.Fatalf("caller %d: pinned schedule = %q", i, r.res.pinned.Schedule)
		}
		if r.res.outcome == scheduleStartStarted {
			started++
			winner = r.res.pinned
			winnerField = "fn/" + r.res.pinned.Handler
		}
	}
	if started != 1 {
		t.Fatalf("started callers = %d, want exactly 1", started)
	}
	// The persisted descriptor is the winner's.
	got := pinnedDescriptor(t, cli, stream, group, msgID)
	if got != encodeScheduleDescriptor(winner) {
		t.Fatalf("persisted descriptor = %q, want the winner's %q", got, encodeScheduleDescriptor(winner))
	}
	// Exactly the winner's invocation field exists; no parallel handler field.
	fields, err := cli.HKeys(ctx, invocationStateKey(stream, group, msgID)).Result()
	if err != nil {
		t.Fatalf("hkeys: %v", err)
	}
	var invocationFields []string
	for _, f := range fields {
		if f != scheduleField {
			invocationFields = append(invocationFields, f)
		}
	}
	if len(invocationFields) != 1 || invocationFields[0] != winnerField {
		t.Fatalf("claimed invocation fields = %v, want [%s]", invocationFields, winnerField)
	}
}

// TestIntegrationAtomicScheduleAdmissionAdoptsPinnedWithoutReclaim pins the
// adopt-without-claim contract: once a descriptor is pinned, a delivery with no
// descriptor (its schedule was removed) adopts it and, while the invocation is
// protected, does not re-claim. Only the pinned handler's field is ever written.
func TestIntegrationAtomicScheduleAdmissionAdoptsPinnedWithoutReclaim(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	pinned := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	first, err := store.tryStartScheduled(ctx, stream, group, msgID, "fn/jobs.old", now, now.Add(pinned.Timeout), pinned, true)
	if err != nil || first.outcome != scheduleStartStarted {
		t.Fatalf("first admission = (%+v,%v), want started", first, err)
	}

	// A delivery that can no longer resolve the schedule (known=false) is told to
	// adopt the pinned descriptor; after adopting it, the delivery is protected
	// (the invocation is still in flight) and does not re-claim.
	adopt, err := store.tryStartScheduled(ctx, stream, group, msgID, "", now, now, ScheduleDescriptor{}, false)
	if err != nil {
		t.Fatalf("adopt admission: %v", err)
	}
	if adopt.outcome != scheduleStartConflict || !adopt.hasPinned || adopt.pinned.Handler != "jobs.old" {
		t.Fatalf("adopt outcome = (%v,%+v), want conflict on the pinned jobs.old", adopt.outcome, adopt.pinned)
	}
	second, err := store.tryStartScheduled(ctx, stream, group, msgID, "fn/jobs.old", now, now, adopt.pinned, true)
	if err != nil {
		t.Fatalf("second admission: %v", err)
	}
	if second.outcome != scheduleStartProtected {
		t.Fatalf("second outcome = %v, want protected (adopted, not started)", second.outcome)
	}
	if !second.hasPinned || second.pinned.Handler != "jobs.old" {
		t.Fatalf("adopted descriptor = %+v, want the pinned jobs.old", second.pinned)
	}
	// Only the pinned handler's field exists.
	fields, err := cli.HKeys(ctx, invocationStateKey(stream, group, msgID)).Result()
	if err != nil {
		t.Fatalf("hkeys: %v", err)
	}
	for _, f := range fields {
		if f != scheduleField && f != "fn/jobs.old" {
			t.Fatalf("unexpected invocation field %q (parallel handler claimed)", f)
		}
	}
}

// TestIntegrationAtomicScheduleAdmissionNoDescriptorNoProposalObsolete pins the
// never-admitted-removed case against real Redis: no descriptor pinned + no
// proposal reports no-admission and writes nothing.
func TestIntegrationAtomicScheduleAdmissionNoDescriptorNoProposalObsolete(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()

	res, err := store.tryStartScheduled(ctx, stream, group, msgID, "", time.Now(), time.Now(), ScheduleDescriptor{}, false)
	if err != nil {
		t.Fatalf("tryStartScheduled: %v", err)
	}
	if res.outcome != scheduleStartNoAdmission {
		t.Fatalf("outcome = %v, want no-admission", res.outcome)
	}
	if n, err := cli.Exists(ctx, invocationStateKey(stream, group, msgID)).Result(); err != nil || n != 0 {
		t.Fatalf("obsolete admission wrote state: exists=%d err=%v", n, err)
	}
}

// TestIntegrationAtomicScheduleDescriptorSurvivesRetentionAndStaysImmutable pins
// the descriptor's lifetime on the real store: it is written atomically with the
// first claim, it is immutable while recoverable (a different proposal adopts and
// does not overwrite), and after the message leaves the PEL the hash retains it
// under the terminal-retention TTL, where a stale admission neither re-pins nor
// re-opens the invocation.
func TestIntegrationAtomicScheduleDescriptorSurvivesRetentionAndStaysImmutable(t *testing.T) {
	cli := testutil.RequireRedis(t)
	store, stream, group, msgID := atomicStateStore(t, cli)
	ctx := context.Background()
	now := time.Now()

	pinned := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.old", Timeout: time.Hour, Retries: 4}
	if _, err := store.tryStartScheduled(ctx, stream, group, msgID, "fn/jobs.old", now, now.Add(pinned.Timeout), pinned, true); err != nil {
		t.Fatalf("admission: %v", err)
	}
	// Immutable: a different proposal adopts the pinned one and does not overwrite.
	other := ScheduleDescriptor{Schedule: "cleanup", Handler: "jobs.new", Timeout: time.Minute, Retries: 0}
	res, err := store.tryStartScheduled(ctx, stream, group, msgID, "fn/jobs.new", now, now.Add(other.Timeout), other, true)
	if err != nil {
		t.Fatalf("conflicting admission: %v", err)
	}
	if res.outcome != scheduleStartConflict || res.pinned.Handler != "jobs.old" {
		t.Fatalf("conflict = (%v,%+v), want conflict on jobs.old", res.outcome, res.pinned)
	}
	if got := pinnedDescriptor(t, cli, stream, group, msgID); got != encodeScheduleDescriptor(pinned) {
		t.Fatalf("descriptor overwritten: %q, want %q", got, encodeScheduleDescriptor(pinned))
	}
	// Complete the running marker, ACK-retain, then a stale delivery: the
	// descriptor stays readable and the terminal-retained hash is never reopened.
	running := marker(t, cli, stream, group, msgID, "fn/jobs.old")
	claim := InvocationClaim{Attempt: markerAttemptOf(t, running), Token: activeToken(running)}
	if ok, err := store.markComplete(ctx, stream, group, msgID, "fn/jobs.old", claim); err != nil || !ok {
		t.Fatalf("markComplete = (%v,%v)", ok, err)
	}
	if err := store.retainTerminal(ctx, stream, group, msgID, DefaultInvocationRetention); err != nil {
		t.Fatalf("retainTerminal: %v", err)
	}
	if got := pinnedDescriptor(t, cli, stream, group, msgID); got != encodeScheduleDescriptor(pinned) {
		t.Fatalf("descriptor after retention = %q, want the pinned one", got)
	}
	stale, err := store.tryStartScheduled(ctx, stream, group, msgID, "", time.Now(), time.Now(), ScheduleDescriptor{}, false)
	if err != nil {
		t.Fatalf("stale admission: %v", err)
	}
	if stale.outcome != scheduleStartTerminal {
		t.Fatalf("stale outcome = %v, want terminal (never re-opened)", stale.outcome)
	}
}

// markerAttemptOf extracts the attempt number from a well-formed active marker
// ("running|next_attempt_at:<deadline>:<attempt>:<token>"). It returns 0 when the
// marker cannot be parsed.
func markerAttemptOf(t *testing.T, v string) int {
	t.Helper()
	rest := ""
	switch {
	case strings.HasPrefix(v, "running:"):
		rest = strings.TrimPrefix(v, "running:")
	case strings.HasPrefix(v, "next_attempt_at:"):
		rest = strings.TrimPrefix(v, "next_attempt_at:")
	default:
		t.Fatalf("marker %q is not an active marker", v)
	}
	_, rest, _ = strings.Cut(rest, ":")
	attemptStr, _, _ := strings.Cut(rest, ":")
	n, err := strconv.Atoi(attemptStr)
	if err != nil {
		t.Fatalf("marker %q attempt parse: %v", v, err)
	}
	return n
}

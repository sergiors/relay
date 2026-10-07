//go:build integration

// This file exercises the runner's shutdown-cancellation disposition at the
// Redis stream boundary (F-003): a delivery canceled by the worker lifecycle
// must not be ACKed or dead-lettered, a pre-claim cancellation must not spend a
// handler attempt, and a post-claim cancellation must leave a recoverable
// running marker (not a retry or exhaustion) so a later delivery completes the
// invocation. It requires Redis at REDIS_TEST_ADDR (default localhost:6379),
// matching the other stream integration tests.
package stream

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// TestIntegrationShutdownCancellationLeavesPendingAndReclaimSucceeds pins the
// stream-layer half of F-003: a handler whose delivery is canceled by the worker
// lifecycle (it returns the runner's ErrInvocationNotEligible disposition after
// ctx is done) leaves the message pending in the PEL — never ACKed, never
// dead-lettered — and a fresh consumer in the same group reclaims and completes
// it. The message is proven DELIVERED (in the PEL) before shutdown, so the
// assertion distinguishes it from a still-unread stream entry.
func TestIntegrationShutdownCancellationLeavesPendingAndReclaimSucceeds(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("cancel-%d", time.Now().UnixNano())
	streamName, groupName := prefix+"-stream", prefix+"-group"

	envA := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "cancel-A"})
	id := envA.xadd(t, `{"cancel":1}`)

	entered := make(chan struct{})
	envA.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		if msgID != id {
			return nil
		}
		close(entered)
		// Model the runner's canceled execution: wait for lifecycle cancellation,
		// then leave the message pending without a retry/exhaustion transition.
		<-ctx.Done()
		return ErrInvocationNotEligible
	})

	<-entered
	envA.waitDelivered(t, id) // deterministic: the entry is in the PEL
	envA.stop(t)              // lifecycle shutdown; Consume joins

	if _, ok := envA.pending()[id]; !ok {
		t.Fatalf("message %s left the PEL on shutdown; want pending (no ACK, no DLQ)", id)
	}
	if got := len(envA.dlq()); got != 0 {
		t.Fatalf("shutdown dead-lettered %d entries; want 0", got)
	}

	// A fresh consumer in the same group reclaims the now-idle pending message
	// and completes it.
	envB := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "cancel-B"})
	acked := make(chan struct{})
	envB.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		if msgID != id {
			return nil
		}
		close(acked)
		return nil
	})
	select {
	case <-acked:
	case <-time.After(8 * time.Second):
		t.Fatal("reclaimed message was not delivered to the fresh consumer")
	}
	envB.waitGone(t, id)
	envB.stop(t)
}

// TestIntegrationCancellationAfterClaimKeepsRecoverableMarker pins the post-claim
// half of F-003 against real Redis state: a shutdown cancellation that lands
// AFTER a confirmed TryStart spends that attempt (the accepted claim-before-
// execute window) but writes NO retry backoff and NO exhausted marker, and
// leaves the running marker present and PERSISTENT while the message is pending.
// A later reclaim, once the deadline elapses, claims attempt 2 and completes the
// invocation — proving the shutdown changed no persisted retry/exhaustion state
// and lost nothing.
func TestIntegrationCancellationAfterClaimKeepsRecoverableMarker(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("claimcancel-%d", time.Now().UnixNano())
	streamName, groupName := prefix+"-stream", prefix+"-group"

	envA := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "claimcancel-A"})
	id := envA.xadd(t, `{"cancel":2}`)
	key := invocationStateKey(streamName, groupName, id)

	claimed := make(chan struct{})
	envA.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, err := p.TryStart("fn/h", 400*time.Millisecond)
		if err != nil || !started {
			return ErrInvocationNotEligible
		}
		_ = claim
		close(claimed)
		// Post-claim lifecycle cancellation: the runner's disposition is the
		// not-eligible signal with NO RecordFailure / MarkExhausted.
		<-ctx.Done()
		return ErrInvocationNotEligible
	})

	<-claimed
	envA.waitDelivered(t, id)

	// While still pending, the running marker must be present and PERSISTENT (no
	// TTL): the claim is recoverable, not terminal.
	v, err := envA.client.HGet(context.Background(), key, "fn/h").Result()
	if err != nil {
		t.Fatalf("hget running marker: %v", err)
	}
	if !strings.HasPrefix(v, "running:") {
		t.Fatalf("invocation marker = %q, want a running:<deadline_ms>:<attempt>:<token> marker", v)
	}
	if d, err := envA.client.PTTL(context.Background(), key).Result(); err != nil || d != -1 {
		t.Fatalf("invocation-state PTTL = %s (err %v), want -1 (persistent while recoverable)", d, err)
	}

	envA.stop(t)
	if _, ok := envA.pending()[id]; !ok {
		t.Fatalf("message %s not pending after the canceled delivery", id)
	}
	if got := len(envA.dlq()); got != 0 {
		t.Fatalf("canceled delivery dead-lettered %d entries; want 0", got)
	}

	// A fresh consumer reclaims after the deadline: attempt 2 runs and completes.
	envB := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "claimcancel-B"})
	var (
		mu       sync.Mutex
		attempts []int
	)
	acked := make(chan struct{})
	envB.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, err := p.TryStart("fn/h", time.Second)
		if err != nil {
			return err
		}
		if !started {
			// Still protected by the 400ms running deadline: stay pending and
			// let a later reclaim retry.
			return ErrInvocationNotEligible
		}
		if !p.MarkComplete("fn/h", claim) {
			return ErrInvocationNotEligible
		}
		mu.Lock()
		attempts = append(attempts, claim.Attempt)
		mu.Unlock()
		close(acked)
		return nil
	})
	select {
	case <-acked:
	case <-time.After(8 * time.Second):
		t.Fatal("reclaimed invocation did not complete after the canceled attempt's deadline")
	}
	envB.waitGone(t, id)
	envB.stop(t)

	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 1 || attempts[0] != 2 {
		t.Fatalf("reclaim attempts = %v, want [2] (shutdown spent attempt 1 but made no retry/exhaustion transition)", attempts)
	}
}

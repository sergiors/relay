//go:build integration

// Focused F-03 regression coverage for the destructive edge cases of group
// recovery against a real Redis server: deleting the whole stream key (not
// just the group), an external actor deleting the group as fast as Relay
// recreates it (recovery must be paced, never a hot loop), concurrent NOGROUP
// observers coalescing into ONE recovery, and a Redis outage DURING a recovery
// attempt — which must leave the generation unadvanced and readiness
// unhealthy until a confirmed consumption success.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping.
package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// TestIntegrationStreamKeyDeletionRecreatedProcessingResumes pins stream
// deletion (not only XGROUP DESTROY): when the whole stream key disappears
// under a running Consumer, the next group operation observes NOGROUP, the
// shared recovery recreates the stream (MKSTREAM) and its group at ID 0, and
// processing resumes — without restarting the Consumer.
func TestIntegrationStreamKeyDeletionRecreatedProcessingResumes(t *testing.T) {
	testutil.RequireRedis(t)
	m := metrics.New()
	e := newEnv(t, ConsumerConfig{Metrics: m})

	var mu sync.Mutex
	runs := map[string]int{}
	e.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		if p.IsComplete("fn/h") {
			return nil
		}
		started, claim, _, err := p.TryStart("fn/h", 5*time.Second)
		if err != nil {
			return err
		}
		if !started {
			return ErrInvocationNotEligible
		}
		mu.Lock()
		runs[msgID]++
		mu.Unlock()
		p.MarkComplete("fn/h", claim)
		return nil
	})
	count := func(msgID string) int {
		mu.Lock()
		defer mu.Unlock()
		return runs[msgID]
	}

	// Given a running consumer that processed one event from a real stream.
	first := e.xadd(t, `{"a":1}`)
	testutil.WaitFor(t, 8*time.Second, "first event processed", func() bool {
		return count(first) == 1
	})
	e.waitGone(t, first)

	// When the whole stream key is deleted (stream AND group vanish) and a
	// producer appends the next event to the bare stream key.
	if err := e.client.Del(context.Background(), e.stream).Err(); err != nil {
		t.Fatalf("del stream: %v", err)
	}
	second := e.xadd(t, `{"b":2}`)

	// Then recovery recreates stream + group from ID 0 and the event is
	// processed exactly once by the same Consumer.
	testutil.WaitFor(t, 10*time.Second, "stream and group recreated, second event processed", func() bool {
		if !hasGroup(t, e.client, e.stream, e.group) {
			return false
		}
		if count(second) != 1 {
			return false
		}
		_, stillPending := e.pending()[second]
		return !stillPending
	})
	if got := lastDeliveredID(t, e.client, e.stream, e.group); got != second {
		t.Fatalf("last-delivered = %q, want %q (the recreated group replays from ID 0)", got, second)
	}
	if created := recoveryCounter(m, metrics.GroupRecoveryCreated); created < 1 {
		t.Fatalf("group recovery creations = %d, want >= 1", created)
	}
	testutil.WaitFor(t, 5*time.Second, "consumer healthy after stream recreation", func() bool {
		return e.consumer.Healthy()
	})
	e.stop(t)
}

// TestRecoveryPacesRepeatedDeletionAndCoalescesConcurrentObservers pins the
// recovery loop's DoS bounds against a group deleted as fast as Relay
// recreates it, plus the coalescing guarantee for concurrent NOGROUP
// observers:
//
//   - the first recovery retries immediately (wait 0);
//   - a second deletion with no confirmed consumption success in between is
//     paced by the bounded backoff instead of hot-looping XGROUP CREATE;
//   - a confirmed consumption success resets the pacing;
//   - N concurrent observers of the same loss produce exactly ONE recovery
//     (one generation advance, one metric increment, zero failures — the
//     loser's BUSYGROUP is benign).
func TestRecoveryPacesRepeatedDeletionAndCoalescesConcurrentObservers(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("recvpace-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	m := metrics.New()
	const paced = 40 * time.Millisecond
	c := NewConsumer(ConsumerConfig{
		Client:   cli,
		Stream:   streamName,
		Group:    group,
		Consumer: prefix + "-consumer",
		Log:      testutil.DiscardLogger(),
		Metrics:  m,
		// Deterministic pacing: a single-entry table with identity jitter, so
		// every paced wait is exactly paced.
		backoffTable:  []time.Duration{paced},
		backoffJitter: func(f float64) float64 { return f },
	})
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() { cleanupEnvRedisKeys(context.Background(), cli, streamName, group) })
	destroy := func() {
		t.Helper()
		if err := cli.XGroupDestroy(ctx, streamName, group).Err(); err != nil {
			t.Fatalf("xgroup destroy: %v", err)
		}
	}
	recoverOnce := func(observed uint64) (time.Duration, error) {
		t.Helper()
		return c.recoverConsumerGroup(ctx, observed)
	}

	// Given the group destroyed: the first recovery succeeds and retries
	// immediately.
	destroy()
	if wait, err := recoverOnce(c.groupGeneration()); err != nil || wait != 0 {
		t.Fatalf("first recovery wait = (%s, %v), want (0, nil)", wait, err)
	}
	if gen := c.groupGeneration(); gen != 1 {
		t.Fatalf("generation after first recovery = %d, want 1", gen)
	}

	// When it is destroyed again with no confirmed consumption success in
	// between, the second recovery is paced by the bounded backoff.
	destroy()
	wait2, err := recoverOnce(c.groupGeneration())
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if wait2 != paced {
		t.Fatalf("second recovery wait = %s, want exactly %s (repeated deletion must be paced)", wait2, paced)
	}
	if gen := c.groupGeneration(); gen != 2 {
		t.Fatalf("generation after second recovery = %d, want 2", gen)
	}

	// A confirmed consumption success (the production noteConsumeSuccess path)
	// resets the pacing...
	c.noteConsumeSuccess(c.groupGeneration(), c.recoveryEpoch.Load())
	destroy()
	if wait, err := recoverOnce(c.groupGeneration()); err != nil || wait != 0 {
		t.Fatalf("recovery after confirmed consumption wait = (%s, %v), want (0, nil)", wait, err)
	}

	// ...and concurrent observers of the next loss coalesce on ONE recovery:
	// every goroutine passes the same observed generation, exactly one enters
	// the serialized attempt, and every other is a no-op.
	destroy()
	observed := c.groupGeneration()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.recoverConsumerGroup(ctx, observed)
		}()
	}
	wg.Wait()

	if got, want := c.groupGeneration(), observed+1; got != want {
		t.Fatalf("generation after concurrent observers = %d, want %d (exactly one recovery)", got, want)
	}
	if got := recoveryCounter(m, metrics.GroupRecoveryCreated); got != 4 {
		t.Fatalf("group recovery creations = %d, want 4 (one per loss: three paced + one coalesced)", got)
	}
	if got := recoveryCounter(m, metrics.GroupRecoveryFailed); got != 0 {
		t.Fatalf("group recovery failures = %d, want 0 (BUSYGROUP losers are not failures)", got)
	}
	if !hasGroup(t, cli, streamName, group) {
		t.Fatal("group must exist after the coalesced recovery")
	}
}

// TestRecoveryDuringRedisOutageStaysUnhealthyThenResumes pins the outage
// DURING recovery: a NOGROUP observation whose repair attempt cannot reach
// Redis fails without advancing the generation (nothing is confirmed), the
// consumer reports unhealthy and paces every retry, and when Redis returns the
// next attempt repairs the group — but readiness stays pending until a
// confirmed consumption success observes the current generation.
func TestRecoveryDuringRedisOutageStaysUnhealthyThenResumes(t *testing.T) {
	var buf testutil.SyncBuffer
	live := testutil.RequireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("recvoutage-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	t.Cleanup(func() { cleanupEnvRedisKeys(context.Background(), live, streamName, group) })

	// A client pointing at a free port with no listener: every command fails
	// fast (connect refused), modelling the outage.
	dead := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("127.0.0.1:%d", testutil.FreePort(t)),
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = dead.Close() })

	m := metrics.New()
	const paced = 30 * time.Millisecond
	c := NewConsumer(ConsumerConfig{
		Client:        dead,
		Stream:        streamName,
		Group:         group,
		Consumer:      prefix + "-consumer",
		Log:           slog.New(slog.NewTextHandler(&buf, nil)),
		Metrics:       m,
		backoffTable:  []time.Duration{paced},
		backoffJitter: func(f float64) float64 { return f },
	})
	nogroup := errors.New("NOGROUP No such key or consumer group")

	// When the repair cannot reach Redis, the recovery attempt fails without
	// advancing the generation: nothing is confirmed, readiness is unhealthy,
	// and the failure is counted and logged.
	gen0 := c.groupGeneration()
	handled, wait := c.handleGroupLoss(ctx, "read", gen0, nogroup)
	if !handled {
		t.Fatal("a NOGROUP observation must be handled as a group recovery")
	}
	if wait != paced {
		t.Fatalf("failed recovery wait = %s, want %s (failures are paced)", wait, paced)
	}
	if gen := c.groupGeneration(); gen != gen0 {
		t.Fatalf("generation = %d after a failed recovery, want %d (never advance unconfirmed)", gen, gen0)
	}
	if c.Healthy() {
		t.Fatal("consumer must be unhealthy while the group repair is outstanding")
	}
	if got := recoveryCounter(m, metrics.GroupRecoveryFailed); got != 1 {
		t.Fatalf("recovery failures = %d, want 1", got)
	}
	if !strings.Contains(buf.String(), "recovery failed") {
		t.Fatalf("expected a failed-recovery log, got %q", buf.String())
	}

	// Repeated attempts during the outage keep failing and stay paced.
	if handled, wait := c.handleGroupLoss(ctx, "read", gen0, nogroup); !handled || wait != paced {
		t.Fatalf("second failed recovery = (handled %v, wait %s), want (true, %s)", handled, wait, paced)
	}
	if got := c.groupGeneration(); got != gen0 {
		t.Fatalf("generation = %d after repeated failures, want %d", got, gen0)
	}
	if got := recoveryCounter(m, metrics.GroupRecoveryFailed); got != 2 {
		t.Fatalf("recovery failures = %d, want 2", got)
	}

	// When Redis returns, the next attempt repairs the group for real.
	// Test seam: point the consumer's client at the live server, exactly as a
	// restored connection would (the invocation store is not exercised here).
	c.client = live
	if handled, _ := c.handleGroupLoss(ctx, "read", gen0, nogroup); !handled {
		t.Fatal("the post-outage NOGROUP must still be handled")
	}
	if got := c.groupGeneration(); got != gen0+1 {
		t.Fatalf("generation after heal = %d, want %d", got, gen0+1)
	}
	if got := recoveryCounter(m, metrics.GroupRecoveryCreated); got != 1 {
		t.Fatalf("recovery creations = %d, want 1", got)
	}
	if !hasGroup(t, live, streamName, group) {
		t.Fatal("the group must exist on the live server after the healed recovery")
	}
	// Readiness stays pending across the repair: only a confirmed consumption
	// success under the current generation/epoch clears it.
	if c.Healthy() {
		t.Fatal("readiness must stay unhealthy until a confirmed consumption success")
	}
	c.noteConsumeSuccess(c.groupGeneration(), c.recoveryEpoch.Load())
	if !c.Healthy() {
		t.Fatal("consumer must be healthy after the confirmed consumption success")
	}
}

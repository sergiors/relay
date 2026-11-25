//go:build integration

// Focused F-03 regression coverage for consumer-group recovery against a real
// Redis server:
//
//   - an active XGROUP DESTROY is repaired automatically without restarting
//     the Consumer, and processing resumes from ID 0 with readiness restored;
//   - concurrent consumers recover from the same loss without a duplicate
//     claim (BUSYGROUP is benign for the loser);
//   - a recreated group replays pending and retained entries while completed
//     invocation state dedupes the re-execution;
//   - expired terminal state stops deduping, so the replay genuinely reruns;
//   - partial multi-handler, in-flight, and retry-deadline state survives the
//     recreation byte-for-byte, with persisted attempts carried forward.
//
// Excluded from the default suite by the integration build tag; REQUIRES Redis
// at REDIS_TEST_ADDR (default localhost:6379), failing rather than skipping.
package stream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// hasGroup reports whether the named consumer group exists on the stream.
func hasGroup(t *testing.T, cli *redis.Client, stream, group string) bool {
	t.Helper()
	groups, err := cli.XInfoGroups(context.Background(), stream).Result()
	if err != nil {
		return false
	}
	for _, g := range groups {
		if g.Name == group {
			return true
		}
	}
	return false
}

// lastDeliveredID returns the group's last-delivered ID, or "" when the group
// does not exist.
func lastDeliveredID(t *testing.T, cli *redis.Client, stream, group string) string {
	t.Helper()
	groups, err := cli.XInfoGroups(context.Background(), stream).Result()
	if err != nil {
		return ""
	}
	for _, g := range groups {
		if g.Name == group {
			return g.LastDeliveredID
		}
	}
	return ""
}

// readUnreadIntoPEL reads every unread stream entry into the given consumer's
// PEL and returns them by message ID, so a test can drive each replayed
// delivery through processMessage directly.
func readUnreadIntoPEL(t *testing.T, cli *redis.Client, stream, group, consumer string) map[string]redis.XMessage {
	t.Helper()
	msgs, err := cli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    100,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup unread: %v", err)
	}
	out := map[string]redis.XMessage{}
	for _, s := range msgs {
		for _, m := range s.Messages {
			out[m.ID] = m
		}
	}
	return out
}

// recoveryCounter reads one labeled consumer-group recovery counter from the
// registry.
func recoveryCounter(m *metrics.Registry, result string) int64 {
	return m.CounterLabels(metrics.MetricGroupRecoveries, []metrics.Label{
		{Name: "result", Value: result},
	})
}

func TestIntegrationAckFenceUsesSharedGroupGeneration(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("group-generation-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	generationKey := groupGenerationKey(streamName, group)
	stateKey := ""
	t.Cleanup(func() {
		keys := []string{streamName, generationKey}
		if stateKey != "" {
			keys = append(keys, stateKey)
		}
		_ = cli.Del(ctx, keys...).Err()
	})

	cfg := ConsumerConfig{
		Client: cli, Stream: streamName, Group: group, Consumer: "same-consumer",
		Log: testutil.DiscardLogger(),
	}
	workerA := NewConsumer(cfg)
	workerB := NewConsumer(cfg)
	ensureErrs := make(chan error, 2)
	var ensureWG sync.WaitGroup
	for _, worker := range []*Consumer{workerA, workerB} {
		ensureWG.Add(1)
		go func(worker *Consumer) {
			defer ensureWG.Done()
			ensureErrs <- worker.EnsureGroup(ctx)
		}(worker)
	}
	ensureWG.Wait()
	close(ensureErrs)
	for err := range ensureErrs {
		if err != nil {
			t.Fatalf("racing worker ensure group: %v", err)
		}
	}
	oldToken, err := readGroupGeneration(ctx, cli, streamName, group)
	if err != nil {
		t.Fatalf("read initial generation: %v", err)
	}
	workerToken, err := readGroupGeneration(ctx, cli, streamName, group)
	if err != nil || workerToken != oldToken {
		t.Fatalf("racing workers' generation = (%q, %v), want shared token %q", workerToken, err, oldToken)
	}
	if gotOutcome, gotToken, err := ensureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure existing group: %v", err)
	} else if gotOutcome != GroupAlreadyExists || gotToken != oldToken {
		t.Fatalf("existing group ensure = (%v, %q), want existing token %q", gotOutcome, gotToken, oldToken)
	}

	id, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]any{"value": `{"event_name":"generation-fence"}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	stateKey = invocationStateKey(streamName, group, id)
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: cfg.Consumer, Streams: []string{streamName, ">"},
		Count: 1,
	}).Result(); err != nil {
		t.Fatalf("worker A read: %v", err)
	}
	oldDelivery := withDeliveryGroupGeneration(
		withDeliveryGeneration(ctx, workerA.groupGeneration()), oldToken)

	if err := cli.XGroupDestroy(ctx, streamName, group).Err(); err != nil {
		t.Fatalf("destroy group: %v", err)
	}
	if err := workerB.EnsureGroup(ctx); err != nil {
		t.Fatalf("worker B recreate group: %v", err)
	}
	newToken, err := readGroupGeneration(ctx, cli, streamName, group)
	if err != nil {
		t.Fatalf("read recreated generation: %v", err)
	}
	if newToken == oldToken {
		t.Fatal("group recreation retained its old generation token")
	}
	if gotOutcome, gotToken, err := ensureGroup(ctx, cli, streamName, group); err != nil {
		t.Fatalf("ensure recreated existing group: %v", err)
	} else if gotOutcome != GroupAlreadyExists || gotToken != newToken {
		t.Fatalf("existing recreated group ensure = (%v, %q), want existing token %q", gotOutcome, gotToken, newToken)
	}

	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: cfg.Consumer, Streams: []string{streamName, ">"},
		Count: 1,
	}).Result(); err != nil {
		t.Fatalf("same consumer read replay: %v", err)
	}
	oldResult, err := workerA.ackDelivery(oldDelivery, id, workerA.groupGeneration(), true)
	if err != nil {
		t.Fatalf("stale worker A ack: %v", err)
	}
	if oldResult.Settled || oldResult.Disposition != dispositionStaleGeneration {
		t.Fatalf("stale worker A ack = %+v, want stale-generation unresolved", oldResult)
	}
	pending, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName, Group: group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil {
		t.Fatalf("pending after stale ack: %v", err)
	}
	if len(pending) != 1 || pending[0].Consumer != cfg.Consumer {
		t.Fatalf("pending after stale ack = %+v, want same-consumer replay retained", pending)
	}
	if exists, err := cli.Exists(ctx, stateKey).Result(); err != nil || exists != 0 {
		t.Fatalf("state after stale ACK = (%d, %v), want no terminal marker or receipt", exists, err)
	}

	newDelivery := withDeliveryGroupGeneration(
		withDeliveryGeneration(ctx, workerB.groupGeneration()), newToken)
	newResult, err := workerB.ackDelivery(newDelivery, id, workerB.groupGeneration(), true)
	if err != nil {
		t.Fatalf("new-generation ack: %v", err)
	}
	if !newResult.Settled || !newResult.Removed {
		t.Fatalf("new-generation ack = %+v, want settled and removed", newResult)
	}
	if terminal, err := cli.HGet(ctx, stateKey, terminalField).Result(); err != nil || terminal != "1" {
		t.Fatalf("new-generation terminal marker = (%q, %v), want 1", terminal, err)
	}
	if receipt, err := cli.HGet(ctx, stateKey, ackReceiptField).Result(); err != nil || receipt != "1" {
		t.Fatalf("new-generation ACK receipt = (%q, %v), want 1", receipt, err)
	}
}

func TestIntegrationMissingGenerationMetadataRepairsWithoutGroupReset(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("group-generation-repair-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	generationKey := groupGenerationKey(streamName, group)
	t.Cleanup(func() {
		_ = cli.Del(ctx, streamName, generationKey).Err()
	})
	c := NewConsumer(ConsumerConfig{
		Client: cli, Stream: streamName, Group: group, Consumer: "repair-consumer",
		Log: testutil.DiscardLogger(),
	})
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	oldToken, err := readGroupGeneration(ctx, cli, streamName, group)
	if err != nil {
		t.Fatalf("read initial generation: %v", err)
	}
	id, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName, Values: map[string]any{"value": `{"event_name":"repair"}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if _, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: c.consumer, Streams: []string{streamName, ">"},
		Count: 1,
	}).Result(); err != nil {
		t.Fatalf("read pending entry: %v", err)
	}
	lastDelivered := lastDeliveredID(t, cli, streamName, group)
	if err := cli.Del(ctx, generationKey).Err(); err != nil {
		t.Fatalf("delete generation metadata: %v", err)
	}
	repairedToken, err := readGroupGeneration(ctx, cli, streamName, group)
	if err != nil {
		t.Fatalf("repair generation metadata: %v", err)
	}
	if repairedToken == "" || repairedToken == oldToken {
		t.Fatalf("repaired token = %q, want fresh non-empty token", repairedToken)
	}
	if !hasGroup(t, cli, streamName, group) || lastDeliveredID(t, cli, streamName, group) != lastDelivered {
		t.Fatal("metadata repair reset or changed the existing consumer group")
	}
	pending, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName, Group: group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil {
		t.Fatalf("inspect pending entry after repair: %v", err)
	}
	if len(pending) != 1 || pending[0].Consumer != c.consumer {
		t.Fatalf("pending after metadata repair = %+v, want existing entry preserved", pending)
	}
}

func TestIntegrationDirectProcessMessageUsesStoreGenerationFallback(t *testing.T) {
	cli := testutil.RequireRedis(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("group-generation-direct-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	t.Cleanup(func() {
		cleanupEnvRedisKeys(ctx, cli, streamName, group)
	})
	c := NewConsumer(ConsumerConfig{
		Client: cli, Stream: streamName, Group: group, Consumer: "direct-consumer",
		Log: testutil.DiscardLogger(),
	})
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	id, err := cli.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName, Values: map[string]any{"value": `{"event_name":"direct-ack"}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	streams, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: c.consumer, Streams: []string{streamName, ">"},
		Count: 1,
	}).Result()
	if err != nil {
		t.Fatalf("read into PEL: %v", err)
	}
	if len(streams) != 1 || len(streams[0].Messages) != 1 {
		t.Fatalf("read messages = %+v, want one", streams)
	}

	c.processMessage(ctx, streams[0].Messages[0], 1, func(context.Context, string, map[string]any) error {
		return nil
	})
	pending, err := cli.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamName, Group: group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil {
		t.Fatalf("inspect pending entry: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("direct processMessage left PEL entry pending: %+v", pending)
	}
}

// TestIntegrationXGroupDestroyRecreatedFromZeroProcessingResumes pins the F-03
// runtime repair: while Consume is running, an external actor destroys the
// active consumer group. The next group operation observes NOGROUP and the
// Consumer recreates the group at ID 0 WITHOUT being restarted — the backlog
// replays (including the previously acknowledged entry), completed invocation
// state dedupes its re-execution, the new event runs exactly once, and
// readiness returns to healthy.
func TestIntegrationXGroupDestroyRecreatedFromZeroProcessingResumes(t *testing.T) {
	testutil.RequireRedis(t)
	m := metrics.New()
	e := newEnv(t, ConsumerConfig{Metrics: m})

	var mu sync.Mutex
	executed := map[string]int{}
	skipped := map[string]int{}
	e.start(func(ctx context.Context, msgID string, _ map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		// Retained completed state dedupes a replayed entry: skip the handler
		// and let the message be acknowledged again.
		if p.IsComplete("fn/h") {
			mu.Lock()
			skipped[msgID]++
			mu.Unlock()
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
		executed[msgID]++
		mu.Unlock()
		if !p.MarkComplete("fn/h", claim) {
			return errors.New("mark complete refused")
		}
		return nil
	})
	counts := func(msgID string) (run, skip int) {
		mu.Lock()
		defer mu.Unlock()
		return executed[msgID], skipped[msgID]
	}

	// Given a running consumer that processed and acknowledged one event.
	first := e.xadd(t, `{"a":1}`)
	testutil.WaitFor(t, 8*time.Second, "first event executed once", func() bool {
		run, _ := counts(first)
		return run == 1
	})
	e.waitGone(t, first)

	// When an external actor destroys the active group while Consume runs and
	// a new event arrives.
	if err := e.client.XGroupDestroy(context.Background(), e.stream, e.group).Err(); err != nil {
		t.Fatalf("xgroup destroy: %v", err)
	}
	second := e.xadd(t, `{"b":2}`)

	// Then the group is recreated at ID 0 by the same Consumer: the replay
	// reaches the new event (so the recreated group's last-delivered ID passes
	// it) and the new event executes exactly once.
	testutil.WaitFor(t, 10*time.Second, "group recreated and second event processed", func() bool {
		if !hasGroup(t, e.client, e.stream, e.group) {
			return false
		}
		if lastDeliveredID(t, e.client, e.stream, e.group) != second {
			return false
		}
		run, _ := counts(second)
		if run != 1 {
			return false
		}
		_, pendingEntry := e.pending()[second]
		return !pendingEntry
	})

	// The replayed acknowledged entry was re-delivered from ID 0 but its
	// retained completed state deduped the handler: still exactly one
	// execution, one observed skip.
	runFirst, skipFirst := counts(first)
	if runFirst != 1 {
		t.Fatalf("first event executions = %d, want 1 (completed state must dedupe the replay)", runFirst)
	}
	if skipFirst < 1 {
		t.Fatalf("first event skips = %d, want >= 1 (the replay must reach the completed state)", skipFirst)
	}

	// The recovery is observable: generation advanced, recovery metric
	// counted, and readiness is healthy again (a confirmed consumption
	// success cleared the recovering state).
	if gen := e.consumer.groupGeneration(); gen < 1 {
		t.Fatalf("group generation = %d, want >= 1 (a local recovery must be recorded)", gen)
	}
	created := recoveryCounter(m, metrics.GroupRecoveryCreated)
	existing := recoveryCounter(m, metrics.GroupRecoveryExisting)
	if created+existing < 1 {
		t.Fatalf("group recoveries = created %d existing %d, want >= 1", created, existing)
	}
	testutil.WaitFor(t, 5*time.Second, "consumer healthy after recovery", func() bool {
		return e.consumer.Healthy()
	})
	e.stop(t)
}

// TestIntegrationConcurrentConsumersRecoverGroupLossWithoutDuplicateClaim
// pins the multi-consumer recovery contract: two Consumers share one
// stream/group, an external actor destroys the group while both are running,
// and both repair their view (the second XGROUP CREATE gets BUSYGROUP, which is
// benign). The next event is executed exactly once across both consumers —
// neither the concurrent recovery nor a cross-consumer reclaim produces a
// duplicate claim.
func TestIntegrationConcurrentConsumersRecoverGroupLossWithoutDuplicateClaim(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("grploss-%d", time.Now().UnixNano())
	streamName, group := prefix+"-stream", prefix+"-group"
	mA, mB := metrics.New(), metrics.New()
	envA := newEnv(t, ConsumerConfig{
		Stream: streamName, Group: group, Consumer: prefix + "-A", Metrics: mA,
	})
	envB := newEnv(t, ConsumerConfig{
		Stream: streamName, Group: group, Consumer: prefix + "-B", Metrics: mB,
	})

	var runs atomic.Int64
	handler := func(ctx context.Context, _ string, _ map[string]any) error {
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
		runs.Add(1)
		if !p.MarkComplete("fn/h", claim) {
			return errors.New("mark complete refused")
		}
		return nil
	}
	envA.start(handler)
	envB.start(handler)

	// When the shared group is destroyed while both consume loops are live.
	if err := envA.client.XGroupDestroy(context.Background(), streamName, group).Err(); err != nil {
		t.Fatalf("xgroup destroy: %v", err)
	}
	id := envA.xadd(t, `{"a":1}`)

	// Then the group comes back (one creator, one benign BUSYGROUP loser) and
	// the event is claimed and executed exactly once across both consumers,
	// then acknowledged.
	testutil.WaitFor(t, 10*time.Second, "group recovered and event executed once", func() bool {
		if !hasGroup(t, envA.client, streamName, group) {
			return false
		}
		if runs.Load() != 1 {
			return false
		}
		_, stillPending := envA.pending()[id]
		return !stillPending
	})
	// A late duplicate would re-run after the first completion was recorded;
	// hold the single-execution invariant while both loops keep running.
	waitSustained(t, "exactly one execution across both consumers", 600*time.Millisecond, func() bool {
		return runs.Load() == 1
	})

	if total := recoveryCounter(mA, metrics.GroupRecoveryCreated) +
		recoveryCounter(mA, metrics.GroupRecoveryExisting) +
		recoveryCounter(mB, metrics.GroupRecoveryCreated) +
		recoveryCounter(mB, metrics.GroupRecoveryExisting); total < 1 {
		t.Fatalf("group recoveries across both consumers = %d, want >= 1", total)
	}
	if failed := recoveryCounter(mA, metrics.GroupRecoveryFailed) +
		recoveryCounter(mB, metrics.GroupRecoveryFailed); failed != 0 {
		t.Fatalf("group recovery failures = %d, want 0 (BUSYGROUP is benign, not a failure)", failed)
	}
	testutil.WaitFor(t, 5*time.Second, "both consumers healthy after recovery", func() bool {
		return envA.consumer.Healthy() && envB.consumer.Healthy()
	})
	envA.stop(t)
	envB.stop(t)
}

// TestIntegrationGroupRecreationReplaysPendingAndDedupesCompleted pins the
// ID-0 replay semantics of a recreated group against the invocation-state
// authority: a delivery left pending by a failed attempt replays and completes,
// while an already-completed invocation's retained state dedupes its replay —
// the handler does not run a second time and the message simply acks again.
func TestIntegrationGroupRecreationReplaysPendingAndDedupesCompleted(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	ctx := context.Background()

	// Given one delivery left pending by a failed attempt...
	pendingID := e.xadd(t, `{"a":1}`)
	failedMsg := e.readOneIntoPEL(t)
	failedCalls := 0
	failFirstThenSucceed := func(context.Context, string, map[string]any) error {
		failedCalls++
		if failedCalls == 1 {
			return errors.New("transient attempt failure")
		}
		return nil
	}
	e.consumer.processMessage(ctx, failedMsg, 1, failFirstThenSucceed)
	if _, ok := e.pending()[pendingID]; !ok {
		t.Fatalf("message %s must stay pending after the failed attempt", pendingID)
	}

	// ...and one delivery acknowledged with its completed state retained.
	ackedID := e.xadd(t, `{"b":2}`)
	ackedMsg := e.readOneIntoPEL(t)
	ackedRuns := 0
	completeOnce := func(ctx context.Context, _ string, _ map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		if p.IsComplete("fn/h") {
			return nil
		}
		started, claim, _, err := p.TryStart("fn/h", time.Hour)
		if err != nil {
			return err
		}
		if !started {
			return ErrInvocationNotEligible
		}
		ackedRuns++
		p.MarkComplete("fn/h", claim)
		return nil
	}
	e.consumer.processMessage(ctx, ackedMsg, 1, completeOnce)
	if ackedRuns != 1 {
		t.Fatalf("acked event executions = %d, want 1", ackedRuns)
	}
	e.waitGone(t, ackedID)

	// When the group is destroyed and recreated at ID 0 (external actor).
	if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
		t.Fatalf("xgroup destroy: %v", err)
	}
	if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
		t.Fatalf("recreate group: %v", err)
	}

	// Then both entries replay as fresh unread deliveries.
	replayed := readUnreadIntoPEL(t, e.client, e.stream, e.group, e.consumer.consumer)
	failedReplay, ok := replayed[pendingID]
	if !ok {
		t.Fatalf("pending entry %s must replay after recreation; got %v", pendingID, replayed)
	}
	ackedReplay, ok := replayed[ackedID]
	if !ok {
		t.Fatalf("retained entry %s must replay after recreation; got %v", ackedID, replayed)
	}

	// The previously pending delivery runs again (its attempt never completed)
	// and is acknowledged this time.
	e.consumer.processMessage(ctx, failedReplay, 2, failFirstThenSucceed)
	if _, ok := e.pending()[pendingID]; ok {
		t.Fatalf("message %s must be acknowledged after the successful replay", pendingID)
	}

	// The previously completed delivery is deduped by its retained state: no
	// second execution, then acknowledged again.
	e.consumer.processMessage(ctx, ackedReplay, 2, completeOnce)
	if ackedRuns != 1 {
		t.Fatalf("completed event executions = %d, want 1 (retained state must dedupe the replay)", ackedRuns)
	}
	if _, ok := e.pending()[ackedID]; ok {
		t.Fatalf("message %s must be acknowledged after the deduped replay", ackedID)
	}
	if rows := e.dlqFor(pendingID); len(rows) != 0 {
		t.Fatalf("DLQ rows = %d, want 0 (a replayed failure must not dead-letter)", len(rows))
	}
}

// TestIntegrationExpiredTerminalStateReplaysAfterGroupRecreation pins the
// other half of the dedupe contract: terminal invocation state exists only for
// the configured retention window. Once it expires, a replayed entry runs the
// handler again instead of being skipped — expiry must not permanently
// suppress a redelivered message, nor pretend it completed.
func TestIntegrationExpiredTerminalStateReplaysAfterGroupRecreation(t *testing.T) {
	testutil.RequireRedis(t)
	retention := 400 * time.Millisecond
	e := newEnv(t, ConsumerConfig{InvocationRetention: &retention})
	ctx := context.Background()

	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)
	msg := e.readOneIntoPEL(t)

	runs := 0
	complete := func(ctx context.Context, _ string, _ map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		if p.IsComplete("fn/h") {
			return nil
		}
		started, claim, _, err := p.TryStart("fn/h", time.Hour)
		if err != nil {
			return err
		}
		if !started {
			return ErrInvocationNotEligible
		}
		runs++
		p.MarkComplete("fn/h", claim)
		return nil
	}

	// Given an acknowledged message whose terminal state is retained with the
	// short configured window.
	e.consumer.processMessage(ctx, msg, 1, complete)
	if runs != 1 {
		t.Fatalf("executions = %d, want 1", runs)
	}
	e.waitGone(t, id)
	e.waitRetained(t, key)

	// When the terminal state expires...
	testutil.WaitFor(t, 8*time.Second, "terminal state expired", func() bool {
		n, err := e.client.Exists(ctx, key).Result()
		return err == nil && n == 0
	})

	// ...and the group is recreated so the entry replays, the expired state no
	// longer dedupes: the handler runs a second time and the message acks.
	if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
		t.Fatalf("xgroup destroy: %v", err)
	}
	if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
		t.Fatalf("recreate group: %v", err)
	}
	replayed := readUnreadIntoPEL(t, e.client, e.stream, e.group, e.consumer.consumer)
	replay, ok := replayed[id]
	if !ok {
		t.Fatalf("entry %s must replay after recreation; got %v", id, replayed)
	}
	e.consumer.processMessage(ctx, replay, 2, complete)
	if runs != 2 {
		t.Fatalf("executions = %d, want 2 (expired terminal state must allow a genuine replay)", runs)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s must be acknowledged after the replayed run", id)
	}
}

// TestIntegrationPartialMultiHandlerStateSurvivesGroupRecreation pins the F-03
// state-preservation contract across a group loss: a message with one
// completed handler, one waiting out its retry backoff, and one still in
// flight keeps every persisted marker byte-identical through the recreation
// (no DLQ, no ACK), and once both protected deadlines elapse the redelivery
// carries each persisted attempt forward instead of resetting it.
func TestIntegrationPartialMultiHandlerStateSurvivesGroupRecreation(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	ctx := context.Background()

	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)
	p := NewInvocationState(ctx, e.consumer.invStateStore, e.stream, e.group, id, e.consumer.log)

	// Given fn/a completed, fn/b waiting out a retry backoff, and fn/c in
	// flight — each claim persisted with attempt 1.
	startedA, claimA, _, err := p.TryStart("fn/a", time.Hour)
	if err != nil || !startedA {
		t.Fatalf("TryStart fn/a = (%v,%v), want a confirmed claim", startedA, err)
	}
	if !p.MarkComplete("fn/a", claimA) {
		t.Fatal("MarkComplete fn/a refused")
	}
	startedB, claimB, _, err := p.TryStart("fn/b", time.Hour)
	if err != nil || !startedB {
		t.Fatalf("TryStart fn/b = (%v,%v), want a confirmed claim", startedB, err)
	}
	if !p.RecordFailure("fn/b", claimB, 30*time.Minute) {
		t.Fatal("RecordFailure fn/b refused")
	}
	startedC, _, _, err := p.TryStart("fn/c", time.Hour)
	if err != nil || !startedC {
		t.Fatalf("TryStart fn/c = (%v,%v), want a confirmed claim (in flight)", startedC, err)
	}
	snapshotValues := func() []string {
		t.Helper()
		raw, err := e.client.HMGet(ctx, key, "fn/a", "fn/b", "fn/c").Result()
		if err != nil {
			t.Fatalf("hmget: %v", err)
		}
		out := make([]string, len(raw))
		for i, v := range raw {
			s, _ := v.(string)
			out[i] = s
		}
		return out
	}
	before := snapshotValues()

	// When the group is destroyed and recreated while those states are live.
	if err := e.client.XGroupDestroy(ctx, e.stream, e.group).Err(); err != nil {
		t.Fatalf("xgroup destroy: %v", err)
	}
	if err := EnsureGroup(ctx, e.client, e.stream, e.group); err != nil {
		t.Fatalf("recreate group: %v", err)
	}
	replayed := readUnreadIntoPEL(t, e.client, e.stream, e.group, e.consumer.consumer)
	replay, ok := replayed[id]
	if !ok {
		t.Fatalf("entry %s must replay after recreation", id)
	}

	// Then the redelivery observes every marker unchanged: the completed
	// handler is skipped, and both the retry backoff and the in-flight
	// deadline refuse a new claim, so the message stays pending.
	protected := func(ctx context.Context, _ string, _ map[string]any) error {
		q, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		if !q.IsComplete("fn/a") {
			return errors.New("fn/a must still be complete after the recreation")
		}
		for _, inv := range []string{"fn/b", "fn/c"} {
			started, _, wait, err := q.TryStart(inv, time.Hour)
			if err != nil {
				return err
			}
			if started || wait <= 0 {
				return fmt.Errorf("%s must stay protected (started=%v wait=%s)", inv, started, wait)
			}
		}
		return ErrInvocationNotEligible
	}
	e.consumer.processMessage(ctx, replay, 2, protected)
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending while handlers are protected", id)
	}
	if after := snapshotValues(); !equalStrings(before, after) {
		t.Fatalf("invocation markers changed across the recreation:\nbefore=%v\nafter=%v", before, after)
	}
	if exists, err := e.client.HExists(ctx, key, terminalField).Result(); err != nil || exists {
		t.Fatalf("terminal marker = (%v,%v), want absent (nothing may resolve while protected)", exists, err)
	}
	if rows := e.dlqFor(id); len(rows) != 0 {
		t.Fatalf("DLQ rows = %d, want 0", len(rows))
	}

	// When both protected deadlines elapse (markers rewritten with elapsed
	// deadlines, keeping attempt 1 and the claim tokens)...
	elapsedMarker := func(v string) string {
		parts := strings.Split(v, ":")
		if len(parts) != 4 {
			t.Fatalf("unexpected marker %q", v)
		}
		parts[1] = strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
		return strings.Join(parts, ":")
	}
	after := snapshotValues()
	if err := e.client.HSet(ctx, key,
		"fn/b", elapsedMarker(after[1]),
		"fn/c", elapsedMarker(after[2]),
	).Err(); err != nil {
		t.Fatalf("rewrite deadlines: %v", err)
	}

	// ...the redelivery claims attempt 2 for each (the persisted attempt is
	// carried forward, never reset), completes all handlers, and the message
	// is acknowledged with terminal retention.
	redelivered := e.readPendingForEnv(t, id)
	attempts := map[string]int{}
	completing := func(ctx context.Context, _ string, _ map[string]any) error {
		q, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return errors.New("no invocation state in ctx")
		}
		if !q.IsComplete("fn/a") {
			return errors.New("fn/a must still be complete")
		}
		for _, inv := range []string{"fn/b", "fn/c"} {
			started, claim, _, err := q.TryStart(inv, time.Hour)
			if err != nil {
				return err
			}
			if !started {
				return fmt.Errorf("%s must become eligible after its deadline elapsed", inv)
			}
			attempts[inv] = claim.Attempt
			if !q.MarkComplete(inv, claim) {
				return fmt.Errorf("MarkComplete %s refused", inv)
			}
		}
		return nil
	}
	e.consumer.processMessage(ctx, redelivered, 3, completing)
	if attempts["fn/b"] != 2 || attempts["fn/c"] != 2 {
		t.Fatalf("attempts = fn/b %d fn/c %d, want 2 and 2 (persisted attempts must carry forward)",
			attempts["fn/b"], attempts["fn/c"])
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s must be acknowledged once every handler is complete", id)
	}
	e.waitRetained(t, key)
}

// equalStrings compares two string slices element-wise.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

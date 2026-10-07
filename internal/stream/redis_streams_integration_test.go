//go:build integration

// This file exercises the Redis Streams boundary end to end against a real
// Redis server: consumer groups, the PEL, XAUTOCLAIM recovery, invocation
// state, retention, and reconnect resilience.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES Redis at REDIS_TEST_ADDR
// (default localhost:6379, matching compose.dev.yaml); a missing dependency fails
// the affected tests rather than skipping them. Start the documented dev
// dependencies with `docker compose -f compose.dev.yaml up -d`.
//
// One test, TestIntegrationReconnectAndResume, additionally boots its own
// disposable Redis in a Docker container so it can simulate an outage; it
// REQUIRES Docker (via a local requireDocker) because only that lets it start
// its own Redis. It is kept here because it is a Redis-boundary test — the
// container is merely how it starts its own Redis.
package stream

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// Provisions a Consumer with a unique stream/group/consumer so tests are
// isolated from each other and from any running dev relay. Recovery config
// (MinIdle/Interval) is small so tests control timing without waiting a minute.
type testEnv struct {
	client   *redis.Client
	consumer *Consumer
	stream   string
	group    string
	ctx      context.Context
	cancel   func()
	done     chan struct{} // closed when Consume returns
	errCh    chan error
}

func newEnv(t *testing.T, cfg ConsumerConfig) *testEnv {
	t.Helper()

	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	redisOpts, _ := config.RedisOptions(addr)
	cli := redis.NewClient(redisOpts)
	ctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	if cfg.Stream == "" {
		cfg.Stream = prefix + "-stream"
	}
	if cfg.Group == "" {
		cfg.Group = prefix + "-group"
	}
	if cfg.Consumer == "" {
		cfg.Consumer = prefix + "-consumer"
	}
	cfg.Client = cli
	// Respect a caller-provided logger (e.g. a testutil.SyncBuffer-backed one) so a test
	// can assert on consumer logs; otherwise discard them.
	if cfg.Log == nil {
		cfg.Log = testutil.DiscardLogger()
	}
	// Fast recovery defaults for deterministic tests. A small Block keeps shutdown
	// prompt: go-redis XReadGroup BLOCK is not interrupted by ctx cancellation and
	// waits out the block duration before returning.
	if cfg.Block == 0 {
		cfg.Block = 300 * time.Millisecond
	}
	if cfg.MinPendingIdle == 0 {
		cfg.MinPendingIdle = 300 * time.Millisecond
	}
	if cfg.ReclaimInterval == 0 {
		cfg.ReclaimInterval = 200 * time.Millisecond
	}
	c := NewConsumer(cfg)
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		// Use a fresh context: cancel() above invalidates ctx, but Redis calls
		// still need a live context to run the cleanup DELs and scans.
		cleanupCtx := context.Background()
		_ = cli.Del(cleanupCtx, c.stream, c.dlqStream).Err()
		// Also clear any invocation-state keys this env's stream/group created,
		// otherwise a message claimed on a failure path can leak a
		// `relay:invocation:<stream>:<group>:*` hash in Redis. The scan pattern
		// is bounded to the unique stream/group names this test owns — never a
		// global FLUSHDB/FLUSHALL.
		cleanupInvocationKeys(cleanupCtx, cli, prefix)
		_ = cli.Close()
	})
	return &testEnv{
		client:   cli,
		consumer: c,
		stream:   cfg.Stream,
		group:    cfg.Group,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (e *testEnv) start(handler Handler) {
	e.done = make(chan struct{})
	e.errCh = make(chan error, 1)
	go func() {
		defer close(e.done)
		e.errCh <- e.consumer.Consume(e.ctx, handler)
	}()
}

// stop cancels the context and asserts Consume (and the recovery goroutine it
// owns) exits cleanly with no error.
func (e *testEnv) stop(t *testing.T) {
	t.Helper()
	e.cancel()
	select {
	case <-e.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop on cancellation")
	}
	if err := <-e.errCh; err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
}

func (e *testEnv) xadd(t *testing.T, event string) string {
	t.Helper()
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"event": event},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	return id
}

// pending returns a map of pending message ID -> retry count for the env group.
func (e *testEnv) pending() map[string]int64 {
	entries, err := e.client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: e.stream,
		Group:  e.group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return map[string]int64{}
	}
	out := map[string]int64{}
	for _, pe := range entries {
		out[pe.ID] = pe.RetryCount
	}
	return out
}

// dlq reads all DLQ entries, keyed by original_id.
func (e *testEnv) dlq() map[string]redis.XMessage {
	msgs, err := e.client.XRange(context.Background(), e.consumer.dlqStream, "-", "+").Result()
	if err != nil {
		return nil
	}
	out := map[string]redis.XMessage{}
	for _, m := range msgs {
		if id, ok := m.Values["original_id"].(string); ok {
			out[id] = m
		}
	}
	return out
}

// dlqFor returns every DLQ entry for the given original_id, in stream order. It
// is the per-invocation counterpart of dlq (which collapses to one entry per
// original_id): a message can produce several entries, one per exhausted
// invocation.
func (e *testEnv) dlqFor(id string) []redis.XMessage {
	msgs, err := e.client.XRange(context.Background(), e.consumer.dlqStream, "-", "+").Result()
	if err != nil {
		return nil
	}
	var out []redis.XMessage
	for _, m := range msgs {
		if oid, ok := m.Values["original_id"].(string); ok && oid == id {
			out = append(out, m)
		}
	}
	return out
}

// cleanupInvocationKeys scans for and deletes any `relay:invocation:*` hash
// keys that belong to this test's ownership prefix (a unixnano-unique
// stream/group prefix). The scan MATCH pattern is bounded to keys whose
// percent-encoded prefix matches the unique names this env created, so it never
// touches other tests' or the dev relay's state — never a global FLUSHDB.
func cleanupInvocationKeys(ctx context.Context, cli *redis.Client, prefix string) {
	match := "relay:invocation:*" + encodeComponent(prefix) + "*"
	var cursor uint64
	for {
		keys, next, err := cli.Scan(ctx, cursor, match, 0).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = cli.Del(ctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

// waitDelivered waits until the message is present in the PEL (was read by a
// consumer but not yet acked). Used to avoid treating a never-delivered message
// as "acked".
func (e *testEnv) waitDelivered(t *testing.T, id string) {
	t.Helper()
	testutil.WaitFor(t, 8*time.Second, "message "+id+" delivered into PEL", func() bool {
		_, ok := e.pending()[id]
		return ok
	})
}

// waitGone waits until the message is absent from the PEL.
func (e *testEnv) waitGone(t *testing.T, id string) {
	t.Helper()
	testutil.WaitFor(t, 8*time.Second, "message "+id+" gone from PEL", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
}

// waitRetained waits until the invocation-state key is TERMINAL-RETAINED: still
// present, carrying the reserved terminal marker, and holding a positive
// retention TTL. This is the post-ACK contract that replaced the old eager
// clear: once the message left the PEL its state is kept for a bounded retention
// window (and guarded against stale writes) instead of being deleted.
func (e *testEnv) waitRetained(t *testing.T, key string) {
	t.Helper()
	testutil.WaitFor(t, 8*time.Second, "invocation state terminal-retained", func() bool {
		v, err := e.client.HGet(context.Background(), key, terminalField).Result()
		if err != nil || v != "1" {
			return false
		}
		d, err := e.client.PTTL(context.Background(), key).Result()
		return err == nil && d > 0
	})
}

func TestIntegrationHandlerFailureStaysPending(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	delivered := make(chan struct{}, 1)
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			select {
			case delivered <- struct{}{}:
			default:
			}
		}
		return fmt.Errorf("nope")
	})
	<-delivered
	// After a handler failure the message must stay pending: poll that it
	// remains in the PEL across the reclaim grace window, rather than a fixed
	// sleep.
	waitSustained(t, "message pending after handler failure", 500*time.Millisecond, func() bool {
		_, ok := e.pending()[id]
		return ok
	})
	e.stop(t)
}

func TestIntegrationReclaimAfterIdleRetrySuccess(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	var attempts atomic.Int64
	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		n := attempts.Add(1)
		if n >= 2 {
			close(acked)
			return nil
		}
		return fmt.Errorf("fail first delivery")
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected at least 2 delivery attempts, got %d", got)
	}
}

// TestIntegrationExhaustRetriesRoutesToDLQ verifies that a handler returning
// ErrInvocationExhausted (the runner's per-invocation exhaustion signal) routes
// the whole message to the DLQ. The handler simulates the runner: it claims the
// invocation, and on the final attempt marks it exhausted and returns
// ErrInvocationExhausted.
func TestIntegrationExhaustRetriesRoutesToDLQ(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	// The default DLQ stream is a Relay-owned, `relay:`-prefixed key, never the
	// user-owned source stream name.
	want := DLQStreamFor(e.stream)
	if got := e.consumer.dlqStream; got != want {
		t.Fatalf("dlqStream = %q, want %q", got, want)
	}
	id := e.xadd(t, `{"a":1}`)
	var attempts atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			return ErrInvocationNotEligible
		}
		attempts.Store(int64(claim.Attempt))
		// Simulate the runner's exhaustion: mark the invocation terminal and
		// return the typed HandlerExhaustedError (which wraps
		// ErrInvocationExhausted and carries the exhausted handler attempt) so the
		// message routes to the DLQ with handler metadata attributed from the
		// handler retry state.
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	})
	// The message is routed to the DLQ (and acked) on the first delivery, so it
	// may never linger in the PEL; wait for the DLQ entry instead.
	testutil.WaitFor(t, 8*time.Second, "message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for %s, got %d entries", id, len(e.dlq()))
	}
	// go-redis decodes stream field values as strings (stringInterfaceMapParser).
	if m.Values["original_stream"] != e.stream {
		t.Errorf("original_stream = %v", m.Values["original_stream"])
	}
	if m.Values["original_id"] != id {
		t.Errorf("original_id = %v", m.Values["original_id"])
	}
	if m.Values["event"] != `{"a":1}` {
		t.Errorf("event = %v", m.Values["event"])
	}
	// The entry attributes the exact exhausted invocation.
	if m.Values["app"] != "fn" || m.Values["handler"] != "h" {
		t.Errorf("function/handler = %v/%v, want fn/h", m.Values["app"], m.Values["handler"])
	}
	if m.Values["reason"] == "" {
		t.Errorf("reason missing")
	}
	// The exhaustion was attributed from the handler retry state: the first
	// delivery claimed handler attempt 1, so both deliveries and handler_attempts
	// are 1 here.
	if m.Values["handler_attempts"] != "1" {
		t.Errorf("handler_attempts = %v, want 1 (from the invocation retry state)", m.Values["handler_attempts"])
	}
	if m.Values["deliveries"] != "1" {
		t.Errorf("deliveries = %v, want 1 (first delivery)", m.Values["deliveries"])
	}
}

// TestIntegrationDLQEntryCarriesFinalAttemptTrace proves the durable DLQ trace
// path end to end against real Redis: the runner's RecordTrace persists the
// compact lineage in the invocation-state hash, and routeToDLQ reads it back
// (via dlqTraceFor) into the entry's optional `trace` field. Only the compact
// traceparent[|tracestate] form is stored; baggage is never persisted.
func TestIntegrationDLQEntryCarriesFinalAttemptTrace(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)

	const lineage = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01|vendor=x"
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			return ErrInvocationNotEligible
		}
		// The runner records the attempt's span lineage immediately; the DLQ
		// write reads it back for the final failed attempt.
		p.RecordTrace("fn/h", lineage)
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	})
	testutil.WaitFor(t, 8*time.Second, "message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	e.stop(t)

	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for %s", id)
	}
	if got := m.Values["trace"]; got != lineage {
		t.Fatalf("DLQ entry trace = %v, want %q", got, lineage)
	}
}

// TestIntegrationInvocationTraceLineageSurvivesRestart proves the persisted
// lineage outlives a consumer restart: RecordTrace written by consumer A is read
// back through the store by a fresh consumer handle on the same stream/group, so
// a retry on a restarted worker can link to the previous attempt. The state hash
// is cleared on completion.
func TestIntegrationInvocationTraceLineageSurvivesRestart(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("trace-restart-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"

	envA := newEnv(t, ConsumerConfig{Stream: stream, Group: group, Consumer: "trace-A"})
	id := envA.xadd(t, `{"a":1}`)
	key := invocationStateKey(stream, group, id)
	const lineage = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	delivered := make(chan struct{})
	var once sync.Once
	envA.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		p.RecordTrace("fn/h", lineage)
		once.Do(func() { close(delivered) })
		return fmt.Errorf("leave pending to persist lineage")
	})
	<-delivered
	testutil.WaitFor(t, 8*time.Second, "trace lineage persisted in Redis", func() bool {
		v, err := envA.client.HGet(context.Background(), key, traceField("fn/h")).Result()
		return err == nil && v == lineage
	})
	envA.stop(t)

	// Consumer B (a fresh handle on the same stream/group) reads the lineage back
	// through the store, simulating a restarted worker linking its retry. B never
	// starts Consume: only the store read-back is under test, so cleanup owns its
	// client teardown.
	envB := newEnv(t, ConsumerConfig{Stream: stream, Group: group, Consumer: "trace-B"})
	state := NewInvocationState(context.Background(), envB.consumer.invStateStore, stream, group, id, envB.consumer.log)
	if got := state.TraceReference("fn/h"); got != lineage {
		t.Fatalf("restarted handle TraceReference = %q, want %q", got, lineage)
	}
}

// TestIntegrationDLQDeliveriesExceedHandlerAttempts proves the semantic split:
// `deliveries` is the authoritative Redis Stream/PEL delivery count (including
// reclaims that skipped a protected invocation), while `handler_attempts` is the
// handler execution/retry count that exhausted. A message can therefore be
// DLQ'd with deliveries > handler_attempts.
func TestIntegrationDLQDeliveriesExceedHandlerAttempts(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, claim, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			// Protected: the prior failed attempt is waiting out its (long)
			// backoff. Stay pending so reclaim keeps redelivering the message,
			// growing `deliveries` without advancing the handler attempt.
			return ErrInvocationNotEligible
		}
		if claim.Attempt < 2 {
			// Attempt 1: retryable failure with a long backoff, so subsequent
			// reclaims are skipped as protected.
			p.RecordFailure("fn/h", claim, time.Hour)
			return fmt.Errorf("retryable failure")
		}
		// Attempt 2: exhaust (retries:1 → maxAttempts=2).
		p.MarkExhausted("fn/h", claim)
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted},
		}}
	})

	// Let reclaim redeliver the protected message until the PEL retry count is
	// comfortably above the handler attempt count (still 1 at this point).
	testutil.WaitFor(t, 8*time.Second, "message reclaimed past the handler attempt count", func() bool {
		return e.pending()[id] >= 3
	})
	// Put the retry backoff's deadline in the past by rewriting the marker with
	// the persisted attempt count (1), so the next delivery carries the attempt
	// forward and executes handler attempt 2, which exhausts.
	if err := e.client.HSet(context.Background(), key, "fn/h", nextAttemptValue(time.Now().Add(-time.Hour), InvocationClaim{Attempt: 1, Token: "aabbccdd"})).Err(); err != nil {
		t.Fatalf("rewrite retry marker with elapsed deadline: %v", err)
	}

	testutil.WaitFor(t, 8*time.Second, "message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	e.stop(t)

	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for %s", id)
	}
	// handler_attempts is the handler execution count at exhaustion (2).
	if m.Values["handler_attempts"] != "2" {
		t.Errorf("handler_attempts = %v, want 2 (from the invocation retry state)", m.Values["handler_attempts"])
	}
	// deliveries is the authoritative PEL count, strictly greater than the
	// handler attempt count because protected reclaims were redelivered without
	// executing a handler attempt.
	deliveries, err := strconv.Atoi(m.Values["deliveries"].(string))
	if err != nil {
		t.Fatalf("deliveries = %v, not an int: %v", m.Values["deliveries"], err)
	}
	if deliveries <= 2 {
		t.Errorf("deliveries = %d, want > handler_attempts (2): protected reclaims must count as deliveries", deliveries)
	}
	if m.Values["reason"] == "" {
		t.Errorf("reason missing")
	}
}

func TestIntegrationMalformedEventRoutesToDLQImmediately(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{not json`)
	var handlerRan atomic.Bool
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		handlerRan.Store(true) // must not be reached (malformed routes to DLQ pre-handler)
		return nil
	})
	// The malformed message is routed straight to the DLQ (and acked) so it may
	// never appear in the PEL; wait for the DLQ entry instead.
	testutil.WaitFor(t, 8*time.Second, "malformed message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	testutil.WaitFor(t, 8*time.Second, "malformed message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if handlerRan.Load() {
		t.Fatalf("handler should not be invoked for malformed message")
	}
	m, ok := e.dlq()[id]
	if !ok {
		t.Fatalf("expected DLQ entry for malformed message %s", id)
	}
	// A malformed message is routed straight to the DLQ before any handler runs,
	// so it has NO handler retry state to attribute: handler_attempts is an
	// explicit 0, never fabricated from the delivery count. deliveries is the
	// authoritative PEL count (1 on first delivery).
	if m.Values["handler_attempts"] != "0" {
		t.Errorf("malformed message has no handler retry state, want handler_attempts==0, got %v", m.Values["handler_attempts"])
	}
	// No invocation to attribute, so app/handler are the "-" placeholder.
	if m.Values["app"] != dlqNoHandler || m.Values["handler"] != dlqNoHandler {
		t.Errorf("malformed message function/handler = %v/%v, want %q placeholder",
			m.Values["app"], m.Values["handler"], dlqNoHandler)
	}
	// The malformed payload is preserved verbatim on the DLQ entry.
	if m.Values["event"] != `{not json` {
		t.Errorf("malformed message event = %v, want the original raw payload", m.Values["event"])
	}
	if m.Values["deliveries"] != "1" {
		t.Errorf("malformed message should go straight to DLQ with deliveries==1, got %v", m.Values["deliveries"])
	}
}

func TestIntegrationNoMatchAcked(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"x":1}`)
	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(acked)
		}
		return nil // no-match success acks
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

func TestIntegrationStateRetainedOnFailureClearedOnAck(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// First delivery: mark the invocation succeeded but return an error so the
	// message stays pending. The state must be retained for the redelivery.
	var first atomic.Bool
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if p, ok := InvocationStateFrom(ctx); ok {
			started, claim, _, _ := p.TryStart("fn/h", time.Hour)
			if started {
				p.MarkComplete("fn/h", claim)
			}
		}
		if !first.Swap(true) {
			return fmt.Errorf("fail first delivery")
		}
		return nil
	})
	e.waitDelivered(t, id)
	testutil.WaitFor(t, 8*time.Second, "invocation state retained after failed delivery", func() bool {
		v, err := e.client.HGet(context.Background(), key, "fn/h").Result()
		return err == nil && v == "ok"
	})

	// The recovery loop reclaims the idle message; the handler sees the invocation
	// already done and returns nil, so the message is acked and its state is
	// switched to terminal retention (never deleted).
	testutil.WaitFor(t, 8*time.Second, "message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.waitRetained(t, key)
	e.stop(t)
}

// TestIntegrationStateRetainedOnDLQ pins that, after DLQ routing, the
// invocation-state hash is switched to terminal retention (marker + TTL) rather
// than deleted, so a stale in-memory delivery can neither mutate nor resurrect
// it.
func TestIntegrationStateRetainedOnDLQ(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// The handler returns ErrInvocationExhausted to route the message to the
	// DLQ. Its state must be terminal-retained after the DLQ write + ACK.
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			if p, ok := InvocationStateFrom(ctx); ok {
				if started, claim, _, _ := p.TryStart("fn/h", time.Hour); started {
					p.MarkExhausted("fn/h", claim)
				}
			}
		}
		return ErrInvocationExhausted
	})
	e.waitGone(t, id)
	testutil.WaitFor(t, 8*time.Second, "message in DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	e.waitRetained(t, key)
	e.stop(t)
}

// TestIntegrationProcessMessageRetainsStateOnlyAfterAck drives the shared
// processMessage path directly (no Consume loop) against a real Redis: a
// message is XADDed and read into the group's PEL, then processMessage runs a
// succeeding handler. The invocation-state key must be switched to terminal
// retention only AFTER the ACK, and the message must be gone from the PEL.
func TestIntegrationProcessMessageRetainsStateOnlyAfterAck(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)

	// Read the message into the group's PEL so XAck has an entry to remove.
	msgs, err := e.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Streams:  []string{e.stream, ">"},
		Count:    1,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Messages) != 1 {
		t.Fatalf("expected one message in PEL, got %+v", msgs)
	}
	msg := msgs[0].Messages[0]
	if msg.ID != id {
		t.Fatalf("read message %s, want %s", msg.ID, id)
	}

	// Mark invocation state, then process with a succeeding handler: the message
	// is acked and the invocation-state key switched to terminal retention.
	key := invocationStateKey(e.stream, e.group, id)
	if err := e.client.HSet(context.Background(), key, "fn/h", "ok").Err(); err != nil {
		t.Fatalf("hset invocation state: %v", err)
	}
	e.consumer.processMessage(context.Background(), msg, 1,
		func(ctx context.Context, msgID string, ev map[string]any) error {
			return nil
		})

	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked (gone from PEL)", id)
	}
	e.waitRetained(t, key)
}

// retainCountingStore decorates an invocationStateStore, recording how many
// times retainTerminal is invoked while delegating every method to the wrapped
// store. Because retainTerminal's only real effect can be observed through Redis
// (which this test deliberately makes unreachable for the ACK), the count is
// what proves the ordering contract directly: the failed-ACK path must never
// even attempt terminal retention.
type retainCountingStore struct {
	invocationStateStore
	retainCalls atomic.Int64
}

func (s *retainCountingStore) retainTerminal(ctx context.Context, stream, group, msgID string, retention time.Duration) error {
	s.retainCalls.Add(1)
	return s.invocationStateStore.retainTerminal(ctx, stream, group, msgID, retention)
}

// TestIntegrationSuccessfulHandlerXACKFailureLeavesPendingAndRecoverable pins
// the failed-ACK half of the post-success contract against real Redis: the
// handler's invocation-state mutations SUCCEED (TryStart then MarkComplete, the
// completed marker the redelivery must observe), but the XACK fails, so the
// message must stay in the PEL and its hash must remain RECOVERABLE — present,
// still carrying the completed marker, PERSISTENT (PTTL == -1), and with NO
// terminal marker. retainTerminal must NOT be attempted at all: it is only ever
// called after a successful XACK, and running it here would apply the retention
// TTL to state a redelivery still needs to skip the completed handler.
//
// The ACK failure is made deterministic by closing the consumer's Redis client
// INSIDE the handler, after the state writes and before returning nil, so the
// XAck that follows fails with redis.ErrClosed. Every post-close assertion goes
// through a separate inspection client, because the consumer's own client is
// closed.
func TestIntegrationSuccessfulHandlerXACKFailureLeavesPendingAndRecoverable(t *testing.T) {
	// A separate client for inspecting Redis after the consumer's client is
	// closed. RequireRedis owns its lifecycle (closed last, after this test's
	// cleanup), so it stays usable for the assertions.
	inspect := testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	// Observe retainTerminal attempts directly: the closed client means a
	// (buggy) retain attempt could not have any Redis effect, so the call count
	// is the assertion that survives the outage.
	store := &retainCountingStore{invocationStateStore: e.consumer.invStateStore}
	e.consumer.invStateStore = store

	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)
	// newEnv's cleanup deletes the stream and invocation keys through the
	// consumer's client, which this test closes; delete them through the live
	// inspection client instead. Registered after newEnv so it runs first (LIFO),
	// while `inspect` is still open.
	t.Cleanup(func() {
		ctx := context.Background()
		_ = inspect.Del(ctx, key, e.stream, e.consumer.dlqStream).Err()
	})

	// Read the message into the group's PEL so XAck has an entry to remove.
	msg := e.readOneIntoPEL(t)
	if msg.ID != id {
		t.Fatalf("read message %s, want %s", msg.ID, id)
	}

	// The handler mirrors the runner's success path: it claims the invocation and
	// marks it complete (persisting "ok"), then closes the consumer's client so
	// the subsequent XAck cannot run, and returns nil (success).
	closeErr := make(chan error, 1)
	e.consumer.processMessage(context.Background(), msg, 1,
		func(ctx context.Context, msgID string, ev map[string]any) error {
			p, ok := InvocationStateFrom(ctx)
			if !ok {
				t.Errorf("no invocation state in ctx")
				return fmt.Errorf("no invocation state in ctx")
			}
			started, claim, _, err := p.TryStart("fn/h", time.Hour)
			if err != nil || !started {
				t.Errorf("TryStart = (%v,%v), want a started claim", started, err)
				return fmt.Errorf("TryStart failed: %w", err)
			}
			if !p.MarkComplete("fn/h", claim) {
				t.Errorf("MarkComplete = false, want true")
				return fmt.Errorf("MarkComplete refused")
			}
			// Deterministic ACK failure: the state writes above already succeeded,
			// so closing the client here only breaks the XACK that follows.
			closeErr <- e.client.Close()
			return nil
		})
	if err := <-closeErr; err != nil {
		t.Fatalf("close consumer client in handler: %v", err)
	}

	// The failed-ACK path must not even attempt terminal retention.
	if got := store.retainCalls.Load(); got != 0 {
		t.Fatalf("retainTerminal attempted %d times after a failed XACK; want 0", got)
	}

	// The XACK failed, so the message must still be pending. No Consume loop runs
	// in this test, so nothing else can ack or redeliver it.
	entries, err := inspect.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: e.stream, Group: e.group, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		t.Fatalf("xpending after XACK failure: %v", err)
	}
	pending := false
	for _, pe := range entries {
		if pe.ID == id {
			pending = true
		}
	}
	if !pending {
		t.Fatalf("message %s left the PEL despite the failed XACK; want it pending", id)
	}

	// The hash must survive the failed ACK and stay RECOVERABLE: the completed
	// marker is preserved for the redelivery, no terminal marker was written, and
	// the key remains persistent (no TTL).
	if n, err := inspect.Exists(context.Background(), key).Result(); err != nil || n != 1 {
		t.Fatalf("invocation-state key must survive a failed XACK (exists=%d err=%v)", n, err)
	}
	if v, err := inspect.HGet(context.Background(), key, "fn/h").Result(); err != nil || v != "ok" {
		t.Fatalf("invocation marker = %q err=%v, want %q persisted for the redelivery", v, err, "ok")
	}
	if ok, err := inspect.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("retainTerminal must NOT run after a failed XACK: terminal marker present (ok=%v err=%v)", ok, err)
	}
	if d, err := inspect.PTTL(context.Background(), key).Result(); err != nil || d != -1 {
		t.Fatalf("recoverable invocation state must be persistent; pttl=%s err=%v (want -1)", d, err)
	}
}

// TestIntegrationDLQWriteFailureLeavesPendingAndRetainsState merges the two
// former DLQ-write-failure tests and pins the retention-ordering contract through
// the real processMessage path: when the DLQ XADD fails (the DLQ stream name is a
// wrong-type key) after an exhausted invocation, the original message stays
// pending and its invocation-state key remains RECOVERABLE (persistent, no TTL,
// no terminal marker); once the DLQ write succeeds, the message is acked and the
// key is switched to terminal retention.
func TestIntegrationDLQWriteFailureLeavesPendingAndRetainsState(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)

	// Read the message into the PEL so XAck has an entry to remove.
	msgs, err := e.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Streams:  []string{e.stream, ">"},
		Count:    1,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Messages) != 1 {
		t.Fatalf("expected one message in PEL, got %+v", msgs)
	}
	msg := msgs[0].Messages[0]
	key := invocationStateKey(e.stream, e.group, id)
	if err := e.client.HSet(context.Background(), key, "fn/h", "ok").Err(); err != nil {
		t.Fatalf("hset invocation state: %v", err)
	}

	// A handler reporting exhaustion routes the message to the DLQ. Force the
	// DLQ XADD to fail by making the DLQ stream name a wrong-type key.
	if r := e.client.Set(context.Background(), e.consumer.dlqStream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}
	exhausted := func(ctx context.Context, msgID string, ev map[string]any) error {
		return ErrInvocationExhausted
	}
	e.consumer.processMessage(context.Background(), msg, 1, exhausted)

	// DLQ write failed: message stays pending and invocation state stays
	// recoverable (present, persistent, no terminal marker).
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending when DLQ write fails", id)
	}
	if n, err := e.client.Exists(context.Background(), key).Result(); err != nil || n != 1 {
		t.Fatalf("invocation-state key must be retained on DLQ write failure (exists=%d err=%v)", n, err)
	}
	if ok, err := e.client.HExists(context.Background(), key, terminalField).Result(); err != nil || ok {
		t.Fatalf("invocation-state key must NOT be terminal-retained while pending (hexists=%v err=%v)", ok, err)
	}
	if d, err := e.client.PTTL(context.Background(), key).Result(); err != nil || d != -1 {
		t.Fatalf("pending invocation state must be persistent; pttl=%s err=%v", d, err)
	}

	// Now let the DLQ write succeed: delete the wrong-type key and re-process.
	if err := e.client.Del(context.Background(), e.consumer.dlqStream).Err(); err != nil {
		t.Fatalf("del wrong-type key: %v", err)
	}
	e.consumer.processMessage(context.Background(), msg, 1, exhausted)

	// DLQ write + ACK succeeded: message gone from PEL and state terminal-retained.
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after successful DLQ write", id)
	}
	e.waitRetained(t, key)
}

// TestIntegrationReclaimMissingPayloadSurfacedAndCleared pins the recovery-time
// data-loss path against real Redis: a message is delivered into the PEL and then
// its body is removed EXTERNALLY (an XDEL here; an unsafe out-of-band trim in
// production). The reclaim loop must NOT run the handler, must NOT write a DLQ
// entry, and must count the anomaly as missing_payload_total, while the dangling
// PEL reference ends up cleared either by the server's own XAUTOCLAIM purge
// (Redis 7+, surfaced via the deleted-id array) or by Relay's conservative XACK.
//
// The stream node is filled so the reclaim actually walks it; a single recent
// entry keeps the stream alive after the deleted one.
func TestIntegrationReclaimMissingPayloadSurfacedAndCleared(t *testing.T) {
	testutil.RequireRedis(t)
	m := metrics.New()
	e := newEnv(t, ConsumerConfig{Metrics: m})

	// Read one message into the PEL, then delete its body out from under the
	// group (simulating an external unsafe trim).
	id := e.xadd(t, `{"a":1}`)
	e.readOneIntoPEL(t)
	if _, err := e.client.XDel(context.Background(), e.stream, id).Result(); err != nil {
		t.Fatalf("xdel: %v", err)
	}
	// Seed recoverable invocation state for the soon-to-be-body-less message, so
	// the missing-payload cleanup's terminal retention is observable: without it,
	// a persistent (no-TTL) hash would leak forever.
	key := invocationStateKey(e.stream, e.group, id)
	if err := e.client.HSet(context.Background(), key, "fn/h",
		runningValue(time.Now().Add(time.Hour), InvocationClaim{Attempt: 1, Token: "aabbccdd"})).Err(); err != nil {
		t.Fatalf("seed invocation state: %v", err)
	}
	// A second, live entry so the stream is not empty and the reclaim loop has
	// something to scan past.
	liveID := e.xadd(t, `{"live":1}`)
	e.readOneIntoPEL(t)

	var handlerCalls atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		// Only the live entry should ever reach a handler.
		if msgID == id {
			handlerCalls.Add(1)
		}
		return nil
	})

	testutil.WaitFor(t, 8*time.Second, "missing_payload_total recorded", func() bool {
		return m.Counter(metrics.MetricMissingPayload) >= 1
	})
	// The deleted message's dangling PEL reference must clear (either the server
	// purged it during XAUTOCLAIM or Relay XACKed it).
	testutil.WaitFor(t, 8*time.Second, "deleted entry gone from PEL", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	// The live entry is processed and acked normally.
	testutil.WaitFor(t, 8*time.Second, "live entry acked", func() bool {
		_, ok := e.pending()[liveID]
		return !ok
	})
	e.stop(t)

	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler ran %d times for the body-less entry, want 0", got)
	}
	if got := len(e.dlqFor(id)); got != 0 {
		t.Fatalf("DLQ entries for body-less entry = %d, want 0 (never dead-lettered, never fabricated)", got)
	}
	// The body-less message's invocation state must not leak: once its dangling
	// PEL reference is gone its hash is terminal-retained (marker + TTL).
	e.waitRetained(t, key)
}

// readOneIntoPEL reads exactly one message from the env stream into the group's
// PEL and returns it, so a test can drive processMessage directly.
func (e *testEnv) readOneIntoPEL(t *testing.T) redis.XMessage {
	t.Helper()
	msgs, err := e.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    e.group,
		Consumer: e.consumer.consumer,
		Streams:  []string{e.stream, ">"},
		Count:    1,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	if len(msgs) != 1 || len(msgs[0].Messages) != 1 {
		t.Fatalf("expected one message in PEL, got %+v", msgs)
	}
	return msgs[0].Messages[0]
}

// exhaustedTwo is a terminal exhaustion error for two invocations, mirroring the
// runner's aggregate after a message matches two always-failing handlers.
func exhaustedTwo(aFn, aH string, aAttempts int, bFn, bH string, bAttempts int) *HandlerExhaustedError {
	return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
		{App: aFn, Handler: aH, Attempts: aAttempts},
		{App: bFn, Handler: bH, Attempts: bAttempts},
	}}
}

// TestIntegrationMultiInvocationExhaustionWritesPerInvocationEntries pins the
// stream-layer DLQ contract for a message with TWO exhausted handlers: the
// terminal error carries both invocations and routeToDLQ must write one entry
// per invocation, each with its own app/handler and handler_attempts, then
// ACK once. It drives processMessage directly against real Redis.
func TestIntegrationMultiInvocationExhaustionWritesPerInvocationEntries(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)

	e.consumer.processMessage(context.Background(), msg, 1, func(ctx context.Context, msgID string, ev map[string]any) error {
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		// Two independent invocations: each claims attempt 1 and exhausts.
		for _, inv := range []string{"fnA/h", "fnB/h"} {
			started, claim, _, _ := p.TryStart(inv, time.Hour)
			if !started {
				return ErrInvocationNotEligible
			}
			p.MarkExhausted(inv, claim)
		}
		return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{
			{App: "fnA", Handler: "h", Attempts: 1},
			{App: "fnB", Handler: "h", Attempts: 1},
		}}
	})

	entries := e.dlqFor(id)
	if len(entries) != 2 {
		t.Fatalf("DLQ entries = %d, want 2 (one per exhausted invocation)", len(entries))
	}
	byInv := map[string]redis.XMessage{}
	for _, m := range entries {
		fn, _ := m.Values["app"].(string)
		h, _ := m.Values["handler"].(string)
		byInv[fn+"/"+h] = m
	}
	for _, inv := range []string{"fnA/h", "fnB/h"} {
		m, ok := byInv[inv]
		if !ok {
			t.Fatalf("missing DLQ entry for %s (got %v)", inv, byInv)
		}
		if m.Values["handler_attempts"] != "1" {
			t.Errorf("%s handler_attempts = %v, want 1", inv, m.Values["handler_attempts"])
		}
		if m.Values["event"] != `{"a":1}` {
			t.Errorf("%s event = %v, want the original payload preserved", inv, m.Values["event"])
		}
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after both DLQ writes", id)
	}
}

// TestIntegrationPerInvocationDLQPartialWriteResumes pins the no-DLQ-scan
// idempotent retry across a partial multi-entry write. One exhausted invocation
// already has its entry persisted (marker "exhausted:<attempt>:<token>:dlq", DLQ
// entry present); the other is exhausted but unpersisted. A redelivery must
// write ONLY the missing second entry — never duplicate the first — then ACK.
// This is the recovery path after a crash between two XADDs (or an XADD batch
// where one succeeded).
func TestIntegrationPerInvocationDLQPartialWriteResumes(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)
	key := invocationStateKey(e.stream, e.group, id)

	// Simulate the prior partial write: fnA's entry was written and its marker
	// upgraded; fnB exhausted but its entry was never written.
	if _, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.consumer.dlqStream,
		Values: dlqPayload(e.stream, id, e.group, e.consumer.consumer, `{"a":1}`,
			"fnA exhausted", "fnA", "h", 1, 1, ""),
	}).Result(); err != nil {
		t.Fatalf("seed DLQ entry: %v", err)
	}
	if err := e.client.HSet(context.Background(), key,
		"fnA/h", exhaustedValue(InvocationClaim{Attempt: 1, Token: "aa"}, true),
		"fnB/h", exhaustedValue(InvocationClaim{Attempt: 1, Token: "bb"}, false),
	).Err(); err != nil {
		t.Fatalf("seed invocation state: %v", err)
	}

	e.consumer.processMessage(context.Background(), msg, 2, func(ctx context.Context, msgID string, ev map[string]any) error {
		return exhaustedTwo("fnA", "h", 1, "fnB", "h", 1)
	})

	// Only fnB's entry was written: exactly two total, no duplicate of fnA.
	entries := e.dlqFor(id)
	if len(entries) != 2 {
		t.Fatalf("DLQ entries = %d, want 2 (seeded fnA + resumed fnB; no duplicate)", len(entries))
	}
	// The message is acked and state terminal-retained after the resumed write.
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after the resumed write", id)
	}
	e.waitRetained(t, key)
}

// TestIntegrationPerInvocationDLQXACKFailureRetryIsIdempotent pins the retry
// after a successful XADD batch whose XACK failed. Both invocation markers are
// already "exhausted:<attempt>:<token>:dlq" and both DLQ entries exist (the
// state a redelivery sees when the ACK was lost). The redelivery must NOT write
// any new entry (no DLQ scan, no duplicate) and must ACK the message, clearing
// state.
func TestIntegrationPerInvocationDLQXACKFailureRetryIsIdempotent(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	msg := e.readOneIntoPEL(t)
	key := invocationStateKey(e.stream, e.group, id)

	// Both entries were written before the XACK failed; both markers record it.
	for _, fn := range []string{"fnA", "fnB"} {
		if _, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: e.consumer.dlqStream,
			Values: dlqPayload(e.stream, id, e.group, e.consumer.consumer, `{"a":1}`,
				fn+" exhausted", fn, "h", 2, 1, ""),
		}).Result(); err != nil {
			t.Fatalf("seed DLQ entry %s: %v", fn, err)
		}
	}
	if err := e.client.HSet(context.Background(), key,
		"fnA/h", exhaustedValue(InvocationClaim{Attempt: 1, Token: "aa"}, true),
		"fnB/h", exhaustedValue(InvocationClaim{Attempt: 1, Token: "bb"}, true),
	).Err(); err != nil {
		t.Fatalf("seed invocation state: %v", err)
	}

	e.consumer.processMessage(context.Background(), msg, 2, func(ctx context.Context, msgID string, ev map[string]any) error {
		return exhaustedTwo("fnA", "h", 1, "fnB", "h", 1)
	})

	// No new entries: the redelivery skipped both persisted writes.
	if got := len(e.dlqFor(id)); got != 2 {
		t.Fatalf("DLQ entries = %d, want 2 (no duplicates on the XACK-failure retry)", got)
	}
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked on the idempotent retry", id)
	}
	e.waitRetained(t, key)
}

func TestIntegrationRestartResilience(t *testing.T) {
	testutil.RequireRedis(t)
	const event = `{"restart":1}`

	// Both consumers share one stream+group; only the consumer name differs, so
	// the second consumer can reclaim the first's pending message after a "crash".
	prefix := fmt.Sprintf("restart-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"

	// Consumer A reads a message but "crashes" (never acks). It has recovery
	// disabled and a huge min-idle so it does not interfere.
	envA := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "restart-A",
		ReclaimInterval: 0, MinPendingIdle: time.Hour,
	})
	id := envA.xadd(t, event)
	deliveredA := make(chan struct{})
	envA.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(deliveredA)
		}
		// Simulate a crash: consume into the PEL but never ack.
		return fmt.Errorf("crashed before ack")
	})
	<-deliveredA
	testutil.WaitFor(t, 8*time.Second, "message pending under consumer A", func() bool {
		_, ok := envA.pending()[id]
		return ok
	})
	envA.stop(t)

	// Consumer B: a NEW consumer (different name) + recovery reclaims the now-idle
	// pending message and processes it successfully.
	envB := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "restart-B",
	})
	acked := make(chan struct{})
	envB.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if ev["restart"] != float64(1) {
			t.Errorf("unexpected event on redelivery: %v", ev)
			return fmt.Errorf("unexpected event on redelivery: %v", ev)
		}
		close(acked)
		return nil
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message reclaimed and acked by consumer B", func() bool {
		_, ok := envB.pending()[id]
		return !ok
	})
	envB.stop(t)
}

// TestIntegrationReconnectAndResume drives a disposable Redis container through
// an outage: consume an event, stop Redis, observe backoff + unhealthy, start
// Redis again, XADD an event, and assert the handler runs and exactly one
// recovery line is logged.
func TestIntegrationReconnectAndResume(t *testing.T) {
	cli := testutil.RequireDocker(t)

	// Start a disposable redis on a free host port.
	port := testutil.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	create, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{Image: "redis:8-alpine"},
		HostConfig: &container.HostConfig{
			PortBindings: network.PortMap{
				network.MustParsePort("6379/tcp"): []network.PortBinding{{
					HostIP:   netip.MustParseAddr("127.0.0.1"),
					HostPort: fmt.Sprintf("%d", port),
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("create redis container: %v", err)
	}
	containerID := create.ID
	defer func() {
		rmCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_, _ = cli.ContainerRemove(rmCtx, containerID, client.ContainerRemoveOptions{Force: true})
	}()
	if _, err := cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("start redis container: %v", err)
	}

	// Wait for redis to accept connections.
	testutil.WaitFor(t, 8*time.Second, "redis container accepting connections", func() bool {
		probe := redis.NewClient(&redis.Options{Addr: addr})
		defer probe.Close()
		pctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		return probe.Ping(pctx).Err() == nil
	})

	prefix := fmt.Sprintf("outage-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"
	var buf testutil.SyncBuffer
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	consumer := NewConsumer(ConsumerConfig{
		Client:        rc,
		Stream:        stream,
		Group:         group,
		Consumer:      prefix + "-consumer",
		Log:           slog.New(slog.NewTextHandler(&buf, nil)),
		Block:         200 * time.Millisecond,
		backoffTable:  []time.Duration{100 * time.Millisecond},
		backoffJitter: func(f float64) float64 { return f },
	})
	if err := consumer.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Consume an event before the outage.
	firstID, err := rc.XAdd(context.Background(),
		&redis.XAddArgs{Stream: stream, Values: map[string]any{"event": `{"a":1}`}}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	firstDone := make(chan struct{})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx2, func(ctx context.Context, msgID string, ev map[string]any) error {
			if msgID == firstID {
				close(firstDone)
			}
			return nil
		})
	}()
	<-firstDone
	if !consumer.Healthy() {
		t.Fatalf("consumer should be healthy before outage")
	}

	// Stop redis: outage begins.
	if _, err := cli.ContainerStop(ctx, containerID, client.ContainerStopOptions{}); err != nil {
		t.Fatalf("stop redis: %v", err)
	}
	testutil.WaitFor(t, 8*time.Second, "consumer unhealthy during outage", func() bool { return !consumer.Healthy() })
	if !strings.Contains(buf.String(), "Redis: read failed; retrying") {
		t.Fatalf("expected backoff log during outage, got: %q", buf.String())
	}

	// Restart redis and add a new event; the handler must run and recovery logs once.
	if _, err := cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("restart redis: %v", err)
	}
	testutil.WaitFor(t, 8*time.Second, "redis accepting connections after restart", func() bool {
		probe := redis.NewClient(&redis.Options{Addr: addr})
		defer probe.Close()
		pctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		return probe.Ping(pctx).Err() == nil
	})
	testutil.WaitFor(t, 8*time.Second, "consumer healthy after recovery", func() bool { return consumer.Healthy() })

	secondID, err := rc.XAdd(context.Background(),
		&redis.XAddArgs{Stream: stream, Values: map[string]any{"event": `{"b":2}`}}).Result()
	if err != nil {
		t.Fatalf("xadd after recovery: %v", err)
	}
	// The running Consume picks up the new message; assert it is processed (gone
	// from the PEL) rather than re-registering a handler.
	testutil.WaitFor(t, 8*time.Second, "second message processed (gone from PEL)", func() bool {
		entries, err := rc.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: stream, Group: group, Start: "-", End: "+", Count: 100,
		}).Result()
		if err != nil {
			return false
		}
		for _, pe := range entries {
			if pe.ID == secondID {
				return false
			}
		}
		return true
	})

	// Exactly one recovery line across the whole run.
	if got := strings.Count(buf.String(), "Redis connection recovered"); got != 1 {
		t.Fatalf("expected exactly one recovery line, got %d: %q", got, buf.String())
	}

	cancel2()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop")
	}
}

// TestIntegrationPendingGauge verifies the metrics sampler records the
// XPENDING depth gauge and the retries_total counter when a message stays
// pending across redeliveries.
func TestIntegrationPendingGauge(t *testing.T) {
	testutil.RequireRedis(t)
	m := metrics.New()
	e := newEnv(t, ConsumerConfig{
		Metrics:         m,
		MetricsInterval: 100 * time.Millisecond,
	})
	e.xadd(t, `{"a":1}`)
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		return fmt.Errorf("always fail to keep pending")
	})

	// The sampler records pending_entries once the failed message is in the PEL.
	// Use the typed Gauge read rather than substring-matching the Snapshot text.
	testutil.WaitFor(t, 8*time.Second, "pending_entries gauge set", func() bool {
		return m.Gauge(metrics.MetricPendingEntries) == 1
	})
	if got := m.Gauge(metrics.MetricPendingEntries); got != 1 {
		t.Fatalf("pending_entries gauge = %v, want 1", got)
	}
	// The oldest-pending age must be recorded. The typed Gauge read cannot
	// distinguish "set to 0" from "never set", so presence is asserted on the
	// snapshot's display fragment (the relay_ prefix is stripped there; only the
	// stable metric-name fragment is matched, not its value).
	testutil.WaitFor(t, 8*time.Second, "pending_oldest_age_seconds recorded", func() bool {
		return strings.Contains(m.Snapshot(), "pending_oldest_age_seconds")
	})
	// A reclaimed/redelivered pending message counts as a retry event.
	testutil.WaitFor(t, 8*time.Second, "retries_total increments", func() bool {
		return m.Counter(metrics.MetricRetries) >= 1
	})
	e.stop(t)
}

// TestIntegrationInvocationRunningUntilBlocksReexecution verifies the
// timeout-driven eligibility model end to end: a handler that claims the
// invocation via TryStart (persisting a running deadline) and then blocks keeps
// the invocation protected; a redelivery that calls TryStart again within the
// deadline is skipped (the executor is not called). After the deadline elapses,
// a later delivery executes it.
func TestIntegrationInvocationRunningUntilBlocksReexecution(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)

	// The handler simulates the runner: TryStart with a short timeout, then
	// block so the invocation stays protected for a controlled window.
	release := make(chan struct{})
	var calls atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", 2*time.Second); !started {
			// Protected by an active attempt deadline: skip (no execution) and
			// keep the message pending (the protected invocation may still
			// complete or fail on its own).
			return ErrInvocationNotEligible
		}
		calls.Add(1)
		<-release
		return nil
	})

	// The first delivery claims and blocks; the invocation is protected.
	testutil.WaitFor(t, 8*time.Second, "invocation claimed and running", func() bool {
		return calls.Load() >= 1
	})
	// A redelivery (reclaim) within the deadline must be skipped: TryStart
	// returns false, so the executor is not called again. Poll that the call
	// count stays at 1 across the reclaim grace window rather than a fixed sleep.
	waitSustained(t, "executor not called again while invocation protected", 500*time.Millisecond, func() bool {
		return calls.Load() == 1
	})

	// Release the handler; it acks and the message leaves the PEL.
	close(release)
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor called %d times, want exactly 1", got)
	}
}

// TestIntegrationInvocationStateSurvivesRestart verifies that a running marker
// persisted in Redis survives a consumer restart: a new consumer on the same
// stream/group skips the invocation while within the deadline and runs it after
// the deadline elapses.
func TestIntegrationInvocationStateSurvivesRestart(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("restart-inv-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"

	// Consumer A delivers the message and marks the invocation running with a
	// future deadline, then stops (simulating a restart).
	envA := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "restart-inv-A",
	})
	id := envA.xadd(t, `{"a":1}`)
	key := invocationStateKey(stream, group, id)
	deliveredA := make(chan struct{})
	envA.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(deliveredA)
		}
		return fmt.Errorf("leave pending")
	})
	<-deliveredA
	testutil.WaitFor(t, 8*time.Second, "message pending under consumer A", func() bool {
		_, ok := envA.pending()[id]
		return ok
	})
	// Mark running with a deadline ~1s in the future, then stop A.
	deadline := time.Now().Add(time.Second)
	if err := envA.client.HSet(context.Background(), key, "fn/h", runningValue(deadline, InvocationClaim{Attempt: 1, Token: "aabbccdd"})).Err(); err != nil {
		t.Fatalf("hset running marker: %v", err)
	}
	envA.stop(t)

	// Consumer B (new consumer, same stream/group) reclaims the idle message.
	// Within the deadline it must skip the invocation; after the deadline elapses
	// it runs it.
	envB := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "restart-inv-B",
	})
	var calls atomic.Int64
	envB.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", time.Second); !started {
			// Protected by the persisted deadline: skip execution but keep the
			// message pending (return ErrInvocationNotEligible) so a later
			// reclaim can run it once the deadline elapses. This mirrors the
			// runner's skip path without acknowledging a message whose
			// invocation is still protected.
			return ErrInvocationNotEligible
		}
		calls.Add(1)
		return nil
	})
	// B's reclaim loop must skip the invocation while the deadline is still in
	// the future: poll that the executor is never called across the grace
	// window rather than a fixed sleep.
	waitSustained(t, "executor not called before the deadline elapses on restart", 500*time.Millisecond, func() bool {
		return calls.Load() == 0
	})
	// After the deadline elapses, B executes it.
	testutil.WaitFor(t, 8*time.Second, "invocation executed after the deadline elapsed on restart", func() bool {
		return calls.Load() >= 1
	})
	envB.stop(t)
}

// TestIntegrationSuccessThenRetention verifies the success path: a successful
// invocation marks "ok", the message is acked, and then the invocation-state key
// is switched to terminal retention (marker + TTL) rather than deleted.
func TestIntegrationSuccessThenRetention(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			if p, ok := InvocationStateFrom(ctx); ok {
				if started, claim, _, _ := p.TryStart("fn/h", time.Hour); started {
					p.MarkComplete("fn/h", claim)
				}
			}
			close(acked)
		}
		return nil
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.waitRetained(t, key)
	e.stop(t)
}

// TestIntegrationSuccessWithoutStateLeavesNoKey pins that a successful
// stateless delivery (no invocation state was ever written) retains nothing: the
// retention step is a no-op for a missing hash, so it must not create an empty
// key (which would itself be a leak).
func TestIntegrationSuccessWithoutStateLeavesNoKey(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(acked)
		}
		return nil
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if n, err := e.client.Exists(context.Background(), key).Result(); err != nil || n != 0 {
		t.Fatalf("stateless delivery must leave no invocation-state key (exists=%d err=%v)", n, err)
	}
}

// TestIntegrationClassificationClaimSurvivesRedelivery verifies the real Redis
// HSETNX classification claim end to end: the first delivery of a message wins
// the claim (ClaimClassification true), a redelivery of the SAME pending message
// loses it (false). The claim lives in the message's invocation-state hash, which
// is persistent while the message is pending and switches to terminal retention
// once the message is ACKed. This is what keeps
// events_received == events_matched + events_unmatched across retries.
func TestIntegrationClassificationClaimSurvivesRedelivery(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	var claims atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		// The first delivery claims the classification; the message then stays
		// pending (returned error) so the reclaim loop redelivers it.
		if claimed, err := p.ClaimClassification(); err != nil {
			return err
		} else if claimed {
			claims.Add(1)
		}
		if claims.Load() < 1 {
			return nil
		}
		return fmt.Errorf("leave pending to force redelivery")
	})
	e.waitDelivered(t, id)
	// The claim field is persisted in Redis.
	testutil.WaitFor(t, 8*time.Second, "classification claim recorded", func() bool {
		v, err := e.client.HGet(context.Background(), key, classificationField).Result()
		return err == nil && v == "1"
	})
	// A redelivery (reclaim) happens and loses the claim: the count stays 1.
	testutil.WaitFor(t, 8*time.Second, "message redelivered and reclaim lost the claim", func() bool {
		return e.pending()[id] >= 2
	})
	waitSustained(t, "classification claimed exactly once across redeliveries", 500*time.Millisecond, func() bool {
		return claims.Load() == 1
	})
	// Stop and clean up; the claim's cleanup follows the invocation-state key's
	// terminal retention (the hash switch applies to the whole key, claim
	// included).
	e.stop(t)
}

// TestIntegrationFailureSchedulesRetryBackoff verifies the failure path: an
// attempt that fails records a next_attempt_at marker (RecordFailure) gating the
// invocation by its retry backoff, so a redelivery within the backoff is skipped
// (not eligible) rather than re-run.
func TestIntegrationFailureSchedulesRetryBackoff(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	var attempts atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, n, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			// Gated by the retry backoff: skip and keep pending.
			return ErrInvocationNotEligible
		}
		attempt := attempts.Add(1)
		if attempt >= 2 {
			return nil
		}
		// Simulate the runner's failure path: record a retry backoff.
		p.RecordFailure("fn/h", n, time.Minute)
		return fmt.Errorf("fail first delivery")
	})
	e.waitDelivered(t, id)
	// The failure must have recorded a next_attempt_at marker.
	testutil.WaitFor(t, 8*time.Second, "next_attempt_at marker recorded after failure", func() bool {
		v, err := e.client.HGet(context.Background(), key, "fn/h").Result()
		return err == nil && strings.HasPrefix(v, "next_attempt_at:")
	})
	// A redelivery within the backoff is skipped (not eligible), so the executor
	// is not called again: poll the attempt count stays at 1 across the grace
	// window rather than a fixed sleep.
	waitSustained(t, "executor not called again while gated by retry backoff", 500*time.Millisecond, func() bool {
		return attempts.Load() == 1
	})
	e.stop(t)
}

// TestIntegrationConcurrentReplicasNoDuplicate verifies that two consumers (A/B)
// on the same stream/group do not run the same invocation concurrently: while
// A's handler is blocked (in flight, protected by its running deadline), B's
// reclaim redelivery must skip the invocation. Across both replicas, the
// executor runs exactly once during the protected window.
func TestIntegrationConcurrentReplicasNoDuplicate(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("conc-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"

	// Consumer A: its handler claims the invocation (TryStart) and blocks on a
	// channel so the invocation stays in flight (protected by its running
	// deadline) for a controlled window.
	envA := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "conc-A",
	})
	id := envA.xadd(t, `{"a":1}`)
	releaseA := make(chan struct{})
	deliveredA := make(chan struct{})
	var aCalls atomic.Int64
	envA.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
			return ErrInvocationNotEligible
		}
		aCalls.Add(1)
		close(deliveredA)
		// Block until the test releases us, keeping the invocation in flight.
		<-releaseA
		return nil
	})
	<-deliveredA

	// Consumer B: reclaims the idle message while A's handler is still blocked.
	// Its redelivery must skip the invocation while protected.
	envB := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "conc-B",
		MinPendingIdle:  150 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
	})
	var bCalls atomic.Int64
	envB.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
			return ErrInvocationNotEligible
		}
		bCalls.Add(1)
		return nil
	})

	// B's reclaim loop must skip A's in-flight invocation (protected): poll that
	// B never executes across the grace window rather than a fixed sleep.
	waitSustained(t, "consumer B not executing while A's invocation is protected", 800*time.Millisecond, func() bool {
		return bCalls.Load() == 0
	})

	// Release A's handler; it acks and the message leaves the PEL.
	close(releaseA)
	testutil.WaitFor(t, 8*time.Second, "message acked by A (gone from PEL)", func() bool {
		_, ok := envA.pending()[id]
		return !ok
	})
	envA.stop(t)
	envB.stop(t)

	if got := aCalls.Load(); got != 1 {
		t.Fatalf("consumer A executed %d times, want exactly 1", got)
	}
	if got := bCalls.Load(); got != 0 {
		t.Fatalf("consumer B executed %d times, want 0", got)
	}
}

// TestIntegrationCrossReplicaNotEligibleKeepsPending is the regression test for
// the cross-replica ACK hazard: while consumer A holds a message in flight
// (its invocation is protected by a running deadline), consumer B reclaims it
// and its handler returns ErrInvocationNotEligible (the runner's skip path for
// a protected invocation). B must NOT acknowledge the message — it must stay in
// the PEL so A's eventual completion or failure is not lost.
func TestIntegrationCrossReplicaNotEligibleKeepsPending(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("ackhazard-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"

	// Consumer A: claims the invocation and blocks, keeping it in flight.
	envA := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "ackhazard-A",
	})
	id := envA.xadd(t, `{"a":1}`)
	releaseA := make(chan struct{})
	deliveredA := make(chan struct{})
	envA.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
			return ErrInvocationNotEligible
		}
		close(deliveredA)
		<-releaseA
		return nil
	})
	<-deliveredA

	// Consumer B: reclaims the idle message while A is in flight. Its handler
	// returns ErrInvocationNotEligible (protected), so B must leave the message
	// pending rather than ack it.
	envB := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "ackhazard-B",
		MinPendingIdle:  150 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
	})
	envB.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
			return ErrInvocationNotEligible
		}
		return nil
	})

	// B's reclaim loop must leave the message pending (not ack it) while A's
	// invocation is in flight: poll that it stays in the PEL across the grace
	// window rather than a fixed sleep.
	waitSustained(t, "message stays pending while A's invocation is in flight", 800*time.Millisecond, func() bool {
		_, ok := envB.pending()[id]
		return ok
	})

	// Release A; it acks and the message leaves the PEL.
	close(releaseA)
	testutil.WaitFor(t, 8*time.Second, "message acked by A (gone from PEL)", func() bool {
		_, ok := envA.pending()[id]
		return !ok
	})
	envA.stop(t)
	envB.stop(t)
}

// TestIntegrationNextAttemptAtGatesExecution verifies the retry-backoff gate
// end to end: a next_attempt_at marker in the future skips the invocation
// (ErrInvocationNotEligible), and once it is in the past the invocation
// executes.
func TestIntegrationNextAttemptAtGatesExecution(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// Pre-write a next_attempt_at marker ~1s in the future.
	future := time.Now().Add(time.Second)
	if err := e.client.HSet(context.Background(), key, "fn/h", nextAttemptValue(future, InvocationClaim{Attempt: 2, Token: "aabbccdd"})).Err(); err != nil {
		t.Fatalf("hset next_attempt_at marker: %v", err)
	}

	var calls atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		if started, _, _, _ := p.TryStart("fn/h", time.Second); !started {
			// Gated by the retry backoff: skip and keep pending.
			return ErrInvocationNotEligible
		}
		calls.Add(1)
		return nil
	})
	// While the marker is in the future, the invocation is skipped: poll the
	// executor is never called across the grace window rather than a fixed sleep.
	waitSustained(t, "executor not called while gated by next_attempt_at", 500*time.Millisecond, func() bool {
		return calls.Load() == 0
	})
	// After the deadline elapses, the invocation executes.
	testutil.WaitFor(t, 8*time.Second, "invocation executed after next_attempt_at deadline elapsed", func() bool {
		return calls.Load() >= 1
	})
	e.stop(t)
}

// TestIntegrationExhaustedSkipsWithoutRerun verifies that an exhausted marker is
// terminal at the stream layer: a redelivery skips the invocation (never re-runs
// it, as TryStart reports started=false). The handler here is a stand-in that
// returns nil for the terminal skip, so the message is acked; the real runner's
// message-level aggregate instead re-reports exhaustion on such a redelivery
// (see the runner package's event-path integration tests), which is what
// re-routes a message whose DLQ write previously failed.
func TestIntegrationExhaustedSkipsWithoutRerun(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// Pre-write an exhausted marker.
	if err := e.client.HSet(context.Background(), key, "fn/h", exhaustedValue(InvocationClaim{Attempt: 5, Token: "aabbccdd"}, false)).Err(); err != nil {
		t.Fatalf("hset exhausted marker: %v", err)
	}

	var calls atomic.Int64
	// seen records that the handler was invoked for the message at all: the
	// deterministic delivery signal. A PEL-presence poll cannot be used here —
	// the delivery+ack completes within a few milliseconds, so the PEL may
	// never contain the message between two polls.
	seen := &atomic.Bool{}
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		seen.Store(true)
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		started, _, _, _ := p.TryStart("fn/h", time.Second)
		if started {
			calls.Add(1)
		}
		// Terminal skip: return nil so the message is acked (all invocations
		// complete-or-exhausted).
		return nil
	})
	// Wait for the handler to have run (the delivery signal), then for the
	// ack. Without a delivery signal, the "gone from PEL" predicate is
	// trivially true for a never-delivered message, and a delivery that lands
	// after shutdown runs the handler with a cancelled context — TryStart
	// fails open and counts as executed, failing the assertion below even
	// though the terminal skip itself is correct.
	testutil.WaitFor(t, 8*time.Second, "handler invoked for the message", func() bool {
		return seen.Load()
	})
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
	if got := calls.Load(); got != 0 {
		t.Fatalf("exhausted invocation executed %d times; want 0 (terminal skip)", got)
	}
}

// TestIntegrationPanicLeavesPendingAndRetries verifies the stream-level panic
// boundary: a handler that panics is caught by processMessage's recover, the
// message is left pending (not acked), and a later reclaim retries it. On the
// retry the handler succeeds (non-panicking path) and the message is acked.
// Consume must survive the panic and keep running.
func TestIntegrationPanicLeavesPendingAndRetries(t *testing.T) {
	testutil.RequireRedis(t)
	// newEnv respects a pre-set Log, so the panic test injects its buffer through
	// the standard env instead of hand-rolling consumer setup.
	var buf testutil.SyncBuffer
	e := newEnv(t, ConsumerConfig{
		Log:             slog.New(slog.NewTextHandler(&buf, nil)),
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
	})
	id := e.xadd(t, `{"a":1}`)

	// The handler panics on the first delivery of the target message and
	// succeeds on subsequent deliveries (the panic is the boundary substitute:
	// the stream-level net catches it).
	var attempts atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	})

	// Wait for the message to be delivered into the PEL (attempt 1 panicked).
	e.waitDelivered(t, id)

	// The panic must have been logged and the message must NOT have been acked
	// (still pending right after the panic log line appears).
	testutil.WaitFor(t, 8*time.Second, "panic log line", func() bool {
		return strings.Contains(buf.String(), "panic in handler")
	})
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message was acked after the panicking attempt; want it left pending")
	}

	// Consume must still be alive after the panic.
	select {
	case <-e.done:
		t.Fatalf("Consume exited after the handler panic; want it to keep running")
	default:
	}

	// A later reclaim retries the message; the handler succeeds and it is acked.
	testutil.WaitFor(t, 8*time.Second, "message "+id+" gone from PEL", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	if got := attempts.Load(); got < 2 {
		t.Fatalf("handler attempts = %d, want >= 2 (panic then retry)", got)
	}
	e.stop(t)
}

// TestIntegrationMultiHandlerIndependence proves the aggregate multi-handler
// semantics end to end at the stream layer: a handler backed by two independent
// invocations ("fnA/h" fails its first delivery then succeeds; "fnB/h" always
// succeeds) mimics the runner's outcome aggregation. The two invocation-state
// hash fields are tracked independently — fnB is skipped on redelivery (its
// field is "ok") — and the message is ACKed only after both invocations resolve.
func TestIntegrationMultiHandlerIndependence(t *testing.T) {
	testutil.RequireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	var fnAExec, fnBExec atomic.Int64
	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := invocationStateFromCtx(t, ctx)
		if !ok {
			return fmt.Errorf("no invocation state in ctx")
		}
		// fnB: always succeeds, claimed once and completed (skipped on redelivery
		// via its "ok" field).
		if started, claimB, _, _ := p.TryStart("fnB/h", time.Hour); started {
			fnBExec.Add(1)
			p.MarkComplete("fnB/h", claimB)
		}
		// fnA: fails the first delivery (records a retry backoff), succeeds later.
		// If a redelivery arrives before the backoff elapses, TryStart is gated:
		// mirror the runner by leaving the message pending rather than ACKing an
		// unresolved invocation.
		startedA, claimA, _, _ := p.TryStart("fnA/h", time.Hour)
		if startedA {
			if fnAExec.Add(1) == 1 {
				p.RecordFailure("fnA/h", claimA, time.Millisecond) // simulate the runner's retry backoff
				return fmt.Errorf("fnA first delivery failed")
			}
		} else if !p.IsTerminal("fnA/h") {
			// fnA is protected (waiting out its retry backoff) and not yet resolved:
			// keep the message pending, never ack it.
			return ErrInvocationNotEligible
		}
		// All resolutions complete: the aggregate is nil so the stream ACKs.
		close(acked)
		return nil
	})
	<-acked
	testutil.WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)

	if fnAExec.Load() != 2 {
		t.Fatalf("fnA executions = %d, want 2 (first fails, second succeeds)", fnAExec.Load())
	}
	if fnBExec.Load() != 1 {
		t.Fatalf("fnB executions = %d, want 1 (completed and skipped on redelivery)", fnBExec.Load())
	}
	// After ACK the invocation-state key is terminal-retained.
	e.waitRetained(t, key)
}

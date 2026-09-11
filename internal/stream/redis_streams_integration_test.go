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
	"log"
	"net"
	"net/netip"
	"os"
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
	"relay/internal/metrics"
)

// requireRedis fails the test when the test Redis (REDIS_TEST_ADDR, default
// localhost:6379) is not reachable, instead of skipping: the Redis-Streams
// integration suite is meaningless without it.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := config.Env("REDIS_TEST_ADDR", "localhost:6379")
	opts, _ := config.RedisOptions(addr)
	cli := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		t.Fatalf(
			"redis integration test requires a reachable Redis at %s (ping: %v); start one with `docker compose -f compose.dev.yaml up -d`",
			addr,
			err,
		)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// requireDocker fails the test immediately when the Docker Engine API daemon
// cannot be reached via client.FromEnv (DOCKER_HOST, socket, socket proxy are
// all respected). Only TestIntegrationReconnectAndResume needs it: it boots its
// own disposable Redis in a Docker container. Missing infrastructure fails
// rather than skips.
func requireDocker(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		t.Fatalf("docker integration test requires a Docker daemon (client: %v)", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Fatalf("docker integration test requires a reachable Docker daemon (ping: %v); start one or run `docker compose -f compose.dev.yaml up -d`", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// WaitFor polls pred until it returns true or the deadline passes. It is a
// bounded poll: a generous budget avoids spurious CI flakes while the loop
// never spins forever. On timeout it fails the test with what as context.
func WaitFor(t *testing.T, timeout time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if pred() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

// syncBuffer is a mutex-protected strings.Builder for log output written by
// consumer goroutines while the test goroutine asserts on it concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// freePort returns an available TCP port on the loopback interface.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

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

	addr := config.Env("REDIS_TEST_ADDR", "localhost:6379")
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
	cfg.Log = log.New(os.Stderr, "itest: ", log.LstdFlags)
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

// waitSustained polls pred until it has held continuously for the given
// duration. It is the deterministic replacement for a fixed "grace period"
// sleep when we must assert that some state persists for a minimum window
// (e.g. a message stays pending / a protected invocation is not re-run). It
// fails the test if the state does not hold.
func waitSustained(t *testing.T, what string, dur time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	holdStart := time.Time{}
	for time.Now().Before(deadline) {
		if pred() {
			if holdStart.IsZero() {
				holdStart = time.Now()
			} else if time.Since(holdStart) >= dur {
				return
			}
		} else {
			holdStart = time.Time{}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to hold for %v", what, dur)
}

// waitDelivered waits until the message is present in the PEL (was read by a
// consumer but not yet acked). Used to avoid treating a never-delivered message
// as "acked".
func (e *testEnv) waitDelivered(t *testing.T, id string) {
	t.Helper()
	WaitFor(t, 8*time.Second, "message "+id+" delivered into PEL", func() bool {
		_, ok := e.pending()[id]
		return ok
	})
}

// waitGone waits until the message is absent from the PEL.
func (e *testEnv) waitGone(t *testing.T, id string) {
	t.Helper()
	WaitFor(t, 8*time.Second, "message "+id+" gone from PEL", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
}

func TestIntegrationNormalSuccessAck(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	acked := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			close(acked)
		}
		return nil
	})
	<-acked
	WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

func TestIntegrationHandlerFailureStaysPending(t *testing.T) {
	requireRedis(t)
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
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "message acked", func() bool {
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
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	var attempts atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		started, n, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			return ErrInvocationNotEligible
		}
		attempts.Store(int64(n))
		// Simulate the runner's exhaustion: mark the invocation terminal and
		// return ErrInvocationExhausted so the message routes to the DLQ.
		p.MarkExhausted("fn/h", n)
		return ErrInvocationExhausted
	})
	// The message is routed to the DLQ (and acked) on the first delivery, so it
	// may never linger in the PEL; wait for the DLQ entry instead.
	WaitFor(t, 8*time.Second, "message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
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
	if m.Values["reason"] == "" {
		t.Errorf("reason missing")
	}
}

func TestIntegrationMalformedEventRoutesToDLQImmediately(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{not json`)
	var handlerRan atomic.Bool
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		handlerRan.Store(true) // must not be reached (malformed routes to DLQ pre-handler)
		return nil
	})
	// The malformed message is routed straight to the DLQ (and acked) so it may
	// never appear in the PEL; wait for the DLQ entry instead.
	WaitFor(t, 8*time.Second, "malformed message routed to DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	WaitFor(t, 8*time.Second, "malformed message acked", func() bool {
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
	if m.Values["attempts"] != "1" {
		t.Errorf("malformed message should go straight to DLQ with attempts==1, got %v", m.Values["attempts"])
	}
}

func TestIntegrationDLQWriteFailureLeavesPending(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	// Make the DLQ stream name a wrong-type key so XADD fails. The handler
	// returns ErrInvocationExhausted to force DLQ routing, but the DLQ write
	// itself must fail.
	if r := e.client.Set(context.Background(), e.consumer.dlqStream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}
	id := e.xadd(t, `{"a":1}`)
	delivered := make(chan struct{}, 1)
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			select {
			case delivered <- struct{}{}:
			default:
			}
		}
		return ErrInvocationExhausted
	})
	<-delivered
	// The DLQ write failed, so the message must stay pending: poll it remains in
	// the PEL across the reclaim grace window rather than a fixed sleep.
	waitSustained(t, "message stays pending when DLQ write fails", 800*time.Millisecond, func() bool {
		_, ok := e.pending()[id]
		return ok
	})
	e.stop(t)
}

func TestIntegrationNoMatchAcked(t *testing.T) {
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "message acked", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}

func TestIntegrationStateRetainedOnFailureClearedOnAck(t *testing.T) {
	requireRedis(t)
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
			p.MarkComplete("fn/h")
		}
		if !first.Swap(true) {
			return fmt.Errorf("fail first delivery")
		}
		return nil
	})
	e.waitDelivered(t, id)
	WaitFor(t, 8*time.Second, "invocation state retained after failed delivery", func() bool {
		v, err := e.client.HGet(context.Background(), key, "fn/h").Result()
		return err == nil && v == "ok"
	})

	// The recovery loop reclaims the idle message; the handler sees the invocation
	// already done and returns nil, so the message is acked and state cleared.
	WaitFor(t, 8*time.Second, "message acked and invocation state cleared", func() bool {
		_, ok := e.pending()[id]
		if ok {
			return false
		}
		n, err := e.client.Exists(context.Background(), key).Result()
		return err == nil && n == 0
	})
	e.stop(t)
}

func TestIntegrationStateClearedOnDLQ(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// The handler returns ErrInvocationExhausted to route the message to the
	// DLQ. State must be cleared after the DLQ write + ACK.
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID == id {
			if p, ok := InvocationStateFrom(ctx); ok {
				p.MarkExhausted("fn/h", 1)
			}
		}
		return ErrInvocationExhausted
	})
	e.waitGone(t, id)
	WaitFor(t, 8*time.Second, "message in DLQ", func() bool {
		_, ok := e.dlq()[id]
		return ok
	})
	WaitFor(t, 8*time.Second, "invocation state cleared after DLQ", func() bool {
		n, err := e.client.Exists(context.Background(), key).Result()
		return err == nil && n == 0
	})
	e.stop(t)
}

// TestIntegrationProcessMessageClearsStateOnlyAfterAck drives the shared
// processMessage path directly (no Consume loop) against a real Redis: a
// message is XADDed and read into the group's PEL, then processMessage runs a
// succeeding handler. The invocation-state key must be cleared only after the
// ACK, and the message must be gone from the PEL.
func TestIntegrationProcessMessageClearsStateOnlyAfterAck(t *testing.T) {
	requireRedis(t)
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
	// is acked and the invocation-state key cleared.
	key := invocationStateKey(e.stream, e.group, id)
	if err := e.client.HSet(context.Background(), key, "fn/h", "ok").Err(); err != nil {
		t.Fatalf("hset invocation state: %v", err)
	}
	e.consumer.processMessage(context.Background(), msg, 1, func(ctx context.Context, msgID string, ev map[string]any) error {
		return nil
	})

	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked (gone from PEL)", id)
	}
	if n, err := e.client.Exists(context.Background(), key).Result(); err != nil || n != 0 {
		t.Fatalf("invocation-state key should be cleared after ACK (exists=%d err=%v)", n, err)
	}
}

// TestIntegrationRouteToDLQKeepsStateOnDLQWriteFailure verifies the
// clear-ordering contract in routeToDLQ: when the DLQ XADD fails (the DLQ stream
// name is a wrong-type key), the original message stays pending and its
// invocation-state key is retained; once the DLQ write succeeds, the message is
// acked and the invocation-state key is cleared.
func TestIntegrationRouteToDLQKeepsStateOnDLQWriteFailure(t *testing.T) {
	requireRedis(t)
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
	msg := msgs[0].Messages[0]
	key := invocationStateKey(e.stream, e.group, id)
	if err := e.client.HSet(context.Background(), key, "fn/h", "ok").Err(); err != nil {
		t.Fatalf("hset invocation state: %v", err)
	}

	// Force the DLQ XADD to fail by making the DLQ stream name a wrong-type key.
	if r := e.client.Set(context.Background(), e.consumer.dlqStream, "not-a-stream", 0); r.Err() != nil {
		t.Fatalf("set wrong-type key: %v", r.Err())
	}
	e.consumer.routeToDLQ(context.Background(), msg, fmt.Errorf("boom"), 1)

	// DLQ write failed: message stays pending and invocation state is retained.
	if _, ok := e.pending()[id]; !ok {
		t.Fatalf("message %s must stay pending when DLQ write fails", id)
	}
	if n, err := e.client.Exists(context.Background(), key).Result(); err != nil || n != 1 {
		t.Fatalf("invocation-state key must be retained on DLQ write failure (exists=%d err=%v)", n, err)
	}

	// Now let the DLQ write succeed: delete the wrong-type key and re-route.
	if err := e.client.Del(context.Background(), e.consumer.dlqStream).Err(); err != nil {
		t.Fatalf("del wrong-type key: %v", err)
	}
	e.consumer.routeToDLQ(context.Background(), msg, fmt.Errorf("boom"), 1)

	// DLQ write + ACK succeeded: message gone from PEL and invocation state
	// cleared.
	if _, ok := e.pending()[id]; ok {
		t.Fatalf("message %s should be acked after successful DLQ write", id)
	}
	if n, err := e.client.Exists(context.Background(), key).Result(); err != nil || n != 0 {
		t.Fatalf("invocation-state key should be cleared after DLQ (exists=%d err=%v)", n, err)
	}
}

func TestIntegrationRecoveryLoopStopsOnCancel(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error { return nil })
	e.stop(t) // stop asserts the goroutine (including recovery loop) exits, run with -race
}

func TestIntegrationRestartResilience(t *testing.T) {
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "message pending under consumer A", func() bool {
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
		}
		close(acked)
		return nil
	})
	<-acked
	WaitFor(t, 8*time.Second, "message reclaimed and acked by consumer B", func() bool {
		_, ok := envB.pending()[id]
		return !ok
	})
	envB.stop(t)
}

// TestIntegrationConsumeSurvivesOutage verifies that a consumer pointed at a
// Redis address with no listener keeps running (backing off) rather than
// exiting, and returns nil on cancellation.
func TestIntegrationConsumeSurvivesOutage(t *testing.T) {
	// A port with no listener: connect will fail, exercising the backoff path.
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cli := redis.NewClient(&redis.Options{Addr: addr})
	defer cli.Close()

	buf := &syncBuffer{}
	c := NewConsumer(ConsumerConfig{
		Client:        cli,
		Stream:        "outage-stream",
		Group:         "outage-group",
		Consumer:      "outage-consumer",
		Log:           log.New(buf, "", 0),
		backoffTable:  []time.Duration{50 * time.Millisecond},
		backoffJitter: func(f float64) float64 { return f },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = c.Consume(ctx, func(ctx context.Context, msgID string, ev map[string]any) error { return nil })
	}()

	// Give the loop time to fail and back off repeatedly; it must stay up.
	time.Sleep(3 * time.Second)
	select {
	case <-done:
		t.Fatalf("consumer exited during outage: %v", err)
	default:
	}
	if c.Healthy() {
		t.Fatalf("consumer should be unhealthy during outage")
	}
	if !strings.Contains(buf.String(), "redis read failed") {
		t.Fatalf("expected backoff failure log, got: %q", buf.String())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop on cancellation")
	}
	if err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
}

// TestIntegrationReconnectAndResume drives a disposable Redis container through
// an outage: consume an event, stop Redis, observe backoff + unhealthy, start
// Redis again, XADD an event, and assert the handler runs and exactly one
// recovery line is logged.
func TestIntegrationReconnectAndResume(t *testing.T) {
	cli := requireDocker(t)

	// Start a disposable redis on a free host port.
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	create, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{Image: "redis:8-alpine"},
		HostConfig: &container.HostConfig{
			PortBindings: network.PortMap{
				network.MustParsePort("6379/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: fmt.Sprintf("%d", port)}},
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
	WaitFor(t, 8*time.Second, "redis container accepting connections", func() bool {
		probe := redis.NewClient(&redis.Options{Addr: addr})
		defer probe.Close()
		pctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		return probe.Ping(pctx).Err() == nil
	})

	prefix := fmt.Sprintf("outage-%d", time.Now().UnixNano())
	stream, group := prefix+"-stream", prefix+"-group"
	var buf syncBuffer
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	consumer := NewConsumer(ConsumerConfig{
		Client:        rc,
		Stream:        stream,
		Group:         group,
		Consumer:      prefix + "-consumer",
		Log:           log.New(&buf, "", 0),
		Block:         200 * time.Millisecond,
		backoffTable:  []time.Duration{100 * time.Millisecond},
		backoffJitter: func(f float64) float64 { return f },
	})
	if err := consumer.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Consume an event before the outage.
	firstID, err := rc.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": `{"a":1}`}}).Result()
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
	WaitFor(t, 8*time.Second, "consumer unhealthy during outage", func() bool { return !consumer.Healthy() })
	if !strings.Contains(buf.String(), "redis read failed") {
		t.Fatalf("expected backoff log during outage, got: %q", buf.String())
	}

	// Restart redis and add a new event; the handler must run and recovery logs once.
	if _, err := cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{}); err != nil {
		t.Fatalf("restart redis: %v", err)
	}
	WaitFor(t, 8*time.Second, "redis accepting connections after restart", func() bool {
		probe := redis.NewClient(&redis.Options{Addr: addr})
		defer probe.Close()
		pctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		return probe.Ping(pctx).Err() == nil
	})
	WaitFor(t, 8*time.Second, "consumer healthy after recovery", func() bool { return consumer.Healthy() })

	secondID, err := rc.XAdd(context.Background(), &redis.XAddArgs{Stream: stream, Values: map[string]any{"event": `{"b":2}`}}).Result()
	if err != nil {
		t.Fatalf("xadd after recovery: %v", err)
	}
	// The running Consume picks up the new message; assert it is processed (gone
	// from the PEL) rather than re-registering a handler.
	WaitFor(t, 8*time.Second, "second message processed (gone from PEL)", func() bool {
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
	if got := strings.Count(buf.String(), "redis connection recovered"); got != 1 {
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
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "pending_entries gauge set", func() bool {
		return strings.Contains(m.Snapshot(), "pending_entries value=1")
	})

	got := m.Snapshot()
	if !strings.Contains(got, "pending_entries value=1") {
		t.Fatalf("pending_entries not set; snapshot:\n%s", got)
	}
	// The oldest-pending age must be recorded (the just-added message is young
	// but its parseable age is > 0).
	if !strings.Contains(got, "pending_oldest_age_seconds value=") {
		t.Fatalf("pending_oldest_age_seconds not set; snapshot:\n%s", got)
	}
	// A reclaimed/redelivered pending message counts as a retry event.
	WaitFor(t, 8*time.Second, "retries_total increments", func() bool {
		return strings.Contains(m.Snapshot(), "retries_total count=")
	})
	e.stop(t)
}

// TestIntegrationInvocationRunningUntilBlocksReexecution verifies the
// timeout-driven eligibility model end to end: a handler that claims the
// invocation via TryStart (persisting a running deadline) and then blocks keeps
// the invocation protected; a redelivery that calls TryStart again within the
// deadline is skipped (the executor is not called). After the deadline expires,
// a later delivery executes it.
func TestIntegrationInvocationRunningUntilBlocksReexecution(t *testing.T) {
	requireRedis(t)
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", 2*time.Second); !started {
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
	WaitFor(t, 8*time.Second, "invocation claimed and running", func() bool {
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
	WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
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
// the deadline expires.
func TestIntegrationInvocationStateSurvivesRestart(t *testing.T) {
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "message pending under consumer A", func() bool {
		_, ok := envA.pending()[id]
		return ok
	})
	// Mark running with a deadline ~1s in the future, then stop A.
	deadline := time.Now().Add(time.Second)
	if err := envA.client.HSet(context.Background(), key, "fn/h", runningValue(deadline, 1)).Err(); err != nil {
		t.Fatalf("hset running marker: %v", err)
	}
	envA.stop(t)

	// Consumer B (new consumer, same stream/group) reclaims the idle message.
	// Within the deadline it must skip the invocation; after expiry it runs it.
	envB := newEnv(t, ConsumerConfig{
		Stream: stream, Group: group, Consumer: "restart-inv-B",
	})
	var calls atomic.Int64
	envB.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", time.Second); !started {
			// Protected by the persisted deadline: skip execution but keep the
			// message pending (return ErrInvocationNotEligible) so a later
			// reclaim can run it once the deadline expires. This mirrors the
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
	waitSustained(t, "executor not called before deadline expiry on restart", 500*time.Millisecond, func() bool {
		return calls.Load() == 0
	})
	// After the deadline expires, B executes it.
	WaitFor(t, 8*time.Second, "invocation executed after deadline expiry on restart", func() bool {
		return calls.Load() >= 1
	})
	envB.stop(t)
}

// TestIntegrationSuccessThenCleanup verifies the success path: a successful
// invocation marks "ok", the message is acked, and the invocation-state key is
// cleared.
func TestIntegrationSuccessThenCleanup(t *testing.T) {
	requireRedis(t)
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
	WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	WaitFor(t, 8*time.Second, "invocation-state key cleared after ack", func() bool {
		n, err := e.client.Exists(context.Background(), key).Result()
		return err == nil && n == 0
	})
	e.stop(t)
}

// TestIntegrationFailureSchedulesRetryBackoff verifies the failure path: an
// attempt that fails records a next_attempt_at marker (RecordFailure) gating the
// invocation by its retry backoff, so a redelivery within the backoff is skipped
// (not eligible) rather than re-run.
func TestIntegrationFailureSchedulesRetryBackoff(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	var attempts atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		started, _, _ := p.TryStart("fn/h", time.Hour)
		if !started {
			// Gated by the retry backoff: skip and keep pending.
			return ErrInvocationNotEligible
		}
		n := attempts.Add(1)
		if n >= 2 {
			return nil
		}
		// Simulate the runner's failure path: record a retry backoff.
		p.RecordFailure("fn/h", time.Minute)
		return fmt.Errorf("fail first delivery")
	})
	e.waitDelivered(t, id)
	// The failure must have recorded a next_attempt_at marker.
	WaitFor(t, 8*time.Second, "next_attempt_at marker recorded after failure", func() bool {
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
	requireRedis(t)
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
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
	WaitFor(t, 8*time.Second, "message acked by A (gone from PEL)", func() bool {
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
	requireRedis(t)
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", 5*time.Second); !started {
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
	WaitFor(t, 8*time.Second, "message acked by A (gone from PEL)", func() bool {
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
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// Pre-write a next_attempt_at marker ~1s in the future.
	future := time.Now().Add(time.Second)
	if err := e.client.HSet(context.Background(), key, "fn/h", nextAttemptValue(future, 2)).Err(); err != nil {
		t.Fatalf("hset next_attempt_at marker: %v", err)
	}

	var calls atomic.Int64
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		if msgID != id {
			return nil
		}
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		if started, _, _ := p.TryStart("fn/h", time.Second); !started {
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
	// After the marker expires, the invocation executes.
	WaitFor(t, 8*time.Second, "invocation executed after next_attempt_at expiry", func() bool {
		return calls.Load() >= 1
	})
	e.stop(t)
}

// TestIntegrationExhaustedSkipsWithoutRerun verifies that an exhausted marker is
// terminal: a redelivery skips the invocation (never re-runs it) and, because
// nothing executed and the invocation is terminal (not protected), Handle would
// return nil — but here the handler simulates the runner by returning nil for a
// terminal skip, so the message is acked.
func TestIntegrationExhaustedSkipsWithoutRerun(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{})
	id := e.xadd(t, `{"a":1}`)
	key := invocationStateKey(e.stream, e.group, id)

	// Pre-write an exhausted marker.
	if err := e.client.HSet(context.Background(), key, "fn/h", exhaustedValue(5)).Err(); err != nil {
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
		p, ok := InvocationStateFrom(ctx)
		if !ok {
			t.Fatalf("no invocation state in ctx")
		}
		started, _, _ := p.TryStart("fn/h", time.Second)
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
	WaitFor(t, 8*time.Second, "handler invoked for the message", func() bool {
		return seen.Load()
	})
	WaitFor(t, 8*time.Second, "message acked (gone from PEL)", func() bool {
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
	cli := requireRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("panic-%d", time.Now().UnixNano())
	stream, group, consumer := prefix+"-stream", prefix+"-group", prefix+"-consumer"

	var buf syncBuffer
	c := NewConsumer(ConsumerConfig{
		Client:          cli,
		Stream:          stream,
		Group:           group,
		Consumer:        consumer,
		Block:           300 * time.Millisecond,
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
		Log:             log.New(&buf, "", 0),
	})
	if err := c.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cli.Del(ctx, stream, c.dlqStream).Err()
		_ = cli.Close()
	})

	id, err := cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": `{"a":1}`},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}

	// The handler panics on the first delivery of the target message and
	// succeeds on subsequent deliveries (the panic is the boundary substitute:
	// the stream-level net catches it).
	var attempts atomic.Int64
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		errCh <- c.Consume(ctx, func(ctx context.Context, msgID string, ev map[string]any) error {
			if msgID != id {
				return nil
			}
			if attempts.Add(1) == 1 {
				panic("boom")
			}
			return nil
		})
	}()

	// Wait for the message to be delivered into the PEL (attempt 1 panicked).
	WaitFor(t, 8*time.Second, "message "+id+" delivered into PEL", func() bool {
		entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: stream, Group: group, Start: "-", End: "+", Count: 100,
		}).Result()
		if err != nil {
			return false
		}
		for _, pe := range entries {
			if pe.ID == id {
				return true
			}
		}
		return false
	})

	// The panic must have been logged and the message must NOT have been acked
	// (still pending right after the panic log line appears).
	WaitFor(t, 8*time.Second, "panic log line", func() bool {
		return strings.Contains(buf.String(), "PANIC in handler")
	})
	if _, ok := pendingOf(cli, stream, group, id); !ok {
		t.Fatalf("message was acked after the panicking attempt; want it left pending")
	}

	// Consume must still be alive after the panic.
	select {
	case <-done:
		t.Fatalf("Consume exited after the handler panic; want it to keep running")
	default:
	}

	// A later reclaim retries the message; the handler succeeds and it is acked.
	WaitFor(t, 8*time.Second, "message "+id+" gone from PEL", func() bool {
		_, ok := pendingOf(cli, stream, group, id)
		return !ok
	})
	if got := attempts.Load(); got < 2 {
		t.Fatalf("handler attempts = %d, want >= 2 (panic then retry)", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop on cancellation")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
}

// pendingOf returns the pending entry for id, or ok=false when absent.
func pendingOf(cli *redis.Client, stream, group, id string) (int64, bool) {
	entries, err := cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		return 0, false
	}
	for _, pe := range entries {
		if pe.ID == id {
			return pe.RetryCount, true
		}
	}
	return 0, false
}

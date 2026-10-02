//go:build integration

// These tests drive the full schedule-occurrence path against real Redis end to
// end: a real stream.Consumer is wired with a ScheduleRunner that dispatches to
// runner.InvokeHandler, and the real InvokeHandler executes the (fake) executor
// and drives the invocation-state lifecycle — running/complete/backoff/exhausted
// markers — as the stream ACKs, leaves pending, and clears the invocation-state
// key. They live in the runner package (rather than the stream package) because
// the stream package cannot import the runner (an import cycle).
package runner

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/stream"
	"relay/internal/testutil"
)

const scheduleFnName = "courses"
const scheduleHandler = "jobs.cleanup.handler"

// markerAttempt extracts the attempt number from a well-formed active/exhausted
// marker:
//
//	"running:<deadline_ms>:<attempt>:<token>"
//	"next_attempt_at:<deadline_ms>:<attempt>:<token>"
//	"exhausted:<attempt>:<token>[:dlq]"
//
// It returns 0 for a marker it cannot parse, so a test can assert the exact
// attempt in the value grammar.
func markerAttempt(v string) int {
	var rest string
	switch {
	case strings.HasPrefix(v, "running:"):
		rest = strings.TrimPrefix(v, "running:")
		// <deadline>:<attempt>:<token>
		_, rest, _ = strings.Cut(rest, ":")
	case strings.HasPrefix(v, "next_attempt_at:"):
		rest = strings.TrimPrefix(v, "next_attempt_at:")
		_, rest, _ = strings.Cut(rest, ":")
	case strings.HasPrefix(v, "exhausted:"):
		// <attempt>:<token>[:dlq]
		rest = strings.TrimPrefix(v, "exhausted:")
		attempt, _, _ := strings.Cut(rest, ":")
		n, err := strconv.Atoi(attempt)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
	attempt, _, found := strings.Cut(rest, ":")
	if !found {
		return 0
	}
	n, err := strconv.Atoi(attempt)
	if err != nil {
		return 0
	}
	return n
}

// scheduleOcc builds a schedule occurrence under the fixed app/handler the
// runner test apps register.
func scheduleOcc(scheduledAt time.Time) schedule.Occurrence {
	return schedule.Occurrence{
		App:         scheduleFnName,
		Schedule:    scheduleHandler,
		Handler:     scheduleHandler,
		ScheduledAt: scheduledAt,
	}
}

// stateAwareExecutor records each execution, can fail the first failN
// executions, and can block the (failN+1)-th execution on a channel so tests can
// hold the running marker observable. InvokeHandler's invocation-state lifecycle
// is what decides retry/exhaustion; this executor only reports success/failure.
type stateAwareExecutor struct {
	mu          sync.Mutex
	calls       int
	fail        int
	releaseCall <-chan struct{} // when non-nil, the (fail+1)-th call blocks until released
}

func (e *stateAwareExecutor) Execute(_ context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	e.mu.Lock()
	n := e.calls
	e.calls++
	failN := e.fail
	releaseCall := e.releaseCall
	e.mu.Unlock()
	if n < failN {
		return fmt.Errorf("injected failure")
	}
	// The first successful (non-failing) call blocks until the test releases it,
	// holding the running marker observable in Redis.
	if releaseCall != nil && n == failN {
		<-releaseCall
	}
	return nil
}

func (e *stateAwareExecutor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// scheduleEnv is a self-contained schedule integration harness: a real consumer
// (with the real runner's InvokeHandler wired as ScheduleRunner) on a
// unixnano-unique stream/group.
type scheduleEnv struct {
	t      *testing.T
	client *redis.Client
	stream string
	group  string
	dlq    string
	ctx    context.Context
	cancel func()
	holder consumerHolder
	done   chan struct{}
	errCh  chan error
}

// newScheduleEnv builds the harness, wiring r.InvokeHandler as the consumer's
// ScheduleRunner.
func newScheduleEnv(t *testing.T, r *Runner) *scheduleEnv {
	t.Helper()
	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	redisOpts, err := config.RedisOptions(addr)
	if err != nil {
		t.Fatalf("redis options: %v", err)
	}
	cli := redis.NewClient(redisOpts)
	cctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	cfg := stream.ConsumerConfig{
		Client:          cli,
		Stream:          streamName,
		Group:           prefix + "-group",
		Consumer:        prefix + "-consumer",
		Log:             testutil.DiscardLogger(),
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 200 * time.Millisecond,
		Block:           300 * time.Millisecond,
		ScheduleRunner:  r.InvokeOccurrence,
	}
	c := stream.NewConsumer(cfg)
	if err := c.EnsureGroup(cctx); err != nil {
		cancel()
		_ = cli.Close()
		t.Fatalf("ensure group: %v", err)
	}
	e := &scheduleEnv{
		t:      t,
		client: cli,
		stream: streamName,
		group:  cfg.Group,
		dlq:    stream.DLQStreamFor(streamName),
		ctx:    cctx,
		cancel: cancel,
		holder: consumerHolder{c: c},
	}
	t.Cleanup(e.cleanup)
	return e
}

func (e *scheduleEnv) cleanup() {
	// Cancel FIRST, then wait for the consumer goroutine to actually exit.
	// Consume returns only once e.ctx is cancelled (its XREADGROUP block is
	// bounded, but it re-blocks until ctx is done), so waiting for e.done before
	// cancelling can never succeed early: it would always burn the full timeout,
	// which is exactly the fixed-sleep anti-pattern. The wait below remains a
	// genuine bounded join on the real shutdown condition.
	e.cancel()
	if e.done != nil {
		select {
		case <-e.done:
		case <-time.After(5 * time.Second):
		}
	}
	cctx := context.Background()
	_ = e.client.Del(cctx, e.stream, e.dlq).Err()
	// Best-effort removal of this env's invocation-state keys. The stream/group
	// names contain no ':' or '%', so percent-encoding is identity for them and a
	// direct prefix scan is safe and bounded to this env.
	match := "relay:invocation:" + e.stream + ":*"
	var cursor uint64
	for {
		keys, next, err := e.client.Scan(cctx, cursor, match, 100).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = e.client.Del(cctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	_ = e.client.Close()
}

func (e *scheduleEnv) start() {
	e.t.Helper()
	e.done = make(chan struct{})
	e.errCh = make(chan error, 1)
	go func() {
		defer close(e.done)
		e.errCh <- e.consumer().Consume(e.ctx, noopHandler)
	}()
}

// consumer holds the Consumer built by newScheduleEnv so start() can drive it.
type consumerHolder struct{ c *stream.Consumer }

func (e *scheduleEnv) consumer() *stream.Consumer { return e.holder.c }

func (e *scheduleEnv) stop(t *testing.T) {
	t.Helper()
	e.cancel()
	select {
	case <-e.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer did not stop")
	}
	if err := <-e.errCh; err != nil {
		t.Fatalf("consume returned error: %v", err)
	}
}

// noopHandler is a fallback never invoked for a schedule message (schedule
// messages bypass the normal handler); it exists so Consume's non-schedule path
// has a callable handler if a stray non-schedule message is ever read.
func noopHandler(_ context.Context, _ string, _ map[string]any) error { return nil }

// xadd writes a schedule envelope to this env's stream and returns its ID.
func (e *scheduleEnv) xadd(occ schedule.Occurrence) string {
	e.t.Helper()
	env, err := occ.Envelope()
	if err != nil {
		e.t.Fatalf("envelope: %v", err)
	}
	id, err := e.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"event": string(env)},
	}).Result()
	if err != nil {
		e.t.Fatalf("xadd: %v", err)
	}
	return id
}

// pending reports the retry count and presence of a message in the group PEL.
func (e *scheduleEnv) pending(msgID string) (retry int64, ok bool) {
	entries, err := e.client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: e.stream,
		Group:  e.group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return 0, false
	}
	for _, pe := range entries {
		if pe.ID == msgID {
			return pe.RetryCount, true
		}
	}
	return 0, false
}

// invocationKey returns the invocation-state hash key for a message.
func (e *scheduleEnv) invocationKey(msgID string) string {
	return "relay:invocation:" + e.stream + ":" + e.group + ":" + msgID
}

// stateField returns the invocation-state field for the schedule invocation.
func (e *scheduleEnv) stateField(msgID string) (string, error) {
	return e.client.HGet(context.Background(), e.invocationKey(msgID), scheduleFnName+"/"+scheduleHandler).Result()
}

func (e *scheduleEnv) hsetState(msgID, value string) error {
	return e.client.HSet(context.Background(), e.invocationKey(msgID), scheduleFnName+"/"+scheduleHandler, value).Err()
}

func (e *scheduleEnv) hdelState(msgID string) error {
	return e.client.HDel(context.Background(), e.invocationKey(msgID), scheduleFnName+"/"+scheduleHandler).Err()
}

func (e *scheduleEnv) hasStateKey(msgID string) bool {
	n, err := e.client.Exists(context.Background(), e.invocationKey(msgID)).Result()
	return err == nil && n == 1
}

// isRetained reports whether the message's invocation-state hash has been
// switched to terminal retention (present + terminal marker + positive TTL),
// the post-ACK contract that replaced the old eager delete.
func (e *scheduleEnv) isRetained(msgID string) bool {
	key := e.invocationKey(msgID)
	v, err := e.client.HGet(context.Background(), key, "__terminal").Result()
	if err != nil || v != "1" {
		return false
	}
	d, err := e.client.PTTL(context.Background(), key).Result()
	return err == nil && d > 0
}

// notLeaked reports whether a message left no RECOVERABLE invocation-state hash:
// either no key at all (nothing was ever written, e.g. an obsolete occurrence
// resolved before TryStart), or a key that is terminal-retained (marker + TTL).
// A present, un-retained key would be a leak of persistent state.
func (e *scheduleEnv) notLeaked(msgID string) bool {
	return !e.hasStateKey(msgID) || e.isRetained(msgID)
}

// registerScheduleFn builds a runner with a single schedule app whose
// handler is scheduleHandler carrying the given retry count.
func registerScheduleFn(t *testing.T, exec Executor, retries int) *Runner {
	t.Helper()
	return NewWithMetrics(
		[]*PreparedApp{scheduleFnForHandler(t, scheduleHandler, exec, time.Second, retries)},
		testutil.DiscardLogger(), nil)
}

// scheduleFnForHandler builds a prepared app with a single schedule entry
// routed to handler (the handler the stream's schedule messages target), so
// InvokeHandler resolves the schedule entry's timeout AND retry count.
func scheduleFnForHandler(t *testing.T, handler string, exec Executor, scheduleTimeout time.Duration, retries int) *PreparedApp {
	t.Helper()
	return NewPrepared(
		app.App{
			Name: scheduleFnName,
			Template: &app.Template{
				Runtime: "node24",
				Events:  []app.EventRule{{Handler: "index.run", Pattern: app.Pattern{}, Timeout: scheduleTimeout, Retries: app.DefaultRetries}},
				Schedules: []app.Schedule{{
					Name:     handler,
					Handler:  handler,
					Cron:     "* * * * *",
					Location: time.UTC,
					Timeout:  scheduleTimeout,
					Retries:  retries,
				}},
			},
		},
		&runtime.Prepared{Name: scheduleFnName, Image: "x"},
		exec,
	)
}

// eventually polls pred until it holds, failing with what on timeout.
func (e *scheduleEnv) eventually(what string, pred func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("timed out waiting for %s", what)
}

// redisAvailable returns a live client or skips the test when Redis is down.
func redisAvailable(t *testing.T) *redis.Client {
	t.Helper()
	addr := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	opts, err := config.RedisOptions(addr)
	if err != nil {
		t.Fatalf("redis options: %v", err)
	}
	cli := redis.NewClient(opts)
	if err := cli.Ping(context.Background()).Err(); err != nil {
		_ = cli.Close()
		t.Skipf("redis integration test requires a reachable Redis at %s (%v); start `docker compose -f compose.dev.yaml up -d redis`", addr, err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// --- tests ---

// TestIntegrationScheduleInvocationStateLifecycle drives a schedule message whose
// first execution fails and the second succeeds. It asserts the lifecycle
// deterministically: the failure records a next_attempt_at marker at attempt 1;
// the redelivery's running marker is made observable by blocking the executor
// inside the second attempt; and once released, the message is ACKed and its
// invocation-state key cleared (the clear only runs after a successful ACK).
func TestIntegrationScheduleInvocationStateLifecycle(t *testing.T) {
	_ = redisAvailable(t)
	release := make(chan struct{})
	exec := &stateAwareExecutor{fail: 1, releaseCall: release}
	r := registerScheduleFn(t, exec, 1)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	// Delivery 1 fails (attempt 1); the failure is recorded as a next_attempt_at
	// marker carrying attempt number 1.
	e.eventually("failure recorded (next_attempt_at marker at attempt 1)", func() bool {
		v, err := e.stateField(id)
		return err == nil && strings.HasPrefix(v, "next_attempt_at:") && markerAttempt(v) == 1
	})

	// Force eligibility by removing the backoff marker so a reclaim redelivers
	// now (the real backoff is 1m+; we bypass it deterministically).
	if err := e.hdelState(id); err != nil {
		t.Fatalf("hdel: %v", err)
	}

	// Delivery 2 starts: the second execution blocks inside the executor, so a
	// persisted running marker is observable in Redis BEFORE the attempt
	// completes or the message is acked. (Note: HDEL above reset the Redis
	// store's attempt counter, so the marker is `running:<deadline>:1:<token>`,
	// not attempt 2 — the exhaustion decision is what matters, and it is pinned
	// by the unit tests; here we assert the marker's `running:` shape.)
	e.eventually("running marker persisted while attempt in flight", func() bool {
		v, err := e.stateField(id)
		return err == nil && strings.HasPrefix(v, "running:")
	})
	// The message is still pending while the running attempt is protected.
	if _, ok := e.pending(id); !ok {
		t.Fatalf("message must be pending while the running attempt is in flight")
	}

	// Release the second attempt: it succeeds, the message is ACKed (gone from
	// the PEL), and the invocation-state key is terminal-retained after the ACK.
	close(release)
	e.eventually("schedule message acked and invocation-state key terminal-retained", func() bool {
		if _, ok := e.pending(id); ok {
			return false
		}
		return e.isRetained(id)
	})

	if got := exec.count(); got != 2 {
		t.Fatalf("executor calls = %d, want 2 (fail once, retry succeeds after reclaim)", got)
	}
}

// TestIntegrationScheduleOccurrencesDoNotShareState publishes TWO schedule
// occurrences of the same app/handler at different seconds (→ distinct
// msgIDs → distinct invocation-state keys) and verifies each occurrence keeps
// its own lifecycle state: each failing occurrence records an independent
// next_attempt_at marker under its own key, and neither's marker (or lifecycle)
// leaks into the other's hash.
func TestIntegrationScheduleOccurrencesDoNotShareState(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{fail: 1000} // always fails → never ACKs, backoff markers persist
	r := registerScheduleFn(t, exec, 100)
	e := newScheduleEnv(t, r)
	// Two occurrences, one minute apart → different msgIDs and different keys.
	o1 := scheduleOcc(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC))
	o2 := scheduleOcc(time.Date(2026, 8, 1, 10, 1, 0, 0, time.UTC))
	id1 := e.xadd(o1)
	id2 := e.xadd(o2)
	e.start()

	if id1 == id2 {
		t.Fatalf("occurrences share a msgID (%s); cannot test isolation", id1)
	}

	// Both occurrences see their first delivery fail: each records its OWN
	// next_attempt_at marker under its own (distinct) invocation-state key.
	e.eventually("occurrence 1 failure recorded under its own key", func() bool {
		v, err := e.stateField(id1)
		return err == nil && strings.HasPrefix(v, "next_attempt_at:")
	})
	e.eventually("occurrence 2 failure recorded under its own key", func() bool {
		v, err := e.stateField(id2)
		return err == nil && strings.HasPrefix(v, "next_attempt_at:")
	})

	// The two hashes must be physically distinct keys, so neither lifecycle can
	// leak into the other.
	if e.invocationKey(id1) == e.invocationKey(id2) {
		t.Fatalf("occurrences resolved to the same invocation-state key")
	}

	// Each occurrence is pending (never acked, never DLQ'd) with its own marker —
	// the two states coexist independently under separate keys.
	for _, id := range []string{id1, id2} {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("occurrence %s must be pending (independent failure state)", id)
		}
		if v, _ := e.stateField(id); !strings.HasPrefix(v, "next_attempt_at:") {
			t.Fatalf("occurrence %s field = %q, want its own next_attempt_at marker", id, v)
		}
	}
}

// TestIntegrationScheduleExhaustionRoutesToDLQ proves the real-runner exhaustion
// path: a schedule whose template Retries is 0 exhausts after a single failing
// attempt (InvokeHandler marks it terminal and returns ErrInvocationExhausted),
// and the stream routes the message to the DLQ — not left pending and not acked
// silently.
func TestIntegrationScheduleExhaustionRoutesToDLQ(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{fail: 100} // always fails
	r := registerScheduleFn(t, exec, 0)    // retries:0 → exhaust on attempt 1
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	// The exhausted invocation is terminal; the message routes to the DLQ.
	e.eventually("exhausted schedule message routed to DLQ", func() bool {
		return e.inDlq(id)
	})
	// The message is acked after being routed to the DLQ (gone from the PEL).
	e.eventually("exhausted schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 1 {
		t.Fatalf("executor calls = %d, want 1 (only the initial attempt before exhaustion)", got)
	}
	// The DLQ entry attributes the exhaustion from the handler retry state:
	// retries:0 exhausts on handler attempt 1, and this is the first delivery, so
	// handler_attempts and deliveries are both 1.
	m := e.dlqGet()[id]
	if m.Values["handler_attempts"] != "1" {
		t.Errorf("handler_attempts = %v, want 1 (from the invocation retry state)", m.Values["handler_attempts"])
	}
	// The schedule entry names the exact app/handler that exhausted.
	if m.Values["app"] != scheduleFnName || m.Values["handler"] != scheduleHandler {
		t.Errorf("function/handler = %v/%v, want %s/%s",
			m.Values["app"], m.Values["handler"], scheduleFnName, scheduleHandler)
	}
	if m.Values["deliveries"] != "1" {
		t.Errorf("deliveries = %v, want 1 (first delivery)", m.Values["deliveries"])
	}
}

// dlqGet reads all DLQ entries keyed by original message ID.
func (e *scheduleEnv) dlqGet() map[string]redis.XMessage {
	msgs, err := e.client.XRange(context.Background(), e.dlq, "-", "+").Result()
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

func (e *scheduleEnv) inDlq(id string) bool {
	_, ok := e.dlqGet()[id]
	return ok
}

// TestIntegrationScheduleReclaimSkipsCompleted pre-codes a schedule invocation as
// "ok" BEFORE the consumer starts, so a real delivery skips the executor and is
// ACKed (and its state key cleared), proving the already-complete redelivery path.
func TestIntegrationScheduleReclaimSkipsCompleted(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 1)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 7, 2, 9, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	// Pre-code the invocation as "ok".
	if err := e.hsetState(id, "ok"); err != nil {
		t.Fatalf("hset: %v", err)
	}
	e.start()

	e.eventually("already-ok schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (already-complete invocation skipped)", got)
	}
	// The key is terminal-retained after the ACK.
	e.eventually("invocation-state key terminal-retained after ack", func() bool {
		return e.isRetained(id)
	})
}

// TestIntegrationScheduleRetryFailureLeavesPending verifies that a schedule whose
// runner keeps failing leaves the field as a retryable next_attempt_at marker and
// the message pending (never acked, never DLQ'd) across reclaims.
func TestIntegrationScheduleRetryFailureLeavesPending(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{fail: 1000}
	r := registerScheduleFn(t, exec, 100)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 7, 3, 10, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	e.eventually("schedule message delivered into PEL", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	e.eventually("failure recorded (next_attempt_at marker written)", func() bool {
		v, err := e.stateField(id)
		return err == nil && strings.HasPrefix(v, "next_attempt_at:")
	})
	// The message stays pending (never acked, never DLQ'd) across reclaims; the
	// field is a retryable next_attempt_at marker, never exhausted.
	deadline := time.Now().Add(1200 * time.Millisecond)
	wasPending := true
	for time.Now().Before(deadline) {
		_, ok := e.pending(id)
		if !ok {
			wasPending = false
			break
		}
		if !e.hasStateKey(id) {
			t.Fatalf("invocation-state key missing while the message is pending")
		}
		if v, _ := e.stateField(id); strings.HasPrefix(v, "exhausted:") {
			t.Fatalf("invocation must not be exhausted on a retryable failure, got %q", v)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !wasPending {
		t.Fatalf("message must stay pending (never acked) on retryable failure")
	}
	if v, _ := e.stateField(id); !strings.HasPrefix(v, "next_attempt_at:") {
		t.Fatalf("invocation field = %q, want a next_attempt_at marker", v)
	}
}

// TestIntegrationScheduleObsoleteAppRemoved publishes an occurrence for a
// app, then REMOVES that app from the runner's registry before the
// consumer starts. The occurrence must be treated as obsolete: ACKed (gone from
// the PEL), never DLQ'd, and the invocation-state key terminal-retained — never
// retried forever.
func TestIntegrationScheduleObsoleteAppRemoved(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{fail: 1000} // would never succeed if it ran
	r := registerScheduleFn(t, exec, 100)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	id := e.xadd(o)

	// Remove the app from the runner's registry BEFORE the consumer starts,
	// so the reclaim delivery sees it absent.
	r.Registry().Replace(scheduleFnName, nil)

	e.start()
	e.eventually("obsolete (function removed) schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("obsolete schedule message not routed to DLQ", func() bool {
		// Once acked, nothing must reappear, and no DLQ entry may exist.
		if _, ok := e.pending(id); ok {
			return false
		}
		return !e.inDlq(id)
	})
	// Obsolete occurrences resolve before TryStart, so no recoverable state may
	// remain (either no key, or a terminal-retained one).
	e.eventually("obsolete schedule invocation-state key not leaked", func() bool {
		return e.notLeaked(id)
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (removed function must not execute)", got)
	}
}

// TestIntegrationScheduleObsoleteScheduleHandlerRemoved publishes an occurrence,
// then SWAPS the app's template to one whose Schedules no longer include
// the handler. The occurrence must be treated as obsolete: ACKed, not DLQ'd,
// never retried forever, and the executor never runs.
func TestIntegrationScheduleObsoleteScheduleHandlerRemoved(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 100)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 8, 3, 13, 0, 0, 0, time.UTC))
	id := e.xadd(o)

	// Swap the app to a template whose Schedules no longer include the
	// occurrence's handler, BEFORE the consumer starts.
	r.Registry().Replace(scheduleFnName, NewPrepared(
		app.App{
			Name: scheduleFnName,
			Template: &app.Template{
				Runtime: "node24",
				Events:  []app.EventRule{{Handler: "index.run", Pattern: app.Pattern{}}},
				// No Schedules: the handler is removed.
			},
		},
		&runtime.Prepared{Name: scheduleFnName, Image: "x"},
		exec,
	))

	e.start()
	e.eventually("obsolete (handler removed) schedule message acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("obsolete handler-removed schedule message not routed to DLQ", func() bool {
		if _, ok := e.pending(id); ok {
			return false
		}
		return !e.inDlq(id)
	})
	e.eventually("obsolete handler-removed invocation-state key not leaked", func() bool {
		return e.notLeaked(id)
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (removed handler must not execute)", got)
	}
}

// TestIntegrationScheduleUnavailableStaysPending is regression #3: an app
// still REGISTERED but temporarily unavailable (NewUnavailable) must leave the
// occurrence PENDING across reclaim cycles — never ACKed, never DLQ'd. It is a
// temporary condition, not an obsolete removal.
func TestIntegrationScheduleUnavailableStaysPending(t *testing.T) {
	_ = redisAvailable(t)
	r := NewWithMetrics(
		[]*PreparedApp{NewUnavailable(app.App{Name: scheduleFnName, Template: &app.Template{Runtime: "node24"}})},
		testutil.DiscardLogger(), nil)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 8, 3, 14, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	// The message is delivered into the PEL and stays pending (unavailable =
	// retryable) across the reclaim grace window, never ACKed and never DLQ'd.
	e.eventually("unavailable schedule message delivered into PEL", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	deadline := time.Now().Add(1200 * time.Millisecond)
	wasPending := true
	for time.Now().Before(deadline) {
		_, ok := e.pending(id)
		if !ok {
			wasPending = false
			break
		}
		if e.inDlq(id) {
			t.Fatalf("unavailable schedule message must not be DLQ'd")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !wasPending {
		t.Fatalf("message must stay pending (never acked) while the function is temporarily unavailable")
	}
}

// scheduleFnNamed builds a prepared app with ONE schedule whose stable NAME
// is `name` and whose handler is `handler`, so a handler change under the SAME
// name can be exercised end to end.
func scheduleFnNamed(t *testing.T, name, handler string, exec Executor, scheduleTimeout time.Duration, retries int) *PreparedApp {
	t.Helper()
	return NewPrepared(
		app.App{
			Name: scheduleFnName,
			Template: &app.Template{
				Runtime: "node24",
				Schedules: []app.Schedule{{
					Name:     name,
					Handler:  handler,
					Cron:     "* * * * *",
					Location: time.UTC,
					Timeout:  scheduleTimeout,
					Retries:  retries,
				}},
			},
		},
		&runtime.Prepared{Name: scheduleFnName, Image: "x"},
		exec,
	)
}

// namedOcc builds an occurrence for a schedule NAME and envelope handler.
func namedOcc(name, handler string, at time.Time) schedule.Occurrence {
	return schedule.Occurrence{App: scheduleFnName, Schedule: name, Handler: handler, ScheduledAt: at}
}

// stateFieldFor reads an arbitrary invocation field from a message's state hash.
func (e *scheduleEnv) stateFieldFor(msgID, invocation string) (string, error) {
	return e.client.HGet(context.Background(), e.invocationKey(msgID), invocation).Result()
}

// descriptorField reads the pinned schedule admission descriptor value.
func (e *scheduleEnv) descriptorField(msgID string) (string, error) {
	return e.client.HGet(context.Background(), e.invocationKey(msgID), "__schedule").Result()
}

// TestIntegrationScheduleAdmissionPinsCurrentHandlerBeforeStart pins the
// pre-admission handler change end to end: the occurrence's envelope names a
// STALE handler, but by the time the consumer runs, the template's schedule NAME
// resolves to a NEW handler. The occurrence is admitted under the NEW handler and
// its descriptor is pinned with it.
func TestIntegrationScheduleAdmissionPinsCurrentHandlerBeforeStart(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{}
	r := NewWithMetrics(
		[]*PreparedApp{scheduleFnNamed(t, "sched", "jobs.new", exec, time.Second, 0)},
		testutil.DiscardLogger(), nil)
	e := newScheduleEnv(t, r)
	// Envelope carries the OLD handler under the SAME schedule name.
	id := e.xadd(namedOcc("sched", "jobs.old", time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)))
	e.start()

	// Wait for the occurrence to be ADMITTED (descriptor pinned) and executed,
	// before asserting the ACK: `pending` is also false before XREADGROUP reads
	// the entry, so the execution marker is the real progress signal.
	e.eventually("occurrence admitted and executed", func() bool {
		if exec.count() != 1 {
			return false
		}
		v, err := e.descriptorField(id)
		return err == nil && v != ""
	})
	e.eventually("occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	v, err := e.descriptorField(id)
	if err != nil {
		t.Fatalf("read pinned descriptor: %v", err)
	}
	desc, ok := streamDecodeDescriptor(t, v)
	if !ok {
		t.Fatalf("descriptor %q did not decode", v)
	}
	if desc.Handler != "jobs.new" {
		t.Fatalf("pinned handler = %q, want the CURRENT template handler jobs.new", desc.Handler)
	}
	if desc.Schedule != "sched" {
		t.Fatalf("pinned schedule = %q, want sched", desc.Schedule)
	}
}

// TestIntegrationScheduleAdmittedThenScheduleRemovedStillCompletes pins the
// post-admission removal end to end: an occurrence's first attempt fails (so the
// descriptor is pinned and a retry is scheduled), then the schedule NAME is
// REMOVED from the template while the message is pending. The reclaim must still
// run the ADMITTED handler and complete — never treat the admitted occurrence as
// obsolete.
func TestIntegrationScheduleAdmittedThenScheduleRemovedStillCompletes(t *testing.T) {
	_ = redisAvailable(t)
	failing := &firstFailExecutor{failFirst: 1}
	// retries: 4 so the first failure is retryable (does not exhaust).
	r := NewWithMetrics(
		[]*PreparedApp{scheduleFnNamed(t, "sched", scheduleHandler, failing, time.Second, 4)},
		testutil.DiscardLogger(), nil)
	e := newScheduleEnv(t, r)
	id := e.xadd(namedOcc("sched", scheduleHandler, time.Date(2026, 9, 2, 9, 0, 0, 0, time.UTC)))
	e.start()

	// First delivery fails: the descriptor is pinned and a next_attempt_at marker
	// is recorded for the admitted handler.
	e.eventually("first attempt failed and descriptor pinned", func() bool {
		v, err := e.stateFieldFor(id, scheduleFnName+"/"+scheduleHandler)
		if err != nil || !strings.HasPrefix(v, "next_attempt_at:") {
			return false
		}
		_, derr := e.descriptorField(id)
		return derr == nil
	})

	// Remove the schedule NAME from the template, and force the pending retry to
	// be eligible NOW (delete the backoff marker) so a reclaim runs immediately.
	r.Registry().Replace(scheduleFnName, NewPrepared(
		app.App{
			Name: scheduleFnName,
			Template: &app.Template{
				Runtime: "node24",
				// No Schedules: the admitted name is gone.
			},
		},
		&runtime.Prepared{Name: scheduleFnName, Image: "x"},
		failing,
	))
	if err := e.client.HDel(context.Background(), e.invocationKey(id), scheduleFnName+"/"+scheduleHandler).Err(); err != nil {
		t.Fatalf("hdel state: %v", err)
	}

	e.eventually("admitted occurrence completes after its schedule was removed", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := failing.count(); got < 2 {
		t.Fatalf("executor calls = %d, want >= 2 (admitted retry ran)", got)
	}
	if e.inDlq(id) {
		t.Fatal("an admitted occurrence must not be dead-lettered")
	}
}

// firstFailExecutor fails the first failFirst calls and then succeeds, counting
// every call.
type firstFailExecutor struct {
	mu        sync.Mutex
	calls     int
	failFirst int
}

func (e *firstFailExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	e.mu.Lock()
	n := e.calls
	e.calls++
	e.mu.Unlock()
	if n < e.failFirst {
		return fmt.Errorf("injected failure")
	}
	return nil
}

func (e *firstFailExecutor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// streamDecodeDescriptor decodes a pinned descriptor value using the stream
// package's grammar through its exported round-trip: encode/decode are
// unexported, so this reconstructs the fields by parsing the known layout.
func streamDecodeDescriptor(t *testing.T, v string) (streamScheduleDescriptor, bool) {
	t.Helper()
	parts := strings.Split(v, ":")
	if len(parts) != 5 || parts[0] != "sd1" {
		return streamScheduleDescriptor{}, false
	}
	timeoutMs, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return streamScheduleDescriptor{}, false
	}
	retries, err := strconv.Atoi(parts[2])
	if err != nil {
		return streamScheduleDescriptor{}, false
	}
	schedule, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil {
		return streamScheduleDescriptor{}, false
	}
	handler, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil {
		return streamScheduleDescriptor{}, false
	}
	return streamScheduleDescriptor{
		Schedule: string(schedule),
		Handler:  string(handler),
		Timeout:  time.Duration(timeoutMs) * time.Millisecond,
		Retries:  retries,
	}, true
}

// streamScheduleDescriptor mirrors the stream package's descriptor for assertions.
type streamScheduleDescriptor struct {
	Schedule string
	Handler  string
	Timeout  time.Duration
	Retries  int
}

// TestIntegrationScheduleForgedTimestampNeverExecutesAndDLQs pins the end-to-end
// semantic-integrity contract: an occurrence whose scheduled_at is not a real
// firing of the schedule's current cron (the schedule is every minute, but the
// timestamp carries a non-zero second — impossible for the definition) is
// rejected before execution and dead-lettered, never run and never ACKed as
// success.
func TestIntegrationScheduleForgedTimestampNeverExecutesAndDLQs(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	// The schedule is "* * * * *" (every whole minute); :30s cannot be a firing.
	o := scheduleOcc(time.Date(2026, 8, 4, 10, 0, 30, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	e.eventually("forged-timestamp occurrence DLQ'd", func() bool { return e.inDlq(id) })
	e.eventually("forged-timestamp occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (a forged timestamp must never execute)", got)
	}
	m := e.dlqGet()[id]
	reason, _ := m.Values["reason"].(string)
	// The forged :30s instant parses structurally (only a sub-second component
	// is structurally malformed), so it reaches the runner's semantic validation
	// and is rejected there as ErrScheduleInvalid because it is not a firing of
	// the every-minute cron. Both the semantic rejection and a generic invalid
	// claim are the terminal, non-retryable DLQ path.
	if !strings.Contains(reason, stream.ErrScheduleInvalid.Error()) &&
		!strings.Contains(reason, "schedule claim") {
		t.Fatalf("DLQ reason = %q, want it to name the invalid schedule claim", m.Values["reason"])
	}
}

// TestIntegrationScheduleValidTimestampExecutes pins the complement: a real
// whole-minute firing of the same schedule executes and is ACKed.
func TestIntegrationScheduleValidTimestampExecutes(t *testing.T) {
	_ = redisAvailable(t)
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 8, 4, 11, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	e.eventually("valid occurrence executed", func() bool { return exec.count() >= 1 })
	e.eventually("valid occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if e.inDlq(id) {
		t.Fatal("a valid firing must not be dead-lettered")
	}
}

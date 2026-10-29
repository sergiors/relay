//go:build integration

// These tests drive the full schedule-occurrence path against real Redis end to
// end: a real stream.Consumer is wired with a ScheduleRunner that dispatches to
// runner.InvokeOccurrence, whose path executes the fake executor and drives the
// invocation-state lifecycle (running/complete/backoff/exhausted) as the stream
// ACKs, leaves pending, and clears the invocation-state key. They live in the
// runner package (rather than the stream package) because
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
	"relay/internal/dlq"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/state"
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
//	"exhausted:<attempt>:<token>"
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
		// <attempt>:<token>
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
// (with the real runner's InvokeOccurrence wired as ScheduleRunner) on a
// unixnano-unique stream/group.
type scheduleEnv struct {
	t        *testing.T
	client   *redis.Client
	stream   string
	group    string
	dlqStore dlq.Store
	pg       *state.State
	ctx      context.Context
	cancel   func()
	holder   consumerHolder
	done     chan struct{}
	errCh    chan error
}

// newScheduleEnv builds the harness and wires r.InvokeOccurrence as the
// consumer's ScheduleRunner. The Redis client is the fail-fast one owned by
// testutil.RequireRedis; its cleanup closes the client after this harness's own
// key cleanup has run.
func newScheduleEnv(t *testing.T, r *Runner) *scheduleEnv {
	t.Helper()
	cli := testutil.RequireRedis(t)
	cctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("itest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	// The durable DLQ store: fresh per-test PostgreSQL schema, exactly the
	// production wiring.
	pg := testutil.NewState(t)
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
		DLQStore:        pg,
	}
	c := stream.NewConsumer(cfg)
	if err := c.EnsureGroup(cctx); err != nil {
		cancel()
		t.Fatalf("ensure group: %v", err)
	}
	e := &scheduleEnv{
		t:        t,
		client:   cli,
		stream:   streamName,
		group:    cfg.Group,
		dlqStore: pg,
		pg:       pg,
		ctx:      cctx,
		cancel:   cancel,
		holder:   consumerHolder{c: c},
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
	_ = e.client.Del(cctx, e.stream).Err()
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
	// The client is owned by testutil.RequireRedis, whose cleanup closes it after
	// this cleanup (t.Cleanup is LIFO).
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

// --- tests ---

// TestIntegrationScheduleInvocationStateLifecycle drives a schedule message whose
// first execution fails and the second succeeds. It asserts the lifecycle
// deterministically: the failure records a next_attempt_at marker at attempt 1;
// the redelivery's running marker is made observable by blocking the executor
// inside the second attempt; and once released, the message is ACKed and its
// invocation-state key cleared (the clear only runs after a successful ACK).
func TestIntegrationScheduleInvocationStateLifecycle(t *testing.T) {
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
	if m.HandlerAttempts != 1 {
		t.Errorf("HandlerAttempts = %d, want 1 (from the invocation retry state)", m.HandlerAttempts)
	}
	// The schedule entry names the exact app/handler that exhausted.
	if m.App != scheduleFnName || m.Handler != scheduleHandler {
		t.Errorf("function/handler = %q/%q, want %s/%s",
			m.App, m.Handler, scheduleFnName, scheduleHandler)
	}
	if m.Deliveries != 1 {
		t.Errorf("Deliveries = %d, want 1 (first delivery)", m.Deliveries)
	}
	// The whole delivery's rows are settled once the confirmed ACK lands. The
	// settlement write follows the ACK on the wire, so it is observed by
	// polling rather than immediately after the ACK becomes observable.
	e.eventually("the exhausted occurrence's entry settled after the confirmed ACK", func() bool {
		m := e.dlqGet()[id]
		return m.SettledAt != ""
	})
}

// dlqGet reads the durable DLQ entries keyed by their source message ID (the
// first entry per message, so single-invocation schedule occurrences read
// naturally; dlqEntriesFor covers the multi-entry case).
func (e *scheduleEnv) dlqGet() map[string]dlq.Entry {
	out := map[string]dlq.Entry{}
	entries, err := e.dlqStore.ListDLQEntries(context.Background())
	if err != nil {
		e.t.Fatalf("list DLQ entries: %v", err)
	}
	for _, entry := range entries {
		if _, ok := out[entry.SourceMessageID]; !ok {
			out[entry.SourceMessageID] = entry
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
	// The app-absent ACK is a RESERVED obsolete rejection (F-030): the durable
	// reservation outweighs a concurrent admission and survives the terminal
	// retention, so a retry can never be raced into an admission. Wait for the
	// terminal-retained hash (only a real delivery + ACK produces it) so the read
	// cannot race an undelivered message.
	e.eventually("obsolete occurrence terminal-retained", func() bool {
		return e.isRetained(id)
	})
	if v, err := e.client.HGet(context.Background(), e.invocationKey(id), reservedScheduleRejectionField).Result(); err != nil || v != "obsolete" {
		t.Fatalf("reservation = (%q,%v), want \"obsolete\" (an app-absent ACK must be reserved)", v, err)
	}
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (removed function must not execute)", got)
	}
}

// TestIntegrationScheduleObsoleteScheduleHandlerRemoved publishes an occurrence,
// then SWAPS the app's template to one whose Schedules no longer include
// the handler. The occurrence must be treated as obsolete: ACKed, not DLQ'd,
// never retried forever, and the executor never runs.
func TestIntegrationScheduleObsoleteScheduleHandlerRemoved(t *testing.T) {
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
	// The forged :30s instant parses structurally (only a sub-second component
	// is structurally malformed), so it reaches the runner's semantic validation
	// and is rejected there as ErrScheduleInvalid because it is not a firing of
	// the every-minute cron. Both the semantic rejection and a generic invalid
	// claim are the terminal, non-retryable DLQ path.
	if !strings.Contains(m.Reason, stream.ErrScheduleInvalid.Error()) &&
		!strings.Contains(m.Reason, "schedule claim") {
		t.Fatalf("DLQ reason = %q, want it to name the invalid schedule claim", m.Reason)
	}
}

// TestIntegrationScheduleValidTimestampExecutes pins the complement: a real
// whole-minute firing of the same schedule executes and is ACKed.
func TestIntegrationScheduleValidTimestampExecutes(t *testing.T) {
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

// TestIntegrationScheduleCrashAfterAdmissionSpendsAttempt pins the schedule-path
// F-002 window end to end against real Redis: an admitted occurrence whose first
// attempt crashed after the confirmed admission (attempt 1 persisted as running but
// the handler never entered). With retries:0, the reclaim's claim of attempt 2 runs
// the handler once, fails, and exhausts the occurrence — so the message routes to
// the DLQ with handler_attempts 2 despite a single handler execution. As on the
// event path, the crash alone did not exhaust: the reclaim's real failing
// execution is what wrote the terminal marker.
func TestIntegrationScheduleCrashAfterAdmissionSpendsAttempt(t *testing.T) {
	exec := &stateAwareExecutor{fail: 1000} // always fails
	r := registerScheduleFn(t, exec, 0)     // retries:0 → exhaust on the next attempt
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC))
	id := e.xadd(o)

	// Seed the post-admission crash: the descriptor is pinned AND the admitted
	// handler is running at attempt 1 with an already-elapsed deadline, and no
	// transition ever followed.
	if err := e.client.HSet(context.Background(), e.invocationKey(id),
		scheduleFnName+"/"+scheduleHandler, runningValue(time.Now().Add(-time.Hour), 1)).Err(); err != nil {
		t.Fatalf("seed crash state: %v", err)
	}
	if err := e.client.HSet(context.Background(), e.invocationKey(id), "__schedule",
		streamEncodeDescriptor(t, streamScheduleDescriptor{
			Schedule: scheduleHandler, Handler: scheduleHandler, Timeout: time.Second, Retries: 0,
		})).Err(); err != nil {
		t.Fatalf("seed descriptor: %v", err)
	}

	e.start()

	e.eventually("occurrence routed to DLQ after the post-crash attempt", func() bool {
		return e.inDlq(id)
	})
	e.eventually("occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})

	if got := exec.count(); got != 1 {
		t.Fatalf("handler executions = %d, want 1 (the crashed attempt 1 never ran)", got)
	}
	m, ok := e.dlqGet()[id]
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if m.HandlerAttempts != 2 {
		t.Errorf("HandlerAttempts = %d, want 2 (the claim before the crash consumed attempt 1)", m.HandlerAttempts)
	}
}

// streamEncodeDescriptor renders the stream package's descriptor grammar
// ("sd1:<timeout_ms>:<retries>:<b64 schedule>:<b64 handler>") for tests that need
// to seed a pinned admission without going through the runner.
func streamEncodeDescriptor(t *testing.T, d streamScheduleDescriptor) string {
	t.Helper()
	return "sd1:" + strconv.FormatInt(d.Timeout.Milliseconds(), 10) +
		":" + strconv.Itoa(d.Retries) +
		":" + base64.RawURLEncoding.EncodeToString([]byte(d.Schedule)) +
		":" + base64.RawURLEncoding.EncodeToString([]byte(d.Handler))
}

// reservedScheduleRejectionField mirrors the stream package's reserved
// scheduleRejectionField ("__schedule_rejected"), which the runner integration
// tests seed directly to simulate a rejection reserved by an earlier delivery.
const reservedScheduleRejectionField = "__schedule_rejected"

// TestIntegrationScheduleReservedInvalidRetryCompletesDLQ pins the F-030 retry
// contract end to end: an INVALID rejection reserved by a delivery that crashed
// BEFORE its DLQ write is honored by a later delivery — the occurrence is
// dead-lettered and the message ACKed — even though the current template would
// have admitted the (valid) occurrence. The handler never executes.
func TestIntegrationScheduleReservedInvalidRetryCompletesDLQ(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	// A valid whole-minute firing: the current template WOULD admit it.
	o := scheduleOcc(time.Date(2026, 9, 4, 8, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	// Simulate the crashed delivery: the rejection is reserved, no DLQ entry was
	// written, and the message is still pending.
	if err := e.client.HSet(context.Background(), e.invocationKey(id), reservedScheduleRejectionField, "invalid").Err(); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	e.start()

	e.eventually("reserved-invalid occurrence DLQ'd", func() bool { return e.inDlq(id) })
	e.eventually("reserved-invalid occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (a reserved occurrence must never run)", got)
	}
	m, ok := e.dlqGet()[id]
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if !strings.Contains(m.Reason, stream.ErrScheduleInvalid.Error()) {
		t.Fatalf("DLQ reason = %q, want it to name the invalid rejection", m.Reason)
	}
	// Exactly one entry (no duplicate on the retry): the stable identity makes
	// the PostgreSQL write idempotent.
	if entries := listDLQEntriesFor(t, e.dlqStore, id); len(entries) != 1 {
		t.Fatalf("DLQ entries for %s = %d, want 1", id, len(entries))
	}
}

// TestIntegrationScheduleReservedObsoleteRetryAcks pins the obsolete half: a
// reserved OBSOLETE rejection is ACKed on the retry — never dead-lettered, never
// executed — even though the current template would admit the occurrence.
func TestIntegrationScheduleReservedObsoleteRetryAcks(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 4, 9, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	if err := e.client.HSet(context.Background(), e.invocationKey(id), reservedScheduleRejectionField, "obsolete").Err(); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	e.start()

	e.eventually("reserved-obsolete occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	e.eventually("reserved-obsolete occurrence not DLQ'd", func() bool {
		if _, ok := e.pending(id); ok {
			return false
		}
		return !e.inDlq(id)
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (a reserved occurrence must never run)", got)
	}
}

// TestIntegrationScheduleInvalidOccurrenceReservesRejection pins that the FIRST
// delivery of an invalid occurrence records the durable reservation in the
// invocation-state hash (it survives the terminal ACK's retention), so a retry
// can never be raced into an admission.
func TestIntegrationScheduleInvalidOccurrenceReservesRejection(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	// A non-firing instant (:30s) of the every-minute schedule.
	o := scheduleOcc(time.Date(2026, 9, 4, 10, 0, 30, 0, time.UTC))
	id := e.xadd(o)
	e.start()

	e.eventually("invalid occurrence DLQ'd", func() bool { return e.inDlq(id) })
	e.eventually("invalid occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (an invalid occurrence must never run)", got)
	}
	v, err := e.client.HGet(context.Background(), e.invocationKey(id), reservedScheduleRejectionField).Result()
	if err != nil || v != "invalid" {
		t.Fatalf("reservation = (%q,%v), want \"invalid\"", v, err)
	}
}

// scheduleInvocationField is the "<app>/<handler>" invocation-state field the
// runner integration apps record, used to seed legacy no-descriptor markers.
const scheduleInvocationField = scheduleFnName + "/" + scheduleHandler

// assertLegacyMarkerUntouched asserts the seeded legacy invocation marker is
// unchanged and no rejection reservation was written over it.
func assertLegacyMarkerUntouched(t *testing.T, e *scheduleEnv, id, marker string) {
	t.Helper()
	v, err := e.client.HGet(context.Background(), e.invocationKey(id), scheduleInvocationField).Result()
	if err != nil || v != marker {
		t.Fatalf("marker = (%q,%v), want %q (never overwritten)", v, err, marker)
	}
	if n, err := e.client.HExists(context.Background(), e.invocationKey(id), reservedScheduleRejectionField).Result(); err != nil || n {
		t.Fatalf("rejection field present = (%v,%v), want absent (no reservation over an admitted marker)", n, err)
	}
}

// staysPending asserts the message stays in the PEL and is never dead-lettered
// for the whole window, so a "must not ACK/DLQ" contract is observed over time
// rather than at one instant.
func (e *scheduleEnv) staysPending(t *testing.T, id string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("message %s left the PEL (ACKed) while it must stay pending", id)
		}
		if e.inDlq(id) {
			t.Fatalf("message %s was dead-lettered while it must stay pending", id)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestIntegrationScheduleLegacyOKWithoutDescriptorAcked pins the app-absent
// legacy success path: a no-descriptor "ok" marker (the pre-descriptor crash
// window where the invocation succeeded but its ACK never ran) is ACKed
// idempotently when the app is absent — never left PEL-pending forever, never
// DLQ'd — and the marker is never overwritten by a reservation.
func TestIntegrationScheduleLegacyOKWithoutDescriptorAcked(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	if err := e.hsetState(id, "ok"); err != nil {
		t.Fatalf("seed ok marker: %v", err)
	}
	// The app is absent on this replica.
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	// Wait for the terminal-retained hash (only a real delivery + ACK produces
	// it), so the assertion cannot race an undelivered message.
	e.eventually("legacy ok occurrence acked and terminal-retained", func() bool {
		return e.isRetained(id)
	})
	e.eventually("legacy ok occurrence not DLQ'd", func() bool {
		if _, ok := e.pending(id); ok {
			return false
		}
		return !e.inDlq(id)
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (a legacy ok occurrence must never run)", got)
	}
	assertLegacyMarkerUntouched(t, e, id, "ok")
}

// TestIntegrationScheduleLegacyExhaustedWithoutDescriptorDLQOnce pins the
// app-absent legacy exhaustion path: a no-descriptor exhausted marker still owes
// its per-invocation DLQ entry, so the occurrence is dead-lettered exactly once
// (with the exact app/handler and the marker's attempt) and then ACKed. The
// marker carries no persistence state, so it is left untouched: PostgreSQL
// idempotency never needs a marker upgrade.
func TestIntegrationScheduleLegacyExhaustedWithoutDescriptorDLQOnce(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 5, 9, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	if err := e.hsetState(id, "exhausted:2:deadbeef"); err != nil {
		t.Fatalf("seed exhausted marker: %v", err)
	}
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	e.eventually("legacy exhausted occurrence DLQ'd", func() bool { return e.inDlq(id) })
	e.eventually("legacy exhausted occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0 (a legacy exhausted occurrence must never run)", got)
	}
	if entries := listDLQEntriesFor(t, e.dlqStore, id); len(entries) != 1 {
		t.Fatalf("DLQ entries for %s = %d, want exactly 1", id, len(entries))
	}
	m, ok := e.dlqGet()[id]
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if m.App != scheduleFnName || m.Handler != scheduleHandler {
		t.Fatalf("DLQ app/handler = (%q,%q), want (%s,%s)",
			m.App, m.Handler, scheduleFnName, scheduleHandler)
	}
	if m.HandlerAttempts != 2 {
		t.Fatalf("HandlerAttempts = %d, want 2 (the marker's retained attempt)", m.HandlerAttempts)
	}
	// The exhausted marker carries no persistence state: it is left verbatim.
	assertLegacyMarkerUntouched(t, e, id, "exhausted:2:deadbeef")
}

// TestIntegrationScheduleRemovedSuffixMarkerIsEligible pins the end-to-end
// consequence of dropping the read-only legacy exhaustion suffix: an occurrence
// whose invocation hash still carries the OLD ":dlq" persistence record is NOT
// read as the terminal exhaustion (no hidden legacy read path); the delivery is
// eligible again, so the handler executes and the run then completes normally —
// the occurrence is ACKed, terminal-retained, and never dead-lettered (the
// marker was consumed by the fresh run).
func TestIntegrationScheduleRemovedSuffixMarkerIsEligible(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	if err := e.hsetState(id, "exhausted:2:deadbeef:dlq"); err != nil {
		t.Fatalf("seed removed-suffix exhausted marker: %v", err)
	}
	e.start()

	// The occurrence RUNS (the marker is eligible, not the legacy terminal).
	e.eventually("removed-suffix occurrence executed", func() bool { return exec.count() > 0 })
	if got := exec.count(); got != 1 {
		t.Fatalf("executor calls = %d so far, want 1", got)
	}
	// The fresh run completes: acked, terminal-retained, never DLQ'd.
	e.eventually("removed-suffix occurrence acked and terminal-retained", func() bool {
		_, ok := e.pending(id)
		return !ok && e.isRetained(id)
	})
	e.eventually("removed-suffix occurrence never DLQ'd", func() bool { return !e.inDlq(id) })
	if got := exec.count(); got != 1 {
		t.Fatalf("executor calls = %d, want exactly 1 (the legacy value was not a terminal skip)", got)
	}
	field, err := e.stateField(id)
	if err != nil {
		t.Fatalf("read terminal marker: %v", err)
	}
	if field != "ok" {
		t.Fatalf("terminal marker = %q, want the consumed fresh success (not the legacy value)", field)
	}
}

// TestIntegrationScheduleLegacyActiveMarkerStaysPending pins the active-wins
// precedence end to end: an app-absent occurrence whose hash carries an ACTIVE
// running marker AND a terminal "ok" marker stays PENDING — never ACKed, never
// DLQ'd, no reservation written — because the active attempt may still complete.
func TestIntegrationScheduleLegacyActiveMarkerStaysPending(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 5, 11, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	if err := e.hsetState(id, runningValue(time.Now().Add(time.Hour), 1)); err != nil {
		t.Fatalf("seed active marker: %v", err)
	}
	if err := e.client.HSet(context.Background(), e.invocationKey(id), scheduleFnName+"/other.handler", "ok").Err(); err != nil {
		t.Fatalf("seed terminal marker: %v", err)
	}
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	// Wait for the first delivery (the active marker keeps it pending), then
	// assert it stays pending and un-DLQed for a bounded window.
	e.eventually("legacy active occurrence delivered (in PEL)", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	e.staysPending(t, id, time.Second)
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	if n, err := e.client.HExists(context.Background(), e.invocationKey(id), reservedScheduleRejectionField).Result(); err != nil || n {
		t.Fatalf("rejection field present = (%v,%v), want absent (an active marker must win over a rejection)", n, err)
	}
}

// pinnedScheduleField mirrors the stream package's reserved scheduleField
// ("__schedule").
const pinnedScheduleField = "__schedule"

// seedPinnedTerminal seeds the CURRENT-VERSION crash window: a descriptor is
// pinned (with the app's real schedule/handler) AND the invocation carries a
// terminal marker, so an app-absent reserve-only delivery must resolve the marker
// rather than report a blanket "admitted".
func seedPinnedTerminal(t *testing.T, e *scheduleEnv, id, marker string) {
	t.Helper()
	desc := streamEncodeDescriptor(t, streamScheduleDescriptor{
		Schedule: scheduleHandler,
		Handler:  scheduleHandler,
		Timeout:  time.Minute,
		Retries:  4,
	})
	if err := e.client.HSet(context.Background(), e.invocationKey(id), pinnedScheduleField, desc).Err(); err != nil {
		t.Fatalf("seed pinned descriptor: %v", err)
	}
	if err := e.hsetState(id, marker); err != nil {
		t.Fatalf("seed terminal marker: %v", err)
	}
}

// TestIntegrationSchedulePinnedOKAcked pins the descriptor-pinned success crash
// window: a descriptor is pinned and the invocation is marked "ok", but the ACK
// never ran and the app is now absent. The occurrence must be ACKed and
// terminal-retained (never left PEL-pending forever), with no reservation
// overwriting the marker.
func TestIntegrationSchedulePinnedOKAcked(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	seedPinnedTerminal(t, e, id, "ok")
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	e.eventually("pinned ok occurrence acked and terminal-retained", func() bool {
		return e.isRetained(id)
	})
	if entries := listDLQEntriesFor(t, e.dlqStore, id); len(entries) != 0 {
		t.Fatalf("DLQ entries = %d, want 0 (a pinned ok is never dead-lettered)", len(entries))
	}
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	assertLegacyMarkerUntouched(t, e, id, "ok")
}

// TestIntegrationSchedulePinnedExhaustedMissingDLQDLQOnce pins the descriptor-
// pinned exhaustion crash window: a descriptor is pinned and the invocation is
// exhausted, but the DLQ row was never committed and the app is now absent. The
// occurrence must be dead-lettered exactly once (correct app/handler/attempt),
// with the marker left verbatim, and the message ACKed.
func TestIntegrationSchedulePinnedExhaustedMissingDLQDLQOnce(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	seedPinnedTerminal(t, e, id, "exhausted:2:deadbeef")
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	e.eventually("pinned exhausted occurrence DLQ'd", func() bool { return e.inDlq(id) })
	e.eventually("pinned exhausted occurrence acked (gone from PEL)", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	if entries := listDLQEntriesFor(t, e.dlqStore, id); len(entries) != 1 {
		t.Fatalf("DLQ entries for %s = %d, want exactly 1", id, len(entries))
	}
	m, ok := e.dlqGet()[id]
	if !ok {
		t.Fatalf("expected a DLQ entry for %s", id)
	}
	if m.App != scheduleFnName || m.Handler != scheduleHandler {
		t.Fatalf("DLQ app/handler = (%q,%q), want (%s,%s)",
			m.App, m.Handler, scheduleFnName, scheduleHandler)
	}
	if m.HandlerAttempts != 2 {
		t.Fatalf("HandlerAttempts = %d, want 2 (the marker's retained attempt)", m.HandlerAttempts)
	}
	// The exhausted marker carries no persistence state: it is left verbatim.
	assertLegacyMarkerUntouched(t, e, id, "exhausted:2:deadbeef")
}

// TestIntegrationSchedulePinnedActiveStaysPending pins the descriptor-pinned
// active branch: a descriptor is pinned and the invocation carries an ACTIVE
// running marker (the claim may still complete), so an app-absent delivery must
// keep the message PENDING — never ACK, never DLQ, no reservation.
func TestIntegrationSchedulePinnedActiveStaysPending(t *testing.T) {
	exec := &stateAwareExecutor{}
	r := registerScheduleFn(t, exec, 4)
	e := newScheduleEnv(t, r)
	o := scheduleOcc(time.Date(2026, 9, 6, 11, 0, 0, 0, time.UTC))
	id := e.xadd(o)
	seedPinnedTerminal(t, e, id, runningValue(time.Now().Add(time.Hour), 1))
	r.Registry().Replace(scheduleFnName, nil)
	e.start()

	e.eventually("pinned active occurrence delivered (in PEL)", func() bool {
		_, ok := e.pending(id)
		return ok
	})
	e.staysPending(t, id, time.Second)
	if got := exec.count(); got != 0 {
		t.Fatalf("executor calls = %d, want 0", got)
	}
	if n, err := e.client.HExists(context.Background(), e.invocationKey(id), reservedScheduleRejectionField).Result(); err != nil || n {
		t.Fatalf("rejection field present = (%v,%v), want absent (a pinned active marker must not be rejected)", n, err)
	}
}

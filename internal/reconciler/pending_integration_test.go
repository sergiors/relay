//go:build integration

// This file closes the gap between the reconciler's discovery/build window and
// the real event path. The unit tests in pending_test.go verify the registry's
// pending state and the runner's pending matching separately; the runner's
// integration test (internal/runner/event_lifecycle_integration_test.go)
// manually calls Registry.SetPending. This test instead drives the ACTUAL
// Reconciler.reconcileApp against a real Redis consumer/PEL using the SAME
// runner.Registry that runner.Handle matches against, so the discovery/build
// event-loss window is proven end to end:
//
//  1. A brand-new valid app on disk with no registry entry installs its desired
//     rules as PENDING and blocks in its first Prepare.
//  2. A matching event is delivered into the PEL, stays pending across a real
//     reclaim, is never DLQ'd, and executes nothing.
//  3. The first build fails: pending is retained, the event is still pending,
//     and still no attempt/execution/DLQ happened for unavailability.
//  4. The build is retried and succeeds; the atomic Replace installs the active
//     generation and clears pending, and a reclaim redelivery executes the
//     handler exactly once and ACKs the message (no DLQ).
//
// It requires real Redis (like the stream/runner integration suites) but no
// Docker: the builder is a test double. It is excluded from the default suite by
// the integration build tag.
package reconciler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/runner"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// discoveryLossEnv is a minimal, isolated Redis stream/group harness for the
// discovery/build loss-window test. It mirrors the runner package's eventEnv but
// lives here so the reconciler can drive its own reconcileApp.
type discoveryLossEnv struct {
	t      *testing.T
	cli    *redis.Client
	stream string
	group  string
	dlq    string
	ctx    context.Context
	cancel func()
	c      *stream.Consumer
	done   chan struct{}
}

// newDiscoveryLossEnv creates a unixnano-unique stream/group on the required
// Redis, creates the consumer group with MKSTREAM, and registers cleanup. It
// does NOT start consuming; the caller calls start.
func newDiscoveryLossEnv(t *testing.T) *discoveryLossEnv {
	t.Helper()
	cli := testutil.RequireRedis(t)

	cctx, cancel := context.WithCancel(context.Background())
	prefix := fmt.Sprintf("rec-loss-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	groupName := prefix + "-group"
	c := stream.NewConsumer(stream.ConsumerConfig{
		Client:          cli,
		Stream:          streamName,
		Group:           groupName,
		Consumer:        prefix + "-consumer",
		Log:             testutil.DiscardLogger(),
		MinPendingIdle:  250 * time.Millisecond,
		ReclaimInterval: 150 * time.Millisecond,
		Block:           150 * time.Millisecond,
	})
	if err := c.EnsureGroup(cctx); err != nil {
		cancel()
		t.Fatalf("ensure group: %v", err)
	}
	e := &discoveryLossEnv{
		t:      t,
		cli:    cli,
		stream: streamName,
		group:  groupName,
		dlq:    stream.DLQStreamFor(streamName),
		ctx:    cctx,
		cancel: cancel,
		c:      c,
	}
	t.Cleanup(e.cleanup)
	return e
}

func (e *discoveryLossEnv) start(handler stream.Handler) {
	e.t.Helper()
	e.done = make(chan struct{})
	go func() {
		defer close(e.done)
		_ = e.c.Consume(e.ctx, handler)
	}()
}

func (e *discoveryLossEnv) cleanup() {
	// Cancel FIRST, then join the consumer goroutine before deleting keys.
	e.cancel()
	if e.done != nil {
		select {
		case <-e.done:
		case <-time.After(5 * time.Second):
		}
	}
	ctx := context.Background()
	_ = e.cli.Del(ctx, e.stream, e.dlq).Err()
	// Best-effort removal of this env's invocation-state keys. The stream/group
	// names contain no ':' or '%', so a direct prefix scan is safe and bounded to
	// this env.
	var cursor uint64
	for {
		keys, next, err := e.cli.Scan(ctx, cursor, "relay:invocation:"+e.stream+":*", 100).Result()
		if err != nil {
			break
		}
		if len(keys) > 0 {
			_ = e.cli.Del(ctx, keys...).Err()
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

// xadd writes an event to this env's stream and returns its ID.
func (e *discoveryLossEnv) xadd(event string) string {
	e.t.Helper()
	id, err := e.cli.XAdd(context.Background(), &redis.XAddArgs{
		Stream: e.stream,
		Values: map[string]any{"event": event},
	}).Result()
	if err != nil {
		e.t.Fatalf("xadd: %v", err)
	}
	return id
}

// pendingRetry reports the PEL retry count and presence of a message.
func (e *discoveryLossEnv) pendingRetry(id string) (int64, bool) {
	entries, err := e.cli.XPendingExt(context.Background(), &redis.XPendingExtArgs{
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
		if pe.ID == id {
			return pe.RetryCount, true
		}
	}
	return 0, false
}

// hasDLQFor reports whether any DLQ entry carries the given original_id.
func (e *discoveryLossEnv) hasDLQFor(id string) bool {
	msgs, err := e.cli.XRange(context.Background(), e.dlq, "-", "+").Result()
	if err != nil {
		return false
	}
	for _, m := range msgs {
		if orig, _ := m.Values["original_id"].(string); orig == id {
			return true
		}
	}
	return false
}

// invocationKey returns the invocation-state hash key for a message. The
// stream/group names are controlled here (lowercase ASCII with dashes), so the
// stream package's percent-encoding is the identity and the key can be computed
// directly.
func (e *discoveryLossEnv) invocationKey(id string) string {
	return "relay:invocation:" + e.stream + ":" + e.group + ":" + id
}

// hasAttemptState reports whether the invocation field carries a lifecycle
// marker (an execution attempt) for the message. A pending-only match writes
// only the reserved classification field and never an invocation field, so this
// stays false while the app is not runnable.
func (e *discoveryLossEnv) hasAttemptState(id, invocation string) bool {
	exists, err := e.cli.HExists(context.Background(), e.invocationKey(id), invocation).Result()
	return err == nil && exists
}

// TestReconcileDiscoveryBuildLossWindowIntegration pins the discovery/build
// event-loss window end to end with a real Redis consumer/PEL and the actual
// Reconciler.reconcileApp sharing the runner's registry.
func TestReconcileDiscoveryBuildLossWindowIntegration(t *testing.T) {
	const appName = "loss-window"
	const handler = "handler.created"
	const invocation = appName + "/" + handler

	root := t.TempDir()
	tmpl := templateWithEvents("  - handler: " + handler + "\n    pattern:\n      event_name: [created]\n")
	writeTemplateDirFor(t, root, appName, tmpl)

	// The first Prepare blocks at the gate and fails once released; a later
	// Prepare (no gate armed) succeeds. The builder doubles as the executor, so
	// it counts handler executions too.
	b := &barrierBuilder{fail: true}

	// ONE registry, shared by the reconciler (publication) and the runner (the
	// delivery path Handle matches against), exactly as the worker wires them.
	runWorker := runner.NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	reg := runWorker.Registry()
	rec := New(
		Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour},
		reg,
		b,
		testutil.DiscardLogger(),
	)

	e := newDiscoveryLossEnv(t)
	e.start(runWorker.Handle)

	// 1. Start reconcileApp and wait for the builder gate: this proves the
	//    desired rules were installed as pending BEFORE the (blocked) build.
	entered, release := b.arm()
	recDone := runReconcile(rec, appName)
	waitGate(t, entered)
	if !reg.HasPending(appName) {
		t.Fatal("brand-new app must be exposed as pending before its build completes")
	}
	if pf := reg.GetByName(appName); pf != nil {
		t.Fatalf("brand-new app must not be published active before its build succeeds, got %v", pf)
	}

	// 2. A matching event is delivered into the PEL while the build is blocked.
	id := e.xadd(`{"event_name":"created"}`)
	testutil.WaitFor(t, 10*time.Second, "event delivered into the PEL", func() bool {
		_, ok := e.pendingRetry(id)
		return ok
	})
	// It must survive at least one REAL reclaim while blocked: the retry count
	// advances past the first (fresh) delivery.
	testutil.WaitFor(t, 10*time.Second, "event redelivered by reclaim while build is blocked", func() bool {
		retry, ok := e.pendingRetry(id)
		return ok && retry >= 2
	})
	if e.hasDLQFor(id) {
		t.Fatal("pending-desired event must never be DLQ'd for unavailability alone")
	}
	if got := b.executesCount(); got != 0 {
		t.Fatalf("pending generation must not execute a handler: executions = %d, want 0", got)
	}
	if e.hasAttemptState(id, invocation) {
		t.Fatal("a pending-only match must not claim a handler attempt")
	}

	// 3. Release the first build so it FAILS. Pending must be retained, the event
	//    must stay pending, and still no attempt/execution/DLQ may appear.
	close(release)
	waitDone(t, recDone)
	if !reg.HasPending(appName) {
		t.Fatal("a failed first build must retain the pending desired rules")
	}
	if pf := reg.GetByName(appName); pf != nil {
		t.Fatalf("a failed first build must not publish an active entry, got %v", pf)
	}
	if _, ok := e.pendingRetry(id); !ok {
		t.Fatal("event must remain pending after the failed first build")
	}
	if e.hasDLQFor(id) {
		t.Fatal("unavailability must not dead-letter the event")
	}
	if got := b.executesCount(); got != 0 {
		t.Fatalf("failed build must not execute a handler: executions = %d, want 0", got)
	}
	if e.hasAttemptState(id, invocation) {
		t.Fatal("unavailability must not claim a handler attempt")
	}

	// 4. Retry the build (now succeeding): the atomic Replace installs the active
	//    generation and clears pending in one locked step.
	b.setFail(false)
	rec.reconcileApp(appName)
	if pf := reg.GetByName(appName); pf == nil || pf.Prepared() == nil {
		t.Fatal("app must be published active after the successful build")
	}
	if reg.HasPending(appName) {
		t.Fatal("a successful build must clear the pending entry")
	}

	// 5. A reclaim redelivery now executes the handler exactly once and ACKs the
	//    message; it is never DLQ'd.
	testutil.WaitFor(t, 10*time.Second, "activated handler executed once", func() bool {
		return b.executesCount() == 1
	})
	testutil.WaitFor(t, 10*time.Second, "message acked (gone from PEL)", func() bool {
		_, ok := e.pendingRetry(id)
		return !ok
	})
	if e.hasDLQFor(id) {
		t.Fatal("activated event must never be DLQ'd")
	}
	if got := b.executesCount(); got != 1 {
		t.Fatalf("handler executions = %d, want exactly 1", got)
	}
}

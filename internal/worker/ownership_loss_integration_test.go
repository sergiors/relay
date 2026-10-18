//go:build integration

// This file proves the worker's end-to-end OWNERSHIP-LOSS path against real
// external services: a real PostgreSQL session advisory lock on WORKER_ID (the
// pinned lock the monitor verifies), real Docker execution (an in-flight handler
// container), and real Redis (PEL state, invocation keys). The pinned lock
// backend is terminated out from under the running worker; the real ownership
// monitor must latch the loss, clear readiness, and cancel the lifecycle, which
// cancels the in-flight handler exactly as a signal would. A replacement
// incarnation of the same WORKER_ID can then take the identity, and the pending
// message is recoverable through the normal reclaim path.
//
// This file is excluded from the default suite by the integration build tag.
// Running it (`go test -tags=integration ./...`) REQUIRES a reachable Docker
// daemon, Redis at REDIS_TEST_ADDR (default localhost:6379), and PostgreSQL at
// TEST_DATABASE_URL; a missing dependency fails the test rather than skipping
// it.
package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"relay/internal/state"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestIntegrationOwnershipLossMidHandler is the end-to-end ownership-loss test.
// A real worker (full wiring: real PostgreSQL WORKER_ID lock + real ownership
// monitor, real Consume loop, real Docker execution, real Redis PEL) is running
// an in-flight handler when its pinned lock backend is terminated from a separate
// connection. It asserts the loss is detected and acted on exactly as designed:
// the once-only loss action clears readiness and cancels the lifecycle, the
// in-flight handler is cancelled (its container removed), the message is left
// pending (not acked) with no terminal invocation state, a replacement
// incarnation of the same WORKER_ID reacquires the identity, and the message is
// recoverable through the normal reclaim path by a second consumer.
func TestIntegrationOwnershipLossMidHandler(t *testing.T) {
	testutil.RequireStateDatabase(t)
	testutil.RequireRedis(t)
	dcli := testutil.RequireDocker(t)

	redisURI := testutil.EnvOr("REDIS_TEST_ADDR", "localhost:6379")
	prefix := fmt.Sprintf("ownership-itest-%d", time.Now().UnixNano())
	streamName := prefix + "-stream"
	groupName := prefix + "-group"
	// The consumer name is the worker identity: it must be UNIQUE so the startup
	// orphan sweep and label filters never touch the real relay container
	// (hostname "relay-relay").
	consumerName := prefix + "-consumer"

	// Free metrics port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	fnRoot := t.TempDir()
	stateDSN := testutil.NewStateDSN(t)
	fnName := testutil.UniqueName(t, "ownership")

	// The handler sleeps 30s while the rule timeout is 25s, so the running
	// deadline (now+25s) comfortably exceeds the loss point — the handler is
	// genuinely mid-execution when the lock is terminated.
	fnDir := filepath.Join(fnRoot, fnName)
	if err := os.MkdirAll(fnDir, 0o755); err != nil {
		t.Fatalf("mkdir fn: %v", err)
	}
	writeTestFile(t, fnDir, "template.yaml", `
runtime: node24
events:
  - handler: index.slow
    pattern:
      event_name: [INSERT]
    timeout: 25s
`)
	writeTestFile(t, fnDir, "index.js", `
export async function slow(event) {
  console.log("slow started " + event.event_id);
  await new Promise(r => setTimeout(r, 30000));
  console.log("slow done");
}
`)

	env := startWorker(t, workerConfig{
		redisURI: redisURI, stream: streamName, group: groupName,
		consumerName: consumerName, fnRoot: fnRoot, stateDSN: stateDSN,
		metricsAddr: metricsAddr, docker: dcli,
	})

	// Ownership is healthy before anything else.
	ownerPID := env.st.OwnerBackendPID()
	if ownerPID <= 0 {
		t.Fatalf("owner backend pid = %d, want > 0", ownerPID)
	}
	if ready, reason := env.ready.Ready(context.Background()); !ready {
		t.Fatalf("worker must be ready before the loss (reason %q)", reason)
	}

	// XADD an event matching the rule.
	msgID := xadd(t, env.client, streamName, `{"event_id":"evt_ownership","event_name":"INSERT"}`)

	// Synchronize on the handler actually executing.
	containerID := waitForContainer(t, dcli, map[string]string{
		"relay.app":      fnName,
		"relay.hostname": consumerName,
	})
	t.Logf("in-flight container %s observed", containerID)

	// The message is in the PEL (delivered to the worker's consumer) with retry
	// count 1.
	testutil.WaitFor(t, 8*time.Second, "message in PEL", func() bool {
		_, ok := pendingEntry(env.client, streamName, groupName, msgID)
		return ok
	})
	if pe, ok := pendingEntry(env.client, streamName, groupName, msgID); !ok || pe.RetryCount != 1 {
		t.Fatalf("pending entry = %+v, ok=%v; want retry count 1", pe, ok)
	}

	// Terminate the pinned WORKER_ID lock backend from a SEPARATE connection:
	// the residual "lock lost while the process runs" case. pg_terminate_backend
	// kills the session, so the session advisory lock dies with it and the
	// monitor's next pinned-connection verification fails closed.
	admin, err := sql.Open("pgx", stateDSN)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	if _, err := admin.ExecContext(termCtx, `SELECT pg_terminate_backend($1)`, ownerPID); err != nil {
		t.Fatalf("terminate owner backend: %v", err)
	}

	// The real ownership monitor must detect the loss and latch it, clearing
	// readiness and cancelling the lifecycle (the ownershipLossAction).
	select {
	case <-env.monitorDone:
	case <-time.After(15 * time.Second):
		t.Fatal("ownership monitor did not detect the terminated lock backend")
	}
	if !env.loss.isLost() {
		t.Fatal("monitor must latch the ownership loss")
	}
	if env.ctx.Err() == nil {
		t.Fatal("the loss action must cancel the lifecycle")
	}
	if ready, reason := env.ready.Ready(context.Background()); ready || reason != "worker ownership lost" {
		t.Fatalf("readiness after loss = (%v, %q), want not-ready with the loss reason", ready, reason)
	}

	// Consume returns because the lifecycle was cancelled (the in-flight handler
	// was cancelled mid-execution).
	var consumeErr error
	select {
	case consumeErr = <-env.consumeDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Consume did not return within 15s of the ownership loss")
	}
	if consumeErr != nil {
		t.Fatalf("consume returned error: %v", consumeErr)
	}

	// The cancelled in-flight handler's container is gone (the cancellation
	// kills it, plus the deferred best-effort remove / AutoRemove).
	if !waitForContainerGone(t, dcli, map[string]string{
		"relay.app":      fnName,
		"relay.hostname": consumerName,
	}) {
		t.Error("in-flight container should have been removed after the ownership loss")
	}

	// The message is NOT acked: still present in the PEL.
	if _, ok := pendingEntry(env.client, streamName, groupName, msgID); !ok {
		t.Fatal("message should still be pending (not acked) after the ownership loss")
	}

	// Invocation state is NOT terminal success. The real production transition
	// path is Consume -> runner.Handle -> TryStart (a confirmed claim is
	// persisted BEFORE the handler container is created), then the lifecycle
	// cancellation lands mid-execution. recordFailure's cancellation guard
	// (lifecycleCancelled) suppresses both the retry and exhaustion transitions,
	// so the persisted value is the recoverable running marker
	// "running:<deadline_ms>:<attempt>:<token>" — never "ok", and never a
	// next_attempt_at/exhausted marker. The field must EXIST: the confirmed
	// TryStart wrote it and the pending message was never ACKed, so the state
	// hash is left intact (the stream layer only clears it after a terminal
	// ACK).
	invKey := "relay:invocation:" + streamName + ":" + groupName + ":" + msgID
	invValue, err := env.client.HGet(context.Background(), invKey, fnName+"/index.slow").Result()
	if err != nil {
		t.Fatalf("invocation state field %q missing after ownership loss: %v", fnName+"/index.slow", err)
	}
	if invValue == "ok" {
		t.Fatal("invocation state should NOT be 'ok' after an ownership loss")
	}
	if !strings.HasPrefix(invValue, "running:") {
		t.Fatalf("invocation state = %q, want a recoverable running:<deadline_ms>:<attempt>:<token> marker", invValue)
	}

	// The canceled event is NOT dead-lettered: the shutdown-cancel disposition is
	// a pending skip (no retry/exhaustion transition), so no DLQ entry exists for
	// the original message.
	dlqMsgs, err := env.client.XRange(context.Background(), stream.DLQStreamFor(streamName), "-", "+").Result()
	if err != nil {
		t.Fatalf("read dead-letter stream: %v", err)
	}
	for _, m := range dlqMsgs {
		if oid, ok := m.Values["original_id"].(string); ok && oid == msgID {
			t.Fatalf("canceled event %s was dead-lettered; want no DLQ entry", msgID)
		}
	}

	// The monitor logged the structured loss line, and the stream layer logged
	// the shutdown-cancel path (the loss cancels the lifecycle, which the message
	// loop observes exactly as it observes SIGTERM).
	if !strings.Contains(env.buf.String(), "Worker ownership lost") {
		t.Error("expected the structured 'Worker ownership lost' log line")
	}
	if !strings.Contains(env.buf.String(), "handler canceled during shutdown; leaving pending") {
		t.Error("expected 'Message ...: handler canceled during shutdown; leaving pending' log line")
	}

	// A replacement incarnation of the SAME WORKER_ID (a different per-process
	// instance token) can take the identity, because the terminated backend
	// released the session advisory lock.
	replacement, err := state.Open(context.Background(), state.Options{
		DSN:      env.stateDSN,
		Owner:    env.workerID,
		Instance: "replacement-instance",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("replacement state open: %v", err)
	}
	defer func() { _ = replacement.Close() }()
	if err := replacement.VerifyOwnerLock(context.Background()); err != nil {
		t.Fatalf("replacement VerifyOwnerLock = %v, want nil", err)
	}

	// Recovery: a SECOND consumer (new name, same stream/group) reclaims the
	// idle pending message and acks it, proving the message is recoverable
	// through the normal reclaim path after the loss.
	//
	// No test-only DEL is needed: the cancellation leaves the running marker
	// "running:<deadline_ms>:<attempt>:<token>" (now+25s), but per-invocation
	// execution eligibility is decided by the RUNNER's TryStart, and consumer B
	// here delivers to a plain callback that never claims the invocation. So the
	// running deadline does not gate B's normal XAUTOCLAIM/reclaim, and the test
	// exercises the real recovery path (a second consumer reclaims and acks)
	// without test-only mutation of invocation state: B's production ACK does
	// transition it to terminal retention.

	var bCalls atomic.Int64
	bCtx, bCancel := context.WithCancel(context.Background())
	bConsumer := stream.NewConsumer(stream.ConsumerConfig{
		Client:   env.client,
		Stream:   streamName,
		Group:    groupName,
		Consumer: consumerName + "-b",
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Match the harness (shutdown_integration_test.go): a short XREADGROUP
		// BLOCK so the stop below is bounded well inside its 5s wait. The
		// production default is 5s and go-redis's blocking read is not
		// interrupted by ctx cancellation, so a default block could make the
		// stop bound race the block expiry.
		Block:           250 * time.Millisecond,
		MinPendingIdle:  300 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
	})
	if err := bConsumer.EnsureGroup(bCtx); err != nil {
		t.Fatalf("ensure group B: %v", err)
	}
	bDone := make(chan error, 1)
	go func() {
		bDone <- bConsumer.Consume(bCtx, func(ctx context.Context, mid string, ev map[string]any) error {
			if mid == msgID {
				bCalls.Add(1)
			}
			return nil
		})
	}()

	testutil.WaitFor(t, 8*time.Second, "message reclaimed and acked by consumer B", func() bool {
		_, ok := pendingEntry(env.client, streamName, groupName, msgID)
		return !ok
	})
	if got := bCalls.Load(); got != 1 {
		t.Fatalf("consumer B handler ran %d times, want exactly 1", got)
	}

	// Stop consumer B and clean up the stream/DLQ/invocation keys.
	bCancel()
	select {
	case <-bDone:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer B did not stop")
	}
	_ = env.client.Del(context.Background(), streamName, stream.DLQStreamFor(streamName), invKey).Err()
}

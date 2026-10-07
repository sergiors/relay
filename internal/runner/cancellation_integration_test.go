//go:build integration

// End-to-end F-003 coverage: the real runner drives a worker shutdown against
// real Redis. Requires Redis at REDIS_TEST_ADDR (default localhost:6379).
package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"relay/internal/stream"
	"relay/internal/testutil"
)

// TestIntegrationEventShutdownCancellationLeavesRecoverablePending drives the
// real runner (not a stand-in handler) through a worker shutdown against real
// Redis, locking F-003's end-to-end disposition: a handler already running when
// the lifecycle context is canceled leaves the message pending with a
// recoverable running marker and NO retry backoff / exhausted marker, and a
// fresh consumer in the same group reclaims it and completes the invocation
// (attempt 2), proving the shutdown consumed no retry budget.
//
// Caveat on regression value: with the real ctx-bound invocation-state store, a
// post-cancel Redis transition usually fails on the canceled context anyway, so
// this test also passes before the explicit cancellation branch. The
// DETERMINISTIC regression for the race is the runner unit test
// TestRunnerCancellationDuringExecuteNoRetryOrExhaustionTransition, whose
// in-memory store ignores ctx and therefore models a script that commits despite
// cancellation.
// This test remains the end-to-end behavior lock (real PEL, real marker shape,
// real reclaim).
func TestIntegrationEventShutdownCancellationLeavesRecoverablePending(t *testing.T) {
	_ = redisAvailable(t)
	e := newEventEnv(t)
	id := e.xadd(`{"a":1}`)

	// The executor blocks until the invocation context is canceled, then returns
	// the cancellation error — exactly what the runtime does on shutdown.
	exec := newBlockingExecutor(make(chan struct{}))
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 4, exec)}, testutil.DiscardLogger(), nil)
	r.SetMaxConcurrentInvocations(1)
	e.start(r.Handle)

	exec.waitEntered() // the claim is confirmed and the handler is running (PEL)
	e.cancel()         // worker shutdown
	select {
	case <-e.done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop on cancellation")
	}
	if err := <-e.errCh; err != nil {
		t.Fatalf("consume returned error: %v", err)
	}

	// The message is pending (no ACK) and not dead-lettered, and the marker is a
	// recoverable running marker — NOT a retry backoff or exhaustion.
	if _, ok := e.pending(id); !ok {
		t.Fatalf("message %s not pending after shutdown; want pending (no ACK/DLQ)", id)
	}
	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("message %s dead-lettered on shutdown; want pending", id)
	}
	v, err := e.stateField(id, "fn/index.run")
	if err != nil {
		t.Fatalf("state field after shutdown: %v", err)
	}
	if !strings.HasPrefix(v, "running:") {
		t.Fatalf("invocation marker = %q, want a running:<deadline> marker (shutdown must make no retry/exhaustion transition)", v)
	}

	// A fresh consumer in the same group reclaims the message after the running
	// deadline elapses and completes attempt 2.
	c2 := stream.NewConsumer(stream.ConsumerConfig{
		Client:          e.client,
		Stream:          e.stream,
		Group:           e.group,
		Consumer:        "reclaim",
		Log:             testutil.DiscardLogger(),
		MinPendingIdle:  100 * time.Millisecond,
		ReclaimInterval: 100 * time.Millisecond,
		Block:           200 * time.Millisecond,
	})
	if err := c2.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group for reclaim consumer: %v", err)
	}
	reclaimExec := &countingExecutor{}
	r2 := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 4, reclaimExec)}, testutil.DiscardLogger(), nil)
	r2.SetMaxConcurrentInvocations(1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_ = c2.Consume(ctx2, r2.Handle)
	}()
	testutil.WaitFor(t, 10*time.Second, "message reclaimed and ACKed after the canceled attempt's deadline", func() bool {
		_, ok := e.pending(id)
		return !ok
	})
	cancel2()
	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim consumer did not stop")
	}

	if _, ok := e.dlqEntry(id); ok {
		t.Fatalf("message %s was dead-lettered after reclaim; want completed", id)
	}
	if got := reclaimExec.count(); got != 1 {
		t.Fatalf("reclaim handler executions = %d, want 1 (the shutdown must not have exhausted the invocation)", got)
	}
}

//go:build integration

// This file pins the capacity and fairness behavior of the Redis consumer under
// load (F-004/F-005). Both findings are confirmed as non-lossy but weak
// operational guarantees; these tests establish the actual behavior against a
// real Redis:
//
//   - F-004: execution-slot saturation does NOT stop reads while local buffer
//     space remains, so already-delivered messages sit pending in the PEL (not
//     unread in the stream, not lost) and drain once capacity frees.
//   - F-005: the reclaim cursor makes progress through a backlog owned by a
//     "disappeared" consumer even ahead of a slow handler, and multiple group
//     consumers plus fresh arrivals are all eventually processed (no message
//     lost, no per-message starvation beyond reclaimed head-of-line delay).
//
// It requires Redis at REDIS_TEST_ADDR (default localhost:6379), matching the
// other stream integration tests.
package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/testutil"
)

// TestIntegrationExecutionSaturationDeliversToPELThenDrains pins F-004. A
// handler that executes at most one at a time (a stand-in for a saturated runner
// execution slot) is combined with a buffer (10) larger than the workload (6),
// so the consumer reads every message into the PEL although only one executes.
// The messages are proven pending (not ACKed, not unread in the stream) while
// saturated, then drain and ACK once the slot frees — no loss, bounded by the
// buffer.
func TestIntegrationExecutionSaturationDeliversToPELThenDrains(t *testing.T) {
	testutil.RequireRedis(t)
	const n = 6
	e := newEnv(t, ConsumerConfig{MaxBufferedEvents: 10, Count: 10})

	// Execute at most one handler at a time; block it until release, so execution
	// is saturated while buffer space remains.
	sem := make(chan struct{}, 1)
	release := make(chan struct{})
	var (
		inExec  atomic.Int64
		peak    atomic.Int64
		entered = make(chan struct{})
		once    sync.Once
	)
	bump := func(cur int64) {
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				return
			}
		}
	}
	e.start(func(_ context.Context, _ string, _ map[string]any) error {
		sem <- struct{}{}
		cur := inExec.Add(1)
		bump(cur)
		defer func() {
			inExec.Add(-1)
			<-sem
		}()
		once.Do(func() { close(entered) })
		<-release
		return nil
	})

	for i := 0; i < n; i++ {
		e.xadd(t, fmt.Sprintf(`{"i":%d}`, i))
	}

	// Every message is delivered into the PEL (execution saturation does not stop
	// reads while buffer space remains).
	testutil.WaitFor(t, 8*time.Second, "all messages delivered into the PEL", func() bool {
		return len(e.pending()) == n
	})
	<-entered // the single executing handler is in flight

	// The backlog is in the PEL, not still unread: XLEN - delivered == 0.
	length, err := e.client.XLen(context.Background(), e.stream).Result()
	if err != nil {
		t.Fatalf("xlen: %v", err)
	}
	if unread := length - int64(len(e.pending())); unread != 0 {
		t.Fatalf("unread stream entries = %d, want 0 (the batch was delivered into the PEL)", unread)
	}
	// Execution concurrency is bounded to one, and nothing was ACKed.
	waitSustained(t, "all pending while execution is saturated at 1", 300*time.Millisecond, func() bool {
		return len(e.pending()) == n && peak.Load() <= 1
	})
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak executing handlers = %d, want 1 (slot saturated)", got)
	}

	// Release: every delivered message drains and is ACKed.
	close(release)
	testutil.WaitFor(t, 8*time.Second, "PEL drains after capacity frees", func() bool {
		return len(e.pending()) == 0
	})
	e.stop(t)
}

// seedGhostBacklog adds n messages and reads them into the group PEL under a
// distinct "ghost" consumer that never ACKs, so the real consumer must reclaim
// them (a disappeared consumer's backlog).
func seedGhostBacklog(t *testing.T, e *testEnv, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		e.xadd(t, fmt.Sprintf(`{"ghost":%d}`, i))
	}
	res, err := e.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    e.group,
		Consumer: "ghost",
		Streams:  []string{e.stream, ">"},
		Count:    int64(n),
	}).Result()
	if err != nil {
		t.Fatalf("ghost XReadGroup: %v", err)
	}
	var got int
	for _, s := range res {
		got += len(s.Messages)
	}
	if got != n {
		t.Fatalf("ghost read %d messages, want %d", got, n)
	}
	if p := len(e.pending()); p != n {
		t.Fatalf("ghost backlog pending = %d, want %d", p, n)
	}
}

// TestIntegrationReclaimDrainsGhostBacklogAheadOfSlowHandler pins F-005's
// reclaim-progress half. A small Count forces the XAUTOCLAIM cursor to walk in
// multiple steps. The first reclaimed delivery blocks (the synchronous reclaim
// goroutine is held), so the rest cannot be delivered until it returns; after
// release the cursor must still walk the whole backlog and ACK every message.
func TestIntegrationReclaimDrainsGhostBacklogAheadOfSlowHandler(t *testing.T) {
	testutil.RequireRedis(t)
	const n = 7
	e := newEnv(t, ConsumerConfig{Count: 2})
	seedGhostBacklog(t, e, n)

	first := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	e.start(func(_ context.Context, _ string, _ map[string]any) error {
		if calls.Add(1) == 1 {
			close(first)
			<-release
		}
		return nil
	})
	<-first // the first reclaimed handler is mid-flight

	// The held reclaim goroutine cannot deliver the rest, but nothing may be
	// ACKed or lost: the whole backlog stays pending.
	waitSustained(t, "ghost backlog held pending behind the slow handler", 300*time.Millisecond, func() bool {
		return len(e.pending()) == n
	})

	// Release; the reclaim cursor must make progress through the whole backlog.
	close(release)
	testutil.WaitFor(t, 10*time.Second, "ghost backlog fully reclaimed and ACKed", func() bool {
		return len(e.pending()) == 0
	})
	e.stop(t)
	if got := calls.Load(); got != n {
		t.Fatalf("handler invocations = %d, want %d (each reclaimed message exactly once)", got, n)
	}
}

// TestIntegrationReclaimFreshArrivalsAndTwoConsumers pins F-005's multi-consumer
// and concurrency half: two consumers share one group with reclaim enabled while
// new entries arrive, and a ghost backlog must be reclaimed. Every message (ghost
// plus fresh) is eventually delivered exactly once across both consumers and
// ACKed, exercising XREADGROUP concurrently with the reclaim loop.
func TestIntegrationReclaimFreshArrivalsAndTwoConsumers(t *testing.T) {
	testutil.RequireRedis(t)
	prefix := fmt.Sprintf("multi-%d", time.Now().UnixNano())
	streamName, groupName := prefix+"-stream", prefix+"-group"

	envA := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "multi-A", Count: 2})
	const ghost, fresh = 4, 6
	seedGhostBacklog(t, envA, ghost)

	// A shared, mutex-guarded delivery log proves exactly-once delivery of each
	// distinct message across both consumers (the stream/group guarantee).
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	handler := func(_ context.Context, msgID string, _ map[string]any) error {
		mu.Lock()
		seen[msgID]++
		mu.Unlock()
		return nil
	}
	envA.start(handler)

	envB := newEnv(t, ConsumerConfig{Stream: streamName, Group: groupName, Consumer: "multi-B", Count: 2})
	envB.start(handler)

	// Fresh entries arrive while both consumers read and reclaim concurrently.
	for i := 0; i < fresh; i++ {
		envB.xadd(t, fmt.Sprintf(`{"fresh":%d}`, i))
	}

	testutil.WaitFor(t, 12*time.Second, "both consumers drain the shared group PEL", func() bool {
		return len(envA.pending()) == 0 && len(envB.pending()) == 0
	})
	envA.stop(t)
	envB.stop(t)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != ghost+fresh {
		t.Fatalf("distinct messages delivered = %d, want %d", len(seen), ghost+fresh)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s delivered %d times, want exactly 1", id, n)
		}
	}
}

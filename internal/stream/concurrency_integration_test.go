//go:build integration

// This file exercises the consumer's bounded local buffer (MAX_BUFFERED_EVENTS)
// backpressure end to end against a real Redis server.
//
// It requires Redis at REDIS_TEST_ADDR (default localhost:6379), matching the
// other stream integration tests; it is excluded from the default suite by the
// integration build tag.
package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegrationBufferFullPausesReading verifies the core backpressure
// contract: with a small MaxBufferedEvents buffer and a handler that blocks,
// the consumer stops reading from Redis once the local buffer is full, so the
// unwritten backlog STAYS IN THE STREAM (never read into the PEL) and the
// handler is never invoked more than the buffer limit at once. When the
// handlers unblock, reading resumes and every message is eventually ACKed (the
// PEL empties and the whole stream is consumed).
func TestIntegrationBufferFullPausesReading(t *testing.T) {
	requireRedis(t)
	e := newEnv(t, ConsumerConfig{MaxBufferedEvents: 2, Count: 10})
	const n = 5

	// Handler blocks until release closes, then succeeds (→ ACK). It tracks its
	// concurrent occupancy so the test can assert the buffer cap is never
	// exceeded, and counts total completions so the test can assert that ALL
	// messages were eventually consumed after unblocking.
	var (
		occMu     sync.Mutex
		occupied  int
		peak      atomic.Int64
		completed atomic.Int64
	)
	release := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		occMu.Lock()
		occupied++
		if int64(occupied) > peak.Load() {
			peak.Store(int64(occupied))
		}
		occMu.Unlock()
		<-release
		occMu.Lock()
		occupied--
		occMu.Unlock()
		completed.Add(1)
		return nil
	})

	// unread returns how many stream entries have NOT been read by the group
	// yet: XLEN minus everything that ever entered the PEL (delivered). While
	// the buffer is saturated this must stay above zero — the consumer stopped
	// reading instead of accumulating local work.
	unread := func() int64 {
		length, err := e.client.XLen(context.Background(), e.stream).Result()
		if err != nil {
			return -1
		}
		return length - int64(len(e.pending()))
	}

	// Add n events; with a 2-slot buffer, the consumer reads at most 2 before
	// pausing, leaving the rest unread in the stream.
	for i := 0; i < n; i++ {
		e.xadd(t, fmt.Sprintf(`{"i":%d}`, i))
	}

	// Wait until the buffer has saturated: 2 (the capacity) handlers in flight.
	WaitFor(t, 8*time.Second, "buffer full: 2 handlers in flight", func() bool {
		occMu.Lock()
		cur := occupied
		occMu.Unlock()
		return cur >= 2
	})

	// While the buffer is full, the consumer must NOT read further: at most 2
	// messages are in the PEL (held locally, in flight) and the rest stay
	// unread in the stream. The in-flight occupancy never exceeds capacity.
	WaitFor(t, 8*time.Second, "reading paused: unread backlog stays in stream", func() bool {
		return unread() >= int64(n-2) && len(e.pending()) <= 2
	})
	// Sustained, so a transient over-read cannot pass: the paused state holds.
	waitSustained(t, "paused state (≤2 pending, ≥3 unread)", 500*time.Millisecond, func() bool {
		occMu.Lock()
		defer occMu.Unlock()
		return occupied <= 2 && peak.Load() <= 2 && unread() >= int64(n-2) && len(e.pending()) <= 2
	})

	// Unblock the handlers; reading resumes and every message is consumed and
	// ACKed: the PEL empties and all n handlers completed.
	close(release)
	WaitFor(t, 8*time.Second, "all messages acked (PEL empty)", func() bool {
		return len(e.pending()) == 0
	})
	WaitFor(t, 8*time.Second, "all 5 messages completed", func() bool {
		return completed.Load() >= n
	})
	e.stop(t)

	if got := peak.Load(); got > 2 {
		t.Fatalf("peak in-flight handlers = %d, want <= buffer capacity 2", got)
	}
}

// TestIntegrationReclaimSkipsWhenBufferFull verifies that the reclaim path
// never blocks on the buffer: with a saturated local buffer, reclaimed idle
// messages are left pending and replayed on a later tick once capacity frees,
// instead of growing local work beyond the buffer limit.
func TestIntegrationReclaimSkipsWhenBufferFull(t *testing.T) {
	requireRedis(t)
	// Fast reclaim cadence, tiny idle threshold: reclaimed quickly while the
	// buffer is saturated.
	e := newEnv(t, ConsumerConfig{MaxBufferedEvents: 1})
	const n = 3

	// Handler 1 blocks holding the only buffer slot; the other messages stay in
	// the stream unread.
	block := make(chan struct{})
	release := make(chan struct{})
	e.start(func(ctx context.Context, msgID string, ev map[string]any) error {
		select {
		case <-block:
			<-release
		case <-release:
		}
		return nil
	})

	id := e.xadd(t, `{"i":0}`)
	e.waitDelivered(t, id)

	// While the sole slot is held, reclaim must not add a second in-flight
	// handler: saturate the buffer and let the reclaim loop tick several times.
	close(block)
	// The consumer holds the single slot until release; give the reclaim loop a
	// few ticks while the buffer stays full, asserting occupancy stays at 1.
	waitSustained(t, "buffer stays saturated (1 in-flight handler)", 500*time.Millisecond, func() bool {
		return e.consumer.buffer.inflight() == 1
	})

	// Release; the buffered message ACKs, capacity frees, and the reclaim loop
	// is then free to continue (no message was lost or double-run beyond the
	// buffer's bound). The PEL drains.
	close(release)
	WaitFor(t, 8*time.Second, "message acked after release", func() bool {
		_, ok := e.pending()[id]
		return !ok
	})
	e.stop(t)
}
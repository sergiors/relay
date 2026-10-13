//go:build integration

// End-to-end F-004 coverage with a REAL runner and a REAL Redis consumer: the
// runner's own MAX_CONCURRENT_INVOCATIONS execution slot is saturated and the
// stream layer keeps reading while local buffer space remains, so the extra
// events are delivered into the PEL and drain once capacity frees. Requires
// Redis at REDIS_TEST_ADDR (default localhost:6379).
package runner

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

// runnerSlotSaturationFn builds a prepared app whose single rule matches any
// event, carries a long timeout (so a held execution stays held until the test
// releases it), and runs one invocation at a time (template concurrency 1).
func runnerSlotSaturationFn(t *testing.T, name string, exec Executor) *PreparedApp {
	t.Helper()
	rule := alwaysMatchRule(time.Minute)
	rule.Retries = 0
	return buildFn(fnSpec{name: name, concurrency: 1, rules: []app.EventRule{rule}}, exec)
}

// pendingCount reports how many of the given message IDs are currently in the
// group PEL.
func (e *eventEnv) pendingCount(ids []string) int {
	count := 0
	for _, id := range ids {
		if _, ok := e.pending(id); ok {
			count++
		}
	}
	return count
}

// TestIntegrationRunnerSlotSaturationDeliversToPELThenDrains pins F-004's
// end-to-end disposition through the REAL runner and a REAL consumer: with the
// runner's global execution slot saturated (MAX_CONCURRENT_INVOCATIONS = 1) and
// buffer space remaining, stream reads do NOT stop, so the additional events are
// DELIVERED into the PEL. Their slot wait fails IMMEDIATELY (slotWait zero), so
// they are rejected BEFORE any claim and left pending with NO ACK/DLQ. Because
// the rejection is proven to have completed while the slot is still held, no
// waiter can be left blocked to win the slot the moment it frees; releasing the
// holding execution therefore frees the slot for the RECLAIM loop to re-deliver
// the pending events, which now execute and ACK. This complements the
// stream-only handler-semaphore test
// (TestIntegrationExecutionSaturationDeliversToPELThenDrains) by driving the
// runner's own semaphore rather than a test handler's.
func TestIntegrationRunnerSlotSaturationDeliversToPELThenDrains(t *testing.T) {
	_ = redisAvailable(t)
	e := newEventEnv(t)

	const n = 4
	ids := make([]string, n)
	for i := range ids {
		ids[i] = e.xadd(fmt.Sprintf(`{"i":%d}`, i))
	}

	release := make(chan struct{})
	exec := newBlockingExecutor(release)
	m := metrics.New()
	r := NewWithMetrics([]*PreparedApp{runnerSlotSaturationFn(t, "fn", exec)}, testutil.DiscardLogger(), m)
	r.SetMaxConcurrentInvocations(1) // MAX_CONCURRENT_INVOCATIONS = 1
	// A zero slot wait makes the pre-claim rejection deterministic: while the
	// first slot is held, the semaphore's non-blocking probe misses and the
	// fallback select's zero-duration timer is already expired, so reserveSlots
	// returns at once. No saturated delivery is left blocked and able to acquire
	// the slot the instant it is released (which would let the drain below pass
	// without the reclaim loop). Zero is also well below MinPendingIdle (300ms),
	// so a rejected delivery cannot be reclaimed into a tight loop.
	r.slotWait = 0

	e.start(r.Handle)
	exec.waitEntered() // the sole global+per-app slot is now held by one event

	// Execution-slot saturation does not stop reads: every event is delivered
	// into the PEL while buffer space remains.
	e.eventually("all events delivered into the PEL while saturated", func() bool {
		return e.pendingCount(ids) == n
	})

	// Every non-holding delivery has now run and been rejected BEFORE any claim.
	// reserveSlots increments concurrency_waits only after its (immediate) slot
	// wait returns, so observing n-1 of them while the slot is still held proves
	// each saturated delivery timed out pre-claim — not merely that it has not
	// run yet. With that settled, the drain after release can only come from the
	// reclaim loop.
	e.eventually("all saturated deliveries rejected before claim", func() bool {
		return m.Counter(metrics.MetricConcurrencyWaits) >= int64(n-1)
	})

	// While saturated: exactly one invocation is claimed and executing, the rest
	// are pending with NO claimed attempt, nothing ACKed, and nothing DLQ'd.
	if got := exec.callCount(); got != 1 {
		t.Fatalf("executor calls while saturated = %d, want 1 (the holding invocation)", got)
	}
	var claimed int
	for _, id := range ids {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("event %s not pending while saturated; want pending (no ACK)", id)
		}
		if entries := e.dlqEntries(id); len(entries) != 0 {
			t.Fatalf("event %s dead-lettered while saturated; want pending", id)
		}
		v, err := e.stateField(id, "fn/index.run")
		if err == nil && strings.HasPrefix(v, "running:") {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed (running) invocations while saturated = %d, want exactly 1", claimed)
	}

	// Release capacity: the holding invocation completes and ACKs, freeing the
	// slot. No waiter is left blocked (proven above), so the pending events are
	// only re-delivered by the reclaim loop, which executes and ACKs them.
	close(release)
	e.eventually("all events reclaimed, executed, and ACKed after capacity frees", func() bool {
		return e.pendingCount(ids) == 0
	})
	if got := exec.callCount(); got != n {
		t.Fatalf("total executor calls = %d, want %d (each event executed exactly once)", got, n)
	}
	for _, id := range ids {
		if entries := e.dlqEntries(id); len(entries) != 0 {
			t.Fatalf("event %s was dead-lettered; want completed", id)
		}
	}
}

// TestIntegrationCumulativePELExceedsLocalBuffer pins the distinction the
// "Concurrency and backpressure" note in docs/configuration.md makes: the local
// buffer bound is NOT a bound on the consumer-group PEL. With a tiny local buffer
// (2) and more messages than that, a single held execution slot, and a zero slot
// wait, the consumer keeps reading. Every `XREADGROUP`-returned message is added
// to the PEL before it executes; a delivery that cannot claim the slot is
// rejected pre-claim (no attempt, no ACK, no DLQ), releases its local processing
// slot, and lets the next read proceed. The group PEL therefore accumulates past
// the local buffer while the buffered_events gauge stays at or below the
// configured capacity. Releasing the execution then lets the reclaim loop deliver
// the pending backlog, each message executing exactly once.
func TestIntegrationCumulativePELExceedsLocalBuffer(t *testing.T) {
	_ = redisAvailable(t)
	const (
		buffer = 2
		n      = 6
	)
	if n <= buffer {
		t.Fatalf("test precondition: n=%d must exceed the local buffer=%d", n, buffer)
	}
	m := metrics.New()
	e := newEventEnvWith(t, buffer, m)

	ids := make([]string, n)
	for i := range ids {
		ids[i] = e.xadd(fmt.Sprintf(`{"i":%d}`, i))
	}

	release := make(chan struct{})
	exec := newBlockingExecutor(release)
	r := NewWithMetrics([]*PreparedApp{runnerSlotSaturationFn(t, "fn", exec)}, testutil.DiscardLogger(), m)
	r.SetMaxConcurrentInvocations(1) // MAX_CONCURRENT_INVOCATIONS = 1
	// A zero slot wait makes the pre-claim rejection deterministic: while the
	// first slot is held, every other delivery's reserveSlots returns at once, so
	// none is left blocked and able to win the slot the instant it frees (which
	// would let the drain below pass without the reclaim loop). Zero is also well
	// below MinPendingIdle, so a rejected delivery cannot be reclaimed into a
	// tight loop.
	r.slotWait = 0

	e.start(r.Handle)
	exec.waitEntered() // the sole execution slot is now held by one delivery

	// Execution-slot saturation does not stop reads: every message is delivered
	// into the group PEL even though only one execution slot exists. The local
	// buffer cannot hold them all — each rejected delivery releases its slot and
	// the next read proceeds — so the cumulative PEL grows past the buffer.
	e.eventually("all events delivered into the PEL while saturated", func() bool {
		return e.pendingCount(ids) == n
	})

	// Every non-holding delivery has now been rejected BEFORE any claim.
	// reserveSlots increments concurrency_waits only after its (immediate) slot
	// wait returns, so observing n-1 of them while the slot is still held proves
	// each saturated delivery timed out pre-claim, not merely that it has not run
	// yet. With that settled, the drain after release can only come from reclaim.
	e.eventually("all saturated deliveries rejected before claim", func() bool {
		return m.Counter(metrics.MetricConcurrencyWaits) >= int64(n-1)
	})

	// The group PEL exceeds the local buffer, yet the local buffer gauge never
	// exceeds its configured capacity: the local bound is not a PEL bound.
	waitSustained(t, "local buffer gauge within capacity while the group PEL exceeds it", 300*time.Millisecond, func() bool {
		return e.pendingCount(ids) == n && m.Gauge(metrics.MetricBufferedEvents) <= buffer
	})

	// While saturated: exactly one invocation is claimed and executing, the rest
	// are pending with NO claimed attempt, nothing ACKed, and nothing DLQ'd. The
	// holding message is the ONLY one carrying a running: marker; each of the
	// other n-1 slot-rejected messages must have NO invocation-state field at all
	// (redis.Nil), which makes the pre-claim/no-attempt invariant direct rather
	// than inferred from the eventual drain.
	if got := exec.callCount(); got != 1 {
		t.Fatalf("executor calls while saturated = %d, want 1 (the holding invocation)", got)
	}
	var claimed int
	for _, id := range ids {
		if _, ok := e.pending(id); !ok {
			t.Fatalf("event %s not pending while saturated; want pending (no ACK)", id)
		}
		if entries := e.dlqEntries(id); len(entries) != 0 {
			t.Fatalf("event %s dead-lettered while saturated; want pending", id)
		}
		v, err := e.stateField(id, "fn/index.run")
		if err == nil && strings.HasPrefix(v, "running:") {
			claimed++
			continue
		}
		if !errors.Is(err, redis.Nil) {
			t.Fatalf("event %s fn/index.run = (%q, %v); slot-rejected delivery must have no invocation-state field (redis.Nil)", id, v, err)
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed (running) invocations while saturated = %d, want exactly 1", claimed)
	}

	// Release capacity: the holding invocation completes and ACKs, freeing the
	// slot. No waiter is left blocked (proven above), so the pending backlog is
	// re-delivered only by the reclaim loop, which executes and ACKs each message
	// exactly once.
	close(release)
	e.eventually("all events reclaimed, executed, and ACKed after capacity frees", func() bool {
		return e.pendingCount(ids) == 0
	})
	if got := exec.callCount(); got != n {
		t.Fatalf("total executor calls = %d, want %d (each event executed exactly once)", got, n)
	}
	for _, id := range ids {
		if entries := e.dlqEntries(id); len(entries) != 0 {
			t.Fatalf("event %s was dead-lettered; want completed", id)
		}
	}
}

// waitSustained asserts pred holds continuously for dur, then returns; it fails
// after a generous 10s budget. It is the deterministic replacement for a fixed
// "grace period" sleep when a state must persist for a minimum window (mirrors
// the stream package's test helper). Callers must invoke it from the test
// goroutine.
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

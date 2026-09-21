package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/function"
)

// TestNewConsumerDefaults consolidates the default-derivation assertions for the
// consumer: the DLQ name, the reclaim MinPendingIdle threshold, the reclaim
// interval, the bounded local buffer (MaxBufferedEvents), and the invariant that
// the production constructor always supplies an invocation-state store. These
// defaults pace message-level retry/recovery and backpressure, so they are locked
// down here.
func TestNewConsumerDefaults(t *testing.T) {
	// A client is required for the consumer to be constructed; the address is
	// never dialed by NewConsumer.
	cfg := func() ConsumerConfig {
		return ConsumerConfig{
			Client: redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
			Stream: "events",
			Log:    slog.New(slog.DiscardHandler),
		}
	}

	c := NewConsumer(cfg())
	if got := c.dlqStream; got != "relay:events:dlq" {
		t.Errorf("dlqStream = %q, want relay:events:dlq", got)
	}

	// Unset MinPendingIdle -> DefaultReclaimInterval (1m).
	if got := c.minPendingIdle; got != DefaultReclaimInterval {
		t.Errorf("minPendingIdle = %s, want DefaultReclaimInterval = %s", got, DefaultReclaimInterval)
	}

	// Unset ReclaimInterval -> DefaultReclaimInterval (1m).
	if got := c.reclaimInterval; got != DefaultReclaimInterval {
		t.Errorf("reclaimInterval = %s, want DefaultReclaimInterval = %s", got, DefaultReclaimInterval)
	}

	// Unset MaxBufferedEvents -> DefaultMaxBufferedEvents (16).
	if got := c.capacity; got != DefaultMaxBufferedEvents {
		t.Errorf("capacity = %d, want DefaultMaxBufferedEvents = %d", got, DefaultMaxBufferedEvents)
	}

	// A zero or negative MaxBufferedEvents also falls back to the default (a
	// value of 0 must not mean unbounded).
	for _, bad := range []int{0, -1} {
		badCfg := cfg()
		badCfg.MaxBufferedEvents = bad
		if got := NewConsumer(badCfg).capacity; got != DefaultMaxBufferedEvents {
			t.Errorf("MaxBufferedEvents=%d capacity = %d, want default %d", bad, got, DefaultMaxBufferedEvents)
		}
	}

	// An explicit positive value is honored.
	okCfg := cfg()
	okCfg.MaxBufferedEvents = 4
	if got := NewConsumer(okCfg).capacity; got != 4 {
		t.Errorf("explicit MaxBufferedEvents=4 capacity = %d, want 4", got)
	}

	// Invariant: the consumer is always constructed with a functional
	// invocation-state store (the production path supplies a Redis-backed one).
	if c.invStateStore == nil {
		t.Error("invStateStore is nil, want non-nil (consumer must always carry an invocation-state store)")
	}
}

// TestMaxRuleTimeoutMatchesFunctionMaxTimeout pins the cross-package invariant
// that stream.MaxRuleTimeout and function.MaxTimeout are the same value. The
// stream layer derives its reclaim threshold from this cap, so a divergence
// would break the reclaim-while-in-flight guard.
func TestMaxRuleTimeoutMatchesFunctionMaxTimeout(t *testing.T) {
	if MaxRuleTimeout != function.MaxTimeout {
		t.Errorf("stream.MaxRuleTimeout = %s, function.MaxTimeout = %s; they must match", MaxRuleTimeout, function.MaxTimeout)
	}
}

// TestPendingAgeParsing pins the age of an old stream ID: the returned duration
// is the elapsed time since its millisecond timestamp.
func TestPendingAgeParsing(t *testing.T) {
	age, ok := pendingAge("1234567-0")
	if !ok {
		t.Fatal("expected 1234567-0 to parse")
	}
	want := time.Since(time.UnixMilli(1234567))
	if age < want-2*time.Second || age > want+2*time.Second {
		t.Errorf("age = %v, want ~%v for %q", age, want, "1234567-0")
	}
}

// TestPendingAgeFreshIDClampsFutureToZero pins both the fresh-ID path and the
// negative-age clamp: a timestamp in the future (clock skew) yields 0, never a
// negative gauge.
func TestPendingAgeFreshIDClampsFutureToZero(t *testing.T) {
	ms := time.Now().UnixMilli()
	age, ok := pendingAge(fmt.Sprintf("%d-0", ms))
	if !ok {
		t.Fatalf("expected %d-0 to parse", ms)
	}
	if age < 0 || age > 2*time.Second {
		t.Errorf("fresh id age = %v, want ~0", age)
	}

	future := time.Now().Add(time.Hour).UnixMilli()
	age, ok = pendingAge(fmt.Sprintf("%d-0", future))
	if !ok {
		t.Fatalf("expected future id %d-0 to parse", future)
	}
	if age != 0 {
		t.Errorf("future id age = %v, want 0 (negative-age clamp)", age)
	}
}

func TestPendingAgeInvalidInputs(t *testing.T) {
	cases := []string{
		"",
		"-",
		"abc",
		"no-dash",
		"1234567", // no seq part
		"-0",
	}
	for _, c := range cases {
		if _, ok := pendingAge(c); ok {
			t.Errorf("expected %q to fail to parse", c)
		}
	}
}

// TestIsBusyGroup pins the BUSYGROUP tolerance predicate used by EnsureGroup:
// only an error whose text carries BUSYGROUP is tolerated; nil and unrelated
// errors are not.
func TestIsBusyGroup(t *testing.T) {
	if isBusyGroup(nil) {
		t.Error("isBusyGroup(nil) = true, want false")
	}
	if !isBusyGroup(errors.New("BUSYGROUP Consumer Group name already exists")) {
		t.Error("isBusyGroup(BUSYGROUP error) = false, want true")
	}
	if isBusyGroup(errors.New("connection refused")) {
		t.Error("isBusyGroup(unrelated error) = true, want false")
	}
}

// TestBufferSemaphoreAcquireReleaseGauge pins the semaphore's capacity bound and
// its occupancy counter: acquire/tryAcquire take a slot and increment inflight,
// release frees it, and tryAcquire fails (without blocking) at capacity.
func TestBufferSemaphoreAcquireReleaseGauge(t *testing.T) {
	const capacity = 2
	s := newBufferSemaphore(capacity)
	ctx := context.Background()

	for i := 0; i < capacity; i++ {
		if !s.acquire(ctx) {
			t.Fatalf("acquire %d failed, want success", i)
		}
	}
	if got := s.inflight(); got != capacity {
		t.Fatalf("inflight = %d, want %d", got, capacity)
	}
	// At capacity, tryAcquire must fail immediately (non-blocking) and not
	// change occupancy.
	if s.tryAcquire() {
		t.Fatal("tryAcquire at capacity = true, want false")
	}
	if got := s.inflight(); got != capacity {
		t.Fatalf("inflight after failed tryAcquire = %d, want %d", got, capacity)
	}

	s.release()
	if got := s.inflight(); got != capacity-1 {
		t.Fatalf("inflight after release = %d, want %d", got, capacity-1)
	}
	if !s.tryAcquire() {
		t.Fatal("tryAcquire after release = false, want true")
	}
	if got := s.inflight(); got != capacity {
		t.Fatalf("inflight after re-acquire = %d, want %d", got, capacity)
	}
}

// TestBufferSemaphoreAcquireCancelledContext pins the context-aware acquire: a
// full semaphore returns false promptly when ctx is cancelled rather than
// blocking past shutdown.
func TestBufferSemaphoreAcquireCancelledContext(t *testing.T) {
	s := newBufferSemaphore(1)
	if !s.acquire(context.Background()) {
		t.Fatal("initial acquire failed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.acquire(ctx) {
		t.Fatal("acquire on a cancelled ctx with a full semaphore = true, want false")
	}
	if got := s.inflight(); got != 1 {
		t.Fatalf("inflight after cancelled acquire = %d, want 1 (unchanged)", got)
	}
}

// TestBufferSemaphoreAcquireAfterReleaseWakesWaiter pins that a blocked acquire
// is woken by release (deterministic channel handoff, no sleep): the waiting
// goroutine completes once a slot frees.
func TestBufferSemaphoreAcquireAfterReleaseWakesWaiter(t *testing.T) {
	s := newBufferSemaphore(1)
	if !s.acquire(context.Background()) {
		t.Fatal("initial acquire failed")
	}
	acquired := make(chan struct{})
	go func() {
		if s.acquire(context.Background()) {
			close(acquired)
		}
	}()
	// The waiter cannot proceed until the sole slot is released.
	select {
	case <-acquired:
		t.Fatal("acquire returned before a slot was released")
	default:
	}
	s.release()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter was not woken by release")
	}
}

// newTestConsumer returns a Consumer with a buffer logger and a fast,
// deterministic backoff for health-transition tests.
func newTestConsumer(t *testing.T) (*Consumer, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	c := NewConsumer(ConsumerConfig{
		Log:           slog.New(slog.NewTextHandler(&buf, nil)),
		backoffTable:  []time.Duration{time.Second},
		backoffJitter: func(f float64) float64 { return f },
	})
	return c, &buf
}

func TestNoteOutcomeFailureMarksUnhealthyAndLogsOnce(t *testing.T) {
	c, buf := newTestConsumer(t)
	if !c.Healthy() {
		t.Fatalf("consumer should start healthy")
	}
	c.noteOutcome(errors.New("boom"), time.Second)
	if c.Healthy() {
		t.Fatalf("consumer should be unhealthy after failure")
	}
	if got := buf.String(); !strings.Contains(got, "Redis: read failed; retrying") {
		t.Fatalf("expected one failure log, got: %q", got)
	}
}

func TestNoteOutcomeSuccessRecoversAndLogsOnce(t *testing.T) {
	c, buf := newTestConsumer(t)
	// Enter outage.
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	// Many successes must log exactly one recovery line.
	for i := 0; i < 5; i++ {
		c.noteOutcome(nil, 0)
	}
	if !c.Healthy() {
		t.Fatalf("consumer should be healthy after success")
	}
	if got := strings.Count(buf.String(), "Redis connection recovered"); got != 1 {
		t.Fatalf("expected exactly one recovery line, got %d: %q", got, buf.String())
	}
}

func TestNoteOutcomeNoRecoverySpamDuringOutage(t *testing.T) {
	c, buf := newTestConsumer(t)
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	// Repeated failures during an ongoing outage must not log recovery lines.
	for i := 0; i < 5; i++ {
		c.noteOutcome(errors.New("boom"), time.Second)
	}
	if got := strings.Count(buf.String(), "Redis connection recovered"); got != 0 {
		t.Fatalf("no recovery line expected during outage, got %d", got)
	}
}

func TestNoteOutcomeRedisNilCountsAsSuccess(t *testing.T) {
	c, buf := newTestConsumer(t)
	c.noteOutcome(errors.New("boom"), time.Second)
	buf.Reset()
	c.noteOutcome(redis.Nil, 0)
	if !c.Healthy() {
		t.Fatalf("redis.Nil should count as healthy")
	}
	if got := strings.Count(buf.String(), "Redis connection recovered"); got != 1 {
		t.Fatalf("expected one recovery line, got %d", got)
	}
}

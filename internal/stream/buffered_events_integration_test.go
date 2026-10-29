//go:build integration

package stream

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/testutil"
)

func waitBufferedEvents(t *testing.T, m *metrics.Registry, want int) {
	t.Helper()
	testutil.WaitFor(t, 8*time.Second, fmt.Sprintf("buffered_events = %d", want), func() bool {
		return m.Gauge(metrics.MetricBufferedEvents) == float64(want)
	})
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestIntegrationBufferedEventsTracksLocalOccupancy covers terminal ACK, a
// failed-but-pending delivery, DLQ, shutdown cancellation, reclaim, and a
// concurrent drain. The gauge intentionally follows local semaphore occupancy,
// not PEL depth: pending failures and canceled work leave the PEL after the
// local slot has been released.
func TestIntegrationBufferedEventsTracksLocalOccupancy(t *testing.T) {
	testutil.RequireRedis(t)

	t.Run("successful ack", func(t *testing.T) {
		m := metrics.New()
		e := newEnv(t, ConsumerConfig{Metrics: m})
		if got := m.Gauge(metrics.MetricBufferedEvents); got != 0 {
			t.Fatalf("initial buffered_events = %v, want 0", got)
		}
		id := e.xadd(t, `{"case":"ack"}`)
		entered, release := make(chan struct{}), make(chan struct{})
		e.start(func(context.Context, string, map[string]any) error {
			close(entered)
			<-release
			return nil
		})
		waitSignal(t, entered, "handler")
		waitBufferedEvents(t, m, 1)
		e.waitDelivered(t, id)
		close(release)
		e.waitGone(t, id)
		waitBufferedEvents(t, m, 0)
		e.stop(t)
	})

	t.Run("handler failure remains pending", func(t *testing.T) {
		m := metrics.New()
		e := newEnv(t, ConsumerConfig{Metrics: m})
		id := e.xadd(t, `{"case":"retry"}`)
		entered, release := make(chan struct{}), make(chan struct{})
		e.start(func(context.Context, string, map[string]any) error {
			close(entered)
			<-release
			return fmt.Errorf("retry later")
		})
		waitSignal(t, entered, "failing handler")
		waitBufferedEvents(t, m, 1)
		e.waitDelivered(t, id)
		close(release)
		waitBufferedEvents(t, m, 0)
		if _, ok := e.pending()[id]; !ok {
			t.Fatal("failed message left the PEL; want pending retry")
		}
		e.stop(t)
	})

	t.Run("terminal DLQ", func(t *testing.T) {
		m := metrics.New()
		e := newEnv(t, ConsumerConfig{Metrics: m})
		id := e.xadd(t, `{"case":"dlq"}`)
		entered, release := make(chan struct{}), make(chan struct{})
		e.start(func(ctx context.Context, _ string, _ map[string]any) error {
			p, ok := InvocationStateFrom(ctx)
			if !ok {
				return fmt.Errorf("no invocation state in context")
			}
			started, claim, _, err := p.TryStart("fn/h", time.Hour)
			if err != nil || !started {
				return ErrInvocationNotEligible
			}
			p.MarkExhausted("fn/h", claim)
			close(entered)
			<-release
			return &HandlerExhaustedError{Invocations: []ExhaustedInvocation{{
				App: "fn", Handler: "h", Attempts: claim.Attempt, Err: ErrInvocationExhausted,
			}}}
		})
		waitSignal(t, entered, "exhausted handler")
		waitBufferedEvents(t, m, 1)
		close(release)
		testutil.WaitFor(t, 8*time.Second, "DLQ entry", func() bool {
			_, ok := e.dlqEntry(id)
			return ok
		})
		e.waitGone(t, id)
		waitBufferedEvents(t, m, 0)
		e.stop(t)
	})

	t.Run("shutdown cancellation", func(t *testing.T) {
		m := metrics.New()
		e := newEnv(t, ConsumerConfig{Metrics: m})
		id := e.xadd(t, `{"case":"cancel"}`)
		entered := make(chan struct{})
		e.start(func(ctx context.Context, _ string, _ map[string]any) error {
			close(entered)
			<-ctx.Done()
			return ErrInvocationNotEligible
		})
		waitSignal(t, entered, "cancelable handler")
		waitBufferedEvents(t, m, 1)
		e.waitDelivered(t, id)
		e.stop(t)
		waitBufferedEvents(t, m, 0)
		if _, ok := e.pending()[id]; !ok {
			t.Fatal("canceled message left the PEL; want recoverable pending delivery")
		}
	})

	t.Run("reclaimed delivery", func(t *testing.T) {
		stream := fmt.Sprintf("buffer-reclaim-%d", time.Now().UnixNano())
		group := stream + "-group"
		eA := newEnv(t, ConsumerConfig{
			Stream: stream, Group: group, Consumer: "buffer-A", MinPendingIdle: time.Second,
			Metrics: metrics.New(),
		})
		id := eA.xadd(t, `{"case":"reclaim"}`)
		eA.start(func(context.Context, string, map[string]any) error { return fmt.Errorf("leave pending") })
		eA.waitDelivered(t, id)
		waitBufferedEvents(t, eA.consumer.metrics, 0)
		eA.stop(t)

		m := metrics.New()
		eB := newEnv(t, ConsumerConfig{
			Stream: stream, Group: group, Consumer: "buffer-B", MinPendingIdle: 10 * time.Millisecond,
			ReclaimInterval: 20 * time.Millisecond, Metrics: m,
		})
		entered, release := make(chan struct{}), make(chan struct{})
		eB.start(func(context.Context, string, map[string]any) error {
			close(entered)
			<-release
			return nil
		})
		waitSignal(t, entered, "reclaimed handler")
		waitBufferedEvents(t, m, 1)
		if _, ok := eB.pending()[id]; !ok {
			t.Fatal("reclaimed message not in PEL while locally buffered")
		}
		close(release)
		eB.waitGone(t, id)
		waitBufferedEvents(t, m, 0)
		eB.stop(t)
	})

	t.Run("concurrent drain", func(t *testing.T) {
		const count = 4
		m := metrics.New()
		e := newEnv(t, ConsumerConfig{MaxBufferedEvents: count, Count: count, Metrics: m})
		var entered atomic.Int64
		release := make(chan struct{})
		e.start(func(context.Context, string, map[string]any) error {
			entered.Add(1)
			<-release
			return nil
		})
		for i := 0; i < count; i++ {
			e.xadd(t, fmt.Sprintf(`{"case":"drain","i":%d}`, i))
		}
		testutil.WaitFor(t, 8*time.Second, "all handlers concurrently in flight", func() bool {
			return entered.Load() == count
		})
		waitBufferedEvents(t, m, count)
		if got := e.consumer.buffer.inflight(); got != count {
			t.Fatalf("local buffer occupancy = %d, want %d", got, count)
		}
		close(release)
		testutil.WaitFor(t, 8*time.Second, "all concurrent messages acked", func() bool {
			return len(e.pending()) == 0
		})
		waitBufferedEvents(t, m, 0)
		e.stop(t)
	})
}

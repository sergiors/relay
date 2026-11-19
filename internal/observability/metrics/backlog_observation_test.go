package metrics

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins the backlog observation contract the persisted stats snapshot
// relies on: the pending depth and oldest-pending age are published as ONE
// pair with ONE successful-observation instant, so a snapshot can never mix a
// new depth with an old age or mark an unobserved pair as fresh.

// TestSetPendingBacklogObservedPublishesOnePair pins that both gauges, both
// per-gauge freshness markers, and the atomic pair move together under the
// caller's instant, and that every read path sees that same pair.
func TestSetPendingBacklogObservedPublishesOnePair(t *testing.T) {
	r := New()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	r.SetPendingBacklogObserved(12, 34, at)

	entries, age, gotAt, ok := r.PendingBacklogObservation()
	if !ok || entries != 12 || age != 34 || !gotAt.Equal(at) {
		t.Fatalf("backlog observation = (%v, %v, %v, %v), want (12, 34, %s, true)",
			entries, age, gotAt, ok, at)
	}
	if v, obsAt, obsOK := r.GaugeObservation(MetricPendingEntries); !obsOK || v != 12 || !obsAt.Equal(at) {
		t.Fatalf("pending entries observation = (%v, %v, %v), want (12, %s, true)", v, obsAt, obsOK, at)
	}
	if v, obsAt, obsOK := r.GaugeObservation(MetricPendingOldestAge); !obsOK || v != 34 || !obsAt.Equal(at) {
		t.Fatalf("pending age observation = (%v, %v, %v), want (34, %s, true)", v, obsAt, obsOK, at)
	}
	// The gauges themselves move with the observation, so the exposition
	// surface and the persisted snapshot agree.
	if got := r.Gauge(MetricPendingEntries); got != 12 {
		t.Fatalf("pending_entries = %v, want 12", got)
	}
	if got := r.Gauge(MetricPendingOldestAge); got != 34 {
		t.Fatalf("pending_oldest_age_seconds = %v, want 34", got)
	}
}

// TestPendingBacklogObservationAbsentUntilObserved pins that a registry that
// never observed the backlog reports NO observation, and that a plain gauge set
// (a test, or a non-backlog source) stays readable as a live value WITHOUT
// claiming freshness — the persisted snapshot only stamps an instant it really
// observed.
func TestPendingBacklogObservationAbsentUntilObserved(t *testing.T) {
	r := New()
	if _, _, _, ok := r.PendingBacklogObservation(); ok {
		t.Fatal("a fresh registry must report no backlog observation")
	}

	r.SetGauge(MetricPendingEntries, 5)
	if _, _, _, ok := r.PendingBacklogObservation(); ok {
		t.Fatal("a caller-set gauge must not claim a successful backlog observation")
	}
	if v, at, ok := r.GaugeObservation(MetricPendingEntries); ok || v != 5 || !at.IsZero() {
		t.Fatalf("GaugeObservation = (%v, %v, %v), want the live value with no observation", v, at, ok)
	}
}

// TestSetPendingBacklogObservedZeroInstantPreserves pins that a zero instant is
// ignored entirely: the previously observed pair survives untouched, so no
// caller can publish an untimed pair as fresh or move the gauges without a
// real observation instant.
func TestSetPendingBacklogObservedZeroInstantPreserves(t *testing.T) {
	r := New()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r.SetPendingBacklogObserved(3, 9, at)

	r.SetPendingBacklogObserved(99, 99, time.Time{})

	entries, age, gotAt, ok := r.PendingBacklogObservation()
	if !ok || entries != 3 || age != 9 || !gotAt.Equal(at) {
		t.Fatalf("backlog observation = (%v, %v, %v, %v), want the prior pair (3, 9, %s)",
			entries, age, gotAt, ok, at)
	}
	if got := r.Gauge(MetricPendingEntries); got != 3 {
		t.Fatalf("pending_entries = %v, want 3 (a zero-instant call must not move the gauge)", got)
	}
}

// TestBacklogObservationNilSafe pins nil-receiver safety on both observation
// surfaces: recording is a no-op and reading reports no observation.
func TestBacklogObservationNilSafe(t *testing.T) {
	var r *Registry
	r.SetPendingBacklogObserved(1, 2, time.Now())
	if _, _, _, ok := r.PendingBacklogObservation(); ok {
		t.Fatal("nil registry must report no backlog observation")
	}
	if _, _, ok := r.GaugeObservation(MetricPendingEntries); ok {
		t.Fatal("nil registry must report no gauge observation")
	}
}

// TestPendingBacklogObservationAtomicUnderConcurrentWrites is the concurrency
// regression for the atomic pair: writers publish many distinguishable
// depth/age/timestamp tuples while readers sample PendingBacklogObservation.
// Every tuple is internally self-describing — depth encodes the iteration, age
// is a deterministic function of depth, and the instant encodes the same
// iteration — so ANY read whose three fields do not all come from the same
// write (a mixed depth/age pair, or a pair re-dated with another write's
// instant) fails at least one equality. Under -race this also pins that the
// pair and its instant move under one lock, never as separately published
// fields.
func TestPendingBacklogObservationAtomicUnderConcurrentWrites(t *testing.T) {
	const iterations = 2000
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	// tupleFor builds write i's payload: age is a pure function of the depth,
	// and the instant encodes the same iteration, so a reader can derive the
	// ONLY acceptable age and instant from the depth it received.
	tupleFor := func(i int) (depth, age float64, at time.Time) {
		return float64(i), float64(i * i), base.Add(time.Duration(i) * time.Millisecond)
	}

	r := New()
	// Seed one observation before any goroutine starts, so a reader's first
	// successful read is guaranteed regardless of scheduling.
	seedDepth, seedAge, seedAt := tupleFor(1)
	r.SetPendingBacklogObserved(seedDepth, seedAge, seedAt)

	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := 0; w < 2; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 1; i <= iterations; i++ {
				depth, age, at := tupleFor(i)
				r.SetPendingBacklogObserved(depth, age, at)
			}
		}()
	}
	go func() {
		writers.Wait()
		close(stop)
	}()

	type violation struct {
		depth, age float64
		at         time.Time
		reason     string
	}
	firstViolation := make(chan violation, 1)
	var validated atomic.Int64

	var readers sync.WaitGroup
	for g := 0; g < 2; g++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				depth, age, at, ok := r.PendingBacklogObservation()
				if ok {
					i := int(depth)
					var reason string
					switch {
					case float64(i) != depth || i < 1 || i > iterations:
						reason = "depth was never written"
					case age != float64(i*i):
						reason = "age does not match the depth (mixed pair)"
					case !at.Equal(base.Add(time.Duration(i) * time.Millisecond)):
						reason = "instant does not match the depth (mixed pair/timestamp)"
					}
					if reason != "" {
						select {
						case firstViolation <- violation{depth: depth, age: age, at: at, reason: reason}:
						default:
						}
						return
					}
					validated.Add(1)
				}
				// The stop check comes AFTER one read, so every reader
				// performs at least one successful validation even when the
				// writers finished first.
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	readers.Wait()

	select {
	case v := <-firstViolation:
		t.Fatalf("torn backlog observation: %s — got (depth %v, age %v, instant %s)",
			v.reason, v.depth, v.age, v.at.Format(time.RFC3339Nano))
	default:
	}
	if got := validated.Load(); got < 2 {
		t.Fatalf("validated observations = %d, want at least one per reader", got)
	}
}

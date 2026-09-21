package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"relay/internal/metrics"
)

// newUnitPublisher builds a Publisher with the unexported test seams set: no
// Redis is contacted (client is a never-dialed stub), the envelope function is
// the real one unless overridden, and runScript is the supplied fake returning
// (res, err). It mirrors how production wires the seams.
func newUnitPublisher(t *testing.T, m *metrics.Registry, res int, scriptErr error) *SchedulePublisher {
	t.Helper()
	cli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = cli.Close() })
	p := NewPublisher(cli, "unit-stream", nil, m)
	p.runScript = func(context.Context, redis.Scripter, []string, ...any) (int, error) {
		return res, scriptErr
	}
	return p
}

func unitOccurrence() Occurrence {
	return Occurrence{Function: "courses", Handler: "jobs.cleanup.handler", ScheduledAt: fixedInstant}
}

// TestPublishOccurrenceBranchMatrix drives the three result branches of the
// publish-if-new script through the runScript seam: a 1 is a fresh publication, a
// 0 is a clean duplicate (not an error), and a script error is surfaced while
// incrementing the failure counter.
func TestPublishOccurrenceBranchMatrix(t *testing.T) {
	t.Run("published", func(t *testing.T) {
		m := metrics.New()
		p := newUnitPublisher(t, m, 1, nil)
		published, err := p.PublishOccurrence(context.Background(), unitOccurrence())
		if err != nil {
			t.Fatalf("PublishOccurrence: %v", err)
		}
		if !published {
			t.Fatal("result 1 should report published=true")
		}
		if got := m.Counter(metrics.MetricScheduleOccurrencesPublished); got != 1 {
			t.Errorf("schedule published counter = %d, want 1", got)
		}
		if got := m.Counter(metrics.MetricScheduleOccurrencesDuplicate); got != 0 {
			t.Errorf("schedule duplicate counter = %d, want 0", got)
		}
		if got := m.Counter(metrics.MetricSchedulePublishFailures); got != 0 {
			t.Errorf("schedule publish failures counter = %d, want 0", got)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		m := metrics.New()
		p := newUnitPublisher(t, m, 0, nil)
		published, err := p.PublishOccurrence(context.Background(), unitOccurrence())
		if err != nil {
			t.Fatalf("PublishOccurrence: %v", err)
		}
		if published {
			t.Fatal("result 0 should report published=false (duplicate)")
		}
		if got := m.Counter(metrics.MetricScheduleOccurrencesDuplicate); got != 1 {
			t.Errorf("schedule duplicate counter = %d, want 1", got)
		}
		if got := m.Counter(metrics.MetricScheduleOccurrencesPublished); got != 0 {
			t.Errorf("schedule published counter = %d, want 0", got)
		}
		if got := m.Counter(metrics.MetricSchedulePublishFailures); got != 0 {
			t.Errorf("schedule publish failures counter = %d, want 0", got)
		}
	})

	t.Run("script error", func(t *testing.T) {
		m := metrics.New()
		boom := errors.New("wrongtype")
		p := newUnitPublisher(t, m, 0, boom)
		published, err := p.PublishOccurrence(context.Background(), unitOccurrence())
		if err == nil {
			t.Fatal("a script error must be returned")
		}
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want it to wrap %v", err, boom)
		}
		if published {
			t.Fatal("a failed publish must report published=false")
		}
		if got := m.Counter(metrics.MetricSchedulePublishFailures); got != 1 {
			t.Errorf("schedule publish failures counter = %d, want 1", got)
		}
		if got := m.Counter(metrics.MetricScheduleOccurrencesPublished); got != 0 {
			t.Errorf("schedule published counter = %d, want 0", got)
		}
	})
}

// TestPublishOccurrenceEnvelopeErrorIncrementsFailure pins the marshal-error
// path: when the envelope cannot be produced, the failure counter increments and
// the runScript seam is never reached.
func TestPublishOccurrenceEnvelopeErrorIncrementsFailure(t *testing.T) {
	m := metrics.New()
	p := newUnitPublisher(t, m, 1, nil)
	scriptCalled := false
	p.runScript = func(context.Context, redis.Scripter, []string, ...any) (int, error) {
		scriptCalled = true
		return 1, nil
	}
	// Force the marshal to fail via the envelope seam.
	boom := errors.New("marshal boom")
	p.envelopeFn = func(Occurrence) ([]byte, error) { return nil, boom }

	published, err := p.PublishOccurrence(context.Background(), unitOccurrence())
	if err == nil {
		t.Fatal("an envelope error must be returned")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
	if published {
		t.Fatal("a failed publish must report published=false")
	}
	if scriptCalled {
		t.Fatal("runScript was called despite an envelope error")
	}
	if got := m.Counter(metrics.MetricSchedulePublishFailures); got != 1 {
		t.Errorf("schedule publish failures counter = %d, want 1", got)
	}
}

// TestPublishOccurrencePassesExpectedScriptArgs pins the arguments handed to the
// atomic script: KEYS = [dedupKey, stream] and ARGV = [occurrence ID, TTL ms,
// envelope JSON]. A change here would break the Lua script's key contract.
func TestPublishOccurrencePassesExpectedScriptArgs(t *testing.T) {
	m := metrics.New()
	p := newUnitPublisher(t, m, 1, nil)
	o := unitOccurrence()

	var gotKeys []string
	var gotArgs []any
	p.runScript = func(_ context.Context, _ redis.Scripter, keys []string, args ...any) (int, error) {
		gotKeys = keys
		gotArgs = args
		return 1, nil
	}
	if _, err := p.PublishOccurrence(context.Background(), o); err != nil {
		t.Fatalf("PublishOccurrence: %v", err)
	}

	wantKeys := []string{dedupKey(o), "unit-stream"}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("keys = %v, want %v", gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Errorf("keys[%d] = %q, want %q", i, gotKeys[i], wantKeys[i])
		}
	}
	if len(gotArgs) != 3 {
		t.Fatalf("args = %v, want 3 (id, ttl ms, envelope)", gotArgs)
	}
	if gotArgs[0] != o.ID() {
		t.Errorf("args[0] = %v, want occurrence ID %q", gotArgs[0], o.ID())
	}
	if gotArgs[1] != occurrenceTTL.Milliseconds() {
		t.Errorf("args[1] = %v, want TTL ms %d", gotArgs[1], occurrenceTTL.Milliseconds())
	}
	env, err := o.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if gotArgs[2] != string(env) {
		t.Errorf("args[2] = %v, want envelope %s", gotArgs[2], env)
	}
}

// fixedInstant is a deterministic, second-aligned scheduled instant.
var fixedInstant = time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/testutil"
)

// newUnitPublisher builds a Publisher with the unexported test seams set: no
// Redis is contacted (client is a never-dialed stub), the envelope function is
// the real one unless overridden, and runScript is the supplied fake returning
// (res, err). It mirrors how production wires the seams.
func newUnitPublisher(t *testing.T, m *metrics.Registry, res int, scriptErr error) *SchedulePublisher {
	t.Helper()
	cli := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = cli.Close() })
	p := NewPublisher(cli, "unit-stream", testutil.DiscardLogger(), m)
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
// envelope JSON, traceparent, tracestate, baggage]. With no trace context the
// three carrier arguments are empty strings (the script omits empty fields), so
// the stream entry is identical to before. A change here would break the Lua
// script's key/arg contract.
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
	if len(gotArgs) != 6 {
		t.Fatalf("args = %v, want 6 (id, ttl ms, envelope, traceparent, tracestate, baggage)", gotArgs)
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
	for i, want := range []any{"", "", ""} {
		if gotArgs[3+i] != want {
			t.Errorf("args[%d] = %v, want empty trace field with tracing disabled", 3+i, gotArgs[3+i])
		}
	}
}

// fixedInstant is a deterministic, second-aligned scheduled instant.
var fixedInstant = time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

// withSpanRecorder installs an SDK provider backed by an in-memory exporter
// through the production Setup seam, restoring the globals on cleanup. The
// schedule package's tests do not run in parallel, so the global swap is
// race-free within the package.
func withSpanRecorder(t *testing.T) (*tracetest.InMemoryExporter, *tracing.Provider) {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "")
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), testutil.DiscardLogger(), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	return exp, provider
}

// TestPublishOccurrenceInjectsTraceContext proves the schedule publisher starts a
// `schedule.publish` span and renders its trace context into the flat Redis
// fields handed to the atomic script (traceparent/tracestate/baggage), so the
// stream consumer continues the same trace. It also proves the event envelope is
// still passed through verbatim.
func TestPublishOccurrenceInjectsTraceContext(t *testing.T) {
	exp, provider := withSpanRecorder(t)

	// A known remote parent so the injected traceparent is deterministic.
	remoteTraceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	remoteTraceID, _ := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	remoteSpanID, _ := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: remoteTraceID, SpanID: remoteSpanID, TraceFlags: oteltrace.FlagsSampled, Remote: true,
	})
	ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), sc)

	m := metrics.New()
	p := newUnitPublisher(t, m, 1, nil)
	o := unitOccurrence()
	var gotArgs []any
	p.runScript = func(_ context.Context, _ redis.Scripter, _ []string, args ...any) (int, error) {
		gotArgs = args
		return 1, nil
	}

	if _, err := p.PublishOccurrence(ctx, o); err != nil {
		t.Fatalf("PublishOccurrence: %v", err)
	}
	if len(gotArgs) != 6 {
		t.Fatalf("args = %v, want 6", gotArgs)
	}
	if gotArgs[3] == "" || gotArgs[3] == remoteTraceparent {
		// The injected traceparent is for schedule.publish (a child of the remote
		// parent), so it must be present and carry the remote TRACE id, not equal
		// the remote span's own traceparent.
		t.Errorf("args[3] (traceparent) = %v, want a schedule.publish traceparent", gotArgs[3])
	}
	env, _ := o.Envelope()
	if gotArgs[2] != string(env) {
		t.Errorf("args[2] = %v, want the verbatim envelope", gotArgs[2])
	}

	// The schedule.publish span must be exported as a child of the remote parent.
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	spans := exp.GetSpans()
	var publish *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "schedule.publish" {
			publish = &spans[i]
		}
	}
	if publish == nil {
		t.Fatalf("no schedule.publish span; got %v", spans)
	}
	if publish.SpanContext.TraceID() != remoteTraceID {
		t.Errorf("schedule.publish trace id = %s, want remote %s", publish.SpanContext.TraceID(), remoteTraceID)
	}
	if publish.Parent.SpanID() != remoteSpanID {
		t.Errorf("schedule.publish parent = %s, want remote %s", publish.Parent.SpanID(), remoteSpanID)
	}
}

// TestPublishOccurrenceWithoutTraceContextLeavesNoMetadata proves that with no
// recording span the three carrier arguments are empty strings, so the script
// omits the trace fields and the stream entry stays identical to the
// pre-tracing shape.
func TestPublishOccurrenceWithoutTraceContextLeavesNoMetadata(t *testing.T) {
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	otel.SetTracerProvider(noop.NewTracerProvider())

	m := metrics.New()
	p := newUnitPublisher(t, m, 1, nil)
	var gotArgs []any
	p.runScript = func(_ context.Context, _ redis.Scripter, _ []string, args ...any) (int, error) {
		gotArgs = args
		return 1, nil
	}
	if _, err := p.PublishOccurrence(context.Background(), unitOccurrence()); err != nil {
		t.Fatalf("PublishOccurrence: %v", err)
	}
	if len(gotArgs) != 6 || gotArgs[3] != "" || gotArgs[4] != "" || gotArgs[5] != "" {
		t.Fatalf("args = %v, want empty trace fields with tracing disabled", gotArgs)
	}
}

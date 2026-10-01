package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"relay/internal/app"
	"relay/internal/observability/tracing"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// spanRecorder bundles the in-memory exporter with the provider that owns it, so
// a test can force-flush before reading (production Setup uses a batch
// processor; the tests flush synchronously rather than waiting for its timer).
type spanRecorder struct {
	exp      *tracetest.InMemoryExporter
	provider *tracing.Provider
}

// withSpanRecorder installs an SDK tracer provider backed by an in-memory
// recorder through the production Setup seam, restoring the previous global
// provider on cleanup. No test hook is added to production code: Setup is the
// same path the worker uses, only the exporter is swapped. The runner package's
// tests do not run in parallel, so the global provider swap is race-free within
// the package.
func withSpanRecorder(t *testing.T) *spanRecorder {
	t.Helper()
	// The production Setup honors OTEL_SDK_DISABLED; clear it so an ambient
	// value in the test environment cannot disable the injected exporter.
	t.Setenv("OTEL_SDK_DISABLED", "")
	prev := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	exp := tracetest.NewInMemoryExporter()
	provider, err := tracing.Setup(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), tracing.WithExporter(exp))
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
		otel.SetTextMapPropagator(prevProp)
	})
	return &spanRecorder{exp: exp, provider: provider}
}

// flush forces the batch processor to export, then returns the recorded spans.
func (r *spanRecorder) flush(t *testing.T) tracetest.SpanStubs {
	t.Helper()
	if err := r.provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	return r.exp.GetSpans()
}

// spanByName returns the single recorded span with the given name, or fails the
// test.
func spanByName(t *testing.T, rec *spanRecorder, name string) tracetest.SpanStub {
	t.Helper()
	spans := rec.flush(t)
	var found []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("found %d spans named %q, want 1; all: %v", len(found), name, spanNames(spans))
	}
	return found[0]
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

// attrString returns the string value of a recorded span attribute, or "".
func attrString(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestHandleEmitsAppInvokeSpan proves the shared invocation path emits one
// function.invoke span per executed handler, carrying the low-cardinality
// relay.app.name / relay.handler.name / relay.app.runtime attributes and a
// success result.
func TestHandleEmitsAppInvokeSpan(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &countingExecutor{}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	if err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	span := spanByName(t, exp, "function.invoke")
	if got := attrString(span, "relay.app.name"); got != "demo" {
		t.Errorf("relay.app.name = %q, want demo", got)
	}
	if got := attrString(span, "relay.handler.name"); got != "index.run" {
		t.Errorf("relay.handler.name = %q, want index.run", got)
	}
	if got := attrString(span, "relay.app.runtime"); got != "node24" {
		t.Errorf("relay.app.runtime = %q, want node24", got)
	}
	if got := attrString(span, "function.result"); got != "success" {
		t.Errorf("function.result = %q, want success", got)
	}
	if span.Status.Code != codes.Ok {
		t.Errorf("span status = %v, want codes.Ok", span.Status.Code)
	}
}

// TestHandleFailureSpanRecordsError proves a failed invocation records the error
// and sets codes.Error on its function.invoke span, so a trace surfaces the
// failure at the boundary while the existing retry behavior is unchanged.
func TestHandleFailureSpanRecordsError(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &countingExecutor{fail: true}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"})
	if err == nil {
		t.Fatal("Handle returned nil, want the executor failure")
	}

	span := spanByName(t, exp, "function.invoke")
	if got := attrString(span, "function.result"); got != "failure" {
		t.Errorf("function.result = %q, want failure", got)
	}
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want codes.Error", span.Status.Code)
	}
	if len(span.Events) == 0 {
		t.Error("failure span has no recorded error event")
	}
}

// TestRunInvocationSpanPropagatesToExecutor proves the function.invoke span's
// context reaches the executor, so a runtime layer can nest its own spans under
// the invocation span.
func TestRunInvocationSpanPropagatesToExecutor(t *testing.T) {
	exp := withSpanRecorder(t)
	exec := &spanAwareExecutor{}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	if err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !exec.sawSpan {
		t.Fatal("executor context carries no recording span; function.invoke was not propagated")
	}
	// The child span the executor started must be parented to function.invoke.
	child := spanByName(t, exp, "test.executor.child")
	parent := spanByName(t, exp, "function.invoke")
	if child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Errorf("child parent = %s, want function.invoke %s", child.Parent.SpanID(), parent.SpanContext.SpanID())
	}
}

// spanAwareExecutor starts a child span from its invocation context so the test
// can prove the trace context is handed to the executor. It also records the
// span context it observed, so a caller can assert the exact current attempt's
// (retry/replay) context reached the executor.
type spanAwareExecutor struct {
	sawSpan  bool
	observed oteltrace.SpanContext
}

func (e *spanAwareExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	e.observed = oteltrace.SpanContextFromContext(ctx)
	_, span := tracing.Start(ctx, "test.executor.child")
	e.sawSpan = span.IsRecording()
	span.End()
	return nil
}

// retryFlipExecutor fails its first execution and succeeds thereafter, so one
// Handle call per delivery can drive a retry without a second executor.
type retryFlipExecutor struct {
	calls int
}

func (e *retryFlipExecutor) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	e.calls++
	if e.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

// linkTargetsOf returns the span contexts a recorded span links to.
func linkTargetsOf(span tracetest.SpanStub) []oteltrace.SpanContext {
	out := make([]oteltrace.SpanContext, 0, len(span.Links))
	for _, l := range span.Links {
		out = append(out, l.SpanContext)
	}
	return out
}

// invokeSpans returns every recorded span named function.invoke in emission
// order.
func invokeSpans(t *testing.T, rec *spanRecorder) []tracetest.SpanStub {
	t.Helper()
	var out []tracetest.SpanStub
	for _, s := range rec.flush(t) {
		if s.Name == "function.invoke" {
			out = append(out, s)
		}
	}
	return out
}

// TestHandleRetrySpanIsDistinctAndLinksToPreviousAttempt proves the retry
// lineage model: a first attempt produces a function.invoke span with no link,
// and a retry (after the backoff) produces a DISTINCT function.invoke span
// (different trace/span id) carrying a link to the previous attempt's span
// context — the causal retry chain across attempts. The retry does not become a
// child of the previous attempt; each attempt is its own delivery-rooted span.
func TestHandleRetrySpanIsDistinctAndLinksToPreviousAttempt(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &retryFlipExecutor{}
	r := New([]*PreparedApp{fnWithRetries(t, "demo", 1, exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Attempt 1 fails (retryable).
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err == nil {
		t.Fatal("attempt 1 unexpectedly succeeded")
	}
	// Persisted lineage exists for the invocation after attempt 1.
	if prog.TraceReference("demo/index.run") == "" {
		t.Fatal("attempt 1 did not persist a trace reference")
	}

	// Advance past the retry backoff and deliver again: attempt 2 succeeds.
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}

	spans := invokeSpans(t, rec)
	if len(spans) != 2 {
		t.Fatalf("function.invoke spans = %d, want 2 (one per attempt)", len(spans))
	}
	first, retry := spans[0], spans[1]
	if first.SpanContext.TraceID() == retry.SpanContext.TraceID() {
		t.Fatal("retry reused the first attempt's trace id; spans must be distinct")
	}
	if first.SpanContext.SpanID() == retry.SpanContext.SpanID() {
		t.Fatal("retry reused the first attempt's span id")
	}
	// The first attempt has no link; the retry links to the first attempt.
	if got := linkTargetsOf(first); len(got) != 0 {
		t.Fatalf("first attempt has %d links, want none", len(got))
	}
	links := linkTargetsOf(retry)
	if len(links) != 1 {
		t.Fatalf("retry links = %d, want 1", len(links))
	}
	if links[0].TraceID() != first.SpanContext.TraceID() || links[0].SpanID() != first.SpanContext.SpanID() {
		t.Fatalf("retry link = %s/%s, want first attempt %s/%s",
			links[0].TraceID(), links[0].SpanID(), first.SpanContext.TraceID(), first.SpanContext.SpanID())
	}
	// The retry's recorded lineage is the retry's own span (last attempt wins),
	// still independent of the first.
	lineage := prog.TraceReference("demo/index.run")
	back, ok := tracing.SpanContextFromString(lineage)
	if !ok || back.SpanID() != retry.SpanContext.SpanID() {
		t.Fatalf("persisted lineage = %q, want the retry span %s", lineage, retry.SpanContext.SpanID())
	}
}

// TestHandleRetryLinksToPersistedLineageAfterRestart proves the lineage survives
// a worker restart: a lineage persisted by a PREVIOUS worker is read from the
// shared invocation state and links the new attempt, even though this attempt's
// process never created the linked span.
func TestHandleRetryLinksToPersistedLineageAfterRestart(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &countingExecutor{}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())

	// A known reference as another worker would have persisted it.
	traceID, _ := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	prev := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: oteltrace.FlagsSampled,
	})
	prog := newFakeInvocationState()
	prog.RecordTrace("demo/index.run", tracing.SpanContextToString(prev))

	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	span := spanByName(t, rec, "function.invoke")
	links := linkTargetsOf(span)
	if len(links) != 1 || links[0].SpanID() != spanID || links[0].TraceID() != traceID {
		t.Fatalf("links = %+v, want the persisted reference %s/%s", links, traceID, spanID)
	}
	// The persisted lineage is Relay's own historical attempt, so the link
	// target must be local (Remote=false), not a remote parent.
	if links[0].IsRemote() {
		t.Error("persisted-lineage link is marked Remote; want a local historical context")
	}
}

// TestHandleMalformedPersistedLineageIgnoresIt proves a corrupt stored lineage
// never fails an invocation: the attempt runs normally with no link.
func TestHandleMalformedPersistedLineageIgnoresIt(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &countingExecutor{}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	prog.RecordTrace("demo/index.run", "not-a-traceparent")

	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	span := spanByName(t, rec, "function.invoke")
	if got := linkTargetsOf(span); len(got) != 0 {
		t.Fatalf("malformed lineage produced %d links, want none", len(got))
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

// TestHandleFanOutRetryLinksOnlyOwnLineage proves the fan-out invariant: two
// apps matching the same event run as SIBLING function.invoke spans under
// the same delivery context (neither is the other's parent), and a retried one
// links only to its own previous attempt's lineage — never to its sibling's.
func TestHandleFanOutRetryLinksOnlyOwnLineage(t *testing.T) {
	rec := withSpanRecorder(t)
	a := &retryFlipExecutor{} // fails once, then succeeds
	b := &countingExecutor{}  // always succeeds
	r := New([]*PreparedApp{
		fnWithRetries(t, "alpha", 1, a),
		fnWithRetries(t, "beta", 1, b),
	}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1: alpha fails, beta succeeds. Both persist their own lineage.
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err == nil {
		t.Fatal("delivery 1 unexpectedly succeeded (alpha should fail)")
	}
	alphaLineage := prog.TraceReference("alpha/index.run")
	betaLineage := prog.TraceReference("beta/index.run")
	if alphaLineage == "" || betaLineage == "" {
		t.Fatal("both invocations must persist their own lineage")
	}
	if alphaLineage == betaLineage {
		t.Fatal("siblings must not share a persisted lineage")
	}

	// Delivery 2: alpha retried (links to its own attempt-1 span), beta skipped.
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("delivery 2: %v", err)
	}

	var betaFirst, alphaFirst, alphaRetry *tracetest.SpanStub
	for _, s := range rec.flush(t) {
		if s.Name != "function.invoke" {
			continue
		}
		stub := s
		switch attrString(s, "relay.app.name") {
		case "beta":
			if betaFirst == nil {
				betaFirst = &stub
			}
		case "alpha":
			if alphaFirst == nil {
				alphaFirst = &stub
			} else {
				alphaRetry = &stub
			}
		}
	}
	if betaFirst == nil || alphaFirst == nil || alphaRetry == nil {
		t.Fatal("missing expected function.invoke spans for the fan-out retry")
	}
	// Siblings: same parent (the delivery root is absent in this test, so both
	// are roots) and distinct traces.
	if betaFirst.SpanContext.TraceID() == alphaFirst.SpanContext.TraceID() {
		t.Fatal("sibling handlers must be separate spans, not parented to each other")
	}
	// Only alpha's retry links, and it links to alpha's own first attempt.
	links := linkTargetsOf(*alphaRetry)
	if len(links) != 1 || links[0].SpanID() != alphaFirst.SpanContext.SpanID() {
		t.Fatalf("alpha retry link = %+v, want alpha's own attempt-1 span %s", links, alphaFirst.SpanContext.SpanID())
	}
	if betaFirst.SpanContext.SpanID() == links[0].SpanID() {
		t.Fatal("alpha retry linked to its sibling beta instead of its own lineage")
	}
}

// TestHandleTracingDisabledPersistsNoLineage proves the disabled default is
// inert: no exporter, no persisted lineage, and invocations still run.
func TestHandleTracingDisabledPersistsNoLineage(t *testing.T) {
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())

	exec := &countingExecutor{}
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
	if got := prog.TraceReference("demo/index.run"); got != "" {
		t.Fatalf("persisted lineage = %q under disabled tracing, want empty", got)
	}
}

// TestInvokeAppEmitsManualRootOperation proves a manual invocation creates
// a worker-side app.manual_invoke ROOT operation span (new root) with each
// matching function.invoke as its child, without any caller-supplied context.
func TestInvokeAppEmitsManualRootOperation(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &countingExecutor{}
	r := New([]*PreparedApp{invokeFn(t, "fn", `runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`, exec)}, testutil.DiscardLogger())

	if _, err := r.InvokeApp(context.Background(), "fn", map[string]any{"event_name": "INSERT"}); err != nil {
		t.Fatalf("InvokeApp: %v", err)
	}
	root := spanByName(t, rec, "app.manual_invoke")
	if root.Parent.IsValid() {
		t.Fatalf("manual_invoke parent = %s, want a new root", root.Parent.SpanID())
	}
	if got := attrString(root, "relay.app.name"); got != "fn" {
		t.Errorf("manual_invoke relay.app.name = %q, want fn", got)
	}
	if got := attrString(root, "function.result"); got != "success" {
		t.Errorf("manual_invoke result = %q, want success", got)
	}
	child := spanByName(t, rec, "function.invoke")
	if child.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Errorf("function.invoke parent = %s, want manual_invoke %s", child.Parent.SpanID(), root.SpanContext.SpanID())
	}
	if child.SpanContext.TraceID() != root.SpanContext.TraceID() {
		t.Errorf("function.invoke trace = %s, want manual_invoke %s", child.SpanContext.TraceID(), root.SpanContext.TraceID())
	}
}

// TestReplayDLQEmitsNewRootOperationWithLink proves the replay trace model: the
// replay creates a dlq.replay span as a NEW ROOT (independent of the caller's
// context) with a link to the original failed invocation's lineage, and the
// replayed function.invoke is its child (so the executor sees the current replay
// span context).
func TestReplayDLQEmitsNewRootOperationWithLink(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &spanAwareExecutor{}
	r := New([]*PreparedApp{replayFn(t, "fn", exec)}, testutil.DiscardLogger())

	traceID, _ := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	orig := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: oteltrace.FlagsSampled,
	})
	lineage := tracing.SpanContextToString(orig)

	// A caller context with its own span must NOT become the replay's parent.
	callerCtx, callerSpan := tracing.Start(context.Background(), "cli.replay")
	defer callerSpan.End()
	if err := r.ReplayDLQ(callerCtx, "fn", "events.created.handler", []byte(`{}`), lineage); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	replay := spanByName(t, rec, "dlq.replay")
	if replay.Parent.IsValid() {
		t.Fatalf("dlq.replay parent = %s, want a new root", replay.Parent.SpanID())
	}
	if replay.SpanContext.TraceID() == callerSpan.SpanContext().TraceID() {
		t.Fatal("dlq.replay inherited the caller's trace; it must be a new root")
	}
	links := linkTargetsOf(replay)
	if len(links) != 1 || links[0].SpanID() != spanID || links[0].TraceID() != traceID {
		t.Fatalf("dlq.replay links = %+v, want the original invocation %s/%s", links, traceID, spanID)
	}
	child := spanByName(t, rec, "function.invoke")
	if child.Parent.SpanID() != replay.SpanContext.SpanID() {
		t.Errorf("function.invoke parent = %s, want dlq.replay %s", child.Parent.SpanID(), replay.SpanContext.SpanID())
	}
	// The executor observed the replay's span context (current runtime context).
	if !exec.sawSpan {
		t.Error("executor context carries no recording span; the replay context did not propagate")
	}
	if exec.observed.TraceID() != child.SpanContext.TraceID() {
		t.Errorf("executor observed trace %s, want function.invoke %s (the replay's child)",
			exec.observed.TraceID(), child.SpanContext.TraceID())
	}
	if got := attrString(replay, "function.result"); got != "success" {
		t.Errorf("dlq.replay result = %q, want success", got)
	}
}

// TestReplayDLQWithoutLineageIsRootWithoutLink proves a pre-tracing DLQ entry
// (empty/malformed lineage) still replays as a new root operation with no link.
func TestReplayDLQWithoutLineageIsRootWithoutLink(t *testing.T) {
	for _, lineage := range []string{"", "not-a-traceparent"} {
		rec := withSpanRecorder(t)
		r := New([]*PreparedApp{replayFn(t, "fn", &countingExecutor{})}, testutil.DiscardLogger())
		if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`), lineage); err != nil {
			t.Fatalf("ReplayDLQ(lineage=%q): %v", lineage, err)
		}
		replay := spanByName(t, rec, "dlq.replay")
		if replay.Parent.IsValid() {
			t.Fatalf("lineage=%q: dlq.replay parent = %s, want root", lineage, replay.Parent.SpanID())
		}
		if links := linkTargetsOf(replay); len(links) != 0 {
			t.Fatalf("lineage=%q: dlq.replay has %d links, want none", lineage, len(links))
		}
	}
}

// TestReplayDLQFailingOperationRecordsError proves a failed replay records the
// error on the dlq.replay operation span, preserving the error contract.
func TestReplayDLQFailingOperationRecordsError(t *testing.T) {
	rec := withSpanRecorder(t)
	r := New([]*PreparedApp{replayFn(t, "fn", &countingExecutor{fail: true})}, testutil.DiscardLogger())
	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`), ""); err == nil {
		t.Fatal("expected a failed replay")
	}
	replay := spanByName(t, rec, "dlq.replay")
	if replay.Status.Code != codes.Error {
		t.Fatalf("dlq.replay status = %v, want codes.Error", replay.Status.Code)
	}
	if len(replay.Events) == 0 {
		t.Error("failed replay span has no recorded error event")
	}
}

// TestRunnerTracingHelpersStayLowCardinality is a guard that the shared
// invocation span still carries the configuration attributes (app/handler/
// runtime) and nothing payload-derived.
func TestRunnerTracingHelpersStayLowCardinality(t *testing.T) {
	rec := withSpanRecorder(t)
	r := New([]*PreparedApp{alwaysMatchFn(t, "demo", &countingExecutor{})}, testutil.DiscardLogger())
	if err := r.Handle(context.Background(), "m-1", map[string]any{"event_name": "X", "secret": "s3cr3t"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	span := spanByName(t, rec, "function.invoke")
	for _, kv := range span.Attributes {
		if strings.Contains(kv.Value.AsString(), "s3cr3t") {
			t.Fatalf("span attribute %q leaked the event payload", kv.Key)
		}
	}
}

// TestInvokeHandlerScheduleRetryLinksItsOwnLineage proves the schedule path
// shares the retry-lineage model: two eligible deliveries of a schedule
// invocation (InvokeHandler is the schedule runner) produce distinct
// function.invoke spans, and the retry links to the previous attempt's lineage
// persisted in the shared invocation state.
func TestInvokeHandlerScheduleRetryLinksItsOwnLineage(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &retryFlipExecutor{}
	r := New([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, 1)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Delivery 1 fails (retryable), leaving the schedule message pending.
	if err := r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`)); err == nil {
		t.Fatal("schedule delivery 1 unexpectedly succeeded")
	}
	if prog.TraceReference("fn/index.run") == "" {
		t.Fatal("schedule attempt 1 did not persist a trace reference")
	}

	// Delivery 2 (after the backoff): the retry succeeds and links to attempt 1.
	prog.advance(2 * time.Minute)
	if err := r.InvokeHandler(ctx, "m-1", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("schedule delivery 2: %v", err)
	}

	spans := invokeSpans(t, rec)
	if len(spans) != 2 {
		t.Fatalf("schedule function.invoke spans = %d, want 2", len(spans))
	}
	first, retry := spans[0], spans[1]
	if first.SpanContext.SpanID() == retry.SpanContext.SpanID() {
		t.Fatal("schedule retry reused the first attempt's span id")
	}
	links := linkTargetsOf(retry)
	if len(links) != 1 || links[0].SpanID() != first.SpanContext.SpanID() {
		t.Fatalf("schedule retry link = %+v, want attempt-1 span %s", links, first.SpanContext.SpanID())
	}
	if got := linkTargetsOf(first); len(got) != 0 {
		t.Fatalf("schedule attempt 1 has %d links, want none", len(got))
	}
	// The schedule path also exposes the handler attempt (from TryStart).
	for i, want := range []int64{1, 2} {
		got, ok := intAttr(spans[i], "function.attempt")
		if !ok || got != want {
			t.Errorf("schedule attempt %d function.attempt = %d (present=%v), want %d", i+1, got, ok, want)
		}
	}
}

// TestHandleRetriesShareUpstreamTraceAndLink proves the full retry semantics
// with an upstream delivery trace: when the delivery context already carries a
// trace (as a stream.message span does across redeliveries), each attempt's
// function.invoke is a CHILD of that same upstream trace (so all attempts of one
// logical delivery belong to the original trace), yet each attempt has a
// DISTINCT span id and links to the previous attempt's span.
func TestHandleRetriesShareUpstreamTraceAndLink(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &retryFlipExecutor{}
	r := New([]*PreparedApp{fnWithRetries(t, "demo", 1, exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()

	// One stable upstream delivery span stands in for the stream.message span
	// that a redelivery would re-create under the same trace.
	upstreamCtx, upstream := tracing.Start(context.Background(), "stream.message")
	defer upstream.End()
	upstreamTraceID := upstream.SpanContext().TraceID()
	ctx := stream.WithInvocationState(upstreamCtx, prog)

	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err == nil {
		t.Fatal("attempt 1 unexpectedly succeeded")
	}
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}

	spans := invokeSpans(t, rec)
	if len(spans) != 2 {
		t.Fatalf("function.invoke spans = %d, want 2", len(spans))
	}
	first, retry := spans[0], spans[1]
	for i, s := range spans {
		if s.SpanContext.TraceID() != upstreamTraceID {
			t.Errorf("attempt %d trace = %s, want the upstream trace %s", i+1, s.SpanContext.TraceID(), upstreamTraceID)
		}
		if s.Parent.SpanID() != upstream.SpanContext().SpanID() {
			t.Errorf("attempt %d parent = %s, want the upstream span %s", i+1, s.Parent.SpanID(), upstream.SpanContext().SpanID())
		}
	}
	if first.SpanContext.SpanID() == retry.SpanContext.SpanID() {
		t.Fatal("retry reused the first attempt's span id")
	}
	links := linkTargetsOf(retry)
	if len(links) != 1 || links[0].SpanID() != first.SpanContext.SpanID() {
		t.Fatalf("retry link = %+v, want attempt-1 span %s", links, first.SpanContext.SpanID())
	}
}

// intAttr returns the int64 value of a recorded span attribute, or ok=false.
func intAttr(span tracetest.SpanStub, key string) (int64, bool) {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsInt64(), true
		}
	}
	return 0, false
}

// TestHandleRetrySpanCarriesHandlerAttempt proves the low-cardinality
// function.attempt attribute is present on an execution attempt, and (below)
// omitted on a state-free manual/replay invocation where no handler attempt is
// known.
func TestHandleRetrySpanCarriesHandlerAttempt(t *testing.T) {
	rec := withSpanRecorder(t)
	exec := &retryFlipExecutor{}
	r := New([]*PreparedApp{fnWithRetries(t, "demo", 1, exec)}, testutil.DiscardLogger())
	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err == nil {
		t.Fatal("attempt 1 unexpectedly succeeded")
	}
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "m-1", map[string]any{"event_name": "X"}); err != nil {
		t.Fatalf("attempt 2: %v", err)
	}

	spans := invokeSpans(t, rec)
	if len(spans) != 2 {
		t.Fatalf("function.invoke spans = %d, want 2", len(spans))
	}
	for i, want := range []int64{1, 2} {
		got, ok := intAttr(spans[i], "function.attempt")
		if !ok || got != want {
			t.Errorf("attempt %d function.attempt = %d (present=%v), want %d", i+1, got, ok, want)
		}
	}
}

// TestInvokeAppManualSpanOmitsUnknownAttempt proves a manual invocation
// (state-free, so no handler attempt is known) still emits the operation span and
// function.invoke without a fabricated function.attempt attribute.
func TestInvokeAppManualSpanOmitsUnknownAttempt(t *testing.T) {
	rec := withSpanRecorder(t)
	r := New([]*PreparedApp{invokeFn(t, "fn", `runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`, &countingExecutor{})}, testutil.DiscardLogger())

	if _, err := r.InvokeApp(context.Background(), "fn", map[string]any{"event_name": "INSERT"}); err != nil {
		t.Fatalf("InvokeApp: %v", err)
	}
	child := spanByName(t, rec, "function.invoke")
	if _, ok := intAttr(child, "function.attempt"); ok {
		t.Error("manual invocation fabricated a function.attempt attribute with no known handler attempt")
	}
}

// TestInvokeAppInvalidStillTraced proves the operation span covers
// validation failures: an unknown app, an unavailable app, and a
// no-match invocation each emit app.manual_invoke (with the error recorded
// where one occurs) without changing the existing return values.
func TestInvokeAppInvalidStillTraced(t *testing.T) {
	t.Run("unknown function", func(t *testing.T) {
		rec := withSpanRecorder(t)
		r := New(nil, testutil.DiscardLogger())
		count, err := r.InvokeApp(context.Background(), "ghost", map[string]any{"x": 1})
		if count != 0 || !errors.Is(err, ErrAppNotFound) {
			t.Fatalf("InvokeApp = (%d, %v), want (0, ErrAppNotFound)", count, err)
		}
		op := spanByName(t, rec, "app.manual_invoke")
		if op.Status.Code != codes.Error {
			t.Errorf("manual_invoke status = %v, want codes.Error", op.Status.Code)
		}
		if got := attrString(op, "relay.app.name"); got != "ghost" {
			t.Errorf("manual_invoke relay.app.name = %q, want ghost", got)
		}
	})

	t.Run("unavailable function", func(t *testing.T) {
		rec := withSpanRecorder(t)
		r := New([]*PreparedApp{NewUnavailable(app.App{Name: "broken"})}, testutil.DiscardLogger())
		count, err := r.InvokeApp(context.Background(), "broken", map[string]any{"x": 1})
		if count != 0 || !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("InvokeApp = (%d, %v), want (0, ErrAppUnavailable)", count, err)
		}
		op := spanByName(t, rec, "app.manual_invoke")
		if op.Status.Code != codes.Error {
			t.Errorf("manual_invoke status = %v, want codes.Error", op.Status.Code)
		}
	})

	t.Run("no matching rule", func(t *testing.T) {
		rec := withSpanRecorder(t)
		r := New([]*PreparedApp{invokeFn(t, "fn", `runtime: node24
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
`, &countingExecutor{})}, testutil.DiscardLogger())
		count, err := r.InvokeApp(context.Background(), "fn", map[string]any{"event_name": "DELETE"})
		if count != 0 || err != nil {
			t.Fatalf("InvokeApp = (%d, %v), want (0, nil)", count, err)
		}
		// The no-match no-op is a successful operation and is still traced.
		op := spanByName(t, rec, "app.manual_invoke")
		if op.Status.Code == codes.Error {
			t.Errorf("no-match manual_invoke status = %v, want non-error", op.Status.Code)
		}
	})
}

// TestReplayDLQInvalidStillTraced proves the replay operation span covers
// validation failures: a removed app and a removed handler each emit
// dlq.replay (with the error recorded) without changing the return sentinels,
// and no function.invoke child runs.
func TestReplayDLQInvalidStillTraced(t *testing.T) {
	t.Run("removed function", func(t *testing.T) {
		rec := withSpanRecorder(t)
		r := New(nil, testutil.DiscardLogger())
		err := r.ReplayDLQ(context.Background(), "ghost", "h", []byte(`{}`), "")
		if !errors.Is(err, ErrAppNotFound) {
			t.Fatalf("err = %v, want ErrAppNotFound", err)
		}
		op := spanByName(t, rec, "dlq.replay")
		if op.Status.Code != codes.Error {
			t.Errorf("dlq.replay status = %v, want codes.Error", op.Status.Code)
		}
	})

	t.Run("removed handler", func(t *testing.T) {
		rec := withSpanRecorder(t)
		r := New([]*PreparedApp{replayFn(t, "fn", &countingExecutor{})}, testutil.DiscardLogger())
		err := r.ReplayDLQ(context.Background(), "fn", "events.removed.handler", []byte(`{}`), "")
		if !errors.Is(err, ErrHandlerNotFound) {
			t.Fatalf("err = %v, want ErrHandlerNotFound", err)
		}
		op := spanByName(t, rec, "dlq.replay")
		if op.Status.Code != codes.Error {
			t.Errorf("dlq.replay status = %v, want codes.Error", op.Status.Code)
		}
		for _, s := range rec.flush(t) {
			if s.Name == "function.invoke" {
				t.Error("a rejected replay must not run function.invoke")
			}
		}
	})
}

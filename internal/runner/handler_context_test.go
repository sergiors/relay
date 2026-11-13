package runner

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// handlerContextProbe records the HandlerContext each execution receives and can
// fail the first call so a test can observe the same invocation across a retry.
type handlerContextProbe struct {
	mu        sync.Mutex
	contexts  []runtime.HandlerContext
	failFirst bool
}

func (p *handlerContextProbe) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	p.mu.Lock()
	p.contexts = append(p.contexts, runtime.HandlerContextFrom(ctx))
	fail := p.failFirst && len(p.contexts) == 1
	p.mu.Unlock()
	if fail {
		return errors.New("transient handler failure")
	}
	return nil
}

func (p *handlerContextProbe) at(i int) runtime.HandlerContext {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.contexts[i]
}

func (p *handlerContextProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.contexts)
}

// TestHandleHandlerContextStableAcrossRetries verifies a broker-backed attempt
// carries the confirmed attempt and configured attempt budget, and that the
// request ID is derived from the source family plus message identity and
// app/handler so it does not change between retries.
func TestHandleHandlerContextStableAcrossRetries(t *testing.T) {
	prog := newFakeInvocationState()
	exec := &handlerContextProbe{failFirst: true}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)
	ctx := stream.WithInvocationState(context.Background(), prog)
	event := map[string]any{"status": "ok"}

	if err := r.Handle(ctx, "1757-0", event); err == nil {
		t.Fatal("first attempt returned nil, want a retryable failure")
	}
	prog.advance(2 * time.Minute)
	if err := r.Handle(ctx, "1757-0", event); err != nil {
		t.Fatalf("retry attempt: %v", err)
	}

	if got := exec.count(); got != 2 {
		t.Fatalf("executions = %d, want 2", got)
	}
	first, second := exec.at(0), exec.at(1)
	for i, hc := range []runtime.HandlerContext{first, second} {
		if hc.RequestID != "event:1757-0:fn/index.run" {
			t.Errorf("attempt %d requestId = %q, want the stable source identity", i+1, hc.RequestID)
		}
		if hc.MaxAttempts != 3 {
			t.Errorf("attempt %d maxAttempts = %d, want 3 (initial + 2 retries)", i+1, hc.MaxAttempts)
		}
		if hc.MessageID != "1757-0" || hc.App != "fn" || hc.Handler != "index.run" {
			t.Errorf("attempt %d identity = %+v", i+1, hc)
		}
	}
	if first.Attempt != 1 || second.Attempt != 2 {
		t.Errorf("attempts = %d/%d, want 1/2", first.Attempt, second.Attempt)
	}
}

// TestHandleHandlerContextStableAcrossRedelivery verifies a redelivery after a
// worker restart (fresh invocation state, same broker message) re-derives the
// SAME request ID while the confirmed attempt restarts at 1: the request ID
// tracks the logical invocation, never the delivery or attempt count.
func TestHandleHandlerContextStableAcrossRedelivery(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)
	event := map[string]any{"status": "ok"}

	prog := newFakeInvocationState()
	if err := r.Handle(stream.WithInvocationState(context.Background(), prog), "1757-0", event); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	restarted := newFakeInvocationState()
	if err := r.Handle(stream.WithInvocationState(context.Background(), restarted), "1757-0", event); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	if got := exec.count(); got != 2 {
		t.Fatalf("executions = %d, want 2", got)
	}
	first, second := exec.at(0), exec.at(1)
	if first.RequestID != "event:1757-0:fn/index.run" || second.RequestID != first.RequestID {
		t.Errorf("requestIds = %q/%q, want the same stable identity", first.RequestID, second.RequestID)
	}
	if first.Attempt != 1 || second.Attempt != 1 {
		t.Errorf("attempts = %d/%d, want 1/1 (each redelivery claims a fresh first attempt)", first.Attempt, second.Attempt)
	}
	if second.MaxAttempts != 3 {
		t.Errorf("maxAttempts = %d, want 3 (initial + 2 retries)", second.MaxAttempts)
	}
}

// TestHandleHandlerContextBrokerIdentityWithoutState verifies an event delivery
// that carries a broker message ID but no invocation state is still a
// state-free execution with a DETERMINISTIC request ID: two stateless calls for
// the same message compute the same identity, attempt stays 1 (a state-free
// execution never reports a fabricated attempt), and the configured budget is
// unchanged.
func TestHandleHandlerContextBrokerIdentityWithoutState(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)

	var requestIDs []string
	for i := 0; i < 2; i++ {
		if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
			t.Fatalf("handle %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID != "event:1757-0:fn/index.run" {
			t.Errorf("call %d requestId = %q, want the deterministic broker identity", i+1, hc.RequestID)
		}
		if hc.Attempt != 1 {
			t.Errorf("call %d attempt = %d, want 1 (no confirmed claim)", i+1, hc.Attempt)
		}
		if hc.MaxAttempts != 3 {
			t.Errorf("call %d maxAttempts = %d, want 3 (initial + 2 retries)", i+1, hc.MaxAttempts)
		}
		if hc.MessageID != "1757-0" {
			t.Errorf("call %d messageId = %q, want the source message ID", i+1, hc.MessageID)
		}
		requestIDs = append(requestIDs, hc.RequestID)
	}
	if requestIDs[0] != requestIDs[1] {
		t.Errorf("stateless executions disagree on requestId: %q vs %q", requestIDs[0], requestIDs[1])
	}
}

// TestHandleHandlerContextWithoutSourceIDIsFresh verifies an event execution
// with NO broker message identity (a direct caller) gets a fresh request ID per
// logical invocation while still reporting a well-formed state-free context:
// attempt 1, the configured budget, and an empty message ID.
func TestHandleHandlerContextWithoutSourceIDIsFresh(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)

	var requestIDs []string
	for i := 0; i < 2; i++ {
		if err := r.Handle(context.Background(), "", map[string]any{"status": "ok"}); err != nil {
			t.Fatalf("handle %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID == "" {
			t.Fatalf("call %d requestId is empty", i+1)
		}
		if hc.Attempt != 1 || hc.MaxAttempts != 3 {
			t.Errorf("call %d attempt/maxAttempts = %d/%d, want 1/3", i+1, hc.Attempt, hc.MaxAttempts)
		}
		if hc.MessageID != "" {
			t.Errorf("call %d messageId = %q, want empty for a source-less execution", i+1, hc.MessageID)
		}
		requestIDs = append(requestIDs, hc.RequestID)
	}
	if requestIDs[0] == requestIDs[1] {
		t.Fatalf("source-less executions share requestId %q, want a fresh ID each time", requestIDs[0])
	}
}

// TestHandleHandlerContextDistinctPerMessage verifies two messages matched by
// the same rule are distinct logical invocations: each derives its own request
// ID from its own message identity.
func TestHandleHandlerContextDistinctPerMessage(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)

	if err := r.Handle(context.Background(), "m-a", map[string]any{"n": 1}); err != nil {
		t.Fatalf("handle m-a: %v", err)
	}
	if err := r.Handle(context.Background(), "m-b", map[string]any{"n": 2}); err != nil {
		t.Fatalf("handle m-b: %v", err)
	}

	first, second := exec.at(0), exec.at(1)
	if first.RequestID != "event:m-a:fn/index.run" {
		t.Errorf("first requestId = %q, want event:m-a:fn/index.run", first.RequestID)
	}
	if second.RequestID != "event:m-b:fn/index.run" {
		t.Errorf("second requestId = %q, want event:m-b:fn/index.run", second.RequestID)
	}
	if first.RequestID == second.RequestID {
		t.Error("distinct messages must not share a requestId")
	}
}

// TestInvokeAppHandlerContextFreshPerInvocation verifies a manual invocation is
// never treated as a broker delivery: each operator call gets a fresh request
// ID, attempt 1, no message ID, and the rule's configured initial-plus-retries
// budget.
func TestInvokeAppHandlerContextFreshPerInvocation(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{fnWithRetries(t, "fn", 2, exec)}, testutil.DiscardLogger(), nil)

	for i := 0; i < 2; i++ {
		if _, err := r.InvokeApp(context.Background(), "fn", map[string]any{"status": "ok"}); err != nil {
			t.Fatalf("InvokeApp %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID == "" {
			t.Fatalf("call %d requestId is empty", i+1)
		}
		if hc.Attempt != 1 {
			t.Errorf("call %d attempt = %d, want 1 (a manual call never claims)", i+1, hc.Attempt)
		}
		if hc.MaxAttempts != 3 {
			t.Errorf("call %d maxAttempts = %d, want 3 (initial + 2 retries)", i+1, hc.MaxAttempts)
		}
		if hc.MessageID != "" {
			t.Errorf("call %d messageId = %q, want empty (no broker delivery)", i+1, hc.MessageID)
		}
		if hc.App != "fn" || hc.Handler != "index.run" {
			t.Errorf("call %d app/handler = %q/%q, want fn/index.run", i+1, hc.App, hc.Handler)
		}
	}
	if exec.at(0).RequestID == exec.at(1).RequestID {
		t.Fatalf("manual invocations share requestId %q, want a fresh ID each time", exec.at(0).RequestID)
	}
}

// TestInvokeHandlerHandlerContextBrokerIdentityWithoutState verifies a
// schedule occurrence delivered with a broker message ID but no invocation
// state keeps the deterministic schedule-family request ID across stateless
// executions, reports attempt 1 (no claim was confirmed), and carries the
// resolved schedule's initial-plus-retries budget.
func TestInvokeHandlerHandlerContextBrokerIdentityWithoutState(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, 2)}, testutil.DiscardLogger(), nil)

	var requestIDs []string
	for i := 0; i < 2; i++ {
		if err := r.InvokeHandler(context.Background(), "1757-0", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
			t.Fatalf("InvokeHandler %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID != "schedule:1757-0:fn/index.run" {
			t.Errorf("call %d requestId = %q, want the deterministic schedule identity", i+1, hc.RequestID)
		}
		if hc.Attempt != 1 || hc.MaxAttempts != 3 {
			t.Errorf("call %d attempt/maxAttempts = %d/%d, want 1/3", i+1, hc.Attempt, hc.MaxAttempts)
		}
		if hc.MessageID != "1757-0" {
			t.Errorf("call %d messageId = %q, want the source message ID", i+1, hc.MessageID)
		}
		requestIDs = append(requestIDs, hc.RequestID)
	}
	if requestIDs[0] != requestIDs[1] {
		t.Errorf("stateless schedule executions disagree on requestId: %q vs %q", requestIDs[0], requestIDs[1])
	}
}

// TestInvokeHandlerHandlerContextStableAcrossRetries verifies the
// broker-backed schedule path stamps the confirmed claim attempt across a
// retry: the request ID and budget stay fixed while the attempt advances from
// 1 to 2.
func TestInvokeHandlerHandlerContextStableAcrossRetries(t *testing.T) {
	prog := newFakeInvocationState()
	exec := &handlerContextProbe{failFirst: true}
	r := NewWithMetrics([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, 2)}, testutil.DiscardLogger(), nil)
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.InvokeHandler(ctx, "1757-0", "fn", "sched", "index.run", []byte(`{}`)); err == nil {
		t.Fatal("first attempt returned nil, want a retryable failure")
	}
	prog.advance(2 * time.Minute)
	if err := r.InvokeHandler(ctx, "1757-0", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("retry attempt: %v", err)
	}

	if got := exec.count(); got != 2 {
		t.Fatalf("executions = %d, want 2", got)
	}
	first, second := exec.at(0), exec.at(1)
	if first.RequestID != "schedule:1757-0:fn/index.run" || second.RequestID != first.RequestID {
		t.Errorf("requestIds = %q/%q, want the same stable identity", first.RequestID, second.RequestID)
	}
	if first.Attempt != 1 || second.Attempt != 2 {
		t.Errorf("attempts = %d/%d, want 1/2", first.Attempt, second.Attempt)
	}
	if first.MaxAttempts != 3 || second.MaxAttempts != 3 {
		t.Errorf("maxAttempts = %d/%d, want 3/3 (initial + 2 retries)", first.MaxAttempts, second.MaxAttempts)
	}
	if second.MessageID != "1757-0" {
		t.Errorf("messageId = %q, want the source message ID", second.MessageID)
	}
}

// TestInvokeHandlerHandlerContextWithoutSourceIDIsFresh verifies the state-free
// schedule path taken by direct callers and DLQ replay (no source message ID)
// stamps a fresh request ID per execution with attempt 1, the resolved
// schedule's budget, and no message ID.
func TestInvokeHandlerHandlerContextWithoutSourceIDIsFresh(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{schedFnRetries(t, "fn", exec, time.Second, 2)}, testutil.DiscardLogger(), nil)

	var requestIDs []string
	for i := 0; i < 2; i++ {
		if err := r.InvokeHandler(context.Background(), "", "fn", "sched", "index.run", []byte(`{}`)); err != nil {
			t.Fatalf("InvokeHandler %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID == "" {
			t.Fatalf("call %d requestId is empty", i+1)
		}
		if hc.Attempt != 1 || hc.MaxAttempts != 3 {
			t.Errorf("call %d attempt/maxAttempts = %d/%d, want 1/3", i+1, hc.Attempt, hc.MaxAttempts)
		}
		if hc.MessageID != "" {
			t.Errorf("call %d messageId = %q, want empty for a state-free replay", i+1, hc.MessageID)
		}
		if hc.App != "fn" || hc.Handler != "index.run" {
			t.Errorf("call %d app/handler = %q/%q, want fn/index.run", i+1, hc.App, hc.Handler)
		}
		requestIDs = append(requestIDs, hc.RequestID)
	}
	if requestIDs[0] == requestIDs[1] {
		t.Fatalf("state-free executions share requestId %q, want a fresh ID each time", requestIDs[0])
	}
}

// TestReplayDLQHandlerContextIsStateFree verifies a DLQ replay is a direct,
// state-free execution: a fresh request ID per replay, attempt 1, the original
// source message ID as provenance, and the replayed rule's configured budget —
// never a fabricated broker attempt.
func TestReplayDLQHandlerContextIsStateFree(t *testing.T) {
	exec := &handlerContextProbe{}
	r := NewWithMetrics([]*PreparedApp{replayFn(t, "fn", exec)}, testutil.DiscardLogger(), nil)

	var requestIDs []string
	for i := 0; i < 2; i++ {
		err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{"event_name":"INSERT"}`), "", "original-1")
		if err != nil {
			t.Fatalf("ReplayDLQ %d: %v", i+1, err)
		}
		hc := exec.at(i)
		if hc.RequestID == "" {
			t.Fatalf("replay %d requestId is empty", i+1)
		}
		if hc.Attempt != 1 || hc.MaxAttempts != 1 {
			t.Errorf("replay %d attempt/maxAttempts = %d/%d, want 1/1 (retries:0 rule)", i+1, hc.Attempt, hc.MaxAttempts)
		}
		if hc.MessageID != "original-1" {
			t.Errorf("replay %d messageId = %q, want the original source ID", i+1, hc.MessageID)
		}
		if hc.App != "fn" || hc.Handler != "events.created.handler" {
			t.Errorf("replay %d app/handler = %q/%q, want fn/events.created.handler", i+1, hc.App, hc.Handler)
		}
		requestIDs = append(requestIDs, hc.RequestID)
	}
	if requestIDs[0] == requestIDs[1] {
		t.Fatalf("replays share requestId %q, want a fresh ID each time", requestIDs[0])
	}
}

func TestStateFreeRequestIDRandomFailureIsUniqueConcurrently(t *testing.T) {
	const count = 64
	start := make(chan struct{})
	ids := make(chan string, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id := stateFreeRequestIDWithRandom(func([]byte) (int, error) {
				return 0, errors.New("random source unavailable")
			})
			ids <- id
		}()
	}
	close(start)
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, count)
	for id := range ids {
		if id == "" {
			t.Fatal("fallback request ID is empty")
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("fallback request ID collision: %q", id)
		}
		seen[id] = struct{}{}
	}
}

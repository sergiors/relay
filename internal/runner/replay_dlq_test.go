package runner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// recordingExecutor records every executed handler so a test can prove exactly
// which handler ran and how many times.
type recordingExecutor struct {
	mu       sync.Mutex
	handlers []string
	payloads [][]byte
}

func (e *recordingExecutor) Execute(_ context.Context, _ *runtime.Prepared, handler string, eventJSON []byte, _ []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, handler)
	e.payloads = append(e.payloads, append([]byte(nil), eventJSON...))
	return nil
}

func (e *recordingExecutor) got() ([]string, [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.handlers...), append([][]byte(nil), e.payloads...)
}

// replayFn builds a prepared function with one event rule AND one schedule entry
// for distinct handlers, so ReplayDLQ's handler validation can be exercised
// against both surfaces.
func replayFn(t *testing.T, name string, exec Executor) *PreparedFunction {
	t.Helper()
	return buildFn(fnSpec{
		name:  name,
		rules: []function.EventRule{{Handler: "events.created.handler", Pattern: function.Pattern{}, Timeout: time.Second, Retries: 0}},
		schedules: []function.Schedule{{
			Handler:  "jobs.cleanup.handler",
			Cron:     "0 3 * * *",
			Location: time.UTC,
			Timeout:  time.Second,
			Retries:  0,
		}},
	}, exec)
}

// TestReplayDLQExecutesExactHandlerOnce pins the core contract: ReplayDLQ runs
// EXACTLY the named handler once, never event matching — a second matching event
// rule in the same function is not executed.
func TestReplayDLQExecutesExactHandlerOnce(t *testing.T) {
	exec := &recordingExecutor{}
	// Two event rules: the replayed one and another that would match any event.
	pf := buildFn(fnSpec{
		name: "fn",
		rules: []function.EventRule{
			{Handler: "events.created.handler", Pattern: function.Pattern{}, Timeout: time.Second, Retries: 0},
			{Handler: "events.other.handler", Pattern: function.Pattern{}, Timeout: time.Second, Retries: 0},
		},
	}, exec)
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), metrics.New())

	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{"event_name":"INSERT"}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	handlers, payloads := exec.got()
	if len(handlers) != 1 {
		t.Fatalf("executed handlers = %v, want exactly the one replayed handler", handlers)
	}
	if handlers[0] != "events.created.handler" {
		t.Fatalf("executed handler = %q, want the exact replayed handler", handlers[0])
	}
	if !strings.Contains(string(payloads[0]), "INSERT") {
		t.Fatalf("payload = %q, want the replayed event", payloads[0])
	}
}

// TestReplayDLQScheduleHandler pins that a schedule-only handler (present in the
// template's schedules, not its event rules) is replayable.
func TestReplayDLQScheduleHandler(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{replayFn(t, "fn", exec)}, testutil.DiscardLogger())

	if err := r.ReplayDLQ(context.Background(), "fn", "jobs.cleanup.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

// deadlineExecutor records the invocation context's deadline, so a test can
// assert the exact handler timeout the runner imposed without sleeping.
type deadlineExecutor struct {
	mu       sync.Mutex
	deadline time.Time
	has      bool
}

func (e *deadlineExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte, _ []string) error {
	dl, ok := ctx.Deadline()
	e.mu.Lock()
	e.deadline, e.has = dl, ok
	e.mu.Unlock()
	return nil
}

func (e *deadlineExecutor) got() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.deadline, e.has
}

// TestReplayDLQUsesCurrentEventRuleTimeout pins the current-handler-timeout
// contract for an EVENT-rule handler: the replay must bound the handler by the
// CURRENT event rule's Timeout, not the function default. The rule timeout is
// deliberately far above function.DefaultTimeout so the two are distinguishable;
// the recorded deadline makes the assertion exact and sleep-free.
func TestReplayDLQUsesCurrentEventRuleTimeout(t *testing.T) {
	ruleTimeout := 45 * time.Second
	exec := &deadlineExecutor{}
	pf := buildFn(fnSpec{
		name:  "fn",
		rules: []function.EventRule{{Handler: "events.created.handler", Pattern: function.Pattern{}, Timeout: ruleTimeout, Retries: 0}},
	}, exec)
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())

	before := time.Now()
	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	deadline, ok := exec.got()
	if !ok {
		t.Fatal("replayed handler ran without a timeout deadline")
	}
	got := deadline.Sub(before)
	if got < ruleTimeout-time.Second || got > ruleTimeout+time.Second {
		t.Fatalf("handler timeout = %s, want the current rule timeout %s (not the %s default)", got, ruleTimeout, function.DefaultTimeout)
	}
}

// TestReplayDLQEventRuleTimeoutCap pins that the current event rule's timeout is
// still capped by the configured maximum. Like the manual-invoke cap test, the
// function is built directly so the rule can carry a timeout above the
// template-validation cap, isolating the runner's runtime cap as the thing under
// test. The cap (10s) is deliberately ABOVE function.DefaultTimeout (6s): the
// observed deadline is then distinguishable from both the raw rule timeout (1h,
// uncapped) and the function default (6s), so the assertion proves the resolved
// rule timeout was capped rather than the default being used.
func TestReplayDLQEventRuleTimeoutCap(t *testing.T) {
	const cap = 10 * time.Second
	exec := &deadlineExecutor{}
	pf := buildFn(fnSpec{
		name:  "fn",
		rules: []function.EventRule{{Handler: "events.created.handler", Pattern: function.Pattern{}, Timeout: time.Hour, Retries: 0}},
	}, exec)
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())
	r.SetMaxHandlerTimeout(cap)

	before := time.Now()
	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	deadline, ok := exec.got()
	if !ok {
		t.Fatal("replayed handler ran without a timeout deadline")
	}
	got := deadline.Sub(before)
	if got < cap-time.Second || got > cap+time.Second {
		t.Fatalf("handler timeout = %s, want the current rule timeout capped at %s (not the default %s)", got, cap, function.DefaultTimeout)
	}
}

// TestReplayDLQUsesCurrentRuntimeAndSecrets pins that replay resolves the
// function's CURRENT template env/secrets (the live runner path), not any stored
// snapshot.
func TestReplayDLQUsesCurrentRuntimeAndSecrets(t *testing.T) {
	exec := &captureExecutor{}
	prov := &fakeProvider{vals: map[string]string{"db-url": "postgres://secret"}}
	pf := fnWithEnv(t, "fn", exec,
		map[string]string{"API_URL": "https://api.example.com"},
		map[string]function.SecretRef{"DATABASE_URL": "db-url"})
	r := New([]*PreparedFunction{pf}, testutil.DiscardLogger())
	r.SetSecretProvider(prov)

	if err := r.ReplayDLQ(context.Background(), "fn", "index.run", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	joined := strings.Join(exec.gotEnv(), " ")
	for _, want := range []string{"API_URL=https://api.example.com", "DATABASE_URL=postgres://secret"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("extra env missing %q: %v", want, exec.gotEnv())
		}
	}
}

// TestReplayDLQUnknownFunction pins ErrFunctionNotFound for a function absent
// from the current registry.
func TestReplayDLQUnknownFunction(t *testing.T) {
	r := New(nil, testutil.DiscardLogger())
	err := r.ReplayDLQ(context.Background(), "ghost", "index.run", []byte(`{}`))
	if !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("err = %v, want ErrFunctionNotFound", err)
	}
}

// TestReplayDLQUnavailableFunction pins ErrFunctionUnavailable for a registered
// but unrunnable function.
func TestReplayDLQUnavailableFunction(t *testing.T) {
	r := New([]*PreparedFunction{NewUnavailable(function.Function{
		Name:     "broken",
		Template: &function.Template{Runtime: "node24"},
	})}, testutil.DiscardLogger())

	err := r.ReplayDLQ(context.Background(), "broken", "index.run", []byte(`{}`))
	if !errors.Is(err, ErrFunctionUnavailable) {
		t.Fatalf("err = %v, want ErrFunctionUnavailable", err)
	}
}

// TestReplayDLQRemovedHandler pins ErrHandlerNotFound when the function is
// present and runnable but the exact recorded handler is no longer in its
// current template. The executor must never run.
func TestReplayDLQRemovedHandler(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{replayFn(t, "fn", exec)}, testutil.DiscardLogger())

	err := r.ReplayDLQ(context.Background(), "fn", "events.removed.handler", []byte(`{}`))
	if !errors.Is(err, ErrHandlerNotFound) {
		t.Fatalf("err = %v, want ErrHandlerNotFound", err)
	}
	if exec.count() != 0 {
		t.Fatalf("executor calls = %d, want 0 for a removed handler", exec.count())
	}
}

// TestReplayDLQRecordsHandlerStats pins that a replay emits the normal handler
// success/failure execution metrics (like a manual invocation) but NO
// event/retry/DLQ counters: those belong to the broker lifecycle.
func TestReplayDLQRecordsHandlerStats(t *testing.T) {
	// Success: handler success metrics, no event/retry/DLQ.
	m := metrics.New()
	ok := &captureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{replayFn(t, "fn", ok)}, testutil.DiscardLogger(), m)
	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	// Snapshot() renders display names (the relay_ prefix is stripped), so the
	// assertions use the display forms the existing runner tests do.
	noBrokerCounters := []string{
		"events_received_total",
		"events_matched_total",
		"events_unmatched_total",
		"function_events_matched_total",
		"retries_total",
		"dlq_entries_total",
		"function_retries_total",
		"function_dlq_total",
	}
	got := m.Snapshot()
	if !strings.Contains(got, "handler_success_total") {
		t.Fatalf("expected handler success metric; got:\n%s", got)
	}
	for _, forbidden := range noBrokerCounters {
		if strings.Contains(got, forbidden) {
			t.Fatalf("replay must not write %s:\n%s", forbidden, got)
		}
	}

	// Failure: handler failure metric, still no event/retry/DLQ.
	m2 := metrics.New()
	bad := &countingExecutor{fail: true}
	r2 := NewWithMetrics([]*PreparedFunction{replayFn(t, "fn", bad)}, testutil.DiscardLogger(), m2)
	if err := r2.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`)); err == nil {
		t.Fatal("expected a failed replay")
	}
	got2 := m2.Snapshot()
	if !strings.Contains(got2, "handler_failure_total") {
		t.Fatalf("expected handler failure metric; got:\n%s", got2)
	}
	for _, forbidden := range noBrokerCounters {
		if strings.Contains(got2, forbidden) {
			t.Fatalf("failed replay must not write %s:\n%s", forbidden, got2)
		}
	}
}

// noopInvocationState implements stream.InvocationState and records every call,
// so a test can prove a replay never consults the broker lifecycle.
type noopInvocationState struct {
	mu    sync.Mutex
	calls int
}

func (n *noopInvocationState) touch() {
	n.mu.Lock()
	n.calls++
	n.mu.Unlock()
}

func (n *noopInvocationState) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls
}

func (n *noopInvocationState) IsComplete(string) bool { n.touch(); return false }
func (n *noopInvocationState) MarkComplete(string)    { n.touch() }
func (n *noopInvocationState) TryStart(string, time.Duration) (bool, int, time.Duration) {
	n.touch()
	return true, 1, 0
}
func (n *noopInvocationState) RecordFailure(string, time.Duration) { n.touch() }
func (n *noopInvocationState) MarkExhausted(string, int)           { n.touch() }
func (n *noopInvocationState) IsTerminal(string) bool              { n.touch(); return false }
func (n *noopInvocationState) ClaimClassification() (bool, error)  { n.touch(); return true, nil }

// TestReplayDLQWritesNoBrokerState pins that ReplayDLQ never consults the
// invocation-state machinery: even when a context carries an InvocationState
// (as the stream delivery path always does), a replay leaves it untouched.
func TestReplayDLQWritesNoBrokerState(t *testing.T) {
	exec := &countingExecutor{}
	r := New([]*PreparedFunction{replayFn(t, "fn", exec)}, testutil.DiscardLogger())

	state := &noopInvocationState{}
	ctx := stream.WithInvocationState(context.Background(), state)
	if err := r.ReplayDLQ(ctx, "fn", "events.created.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
	if state.count() != 0 {
		t.Fatalf("invocation-state calls = %d, want 0 (replay writes no broker state)", state.count())
	}
}

// TestReplayDLQStampsInvocationMeta pins the diagnostic RunMeta of a replayed
// handler: it carries the exact function and handler and no stream message ID
// (a replay has no source message). The type is the schedule marker because
// ReplayDLQ reuses InvokeHandler, the single-handler execution primitive whose
// container type is the schedule one; it remains a managed execution container
// for the sweep's relay.type + relay.hostname ownership predicate either way.
func TestReplayDLQStampsInvocationMeta(t *testing.T) {
	exec := &captureExecutor{}
	r := New([]*PreparedFunction{replayFn(t, "fn", exec)}, testutil.DiscardLogger())

	if err := r.ReplayDLQ(context.Background(), "fn", "events.created.handler", []byte(`{}`)); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}
	meta := exec.gotMeta()
	if meta.Function != "fn" || meta.Handler != "events.created.handler" {
		t.Fatalf("RunMeta = %+v", meta)
	}
	if meta.MessageID != "" {
		t.Fatalf("RunMeta.MessageID = %q, want empty (a replay has no source message)", meta.MessageID)
	}
	if meta.Type != runtime.ContainerTypeSchedule {
		t.Fatalf("RunMeta.Type = %q, want %q (InvokeHandler's marker)", meta.Type, runtime.ContainerTypeSchedule)
	}
}

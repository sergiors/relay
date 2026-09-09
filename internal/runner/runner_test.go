package runner

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// Set and Replace keep the function set sorted by name, so Names() and iteration
// order never depend on the order functions were discovered or swapped in.
func TestRegistryNamesDeterministic(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))
	reg := r.Registry()

	// Inserting "zeta" before "alpha": the slice must come out sorted anyway.
	reg.Set([]*PreparedFunction{newFn(t, "zeta"), newFn(t, "alpha")})
	reg.Replace("mid", newFn(t, "mid"))
	reg.Replace("beta", newFn(t, "beta"))

	if got := strings.Join(reg.Names(), ","); got != "alpha,beta,mid,zeta" {
		t.Fatalf("Names order not deterministic: got %q", got)
	}

	// Removal keeps the remainder sorted.
	reg.Replace("alpha", nil)
	if got := strings.Join(reg.Names(), ","); got != "beta,mid,zeta" {
		t.Fatalf("Names order after removal: got %q", got)
	}
}

// Records invocations without touching Docker, satisfying the runner's local
// executor interface.
type fakeExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	// Yield inside the invocation so registry swaps can interleave; the runner
	// must keep using its snapshot regardless.
	time.Sleep(time.Millisecond)
	return nil
}

func newFn(t *testing.T, name string) *PreparedFunction {
	t.Helper()
	return NewPrepared(
		function.Function{Name: name, Template: &function.Template{Runtime: "node24"}},
		&runtime.Prepared{Name: name, Image: "x"},
		&fakeExecutor{},
	)
}

// Runs Handle concurrently with registry swaps under -race and asserts the
// runner neither panics nor errors while the set is being replaced mid-iteration.
func TestHandleVsSwapSnapshotConsistency(t *testing.T) {
	r := New(nil, log.New(nil, "", 0))

	names := []string{"a", "b", "c"}
	var fns []*PreparedFunction
	for _, n := range names {
		fns = append(fns, newFn(t, n))
	}
	r.Registry().Set(fns)

	var swaps atomic.Int64
	stop := make(chan struct{})

	var swappers sync.WaitGroup
	for w := 0; w < 4; w++ {
		swappers.Add(1)
		go func() {
			defer swappers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Replace a single entry; occasionally mark it unavailable to
				// exercise the skip path.
				i := int(swaps.Add(1)) % len(names)
				if i%2 == 0 {
					r.Registry().Replace(names[i], NewUnavailable(function.Function{Name: names[i]}))
				} else {
					r.Registry().Replace(names[i], newFn(t, names[i]))
				}
			}
		}()
	}

	var running atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			running.Add(1)
			err := r.Handle(context.Background(), "m", map[string]any{"a": 1})
			running.Add(-1)
			if err != nil {
				t.Errorf("handle returned error: %v", err)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	swappers.Wait()
	<-done

	if swaps.Load() == 0 {
		t.Fatal("expected at least one swap during the test")
	}
}

// countingExecutor records how many times it was invoked, so a test can assert
// that a completed invocation is skipped (not executed) on redelivery.
type countingExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *countingExecutor) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil
}

func (f *countingExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// metaCaptureExecutor records the RunMeta the runner injects into ctx, so tests
// can assert the executor receives the diagnostic hostname/message/event labels
// without Docker.
type metaCaptureExecutor struct {
	mu   sync.Mutex
	meta runtime.RunMeta
}

func (m *metaCaptureExecutor) Execute(ctx context.Context, _ *runtime.Prepared, _ string, _ []byte) error {
	m.mu.Lock()
	m.meta = runtime.RunMetaFrom(ctx)
	m.mu.Unlock()
	return nil
}

func (m *metaCaptureExecutor) got() runtime.RunMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta
}

// TestHandleInjectsRunMeta verifies the runner stamps each invocation's
// diagnostic metadata (with the configured hostname) into the context the
// executor sees, without changing the Executor interface.
func TestHandleInjectsRunMeta(t *testing.T) {
	exec := &metaCaptureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	r.SetHostname("worker-9")

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"event_id": "evt_42", "event_name": "INSERT", "status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	want := runtime.RunMeta{
		Function:  "user-events",
		Handler:   "index.run",
		MessageID: "1757-0",
		EventID:   "evt_42",
		EventName: "INSERT",
		Hostname:  "worker-9",
		Image:     "x",
	}
	if got := exec.got(); got != want {
		t.Fatalf("RunMeta = %+v, want %+v", got, want)
	}
}

// TestHandleRunMetaEmptyHostnameWhenUnset verifies that not calling SetHostname
// yields an empty hostname label (never a panic) — the diagnostic hostname is
// best-effort.
func TestHandleRunMetaEmptyHostnameWhenUnset(t *testing.T) {
	exec := &metaCaptureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	// SetHostname deliberately NOT called.

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	meta := exec.got()
	if meta.Hostname != "" {
		t.Fatalf("expected empty hostname when SetHostname unset, got %q", meta.Hostname)
	}
	if meta.Function != "user-events" {
		t.Fatalf("expected Function set, got %q", meta.Function)
	}
}

// fakeInvocationState is an in-memory InvocationState for runner tests,
// avoiding a Redis dependency. It records which invocations have completed.
type fakeInvocationState struct {
	mu    sync.Mutex
	done  map[string]bool
	marks []string
}

func newFakeInvocationState() *fakeInvocationState {
	return &fakeInvocationState{done: map[string]bool{}}
}

func (p *fakeInvocationState) IsComplete(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[invocation]
}

func (p *fakeInvocationState) MarkComplete(invocation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[invocation] = true
	p.marks = append(p.marks, invocation)
}

// TestHandleSkipsCompletedInvocation verifies that when invocation state is
// present in ctx and an invocation already completed, Handle skips it: the
// executor is not called and no success/failure metrics are recorded for it.
func TestHandleSkipsCompletedInvocation(t *testing.T) {
	m := metrics.New()
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), m)

	// First delivery: no invocation state, so the handler runs and records success.
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}

	// Second delivery with invocation state marking the invocation already
	// complete: the handler must be skipped (not executed) and not counted as a
	// success.
	prog := newFakeInvocationState()
	prog.done["user-events/index.run"] = true
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1 (skipped)", exec.count())
	}

	got := m.Snapshot()
	// The skip must not add a second success invocation.
	if !strings.Contains(got, "handler_invocations_total{function=user-events,handler=index.run,outcome=success} count=1") {
		t.Errorf("expected exactly one success invocation; got:\n%s", got)
	}
	if strings.Contains(got, "outcome=failure") {
		t.Errorf("unexpected failure metric; got:\n%s", got)
	}
}

// TestHandleMarksCompleteOnExecution verifies that a successful execution
// records the invocation via MarkComplete so a later redelivery can skip it.
func TestHandleMarksCompleteOnExecution(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	if err := r.Handle(ctx, "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(prog.marks) != 1 || prog.marks[0] != "user-events/index.run" {
		t.Fatalf("marks = %v, want [user-events/index.run]", prog.marks)
	}
	if !prog.IsComplete("user-events/index.run") {
		t.Fatalf("invocation should be marked complete")
	}
}

// TestHandleNoInvocationStateBehavesAsBefore verifies that Handle without
// invocation state in ctx runs every matching handler (nil-safe, backward
// compatible).
func TestHandleNoInvocationStateBehavesAsBefore(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, silentLogger(), nil)
	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executor calls = %d, want 1", exec.count())
	}
}

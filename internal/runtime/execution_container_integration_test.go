//go:build integration

// This file exercises the REUSED execution container lifecycle end to end
// against a real Docker daemon: container reuse per function, discard paths
// (timeout, process exit, image change, shutdown), per-function isolation, and
// concurrency serialization. It complements docker_integration_test.go, which
// covers the image/build/label/sweep layer.
package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/metrics"
)

// newManagerWithIdleTimeout builds a Manager with an explicit warm-container
// idle timeout, used by the idle-eviction integration test (the default 5m
// window is impractical for a test). Close is registered as cleanup.
func newManagerWithIdleTimeout(t *testing.T, idle time.Duration) *Manager {
	t.Helper()
	m, err := NewManager(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		"test-host",
		WithWarmContainerIdleTimeout(idle),
	)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// newMetricsManager builds a Manager wired to a fresh metrics registry so the
// warm-container pool observability can be asserted end to end against a real
// Docker daemon. Close is registered as cleanup.
func newMetricsManager(t *testing.T) (*Manager, *metrics.Registry) {
	t.Helper()
	reg := metrics.New()
	m, err := NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)), reg, "test-host")
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, reg
}

// reusedContainerID polls (bridging start latency) for the running container
// carrying the function's label and returns its id.
func reusedContainerID(t *testing.T, ctx context.Context, m *Manager, fnName string) string {
	t.Helper()
	return waitForContainerByLabel(ctx, m.cli, labelFunction, fnName)
}

// TestIntegrationProcessExitDiscardsContainer verifies the process_exit
// discard: a handler that kills its own process produces an invocation error
// mentioning the exit status, the container is gone (process death +
// explicit remove), and the NEXT invocation creates a FRESH container.
func TestIntegrationProcessExitDiscardsContainer(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function die(event) {
  // Give the poll below time to observe the running container, then die
  // BEFORE any response frame is written: process.exit terminates the process
  // immediately (PID 1 in a container cannot be killed by signals — the
  // kernel ignores them — so process.kill would hang the test instead).
  await new Promise(r => setTimeout(r, 250));
  process.exit(1);
}
export function run(event) {
  console.log("resurrected");
}
`)
	fn := function.Function{Name: "proc-exit-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	id1 := ""
	// Run the killer in a goroutine so mid-flight we can capture the container
	// id of the FIRST container for the was-reused assertion below.
	done := make(chan error, 1)
	go func() {
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: "proc-exit-e2e", Handler: "index.die", Image: prepared.Image})
		done <- m.Execute(execCtx, prepared, "index.die", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	id1 = reusedContainerID(t, ctx, m, "proc-exit-e2e")
	if err := <-done; err == nil {
		t.Fatal("expected execute to fail for a handler that killed its own process")
	}
	if id1 == "" {
		t.Fatalf("expected the first container to have been running")
	}
	if !waitForContainerGone(ctx, m.cli, labelFunction, "proc-exit-e2e") {
		t.Error("process-exit container should have been discarded (removed by AutoRemove)")
	}

	// Next invocation: fresh container.
	secondCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "proc-exit-e2e", Handler: "index.run", Image: prepared.Image})
	if err := m.Execute(secondCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute after process exit: %v", err)
	}
	id2 := reusedContainerID(t, ctx, m, "proc-exit-e2e")
	if id2 == "" || id2 == id1 {
		t.Errorf("expected a FRESH container after process exit: id1=%s id2=%s", id1, id2)
	}
	if !strings.Contains(out.String(), "resurrected") {
		t.Errorf("expected handler output after restart, got: %s", out.String())
	}
}

// TestIntegrationImageChangeDiscardsContainer verifies the image_changed
// discard: a function's image version changes (source modified -> new
// fingerprint), the next Execute discards the old container (even healthy)
// and runs the NEW image; the old container is removed.
func TestIntegrationImageChangeDiscardsContainer(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(m.cli, "relay-fn-img-change:"))
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function run(e){ console.log('v1'); }\n")
	fn := function.Function{Name: "img-change", Dir: dir, Template: &function.Template{Runtime: "node24"}}

	p1, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "img-change", Image: p1.Image})
	if err := m.Execute(execCtx, p1, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute v1: %v", err)
	}
	id1 := reusedContainerID(t, ctx, m, "img-change")
	if id1 == "" {
		t.Fatal("expected the v1 container to be running")
	}

	// Change the source: new fingerprint -> new image.
	writeFile(t, dir, "index.js", "export function run(e){ console.log('v2'); }\n")
	p2, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image == p1.Image {
		t.Fatalf("v2 image = %q, want a different tag than v1", p2.Image)
	}

	if err := m.Execute(execCtx, p2, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute v2: %v", err)
	}
	id2 := reusedContainerID(t, ctx, m, "img-change")
	if id2 == "" || id2 == id1 {
		t.Errorf("expected a NEW container for the new image: id1=%s id2=%s", id1, id2)
	}
	if !strings.Contains(out.String(), "v2") {
		t.Errorf("expected v2 output, got: %s", out.String())
	}
}

// TestIntegrationTwoFunctionsDistinctContainers verifies no cross-function
// reuse: two functions, two distinct running container ids.
func TestIntegrationTwoFunctionsDistinctContainers(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var ids []string
	for _, name := range []string{"two-fn-a", "two-fn-b"} {
		dir := t.TempDir()
		writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
		writeFile(t, dir, "index.js", "export function run(e){ console.log('"+name+"'); }\n")
		fn := function.Function{Name: name, Dir: dir, Template: &function.Template{Runtime: "node24"}}
		prepared, err := m.Prepare(ctx, fn)
		if err != nil {
			t.Fatalf("prepare %s: %v", name, err)
		}
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: name, Image: prepared.Image})
		if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
			t.Fatalf("execute %s: %v", name, err)
		}
		ids = append(ids, reusedContainerID(t, ctx, m, name))
	}
	if len(ids) != 2 || ids[0] == "" || ids[1] == "" || ids[0] == ids[1] {
		t.Errorf("expected two distinct container ids, got %v", ids)
	}
}

// pollingSink is a concurrency-safe function-output sink that lets a test
// observe handler progress WHILE invocations are in flight. newFunctionOutputSink's
// bytes.Buffer cannot be read concurrently with the container output reader
// goroutines (and would race under -race), so this sink guards every write and
// read with a mutex.
type pollingSink struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (s *pollingSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *pollingSink) contains(sub string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Contains(s.buf.String(), sub)
}

func (s *pollingSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// waitForSinkContains polls until the sink holds sub or the deadline passes. It
// bridges container start plus output-forwarding latency without a fixed sleep.
func waitForSinkContains(ctx context.Context, sink *pollingSink, sub string) bool {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if sink.contains(sub) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(30 * time.Millisecond):
		}
	}
	return false
}

// TestIntegrationConcurrentInvocationsDistinctContainers verifies the warm pool
// at the function's resolved concurrency (2): two concurrent Execute calls for
// the SAME function lease DISTINCT containers and run concurrently, and a THIRD
// concurrent call CANNOT start until one of the first two releases — it is
// bounded by the pool instead of starting a third container — then runs on the
// released (reused) container. The scenario is made deterministic with handler
// timing as a barrier: A and B print a START line and then block in their
// handlers, so the test starts C only after observing BOTH mid-flight, and then
// asserts C has not started while both remain blocked.
func TestIntegrationConcurrentInvocationsDistinctContainers(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	sink := &pollingSink{}
	prev := SetFunctionOutput(sink)
	t.Cleanup(func() { SetFunctionOutput(prev) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 2
events:
  - handler: index.a
    pattern:
      event_name: [INSERT]
`)
	// A and B print a START marker immediately, then block (blockMs from the
	// event) before returning; C prints its marker and returns at once. The
	// markers are the observable barrier: seeing START a and START b means both
	// pool slots are leased and both handlers are mid-flight, so C must wait.
	writeFile(t, dir, "index.js", `
export async function a(event) {
  console.log("START a");
  await new Promise(r => setTimeout(r, event.blockMs));
  console.log("END a");
}
export async function b(event) {
  console.log("START b");
  await new Promise(r => setTimeout(r, event.blockMs));
  console.log("END b");
}
export async function c(event) {
  console.log("START c");
}
`)
	fn := function.Function{Name: "concurrent-e2e", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 2}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Concurrency != 2 {
		t.Fatalf("prepared concurrency = %d, want 2", prepared.Concurrency)
	}

	exec := func(handler, event string) error {
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: "concurrent-e2e", Handler: handler, Image: prepared.Image})
		return m.Execute(execCtx, prepared, handler, []byte(event), nil)
	}

	// A and B occupy both pool slots and block in their handlers long enough
	// for the assertions below (6s; the blocked-C window is <1s).
	aErr := make(chan error, 1)
	bErr := make(chan error, 1)
	go func() { aErr <- exec("index.a", `{"event_name":"INSERT","blockMs":6000}`) }()
	go func() { bErr <- exec("index.b", `{"event_name":"INSERT","blockMs":6000}`) }()

	if !waitForSinkContains(ctx, sink, "START a") || !waitForSinkContains(ctx, sink, "START b") {
		t.Fatalf("expected both handlers to start concurrently; sink:\n%s", sink.String())
	}
	if !waitForContainersCount(ctx, m.cli, labelFunction, "concurrent-e2e", 2) {
		t.Fatalf("expected 2 pooled containers while A and B run, got %d:\n%s",
			countContainersByLabel(ctx, m.cli, labelFunction, "concurrent-e2e"), sink.String())
	}

	// C is the third concurrent invocation. Both slots are leased and blocked,
	// so C must WAIT for a release rather than start a third container.
	cErr := make(chan error, 1)
	go func() { cErr <- exec("index.c", `{"event_name":"INSERT"}`) }()

	// Barrier: while A and B are still blocked (observed started above and
	// sleeping 6s), C must not have started and the pool must not exceed its
	// bound. A third container here would mean the pool is not bounded by the
	// function's concurrency.
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sink.contains("START c") {
			t.Fatalf("C started before A or B released:\n%s", sink.String())
		}
		if got := countContainersByLabel(ctx, m.cli, labelFunction, "concurrent-e2e"); got != 2 {
			t.Fatalf("pool exceeded its concurrency bound while C waited: %d containers", got)
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Once A or B releases, C runs on a released (reused) container.
	for i, ch := range []chan error{aErr, bErr, cErr} {
		if err := <-ch; err != nil {
			t.Fatalf("concurrent execute %d failed: %v", i, err)
		}
	}
	if !sink.contains("START c") {
		t.Errorf("C never ran after A or B released:\n%s", sink.String())
	}
	// Exactly the two pooled containers remain: C reused a released one rather
	// than starting a third.
	if got := countContainersByLabel(ctx, m.cli, labelFunction, "concurrent-e2e"); got != 2 {
		t.Errorf("pooled containers after execution = %d, want 2", got)
	}
	if strings.Contains(sink.String(), relayProtocolSentinel) {
		t.Errorf("protocol frames leaked to the function-output sink:\n%s", sink.String())
	}
}

// TestIntegrationPoolBoundedByConcurrency verifies the pool max is the
// function's resolved concurrency: with concurrency 1, three concurrent
// Execute calls serialize over a single container (never a second one), and all
// succeed.
func TestIntegrationPoolBoundedByConcurrency(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 1
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function run(event) {
  await new Promise(r => setTimeout(r, 150));
}
`)
	fn := function.Function{Name: "bounded-e2e", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 1}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Concurrency != 1 {
		t.Fatalf("prepared concurrency = %d, want 1", prepared.Concurrency)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			execCtx := context.WithValue(context.Background(), runMetaKey{},
				RunMeta{Hostname: "test-host", Function: "bounded-e2e", Image: prepared.Image})
			errCh <- m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil)
		}()
	}
	wg.Wait()
	for i := 0; i < 3; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("execute failed: %v", err)
		}
	}
	// Never more than the concurrency bound of containers, and exactly one
	// pooled after all three serialized over it.
	if got := countContainersByLabel(ctx, m.cli, labelFunction, "bounded-e2e"); got != 1 {
		t.Errorf("pooled containers = %d, want exactly 1 (concurrency bound)", got)
	}
}

// TestIntegrationInvalidateImage discards a healthy reused container via the
// image-invalidation hook the runner calls on retirement.
func TestIntegrationInvalidateImage(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function run(e){ console.log('ok'); }\n")
	fn := function.Function{Name: "invalidate-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "invalidate-e2e", Image: prepared.Image})
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	id1 := reusedContainerID(t, ctx, m, "invalidate-e2e")
	if id1 == "" {
		t.Fatal("expected a running container")
	}

	// Invalidating a DIFFERENT image must not touch ours.
	m.InvalidateImage("relay-fn-some-other-fn:abc")
	if id := reusedContainerID(t, ctx, m, "invalidate-e2e"); id != id1 {
		t.Errorf("container for another image must survive: id1=%s now=%s", id1, id)
	}

	// Invalidating OUR image discards the (healthy!) container promptly.
	m.InvalidateImage(prepared.Image)
	if !waitForContainerGone(ctx, m.cli, labelFunction, "invalidate-e2e") {
		t.Error("container should have been discarded on InvalidateImage")
	}

	// A following sweep clean invocation starts a new container (the entry was
	// cleared) and succeeds.
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute after invalidation: %v", err)
	}
}

// TestIntegrationIdleEviction verifies the warm-container idle timeout end to
// end against Docker: a healthy idle container is evicted after the configured
// window, and the next invocation starts a fresh container. The window is
// short (1s) because the production default (5m) is impractical here; the
// maintenance loop derives its tick from the timeout, so eviction happens
// within a small multiple of the window. No Docker protocol changes are
// involved.
func TestIntegrationIdleEviction(t *testing.T) {
	requireDocker(t)
	m := newManagerWithIdleTimeout(t, time.Second)
	out := newFunctionOutputSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function run(e){ console.log('ok'); }\n")
	fn := function.Function{Name: "idle-evict-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "idle-evict-e2e", Image: prepared.Image})
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	id1 := reusedContainerID(t, ctx, m, "idle-evict-e2e")
	if id1 == "" {
		t.Fatal("expected a warm container after execute")
	}

	// The container must be evicted within a bounded multiple of the 1s window
	// (tick = 500ms, so ~1.5s worst case; allow generous Docker latency).
	if !waitForContainerGone(ctx, m.cli, labelFunction, "idle-evict-e2e") {
		t.Fatal("idle container was not evicted after the configured timeout")
	}

	// The next invocation starts a fresh, distinct container.
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute after eviction: %v", err)
	}
	id2 := reusedContainerID(t, ctx, m, "idle-evict-e2e")
	if id2 == "" || id2 == id1 {
		t.Errorf("expected a FRESH container after eviction: id1=%s id2=%s", id1, id2)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("expected handler output after re-execution, got: %s", out.String())
	}
}

// TestIntegrationBusyImageChangeDrains verifies the generation/draining model
// end to end: while an old-image invocation is BUSY, a new image's Execute
// retires (not discards) the old busy container; when the invocation finishes,
// the old container is discarded and the new image is served by its own
// container. The old image is never reused.
func TestIntegrationBusyImageChangeDrains(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	sink := &pollingSink{}
	prev := SetFunctionOutput(sink)
	t.Cleanup(func() { SetFunctionOutput(prev) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Cleanup(cleanupImagePrefixes(m.cli, "relay-fn-busy-change:"))
	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 2
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	// v1 blocks (printing START/END v1 around a delay) so the test can hold an
	// old-image invocation in flight while v2 is prepared and executed.
	writeFile(t, dir, "index.js", `
export async function run(event) {
  console.log("START v1");
  await new Promise(r => setTimeout(r, event.blockMs));
  console.log("END v1");
}
`)
	fn := function.Function{Name: "busy-change", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 2}}
	p1, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v1: %v", err)
	}

	v1Ctx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "busy-change", Handler: "index.run", Image: p1.Image})
	v1Done := make(chan error, 1)
	go func() {
		v1Done <- m.Execute(v1Ctx, p1, "index.run", []byte(`{"event_name":"INSERT","blockMs":4000}`), nil)
	}()
	if !waitForSinkContains(ctx, sink, "START v1") {
		t.Fatalf("v1 did not start; sink:\n%s", sink.String())
	}

	// Change source -> v2 image while v1 is busy.
	writeFile(t, dir, "index.js", "export function run(e){ console.log('v2'); }\n")
	p2, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare v2: %v", err)
	}
	if p2.Image == p1.Image {
		t.Fatalf("v2 image = %q, want different from v1", p2.Image)
	}

	// A v2 execute must NOT block on the busy v1 container: it starts its own
	// container (the draining generation stays within capacity). The old v1
	// container must not be discarded mid-invocation.
	v2Ctx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "busy-change", Handler: "index.run", Image: p2.Image})
	if err := m.Execute(v2Ctx, p2, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute v2 while v1 busy: %v", err)
	}
	if !sink.contains("v2") {
		t.Errorf("v2 did not run; sink:\n%s", sink.String())
	}

	// Let v1 finish: its container is drained (discarded) rather than pooled.
	if err := <-v1Done; err != nil {
		t.Fatalf("v1 execute: %v", err)
	}
	if !sink.contains("END v1") {
		t.Errorf("expected v1 to complete; sink:\n%s", sink.String())
	}
	// Exactly the v2 container remains: the v1 container is gone.
	if !waitForContainersCount(ctx, m.cli, labelFunction, "busy-change", 1) {
		t.Errorf("expected exactly one pooled container after drain, got %d:\n%s",
			countContainersByLabel(ctx, m.cli, labelFunction, "busy-change"), sink.String())
	}
}

// TestIntegrationRemoveFunctionDiscardsContainers verifies the function-removal
// lifecycle end to end: m.RemoveFunction discards its idle warm container, a
// new Execute for the removed function fails with errPoolClosed (and the
// invocation is left pending, not run on stale state), and re-preparing the
// function warms it again.
func TestIntegrationRemoveFunctionDiscardsContainers(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", "export function run(e){ console.log('ok'); }\n")
	fn := function.Function{Name: "remove-fn-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "remove-fn-e2e", Image: prepared.Image})
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if id := reusedContainerID(t, ctx, m, "remove-fn-e2e"); id == "" {
		t.Fatal("expected a warm container")
	}

	m.RemoveFunction("remove-fn-e2e")
	if !waitForContainerGone(ctx, m.cli, labelFunction, "remove-fn-e2e") {
		t.Fatal("idle container should have been discarded on RemoveFunction")
	}

	// A new execute for the removed function must fail without running a
	// throwaway container (the invocation stays pending for replay).
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); !errors.Is(err, errPoolClosed) {
		t.Fatalf("execute after RemoveFunction = %v, want errPoolClosed", err)
	}
	if countContainersByLabel(ctx, m.cli, labelFunction, "remove-fn-e2e") != 0 {
		t.Fatal("a removed function must not start new warm containers")
	}

	// Re-preparing (reactivation) warms it again.
	if _, err := m.Prepare(ctx, fn); err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if err := m.Execute(execCtx, prepared, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute after reactivation: %v", err)
	}
	if id := reusedContainerID(t, ctx, m, "remove-fn-e2e"); id == "" {
		t.Fatal("expected a warm container after reactivation")
	}
}

// TestIntegrationPoolMetricsColdWarmDiscard drives the warm-container pool
// observability end to end against Docker: a first invocation is a cold start,
// a second reuses (warm), a timeout discards with the timeout reason, and the
// live gauges settle at the expected values. It also reads the /metrics
// exposition to prove the new series are scrapeable.
func TestIntegrationPoolMetricsColdWarmDiscard(t *testing.T) {
	requireDocker(t)
	m, reg := newMetricsManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.ok
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function ok(event) { console.log("ok"); }
export async function slow(event) {
  await new Promise(r => setTimeout(r, 10000));
}
`)
	fn := function.Function{Name: "pool-metrics-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	exec := func(handler string, timeout time.Duration) error {
		ectx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: "pool-metrics-e2e", Handler: handler, Image: prepared.Image})
		if timeout > 0 {
			var cancelFn context.CancelFunc
			ectx, cancelFn = context.WithTimeout(ectx, timeout)
			defer cancelFn()
		}
		return m.Execute(ectx, prepared, handler, []byte(`{"event_name":"INSERT"}`), nil)
	}

	// First invocation: cold.
	if err := exec("index.ok", 0); err != nil {
		t.Fatalf("execute 1: %v", err)
	}
	if got := poolAcquireCount(reg, "pool-metrics-e2e", metrics.RuntimeOutcomeCold); got != 1 {
		t.Fatalf("cold acquires = %d, want 1", got)
	}

	// Second invocation: warm (same container reused).
	if err := exec("index.ok", 0); err != nil {
		t.Fatalf("execute 2: %v", err)
	}
	if got := poolAcquireCount(reg, "pool-metrics-e2e", metrics.RuntimeOutcomeWarm); got != 1 {
		t.Fatalf("warm acquires = %d, want 1", got)
	}

	// Capacity gauge is the function's resolved concurrency.
	if got := reg.GaugeLabels(metrics.MetricRuntimePoolCapacity,
		[]metrics.Label{{Name: "function", Value: "pool-metrics-e2e"}}); got != float64(function.DefaultConcurrency) {
		t.Fatalf("capacity gauge = %v, want %d", got, function.DefaultConcurrency)
	}

	// Timeout discards the container with the timeout reason.
	if err := exec("index.slow", 1*time.Second); err == nil {
		t.Fatal("expected the slow handler to time out")
	}
	if got := reg.CounterLabels(metrics.MetricRuntimeContainerDiscards,
		[]metrics.Label{{Name: "function", Value: "pool-metrics-e2e"}, {Name: "reason", Value: "timeout"}}); got != 1 {
		t.Fatalf("timeout discards = %d, want 1", got)
	}
	if !waitForContainerGone(ctx, m.cli, labelFunction, "pool-metrics-e2e") {
		t.Error("timed-out container should have been discarded")
	}

	// The new series are scrapeable on /metrics.
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"relay_runtime_container_acquires_total",
		"relay_runtime_container_discards_total",
		"relay_runtime_container_acquire_duration_seconds_count",
		"relay_runtime_pool_capacity",
		`relay_runtime_containers{function="pool-metrics-e2e",state="idle"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, body)
		}
	}
}

// poolAcquireCount reads one outcome's acquire counter for an integration test.
func poolAcquireCount(reg *metrics.Registry, fn, outcome string) int64 {
	return reg.CounterLabels(metrics.MetricRuntimeContainerAcquires,
		[]metrics.Label{{Name: "function", Value: fn}, {Name: "outcome", Value: outcome}})
}

// TestIntegrationPoolMetricsConcurrent drives concurrent invocations of the
// same function end to end and asserts the pool observes only cold starts (all
// pool slots start fresh), the starting gauge rolls back to zero, and the busy
// gauge equals the function's concurrency.
func TestIntegrationPoolMetricsConcurrent(t *testing.T) {
	requireDocker(t)
	m, reg := newMetricsManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 2
events:
  - handler: index.sleep
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function sleep(event) {
  await new Promise(r => setTimeout(r, 3000));
  console.log("done");
}
`)
	fn := function.Function{Name: "pool-metrics-conc", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 2}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Concurrency != 2 {
		t.Fatalf("prepared concurrency = %d, want 2", prepared.Concurrency)
	}

	const n = 2
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ectx := context.WithValue(context.Background(), runMetaKey{},
				RunMeta{Hostname: "test-host", Function: "pool-metrics-conc", Handler: "index.sleep", Image: prepared.Image})
			errCh <- m.Execute(ectx, prepared, "index.sleep", []byte(`{"event_name":"INSERT"}`), nil)
		}()
	}
	// Wait until both containers are leased (busy == 2).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if reg.GaugeLabels(metrics.MetricRuntimeContainers,
			[]metrics.Label{{Name: "function", Value: "pool-metrics-conc"}, {Name: "state", Value: metrics.RuntimeStateBusy}}) == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := reg.GaugeLabels(metrics.MetricRuntimeContainers,
		[]metrics.Label{{Name: "function", Value: "pool-metrics-conc"}, {Name: "state", Value: metrics.RuntimeStateBusy}}); got != 2 {
		t.Fatalf("busy gauge during concurrent run = %v, want 2", got)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent execute: %v", err)
		}
	}
	if got := poolAcquireCount(reg, "pool-metrics-conc", metrics.RuntimeOutcomeCold); got != 2 {
		t.Fatalf("cold acquires = %d, want 2", got)
	}
	if got := poolAcquireCount(reg, "pool-metrics-conc", metrics.RuntimeOutcomeWarm); got != 0 {
		t.Fatalf("warm acquires = %d, want 0", got)
	}
	for _, state := range []string{metrics.RuntimeStateStarting} {
		if got := reg.GaugeLabels(metrics.MetricRuntimeContainers,
			[]metrics.Label{{Name: "function", Value: "pool-metrics-conc"}, {Name: "state", Value: state}}); got != 0 {
			t.Fatalf("%s gauge after run = %v, want 0", state, got)
		}
	}
}

// TestIntegrationPrepareResizesLivePool drives the full Prepare -> pool-resize
// path end to end against Docker: a function prepared at concurrency 1 warms a
// pool bounded by 1, then a hot-swapped template raising concurrency propagates
// the new bound to the ALREADY-CREATED pool through Manager.Prepare (no restart
// and no new Manager). The capacity gauge, live snapshot, and acquisition bound
// must all reflect the new value.
func TestIntegrationPrepareResizesLivePool(t *testing.T) {
	requireDocker(t)
	m, reg := newMetricsManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "index.js", "export function run(e){ console.log('ok'); }\n")
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 1
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	fn := function.Function{Name: "prepare-resize-e2e", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 1}}
	p1, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare concurrency 1: %v", err)
	}
	if p1.Concurrency != 1 {
		t.Fatalf("prepared concurrency = %d, want 1", p1.Concurrency)
	}
	// Warm the pool at 1.
	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "prepare-resize-e2e", Image: p1.Image})
	if err := m.Execute(execCtx, p1, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute at concurrency 1: %v", err)
	}
	if s, ok := m.PoolSnapshot("prepare-resize-e2e"); !ok || s.Capacity != 1 {
		t.Fatalf("snapshot after concurrency 1 = %+v, ok=%v; want capacity 1", s, ok)
	}

	// Hot-swap template.yaml to concurrency 3 (changing the function content, so
	// the reconciler rebuilds) and re-Prepare.
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 3
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	fn.Template = &function.Template{Runtime: "node24", Concurrency: 3}
	p2, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare concurrency 3: %v", err)
	}
	if p2.Concurrency != 3 {
		t.Fatalf("re-prepared concurrency = %d, want 3", p2.Concurrency)
	}

	// The already-created pool's bound follows the successful Prepare.
	if got := reg.GaugeLabels(metrics.MetricRuntimePoolCapacity,
		[]metrics.Label{{Name: "function", Value: "prepare-resize-e2e"}}); got != 3 {
		t.Fatalf("capacity gauge after re-prepare = %v, want 3", got)
	}
	s, ok := m.PoolSnapshot("prepare-resize-e2e")
	if !ok || s.Capacity != 3 {
		t.Fatalf("snapshot after re-prepare = %+v, ok=%v; want capacity 3", s, ok)
	}

	// Admission opened: the new image's containers now fit up to 3 concurrently.
	execCtx2 := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "prepare-resize-e2e", Image: p2.Image})
	if err := m.Execute(execCtx2, p2, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute after re-prepare: %v", err)
	}
}

// TestIntegrationPrepareClipsConcurrencyToGlobal drives the MAX_CONCURRENCY clip
// end to end against Docker: a function whose template asks for concurrency 15
// under the default worker-global cap of 8 is prepared with an EFFECTIVE bound
// of 8, and its warm pool's capacity gauge and live snapshot report 8 (not 15).
// A later hot-swap to concurrency 4 re-clips to 4. This proves the effective
// min(function concurrency, MAX_CONCURRENCY) — not the raw template value —
// drives runtime pool capacity.
func TestIntegrationPrepareClipsConcurrencyToGlobal(t *testing.T) {
	requireDocker(t)
	// newMetricsManager builds via NewManager with no WithMaxConcurrency, so the
	// worker-global cap is the package default (8), mirroring config's default.
	m, reg := newMetricsManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "index.js", "export function run(e){ console.log('ok'); }\n")
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 15
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	fn := function.Function{Name: "clip-e2e", Dir: dir, Template: &function.Template{Runtime: "node24", Concurrency: 15}}
	p1, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare concurrency 15: %v", err)
	}
	if p1.Concurrency != 8 {
		t.Fatalf("prepared concurrency = %d, want 8 (clipped to MAX_CONCURRENCY)", p1.Concurrency)
	}
	// Warm the pool and verify the clipped bound, not 15.
	execCtx := context.WithValue(context.Background(), runMetaKey{},
		RunMeta{Hostname: "test-host", Function: "clip-e2e", Image: p1.Image})
	if err := m.Execute(execCtx, p1, "index.run", []byte(`{"event_name":"INSERT"}`), nil); err != nil {
		t.Fatalf("execute at clipped concurrency 8: %v", err)
	}
	if got := reg.GaugeLabels(metrics.MetricRuntimePoolCapacity,
		[]metrics.Label{{Name: "function", Value: "clip-e2e"}}); got != 8 {
		t.Fatalf("capacity gauge = %v, want 8 (clipped)", got)
	}
	if s, ok := m.PoolSnapshot("clip-e2e"); !ok || s.Capacity != 8 {
		t.Fatalf("snapshot = %+v, ok=%v; want capacity 8", s, ok)
	}

	// Hot-swap to concurrency 4 (below the cap): the live pool follows to 4.
	writeFile(t, dir, "template.yaml", `
runtime: node24
concurrency: 4
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
`)
	fn.Template = &function.Template{Runtime: "node24", Concurrency: 4}
	p2, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare concurrency 4: %v", err)
	}
	if p2.Concurrency != 4 {
		t.Fatalf("re-prepared concurrency = %d, want 4 (below cap, untouched)", p2.Concurrency)
	}
	if got := reg.GaugeLabels(metrics.MetricRuntimePoolCapacity,
		[]metrics.Label{{Name: "function", Value: "clip-e2e"}}); got != 4 {
		t.Fatalf("capacity gauge after re-prepare = %v, want 4", got)
	}
	if s, ok := m.PoolSnapshot("clip-e2e"); !ok || s.Capacity != 4 {
		t.Fatalf("snapshot after re-prepare = %+v, ok=%v; want capacity 4", s, ok)
	}
}

//go:build integration

// This file exercises the REUSED execution container lifecycle end to end
// against a real Docker daemon: container reuse per function, discard paths
// (timeout, process exit, image change, shutdown), per-function isolation, and
// concurrency serialization. It complements docker_integration_test.go, which
// covers the image/build/label/sweep layer.
package runtime

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
)

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

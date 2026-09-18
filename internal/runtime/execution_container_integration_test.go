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

// TestIntegrationConcurrentInvocationsSerialize verifies two concurrent
// Execute calls for the SAME function both succeed (phase-1 serialization does
// not corrupt the protocol) on the same container, with different handlers.
func TestIntegrationConcurrentInvocationsSerialize(t *testing.T) {
	requireDocker(t)
	m, _ := newManager(t)
	out := newFunctionOutputSink(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir := t.TempDir()
	writeFile(t, dir, "template.yaml", `
runtime: node24
events:
  - handler: index.one
    pattern:
      event_name: [INSERT]
`)
	writeFile(t, dir, "index.js", `
export async function one(event) {
  await new Promise(r => setTimeout(r, 300));
  console.log("one " + event.n);
}
export async function two(event) {
  await new Promise(r => setTimeout(r, 200));
  console.log("two " + event.msg);
}
`)
	fn := function.Function{Name: "concurrent-e2e", Dir: dir, Template: &function.Template{Runtime: "node24"}}
	prepared, err := m.Prepare(ctx, fn)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: "concurrent-e2e", Image: prepared.Image})
		errCh <- m.Execute(execCtx, prepared, "index.one", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	go func() {
		defer wg.Done()
		execCtx := context.WithValue(context.Background(), runMetaKey{},
			RunMeta{Hostname: "test-host", Function: "concurrent-e2e", Image: prepared.Image})
		errCh <- m.Execute(execCtx, prepared, "index.two", []byte(`{"event_name":"INSERT"}`), nil)
	}()
	wg.Wait()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent execute failed: %v", err)
		}
	}
	logs := out.String()
	if !strings.Contains(logs, "one") && !strings.Contains(logs, "two") {
		t.Errorf("expected both handler outputs, got:\n%s", logs)
	}
	if strings.Contains(logs, relayProtocolSentinel) {
		t.Errorf("protocol frames leaked to the function-output sink:\n%s", logs)
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

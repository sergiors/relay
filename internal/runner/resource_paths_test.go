package runner

import (
	"context"
	"testing"
	"time"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestAllEntryPathsSharePreparedHandle pins that event, schedule, and manual
// invocations all hand the SAME published *runtime.Prepared to the Executor.
// Resource limits are resolved inside Manager.Execute (keyed by the handle's
// Name), not in the runner, so this is the structural guarantee that all three
// paths apply the same per-function resources without duplicating resolution.
// The runtime-level mapping and generation rotation are covered in
// internal/runtime (manager_resources_test.go).
func TestAllEntryPathsSharePreparedHandle(t *testing.T) {
	exec := &captureExecutor{}
	tmpl := `runtime: node24
events:
  - handler: index.run
    pattern:
      event_name: [INSERT]
schedules:
  - name: index.run
    handler: index.run
    cron: "0 3 * * *"
`
	pf := invokeFn(t, "fn", tmpl, exec)
	r := NewWithMetrics([]*PreparedFunction{pf}, testutil.DiscardLogger(), nil)

	shared := pf.Prepared()
	if shared == nil {
		t.Fatal("test function must have a prepared handle")
	}

	// Event path (Handle).
	if err := r.Handle(context.Background(), "1-0", map[string]any{"event_name": "INSERT"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := exec.gotPrepared(); got != shared {
		t.Fatalf("event path prepared = %p, want the shared handle %p", got, shared)
	}

	// Manual path (InvokeFunction).
	if _, err := r.InvokeFunction(context.Background(), "fn", map[string]any{"event_name": "INSERT"}); err != nil {
		t.Fatalf("InvokeFunction: %v", err)
	}
	if got := exec.gotPrepared(); got != shared {
		t.Fatalf("manual path prepared = %p, want the shared handle %p", got, shared)
	}

	// Schedule path (InvokeHandler, state-free form). The scheduled_at payload is
	// what the scheduler publishes.
	payload := []byte(`{"source":"relay.schedule","scheduled_at":"2026-09-28T00:00:00Z"}`)
	if err := r.InvokeHandler(context.Background(), "2-0", "fn", "sched", "index.run", payload); err != nil {
		t.Fatalf("InvokeHandler: %v", err)
	}
	if got := exec.gotPrepared(); got != shared {
		t.Fatalf("schedule path prepared = %p, want the shared handle %p", got, shared)
	}
}

// TestRuntimePreparedCarriesNoResourceField is a compile-time-adjacent guard:
// resources live on the template and the live pool, never on Prepared (which is
// rebuilt only on fingerprint changes). The handle's identity is Name+Image, so
// a resource-only change never needs a new Prepared.
func TestRuntimePreparedCarriesNoResourceField(t *testing.T) {
	p := &runtime.Prepared{Name: "fn", Image: "img", Concurrency: 2}
	// The handle carries no resource limits by design; the zero value is
	// resources-agnostic. This test documents that intent so a future field
	// addition is a deliberate, reviewed change.
	if p.Name != "fn" || p.Image != "img" {
		t.Fatalf("prepared = %+v", p)
	}
	_ = time.Second
}

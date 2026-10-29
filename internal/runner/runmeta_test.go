package runner

import (
	"context"
	"testing"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestHandleInjectsRunMeta verifies the runner stamps each invocation's
// diagnostic metadata (with the configured node ID) into the context the
// executor sees, without changing the Executor interface.
func TestHandleInjectsRunMeta(t *testing.T) {
	exec := &captureExecutor{}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)
	r.SetNodeID("worker-9")

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"event_id": "evt_42", "event_name": "INSERT", "status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	want := runtime.RunMeta{
		Type:      runtime.ContainerTypeEvent,
		App:       "user-events",
		Handler:   "index.run",
		MessageID: "1757-0",
		EventID:   "evt_42",
		EventName: "INSERT",
		NodeID:    "worker-9",
		Image:     "x",
	}
	if got := exec.gotMeta(); got != want {
		t.Fatalf("RunMeta = %+v, want %+v", got, want)
	}
}

// TestHandleRunMetaEmptyNodeIDWhenUnset verifies that not calling SetNodeID
// yields an empty node_id label (never a panic) — the diagnostic node ID is
// best-effort.
func TestHandleRunMetaEmptyNodeIDWhenUnset(t *testing.T) {
	exec := &captureExecutor{}
	r := NewWithMetrics([]*PreparedApp{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)
	// SetNodeID deliberately NOT called.

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	meta := exec.gotMeta()
	if meta.NodeID != "" {
		t.Fatalf("expected empty node ID when SetNodeID unset, got %q", meta.NodeID)
	}
	if meta.App != "user-events" {
		t.Fatalf("expected App set, got %q", meta.App)
	}
}

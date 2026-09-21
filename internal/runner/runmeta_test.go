package runner

import (
	"context"
	"testing"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestHandleInjectsRunMeta verifies the runner stamps each invocation's
// diagnostic metadata (with the configured hostname) into the context the
// executor sees, without changing the Executor interface.
func TestHandleInjectsRunMeta(t *testing.T) {
	exec := &captureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)
	r.SetHostname("worker-9")

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"event_id": "evt_42", "event_name": "INSERT", "status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}

	want := runtime.RunMeta{
		Type:      runtime.ContainerTypeEvent,
		Function:  "user-events",
		Handler:   "index.run",
		MessageID: "1757-0",
		EventID:   "evt_42",
		EventName: "INSERT",
		Hostname:  "worker-9",
		Image:     "x",
	}
	if got := exec.gotMeta(); got != want {
		t.Fatalf("RunMeta = %+v, want %+v", got, want)
	}
}

// TestHandleRunMetaEmptyHostnameWhenUnset verifies that not calling SetHostname
// yields an empty hostname label (never a panic) — the diagnostic hostname is
// best-effort.
func TestHandleRunMetaEmptyHostnameWhenUnset(t *testing.T) {
	exec := &captureExecutor{}
	r := NewWithMetrics([]*PreparedFunction{alwaysMatchFn(t, "user-events", exec)}, testutil.DiscardLogger(), nil)
	// SetHostname deliberately NOT called.

	if err := r.Handle(context.Background(), "1757-0", map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	meta := exec.gotMeta()
	if meta.Hostname != "" {
		t.Fatalf("expected empty hostname when SetHostname unset, got %q", meta.Hostname)
	}
	if meta.Function != "user-events" {
		t.Fatalf("expected Function set, got %q", meta.Function)
	}
}

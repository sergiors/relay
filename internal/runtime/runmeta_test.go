package runtime

import (
	"context"
	"testing"
)

// TestRunLabels verifies that runLabels maps every RunMeta field to the exact
// seven diagnostic label keys, that empty values are preserved as empty labels
// (total, greppable set), and that no unexpected keys are introduced. The label
// set deliberately contains only bounded identifiers — never payload content.
func TestRunLabels(t *testing.T) {
	meta := RunMeta{
		Function:  "user-events",
		Handler:   "events.created.handler",
		MessageID: "1791234567890-0",
		EventID:   "evt_123",
		EventName: "INSERT",
		Hostname:  "worker-1",
		Image:     "relay-fn-user-events:632aca75fa306911",
	}
	got := runLabels(meta)

	want := map[string]string{
		labelFunction:  "user-events",
		labelHandler:   "events.created.handler",
		labelMessageID: "1791234567890-0",
		labelEventID:   "evt_123",
		labelEventName: "INSERT",
		labelHostname:  "worker-1",
		labelImage:     "relay-fn-user-events:632aca75fa306911",
	}

	if len(got) != len(want) {
		t.Fatalf("label count = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %q = %q, want %q", k, got[k], v)
		}
	}
	// Reverse check: no unexpected keys.
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected label key %q", k)
		}
	}
}

// TestRunLabelsEmptyValues verifies empty RunMeta fields are still materialized
// as empty labels, so every Relay execution container carries the full label set
// even when a value is unavailable. This is what keeps the sweep's grep-ability
// and total set stable.
func TestRunLabelsEmptyValues(t *testing.T) {
	got := runLabels(RunMeta{})
	if len(got) != 7 {
		t.Fatalf("label count with empty meta = %d, want 7", len(got))
	}
	for _, k := range []string{
		labelFunction, labelHandler, labelMessageID, labelEventID,
		labelEventName, labelHostname, labelImage,
	} {
		if v, ok := got[k]; !ok || v != "" {
			t.Errorf("expected empty label %q, got %q (present=%v)", k, v, ok)
		}
	}
}

// TestRunMetaFromDefaults verifies RunMetaFrom is safe on a context without a
// RunMeta: it yields the zero value, never a panic.
func TestRunMetaFromDefaults(t *testing.T) {
	meta := RunMetaFrom(context.Background())
	if meta != (RunMeta{}) {
		t.Fatalf("RunMetaFrom(background) = %+v, want zero value", meta)
	}

	want := RunMeta{Function: "f", Hostname: "h"}
	ctx := WithRunMeta(context.Background(), want)
	if got := RunMetaFrom(ctx); got != want {
		t.Fatalf("RunMetaFrom = %+v, want %+v", got, want)
	}
}

package state

import (
	"testing"
	"time"
)

// TestRecordInvalidDesiredFreshRowIsUnavailable pins the fresh invalid case: a
// present-but-invalid desired definition with no prior row is INSERTED (not
// silently absent), recorded as unavailable (no usable active generation),
// failed with the error, and carries no fabricated configuration snapshot.
func TestRecordInvalidDesiredFreshRowIsUnavailable(t *testing.T) {
	c := openTestState(t)

	c.RecordInvalidDesired("broken", &boomErr{})

	detail, ok := c.GetApp("broken")
	if !ok {
		t.Fatal("a present invalid function must be visible as a row")
	}
	if detail.Status != StatusUnavailable {
		t.Fatalf("status = %q, want unavailable (no usable generation)", detail.Status)
	}
	if detail.LastReconcileStatus != ReconcileFailed {
		t.Fatalf("last_reconcile_status = %q, want failed", detail.LastReconcileStatus)
	}
	if detail.LastReconcileAt == "" {
		t.Fatal("last_reconcile_at must be stamped")
	}
	if detail.LastError != "boom" {
		t.Fatalf("last_error = %q, want boom", detail.LastError)
	}
	if detail.Image != "" || detail.Fingerprint != "" || detail.PreparedAt != "" {
		t.Fatalf("fresh invalid row must have no active generation: image=%q fingerprint=%q prepared_at=%q",
			detail.Image, detail.Fingerprint, detail.PreparedAt)
	}
	if detail.DesiredFingerprint != "" {
		t.Fatalf("desired_fingerprint = %q, want empty for an invalid desired definition", detail.DesiredFingerprint)
	}
}

// TestRecordInvalidDesiredPreservesActiveGeneration pins the key invariant: an
// invalid desired definition retains the last usable active generation
// (image/fingerprint/prepared_at) untouched — the previously-serving version is
// not erased and its image stays in the startup keep-set — while status becomes
// degraded and the failure is visible.
func TestRecordInvalidDesiredPreservesActiveGeneration(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)
	fn := fnFor(t, "fn", tmpl)
	prepared := time.Now().Add(-time.Hour)

	c.RecordReconcileSuccess("fn", "img-active", "fp-active", prepared, fn)

	c.RecordInvalidDesired("fn", &boomErr{})

	detail, ok := c.GetApp("fn")
	if !ok {
		t.Fatal("expected row after invalid desired")
	}
	if detail.Status != StatusDegraded {
		t.Fatalf("status = %q, want degraded (prior usable generation retained)", detail.Status)
	}
	if detail.Image != "img-active" || detail.Fingerprint != "fp-active" {
		t.Fatalf("active generation = %q/%q, want preserved img-active/fp-active", detail.Image, detail.Fingerprint)
	}
	if detail.PreparedAt != prepared.UTC().Format(time.RFC3339) {
		t.Fatalf("prepared_at = %q, want preserved %q", detail.PreparedAt, prepared.UTC().Format(time.RFC3339))
	}
	if detail.LastReconcileStatus != ReconcileFailed || detail.LastError != "boom" {
		t.Fatalf("outcome = status=%q error=%q, want failed/boom", detail.LastReconcileStatus, detail.LastError)
	}
	// The invalid definition's untrustworthy snapshot is cleared: no stale
	// desired fingerprint or template-derived fields remain.
	if detail.DesiredFingerprint != "" {
		t.Fatalf("desired_fingerprint = %q, want cleared for an invalid desired definition", detail.DesiredFingerprint)
	}
	if len(detail.Handlers) != 0 || len(detail.Schedules) != 0 || len(detail.Services) != 0 {
		t.Fatalf("template-derived snapshot must be cleared: handlers=%d schedules=%d services=%d",
			len(detail.Handlers), len(detail.Schedules), len(detail.Services))
	}
}

// TestRecordInvalidDesiredObserverNotified pins the status observer consistency:
// an invalid desired write notifies with the resulting public status so the
// one-hot gauge never keeps a stale ready value.
func TestRecordInvalidDesiredObserverNotified(t *testing.T) {
	c := openTestState(t)
	observer, events := recordingObserver()
	c.SetStatusObserver(observer)

	tmpl := mustTemplate(t, twoHandlerTmpl)
	c.RecordReconcileSuccess("fn", "img", "fp", time.Now(), fnFor(t, "fn", tmpl))
	*events = nil

	c.RecordInvalidDesired("fn", &boomErr{})
	if len(*events) != 1 || (*events)[0].name != "fn" || (*events)[0].status != StatusDegraded {
		t.Fatalf("events = %+v, want one degraded notification for fn", *events)
	}

	c.RecordInvalidDesired("fresh", &boomErr{})
	last := (*events)[len(*events)-1]
	if last.name != "fresh" || last.status != StatusUnavailable {
		t.Fatalf("last event = %+v, want unavailable for fresh", last)
	}
}

// TestActiveImagesListsRecordedImages pins the narrow read seam: only apps
// with a recorded image appear, keyed by name; an app with no image (never
// prepared) is omitted.
func TestActiveImagesListsRecordedImages(t *testing.T) {
	c := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl)

	c.RecordReconcileSuccess("with-img", "img-with", "fp", time.Now(), fnFor(t, "with-img", tmpl))
	c.RecordDiscovered(fnFor(t, "no-img", tmpl))

	got := c.ActiveImages()
	if got["with-img"] != "img-with" {
		t.Fatalf("ActiveImages[with-img] = %q, want img-with", got["with-img"])
	}
	if _, ok := got["no-img"]; ok {
		t.Fatalf("ActiveImages must omit a function with no recorded image: %v", got)
	}
}

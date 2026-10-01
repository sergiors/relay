package state

import (
	"testing"
	"time"
)

const schedulesTmpl = `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - name: jobs.report.handler
    handler: jobs.report.handler
    cron: "0 8 * * 1-5"
    timezone: Europe/Rome
    timeout: 20s
`

// TestScheduleRoundTrip seeds a template with schedules through
// RecordReconcileSuccess and asserts GetApp returns the resolved rows
// (name, handler, verbatim cron, effective timezone name, resolved timeout).
func TestScheduleRoundTrip(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, schedulesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Schedules) != 2 {
		t.Fatalf("schedules = %d, want 2", len(detail.Schedules))
	}
	// Rows are ordered by NAME.
	s0 := detail.Schedules[0]
	if s0.Name != "jobs.cleanup.handler" || s0.Handler != "jobs.cleanup.handler" || s0.Cron != "0 3 * * *" || s0.Timezone != "UTC" {
		t.Fatalf("schedule 0 = %+v", s0)
	}
	if s0.Timeout != 6*time.Second {
		t.Fatalf("schedule 0 timeout = %s, want default 6s", s0.Timeout)
	}
	s1 := detail.Schedules[1]
	if s1.Name != "jobs.report.handler" || s1.Handler != "jobs.report.handler" || s1.Cron != "0 8 * * 1-5" || s1.Timezone != "Europe/Rome" {
		t.Fatalf("schedule 1 = %+v", s1)
	}
	if s1.Timeout != 20*time.Second {
		t.Fatalf("schedule 1 timeout = %s, want 20s", s1.Timeout)
	}
}

// Template change replacing schedules updates the rows.
func TestScheduleReplacementUpdatesRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, schedulesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	// Change: drop one schedule, change the other's cron/timezone/timeout.
	changed := mustTemplate(t, `runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - name: jobs.report.handler
    handler: jobs.report.handler
    cron: "0 4 * * *"
    timezone: America/New_York
`)
	st.RecordReconcileSuccess("demo", "img2", "fp2", time.Now(), fnFor(t, "demo", changed))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Schedules) != 1 {
		t.Fatalf("schedules = %d, want 1 after replacement", len(detail.Schedules))
	}
	s := detail.Schedules[0]
	if s.Name != "jobs.report.handler" || s.Handler != "jobs.report.handler" || s.Cron != "0 4 * * *" || s.Timezone != "America/New_York" {
		t.Fatalf("schedule after change = %+v", s)
	}
	if s.Timeout != 6*time.Second {
		t.Fatalf("timeout after change = %s, want default 6s", s.Timeout)
	}
}

// Removal clears the schedule rows.
func TestScheduleRemovalClearsRows(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, schedulesTmpl)
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	st.RecordRemoved("demo")
	if _, ok := st.GetApp("demo"); ok {
		t.Fatal("function row should be gone after removal")
	}
}

// A template without schedules stores none, so inspect renders no Schedules
// section.
func TestScheduleEmptyStored(t *testing.T) {
	st := openTestState(t)
	tmpl := mustTemplate(t, twoHandlerTmpl) // no schedules key
	st.RecordReconcileSuccess("demo", "img", "fp", time.Now(), fnFor(t, "demo", tmpl))

	detail, ok := st.GetApp("demo")
	if !ok {
		t.Fatal("expected function")
	}
	if len(detail.Schedules) != 0 {
		t.Fatalf("schedules = %d, want 0 for a template without schedules", len(detail.Schedules))
	}
}

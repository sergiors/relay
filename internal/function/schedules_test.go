package function

import (
	"strings"
	"testing"
	"time"
)

// A template without a `schedules` key renders a nil/empty Schedules slice and
// parses exactly as before.
func TestParseNoSchedulesNil(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
`)
	if len(tmpl.Schedules) != 0 {
		t.Fatalf("expected 0 schedules, got %d", len(tmpl.Schedules))
	}
	if len(tmpl.Events) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(tmpl.Events))
	}
}

// A valid schedule parses with a UTC default timezone and the default 6s
// timeout, matching event-rule defaults.
func TestParseScheduleValidDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: 0 3 * * *
`)
	if len(tmpl.Schedules) != 1 {
		t.Fatalf("expected 1 schedule, got %d", len(tmpl.Schedules))
	}
	s := tmpl.Schedules[0]
	if s.Handler != "jobs.cleanup.handler" {
		t.Fatalf("handler = %q", s.Handler)
	}
	if s.Cron != "0 3 * * *" {
		t.Fatalf("cron = %q", s.Cron)
	}
	if s.Location != time.UTC {
		t.Fatalf("location = %v, want UTC", s.Location)
	}
	if s.Timeout != DefaultTimeout {
		t.Fatalf("timeout = %s, want %s", s.Timeout, DefaultTimeout)
	}
}

// Multiple schedules (including the same handler twice) are registered
// independently with their own cron/timezone/timeout.
func TestParseMultipleSchedules(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - name: jobs.cleanup.handler-2
    handler: jobs.cleanup.handler
    cron: "0 8 * * 1-5"
    timezone: Europe/Rome
    timeout: 20s
  - name: jobs.report.handler
    handler: jobs.report.handler
    cron: "0 4 * * *"
    timezone: America/New_York
`)
	if len(tmpl.Schedules) != 3 {
		t.Fatalf("expected 3 schedules, got %d", len(tmpl.Schedules))
	}
	if tmpl.Schedules[0].Handler != "jobs.cleanup.handler" || tmpl.Schedules[0].Cron != "0 3 * * *" {
		t.Fatalf("schedule 0 = %+v", tmpl.Schedules[0])
	}
	if tmpl.Schedules[1].Handler != "jobs.cleanup.handler" || tmpl.Schedules[1].Cron != "0 8 * * 1-5" {
		t.Fatalf("schedule 1 = %+v", tmpl.Schedules[1])
	}
	if tmpl.Schedules[1].Location.String() != "Europe/Rome" {
		t.Fatalf("schedule 1 location = %v, want Europe/Rome", tmpl.Schedules[1].Location)
	}
	if tmpl.Schedules[1].Timeout != 20*time.Second {
		t.Fatalf("schedule 1 timeout = %s, want 20s", tmpl.Schedules[1].Timeout)
	}
	if tmpl.Schedules[2].Location.String() != "America/New_York" {
		t.Fatalf("schedule 2 location = %v, want America/New_York", tmpl.Schedules[2].Location)
	}
}

func TestParseScheduleMissingHandler(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: cleanup
    cron: "0 3 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "schedule \"cleanup\" is missing a handler") {
		t.Fatalf("err = %v, want missing-handler error", err)
	}
}

// A schedule without a name is rejected: the name is its mandatory identity.
func TestParseScheduleMissingName(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "schedule is missing a name") {
		t.Fatalf("err = %v, want missing-name error", err)
	}
}

// Duplicate schedule NAMES are rejected, but multiple schedules MAY share a
// handler (each name is an independent schedule).
func TestParseScheduleDuplicateNameAndSharedHandler(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: cleanup
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - name: cleanup
    handler: jobs.cleanup.handler
    cron: "0 4 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), `duplicate schedule name "cleanup"`) {
		t.Fatalf("err = %v, want duplicate-schedule-name rejection", err)
	}

	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: morning
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
  - name: evening
    handler: jobs.cleanup.handler
    cron: "0 20 * * *"
`)
	if len(tmpl.Schedules) != 2 {
		t.Fatalf("schedules = %d, want 2 (same handler, distinct names)", len(tmpl.Schedules))
	}
	if tmpl.Schedules[0].Name == tmpl.Schedules[1].Name {
		t.Fatal("distinct schedule names expected")
	}
}

// An invalid schedule name is rejected with the shared conservative name rule.
func TestParseScheduleInvalidName(t *testing.T) {
	for _, bad := range []string{"Cleanup", "trailing.", "a/b"} {
		t.Run(bad, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: "` + bad + `"
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`))
			if err == nil || !strings.Contains(err.Error(), "invalid schedule name") {
				t.Fatalf("name %q: err = %v, want invalid-name rejection", bad, err)
			}
		})
	}
}

func TestParseScheduleMissingCron(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
`))
	if err == nil || !strings.Contains(err.Error(), "cron is required") {
		t.Fatalf("err = %v, want cron-required error", err)
	}
}

// Invalid cron expressions are rejected with a clear error that names the
// schedule's handler.
func TestParseScheduleInvalidCron(t *testing.T) {
	// Note: an empty cron is handled earlier by the "cron is required" check
	// (see TestParseScheduleMissingCron), so it never reaches gocron validation.
	for _, cron := range []string{"0 25 * * *", "0 3 * *", "garbage"} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "` + cron + `"
`))
		if err == nil || !strings.Contains(err.Error(), "jobs.cleanup.handler") ||
			!strings.Contains(err.Error(), "invalid cron expression") {
			t.Fatalf("cron %q: err = %v, want handler-naming invalid-cron error", cron, err)
		}
	}
}

// Second-granularity (6-field) cron is rejected at validation: gocron's
// callback exposes no scheduled-due instant, so a per-second occurrence cannot
// be identified deterministically across workers. The error is clear and names
// the seconds form.
func TestParseScheduleRejectsSeconds(t *testing.T) {
	for _, cron := range []string{"30 0 0 * * *", "*/10 * * * * *", "0 0 12 * * ?"} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "` + cron + `"
`))
		if err == nil {
			t.Fatalf("cron %q: expected rejection", cron)
		}
		if !strings.Contains(err.Error(), "jobs.cleanup.handler") ||
			!strings.Contains(err.Error(), "6-field (seconds) cron is not supported") {
			t.Fatalf("cron %q: err = %v, want a clear seconds-unsupported error naming the handler", cron, err)
		}
	}
}

// The `@every` relative-delay descriptor is rejected: it anchors to each
// worker's own job start, so workers do not agree on an occurrence.
func TestParseScheduleRejectsEveryDescriptor(t *testing.T) {
	for _, cron := range []string{"@every 30s", "@every 1h", "  @every 5m"} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "` + cron + `"
`))
		if err == nil || !strings.Contains(err.Error(), "jobs.cleanup.handler") ||
			!strings.Contains(err.Error(), "`@every` relative schedules are not supported") {
			t.Fatalf("cron %q: err = %v, want a clear @every-unsupported error", cron, err)
		}
	}
}

// Calendar descriptors remain accepted at minute granularity and default to UTC
// with the standard timeout/retries.
func TestParseScheduleAcceptsCalendarDescriptors(t *testing.T) {
	for _, cron := range []string{"@hourly", "@daily", "@midnight", "@weekly", "@monthly", "@yearly", "@annually"} {
		tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "`+cron+`"
`)
		if len(tmpl.Schedules) != 1 {
			t.Fatalf("cron %q: expected 1 schedule, got %d", cron, len(tmpl.Schedules))
		}
		s := tmpl.Schedules[0]
		if s.Cron != cron {
			t.Fatalf("cron = %q, want %q", s.Cron, cron)
		}
		if s.Location != time.UTC {
			t.Fatalf("cron %q: location = %v, want UTC", cron, s.Location)
		}
		if s.Timeout != DefaultTimeout {
			t.Fatalf("cron %q: timeout = %s, want %s", cron, s.Timeout, DefaultTimeout)
		}
		if s.Retries != DefaultRetries {
			t.Fatalf("cron %q: retries = %d, want %d", cron, s.Retries, DefaultRetries)
		}
	}
}

// An embedded TZ=/CRON_TZ= prefix is rejected: timezone is a separate template
// field and an embedded override would silently win.
func TestParseScheduleRejectsEmbeddedTZ(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "CRON_TZ=UTC 0 3 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "must not embed TZ=") {
		t.Fatalf("err = %v, want embedded-TZ rejection", err)
	}
}

// Omitted and explicit UTC timezones both resolve to time.UTC.
func TestParseScheduleTimezoneUTC(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.a.handler
    handler: jobs.a.handler
    cron: "0 3 * * *"
  - name: jobs.b.handler
    handler: jobs.b.handler
    cron: "0 3 * * *"
    timezone: UTC
`)
	if tmpl.Schedules[0].Location != time.UTC {
		t.Fatalf("omitted timezone = %v, want UTC", tmpl.Schedules[0].Location)
	}
	if tmpl.Schedules[1].Location != time.UTC {
		t.Fatalf("explicit UTC = %v, want UTC", tmpl.Schedules[1].Location)
	}
}

func TestParseScheduleTimezoneIANA(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 8 * * *"
    timezone: Europe/Rome
`)
	if tmpl.Schedules[0].Location.String() != "Europe/Rome" {
		t.Fatalf("location = %v, want Europe/Rome", tmpl.Schedules[0].Location)
	}
}

func TestParseScheduleInvalidTimezone(t *testing.T) {
	for _, tz := range []string{"Nowhere/Fake", "Local"} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    timezone: ` + tz + `
`))
		if err == nil {
			t.Fatalf("timezone %q: expected error", tz)
		}
	}
}

// Timeout resolution mirrors event rules: omitted -> 6s; valid -> the value;
// zero/negative/unparseable/over-5m rejected.
func TestParseScheduleTimeout(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.a.handler
    handler: jobs.a.handler
    cron: "0 3 * * *"
  - name: jobs.b.handler
    handler: jobs.b.handler
    cron: "0 4 * * *"
    timeout: 20s
`)
	if tmpl.Schedules[0].Timeout != 6*time.Second {
		t.Fatalf("omitted timeout = %s, want 6s", tmpl.Schedules[0].Timeout)
	}
	if tmpl.Schedules[1].Timeout != 20*time.Second {
		t.Fatalf("explicit timeout = %s, want 20s", tmpl.Schedules[1].Timeout)
	}

	for _, bad := range []string{"0", "-1s", "abc", "20m"} {
		_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    timeout: ` + bad + `
`))
		if err == nil || !strings.Contains(err.Error(), "jobs.cleanup.handler") {
			t.Fatalf("timeout %q: err = %v, want handler-naming error", bad, err)
		}
	}
}

// A template with both events and schedules still yields correct Events and
// Schedules.
func TestParseEventsAndSchedules(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
    timeout: 20s
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`)
	if len(tmpl.Events) != 2 {
		t.Fatalf("rules = %d, want 2", len(tmpl.Events))
	}
	if tmpl.Events[1].Timeout != 20*time.Second {
		t.Fatalf("rule 1 timeout = %s, want 20s", tmpl.Events[1].Timeout)
	}
	if len(tmpl.Schedules) != 1 || tmpl.Schedules[0].Handler != "jobs.cleanup.handler" {
		t.Fatalf("schedules = %+v", tmpl.Schedules)
	}
}

// A schedule whose handler is malformed (not module.function) is rejected.
func TestParseScheduleInvalidHandler(t *testing.T) {
	_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: nohandler
    handler: nohandler
    cron: "0 3 * * *"
`))
	if err == nil || !strings.Contains(err.Error(), "nohandler") {
		t.Fatalf("err = %v, want handler-form error naming the handler", err)
	}
}

// TestParseScheduleMissingRetriesDefaults pins the default retry count for a
// schedule entry that omits `retries`, matching the event-rule default.
func TestParseScheduleMissingRetriesDefaults(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
`)
	if got := tmpl.Schedules[0].Retries; got != DefaultRetries {
		t.Fatalf("missing retries schedule = %d, want default %d", got, DefaultRetries)
	}
}

// TestParseScheduleExplicitRetries verifies an explicit non-negative `retries`
// is honored, including zero (only the initial attempt).
func TestParseScheduleExplicitRetries(t *testing.T) {
	tmpl := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    retries: 2
`)
	if got := tmpl.Schedules[0].Retries; got != 2 {
		t.Fatalf("explicit retries = %d, want 2", got)
	}

	zero := mustParse(t, `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    retries: 0
`)
	if got := zero.Schedules[0].Retries; got != 0 {
		t.Fatalf("retries: 0 = %d, want 0", got)
	}
}

// TestParseScheduleRetriesRejected verifies that a negative or non-integer
// `retries` fails template validation with a clear message naming the field,
// mirroring the event-rule retries tests.
func TestParseScheduleRetriesRejected(t *testing.T) {
	cases := []struct {
		name    string
		retries string
	}{
		{"negative", "-1"},
		{"string", "abc"},
		{"float", "1.5"},
		{"bool", "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(`
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      status: [COMPLETED]
schedules:
  - name: jobs.cleanup.handler
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    retries: ` + tc.retries + `
`))
			if err == nil {
				t.Fatalf("expected error for retries %q", tc.retries)
			}
			if !strings.Contains(err.Error(), "retries") {
				t.Errorf("expected error to mention retries, got: %v", err)
			}
		})
	}
}

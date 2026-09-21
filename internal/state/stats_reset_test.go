package state

import (
	"context"
	"testing"
	"time"
)

// resetSeedTmpl exercises every functions-adjacent table (handlers, schedules,
// services) plus the env/secret MAPPINGS so a reset can be proven not to touch
// them.
const resetSeedTmpl = `runtime: python3.14
env:
  API_URL: https://api.example.com
secrets:
  DATABASE_URL: database-url
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
schedules:
  - handler: jobs.cleanup.handler
    cron: "0 3 * * *"
services:
  - entrypoint: api.js
    port: 3000
    replicas: 2
`

// TestResetStatsZeroesCountersKeepsGauges verifies ResetStats zeroes the five
// global cumulative counters while PRESERVING the two point-in-time backlog
// gauges, deletes every function_stats row, refreshes updated_at, and leaves
// the unrelated functions/handlers/schedules/services data intact.
func TestResetStatsZeroesCountersKeepsGauges(t *testing.T) {
	c := openTestState(t)

	// A deterministic clock so the updated_at refresh is observable without
	// sleeping: seed at base, reset one minute later.
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.nowFn = func() time.Time { return base }

	tmpl := mustTemplate(t, resetSeedTmpl)
	c.RecordReconcileSuccess("alpha", "img", "fp", base, fnFor(t, "alpha", tmpl))

	c.RecordStats(Stats{
		EventsProcessedTotal:    100,
		HandlerSuccessTotal:     90,
		HandlerFailureTotal:     10,
		RetryTotal:              7,
		DLQTotal:                3,
		PendingEntries:          17,
		OldestPendingAgeSeconds: 134,
	})
	exec := base.Add(-time.Hour).Format(time.RFC3339)
	c.RecordFunctionStats(FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 40,
		HandlerSuccessTotal:  30,
		HandlerFailureTotal:  10,
		RetryTotal:           5,
		DLQTotal:             2,
		WarmAcquiresTotal:    11,
		ColdStartsTotal:      4,
		DiscardedTotal:       2,
		LastExecutionAt:      exec,
		LastSuccessAt:        exec,
		LastFailureAt:        exec,
		LastDLQAt:            exec,
	})
	c.RecordFunctionStats(FunctionStats{Function: "beta", EventsProcessedTotal: 3})

	c.nowFn = func() time.Time { return base.Add(time.Minute) }
	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	// Global counters are zero; the live backlog gauges are preserved; the row
	// still exists and its updated_at was refreshed to now().
	s, ok := c.Stats()
	if !ok {
		t.Fatal("stats row must still exist after reset")
	}
	if s.EventsProcessedTotal != 0 || s.HandlerSuccessTotal != 0 ||
		s.HandlerFailureTotal != 0 || s.RetryTotal != 0 || s.DLQTotal != 0 {
		t.Fatalf("cumulative counters must be zeroed: %+v", s)
	}
	if s.PendingEntries != 17 || s.OldestPendingAgeSeconds != 134 {
		t.Fatalf("backlog gauges must be preserved: %+v", s)
	}
	if want := base.Add(time.Minute).Format(time.RFC3339); s.UpdatedAt != want {
		t.Fatalf("updated_at = %q, want refreshed %q", s.UpdatedAt, want)
	}

	// function_stats is empty (deletion IS the fresh state), readable both
	// through the API and directly.
	if all := c.AllFunctionStats(); len(all) != 0 {
		t.Fatalf("function_stats must be cleared: %+v", all)
	}
	var n int
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM function_stats`).Scan(&n); err != nil {
		t.Fatalf("count function_stats: %v", err)
	}
	if n != 0 {
		t.Fatalf("function_stats rows = %d, want 0", n)
	}

	// Unrelated data is untouched: the function record, handlers, schedules,
	// services, and env/secret mappings all survive.
	d, ok := c.GetFunction("alpha")
	if !ok {
		t.Fatal("function row must survive the stats reset")
	}
	if len(d.Handlers) != 1 || d.Handlers[0].Name != "events.created.handler" {
		t.Fatalf("handlers must survive: %+v", d.Handlers)
	}
	if len(d.Schedules) != 1 || d.Schedules[0].Cron != "0 3 * * *" {
		t.Fatalf("schedules must survive: %+v", d.Schedules)
	}
	if len(d.Services) != 1 || d.Services[0].Port != 3000 || d.Services[0].Replicas != 2 {
		t.Fatalf("services must survive: %+v", d.Services)
	}
	if d.Env["API_URL"] != "https://api.example.com" || d.Secrets["DATABASE_URL"] != "database-url" {
		t.Fatalf("env/secret mappings must survive: env=%v secrets=%v", d.Env, d.Secrets)
	}
}

// TestResetStatsThenAccumulate verifies ordinary accumulation resumes after a
// reset: fresh global counters and fresh per-function counters (with timestamps)
// record and read back normally on the cleared slate.
func TestResetStatsThenAccumulate(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsProcessedTotal: 50, PendingEntries: 3})
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 5})

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	c.RecordStats(Stats{EventsProcessedTotal: 3, PendingEntries: 9})
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	c.RecordFunctionStatsContext(context.Background(), FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 3,
		HandlerSuccessTotal:  3,
		WarmAcquiresTotal:    2,
		LastExecutionAt:      exec,
		LastSuccessAt:        exec,
	})

	s, ok := c.Stats()
	if !ok || s.EventsProcessedTotal != 3 || s.PendingEntries != 9 {
		t.Fatalf("post-reset global stats = %+v, ok=%v", s, ok)
	}
	fs, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("post-reset function stats row must exist after a new record")
	}
	if fs.EventsProcessedTotal != 3 || fs.HandlerSuccessTotal != 3 || fs.WarmAcquiresTotal != 2 {
		t.Fatalf("post-reset function counters = %+v", fs)
	}
	// The fresh row had no stored timestamps, so the incoming ones land as-is.
	if fs.LastExecutionAt != exec || fs.LastSuccessAt != exec {
		t.Fatalf("post-reset timestamps = %+v, want %q", fs, exec)
	}
}

// TestResetStatsIdempotent verifies a second reset on already-reset state
// succeeds and keeps the fresh state.
func TestResetStatsIdempotent(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsProcessedTotal: 1, PendingEntries: 4})
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 1})

	for i := 1; i <= 2; i++ {
		if err := c.ResetStats(); err != nil {
			t.Fatalf("ResetStats run %d: %v", i, err)
		}
	}

	s, ok := c.Stats()
	if !ok || s.EventsProcessedTotal != 0 || s.PendingEntries != 4 {
		t.Fatalf("stats after repeated reset = %+v, ok=%v", s, ok)
	}
	if all := c.AllFunctionStats(); len(all) != 0 {
		t.Fatalf("function_stats after repeated reset = %+v", all)
	}
}

// TestResetStatsFreshDB verifies a reset on a database with no stats row at all
// succeeds without creating a row: there is nothing cumulative to zero.
func TestResetStatsFreshDB(t *testing.T) {
	c := openTestState(t)

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats on fresh DB: %v", err)
	}
	if s, ok := c.Stats(); ok {
		t.Fatalf("fresh DB must stay without a stats row, got %+v", s)
	}
	var n int
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM stats`).Scan(&n); err != nil {
		t.Fatalf("count stats: %v", err)
	}
	if n != 0 {
		t.Fatalf("stats rows = %d, want 0 (reset must not create one)", n)
	}
}

// TestResetStatsRollsBackOnCorruptGlobalPayload verifies the reset is
// transactional: if the global payload cannot be decoded, the whole
// transaction fails and the function_stats rows are NOT deleted.
func TestResetStatsRollsBackOnCorruptGlobalPayload(t *testing.T) {
	c := openTestState(t)
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 5})

	// Bypass the marshaller to plant an undecodable payload (the schema allows
	// arbitrary TEXT in data).
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO stats (id, data, updated_at) VALUES (1, 'not-json', '2026-01-02T03:04:05Z')`); err != nil {
		t.Fatalf("seed corrupt stats: %v", err)
	}

	if err := c.ResetStats(); err == nil {
		t.Fatal("ResetStats with a corrupt payload: err = nil, want error")
	}
	if all := c.AllFunctionStats(); len(all) != 1 || all[0].EventsProcessedTotal != 5 {
		t.Fatalf("failed reset must roll back function_stats deletion: %+v", all)
	}
}

package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// resetSeedTmpl exercises every part of the persisted functions snapshot
// (handlers, schedules, services, env/secret references) so a reset
// can be proven not to touch them.
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
// gauges and every function_stats ROW (zeroed in place, never deleted),
// refreshes updated_at, and leaves the unrelated functions snapshot
// (handlers/schedules/services/env/secret references) intact.
func TestResetStatsZeroesCountersKeepsGauges(t *testing.T) {
	c := openTestState(t)

	// A deterministic clock so the updated_at refresh is observable without
	// sleeping: seed at base, reset one minute later.
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c.nowFn = func() time.Time { return base }

	tmpl := mustTemplate(t, resetSeedTmpl)
	c.RecordReconcileSuccess("alpha", "img", "fp", base, fnFor(t, "alpha", tmpl))

	c.RecordStats(Stats{
		EventsReceivedTotal:     110,
		EventsMatchedTotal:      100,
		EventsUnmatchedTotal:    10,
		HandlerSuccessTotal:     90,
		HandlerFailureTotal:     10,
		RetryTotal:              7,
		DLQTotal:                3,
		PendingEntries:          17,
		OldestPendingAgeSeconds: 134,
	})
	exec := base.Add(-time.Hour).Format(time.RFC3339)
	c.RecordFunctionStats(FunctionStats{
		Function:            "alpha",
		EventsMatchedTotal:  40,
		HandlerSuccessTotal: 30,
		HandlerFailureTotal: 10,
		RetryTotal:          5,
		DLQTotal:            2,
		WarmAcquiresTotal:   11,
		ColdStartsTotal:     4,
		DiscardedTotal:      2,
		LastExecutionAt:     exec,
		LastSuccessAt:       exec,
		LastFailureAt:       exec,
		LastDLQAt:           exec,
	})
	c.RecordFunctionStats(FunctionStats{Function: "beta", EventsMatchedTotal: 3})

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
	if s.EventsReceivedTotal != 0 || s.EventsMatchedTotal != 0 || s.EventsUnmatchedTotal != 0 ||
		s.HandlerSuccessTotal != 0 ||
		s.HandlerFailureTotal != 0 || s.RetryTotal != 0 || s.DLQTotal != 0 {
		t.Fatalf("cumulative counters must be zeroed: %+v", s)
	}
	if s.PendingEntries != 17 || s.OldestPendingAgeSeconds != 134 {
		t.Fatalf("backlog gauges must be preserved: %+v", s)
	}
	if want := base.Add(time.Minute).Format(time.RFC3339); s.UpdatedAt != want {
		t.Fatalf("updated_at = %q, want refreshed %q", s.UpdatedAt, want)
	}

	// Both function_stats ROWS survive with every cumulative field zeroed and
	// every Last*At timestamp cleared.
	all := c.AllFunctionStats()
	if len(all) != 2 {
		t.Fatalf("function_stats rows = %d, want 2 (rows preserved): %+v", len(all), all)
	}
	for _, fs := range all {
		if fs.EventsMatchedTotal != 0 || fs.HandlerSuccessTotal != 0 ||
			fs.HandlerFailureTotal != 0 || fs.RetryTotal != 0 || fs.DLQTotal != 0 ||
			fs.WarmAcquiresTotal != 0 || fs.ColdStartsTotal != 0 || fs.DiscardedTotal != 0 {
			t.Fatalf("function %q counters must be zeroed: %+v", fs.Function, fs)
		}
		if fs.LastExecutionAt != "" || fs.LastSuccessAt != "" || fs.LastFailureAt != "" || fs.LastDLQAt != "" {
			t.Fatalf("function %q timestamps must be cleared: %+v", fs.Function, fs)
		}
		want := base.Add(time.Minute).Format(time.RFC3339)
		if fs.UpdatedAt != want {
			t.Fatalf("function %q updated_at = %q, want refreshed %q", fs.Function, fs.UpdatedAt, want)
		}
	}
	var n int
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM function_stats`).Scan(&n); err != nil {
		t.Fatalf("count function_stats: %v", err)
	}
	if n != 2 {
		t.Fatalf("function_stats rows = %d, want 2 (no DELETE in reset path)", n)
	}

	// Unrelated data is untouched: the function record, handlers, schedules,
	// services, and env/secret mappings all survive.
	detail, ok := c.GetFunction("alpha")
	if !ok {
		t.Fatal("function row must survive the stats reset")
	}
	if len(detail.Handlers) != 1 || detail.Handlers[0].Name != "events.created.handler" {
		t.Fatalf("handlers must survive: %+v", detail.Handlers)
	}
	if len(detail.Schedules) != 1 || detail.Schedules[0].Cron != "0 3 * * *" {
		t.Fatalf("schedules must survive: %+v", detail.Schedules)
	}
	if len(detail.Services) != 1 || detail.Services[0].Port != 3000 || detail.Services[0].Replicas != 2 {
		t.Fatalf("services must survive: %+v", detail.Services)
	}
	if detail.Env["API_URL"] != "https://api.example.com" || detail.Secrets["DATABASE_URL"] != "database-url" {
		t.Fatalf("env/secret mappings must survive: env=%v secrets=%v", detail.Env, detail.Secrets)
	}
}

// TestResetStatsPreservesUnrelatedGlobalJSONFields verifies the reset decodes
// the typed global payload and rewrites it, so a live gauge (and every other
// known field) survives while only the cumulative counters are zeroed.
func TestResetStatsPreservesUnrelatedGlobalJSONFields(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{
		EventsMatchedTotal:      9,
		HandlerSuccessTotal:     8,
		HandlerFailureTotal:     1,
		RetryTotal:              1,
		DLQTotal:                1,
		PendingEntries:          42,
		OldestPendingAgeSeconds: 3600,
	})

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}
	// The payload is still valid JSON with exactly the known keys (no unrelated
	// field was dropped or duplicated).
	data, _ := rawStatsData(t, c)
	assertJSONKeysExactly(t, data, statsJSONKeys)
	var m map[string]any
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if m["pending_entries"] != float64(42) || m["oldest_pending_age_seconds"] != float64(3600) {
		t.Fatalf("live gauges must survive the rewrite: %s", data)
	}
}

// TestResetStatsZeroesPoolCountersAndTimestamps verifies the per-function reset
// covers the cumulative warm-container pool counters and clears all four
// execution-history timestamps, while keeping the row.
func TestResetStatsZeroesPoolCountersAndTimestamps(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	c.RecordFunctionStats(FunctionStats{
		Function:            "alpha",
		EventsMatchedTotal:  5,
		HandlerSuccessTotal: 4,
		HandlerFailureTotal: 1,
		RetryTotal:          1,
		DLQTotal:            1,
		WarmAcquiresTotal:   11,
		ColdStartsTotal:     4,
		DiscardedTotal:      2,
		LastExecutionAt:     exec,
		LastSuccessAt:       exec,
		LastFailureAt:       exec,
		LastDLQAt:           exec,
	})

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}
	fs, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("alpha row must survive the reset")
	}
	if fs.EventsMatchedTotal != 0 || fs.HandlerSuccessTotal != 0 || fs.HandlerFailureTotal != 0 ||
		fs.RetryTotal != 0 || fs.DLQTotal != 0 || fs.WarmAcquiresTotal != 0 ||
		fs.ColdStartsTotal != 0 || fs.DiscardedTotal != 0 {
		t.Fatalf("all cumulative counters must be zeroed: %+v", fs)
	}
	if fs.LastExecutionAt != "" || fs.LastSuccessAt != "" || fs.LastFailureAt != "" || fs.LastDLQAt != "" {
		t.Fatalf("all timestamps must be cleared: %+v", fs)
	}
}

// TestResetStatsMultipleFunctionRows verifies every row is reset independently,
// including one with no timestamps and one with only pool counters.
func TestResetStatsMultipleFunctionRows(t *testing.T) {
	c := openTestState(t)
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 5, WarmAcquiresTotal: 2})
	c.RecordFunctionStats(FunctionStats{Function: "beta", EventsMatchedTotal: 3})
	c.RecordFunctionStats(FunctionStats{Function: "gamma", WarmAcquiresTotal: 7, DiscardedTotal: 1})

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}
	all := c.AllFunctionStats()
	if len(all) != 3 {
		t.Fatalf("rows = %d, want 3: %+v", len(all), all)
	}
	for _, fs := range all {
		if fs != (FunctionStats{Function: fs.Function, UpdatedAt: fs.UpdatedAt}) {
			t.Fatalf("function %q not fully zeroed: %+v", fs.Function, fs)
		}
	}
}

// TestResetStatsThenAccumulate verifies ordinary accumulation resumes after a
// reset: fresh global counters and fresh per-function counters (with timestamps)
// record and read back normally on the cleared slate.
func TestResetStatsThenAccumulate(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsMatchedTotal: 50, PendingEntries: 3})
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 5})

	if err := c.ResetStats(); err != nil {
		t.Fatalf("ResetStats: %v", err)
	}

	c.RecordStats(Stats{EventsMatchedTotal: 3, PendingEntries: 9})
	exec := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	c.RecordFunctionStatsContext(context.Background(), FunctionStats{
		Function:            "alpha",
		EventsMatchedTotal:  3,
		HandlerSuccessTotal: 3,
		WarmAcquiresTotal:   2,
		LastExecutionAt:     exec,
		LastSuccessAt:       exec,
	})

	s, ok := c.Stats()
	if !ok || s.EventsMatchedTotal != 3 || s.PendingEntries != 9 {
		t.Fatalf("post-reset global stats = %+v, ok=%v", s, ok)
	}
	fs, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("post-reset function stats row must exist after a new record")
	}
	if fs.EventsMatchedTotal != 3 || fs.HandlerSuccessTotal != 3 || fs.WarmAcquiresTotal != 2 {
		t.Fatalf("post-reset function counters = %+v", fs)
	}
	// The reset cleared the timestamps, so the incoming ones land as-is.
	if fs.LastExecutionAt != exec || fs.LastSuccessAt != exec {
		t.Fatalf("post-reset timestamps = %+v, want %q", fs, exec)
	}
}

// TestResetStatsIdempotent verifies a second reset on already-reset state
// succeeds and keeps the fresh state.
func TestResetStatsIdempotent(t *testing.T) {
	c := openTestState(t)
	c.RecordStats(Stats{EventsMatchedTotal: 1, PendingEntries: 4})
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 1})

	for i := 1; i <= 2; i++ {
		if err := c.ResetStats(); err != nil {
			t.Fatalf("ResetStats run %d: %v", i, err)
		}
	}

	s, ok := c.Stats()
	if !ok || s.EventsMatchedTotal != 0 || s.PendingEntries != 4 {
		t.Fatalf("stats after repeated reset = %+v, ok=%v", s, ok)
	}
	if all := c.AllFunctionStats(); len(all) != 1 || all[0].EventsMatchedTotal != 0 {
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
// transaction fails and the function_stats rows are NOT modified.
func TestResetStatsRollsBackOnCorruptGlobalPayload(t *testing.T) {
	c := openTestState(t)
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsMatchedTotal: 5})

	// Bypass the marshaller to plant an undecodable payload (the schema allows
	// arbitrary TEXT in data).
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO stats (id, data, updated_at) VALUES (1, 'not-json', '2026-01-02T03:04:05Z')`); err != nil {
		t.Fatalf("seed corrupt stats: %v", err)
	}

	if err := c.ResetStats(); err == nil {
		t.Fatal("ResetStats with a corrupt payload: err = nil, want error")
	}
	if all := c.AllFunctionStats(); len(all) != 1 || all[0].EventsMatchedTotal != 5 {
		t.Fatalf("failed reset must roll back function_stats changes: %+v", all)
	}
}

// TestResetStatsRollsBackOnCorruptFunctionPayload verifies a corrupt
// per-function payload is fatal: it cannot be decoded and zeroed, so the reset
// transaction rolls back and every row — the global row and the good function
// row — keeps its pre-reset values.
func TestResetStatsRollsBackOnCorruptFunctionPayload(t *testing.T) {
	c := openTestState(t)
	c.RecordFunctionStats(FunctionStats{Function: "good", EventsMatchedTotal: 5})
	if _, err := c.db.ExecContext(context.Background(),
		`INSERT INTO function_stats (function_name, data, updated_at) VALUES ('broken', '{not-json', '2020-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed corrupt row: %v", err)
	}
	c.RecordStats(Stats{EventsMatchedTotal: 9})

	if err := c.ResetStats(); err == nil {
		t.Fatal("ResetStats with a corrupt function payload: err = nil, want error")
	}
	// Nothing landed: the global counters and the good function row keep their
	// pre-reset values.
	if s, _ := c.Stats(); s.EventsMatchedTotal != 9 {
		t.Fatalf("global counters must roll back: %+v", s)
	}
	good, ok := c.FunctionStats("good")
	if !ok || good.EventsMatchedTotal != 5 {
		t.Fatalf("good row must roll back: %+v, ok=%v", good, ok)
	}
	// The corrupt row survives untouched (its raw stored bytes are unchanged).
	brokenData, _, _ := rawFunctionStatsBlob(t, c, "broken")
	if string(brokenData) != "{not-json" {
		t.Fatalf("corrupt row must be preserved unchanged, got %q", brokenData)
	}
}

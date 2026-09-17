package state

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestFunctionStatsTimestampsRoundTrip verifies RecordFunctionStatsContext
// persists the four execution-history timestamp columns and FunctionStats
// reads them back as RFC3339 strings with UpdatedAt round-trip semantics.
func TestFunctionStatsTimestampsRoundTrip(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-2 * time.Minute).UTC()
	in := FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 5,
		HandlerSuccessTotal:  4,
		HandlerFailureTotal:  1,
		RetryTotal:           1,
		LastExecutionAt:      exec.Format(time.RFC3339),
		LastSuccessAt:        time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		LastFailureAt:        time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339),
		LastDLQAt:            time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339),
	}
	c.RecordFunctionStats(in)

	s, ok := c.FunctionStats("alpha")
	if !ok {
		t.Fatal("expected function stats row after record")
	}
	in.UpdatedAt = s.UpdatedAt
	if s != in {
		t.Fatalf("function stats = %+v, want %+v", s, in)
	}
	// Every persisted timestamp parses as RFC3339 (same convention as
	// updated_at).
	for name, v := range map[string]string{
		"LastExecutionAt": s.LastExecutionAt,
		"LastSuccessAt":   s.LastSuccessAt,
		"LastFailureAt":   s.LastFailureAt,
		"LastDLQAt":       s.LastDLQAt,
	} {
		if _, err := time.Parse(time.RFC3339, v); err != nil {
			t.Fatalf("%s not RFC3339: %q: %v", name, v, err)
		}
	}
}

// TestRecordFunctionStatsEmptyTimestampPreservesExisting pins the CASE guard:
// an empty incoming timestamp PRESERVES the previously stored one — "no
// observation" never erases "last observed at" — and a NEW value overwrites.
func TestRecordFunctionStatsEmptyTimestampPreservesExisting(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().Add(-time.Hour).UTC()
	c.RecordFunctionStats(FunctionStats{
		Function:             "alpha",
		EventsProcessedTotal: 1,
		LastExecutionAt:      exec.Format(time.RFC3339),
		LastDLQAt:            exec.Format(time.RFC3339),
	})

	// A later flush carries counters but NO timestamps: both must survive.
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 2})
	s, ok := c.FunctionStats("alpha")
	if !ok || s.EventsProcessedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2 (counters replaced)", s, ok)
	}
	if s.LastExecutionAt != exec.Format(time.RFC3339) || s.LastDLQAt != exec.Format(time.RFC3339) {
		t.Fatalf("empty incoming timestamps must preserve persisted ones: %+v", s)
	}

	// A NEW value does overwrite (latest wins).
	reExec := time.Now().UTC()
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 3, LastExecutionAt: reExec.Format(time.RFC3339)})
	s, _ = c.FunctionStats("alpha")
	if s.LastExecutionAt != reExec.Format(time.RFC3339) {
		t.Fatalf("newer execution timestamp must overwrite: %+v", s)
	}
	// ...while the untouched empty one is still preserved.
	if s.LastDLQAt != exec.Format(time.RFC3339) {
		t.Fatalf("untouched DLQ timestamp must survive the overwrite: %+v", s)
	}
}

// TestRecordStatsSnapshotTimestampsPersist verifies RecordStatsSnapshot's
// per-function upsert persists timestamps and applies the same empty-preserves
// CASE guards.
func TestRecordStatsSnapshotTimestampsPersist(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	exec := time.Now().Add(-time.Minute).UTC()
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 1},
		[]FunctionStats{{Function: "alpha", EventsProcessedTotal: 1, LastExecutionAt: exec.Format(time.RFC3339)}}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// A follow-up flush carries NO timestamps: the stored one must survive.
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 2},
		[]FunctionStats{{Function: "alpha", EventsProcessedTotal: 2}}); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	s, ok := c.FunctionStats("alpha")
	if !ok || s.EventsProcessedTotal != 2 {
		t.Fatalf("alpha = %+v, ok=%v; want events 2", s, ok)
	}
	if s.LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("snapshot empty timestamp must preserve persisted value: %+v", s)
	}
}

// TestMigrateFunctionStatsTimestampColumns builds a legacy function_stats
// table WITHOUT the four timestamp columns (as an old database would have),
// rows included, then reopens it THROUGH Open (which runs the migrations) —
// PRAGMA table_info must report the columns after the reopen (checking via a
// raw read) and the old rows' counters must be preserved.
func TestMigrateFunctionStatsTimestampColumns(t *testing.T) {
	path := t.TempDir() + "/db.sqlite3"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("legacy open: %v", err)
	}
	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE functions (
			name TEXT PRIMARY KEY, runtime TEXT, status TEXT, image TEXT,
			fingerprint TEXT, prepared_at TEXT, last_reconcile_at TEXT,
			last_reconcile_status TEXT, last_error TEXT, updated_at TEXT,
			env TEXT, secrets TEXT
		);
		CREATE TABLE function_stats (
			function_name TEXT PRIMARY KEY,
			events_processed_total INTEGER NOT NULL DEFAULT 0,
			handler_success_total INTEGER NOT NULL DEFAULT 0,
			handler_failure_total INTEGER NOT NULL DEFAULT 0,
			retry_total INTEGER NOT NULL DEFAULT 0,
			dlq_total INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT
		);`)
	if err != nil {
		t.Fatalf("legacy schema: %v", err)
	}
	_, err = db.ExecContext(context.Background(),
		`INSERT INTO function_stats (function_name, events_processed_total, updated_at) VALUES ('alpha', 7, '2020-01-01T00:00:00Z')`)
	if err != nil {
		t.Fatalf("legacy seed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("legacy close: %v", err)
	}

	// Reopen through Open: the migrateFunctionStatsTimestampColumns ALTERs must
	// add the missing columns idempotently without touching existing rows.
	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	// The legacy row survived with its counters and empty (never-observed)
	// timestamps, readable through the normal reader (COALESCE'd from NULL).
	s, ok := c2.FunctionStats("alpha")
	if !ok || s.EventsProcessedTotal != 7 {
		t.Fatalf("alpha after migration = %+v, ok=%v; want events 7", s, ok)
	}
	if s.LastExecutionAt != "" || s.LastSuccessAt != "" || s.LastFailureAt != "" || s.LastDLQAt != "" {
		t.Fatalf("legacy row must have no timestamps: %+v", s)
	}

	// The migrated table is fully functional: record timestamps on it.
	migrated := time.Now().Add(-time.Minute).UTC()
	c2.RecordFunctionStats(FunctionStats{Function: "beta", EventsProcessedTotal: 3, LastExecutionAt: migrated.Format(time.RFC3339)})
	b, ok := c2.FunctionStats("beta")
	if !ok || b.LastExecutionAt != migrated.Format(time.RFC3339) {
		t.Fatalf("beta after migration = %+v, ok=%v", b, ok)
	}
	// Reopen AGAIN: the migration must be idempotent (no duplicate-column error).
	c3, err := Open(path)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer c3.Close()
}

// TestAllFunctionStatsTimestamps verifies AllFunctionStats reads the timestamp
// columns alongside the counters.
func TestAllFunctionStatsTimestamps(t *testing.T) {
	c := openTestState(t)
	exec := time.Now().UTC()
	c.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 1, LastExecutionAt: exec.Format(time.RFC3339)})
	c.RecordFunctionStats(FunctionStats{Function: "beta", EventsProcessedTotal: 2})

	all := c.AllFunctionStats()
	if len(all) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(all), all)
	}
	if all[0].Function != "alpha" || all[0].LastExecutionAt != exec.Format(time.RFC3339) {
		t.Fatalf("alpha = %+v, want execution timestamp %q", all[0], all[0].LastExecutionAt)
	}
	if all[1].Function != "beta" || all[1].LastExecutionAt != "" {
		t.Fatalf("beta = %+v, want empty timestamps", all[1])
	}
}

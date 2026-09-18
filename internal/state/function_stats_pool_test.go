package state

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

// TestFunctionStatsPoolCountersRoundTrip verifies RecordFunctionStats persists
// the three cumulative warm-container pool counters and FunctionStats reads them
// back, including the absent-row zero value.
func TestFunctionStatsPoolCountersRoundTrip(t *testing.T) {
	c := openTestState(t)
	in := FunctionStats{
		Function:          "alpha",
		WarmAcquiresTotal: 7,
		ColdStartsTotal:   3,
		DiscardedTotal:    2,
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

	// A repeated record REPLACES the pool counters (absolute snapshot).
	c.RecordFunctionStats(FunctionStats{Function: "alpha", WarmAcquiresTotal: 9, ColdStartsTotal: 4, DiscardedTotal: 5})
	s, _ = c.FunctionStats("alpha")
	if s.WarmAcquiresTotal != 9 || s.ColdStartsTotal != 4 || s.DiscardedTotal != 5 {
		t.Fatalf("replaced pool counters = %+v", s)
	}

	// An absent function reads zero pool counters.
	if z, ok := c.FunctionStats("ghost"); ok || z != (FunctionStats{}) {
		t.Fatalf("absent function = %+v, ok=%v; want zero,false", z, ok)
	}
}

// TestRecordStatsSnapshotPersistsPoolCounters verifies the worker flush path
// persists pool counters alongside the operational counters and idempotently
// replaces them, and that AllFunctionStats reads them back.
func TestRecordStatsSnapshotPersistsPoolCounters(t *testing.T) {
	c := openTestState(t)
	c.RecordDiscovered(fnFor(t, "alpha", mustTemplate(t, twoHandlerTmpl)))

	fns := []FunctionStats{
		{Function: "alpha", EventsProcessedTotal: 10, WarmAcquiresTotal: 7, ColdStartsTotal: 3, DiscardedTotal: 2},
	}
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 10}, fns); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s, ok := c.FunctionStats("alpha")
	if !ok || s.WarmAcquiresTotal != 7 || s.ColdStartsTotal != 3 || s.DiscardedTotal != 2 {
		t.Fatalf("alpha after snapshot = %+v, ok=%v", s, ok)
	}

	// A repeat flush with identical values is idempotent (absolute, not delta).
	if err := c.RecordStatsSnapshot(context.Background(), Stats{EventsProcessedTotal: 10}, fns); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	s, _ = c.FunctionStats("alpha")
	if s.WarmAcquiresTotal != 7 || s.ColdStartsTotal != 3 || s.DiscardedTotal != 2 {
		t.Fatalf("pool counters double-counted: %+v", s)
	}

	all := c.AllFunctionStats()
	if len(all) != 1 || all[0].WarmAcquiresTotal != 7 || all[0].ColdStartsTotal != 3 || all[0].DiscardedTotal != 2 {
		t.Fatalf("AllFunctionStats = %+v", all)
	}
}

// TestMigrateFunctionStatsPoolColumns builds a legacy function_stats table
// WITHOUT the three pool counter columns (as a pre-Phase-4 database would
// have), rows included, then reopens it THROUGH Open: the migration must add
// the columns, leave the existing row's counters intact, and default the new
// columns to 0 (never NULL, so the plain integer readers stay valid). Reopening
// again must be idempotent.
func TestMigrateFunctionStatsPoolColumns(t *testing.T) {
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
			last_execution_at TEXT,
			last_success_at TEXT,
			last_failure_at TEXT,
			last_dlq_at TEXT,
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

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()

	// The legacy row survived with its counter and zero pool counters.
	s, ok := c2.FunctionStats("alpha")
	if !ok || s.EventsProcessedTotal != 7 {
		t.Fatalf("alpha after migration = %+v, ok=%v; want events 7", s, ok)
	}
	if s.WarmAcquiresTotal != 0 || s.ColdStartsTotal != 0 || s.DiscardedTotal != 0 {
		t.Fatalf("legacy row pool counters must default to 0: %+v", s)
	}

	// The migrated table is fully functional: record and read pool counters.
	c2.RecordFunctionStats(FunctionStats{Function: "alpha", EventsProcessedTotal: 7, WarmAcquiresTotal: 5, ColdStartsTotal: 1, DiscardedTotal: 3})
	s, _ = c2.FunctionStats("alpha")
	if s.WarmAcquiresTotal != 5 || s.ColdStartsTotal != 1 || s.DiscardedTotal != 3 {
		t.Fatalf("pool counters after migration = %+v", s)
	}

	// Reopen AGAIN: the migration must be idempotent (no duplicate-column error).
	c3, err := Open(path)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer c3.Close()
}

// TestConcurrentOpenMigratesPoolColumns simulates a legacy function_stats table
// WITHOUT the three pool counter columns and then calls Open from several
// goroutines at once. Every Open independently reads PRAGMA and may attempt the
// same ALTER; the loser must tolerate the duplicate-column error (a concurrent
// migrator won) rather than fail the open. The column must end up present and
// the state usable. Run with -race.
//
// The database is first created through Open so it is already in WAL mode: the
// migration race is the subject here, and pre-setting WAL keeps the test from
// also tripping the separate, pre-existing concurrent journal_mode switch on a
// brand-new file.
func TestConcurrentOpenMigratesPoolColumns(t *testing.T) {
	path := t.TempDir() + "/db.sqlite3"
	seed, err := Open(path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	// Drop the pool columns to simulate a pre-Phase-4 database.
	for _, col := range []string{"warm_acquires_total", "cold_starts_total", "discarded_total"} {
		if _, err := seed.db.ExecContext(context.Background(), `ALTER TABLE function_stats DROP COLUMN `+col); err != nil {
			t.Fatalf("drop column %s: %v", col, err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}

	const openers = 8
	var wg sync.WaitGroup
	states := make([]*State, openers)
	errs := make([]error, openers)
	start := make(chan struct{})
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			states[i], errs[i] = Open(path)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Open %d failed: %v", i, err)
		}
		defer states[i].Close()
	}
	// The winning Open's schema is complete on every handle: record and read
	// pool counters through each one.
	for i, c := range states {
		c.RecordFunctionStats(FunctionStats{Function: "alpha", WarmAcquiresTotal: 5, ColdStartsTotal: 1, DiscardedTotal: 2})
		s, ok := c.FunctionStats("alpha")
		if !ok || s.WarmAcquiresTotal != 5 || s.ColdStartsTotal != 1 || s.DiscardedTotal != 2 {
			t.Fatalf("handle %d after concurrent migration = %+v, ok=%v", i, s, ok)
		}
	}
}

// TestExecAddColumnToleratesDuplicateColumn pins the tolerance branch of the
// migration deterministically: execAddColumn is invoked for a column that
// ALREADY exists (the state a concurrent migrator leaves behind), so its ALTER
// fails with a duplicate-column error and the re-read must turn that into
// success. A still-missing column is returned as a genuine error.
func TestExecAddColumnToleratesDuplicateColumn(t *testing.T) {
	c := openTestState(t)
	ctx := context.Background()

	// discarded_total already exists (initSchema created it): a duplicate-column
	// ALTER must be tolerated.
	if err := c.execAddColumn(ctx, "function_stats", "discarded_total", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		t.Fatalf("execAddColumn on an existing column = %v; want nil (concurrent-win tolerance)", err)
	}

	// A genuine failure (a non-existent table) must still be returned: the
	// re-read cannot find the column, so the error is real, not a lost race.
	if err := c.execAddColumn(ctx, "no_such_table", "c", "TEXT"); err == nil {
		t.Fatal("execAddColumn on a missing table must return an error")
	}
}

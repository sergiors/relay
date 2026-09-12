package state

import (
	"context"
	"database/sql"
)

// FunctionStats is the per-function operational snapshot persisted in the
// function_stats table. Unlike the single-row global Stats, it is keyed by
// function name so each function's counters are attributed independently.
//
// Semantics (see runner.Handle): EventsProcessedTotal counts a function once
// per event for which at least one of its rules matched — a functions-engaged
// counter, distinct from the message-level global events_processed_total (an
// event matching two functions counts once globally, once per function here).
// HandlerSuccessTotal/HandlerFailureTotal are per rule execution.
// RetryTotal counts every failing rule execution (a retry driver);
// DLQTotal counts a function once when its failing rule execution is the one
// that exhausts the delivery attempts and the message is routed to the DLQ.
type FunctionStats struct {
	Function             string
	EventsProcessedTotal int64
	HandlerSuccessTotal  int64
	HandlerFailureTotal  int64
	RetryTotal           int64
	DLQTotal             int64
	UpdatedAt            string
}

// RecordFunctionStats upserts the function_stats row for s.Function. It is a
// thin wrapper over RecordFunctionStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (c *State) RecordFunctionStats(s FunctionStats) {
	c.RecordFunctionStatsContext(context.Background(), s)
}

// RecordFunctionStatsContext upserts the function_stats row for s.Function,
// replacing every counter column with the supplied value and setting updated_at
// to now(). There is no accumulation: the worker hands over the CURRENT
// cumulative registry values, so the row always mirrors the latest known
// totals. Callers must pass CURRENT cumulative values; the worker seeds the
// fresh process registry from this table at startup so the first snapshot
// never resets counters. It is non-fatal on error: it logs and returns.
func (c *State) RecordFunctionStatsContext(ctx context.Context, s FunctionStats) {
	ts := now()
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats
		   (function_name, events_processed_total, handler_success_total,
		    handler_failure_total, retry_total, dlq_total, updated_at)
		 VALUES
		   (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(function_name) DO UPDATE SET
		   events_processed_total  = excluded.events_processed_total,
		   handler_success_total   = excluded.handler_success_total,
		   handler_failure_total   = excluded.handler_failure_total,
		   retry_total             = excluded.retry_total,
		   dlq_total               = excluded.dlq_total,
		   updated_at              = excluded.updated_at`,
		s.Function, s.EventsProcessedTotal, s.HandlerSuccessTotal,
		s.HandlerFailureTotal, s.RetryTotal, s.DLQTotal, ts)
	if err != nil {
		c.log.Printf("state: record function stats %q: %v", s.Function, err)
	}
}

// FunctionStats returns the per-function snapshot for name, or (zero, false)
// when no row has been recorded yet or the read fails (which is logged). The
// Function field is set to name on success.
func (c *State) FunctionStats(name string) (FunctionStats, bool) {
	ctx := context.Background()
	var s FunctionStats
	err := c.db.QueryRowContext(ctx,
		`SELECT function_name, events_processed_total, handler_success_total,
		        handler_failure_total, retry_total, dlq_total, updated_at
		 FROM function_stats WHERE function_name = ?`, name,
	).Scan(&s.Function, &s.EventsProcessedTotal, &s.HandlerSuccessTotal,
		&s.HandlerFailureTotal, &s.RetryTotal, &s.DLQTotal, &s.UpdatedAt)
	if err == sql.ErrNoRows {
		return FunctionStats{}, false
	}
	if err != nil {
		c.log.Printf("state: read function stats %q: %v", name, err)
		return FunctionStats{}, false
	}
	return s, true
}

// FunctionNames returns the names of every function row, ordered by name, and
// whether the read succeeded. It is the live-set reader the worker's flush
// sweep consults to know which functions currently exist, so its metrics
// registry can drop series for functions that no longer exist (see
// metrics.SweepFunctionMetrics). A removed function has no functions row, so
// it is absent here and its stale series are swept. The second return is the
// ok flag: (names, true) on success, (nil, false) on error (which is logged).
// The worker's sweep treats a failed read as an unknown live set and must fail
// OPEN — retain every series; the next successful flush sweeps — because
// sweeping with an empty set would delete live functions' series. This is why
// callers must distinguish an empty-but-known live set from a failed read.
func (c *State) FunctionNames() ([]string, bool) {
	ctx := context.Background()
	rows, err := c.db.QueryContext(ctx, `SELECT name FROM functions ORDER BY name`)
	if err != nil {
		c.log.Printf("state: list function names: %v", err)
		return nil, false
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.log.Printf("state: scan function name: %v", err)
			return out, false
		}
		out = append(out, name)
	}
	return out, true
}

// AllFunctionStats returns every function_stats row ordered by function name.
// The worker uses it at startup to restore counters for functions that have
// persisted stats even when the fresh registry snapshot is empty (e.g. a
// function idle this process but active last process). It returns nil on error
// (which is logged), matching the file's non-fatal style.
func (c *State) AllFunctionStats() []FunctionStats {
	ctx := context.Background()
	rows, err := c.db.QueryContext(ctx,
		`SELECT function_name, events_processed_total, handler_success_total,
		        handler_failure_total, retry_total, dlq_total, updated_at
		 FROM function_stats ORDER BY function_name`)
	if err != nil {
		c.log.Printf("state: list function stats: %v", err)
		return nil
	}
	defer rows.Close()

	var out []FunctionStats
	for rows.Next() {
		var s FunctionStats
		if err := rows.Scan(&s.Function, &s.EventsProcessedTotal, &s.HandlerSuccessTotal,
			&s.HandlerFailureTotal, &s.RetryTotal, &s.DLQTotal, &s.UpdatedAt); err != nil {
			c.log.Printf("state: scan function stats: %v", err)
			return out
		}
		out = append(out, s)
	}
	return out
}

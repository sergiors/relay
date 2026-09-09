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

// RecordFunctionStats upserts the function_stats row for s.Function, replacing
// every counter column with the supplied value and setting updated_at to now().
// There is no accumulation: the worker hands over the CURRENT cumulative
// registry values, so the row always mirrors the latest known totals. It is
// non-fatal on error: it logs and returns.
func (c *State) RecordFunctionStats(s FunctionStats) {
	ctx := context.Background()
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

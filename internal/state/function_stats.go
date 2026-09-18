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
//
// The four Last*At fields are per-function execution-history timestamps in the
// RFC3339 convention of UpdatedAt (empty string = never observed):
// LastExecutionAt is the last handler-execution attempt (retries count — every
// claimed attempt is an execution); LastSuccessAt / LastFailureAt the last
// successful / failed handler execution (a failed attempt that will retry
// counts as a failure); LastDLQAt the last invocation that exhausted its
// retries and was ROUTED TO THE DLQ (the actual DLQ attribution point, not
// every failure). They are reset by nothing but a genuine removal: unlike the
// counters, an incoming empty value must never clobber a persisted timestamp
// (see the upsert's CASE guards).
//
// WarmAcquiresTotal / ColdStartsTotal / DiscardedTotal are the cumulative
// warm-container pool counters (see runtime.PoolSnapshot): warm acquires served
// by an existing idle container, cold starts that created a fresh container,
// and container discards (all reasons summed). They are cumulative absolute
// totals like the other counters, persisted so the standalone
// `relay function inspect` process can render the Runtime pool section without
// access to the worker's in-memory pool. The LIVE pool gauges (capacity,
// container counts by lease state) are deliberately NOT persisted: a persisted
// live gauge would go stale between flushes.
type FunctionStats struct {
	Function             string
	EventsProcessedTotal int64
	HandlerSuccessTotal  int64
	HandlerFailureTotal  int64
	RetryTotal           int64
	DLQTotal             int64
	WarmAcquiresTotal    int64
	ColdStartsTotal      int64
	DiscardedTotal       int64
	LastExecutionAt      string
	LastSuccessAt        string
	LastFailureAt        string
	LastDLQAt            string
	UpdatedAt            string
}

// tsColumns lists the four execution-history timestamp columns and their
// FunctionStats destination fields, in the order the upsert and the readers use.
var tsCols = []struct {
	col, field string
}{
	{"last_execution_at", "LastExecutionAt"},
	{"last_success_at", "LastSuccessAt"},
	{"last_failure_at", "LastFailureAt"},
	{"last_dlq_at", "LastDLQAt"},
}

// functionStatsTSUpsert builds the ON CONFLICT DO UPDATE SET clauses for the
// four timestamp columns: counters are plainly REPLACED above it, while an
// empty incoming timestamp PRESERVES the previously stored one (services must
// never regress to "never" because one flush had no timestamp observation).
// Each guard also COALESCEs the stored column so the value never becomes NULL
// (a legacy row migrated from a pre-column schema stores NULL, which the
// readers treat as empty).
func functionStatsTSUpsert() []string {
	out := make([]string, 0, len(tsCols))
	for _, c := range tsCols {
		out = append(out, c.col+` = CASE
			        WHEN excluded.`+c.col+` IS NOT NULL AND excluded.`+c.col+` <> ''
			        THEN excluded.`+c.col+` ELSE COALESCE(function_stats.`+c.col+`, '') END`)
	}
	return out
}

// RecordFunctionStats upserts the function_stats row for s.Function. It is a
// thin wrapper over RecordFunctionStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (c *State) RecordFunctionStats(s FunctionStats) {
	c.RecordFunctionStatsContext(context.Background(), s)
}

// RecordFunctionStatsContext upserts the function_stats row for s.Function,
// replacing every counter column with the supplied value and setting updated_at
// to now(). Counter columns behave as an absolute snapshot (see RecordStats);
// the four last_*_at timestamp columns are additionally CASE-guarded so an
// EMPTY incoming value preserves the previously stored timestamp instead of
// overwriting it with "" — "no observation" must never erase "last observed
// at". Callers must pass CURRENT cumulative values; the worker seeds the
// fresh process registry from this table at startup so the first snapshot
// never resets counters. It is non-fatal on error: it logs and returns.
func (c *State) RecordFunctionStatsContext(ctx context.Context, s FunctionStats) {
	ts := now()
	upd := `events_processed_total  = excluded.events_processed_total,
		   handler_success_total   = excluded.handler_success_total,
		   handler_failure_total   = excluded.handler_failure_total,
		   retry_total             = excluded.retry_total,
		   dlq_total               = excluded.dlq_total,
		   warm_acquires_total     = excluded.warm_acquires_total,
		   cold_starts_total       = excluded.cold_starts_total,
		   discarded_total         = excluded.discarded_total,`
	for _, g := range functionStatsTSUpsert() {
		upd += "\n" + g + ","
	}
	upd += "\n	   updated_at              = excluded.updated_at"
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO function_stats
		   (function_name, events_processed_total, handler_success_total,
		    handler_failure_total, retry_total, dlq_total,
		    warm_acquires_total, cold_starts_total, discarded_total,
		    last_execution_at, last_success_at, last_failure_at, last_dlq_at, updated_at)
		 VALUES
		   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(function_name) DO UPDATE SET
		   `+upd,
		s.Function, s.EventsProcessedTotal, s.HandlerSuccessTotal,
		s.HandlerFailureTotal, s.RetryTotal, s.DLQTotal,
		s.WarmAcquiresTotal, s.ColdStartsTotal, s.DiscardedTotal,
		s.LastExecutionAt, s.LastSuccessAt, s.LastFailureAt, s.LastDLQAt, ts)
	if err != nil {
		c.log.Warn("State: record function stats failed", "function", s.Function, "error", err)
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
		        handler_failure_total, retry_total, dlq_total,
		        warm_acquires_total, cold_starts_total, discarded_total,
		        COALESCE(last_execution_at, ''), COALESCE(last_success_at, ''),
		        COALESCE(last_failure_at, ''), COALESCE(last_dlq_at, ''), updated_at
		 FROM function_stats WHERE function_name = ?`, name,
	).Scan(&s.Function, &s.EventsProcessedTotal, &s.HandlerSuccessTotal,
		&s.HandlerFailureTotal, &s.RetryTotal, &s.DLQTotal,
		&s.WarmAcquiresTotal, &s.ColdStartsTotal, &s.DiscardedTotal,
		&s.LastExecutionAt, &s.LastSuccessAt, &s.LastFailureAt, &s.LastDLQAt, &s.UpdatedAt)
	if err == sql.ErrNoRows {
		return FunctionStats{}, false
	}
	if err != nil {
		c.log.Warn("State: read function stats failed", "function", name, "error", err)
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
		c.log.Warn("State: list function names failed", "error", err)
		return nil, false
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.log.Warn("State: scan function name failed", "error", err)
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
		        handler_failure_total, retry_total, dlq_total,
		        warm_acquires_total, cold_starts_total, discarded_total,
		        COALESCE(last_execution_at, ''), COALESCE(last_success_at, ''),
		        COALESCE(last_failure_at, ''), COALESCE(last_dlq_at, ''), updated_at
		 FROM function_stats ORDER BY function_name`)
	if err != nil {
		c.log.Warn("State: list function stats failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []FunctionStats
	for rows.Next() {
		var s FunctionStats
		if err := rows.Scan(&s.Function, &s.EventsProcessedTotal, &s.HandlerSuccessTotal,
			&s.HandlerFailureTotal, &s.RetryTotal, &s.DLQTotal,
			&s.WarmAcquiresTotal, &s.ColdStartsTotal, &s.DiscardedTotal,
			&s.LastExecutionAt, &s.LastSuccessAt, &s.LastFailureAt, &s.LastDLQAt, &s.UpdatedAt); err != nil {
			c.log.Warn("State: scan function stats failed", "error", err)
			return out
		}
		out = append(out, s)
	}
	return out
}

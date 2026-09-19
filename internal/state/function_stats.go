package state

import (
	"context"
	"database/sql"
)

// FunctionStats is the per-function operational snapshot persisted as the JSON
// payload of the function_stats table. Unlike the single-row global Stats, it is
// keyed by function name (the relational function_stats.function_name column) so
// each function's payload is attributed independently.
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
// (see mergeFunctionStatsTimestamps).
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
//
// The struct is the source of truth for the payload. Function and UpdatedAt are
// stable relational metadata (function_stats.function_name / .updated_at) and
// are excluded from the JSON by their json:"-" tags; JSON (un)marshalling is
// centralized in stats_json.go.
type FunctionStats struct {
	Function string `json:"-"`
	// Counters are absolute snapshots: always emitted (even when 0) so a
	// legitimate reset is representable. The Last*At fields are omitted when
	// empty, which is what lets an empty incoming value preserve the persisted
	// timestamp during the flush merge.
	EventsProcessedTotal int64  `json:"events_processed_total"`
	HandlerSuccessTotal  int64  `json:"handler_success_total"`
	HandlerFailureTotal  int64  `json:"handler_failure_total"`
	RetryTotal           int64  `json:"retry_total"`
	DLQTotal             int64  `json:"dlq_total"`
	WarmAcquiresTotal    int64  `json:"warm_acquires_total"`
	ColdStartsTotal      int64  `json:"cold_starts_total"`
	DiscardedTotal       int64  `json:"discarded_total"`
	LastExecutionAt      string `json:"last_execution_at,omitempty"`
	LastSuccessAt        string `json:"last_success_at,omitempty"`
	LastFailureAt        string `json:"last_failure_at,omitempty"`
	LastDLQAt            string `json:"last_dlq_at,omitempty"`
	UpdatedAt            string `json:"-"`
}

// RecordFunctionStats upserts the function_stats row for s.Function. It is a
// thin wrapper over RecordFunctionStatsContext using a background context, kept
// for callers (tests, CLI) that do not need to bound the write.
func (c *State) RecordFunctionStats(s FunctionStats) {
	c.RecordFunctionStatsContext(context.Background(), s)
}

// RecordFunctionStatsContext upserts the function_stats row for s.Function,
// replacing the JSON payload with the supplied value and setting updated_at to
// now(). Counters behave as an absolute snapshot (see RecordStats); the
// four Last*At timestamps are additionally merged so an EMPTY incoming value
// preserves the previously stored timestamp instead of overwriting it with ""
// — "no observation" must never erase "last observed at". The read and write
// happen in one transaction (the same short-transaction pattern as
// RecordStatsSnapshot), so the merge is atomic with respect to other writers.
// Callers must pass CURRENT cumulative values; the worker seeds the fresh
// process registry from this table at startup so the first snapshot never
// resets counters. It is non-fatal on error: it logs and returns.
func (c *State) RecordFunctionStatsContext(ctx context.Context, s FunctionStats) {
	err := c.rebuildTx(ctx, func(tx *sql.Tx) error {
		stored := c.storedFunctionStatsTx(ctx, tx, s.Function)
		merged := mergeFunctionStatsTimestamps(stored, s)
		payload, err := marshalFunctionStats(merged)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO function_stats (function_name, data, updated_at) VALUES (?, ?, ?)
			 ON CONFLICT(function_name) DO UPDATE SET
			   data       = excluded.data,
			   updated_at = excluded.updated_at`,
			s.Function, payload, now())
		return err
	})
	if err != nil {
		c.log.Warn("State: record function stats failed", "function", s.Function, "error", err)
	}
}

// FunctionStats returns the per-function snapshot for name, or (zero, false)
// when no row has been recorded yet or the read/decoding fails (which is
// logged). The Function field is set to name on success (it is relational
// metadata, not part of the JSON payload).
func (c *State) FunctionStats(name string) (FunctionStats, bool) {
	ctx := context.Background()
	var data sql.NullString
	var updatedAt sql.NullString
	err := c.db.QueryRowContext(ctx,
		`SELECT data, updated_at FROM function_stats WHERE function_name = ?`, name,
	).Scan(&data, &updatedAt)
	if err == sql.ErrNoRows {
		return FunctionStats{}, false
	}
	if err != nil {
		c.log.Warn("State: read function stats failed", "function", name, "error", err)
		return FunctionStats{}, false
	}
	s, err := unmarshalFunctionStats(data)
	if err != nil {
		c.log.Warn("State: read function stats failed", "function", name, "error", err)
		return FunctionStats{}, false
	}
	s.Function = name
	s.UpdatedAt = updatedAt.String
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
// function idle this process but active last process). The Function field is
// taken from the relational key column (not the JSON payload). A row whose JSON
// is invalid is logged and skipped (its stats are unknowable); it returns nil
// on a read error, matching the file's non-fatal style.
func (c *State) AllFunctionStats() []FunctionStats {
	ctx := context.Background()
	rows, err := c.db.QueryContext(ctx,
		`SELECT function_name, data, updated_at FROM function_stats ORDER BY function_name`)
	if err != nil {
		c.log.Warn("State: list function stats failed", "error", err)
		return nil
	}
	defer rows.Close()

	var out []FunctionStats
	for rows.Next() {
		var name string
		var data, updatedAt sql.NullString
		if err := rows.Scan(&name, &data, &updatedAt); err != nil {
			c.log.Warn("State: scan function stats failed", "error", err)
			return out
		}
		s, err := unmarshalFunctionStats(data)
		if err != nil {
			c.log.Warn("State: read function stats failed", "function", name, "error", err)
			continue
		}
		s.Function = name
		s.UpdatedAt = updatedAt.String
		out = append(out, s)
	}
	return out
}
